from __future__ import annotations

import json
from hashlib import sha256
from typing import Any

from groupagent.domain import ActorContext, DomainError, TeamTask
from groupagent.memory.causality import after_boundary_sql, causal_sources
from groupagent.memory.common import BoundStore, memory_lock, source_rows, topic_row
from groupagent.memory.compaction import CompactionMethods
from groupagent.memory.deletions import DeletionSink
from groupagent.memory.facts import FactMethods
from groupagent.memory.forget import ForgetMethods
from groupagent.memory.retrieval import RetrievalMethods
from groupagent.memory.summarizers import Summarizer
from groupagent.persistence import PostgresStore, UnitOfWork


class MemoryService(RetrievalMethods, FactMethods, CompactionMethods, ForgetMethods):
    def __init__(
        self,
        store: PostgresStore,
        summarizer: Summarizer | None = None,
        *,
        deletion_sink: DeletionSink | None = None,
    ) -> None:
        self.store = store
        self.summarizer = summarizer
        self.deletion_sink = deletion_sink

    @staticmethod
    def prepare_in_tx(tx: UnitOfWork) -> None:
        """Acquire before coordinator event-row locks for a consistent write order."""
        memory_lock(tx)

    def in_tx(self, tx: UnitOfWork) -> MemoryService:
        return MemoryService(BoundStore(tx), self.summarizer, deletion_sink=self.deletion_sink)

    @staticmethod
    def _fingerprint(tx: UnitOfWork, event_id: str) -> str:
        return tx.event_fingerprint(event_id)

    def task_query_in_tx(
        self,
        tx: UnitOfWork,
        query: str = "",
        *,
        topic_id: str | None = None,
        limit: int = 20,
        public_only: bool = False,
        through_sequence: int | None = None,
    ) -> tuple[list[TeamTask], dict[str, Any]]:
        """Read a bounded task list and its scoped membership/revision witness.

        A business revision invalidates consumers of that task or query, never
        an unrelated conversation. Privacy still uses the shared epoch/lock.
        """
        if len(query) > 1000 or type(limit) is not int or not 1 <= limit <= 100:
            raise DomainError("invalid_input", "Task query or result limit is too large")
        memory_lock(tx)
        topic_row(tx, topic_id)
        cutoff = (
            tx.conn.execute(
                "SELECT created_at FROM ga_events WHERE group_id=%s AND sequence=%s",
                (tx.group_id, through_sequence),
            ).fetchone()
            if through_sequence is not None
            else None
        )
        topic_events = (
            {
                row["event_id"]
                for row in tx.conn.execute(
                    "SELECT event_id FROM ga_topic_events WHERE group_id=%s AND topic_id=%s",
                    (tx.group_id, topic_id),
                ).fetchall()
            }
            if topic_id
            else None
        )
        selected = []
        for task in tx.find_team_tasks(query, limit=1000):
            if through_sequence is not None:
                if task.source_event_ids:
                    future = tx.conn.execute(
                        "SELECT 1 FROM ga_events e WHERE e.group_id=%s AND e.id=ANY(%s) AND "
                        + after_boundary_sql("e")
                        + " LIMIT 1",
                        (tx.group_id, task.source_event_ids, through_sequence, through_sequence),
                    ).fetchone()
                    if future:
                        continue
                elif cutoff is None or task.updated_at > cutoff["created_at"]:
                    continue
            if topic_events is not None and not set(task.source_event_ids) & topic_events:
                continue
            try:
                if task.source_event_ids:
                    source_rows(
                        tx,
                        task.source_event_ids,
                        shared=public_only,
                        allow_expired=True,
                        require_complete=True,
                    )
            except DomainError as error:
                if error.code not in {
                    "source_unavailable",
                    "permission_denied",
                    "scope_violation",
                    "not_found",
                }:
                    raise
                continue
            selected.append(task)
            if len(selected) >= limit:
                break
        witness = {
            "query": query,
            "topic_id": topic_id,
            "limit": limit,
            "public_only": public_only,
            "fingerprint": sha256(
                json.dumps([(task.id, task.revision) for task in selected]).encode()
            ).hexdigest(),
        }
        if through_sequence is not None:
            witness["through_sequence"] = through_sequence
        return selected, witness

    def snapshot_in_tx(
        self,
        tx: UnitOfWork,
        query: str = "",
        *,
        topic_id: str | None = None,
        limit: int = 20,
        source_event_ids: list[str] | None = None,
    ) -> dict:
        epoch = memory_lock(tx)
        if source_event_ids:
            source_rows(tx, source_event_ids)
        hits = self.in_tx(tx).retrieve(tx.actor, query, topic_id=topic_id, limit=limit)
        sources = set(source_event_ids or [])
        sources.update(ref.event_id for hit in hits for ref in hit.source_refs)
        sources = causal_sources(tx, sources)
        task_count = sum(hit.kind == "team_task" for hit in hits)
        task_queries = []
        if task_count:
            _, witness = self.task_query_in_tx(tx, query, limit=task_count)
            task_queries.append(witness)
        return {
            "group_id": tx.group_id,
            "member_id": tx.actor.member_id,
            "policy_revision": tx.actor.policy_revision,
            "topic_id": topic_id,
            "privacy_epoch": epoch,
            "items": [hit.model_dump(mode="json") for hit in hits],
            "source_fingerprints": {
                identifier: self._fingerprint(tx, identifier) for identifier in sorted(sources)
            },
            "source_task_revisions": {
                task_id: revision
                for hit in hits
                for task_id, revision in hit.source_task_revisions.items()
            },
            "input_event_ids": source_event_ids or [],
            "summaries_are_lossy": True,
            "task_queries": task_queries,
        }

    def assert_snapshot_in_tx(self, tx: UnitOfWork, snapshot: dict) -> None:
        epoch = memory_lock(tx)
        if (
            snapshot.get("group_id"),
            snapshot.get("member_id"),
            snapshot.get("policy_revision"),
        ) != (tx.group_id, tx.actor.member_id, tx.actor.policy_revision):
            raise DomainError(
                "scope_violation", "Memory snapshot belongs to another trusted context"
            )
        if snapshot.get("privacy_epoch") != epoch:
            raise DomainError(
                "memory_changed", "Memory changed while a provider candidate was being generated"
            )
        through = snapshot.get("through_sequence")
        if through is not None and (
            type(through) is not int
            or tx.conn.execute(
                "SELECT 1 FROM ga_events e WHERE e.group_id=%s AND e.id=ANY(%s) AND "
                + after_boundary_sql("e")
                + " LIMIT 1",
                (tx.group_id, list(snapshot.get("source_fingerprints", {})), through, through),
            ).fetchone()
        ):
            raise DomainError("memory_changed", "Evidence is outside this turn's causal boundary")
        tx.assert_task_sources(snapshot.get("source_task_revisions", {}))
        facts = snapshot.get("source_fact_revisions", {})
        if (
            not isinstance(facts, dict)
            or len(facts) > 100
            or any(
                not isinstance(identifier, str) or type(revision) is not int
                for identifier, revision in facts.items()
            )
        ):
            raise DomainError("scope_violation", "Fact revision witnesses must be bounded")
        if facts:
            rows = tx.conn.execute(
                "SELECT id,revision FROM ga_memory_facts "
                "WHERE group_id=%s AND id=ANY(%s) AND state='active'",
                (tx.group_id, list(facts)),
            ).fetchall()
            if {row["id"]: row["revision"] for row in rows} != facts:
                raise DomainError("memory_changed", "A consumed memory fact changed")
        queries = snapshot.get("task_queries", [])
        if not isinstance(queries, list) or len(queries) > 8:
            raise DomainError("scope_violation", "Task query witnesses must be bounded")
        for witness in queries:
            if (
                not isinstance(witness, dict)
                or set(witness) - {"through_sequence"}
                != {"query", "topic_id", "limit", "public_only", "fingerprint"}
                or not isinstance(witness["query"], str)
                or witness["topic_id"] is not None
                and not isinstance(witness["topic_id"], str)
                or type(witness["public_only"]) is not bool
            ):
                raise DomainError("scope_violation", "Task query witness is invalid")
            _, current = self.task_query_in_tx(
                tx,
                witness["query"],
                topic_id=witness["topic_id"],
                limit=witness["limit"],
                public_only=witness["public_only"],
                through_sequence=witness.get("through_sequence"),
            )
            if current != witness:
                raise DomainError("memory_changed", "A consumed task query changed")
        if snapshot.get("input_event_ids"):
            source_rows(tx, snapshot["input_event_ids"])
        for identifier, fingerprint in snapshot.get("source_fingerprints", {}).items():
            if self._fingerprint(tx, identifier) != fingerprint:
                raise DomainError("memory_changed", "A canonical source changed after the snapshot")

    def query_context(
        self,
        actor: ActorContext,
        query: str = "",
        *,
        topic_id: str | None = None,
        limit: int = 20,
        source_event_ids: list[str] | None = None,
    ) -> dict:
        with self.store.transaction(actor) as tx:
            return self.snapshot_in_tx(
                tx, query, topic_id=topic_id, limit=limit, source_event_ids=source_event_ids
            )
