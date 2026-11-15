from __future__ import annotations

from groupagent.domain import ActorContext, DomainError
from groupagent.memory.common import BoundStore, memory_lock
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

    def snapshot_in_tx(
        self,
        tx: UnitOfWork,
        query: str = "",
        *,
        topic_id: str | None = None,
        limit: int = 20,
        source_event_ids: list[str] | None = None,
    ) -> dict:
        from groupagent.memory.common import source_rows

        epoch = memory_lock(tx)
        if source_event_ids:
            source_rows(tx, source_event_ids)
        hits = self.in_tx(tx).retrieve(tx.actor, query, topic_id=topic_id, limit=limit)
        sources = set(source_event_ids or [])
        sources.update(ref.event_id for hit in hits for ref in hit.source_refs)
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
        }

    def assert_snapshot_in_tx(self, tx: UnitOfWork, snapshot: dict) -> None:
        from groupagent.memory.common import source_rows

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
        tx.assert_task_sources(snapshot.get("source_task_revisions", {}))
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
