from __future__ import annotations

import json
from uuid import uuid4

from psycopg.types.json import Jsonb

from groupagent.domain import ActorContext, DomainError, Role
from groupagent.memory.common import invalidate_summaries, memory_lock, source_rows, topic_row
from groupagent.memory.models import MemoryHit, SourceReference, Topic
from groupagent.persistence import PostgresStore, UnitOfWork


def references(tx: UnitOfWork, event_ids: list[str]) -> list[SourceReference]:
    rows = tx.conn.execute(
        "SELECT id,actor_id,occurred_at,memory_state FROM ga_events WHERE group_id=%s AND id=ANY(%s) ORDER BY sequence",
        (tx.group_id, event_ids),
    ).fetchall()
    return [
        SourceReference(
            event_id=row["id"],
            actor_id=row["actor_id"],
            occurred_at=row["occurred_at"],
            state=row["memory_state"],
        )
        for row in rows
    ]


def visible_events(
    tx: UnitOfWork,
    *,
    topic_id: str | None,
    after_sequence: int = 0,
    query: str = "",
    limit: int = 100,
    public_only: bool = False,
    newest_first: bool = False,
):
    topic_row(tx, topic_id)
    pattern = "%" + query.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_") + "%"
    rows = tx.conn.execute(
        "SELECT e.* FROM ga_events e WHERE e.group_id=%s AND e.memory_state='active' AND e.sequence>%s "
        "AND e.kind IN ('text','quote','image','audio','file','forwarded','unsupported_video') "
        "AND (%s::text IS NULL OR EXISTS (SELECT 1 FROM ga_topic_events r WHERE r.group_id=e.group_id AND r.event_id=e.id AND r.topic_id=%s)) "
        "AND NOT EXISTS (SELECT 1 FROM ga_topic_events r JOIN ga_memory_topics t ON t.group_id=r.group_id AND t.id=r.topic_id "
        "WHERE r.group_id=e.group_id AND r.event_id=e.id AND t.visibility='participants' "
        "AND (%s OR NOT (%s OR t.created_by=%s OR t.participant_ids ? %s))) "
        "AND e.body->>'text' ILIKE %s ORDER BY "
        + ("e.occurred_at DESC,e.sequence DESC" if newest_first else "e.sequence")
        + " LIMIT %s",
        (
            tx.group_id,
            after_sequence,
            topic_id,
            topic_id,
            public_only,
            tx.actor.role == Role.OWNER,
            tx.actor.member_id,
            tx.actor.member_id,
            pattern,
            limit,
        ),
    ).fetchall()
    result = []
    for row in rows:
        canonical = tx.get_event(row["id"])
        if canonical.content_state == "active":
            result.append({**row, "body": canonical.model_dump(mode="json")})
    return result


class RetrievalMethods:
    store: PostgresStore

    def create_topic(
        self,
        actor: ActorContext,
        title: str,
        *,
        participant_ids: list[str] | None = None,
        visibility: str = "group",
    ) -> Topic:
        if not title.strip() or visibility not in {"group", "participants"}:
            raise DomainError("invalid_input", "Topic title and visibility are required")
        participants = list(dict.fromkeys([actor.member_id, *(participant_ids or [])]))
        with self.store.transaction(actor) as tx:
            tx._writer()
            row = tx.conn.execute(
                "INSERT INTO ga_memory_topics(id,group_id,title,visibility,participant_ids,created_by) VALUES (%s,%s,%s,%s,%s,%s) RETURNING *",
                (
                    uuid4().hex,
                    actor.group_id,
                    title,
                    visibility,
                    Jsonb(participants),
                    actor.member_id,
                ),
            ).fetchone()
            return Topic.model_validate(row)

    def link_event(
        self, actor: ActorContext, event_id: str, topic_id: str, *, evidence_event_ids: list[str]
    ) -> None:
        with self.store.transaction(actor) as tx:
            memory_lock(tx)
            topic = topic_row(tx, topic_id)
            assert topic is not None
            event = source_rows(tx, [event_id])[0]
            if event["actor_id"] != actor.member_id and actor.role != Role.OWNER:
                raise DomainError(
                    "permission_denied", "Only an event's author or owner may link its topic"
                )
            source_rows(tx, evidence_event_ids)
            if (
                event_id not in evidence_event_ids
                and event["body"].get("reply_to") not in evidence_event_ids
            ):
                raise DomainError(
                    "ambiguous_reference",
                    "Topic association needs the event or native quote evidence",
                )
            tx.conn.execute(
                "INSERT INTO ga_topic_events(group_id,topic_id,event_id,linked_by,evidence_refs) VALUES (%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING",
                (actor.group_id, topic_id, event_id, actor.member_id, Jsonb(evidence_event_ids)),
            )
            invalidate_summaries(tx, [event_id])
            if topic["visibility"] == "participants":
                tx.conn.execute(
                    "UPDATE ga_memory_facts SET state='invalidated',revision=revision+1 WHERE group_id=%s AND topic_id IS NULL "
                    "AND id IN (SELECT fact_id FROM ga_fact_sources WHERE group_id=%s AND event_id=%s)",
                    (actor.group_id, actor.group_id, event_id),
                )
            tx._audit(
                "topic_event_linked",
                topic_id,
                event_id=event_id,
                evidence_event_ids=evidence_event_ids,
            )

    def topics_for_event(self, actor: ActorContext, event_id: str) -> list[Topic]:
        with self.store.transaction(actor) as tx:
            source_rows(tx, [event_id])
            rows = tx.conn.execute(
                "SELECT t.* FROM ga_memory_topics t JOIN ga_topic_events r ON t.group_id=r.group_id AND t.id=r.topic_id WHERE r.group_id=%s AND r.event_id=%s",
                (actor.group_id, event_id),
            ).fetchall()
            return [Topic.model_validate(row) for row in rows]

    def source_available(self, actor: ActorContext, event_id: str) -> bool:
        try:
            self.source_guard(actor, [event_id])
            return True
        except DomainError as error:
            if error.code in {"not_found", "source_unavailable", "permission_denied"}:
                return False
            raise

    def source_guard(self, actor: ActorContext, event_ids: list[str]) -> None:
        with self.store.transaction(actor) as tx:
            source_rows(tx, event_ids, require_complete=True)

    def retrieve(
        self,
        actor: ActorContext,
        query: str = "",
        *,
        topic_id: str | None = None,
        limit: int = 20,
        include_tasks: bool = True,
    ) -> list[MemoryHit]:
        if len(query) > 1000 or not 1 <= limit <= 100:
            raise DomainError("invalid_input", "Retrieval query or result limit is too large")
        with self.store.transaction(actor) as tx:
            topic_row(tx, topic_id)
            hits: list[MemoryHit] = []
            pattern = (
                "%" + query.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_") + "%"
            )
            for row in visible_events(
                tx, topic_id=topic_id, query=query, limit=limit, newest_first=True
            ):
                hits.append(
                    MemoryHit(
                        kind="event",
                        id=row["id"],
                        text=row["body"].get("text", ""),
                        source_refs=references(tx, [row["id"]]),
                        author_id=row["actor_id"],
                        topic_id=topic_id,
                        retrieval_reason="source_text_match" if query else "recent_source_context",
                    )
                )
            for table, kind in (("ga_memory_facts", "fact"), ("ga_memory_summaries", "summary")):
                text_column = "f.predicate || ' ' || f.value::text" if kind == "fact" else "f.text"
                rows = tx.conn.execute(
                    "SELECT f.* FROM "
                    + table
                    + " f LEFT JOIN ga_memory_topics t ON t.group_id=f.group_id AND t.id=f.topic_id "
                    "WHERE f.group_id=%s AND f.state='active' AND (%s::text IS NULL OR f.topic_id=%s) "
                    "AND (t.id IS NULL OR t.visibility='group' OR %s OR t.created_by=%s OR t.participant_ids ? %s) "
                    "AND (" + text_column + ") ILIKE %s ORDER BY f.created_at DESC LIMIT %s",
                    (
                        actor.group_id,
                        topic_id,
                        topic_id,
                        actor.role == Role.OWNER,
                        actor.member_id,
                        actor.member_id,
                        pattern,
                        limit,
                    ),
                ).fetchall()
                for row in rows:
                    try:
                        source_rows(
                            tx,
                            row["source_event_ids"],
                            allow_expired=kind == "fact",
                            require_complete=kind == "fact",
                        )
                    except DomainError as error:
                        if error.code in {"source_unavailable", "permission_denied", "not_found"}:
                            continue
                        raise
                    hits.append(
                        MemoryHit(
                            kind=row["kind"] if kind == "fact" else "summary",
                            id=row["id"],
                            text=(
                                row["predicate"]
                                + ": "
                                + json.dumps(row["value"], ensure_ascii=False)
                            )
                            if kind == "fact"
                            else row["text"],
                            source_refs=references(tx, row["source_event_ids"]),
                            author_id=row["created_by"],
                            topic_id=row["topic_id"],
                            retrieval_reason="explicit_long_term_record"
                            if kind == "fact"
                            else "derived_summary_match",
                            incomplete=kind == "summary",
                        )
                    )
            if include_tasks and topic_id is None:
                for task in tx.find_team_tasks(query, limit=limit):
                    try:
                        if task.source_event_ids:
                            source_rows(
                                tx, task.source_event_ids, allow_expired=True, require_complete=True
                            )
                    except DomainError as error:
                        if error.code in {"source_unavailable", "permission_denied", "not_found"}:
                            continue
                        raise
                    hits.append(
                        MemoryHit(
                            kind="team_task",
                            id=task.id,
                            text=task.title + " " + task.progress,
                            author_id=task.owner_id,
                            source_refs=references(tx, task.source_event_ids),
                            source_task_revisions={task.id: task.revision},
                            provenance_kind=task.provenance_kind,
                            retrieval_reason="independent_human_task",
                        )
                    )
            # Explicit records are authoritative business state; raw historical
            # discussion and lossy summaries are separately labeled evidence.
            priority = {"fact": 0, "decision": 0, "team_task": 1, "summary": 2, "event": 3}
            return sorted(hits, key=lambda hit: priority[hit.kind])[:limit]

    def query_context(
        self, actor: ActorContext, query: str = "", *, topic_id: str | None = None, limit: int = 20
    ) -> dict:
        return {
            "group_id": actor.group_id,
            "topic_id": topic_id,
            "items": [
                hit.model_dump(mode="json")
                for hit in self.retrieve(actor, query, topic_id=topic_id, limit=limit)
            ],
            "summaries_are_lossy": True,
        }
