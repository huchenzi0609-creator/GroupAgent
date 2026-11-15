from __future__ import annotations

import hashlib
import json
from typing import Literal
from uuid import uuid4

from psycopg.types.json import Jsonb
from pydantic import JsonValue

from groupagent.domain import ActorContext, DomainError, Role
from groupagent.memory.common import invalidate_summaries, memory_lock, source_rows, topic_row
from groupagent.memory.models import Fact
from groupagent.persistence import PostgresStore, UnitOfWork


class FactMethods:
    store: PostgresStore

    @staticmethod
    def _fact(tx: UnitOfWork, fact_id: str):
        row = tx.conn.execute(
            "SELECT * FROM ga_memory_facts WHERE group_id=%s AND id=%s FOR UPDATE",
            (tx.group_id, fact_id),
        ).fetchone()
        if row is None:
            raise DomainError("not_found", "Fact was not found in this group")
        topic_row(tx, row["topic_id"])
        return row

    @staticmethod
    def _version_fact(tx: UnitOfWork, row) -> None:
        tx.conn.execute(
            "INSERT INTO ga_fact_revisions(group_id,fact_id,revision,predicate,value,source_event_ids,state,changed_by) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
            (
                tx.group_id,
                row["id"],
                row["revision"],
                row["predicate"],
                Jsonb(row["value"]),
                Jsonb(row["source_event_ids"]),
                row["state"],
                tx.actor.member_id,
            ),
        )
        for source in row["source_event_ids"]:
            tx.conn.execute(
                "INSERT INTO ga_fact_sources(group_id,fact_id,event_id) VALUES (%s,%s,%s) ON CONFLICT DO NOTHING",
                (tx.group_id, row["id"], source),
            )

    def record_fact(
        self,
        actor: ActorContext,
        predicate: str,
        value: JsonValue,
        *,
        source_event_ids: list[str],
        explicit: bool,
        kind: Literal["fact", "decision"] = "fact",
        subject_id: str | None = None,
        topic_id: str | None = None,
        request_key: str | None = None,
    ) -> Fact:
        if explicit is not True or not predicate.strip() or kind not in {"fact", "decision"}:
            raise DomainError(
                "invalid_input",
                "Long-term memory requires an explicit statement and valid predicate",
            )
        if len(json.dumps(value, ensure_ascii=False).encode()) > 16000:
            raise DomainError("quota_exceeded", "Fact value exceeds the bounded memory record size")
        subject_id = (
            subject_id if subject_id is not None else (actor.member_id if kind == "fact" else None)
        )
        if subject_id is not None and subject_id != actor.member_id and actor.role != Role.OWNER:
            raise DomainError(
                "permission_denied", "Members cannot assert another person's personal facts"
            )
        key = (
            request_key
            or "fact:"
            + hashlib.sha256(
                json.dumps(
                    [source_event_ids, predicate, kind, subject_id], ensure_ascii=False
                ).encode()
            ).hexdigest()
        )
        with self.store.transaction(actor) as tx:
            tx._writer()
            memory_lock(tx)
            topic = topic_row(tx, topic_id)
            source_rows(
                tx,
                source_event_ids,
                shared=topic is None or topic["visibility"] == "group",
                require_complete=True,
            )
            old = tx.conn.execute(
                "SELECT * FROM ga_memory_facts WHERE group_id=%s AND request_key=%s",
                (actor.group_id, key),
            ).fetchone()
            if old:
                tx._controller(old, "created_by")
                if old["state"] != "active":
                    raise DomainError(
                        "source_unavailable", "An invalidated fact cannot be revived by replay"
                    )
                if (
                    old["predicate"],
                    old["value"],
                    old["subject_id"],
                    old["kind"],
                    old["topic_id"],
                ) != (predicate, value, subject_id, kind, topic_id):
                    raise DomainError(
                        "idempotency_conflict", "Fact request key belongs to another statement"
                    )
                return Fact.model_validate(old)
            row = tx.conn.execute(
                "INSERT INTO ga_memory_facts(id,group_id,topic_id,subject_id,kind,predicate,value,source_event_ids,created_by,request_key) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING *",
                (
                    uuid4().hex,
                    actor.group_id,
                    topic_id,
                    subject_id,
                    kind,
                    predicate,
                    Jsonb(value),
                    Jsonb(source_event_ids),
                    actor.member_id,
                    key,
                ),
            ).fetchone()
            assert row is not None
            self._version_fact(tx, row)
            tx._audit("fact_recorded", row["id"], source_event_ids=source_event_ids, kind=kind)
            return Fact.model_validate(row)

    def get_fact(self, actor: ActorContext, fact_id: str) -> Fact:
        with self.store.transaction(actor) as tx:
            row = self._fact(tx, fact_id)
            if row["state"] != "active":
                raise DomainError("source_unavailable", "Fact is no longer active")
            source_rows(tx, row["source_event_ids"], allow_expired=True, require_complete=True)
            return Fact.model_validate(row)

    def correct_fact(
        self,
        actor: ActorContext,
        fact_id: str,
        value: JsonValue,
        *,
        expected_revision: int,
        source_event_ids: list[str],
        explicit: bool,
    ) -> Fact:
        if explicit is not True or len(json.dumps(value, ensure_ascii=False).encode()) > 16000:
            raise DomainError("invalid_input", "Correction needs an explicit bounded statement")
        with self.store.transaction(actor) as tx:
            memory_lock(tx)
            old = self._fact(tx, fact_id)
            tx._controller(old, "created_by")
            tx._revision(old, expected_revision)
            if old["state"] != "active":
                raise DomainError(
                    "source_unavailable", "Inactive facts require a new authorized statement"
                )
            topic = topic_row(tx, old["topic_id"])
            source_rows(
                tx,
                source_event_ids,
                shared=topic is None or topic["visibility"] == "group",
                require_complete=True,
            )
            tx.conn.execute(
                "UPDATE ga_fact_revisions SET state='superseded' WHERE group_id=%s AND fact_id=%s AND state='active'",
                (actor.group_id, fact_id),
            )
            row = tx.conn.execute(
                "UPDATE ga_memory_facts SET value=%s,source_event_ids=%s,revision=revision+1,updated_at=clock_timestamp() "
                "WHERE group_id=%s AND id=%s RETURNING *",
                (Jsonb(value), Jsonb(source_event_ids), actor.group_id, fact_id),
            ).fetchone()
            assert row is not None
            self._version_fact(tx, row)
            invalidate_summaries(tx, old["source_event_ids"])
            tx._audit(
                "fact_corrected",
                fact_id,
                revision=row["revision"],
                source_event_ids=source_event_ids,
            )
            return Fact.model_validate(row)

    def invalidate_fact(self, actor: ActorContext, fact_id: str, *, expected_revision: int) -> None:
        with self.store.transaction(actor) as tx:
            memory_lock(tx)
            row = self._fact(tx, fact_id)
            tx._controller(row, "created_by")
            tx._revision(row, expected_revision)
            tx.conn.execute(
                "UPDATE ga_memory_facts SET state='invalidated',revision=revision+1,updated_at=clock_timestamp() WHERE group_id=%s AND id=%s",
                (actor.group_id, fact_id),
            )
            tx.conn.execute(
                "UPDATE ga_fact_revisions SET state='invalidated' WHERE group_id=%s AND fact_id=%s AND state='active'",
                (actor.group_id, fact_id),
            )
            invalidate_summaries(tx, row["source_event_ids"])
            tx._audit("fact_invalidated", fact_id)
