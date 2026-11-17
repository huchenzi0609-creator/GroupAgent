"""Short, atomic PostgreSQL transactions for the durable control plane.

This repository is an application authorization boundary, not a database role
for untrusted code. Only the trusted control service holds its DSN. Model/tool
processes must not receive a connection or construct their own ActorContext.
"""

from __future__ import annotations

import hashlib
import re
import threading
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from datetime import datetime
from importlib.resources import files
from typing import Any, Literal
from uuid import uuid4

import psycopg
from psycopg.errors import LockNotAvailable
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from pydantic import JsonValue

from groupagent.domain import (
    ActorContext,
    DeliveryIntent,
    DeliveryState,
    DomainError,
    EventCursor,
    EventKind,
    ExecutionLease,
    GroupEvent,
    IngestResult,
    Role,
    TeamTask,
    TeamTaskState,
    WorkOrder,
    WorkState,
    allows_silent_result,
    check_transition,
)
from groupagent.persistence.delivery_knowledge import projected_delivery_state


@dataclass(frozen=True)
class _OwnerLockWait:
    thread_id: int
    active: Callable[[], bool]
    milliseconds: int


_OWNER_LOCK_WAIT: ContextVar[_OwnerLockWait | None] = ContextVar("owner_lock_wait", default=None)


@contextmanager
def bounded_owner_lock_waits(
    active: Callable[[], bool], *, milliseconds: int = 25
) -> Iterator[None]:
    """Runtime-only policy; inherited tasks retry contention without blocking their loop.

    Worker threads may inherit ContextVars, but must retain normal database lock
    semantics. Every connection remains private to its creating transaction.
    """
    if not 1 <= milliseconds <= 50:
        raise ValueError("Owner lock waits must be between one and fifty milliseconds")
    token = _OWNER_LOCK_WAIT.set(_OwnerLockWait(threading.get_ident(), active, milliseconds))
    try:
        yield
    finally:
        _OWNER_LOCK_WAIT.reset(token)


def _owner_lock_wait() -> _OwnerLockWait | None:
    policy = _OWNER_LOCK_WAIT.get()
    return (
        policy
        if policy is not None and policy.thread_id == threading.get_ident() and policy.active()
        else None
    )


Row = dict[str, Any]
Connection = psycopg.Connection[Row]
_RESTORE_LOCK_KEY = 7342938221337


class _Unchanged:
    pass


_UNCHANGED = _Unchanged()
_VERIFIED_REPLY_ORIGINS = frozenset(
    {
        "verified_channel_metadata",
        "agentwechat_authenticated_message_metadata",
        "authenticated_audio_transcription",
    }
)


def _id() -> str:
    return str(uuid4())


def _work(row: Row) -> WorkOrder:
    return WorkOrder.model_validate({key: row[key] for key in WorkOrder.model_fields})


def _delivery(row: Row) -> DeliveryIntent:
    return DeliveryIntent.model_validate(row)


def _nonempty(value: str, name: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise DomainError("invalid_input", f"{name} must be a nonempty string")


def _ttl(value: int) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= 300:
        raise DomainError("invalid_input", "Lease duration must be between 1 and 300 seconds")


class PostgresStore:
    def __init__(self, dsn: str) -> None:
        self.dsn = dsn

    def connect(self) -> Connection:
        conn = psycopg.connect(self.dsn, row_factory=dict_row)
        policy = _owner_lock_wait()
        if policy is not None:
            try:
                conn.execute(
                    "SELECT set_config('lock_timeout',%s,false)",
                    (str(policy.milliseconds) + "ms",),
                )
                conn.commit()  # Preserve the fresh connection's original IDLE contract.
            except BaseException:
                conn.close()
                raise
        return conn

    def migrate(self) -> int:
        """Apply packaged, checksum-verified migrations in one transaction.

        The advisory lock serializes concurrent startup. A failed migration
        rolls back both its DDL and ledger entry; applied SQL is immutable.
        """
        with self.connect() as conn:
            guard = conn.execute(
                "SELECT pg_try_advisory_xact_lock_shared(%s) AS ready", (_RESTORE_LOCK_KEY,)
            ).fetchone()
            if not guard or not guard["ready"]:
                raise DomainError("restore_busy", "Database restoration is in progress")
            if conn.info.server_version < 160000:
                raise DomainError("unsupported_database", "PostgreSQL 16 or later is required")
            conn.execute("SELECT pg_advisory_xact_lock(1734348201)")
            conn.execute(
                "CREATE TABLE IF NOT EXISTS ga_schema_migrations ("
                "version integer PRIMARY KEY, sha256 text NOT NULL, "
                "applied_at timestamptz NOT NULL DEFAULT clock_timestamp())"
            )
            migration_files = sorted(
                files("groupagent.persistence").joinpath("migrations").iterdir(),
                key=lambda path: path.name,
            )
            version = 0
            for path in migration_files:
                if not path.name.endswith(".sql"):
                    continue
                version = int(path.name.split("_", 1)[0])
                source = path.read_text(encoding="utf-8")
                checksum = hashlib.sha256(source.encode()).hexdigest()
                existing = conn.execute(
                    "SELECT sha256 FROM ga_schema_migrations WHERE version = %s", (version,)
                ).fetchone()
                if existing:
                    if existing["sha256"] != checksum:
                        raise DomainError("migration_changed", "An applied migration was modified")
                    continue
                conn.execute(source)
                conn.execute(
                    "INSERT INTO ga_schema_migrations (version, sha256) VALUES (%s, %s)",
                    (version, checksum),
                )
            return version

    def register_group(self, group_id: str, policy_revision: int = 1) -> None:
        """Trusted startup/configuration only; never expose as a model tool."""
        _nonempty(group_id, "group_id")
        if policy_revision < 1:
            raise DomainError("invalid_input", "Policy revision must be positive")
        with self.connect() as conn:
            guard = conn.execute(
                "SELECT pg_try_advisory_xact_lock_shared(%s) AS ready", (_RESTORE_LOCK_KEY,)
            ).fetchone()
            if not guard or not guard["ready"]:
                raise DomainError("restore_busy", "Database restoration is in progress")
            row = conn.execute(
                "INSERT INTO ga_groups (id, policy_revision) VALUES (%s, %s) "
                "ON CONFLICT (id) DO UPDATE SET policy_revision = EXCLUDED.policy_revision "
                "WHERE ga_groups.policy_revision <= EXCLUDED.policy_revision RETURNING id",
                (group_id, policy_revision),
            ).fetchone()
            if row is None:
                raise DomainError("version_conflict", "Policy revision cannot move backwards")

    @contextmanager
    def transaction(
        self, actor: ActorContext, *, trace_id: str | None = None
    ) -> Iterator[UnitOfWork]:
        trace_id = trace_id or _id()
        bounded_wait = _owner_lock_wait() is not None
        try:
            with self.connect() as conn:
                unit = UnitOfWork(conn, actor, trace_id)
                unit._check_group()
                yield unit
        except LockNotAvailable:
            if bounded_wait:
                # The connection context has already rolled back. This is a
                # resource wait, not changed source evidence or a failed task.
                raise DomainError(
                    "resource_deferred",
                    "Maintenance holds a required database resource; retry the durable operation",
                    retryable=True,
                ) from None
            raise
        except DomainError as error:
            if error.code in {"restore_busy", "restore_pending"}:
                raise  # The target schema may not exist yet; never mutate a restore snapshot.
            # Rejection evidence must survive rollback of the rejected mutation.
            with self.connect() as conn:
                conn.execute(
                    "INSERT INTO ga_audit_events "
                    "(group_id, trace_id, actor_id, policy_revision, action, details) "
                    "SELECT id, %s, %s, %s, 'operation_rejected', %s FROM ga_groups WHERE id = %s",
                    (
                        trace_id,
                        actor.member_id,
                        actor.policy_revision,
                        Jsonb({"code": error.code}),
                        actor.group_id,
                    ),
                )
            raise


class UnitOfWork:
    def __init__(
        self,
        conn: Connection,
        actor: ActorContext,
        trace_id: str,
        *,
        recovery_maintenance: bool = False,
    ) -> None:
        if recovery_maintenance and actor.role not in (Role.OWNER, Role.MAINTAINER):
            raise DomainError(
                "permission_denied",
                "Recovery maintenance requires a trusted administrative identity",
            )
        self.conn = conn
        self.actor = actor
        self.trace_id = trace_id
        self.recovery_maintenance = recovery_maintenance
        self._context_event_rows: dict[str, Any] | None = None
        self._context_event_actor: ActorContext | None = None

    @property
    def group_id(self) -> str:
        return self.actor.group_id

    def _check_group(self) -> None:
        ready = self.conn.execute(
            "SELECT pg_try_advisory_xact_lock_shared(%s) AS ready", (_RESTORE_LOCK_KEY,)
        ).fetchone()
        if not ready or not ready["ready"]:
            raise DomainError("restore_busy", "Database restoration is in progress")
        exists = self.conn.execute(
            "SELECT to_regclass('groupagent_recovery.guard') AS guard"
        ).fetchone()
        if exists and exists["guard"] is not None and not self.recovery_maintenance:
            blocked = self.conn.execute(
                "SELECT 1 FROM groupagent_recovery.guard WHERE singleton AND blocked"
            ).fetchone()
            if blocked:
                raise DomainError(
                    "restore_pending",
                    "Restored data requires privacy and delivery reconciliation before business use",
                )
        # Hold only for this short transaction: a policy change cannot race an
        # already-authorized write, and takes effect before the next operation.
        row = self.conn.execute(
            "SELECT policy_revision FROM ga_groups WHERE id = %s FOR SHARE", (self.group_id,)
        ).fetchone()
        if row is None:
            raise DomainError("permission_denied", "Group is not authorized")
        if row["policy_revision"] != self.actor.policy_revision:
            raise DomainError("policy_changed", "Actor policy revision is stale")
        member = self.conn.execute(
            "SELECT membership_state FROM ga_members WHERE group_id=%s AND member_id=%s",
            (self.group_id, self.actor.member_id),
        ).fetchone()
        if member and member["membership_state"] == "left" and self.actor.role != Role.MAINTAINER:
            raise DomainError(
                "member_left", "Reliable departure revoked this member's group access"
            )

    def _writer(self) -> None:
        if self.actor.role not in (Role.MEMBER, Role.OWNER):
            raise DomainError("permission_denied", "Business changes require member or owner role")

    def _controller(self, row: Row, owner_field: str = "requester_id") -> None:
        self._writer()
        if row[owner_field] != self.actor.member_id and self.actor.role != Role.OWNER:
            raise DomainError(
                "permission_denied", "Only the resource owner or group owner may edit"
            )

    def _audit(
        self, action: str, resource_ref: str | None, **details: JsonValue | list[str]
    ) -> None:
        self.conn.execute(
            "INSERT INTO ga_audit_events "
            "(group_id, trace_id, actor_id, policy_revision, action, resource_ref, details) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s)",
            (
                self.group_id,
                self.trace_id,
                self.actor.member_id,
                self.actor.policy_revision,
                action,
                resource_ref,
                Jsonb(details),
            ),
        )

    @staticmethod
    def _revision(row: Row, expected_revision: int) -> None:
        if row["revision"] != expected_revision:
            raise DomainError("version_conflict", "Resource revision has changed")

    def get_cursor(self, stream_id: str) -> EventCursor | None:
        row = self.conn.execute(
            "SELECT * FROM ga_cursors WHERE group_id = %s AND stream_id = %s",
            (self.group_id, stream_id),
        ).fetchone()
        return EventCursor.model_validate(row) if row else None

    def advance_cursor(
        self, stream_id: str, cursor: str, *, expected_cursor_revision: int, reason: str
    ) -> EventCursor:
        """Commit a trusted transport checkpoint without inventing a group event.

        Opaque cursors have no database-defined order. The transport validates
        stream/account scope, source epoch and non-regression before calling.
        CAS prevents competing checkpoints from silently overwriting one another;
        it cannot infer the semantic ordering of opaque cursor strings.
        """
        if self.actor.role not in (Role.OWNER, Role.MAINTAINER):
            raise DomainError(
                "permission_denied", "Only trusted transport identities may checkpoint"
            )
        _nonempty(stream_id, "stream_id")
        _nonempty(cursor, "cursor")
        if (
            isinstance(expected_cursor_revision, bool)
            or not isinstance(expected_cursor_revision, int)
            or expected_cursor_revision < 0
        ):
            raise DomainError(
                "invalid_input", "Expected cursor revision must be a nonnegative integer"
            )
        if not isinstance(reason, str) or not re.fullmatch(r"[a-z][a-z0-9_]{0,63}", reason):
            raise DomainError(
                "invalid_input", "Checkpoint reason must be a short machine reason code"
            )
        self.conn.execute(
            "INSERT INTO ga_cursors (group_id, stream_id) VALUES (%s, %s) ON CONFLICT DO NOTHING",
            (self.group_id, stream_id),
        )
        current = self.conn.execute(
            "SELECT * FROM ga_cursors WHERE group_id = %s AND stream_id = %s FOR UPDATE",
            (self.group_id, stream_id),
        ).fetchone()
        assert current is not None
        if current["cursor"] == cursor:
            return EventCursor.model_validate(current)
        self._revision(current, expected_cursor_revision)
        updated = self.conn.execute(
            "UPDATE ga_cursors SET cursor = %s, revision = revision + 1 "
            "WHERE group_id = %s AND stream_id = %s RETURNING *",
            (cursor, self.group_id, stream_id),
        ).fetchone()
        assert updated is not None
        self._audit(
            "cursor_advanced",
            "cursor:" + stream_id,
            reason=reason,
            previous_revision=current["revision"],
            revision=updated["revision"],
        )
        return EventCursor.model_validate(updated)

    @contextmanager
    def context_event_reads(self) -> Iterator[None]:
        """Deduplicate raw rows only inside a memory-locked context read phase.

        Call after memory preparation. Never use for the later decision/write
        transaction. A bulk exit check rejects same-transaction source changes;
        cached rows and actor bindings are discarded before context can escape.
        Quote/echo sanitization and fingerprints are deliberately not cached.
        """
        if self._context_event_rows is not None:
            raise DomainError("invalid_transition", "Context event reads cannot be nested")
        self.conn.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended(%s,0))", ("memory:" + self.group_id,)
        )
        self._context_event_rows = {}
        self._context_event_actor = self.actor
        try:
            yield
            if self.actor != self._context_event_actor:
                raise DomainError("scope_violation", "Context event actor changed")
            if self._context_event_rows:
                rows = self.conn.execute(
                    "SELECT id,body,memory_state FROM ga_events WHERE group_id=%s AND id=ANY(%s)",
                    (self.group_id, list(self._context_event_rows)),
                ).fetchall()
                current = {
                    row["id"]: {"body": row["body"], "memory_state": row["memory_state"]}
                    for row in rows
                }
                if current != self._context_event_rows:
                    raise DomainError("memory_changed", "A source changed during context reads")
        finally:
            self._context_event_rows = None
            self._context_event_actor = None

    def get_event(self, event_id: str) -> GroupEvent:
        cache = self._context_event_rows
        if cache is not None and self.actor != self._context_event_actor:
            raise DomainError("scope_violation", "Context event actor changed")
        row = cache.get(event_id) if cache is not None else None
        if row is None:
            row = self.conn.execute(
                "SELECT body,memory_state FROM ga_events WHERE group_id = %s AND id = %s",
                (self.group_id, event_id),
            ).fetchone()
            if row is not None and cache is not None and len(cache) < 1024:
                cache[event_id] = row
        if row is None:
            raise DomainError("not_found", "Event was not found in this group")
        event = GroupEvent.model_validate(row["body"])
        if row["memory_state"] != "active":
            return event.model_copy(
                update={
                    "text": "",
                    "attachments": [],
                    "payload_ref": None,
                    "content_state": row["memory_state"],
                }
            )
        return self._sanitize_bot_echo(self._sanitize_quote(event))

    def event_fingerprint(self, event_id: str) -> str:
        import json

        canonical = self.get_event(event_id).model_dump(mode="json")
        return hashlib.sha256(
            json.dumps(canonical, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode(
                "utf-8"
            )
        ).hexdigest()

    def _validate_work_sources(
        self, fingerprints: object, retained: object, input_versions: list[str] | None = None
    ) -> None:
        from groupagent.memory.common import source_rows, topic_row

        if (
            not isinstance(fingerprints, dict)
            or len(fingerprints) > 500
            or not isinstance(retained, list)
            or not all(isinstance(item, str) for item in retained)
            or not set(retained).issubset(fingerprints)
        ):
            raise DomainError("scope_violation", "Work source scope is invalid")
        for identifier, fingerprint in fingerprints.items():
            if (
                not isinstance(identifier, str)
                or not isinstance(fingerprint, str)
                or not re.fullmatch(r"[a-f0-9]{64}", fingerprint)
            ):
                raise DomainError(
                    "invalid_input",
                    "Source fingerprints must contain string IDs and SHA-256 digests",
                )
            source = self.get_event(identifier)
            retained_task = False
            if source.content_state == "expired":
                if identifier not in retained:
                    raise DomainError(
                        "source_unavailable",
                        "Expired source is not authorized as retained fact evidence",
                    )
                candidates = self.conn.execute(
                    "SELECT * FROM ga_memory_facts WHERE group_id=%s AND state='active' AND source_event_ids ? %s",
                    (self.group_id, identifier),
                ).fetchall()
                supported = False
                for fact in candidates:
                    try:
                        topic_row(self, fact["topic_id"])
                        source_rows(
                            self,
                            fact["source_event_ids"],
                            allow_expired=True,
                            require_complete=True,
                        )
                        supported = True
                        break
                    except DomainError:
                        continue
                if not supported:
                    retained_task = (
                        self.conn.execute(
                            "SELECT 1 FROM ga_team_tasks WHERE group_id=%s AND content_state='active' AND (source_event_id=%s OR source_event_ids ? %s) LIMIT 1",
                            (self.group_id, identifier, identifier),
                        ).fetchone()
                        is not None
                    )
                    supported = retained_task
                if not supported and input_versions:
                    catalog = self.conn.execute(
                        "SELECT to_regclass('ga_artifact_catalog') AS catalog"
                    ).fetchone()
                    if catalog and catalog["catalog"] is not None:
                        retained_task = (
                            self.conn.execute(
                                "SELECT 1 FROM ga_artifact_catalog WHERE group_id=%s AND version_id=ANY(%s) "
                                "AND %s=ANY(source_event_ids) AND available AND validation->'passed'='true'::jsonb "
                                "AND (expires_at IS NULL OR expires_at>clock_timestamp()) AND (shared OR created_by=%s OR %s) LIMIT 1",
                                (
                                    self.group_id,
                                    input_versions,
                                    identifier,
                                    self.actor.member_id,
                                    self.actor.role == Role.OWNER,
                                ),
                            ).fetchone()
                            is not None
                        )
                        supported = retained_task
                if not supported:
                    raise DomainError(
                        "source_unavailable",
                        "No retained fact, human task or readable validated input artifact supports this source",
                    )
            elif source.content_state != "active":
                raise DomainError(
                    "source_unavailable", "Forgotten or recalled source cannot enter a work scope"
                )
            if not retained_task:
                source_rows(self, [identifier], allow_expired=identifier in retained)
            if self.event_fingerprint(identifier) != fingerprint:
                raise DomainError(
                    "version_conflict", "A canonical source changed before work admission"
                )

    def _sanitize_quote(self, event: GroupEvent) -> GroupEvent:
        if event.kind != EventKind.QUOTE or not event.reply_to:
            return event
        parts = self.conn.execute("SELECT to_regclass('ga_delivery_parts') AS parts").fetchone()
        part_query = (
            " UNION ALL SELECT substring(d.reason from 8) FROM ancestry a JOIN ga_delivery_parts p ON p.platform_message_id=a.platform_id JOIN ga_deliveries d ON d.group_id=p.group_id AND d.id=p.intent_id WHERE p.group_id=%s AND p.state='accepted' AND d.reason IN ('source_forgotten','source_recalled','source_expired')"
            if parts and parts["parts"]
            else ""
        )
        params: tuple[str, ...] = (
            self.group_id,
            event.reply_to,
            self.group_id,
            self.group_id,
            event.reply_to,
            self.group_id,
        )
        if part_query:
            params = (*params, self.group_id)
        removed = self.conn.execute(
            "WITH RECURSIVE ancestry AS ("
            "SELECT id,body->>'reply_to' AS parent_id,memory_state,body->>'platform_message_id' AS platform_id FROM ga_events WHERE group_id=%s AND id=%s "
            "UNION SELECT e.id,e.body->>'reply_to',e.memory_state,e.body->>'platform_message_id' FROM ga_events e JOIN ancestry a "
            "ON e.id=a.parent_id WHERE e.group_id=%s), unavailable AS ("
            "SELECT memory_state AS reason FROM ancestry WHERE memory_state<>'active' "
            "UNION ALL SELECT reason FROM ga_source_tombstones WHERE group_id=%s "
            "AND (event_id=%s OR event_id IN (SELECT parent_id FROM ancestry)) "
            "UNION ALL SELECT substring(d.reason from 8) FROM ancestry a JOIN ga_deliveries d ON d.platform_message_id=a.platform_id WHERE d.group_id=%s AND d.state='accepted' AND d.reason IN ('source_forgotten','source_recalled','source_expired')"
            + part_query
            + ") SELECT reason FROM unavailable ORDER BY CASE reason WHEN 'forgotten' THEN 0 WHEN 'recalled' THEN 1 ELSE 2 END LIMIT 1",
            params,
        ).fetchone()
        if removed is None:
            return event
        update: dict[str, Any] = {
            "attachments": [
                {
                    "kind": "quote",
                    "source_event_id": event.reply_to,
                    "text": None,
                    "state": "recalled"
                    if removed["reason"] in {"forgotten", "recalled"}
                    else "missing",
                    "media_availability": "unavailable",
                    "privacy_redacted": True,
                    "reason": removed["reason"],
                }
            ]
        }
        if event.trusted_actor_origin == "authenticated_audio_transcription":
            update.update({"text": "", "payload_ref": None, "content_state": removed["reason"]})
        return event.model_copy(update=update)

    def _validate_transcription_origin(self, event: GroupEvent) -> None:
        if event.trusted_actor_origin != "authenticated_audio_transcription":
            return
        if event.kind != EventKind.QUOTE or not event.reply_to:
            raise DomainError(
                "permission_denied", "Audio transcription needs its original audio event"
            )
        parent = self.get_event(event.reply_to)
        if (
            parent.kind != EventKind.AUDIO
            or parent.actor_id != event.actor_id
            or parent.trusted_actor_origin
            not in {"verified_channel_metadata", "agentwechat_authenticated_message_metadata"}
        ):
            raise DomainError(
                "permission_denied",
                "Transcription authority must come from the same sender's verified audio message",
            )
        if parent.platform_message_id and self.is_bot_message(parent.platform_message_id):
            raise DomainError(
                "permission_denied",
                "An accepted bot audio echo is not a new human voice instruction",
            )

    def _sanitize_bot_echo(self, event: GroupEvent) -> GroupEvent:
        if not event.platform_message_id or event.kind in (
            EventKind.SYSTEM,
            EventKind.MEMBERSHIP,
            EventKind.RECALL,
        ):
            return event
        rows = self.conn.execute(
            "SELECT id,reason FROM ga_deliveries WHERE group_id=%s AND platform_message_id=%s AND state='accepted'",
            (self.group_id, event.platform_message_id),
        ).fetchall()
        parts = self.conn.execute("SELECT to_regclass('ga_delivery_parts') AS parts").fetchone()
        if parts and parts["parts"]:
            rows.extend(
                self.conn.execute(
                    "SELECT d.id,d.reason FROM ga_deliveries d JOIN ga_delivery_parts p ON p.group_id=d.group_id AND p.intent_id=d.id WHERE p.group_id=%s AND p.platform_message_id=%s AND p.state='accepted'",
                    (self.group_id, event.platform_message_id),
                ).fetchall()
            )
        reasons = {row["reason"] for row in rows}
        reason = next(
            (
                state
                for state in ("forgotten", "recalled", "expired")
                if "source_" + state in reasons
            ),
            None,
        )
        if reason is not None:
            rank = {"active": 0, "expired": 1, "recalled": 2, "forgotten": 3}
            if rank[event.content_state] > rank[reason]:
                reason = event.content_state
            return event.model_copy(
                update={"text": "", "attachments": [], "payload_ref": None, "content_state": reason}
            )
        return event

    def get_decision(self, event_id: str) -> dict[str, JsonValue] | None:
        """Lock the source event while its decision and business writes are applied."""
        row = self.conn.execute(
            "SELECT e.actor_id, d.body FROM ga_events e LEFT JOIN ga_decisions d "
            "ON d.group_id = e.group_id AND d.event_id = e.id "
            "WHERE e.group_id = %s AND e.id = %s FOR UPDATE OF e",
            (self.group_id, event_id),
        ).fetchone()
        if row is None:
            raise DomainError("not_found", "Event was not found in this group")
        if row["actor_id"] != self.actor.member_id and self.actor.role != Role.OWNER:
            raise DomainError("permission_denied", "Cannot decide another member's event")
        # A separate read after acquiring the event lock sees a competing
        # transaction's decision under PostgreSQL READ COMMITTED semantics.
        decision = self.conn.execute(
            "SELECT body FROM ga_decisions WHERE group_id = %s AND event_id = %s",
            (self.group_id, event_id),
        ).fetchone()
        return dict(decision["body"]) if decision else None

    def record_decision(
        self, event_id: str, decision: dict[str, JsonValue]
    ) -> dict[str, JsonValue]:
        existing = self.get_decision(event_id)
        if existing is not None:
            return existing
        event = self.get_event(event_id)
        if event.content_state != "active" and decision.get("kind") not in {"ignore", "observe"}:
            raise DomainError("source_unavailable", "Removed source cannot authorize a decision")
        if decision.get("group_id", self.group_id) != self.group_id:
            raise DomainError("scope_violation", "Decision belongs to another group")
        if decision.get("actor_id", event.actor_id) != event.actor_id:
            raise DomainError("permission_denied", "Decision cannot replace event identity")
        if decision.get("kind") not in (
            "ignore",
            "observe",
            "reply",
            "command",
            "clarify",
            "start_work",
            "update_work",
            "control_work",
        ):
            raise DomainError("invalid_input", "Decision kind is not recognized")
        self.conn.execute(
            "INSERT INTO ga_decisions (group_id, event_id, actor_id, policy_revision, body) "
            "VALUES (%s, %s, %s, %s, %s)",
            (
                self.group_id,
                event_id,
                self.actor.member_id,
                self.actor.policy_revision,
                Jsonb(decision),
            ),
        )
        self._audit("event_decided", event_id, kind=decision["kind"])
        return dict(decision)

    def list_pending_events(self, limit: int = 100) -> list[GroupEvent]:
        rows = self.conn.execute(
            "SELECT e.body FROM ga_events e WHERE e.group_id = %s AND e.memory_state='active' AND NOT EXISTS "
            "(SELECT 1 FROM ga_decisions d WHERE d.group_id = e.group_id AND d.event_id = e.id "
            "AND d.body->>'agent_pending_response' IS DISTINCT FROM 'true') "
            "ORDER BY e.sequence LIMIT %s",
            (self.group_id, min(max(limit, 1), 1000)),
        ).fetchall()
        return [GroupEvent.model_validate(row["body"]) for row in rows]

    def ingest_event(
        self,
        event: GroupEvent,
        stream_id: str,
        cursor: str,
        expected_cursor_revision: int,
    ) -> IngestResult:
        if event.group_id != self.group_id:
            raise DomainError("scope_violation", "Event belongs to another group")
        if event.actor_id != self.actor.member_id and self.actor.role != Role.OWNER:
            raise DomainError("permission_denied", "Cannot ingest another member's identity")
        if event.content_state != "active":
            raise DomainError("invalid_input", "Only the server may assign content deletion state")
        self.conn.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended(%s,0))", ("memory:" + self.group_id,)
        )
        self._validate_transcription_origin(event)
        event = self._sanitize_bot_echo(self._sanitize_quote(event))
        _nonempty(stream_id, "stream_id")
        _nonempty(cursor, "cursor")
        self.conn.execute(
            "INSERT INTO ga_cursors (group_id, stream_id) VALUES (%s, %s) ON CONFLICT DO NOTHING",
            (self.group_id, stream_id),
        )
        current = self.conn.execute(
            "SELECT * FROM ga_cursors WHERE group_id = %s AND stream_id = %s FOR UPDATE",
            (self.group_id, stream_id),
        ).fetchone()
        assert current is not None
        tombstone = self.conn.execute(
            "SELECT reason,event_id FROM ga_source_tombstones WHERE group_id=%s AND "
            "(source_key=%s OR event_id=%s OR (%s AND platform_message_id IS NOT NULL "
            "AND platform_message_id=%s AND channel_account_id IS NOT DISTINCT FROM %s)) LIMIT 1",
            (
                self.group_id,
                event.source_key,
                event.id,
                event.kind not in (EventKind.RECALL, EventKind.SYSTEM, EventKind.MEMBERSHIP),
                event.platform_message_id,
                event.channel_account_id,
            ),
        ).fetchone()
        if tombstone is None and event.content_state != "active":
            self.conn.execute(
                "INSERT INTO ga_source_tombstones(group_id,source_key,event_id,platform_message_id,channel_account_id,reason) VALUES (%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING",
                (
                    self.group_id,
                    event.source_key,
                    event.id,
                    event.platform_message_id,
                    event.channel_account_id,
                    event.content_state,
                ),
            )
            tombstone = {"reason": event.content_state, "event_id": event.id}
        if tombstone:
            redacted = event.model_copy(
                update={
                    "text": "",
                    "attachments": [],
                    "payload_ref": None,
                    "content_state": tombstone["reason"],
                }
            )
            self.conn.execute(
                "INSERT INTO ga_events(id,group_id,source_key,actor_id,kind,body,occurred_at,memory_state) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING",
                (
                    event.id,
                    self.group_id,
                    event.source_key,
                    event.actor_id,
                    event.kind.value,
                    Jsonb(redacted.model_dump(mode="json")),
                    event.occurred_at,
                    tombstone["reason"],
                ),
            )
            original_row = self.conn.execute(
                "SELECT id,actor_id,body,memory_state FROM ga_events WHERE group_id=%s AND source_key=%s",
                (self.group_id, event.source_key),
            ).fetchone()
            if original_row is None or original_row["actor_id"] != event.actor_id:
                raise DomainError(
                    "idempotency_conflict", "Removed event identity does not match its replay"
                )
            if original_row["memory_state"] == "active":
                stored = GroupEvent.model_validate(original_row["body"]).model_copy(
                    update={
                        "text": "",
                        "attachments": [],
                        "payload_ref": None,
                        "content_state": tombstone["reason"],
                    }
                )
                self.conn.execute(
                    "UPDATE ga_events SET body=%s,memory_state=%s WHERE group_id=%s AND id=%s",
                    (
                        Jsonb(stored.model_dump(mode="json")),
                        tombstone["reason"],
                        self.group_id,
                        stored.id,
                    ),
                )
            canonical = self.get_event(original_row["id"])
            return IngestResult(
                event=canonical, inserted=False, cursor_revision=current["revision"]
            )
        inserted = self.conn.execute(
            "INSERT INTO ga_events (id, group_id, source_key, actor_id, kind, body,occurred_at) "
            "VALUES (%s, %s, %s, %s, %s, %s,%s) ON CONFLICT DO NOTHING RETURNING id",
            (
                event.id,
                self.group_id,
                event.source_key,
                event.actor_id,
                event.kind.value,
                Jsonb(event.model_dump(mode="json")),
                event.occurred_at,
            ),
        ).fetchone()
        if inserted is None:
            old = self.conn.execute(
                "SELECT body, group_id FROM ga_events WHERE source_key = %s", (event.source_key,)
            ).fetchone()
            if old is None:
                raise DomainError("idempotency_conflict", "Event ID belongs to a different source")
            if old["group_id"] != self.group_id:
                raise DomainError("scope_violation", "Source key is already bound to another group")
            original = self._sanitize_quote(GroupEvent.model_validate(old["body"]))
            excluded = {"id", "received_at", "policy_revision"}
            if original.model_dump(exclude=excluded) != event.model_dump(exclude=excluded):
                raise DomainError(
                    "idempotency_conflict", "Source key was reused for different data"
                )
            # A duplicate, even on another stream, can never advance/regress a cursor.
            return IngestResult(event=original, inserted=False, cursor_revision=current["revision"])
        self._revision(current, expected_cursor_revision)
        new_cursor = self.conn.execute(
            "UPDATE ga_cursors SET cursor = %s, revision = revision + 1 "
            "WHERE group_id = %s AND stream_id = %s RETURNING revision",
            (cursor, self.group_id, stream_id),
        ).fetchone()
        assert new_cursor is not None
        self._audit("event_ingested", event.id, source_key=event.source_key)
        if event.kind not in (EventKind.SYSTEM, EventKind.RECALL, EventKind.MEMBERSHIP):
            self.conn.execute(
                "INSERT INTO ga_members(group_id,member_id) VALUES (%s,%s) ON CONFLICT DO NOTHING",
                (self.group_id, event.actor_id),
            )
        return IngestResult(event=event, inserted=True, cursor_revision=new_cursor["revision"])

    def _work_row(self, work_id: str, *, lock: bool = True) -> Row:
        row = self.conn.execute(
            "SELECT * FROM ga_work_orders "
            "WHERE group_id = %s AND id = %s" + (" FOR UPDATE" if lock else ""),
            (self.group_id, work_id),
        ).fetchone()
        if row is None:
            raise DomainError("not_found", "Work was not found in this group")
        # Read the clock after obtaining a potentially contended row lock.
        clock = self.conn.execute("SELECT clock_timestamp() AS now").fetchone()
        assert clock is not None
        row["db_now"] = clock["now"]
        return row

    def _check_work_source(self, row: Row) -> None:
        self.assert_task_sources(
            row["scope"].get("source_task_revisions", {}), require_current=False
        )
        if row["scope"].get("privacy_redacted"):
            raise DomainError(
                "source_unavailable", "Privacy-redacted work requires a new authorized request"
            )
        source = self.get_event(row["request_event_id"])
        if source.content_state in {"forgotten", "recalled"} or any(
            attachment.get("privacy_redacted")
            and attachment.get("reason") in {"forgotten", "recalled"}
            for attachment in source.attachments
        ):
            raise DomainError(
                "source_unavailable", "Removed evidence cannot authorize further execution"
            )

    def get_work(self, work_id: str) -> WorkOrder:
        return _work(self._work_row(work_id, lock=False))

    def list_work(
        self,
        *,
        state: WorkState | None = None,
        limit: int = 100,
        active_first: bool = False,
        newest_first: bool = False,
    ) -> list[WorkOrder]:
        state_value = state.value if state is not None else None
        order = (
            "CASE WHEN state IN ('completed', 'cancelled', 'failed') THEN 1 ELSE 0 END, "
            if active_first
            else ""
        )
        order += "created_at DESC, id DESC" if newest_first else "created_at, id"
        rows = self.conn.execute(
            "SELECT * FROM ga_work_orders WHERE group_id = %s "
            "AND (%s::text IS NULL OR state = %s) ORDER BY " + order + " LIMIT %s",
            (self.group_id, state_value, state_value, min(max(limit, 1), 1000)),
        ).fetchall()
        return [_work(row) for row in rows]

    def _scope(self, scope: dict[str, JsonValue]) -> None:
        metadata_versions = scope.get("context_artifact_versions", [])
        if (
            not isinstance(metadata_versions, list)
            or len(metadata_versions) > 100
            or any(not isinstance(value, str) for value in metadata_versions)
        ):
            raise DomainError(
                "scope_violation", "Context artifact evidence must be a bounded version list"
            )
        if "allowed_group_ids" in scope and scope["allowed_group_ids"] != [self.group_id]:
            raise DomainError("scope_violation", "Work is restricted to its origin group")
        if scope.get("external_account_changes", False):
            raise DomainError("permission_denied", "External account changes are not supported")
        if scope.get("delivery_target", "origin_group") != "origin_group":
            raise DomainError("scope_violation", "Delivery is restricted to the origin group")

    @staticmethod
    def _artifact_evidence_versions(scope: dict[str, Any], versions: list[str] | None) -> list[str]:
        metadata = scope.get("context_artifact_versions", [])
        if not isinstance(metadata, list) or any(not isinstance(value, str) for value in metadata):
            raise DomainError("scope_violation", "Invalid context artifact references")
        return list(versions or []) + [str(value) for value in metadata]

    def create_work(
        self,
        goal: str,
        request_event_id: str,
        *,
        acceptance_criteria: list[str] | None = None,
        input_versions: list[str] | None = None,
        scope: dict[str, JsonValue] | None = None,
        budget: dict[str, JsonValue] | None = None,
        required_checks: list[str] | None = None,
        state: WorkState = WorkState.QUEUED,
    ) -> WorkOrder:
        self._writer()
        _nonempty(goal, "goal")
        self.conn.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended(%s,0))", ("memory:" + self.group_id,)
        )
        self.conn.execute(
            "SELECT id FROM ga_events WHERE group_id = %s AND id = %s FOR UPDATE",
            (self.group_id, request_event_id),
        )
        event = self.get_event(request_event_id)
        if event.content_state != "active" or any(
            attachment.get("privacy_redacted")
            and attachment.get("reason") in {"forgotten", "recalled"}
            for attachment in event.attachments
        ):
            raise DomainError("source_unavailable", "Removed source cannot create work")
        if event.kind == EventKind.UNSUPPORTED_VIDEO:
            raise DomainError("unsupported_video", "Video events cannot create execution work")
        if event.actor_id != self.actor.member_id and self.actor.role != Role.OWNER:
            raise DomainError("permission_denied", "Cannot claim another member's request")
        if state not in (WorkState.QUEUED, WorkState.WAITING_INPUT):
            raise DomainError("invalid_transition", "New work must be queued or waiting for input")
        scope = self._task_source_scope(scope or {}, input_versions or [])
        self._scope(scope)
        self.assert_task_sources(scope.get("source_task_revisions", {}))
        if "source_fingerprints" in scope:
            self._validate_work_sources(
                scope["source_fingerprints"],
                scope.get("retained_fact_source_ids", []),
                self._artifact_evidence_versions(scope, input_versions),
            )
            fingerprints = scope["source_fingerprints"]
            assert isinstance(fingerprints, dict)
            scope = {**scope, "source_event_ids": list(fingerprints)}
        checks = required_checks if required_checks is not None else ["result"]
        if not checks or any(not isinstance(check, str) or not check.strip() for check in checks):
            raise DomainError("invalid_input", "At least one named validation check is required")
        row = self.conn.execute(
            "INSERT INTO ga_work_orders (id, group_id, requester_id, request_event_id, goal, "
            "acceptance_criteria, input_versions, scope, budget, required_checks, state, "
            "policy_revision) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) "
            "ON CONFLICT (group_id, request_event_id) DO NOTHING RETURNING *",
            (
                _id(),
                self.group_id,
                self.actor.member_id,
                request_event_id,
                goal,
                Jsonb(acceptance_criteria or []),
                Jsonb(input_versions or []),
                Jsonb(scope),
                Jsonb(budget or {}),
                Jsonb(list(dict.fromkeys(checks))),
                state.value,
                self.actor.policy_revision,
            ),
        ).fetchone()
        if row is None:
            existing = self.conn.execute(
                "SELECT * FROM ga_work_orders WHERE group_id = %s AND request_event_id = %s",
                (self.group_id, request_event_id),
            ).fetchone()
            assert existing is not None
            self._controller(existing)
            return _work(existing)
        work = _work(row)
        self._save_goal_revision(work)
        self._audit("work_created", work.id, request_event_id=request_event_id)
        return work

    def _save_goal_revision(self, work: WorkOrder) -> None:
        self.conn.execute(
            "INSERT INTO ga_work_revisions (group_id, work_id, goal_revision, body, changed_by) "
            "VALUES (%s, %s, %s, %s, %s)",
            (
                self.group_id,
                work.id,
                work.goal_revision,
                Jsonb(work.model_dump(mode="json")),
                self.actor.member_id,
            ),
        )

    def _revoke(self, work_id: str, *, state: str = "revoked") -> None:
        self.conn.execute(
            "UPDATE ga_work_orders SET lease_owner = NULL, lease_expires_at = NULL "
            "WHERE group_id = %s AND id = %s",
            (self.group_id, work_id),
        )
        self.conn.execute(
            "UPDATE ga_execution_attempts SET state = %s "
            "WHERE group_id = %s AND work_id = %s AND state = 'active'",
            (state, self.group_id, work_id),
        )

    def revise_work(
        self,
        work_id: str,
        goal: str,
        *,
        expected_revision: int,
        acceptance_criteria: list[str] | None = None,
        input_versions: list[str] | None = None,
        source_fingerprints: dict[str, str] | None = None,
        retained_fact_source_ids: list[str] | None = None,
        output_requirements: list[dict[str, JsonValue]] | None = None,
        required_checks: list[str] | None = None,
        delivery_artifacts: bool | None = None,
        speech_requested: bool | None = None,
        source_task_revisions: dict[str, int] | None = None,
        context_artifact_versions: list[str] | None = None,
    ) -> WorkOrder:
        if (
            source_fingerprints is not None
            or retained_fact_source_ids is not None
            or source_task_revisions is not None
        ):
            self.conn.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended(%s,0))", ("memory:" + self.group_id,)
            )
        row = self._work_row(work_id)
        self._controller(row)
        self._revision(row, expected_revision)
        _nonempty(goal, "goal")
        new_scope = dict(row["scope"])
        if context_artifact_versions is not None:
            new_scope["context_artifact_versions"] = context_artifact_versions
            self._scope(new_scope)
        if source_task_revisions is not None:
            self.assert_task_sources(source_task_revisions)
            new_scope["source_task_revisions"] = source_task_revisions
            new_scope = self._task_source_scope(
                new_scope, input_versions if input_versions is not None else row["input_versions"]
            )
        if output_requirements is not None:
            if (
                not isinstance(output_requirements, list)
                or len(output_requirements) > 20
                or any(not isinstance(item, dict) for item in output_requirements)
            ):
                raise DomainError(
                    "invalid_input",
                    "Output requirements must be a bounded list of validated objects",
                )
            new_scope["outputs"] = output_requirements
        for key, value in (
            ("delivery_artifacts", delivery_artifacts),
            ("speech_requested", speech_requested),
        ):
            if value is not None:
                if type(value) is not bool:
                    raise DomainError("invalid_input", "Execution output flags must be booleans")
                new_scope[key] = value
        checks = row["required_checks"] if required_checks is None else required_checks
        if not checks or any(not isinstance(check, str) or not check.strip() for check in checks):
            raise DomainError("invalid_input", "At least one named validation check is required")
        if source_fingerprints is not None or retained_fact_source_ids is not None:
            fingerprints = (
                source_fingerprints
                if source_fingerprints is not None
                else new_scope.get("source_fingerprints", {})
            )
            retained = (
                retained_fact_source_ids
                if retained_fact_source_ids is not None
                else [
                    identifier
                    for identifier in new_scope.get("retained_fact_source_ids", [])
                    if identifier in fingerprints
                ]
            )
            self._validate_work_sources(
                fingerprints,
                retained,
                self._artifact_evidence_versions(
                    new_scope,
                    input_versions if input_versions is not None else row["input_versions"],
                ),
            )
            new_scope.update(
                {
                    "source_fingerprints": fingerprints,
                    "source_event_ids": list(fingerprints),
                    "retained_fact_source_ids": retained,
                }
            )
        if row["state"] in (WorkState.COMPLETED, WorkState.CANCELLED, WorkState.FAILED):
            raise DomainError("invalid_transition", "Closed work cannot have its goal rewritten")
        unresolved = self.conn.execute(
            "SELECT 1 FROM ga_deliveries WHERE group_id = %s AND work_id = %s "
            "AND state IN ('dispatching', 'unknown') LIMIT 1",
            (self.group_id, work_id),
        ).fetchone()
        target = (
            WorkState.PAUSED
            if unresolved
            else (
                WorkState.QUEUED
                if row["state"] in (WorkState.RUNNING, WorkState.VALIDATING, WorkState.DELIVERING)
                else WorkState(row["state"])
            )
        )
        updated = self.conn.execute(
            "UPDATE ga_work_orders SET goal = %s, acceptance_criteria = %s, input_versions = %s, "
            "goal_revision = goal_revision + 1, revision = revision + 1, policy_revision = %s, "
            "state = %s, pause_reason = %s, scope = %s, required_checks = %s, updated_at = clock_timestamp() "
            "WHERE group_id = %s AND id = %s RETURNING *",
            (
                goal,
                Jsonb(
                    acceptance_criteria
                    if acceptance_criteria is not None
                    else row["acceptance_criteria"]
                ),
                Jsonb(input_versions if input_versions is not None else row["input_versions"]),
                self.actor.policy_revision,
                target.value,
                "delivery_uncertain" if unresolved else row["pause_reason"],
                Jsonb(new_scope),
                Jsonb(list(dict.fromkeys(checks))),
                self.group_id,
                work_id,
            ),
        ).fetchone()
        assert updated is not None
        self._revoke(work_id)
        self.conn.execute(
            "UPDATE ga_deliveries SET state = 'rejected', reason = 'superseded_goal', "
            "revision = revision + 1, updated_at = clock_timestamp() "
            "WHERE group_id = %s AND work_id = %s AND state = 'pending'",
            (self.group_id, work_id),
        )
        work = _work(updated)
        self._save_goal_revision(work)
        self._audit("work_revised", work_id, goal_revision=work.goal_revision)
        return work

    def _assert_lease(self, row: Row, lease: ExecutionLease) -> None:
        if (
            lease.group_id != self.group_id
            or lease.work_id != row["id"]
            or lease.worker_id != row["lease_owner"]
            or lease.fencing_token != row["fencing_token"]
            or lease.goal_revision != row["goal_revision"]
            or row["lease_expires_at"] is None
            or row["lease_expires_at"] <= row["db_now"]
            or row["state"] not in (WorkState.RUNNING, WorkState.VALIDATING, WorkState.DELIVERING)
        ):
            raise DomainError("stale_lease", "Execution lease is no longer valid")
        attempt = self.conn.execute(
            "SELECT 1 FROM ga_execution_attempts WHERE group_id = %s AND work_id = %s "
            "AND id = %s AND fencing_token = %s AND state = 'active'",
            (self.group_id, row["id"], lease.attempt_id, lease.fencing_token),
        ).fetchone()
        if attempt is None:
            raise DomainError("stale_lease", "Execution attempt is no longer active")
        if row["policy_revision"] != self.actor.policy_revision:
            raise DomainError(
                "policy_changed", "Work must be reauthorized under the current policy"
            )

    def assert_lease(self, lease: ExecutionLease) -> None:
        row = self._work_row(lease.work_id)
        self._controller(row)
        self._assert_lease(row, lease)

    def claim_work(
        self,
        work_id: str,
        worker_id: str,
        ttl_seconds: int = 30,
        *,
        recovery_evidence_ref: str | None = None,
    ) -> ExecutionLease:
        _nonempty(worker_id, "worker_id")
        _ttl(ttl_seconds)
        row = self._work_row(work_id)
        self._controller(row)
        self._check_work_source(row)
        if row["policy_revision"] != self.actor.policy_revision:
            raise DomainError("policy_changed", "Work must be reauthorized before execution")
        if row["state"] not in (WorkState.QUEUED, WorkState.RUNNING):
            raise DomainError("invalid_transition", "Work is not ready for execution")
        if row["lease_expires_at"] and row["lease_expires_at"] > row["db_now"]:
            raise DomainError("lease_busy", "Another execution lease is active", retryable=True)
        if row["state"] == WorkState.RUNNING and not recovery_evidence_ref:
            raise DomainError("reconciliation_required", "Expired work requires recovery evidence")
        return self._claim_execution_lease(
            row, worker_id, ttl_seconds, WorkState.RUNNING, recovery_evidence_ref
        )

    def _claim_execution_lease(
        self,
        row: Row,
        worker_id: str,
        ttl_seconds: int,
        state: WorkState,
        recovery_evidence_ref: str | None,
    ) -> ExecutionLease:
        work_id = row["id"]
        self._revoke(work_id, state="expired")
        updated = self.conn.execute(
            "UPDATE ga_work_orders SET lease_owner = %s, "
            "lease_expires_at = clock_timestamp() + %s * interval '1 second', "
            "fencing_token = fencing_token + 1, attempt_no = attempt_no + 1, "
            "state = %s, pause_reason = NULL, revision = revision + 1, "
            "updated_at = clock_timestamp() WHERE group_id = %s AND id = %s RETURNING *",
            (worker_id, ttl_seconds, state.value, self.group_id, work_id),
        ).fetchone()
        assert updated is not None
        attempt_id = _id()
        self.conn.execute(
            "INSERT INTO ga_execution_attempts (id, group_id, work_id, attempt_no, worker_id, "
            "fencing_token, goal_revision, state, expires_at) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, 'active', %s)",
            (
                attempt_id,
                self.group_id,
                work_id,
                updated["attempt_no"],
                worker_id,
                updated["fencing_token"],
                updated["goal_revision"],
                updated["lease_expires_at"],
            ),
        )
        self._audit(
            "delivery_supervision_claimed" if state == WorkState.DELIVERING else "work_claimed",
            work_id,
            fencing_token=updated["fencing_token"],
            recovery_evidence_ref=recovery_evidence_ref,
        )
        return ExecutionLease(
            group_id=self.group_id,
            work_id=work_id,
            attempt_id=attempt_id,
            attempt_no=updated["attempt_no"],
            worker_id=worker_id,
            fencing_token=updated["fencing_token"],
            goal_revision=updated["goal_revision"],
            expires_at=updated["lease_expires_at"],
        )

    def claim_delivery_supervision(
        self,
        work_id: str,
        worker_id: str,
        *,
        expected_revision: int,
        checkpoint_attempt_id: str,
        ttl_seconds: int = 30,
    ) -> ExecutionLease:
        """Fence an expired finalizer; this grants no new execution/validation step."""
        _nonempty(worker_id, "worker_id")
        _ttl(ttl_seconds)
        row = self._work_row(work_id)
        self._controller(row)
        self._revision(row, expected_revision)
        self._check_work_source(row)
        if row["policy_revision"] != self.actor.policy_revision:
            raise DomainError("policy_changed", "Delivery supervision needs current policy")
        if (
            row["state"] != WorkState.DELIVERING
            or row["lease_expires_at"] is None
            or row["lease_expires_at"] > row["db_now"]
        ):
            raise DomainError("lease_active", "Delivery supervision needs an expired finalizer")
        candidate_attempt = self.conn.execute(
            "SELECT fencing_token FROM ga_execution_attempts "
            "WHERE group_id=%s AND work_id=%s AND id=%s AND goal_revision=%s",
            (self.group_id, work_id, checkpoint_attempt_id, row["goal_revision"]),
        ).fetchone()
        checks = self.conn.execute(
            "SELECT check_key,passed,fencing_token FROM ga_work_validations "
            "WHERE group_id=%s AND work_id=%s AND goal_revision=%s",
            (self.group_id, work_id, row["goal_revision"]),
        ).fetchall()
        passed = {
            check["check_key"]
            for check in checks
            if candidate_attempt
            and check["passed"]
            and check["fencing_token"] == candidate_attempt["fencing_token"]
        }
        if not passed or not set(row["required_checks"]).issubset(passed):
            raise DomainError("validation_failed", "Original candidate validation is incomplete")
        deliveries = self._work_delivery_rows(work_id, row["goal_revision"])
        bundle = self.conn.execute(
            "SELECT intent_id,position FROM ga_delivery_bundles "
            "WHERE group_id=%s AND bundle_id=%s ORDER BY position",
            (self.group_id, f"result:{work_id}:{row['goal_revision']}"),
        ).fetchall()
        if (
            not deliveries
            or [part["position"] for part in bundle] != list(range(len(deliveries)))
            or {part["intent_id"] for part in bundle} != {delivery["id"] for delivery in deliveries}
        ):
            raise DomainError("reconciliation_required", "Original delivery bundle is incomplete")
        for delivery in deliveries:
            state = projected_delivery_state(delivery["state"], delivery["parts"] or ())
            if state in {DeliveryState.UNKNOWN, DeliveryState.REJECTED} or (
                delivery["state"] == DeliveryState.DISPATCHING
                and projected_delivery_state(DeliveryState.UNKNOWN, delivery["parts"] or ())
                != DeliveryState.DISPATCHING
            ):
                raise DomainError("delivery_unknown", "Native delivery is not safe to supervise")
        return self._claim_execution_lease(
            row, worker_id, ttl_seconds, WorkState.DELIVERING, "runtime-checkpoint:" + work_id
        )

    def heartbeat_lease(
        self, lease: ExecutionLease, ttl_seconds: int = 30, *, checkpoint_ref: str | None = None
    ) -> ExecutionLease:
        _ttl(ttl_seconds)
        self.assert_lease(lease)
        row = self.conn.execute(
            "UPDATE ga_work_orders SET lease_expires_at = "
            "clock_timestamp() + %s * interval '1 second' "
            "WHERE group_id = %s AND id = %s RETURNING lease_expires_at",
            (ttl_seconds, self.group_id, lease.work_id),
        ).fetchone()
        assert row is not None
        self.conn.execute(
            "UPDATE ga_execution_attempts SET heartbeat_at = clock_timestamp(), expires_at = %s, "
            "checkpoint_ref = COALESCE(%s, checkpoint_ref) WHERE group_id = %s AND id = %s",
            (row["lease_expires_at"], checkpoint_ref, self.group_id, lease.attempt_id),
        )
        return lease.model_copy(update={"expires_at": row["lease_expires_at"]})

    def record_validation(
        self,
        work_id: str,
        lease: ExecutionLease,
        evidence_ref: str,
        *,
        check_key: str = "result",
        passed: bool = True,
    ) -> None:
        row = self._work_row(work_id)
        self._controller(row)
        self._assert_lease(row, lease)
        if row["state"] != WorkState.VALIDATING:
            raise DomainError("invalid_transition", "Validation requires validating work")
        _nonempty(evidence_ref, "evidence_ref")
        _nonempty(check_key, "check_key")
        self.conn.execute(
            "INSERT INTO ga_work_validations (group_id, work_id, goal_revision, check_key, "
            "passed, evidence_ref, fencing_token) VALUES (%s, %s, %s, %s, %s, %s, %s) "
            "ON CONFLICT (group_id, work_id, goal_revision, check_key) DO UPDATE SET "
            "passed = EXCLUDED.passed, evidence_ref = EXCLUDED.evidence_ref, "
            "fencing_token = EXCLUDED.fencing_token, created_at = clock_timestamp()",
            (
                self.group_id,
                work_id,
                lease.goal_revision,
                check_key,
                passed,
                evidence_ref,
                lease.fencing_token,
            ),
        )
        self._audit(
            "work_validated",
            work_id,
            check_key=check_key,
            passed=passed,
            evidence_ref=evidence_ref,
            goal_revision=lease.goal_revision,
        )

    def transition_work(
        self,
        work_id: str,
        state: WorkState,
        *,
        expected_revision: int,
        lease: ExecutionLease | None = None,
        reason: str | None = None,
    ) -> WorkOrder:
        row = self._work_row(work_id)
        self._controller(row)
        self._revision(row, expected_revision)
        previous = WorkState(row["state"])
        state = WorkState(state)
        check_transition(previous, state)
        if state == WorkState.RUNNING:
            raise DomainError("lease_required", "Running state is entered by claiming a lease")
        if lease is not None:
            self._assert_lease(row, lease)
        elif previous in (
            WorkState.RUNNING,
            WorkState.VALIDATING,
            WorkState.DELIVERING,
        ) and state not in (
            WorkState.PAUSED,
            WorkState.CANCELLED,
        ):
            raise DomainError("lease_required", "Execution state changes require a valid lease")
        if state == WorkState.QUEUED:
            self._check_work_source(row)
            needed_reason = {
                WorkState.FAILED: {"retry"},
                WorkState.RUNNING: {"recovery", "retry"},
                WorkState.WAITING_INPUT: {"input_received"},
                WorkState.WAITING_EXTERNAL: {"external_result"},
            }.get(previous)
            if needed_reason and reason not in needed_reason:
                raise DomainError("invalid_transition", "Resuming work needs an explicit reason")
            unresolved = self.conn.execute(
                "SELECT 1 FROM ga_deliveries WHERE group_id = %s AND work_id = %s "
                "AND state IN ('unknown', 'dispatching') LIMIT 1",
                (self.group_id, work_id),
            ).fetchone()
            if unresolved:
                raise DomainError("delivery_unknown", "Uncertain delivery must be reconciled first")
        if state == WorkState.DELIVERING:
            checks = self.conn.execute(
                "SELECT check_key, passed FROM ga_work_validations "
                "WHERE group_id = %s AND work_id = %s AND goal_revision = %s",
                (self.group_id, work_id, row["goal_revision"]),
            ).fetchall()
            passed = {check["check_key"] for check in checks if check["passed"]}
            if not set(row["required_checks"]).issubset(passed):
                raise DomainError("validation_failed", "Required validation evidence is missing")
        if state == WorkState.COMPLETED:
            deliveries = self.conn.execute(
                "SELECT state FROM ga_deliveries WHERE group_id = %s AND work_id = %s "
                "AND goal_revision = %s AND NOT (state = 'rejected' "
                "AND reason IS NOT DISTINCT FROM 'candidate_superseded_before_dispatch')",
                (self.group_id, work_id, row["goal_revision"]),
            ).fetchall()
            silent = (
                not deliveries
                and reason == "authorized_silence"
                and lease is not None
                and allows_silent_result(row["scope"])
                and self.conn.execute(
                    "SELECT 1 FROM ga_work_validations WHERE group_id=%s AND work_id=%s "
                    "AND goal_revision=%s AND check_key='silent_result' AND passed AND fencing_token=%s",
                    (self.group_id, work_id, row["goal_revision"], lease.fencing_token),
                ).fetchone()
                is not None
            )
            if not silent and (
                not deliveries
                or any(item["state"] != DeliveryState.ACCEPTED for item in deliveries)
            ):
                raise DomainError("delivery_incomplete", "Required delivery is not accepted")
        updated = self.conn.execute(
            "UPDATE ga_work_orders SET state = %s, revision = revision + 1, pause_reason = %s, "
            "updated_at = clock_timestamp() WHERE group_id = %s AND id = %s RETURNING *",
            (
                state.value,
                (reason or "user_request") if state == WorkState.PAUSED else None,
                self.group_id,
                work_id,
            ),
        ).fetchone()
        assert updated is not None
        if state not in (WorkState.VALIDATING, WorkState.DELIVERING):
            self._revoke(work_id, state="released" if state == WorkState.COMPLETED else "revoked")
        if state in (WorkState.CANCELLED, WorkState.FAILED):
            self.conn.execute(
                "UPDATE ga_deliveries SET state = 'rejected', reason = 'work_stopped', "
                "revision = revision + 1, updated_at = clock_timestamp() "
                "WHERE group_id = %s AND work_id = %s AND state = 'pending'",
                (self.group_id, work_id),
            )
        self._audit(
            "work_transitioned", work_id, previous=previous.value, state=state.value, reason=reason
        )
        return _work(updated)

    def enqueue_delivery(
        self,
        work_id: str | None,
        payload_ref: str,
        payload_hash: str,
        idempotency_key: str,
        *,
        lease: ExecutionLease | None = None,
    ) -> DeliveryIntent:
        self._writer()
        _nonempty(payload_ref, "payload_ref")
        _nonempty(idempotency_key, "idempotency_key")
        if not re.fullmatch(r"[a-f0-9]{64}", payload_hash):
            raise DomainError("invalid_input", "payload_hash must be a lowercase SHA-256 digest")
        goal_revision = None
        if work_id is not None:
            work = self._work_row(work_id)
            self._controller(work)
            if lease is None:
                raise DomainError(
                    "lease_required", "Delivery requests need a valid execution lease"
                )
            self._assert_lease(work, lease)
            if work["state"] != WorkState.DELIVERING:
                raise DomainError("invalid_transition", "Work is not ready for delivery")
            goal_revision = work["goal_revision"]
        return self._insert_delivery(
            work_id,
            payload_ref,
            payload_hash,
            idempotency_key,
            goal_revision=goal_revision,
        )

    def _reply_source(self, request_event_id: str) -> GroupEvent:
        event = self.get_event(request_event_id)
        if event.content_state != "active":
            raise DomainError("source_unavailable", "Removed source cannot authorize a reply")
        self._validate_transcription_origin(event)
        if event.actor_id != self.actor.member_id:
            raise DomainError("permission_denied", "Reply must match the request event's actor")
        if event.trusted_actor_origin not in _VERIFIED_REPLY_ORIGINS or event.kind in (
            EventKind.SYSTEM,
            EventKind.MEMBERSHIP,
            EventKind.RECALL,
        ):
            raise DomainError("permission_denied", "Reply requires a verified conversation request")
        return event

    def enqueue_reply(
        self, request_event_id: str, text: str, *, read_only_authorized: bool = False
    ) -> DeliveryIntent:
        """Enqueue one text-only response to this actor's persisted request.

        The control layer supplies read_only_authorized from trusted group
        policy, never from a model. This grants no work, artifact, recipient or
        generic outbox capability to an observer or maintainer.
        """
        return self._enqueue_request_text(
            request_event_id, text, slot="reply", read_only_authorized=read_only_authorized
        )

    def enqueue_early_ack(
        self, request_event_id: str, text: str, *, read_only_authorized: bool = False
    ) -> DeliveryIntent:
        """Enqueue one source-bound acknowledgement before a slow final reply.

        The acknowledgement has the same actor and trusted policy requirements
        as enqueue_reply, with its own fixed idempotency key. It grants no
        authority to use referenced text or create work.
        """
        return self._enqueue_request_text(
            request_event_id, text, slot="early_ack", read_only_authorized=read_only_authorized
        )

    def _enqueue_request_text(
        self,
        request_event_id: str,
        text: str,
        *,
        slot: Literal["reply", "early_ack"],
        read_only_authorized: bool,
    ) -> DeliveryIntent:
        if self.actor.role in (Role.OBSERVER, Role.MAINTAINER) and read_only_authorized is not True:
            raise DomainError(
                "permission_denied", "Current policy has not authorized a read-only reply"
            )
        self.conn.execute(
            "SELECT id FROM ga_events WHERE group_id=%s AND id=%s FOR SHARE",
            (self.group_id, request_event_id),
        )
        event = self._reply_source(request_event_id)
        _nonempty(text, "reply text")
        return self._insert_delivery(
            None,
            "text:" + text,
            hashlib.sha256(text.encode("utf-8")).hexdigest(),
            slot + ":" + event.id + ":1",
            request_event_id=event.id,
        )

    def _delivery_controller(self, row: Row) -> None:
        if self.actor.role in (Role.MEMBER, Role.OWNER):
            self._controller(row, "created_by")
            return
        if (
            row["created_by"] != self.actor.member_id
            or row["request_event_id"] is None
            or row["work_id"] is not None
            or not row["payload_ref"].startswith("text:")
        ):
            raise DomainError(
                "permission_denied", "Read-only role may control only its bound text reply"
            )
        self._reply_source(row["request_event_id"])

    def _insert_delivery(
        self,
        work_id: str | None,
        payload_ref: str,
        payload_hash: str,
        idempotency_key: str,
        *,
        goal_revision: int | None = None,
        request_event_id: str | None = None,
    ) -> DeliveryIntent:
        # Let every usable unique constraint arbitrate, including the primary
        # key. Only the exact business key can identify the canonical intent:
        # early acknowledgement and final reply share their source event.
        row = self.conn.execute(
            "INSERT INTO ga_deliveries (id, group_id, work_id, goal_revision, created_by, "
            "policy_revision, payload_ref, payload_hash, idempotency_key, request_event_id) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) "
            "ON CONFLICT DO NOTHING RETURNING *",
            (
                _id(),
                self.group_id,
                work_id,
                goal_revision,
                self.actor.member_id,
                self.actor.policy_revision,
                payload_ref,
                payload_hash,
                idempotency_key,
                request_event_id,
            ),
        ).fetchone()
        if row is None:
            row = self.conn.execute(
                "SELECT * FROM ga_deliveries WHERE group_id = %s AND idempotency_key = %s",
                (self.group_id, idempotency_key),
            ).fetchone()
            if row is None:
                raise DomainError(
                    "idempotency_conflict", "Delivery identity conflicts with another purpose"
                )
            self._delivery_controller(row)
            if (
                row["work_id"],
                row["goal_revision"],
                row["payload_ref"],
                row["payload_hash"],
                row["request_event_id"],
            ) != (
                work_id,
                goal_revision,
                payload_ref,
                payload_hash,
                request_event_id,
            ):
                raise DomainError(
                    "idempotency_conflict", "Delivery key is bound to another payload"
                )
            return _delivery(row)
        self._audit(
            "delivery_enqueued",
            row["id"],
            work_id=work_id,
            idempotency_key=idempotency_key,
            request_event_id=request_event_id,
        )
        return _delivery(row)

    def get_delivery(self, intent_id: str) -> DeliveryIntent:
        return _delivery(self._delivery_row(intent_id, lock=False))

    def is_bot_message(self, platform_message_id: str) -> bool:
        """Receipt evidence only; text badges and outgoing direction are not proof."""
        known = (
            self.conn.execute(
                "SELECT 1 FROM ga_deliveries WHERE group_id = %s AND platform_message_id = %s "
                "AND state = 'accepted' LIMIT 1",
                (self.group_id, platform_message_id),
            ).fetchone()
            is not None
        )
        if known:
            return True
        parts = self.conn.execute("SELECT to_regclass('ga_delivery_parts') AS parts").fetchone()
        return bool(
            parts
            and parts["parts"]
            and self.conn.execute(
                "SELECT 1 FROM ga_delivery_parts WHERE group_id=%s AND platform_message_id=%s AND state='accepted' LIMIT 1",
                (self.group_id, platform_message_id),
            ).fetchone()
        )

    def _delivery_row(self, intent_id: str, *, lock: bool = True) -> Row:
        row = self.conn.execute(
            "SELECT * FROM ga_deliveries WHERE group_id = %s AND id = %s"
            + (" FOR UPDATE" if lock else ""),
            (self.group_id, intent_id),
        ).fetchone()
        if row is None:
            raise DomainError("not_found", "Delivery was not found in this group")
        return row

    def list_delivery(
        self, *, state: DeliveryState | None = None, limit: int = 100
    ) -> list[DeliveryIntent]:
        state_value = state.value if state is not None else None
        rows = self.conn.execute(
            "SELECT * FROM ga_deliveries WHERE group_id = %s "
            "AND (%s::text IS NULL OR state = %s) ORDER BY created_at, id LIMIT %s",
            (self.group_id, state_value, state_value, min(max(limit, 1), 1000)),
        ).fetchall()
        return [_delivery(row) for row in rows]

    def _work_delivery_rows(self, work_id: str, goal_revision: int) -> list[Row]:
        return self.conn.execute(
            "SELECT d.id,d.state,(SELECT jsonb_agg(jsonb_build_object("
            "'state',p.state,'platform_job_id',p.platform_job_id,"
            "'platform_message_id',p.platform_message_id)) "
            "FROM ga_delivery_parts p WHERE p.group_id=d.group_id AND p.intent_id=d.id) "
            "AS parts FROM ga_deliveries d "
            "WHERE d.group_id=%s AND d.work_id=%s AND d.goal_revision=%s "
            "AND NOT (d.state='rejected' "
            "AND d.reason IS NOT DISTINCT FROM 'candidate_superseded_before_dispatch') "
            "ORDER BY d.created_at,d.id",
            (self.group_id, work_id, goal_revision),
        ).fetchall()

    def delivery_states(self, work_id: str, *, goal_revision: int) -> tuple[DeliveryState, ...]:
        """One-snapshot supervision view; durable send identities stay unchanged."""
        return tuple(
            projected_delivery_state(row["state"], row["parts"] or ())
            for row in self._work_delivery_rows(work_id, goal_revision)
        )

    def claim_delivery(
        self, worker_id: str, ttl_seconds: int = 30, *, intent_id: str | None = None
    ) -> DeliveryIntent | None:
        _nonempty(worker_id, "worker_id")
        _ttl(ttl_seconds)
        # Work rows are locked before delivery rows everywhere. This also makes
        # a control cancellation serialize before a dispatcher authorizes send.
        candidates = self.conn.execute(
            "SELECT id, work_id FROM ga_deliveries WHERE group_id = %s AND state = 'pending' "
            "AND policy_revision = %s "
            "AND (%s::text IS NULL OR id = %s) AND (%s OR created_by = %s) "
            "AND (%s OR (request_event_id IS NOT NULL AND work_id IS NULL)) "
            "ORDER BY created_at, id LIMIT 100",
            (
                self.group_id,
                self.actor.policy_revision,
                intent_id,
                intent_id,
                self.actor.role == Role.OWNER,
                self.actor.member_id,
                self.actor.role in (Role.MEMBER, Role.OWNER),
            ),
        ).fetchall()
        for candidate in candidates:
            work = None
            if candidate["work_id"]:
                work = self.conn.execute(
                    "SELECT * FROM ga_work_orders WHERE group_id = %s AND id = %s "
                    "FOR UPDATE SKIP LOCKED",
                    (self.group_id, candidate["work_id"]),
                ).fetchone()
                if work is None or work["state"] != WorkState.DELIVERING:
                    continue
                if work["policy_revision"] != self.actor.policy_revision:
                    continue
            row = self.conn.execute(
                "SELECT * FROM ga_deliveries WHERE group_id = %s AND id = %s "
                "AND state = 'pending' FOR UPDATE SKIP LOCKED",
                (self.group_id, candidate["id"]),
            ).fetchone()
            if row is None or (work and row["goal_revision"] != work["goal_revision"]):
                continue
            self._delivery_controller(row)
            updated = self.conn.execute(
                "UPDATE ga_deliveries SET state = 'dispatching', worker_id = %s, "
                "claim_token = claim_token + 1, revision = revision + 1, "
                "claim_expires_at = clock_timestamp() + %s * interval '1 second', "
                "updated_at = clock_timestamp() WHERE group_id = %s AND id = %s RETURNING *",
                (worker_id, ttl_seconds, self.group_id, row["id"]),
            ).fetchone()
            assert updated is not None
            self._audit("delivery_claimed", row["id"], claim_token=updated["claim_token"])
            return _delivery(updated)
        return None

    def _lock_delivery_work(self, intent_id: str) -> Row:
        preview = self._delivery_row(intent_id, lock=False)
        self._delivery_controller(preview)
        if preview["work_id"]:
            self._work_row(preview["work_id"])
        row = self._delivery_row(intent_id)
        self._delivery_controller(row)
        return row

    def _uncertain_work(self, work_id: str | None) -> None:
        if work_id is None:
            return
        row = self._work_row(work_id)
        if row["state"] in (WorkState.COMPLETED, WorkState.CANCELLED, WorkState.FAILED):
            return
        self.conn.execute(
            "UPDATE ga_work_orders SET state = 'paused', pause_reason = 'delivery_uncertain', "
            "revision = revision + 1, updated_at = clock_timestamp() "
            "WHERE group_id = %s AND id = %s",
            (self.group_id, work_id),
        )
        self._revoke(work_id)
        self._audit("work_paused", work_id, reason="delivery_uncertain")

    def finish_delivery(
        self,
        intent_id: str,
        claim_token: int,
        state: DeliveryState,
        *,
        platform_message_id: str | None = None,
        platform_job_id: str | None = None,
        evidence_ref: str | None = None,
        reason: str | None = None,
    ) -> DeliveryIntent:
        row = self._lock_delivery_work(intent_id)
        state = DeliveryState(state)
        if state not in (DeliveryState.ACCEPTED, DeliveryState.UNKNOWN, DeliveryState.REJECTED):
            raise DomainError("invalid_transition", "Invalid delivery outcome")
        if row["claim_token"] != claim_token:
            raise DomainError("stale_lease", "Delivery claim token is stale")
        if row["state"] == state:
            if state == DeliveryState.ACCEPTED and (
                row["platform_message_id"],
                row["platform_job_id"],
            ) != (platform_message_id, platform_job_id):
                raise DomainError("idempotency_conflict", "Receipt cannot be overwritten")
            return _delivery(row)
        if row["state"] != DeliveryState.DISPATCHING:
            raise DomainError("invalid_transition", "Delivery is not dispatching; reconcile it")
        now = self.conn.execute("SELECT clock_timestamp() AS now").fetchone()
        assert now is not None
        if row["claim_expires_at"] <= now["now"]:
            raise DomainError("stale_lease", "Expired delivery claims require reconciliation")
        return self._delivery_outcome(
            row, state, platform_message_id, platform_job_id, evidence_ref, reason
        )

    def _delivery_outcome(
        self,
        row: Row,
        state: DeliveryState,
        message_id: str | None,
        job_id: str | None,
        evidence_ref: str | None,
        reason: str | None,
    ) -> DeliveryIntent:
        if row["reason"] in {"source_forgotten", "source_recalled", "source_expired"}:
            reason = row["reason"]
        if state == DeliveryState.ACCEPTED and not (message_id or job_id):
            raise DomainError("invalid_input", "Accepted delivery requires a platform receipt ID")
        if message_id is not None:
            _nonempty(message_id, "platform_message_id")
        if job_id is not None:
            _nonempty(job_id, "platform_job_id")
        updated = self.conn.execute(
            "UPDATE ga_deliveries SET state = %s, platform_message_id = %s, platform_job_id = %s, "
            "evidence_ref = %s, reason = %s, accepted_at = CASE WHEN %s = 'accepted' "
            "THEN clock_timestamp() ELSE NULL END, claim_expires_at = NULL, "
            "revision = revision + 1, updated_at = clock_timestamp() "
            "WHERE group_id = %s AND id = %s RETURNING *",
            (
                state.value,
                message_id,
                job_id,
                evidence_ref,
                reason,
                state.value,
                self.group_id,
                row["id"],
            ),
        ).fetchone()
        assert updated is not None
        if state == DeliveryState.UNKNOWN and row["work_id"] is not None:
            parts = self.conn.execute(
                "SELECT state,platform_job_id,platform_message_id FROM ga_delivery_parts "
                "WHERE group_id=%s AND intent_id=%s",
                (self.group_id, row["id"]),
            ).fetchall()
            if projected_delivery_state(state, parts) == DeliveryState.UNKNOWN:
                self._uncertain_work(row["work_id"])
        self._audit("delivery_outcome", row["id"], state=state.value, evidence_ref=evidence_ref)
        return _delivery(updated)

    def recover_expired_deliveries(self) -> list[DeliveryIntent]:
        rows = self.conn.execute(
            "SELECT id FROM ga_deliveries WHERE group_id = %s AND state = 'dispatching' "
            "AND claim_expires_at <= clock_timestamp() AND (%s OR created_by = %s) "
            "AND (%s OR (request_event_id IS NOT NULL AND work_id IS NULL)) ORDER BY id",
            (
                self.group_id,
                self.actor.role == Role.OWNER,
                self.actor.member_id,
                self.actor.role in (Role.MEMBER, Role.OWNER),
            ),
        ).fetchall()
        recovered = []
        for candidate in rows:
            row = self._lock_delivery_work(candidate["id"])
            if row["state"] != DeliveryState.DISPATCHING:
                continue
            recovered.append(
                self._delivery_outcome(
                    row, DeliveryState.UNKNOWN, None, None, None, "dispatcher_lease_expired"
                )
            )
        return recovered

    def reconcile_delivery(
        self,
        intent_id: str,
        state: DeliveryState,
        *,
        evidence_ref: str,
        platform_message_id: str | None = None,
        platform_job_id: str | None = None,
        reason: str | None = None,
    ) -> DeliveryIntent:
        row = self._lock_delivery_work(intent_id)
        _nonempty(evidence_ref, "evidence_ref")
        state = DeliveryState(state)
        if row["state"] != DeliveryState.UNKNOWN or state not in (
            DeliveryState.ACCEPTED,
            DeliveryState.REJECTED,
        ):
            raise DomainError("invalid_transition", "Only unknown delivery can be reconciled")
        return self._delivery_outcome(
            row, state, platform_message_id, platform_job_id, evidence_ref, reason
        )

    def cancel_pending_delivery(self, intent_id: str, *, expected_revision: int) -> DeliveryIntent:
        row = self._lock_delivery_work(intent_id)
        self._revision(row, expected_revision)
        if row["state"] != DeliveryState.PENDING:
            raise DomainError("invalid_transition", "Only pending delivery can be cancelled")
        return self._delivery_outcome(
            row, DeliveryState.REJECTED, None, None, None, "cancelled_before_dispatch"
        )

    def create_team_task(
        self,
        title: str,
        *,
        owner_id: str | None = None,
        due_at: datetime | None = None,
        source_event_id: str | None = None,
    ) -> TeamTask:
        self._writer()
        from groupagent.memory.common import memory_lock

        memory_lock(self)
        _nonempty(title, "title")
        owner_id = owner_id or self.actor.member_id
        _nonempty(owner_id, "owner_id")
        if owner_id != self.actor.member_id and self.actor.role != Role.OWNER:
            raise DomainError("permission_denied", "Only the group owner may assign another member")
        if due_at is not None and due_at.utcoffset() is None:
            raise DomainError("invalid_input", "Task due time must include a timezone")
        if source_event_id:
            event = self.get_event(source_event_id)
            if event.content_state != "active":
                raise DomainError("source_unavailable", "Removed source cannot create a human task")
            if event.actor_id != self.actor.member_id:
                raise DomainError(
                    "permission_denied", "Task creation source must match the current actor"
                )
            from groupagent.memory.common import source_rows

            source_rows(self, [source_event_id], require_complete=True)
        fields = self._task_fields(
            {},
            ("title", "owner_id", "status", "progress", "evidence_refs", "due_at"),
            source_event_id,
        )
        row = self.conn.execute(
            "INSERT INTO ga_team_tasks (id, group_id, title, owner_id, assignment_kind, due_at, "
            "source_event_id,created_by,field_sources,source_event_ids,provenance_kind) VALUES (%s, %s, %s, %s, %s, %s, %s,%s,%s,%s,%s) "
            "ON CONFLICT (group_id, source_event_id) DO NOTHING RETURNING *",
            (
                _id(),
                self.group_id,
                title,
                owner_id,
                "self_claim" if owner_id == self.actor.member_id else "owner_assigned",
                due_at,
                source_event_id,
                self.actor.member_id,
                Jsonb(fields),
                Jsonb([source_event_id] if source_event_id else []),
                "source_bound" if source_event_id else "independent",
            ),
        ).fetchone()
        if row is None:
            row = self.conn.execute(
                "SELECT * FROM ga_team_tasks WHERE group_id = %s AND source_event_id = %s",
                (self.group_id, source_event_id),
            ).fetchone()
            assert row is not None
            self._controller(row, "owner_id")
            return TeamTask.model_validate(row)
        self._audit(
            "team_task_created",
            row["id"],
            owner_id=owner_id,
            assignment_kind=row["assignment_kind"],
        )
        return TeamTask.model_validate(row)

    @staticmethod
    def _task_fields(
        existing: dict[str, list[str]],
        names,
        event_id: str | None,
        append_fields: set[str] | frozenset[str] = frozenset(),
    ) -> dict[str, list[str]]:
        result = {field: list(sources) for field, sources in existing.items()}
        if event_id is not None:
            for field in names:
                result[field] = (
                    sorted(set(result.get(field, [])) | {event_id})
                    if field in append_fields
                    else [event_id]
                )
        return result

    def _task_update_source(self, event_id: str | None) -> None:
        if event_id is None:
            return
        event = self.get_event(event_id)
        if event.actor_id != self.actor.member_id:
            raise DomainError(
                "permission_denied", "Task update source must belong to the current actor"
            )
        if event.content_state != "active":
            raise DomainError("source_unavailable", "Removed event cannot authorize task changes")
        from groupagent.memory.common import source_rows

        source_rows(self, [event_id], require_complete=True)

    def assert_task_sources(self, revisions: object, *, require_current: bool = True) -> None:
        if not isinstance(revisions, dict) or len(revisions) > 100:
            raise DomainError("scope_violation", "Task source scope must be a bounded revision map")
        for identifier, revision in revisions.items():
            if (
                not isinstance(identifier, str)
                or not isinstance(revision, int)
                or isinstance(revision, bool)
            ):
                raise DomainError(
                    "scope_violation", "Task source revisions must be integer identities"
                )
            task = self.get_team_task(identifier)
            if task.content_state != "active":
                raise DomainError("source_unavailable", "Forgotten task cannot authorize execution")
            if require_current and task.revision != revision:
                raise DomainError("memory_changed", "Task changed since the request snapshot")

    def _task_source_scope(
        self, scope: dict[str, JsonValue], input_versions: list[str]
    ) -> dict[str, JsonValue]:
        revisions = scope.get("source_task_revisions", {})
        self.assert_task_sources(revisions)
        if not revisions:
            return scope
        # A server-supplied event map is the exact context actually consumed.
        # In particular historical Work/document text must not acquire newer
        # private task fields merely because its logical Task ID is unchanged.
        if "source_fingerprints" in scope:
            return scope
        fingerprints = scope.get("source_fingerprints", {})
        if not isinstance(fingerprints, dict):
            raise DomainError("scope_violation", "Task source scope requires a fingerprint map")
        merged = dict(fingerprints)
        raw_retained = scope.get("retained_fact_source_ids", [])
        if not isinstance(raw_retained, list) or any(
            not isinstance(value, str) for value in raw_retained
        ):
            raise DomainError("scope_violation", "Retained task sources must be string identities")
        retained = {value for value in raw_retained if isinstance(value, str)}
        assert isinstance(revisions, dict)
        for identifier in revisions:
            fixed = (
                self.conn.execute(
                    "SELECT 1 FROM ga_artifact_catalog a JOIN ga_work_orders w ON w.group_id=a.group_id AND w.id=a.work_id WHERE a.group_id=%s AND a.version_id=ANY(%s) AND a.available AND a.validation->'passed'='true'::jsonb AND (a.expires_at IS NULL OR a.expires_at>clock_timestamp()) AND (a.shared OR a.created_by=%s OR %s) AND w.scope->'source_task_revisions' ? %s AND NOT COALESCE((w.scope->>'privacy_redacted')::boolean,false) LIMIT 1",
                    (
                        self.group_id,
                        input_versions,
                        self.actor.member_id,
                        self.actor.role == Role.OWNER,
                        identifier,
                    ),
                ).fetchone()
                if input_versions
                else None
            )
            if fixed:
                continue
            task = self.get_team_task(identifier)
            for source in task.source_event_ids:
                merged.setdefault(source, self.event_fingerprint(source))
                if self.get_event(source).content_state == "expired":
                    retained.add(source)
        retained_json: list[JsonValue] = [value for value in sorted(retained)]
        return {
            **scope,
            "source_fingerprints": merged,
            "source_event_ids": list(merged),
            "retained_fact_source_ids": retained_json,
        }

    def get_team_task(self, task_id: str) -> TeamTask:
        row = self.conn.execute(
            "SELECT * FROM ga_team_tasks WHERE group_id = %s AND id = %s", (self.group_id, task_id)
        ).fetchone()
        if row is None:
            raise DomainError("not_found", "Task was not found in this group")
        return TeamTask.model_validate(row)

    def claim_team_task(
        self, task_id: str, *, expected_revision: int, source_event_id: str | None = None
    ) -> TeamTask:
        """Claim an unassigned human task without inventing an owner during migration."""
        self._writer()
        from groupagent.memory.common import memory_lock

        memory_lock(self)
        self._task_update_source(source_event_id)
        row = self.conn.execute(
            "SELECT * FROM ga_team_tasks WHERE group_id=%s AND id=%s FOR UPDATE",
            (self.group_id, task_id),
        ).fetchone()
        if row is None:
            raise DomainError("not_found", "Task was not found in this group")
        self._revision(row, expected_revision)
        if row["content_state"] != "active":
            raise DomainError("source_unavailable", "Forgotten task cannot be reclaimed")
        if row["owner_id"] not in (None, self.actor.member_id):
            raise DomainError("permission_denied", "Another member already owns this task")
        if row["status"] in (TeamTaskState.COMPLETED, TeamTaskState.CANCELLED):
            raise DomainError("invalid_transition", "Closed tasks cannot be claimed")
        if row["owner_id"] == self.actor.member_id:
            return TeamTask.model_validate(row)
        fields = self._task_fields(row["field_sources"], ("owner_id", "status"), source_event_id)
        updated = self.conn.execute(
            "UPDATE ga_team_tasks SET owner_id=%s,assignment_kind='self_claim',status='in_progress',field_sources=%s,source_event_ids=%s,revision=revision+1,updated_at=clock_timestamp() WHERE group_id=%s AND id=%s RETURNING *",
            (
                self.actor.member_id,
                Jsonb(fields),
                Jsonb(sorted({source for sources in fields.values() for source in sources})),
                self.group_id,
                task_id,
            ),
        ).fetchone()
        self._audit("team_task_claimed", task_id, owner_id=self.actor.member_id)
        return TeamTask.model_validate(updated)

    def list_team_tasks(
        self, *, owner_id: str | None = None, status: TeamTaskState | None = None, limit: int = 100
    ) -> list[TeamTask]:
        return self.find_team_tasks("", owner_id=owner_id, status=status, limit=limit)

    def find_team_tasks(
        self,
        query: str,
        *,
        owner_id: str | None = None,
        status: TeamTaskState | None = None,
        limit: int = 100,
    ) -> list[TeamTask]:
        pattern = "%" + query.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_") + "%"
        rows = self.conn.execute(
            "SELECT * FROM ga_team_tasks WHERE group_id=%s AND content_state='active' AND (%s::text IS NULL OR owner_id=%s) "
            "AND (%s::text IS NULL OR status=%s) AND (title ILIKE %s OR progress ILIKE %s) "
            "ORDER BY updated_at DESC,id LIMIT %s",
            (
                self.group_id,
                owner_id,
                owner_id,
                status.value if status else None,
                status.value if status else None,
                pattern,
                pattern,
                min(max(limit, 1), 1000),
            ),
        ).fetchall()
        return [TeamTask.model_validate(row) for row in rows]

    def update_team_task(
        self,
        task_id: str,
        *,
        expected_revision: int,
        title: str | None = None,
        status: TeamTaskState | None = None,
        progress: str | None = None,
        evidence_refs: list[str] | None = None,
        owner_id: str | None = None,
        due_at: datetime | None | _Unchanged = _UNCHANGED,
        source_event_id: str | None = None,
        append_source_fields: set[str] | frozenset[str] = frozenset(),
    ) -> TeamTask:
        from groupagent.memory.common import memory_lock

        memory_lock(self)
        self._task_update_source(source_event_id)
        row = self.conn.execute(
            "SELECT * FROM ga_team_tasks WHERE group_id = %s AND id = %s FOR UPDATE",
            (self.group_id, task_id),
        ).fetchone()
        if row is None:
            raise DomainError("not_found", "Task was not found in this group")
        self._controller(row, "owner_id")
        self._revision(row, expected_revision)
        if row["content_state"] != "active":
            raise DomainError("source_unavailable", "Forgotten task cannot be updated")
        if owner_id is not None:
            _nonempty(owner_id, "owner_id")
            if self.actor.role != Role.OWNER:
                raise DomainError("permission_denied", "Only the group owner may reassign tasks")
        if title is not None:
            _nonempty(title, "title")
        effective_due = row["due_at"] if isinstance(due_at, _Unchanged) else due_at
        if effective_due is not None and (
            not isinstance(effective_due, datetime) or effective_due.utcoffset() is None
        ):
            raise DomainError("invalid_input", "Task due time must include a timezone")
        changed_fields = [
            name
            for name, value in (
                ("title", title),
                ("status", status),
                ("progress", progress),
                ("evidence_refs", evidence_refs),
                ("owner_id", owner_id),
            )
            if value is not None
        ]
        if not isinstance(due_at, _Unchanged):
            changed_fields.append("due_at")
        if not isinstance(
            append_source_fields, (set, frozenset)
        ) or not append_source_fields.issubset(changed_fields):
            raise DomainError(
                "invalid_input",
                "Append provenance must name fields explicitly updated by this call",
            )
        fields = self._task_fields(
            row["field_sources"], changed_fields, source_event_id, append_source_fields
        )
        updated = self.conn.execute(
            "UPDATE ga_team_tasks SET title = %s, status = %s, progress = %s, evidence_refs = %s, "
            "owner_id = %s, assignment_kind = %s, due_at = %s, revision = revision + 1, "
            "field_sources=%s,source_event_ids=%s,updated_at = clock_timestamp() WHERE group_id = %s AND id = %s RETURNING *",
            (
                title if title is not None else row["title"],
                status.value if status is not None else row["status"],
                progress if progress is not None else row["progress"],
                Jsonb(evidence_refs if evidence_refs is not None else row["evidence_refs"]),
                owner_id or row["owner_id"],
                "owner_assigned"
                if owner_id and owner_id != row["owner_id"]
                else row["assignment_kind"],
                effective_due,
                Jsonb(fields),
                Jsonb(sorted({source for sources in fields.values() for source in sources})),
                self.group_id,
                task_id,
            ),
        ).fetchone()
        assert updated is not None
        self._audit(
            "team_task_updated", task_id, owner_id=updated["owner_id"], revision=updated["revision"]
        )
        return TeamTask.model_validate(updated)
