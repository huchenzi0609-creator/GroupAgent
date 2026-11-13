"""Short, atomic PostgreSQL transactions for the durable control plane.

This repository is an application authorization boundary, not a database role
for untrusted code. Only the trusted control service holds its DSN. Model/tool
processes must not receive a connection or construct their own ActorContext.
"""

from __future__ import annotations

import hashlib
import re
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import datetime
from importlib.resources import files
from typing import Any
from uuid import uuid4

import psycopg
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
    check_transition,
)

Row = dict[str, Any]
Connection = psycopg.Connection[Row]


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
        return psycopg.connect(self.dsn, row_factory=dict_row)

    def migrate(self) -> int:
        """Apply packaged, checksum-verified migrations in one transaction.

        The advisory lock serializes concurrent startup. A failed migration
        rolls back both its DDL and ledger entry; applied SQL is immutable.
        """
        with self.connect() as conn:
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
        try:
            with self.connect() as conn:
                unit = UnitOfWork(conn, actor, trace_id)
                unit._check_group()
                yield unit
        except DomainError as error:
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
    def __init__(self, conn: Connection, actor: ActorContext, trace_id: str) -> None:
        self.conn = conn
        self.actor = actor
        self.trace_id = trace_id

    @property
    def group_id(self) -> str:
        return self.actor.group_id

    def _check_group(self) -> None:
        # Hold only for this short transaction: a policy change cannot race an
        # already-authorized write, and takes effect before the next operation.
        row = self.conn.execute(
            "SELECT policy_revision FROM ga_groups WHERE id = %s FOR SHARE", (self.group_id,)
        ).fetchone()
        if row is None:
            raise DomainError("permission_denied", "Group is not authorized")
        if row["policy_revision"] != self.actor.policy_revision:
            raise DomainError("policy_changed", "Actor policy revision is stale")

    def _writer(self) -> None:
        if self.actor.role not in (Role.MEMBER, Role.OWNER):
            raise DomainError("permission_denied", "Business changes require member or owner role")

    def _controller(self, row: Row, owner_field: str = "requester_id") -> None:
        self._writer()
        if row[owner_field] != self.actor.member_id and self.actor.role != Role.OWNER:
            raise DomainError(
                "permission_denied", "Only the resource owner or group owner may edit"
            )

    def _audit(self, action: str, resource_ref: str | None, **details: JsonValue) -> None:
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

    def get_event(self, event_id: str) -> GroupEvent:
        row = self.conn.execute(
            "SELECT body FROM ga_events WHERE group_id = %s AND id = %s",
            (self.group_id, event_id),
        ).fetchone()
        if row is None:
            raise DomainError("not_found", "Event was not found in this group")
        return GroupEvent.model_validate(row["body"])

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
        if decision.get("group_id", self.group_id) != self.group_id:
            raise DomainError("scope_violation", "Decision belongs to another group")
        if decision.get("actor_id", event.actor_id) != event.actor_id:
            raise DomainError("permission_denied", "Decision cannot replace event identity")
        if decision.get("kind") not in (
            "ignore",
            "observe",
            "reply",
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
            "SELECT e.body FROM ga_events e WHERE e.group_id = %s AND NOT EXISTS "
            "(SELECT 1 FROM ga_decisions d WHERE d.group_id = e.group_id AND d.event_id = e.id) "
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
        inserted = self.conn.execute(
            "INSERT INTO ga_events (id, group_id, source_key, actor_id, kind, body) "
            "VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING RETURNING id",
            (
                event.id,
                self.group_id,
                event.source_key,
                event.actor_id,
                event.kind.value,
                Jsonb(event.model_dump(mode="json")),
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
            original = GroupEvent.model_validate(old["body"])
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
        if "allowed_group_ids" in scope and scope["allowed_group_ids"] != [self.group_id]:
            raise DomainError("scope_violation", "Work is restricted to its origin group")
        if scope.get("external_account_changes", False):
            raise DomainError("permission_denied", "External account changes are not supported")
        if scope.get("delivery_target", "origin_group") != "origin_group":
            raise DomainError("scope_violation", "Delivery is restricted to the origin group")

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
            "SELECT id FROM ga_events WHERE group_id = %s AND id = %s FOR UPDATE",
            (self.group_id, request_event_id),
        )
        event = self.get_event(request_event_id)
        if event.kind == EventKind.UNSUPPORTED_VIDEO:
            raise DomainError("unsupported_video", "Video events cannot create execution work")
        if event.actor_id != self.actor.member_id and self.actor.role != Role.OWNER:
            raise DomainError("permission_denied", "Cannot claim another member's request")
        if state not in (WorkState.QUEUED, WorkState.WAITING_INPUT):
            raise DomainError("invalid_transition", "New work must be queued or waiting for input")
        scope = scope or {}
        self._scope(scope)
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
    ) -> WorkOrder:
        row = self._work_row(work_id)
        self._controller(row)
        self._revision(row, expected_revision)
        _nonempty(goal, "goal")
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
            "state = %s, pause_reason = %s, updated_at = clock_timestamp() "
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
        if row["policy_revision"] != self.actor.policy_revision:
            raise DomainError("policy_changed", "Work must be reauthorized before execution")
        if row["state"] not in (WorkState.QUEUED, WorkState.RUNNING):
            raise DomainError("invalid_transition", "Work is not ready for execution")
        if row["lease_expires_at"] and row["lease_expires_at"] > row["db_now"]:
            raise DomainError("lease_busy", "Another execution lease is active", retryable=True)
        if row["state"] == WorkState.RUNNING and not recovery_evidence_ref:
            raise DomainError("reconciliation_required", "Expired work requires recovery evidence")
        self._revoke(work_id, state="expired")
        updated = self.conn.execute(
            "UPDATE ga_work_orders SET lease_owner = %s, "
            "lease_expires_at = clock_timestamp() + %s * interval '1 second', "
            "fencing_token = fencing_token + 1, attempt_no = attempt_no + 1, "
            "state = 'running', pause_reason = NULL, revision = revision + 1, "
            "updated_at = clock_timestamp() WHERE group_id = %s AND id = %s RETURNING *",
            (worker_id, ttl_seconds, self.group_id, work_id),
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
            "work_claimed",
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
                "AND goal_revision = %s",
                (self.group_id, work_id, row["goal_revision"]),
            ).fetchall()
            if not deliveries or any(
                item["state"] != DeliveryState.ACCEPTED for item in deliveries
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
        if event.actor_id != self.actor.member_id:
            raise DomainError("permission_denied", "Reply must match the request event's actor")
        if event.trusted_actor_origin != "verified_channel_metadata" or event.kind in (
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
        if self.actor.role in (Role.OBSERVER, Role.MAINTAINER) and read_only_authorized is not True:
            raise DomainError(
                "permission_denied", "Current policy has not authorized a read-only reply"
            )
        event = self._reply_source(request_event_id)
        _nonempty(text, "reply text")
        return self._insert_delivery(
            None,
            "text:" + text,
            hashlib.sha256(text.encode("utf-8")).hexdigest(),
            "reply:" + event.id + ":1",
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
        row = self.conn.execute(
            "INSERT INTO ga_deliveries (id, group_id, work_id, goal_revision, created_by, "
            "policy_revision, payload_ref, payload_hash, idempotency_key, request_event_id) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) "
            "ON CONFLICT (group_id, idempotency_key) DO NOTHING RETURNING *",
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
            assert row is not None
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
        return (
            self.conn.execute(
                "SELECT 1 FROM ga_deliveries WHERE group_id = %s AND platform_message_id = %s "
                "AND state = 'accepted' LIMIT 1",
                (self.group_id, platform_message_id),
            ).fetchone()
            is not None
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
        if state == DeliveryState.UNKNOWN:
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
        _nonempty(title, "title")
        owner_id = owner_id or self.actor.member_id
        _nonempty(owner_id, "owner_id")
        if owner_id != self.actor.member_id and self.actor.role != Role.OWNER:
            raise DomainError("permission_denied", "Only the group owner may assign another member")
        if due_at is not None and due_at.utcoffset() is None:
            raise DomainError("invalid_input", "Task due time must include a timezone")
        if source_event_id:
            event = self.get_event(source_event_id)
            if event.actor_id != self.actor.member_id and self.actor.role != Role.OWNER:
                raise DomainError(
                    "permission_denied", "Cannot claim another member's task evidence"
                )
        row = self.conn.execute(
            "INSERT INTO ga_team_tasks (id, group_id, title, owner_id, assignment_kind, due_at, "
            "source_event_id) VALUES (%s, %s, %s, %s, %s, %s, %s) "
            "ON CONFLICT (group_id, source_event_id) DO NOTHING RETURNING *",
            (
                _id(),
                self.group_id,
                title,
                owner_id,
                "self_claim" if owner_id == self.actor.member_id else "owner_assigned",
                due_at,
                source_event_id,
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

    def get_team_task(self, task_id: str) -> TeamTask:
        row = self.conn.execute(
            "SELECT * FROM ga_team_tasks WHERE group_id = %s AND id = %s", (self.group_id, task_id)
        ).fetchone()
        if row is None:
            raise DomainError("not_found", "Task was not found in this group")
        return TeamTask.model_validate(row)

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
    ) -> TeamTask:
        row = self.conn.execute(
            "SELECT * FROM ga_team_tasks WHERE group_id = %s AND id = %s FOR UPDATE",
            (self.group_id, task_id),
        ).fetchone()
        if row is None:
            raise DomainError("not_found", "Task was not found in this group")
        self._controller(row, "owner_id")
        self._revision(row, expected_revision)
        if owner_id is not None and owner_id != row["owner_id"]:
            _nonempty(owner_id, "owner_id")
            if self.actor.role != Role.OWNER:
                raise DomainError("permission_denied", "Only the group owner may reassign tasks")
        if title is not None:
            _nonempty(title, "title")
        updated = self.conn.execute(
            "UPDATE ga_team_tasks SET title = %s, status = %s, progress = %s, evidence_refs = %s, "
            "owner_id = %s, assignment_kind = %s, revision = revision + 1, "
            "updated_at = clock_timestamp() WHERE group_id = %s AND id = %s RETURNING *",
            (
                title if title is not None else row["title"],
                status.value if status is not None else row["status"],
                progress if progress is not None else row["progress"],
                Jsonb(evidence_refs if evidence_refs is not None else row["evidence_refs"]),
                owner_id or row["owner_id"],
                "owner_assigned"
                if owner_id and owner_id != row["owner_id"]
                else row["assignment_kind"],
                self.group_id,
                task_id,
            ),
        ).fetchone()
        assert updated is not None
        self._audit(
            "team_task_updated", task_id, owner_id=updated["owner_id"], revision=updated["revision"]
        )
        return TeamTask.model_validate(updated)
