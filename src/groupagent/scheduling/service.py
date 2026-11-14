"""PostgreSQL scheduler for explicit, time-bounded rules.

The trusted control process supplies current ActorContext and clock values. A
due scan performs no network or tool calls. It atomically admits WorkOrders;
the normal execution lease, validation and outbox gates still apply afterward.
"""

from __future__ import annotations

import builtins
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import uuid4

from psycopg.types.json import Jsonb
from pydantic import JsonValue

from groupagent.domain import ActorContext, DomainError, EventKind, GroupEvent, Role, WorkState
from groupagent.persistence import PostgresStore, UnitOfWork
from groupagent.scheduling.models import (
    AtTiming,
    EveryTiming,
    OccurrenceState,
    Schedule,
    ScheduleOccurrence,
    ScheduleState,
    Timing,
    parse_timing,
)
from groupagent.scheduling.recurrence import latest_due, next_due, utc, zone

Row = dict[str, Any]
TERMINAL_WORK = {WorkState.COMPLETED, WorkState.FAILED, WorkState.CANCELLED}


def _text(value: str, field: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise DomainError("invalid_input", f"{field} must be nonempty")


class Scheduler:
    def __init__(
        self,
        store: PostgresStore,
        *,
        max_active_per_group: int = 10,
        min_interval_seconds: int = 3600,
        default_valid_days: int = 30,
        one_shot_grace_seconds: int = 86400,
    ) -> None:
        if any(
            isinstance(value, bool) or not isinstance(value, int) or value < 1
            for value in (
                max_active_per_group,
                min_interval_seconds,
                default_valid_days,
                one_shot_grace_seconds,
            )
        ):
            raise ValueError("Scheduler limits must be positive integers")
        self.store = store
        self.max_active = max_active_per_group
        self.min_interval = min_interval_seconds
        self.default_validity = timedelta(days=default_valid_days)
        self.one_shot_grace = timedelta(seconds=one_shot_grace_seconds)
        self._bound_transaction: UnitOfWork | None = None

    def in_tx(self, tx: UnitOfWork) -> Scheduler:
        """Compose an interaction atomically without opening a nested connection."""
        bound = Scheduler(
            self.store,
            max_active_per_group=self.max_active,
            min_interval_seconds=self.min_interval,
            default_valid_days=self.default_validity.days,
            one_shot_grace_seconds=int(self.one_shot_grace.total_seconds()),
        )
        bound._bound_transaction = tx
        return bound

    @contextmanager
    def _transaction(self, actor: ActorContext) -> Iterator[UnitOfWork]:
        if self._bound_transaction is not None:
            if actor != self._bound_transaction.actor:
                raise DomainError(
                    "scope_violation", "Bound scheduling cannot replace transaction authority"
                )
            yield self._bound_transaction
        else:
            with self.store.transaction(actor) as tx:
                yield tx

    @contextmanager
    def _mutation(self, actor: ActorContext) -> Iterator[UnitOfWork]:
        with self._transaction(actor) as tx:
            tx._writer()
            from groupagent.memory.common import memory_lock

            memory_lock(tx)
            # Serializes admission/quota/coalescing per group. Do not upgrade the
            # policy row's shared lock: concurrent lock upgrades can deadlock.
            tx.conn.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
                ("groupagent:scheduler:" + actor.group_id,),
            )
            yield tx

    @staticmethod
    def _row(tx: UnitOfWork, schedule_id: str) -> Row:
        row = tx.conn.execute(
            "SELECT * FROM ga_schedules WHERE group_id = %s AND id = %s FOR UPDATE",
            (tx.group_id, schedule_id),
        ).fetchone()
        if row is None:
            raise DomainError("not_found", "Schedule was not found in this group")
        return row

    @staticmethod
    def _policy(tx: UnitOfWork, row: Row) -> None:
        if row["policy_revision"] != tx.actor.policy_revision:
            raise DomainError("policy_changed", "Schedule needs creator reauthorization")

    def _quota(self, tx: UnitOfWork, *, excluding: str | None = None) -> None:
        count = tx.conn.execute(
            "SELECT (SELECT count(*) FROM ga_schedules WHERE group_id = %s AND state = 'active' "
            "AND (%s::text IS NULL OR id <> %s)) + (SELECT count(*) FROM ga_trigger_rules WHERE group_id=%s AND state='active') AS n",
            (tx.group_id, excluding, excluding, tx.group_id),
        ).fetchone()
        assert count is not None
        if count["n"] >= self.max_active:
            raise DomainError("quota_exceeded", "The group has reached its active schedule limit")

    def _timing(self, timing: Timing | dict[str, JsonValue], now: datetime) -> Timing:
        rule = parse_timing(timing)
        if isinstance(rule, EveryTiming):
            if rule.interval_seconds < self.min_interval:
                raise DomainError(
                    "quota_exceeded", "Interval is shorter than the configured minimum"
                )
            if rule.start_at is None:
                rule = rule.model_copy(
                    update={"start_at": now + timedelta(seconds=rule.interval_seconds)}
                )
        return rule

    def create(
        self,
        actor: ActorContext,
        title: str,
        goal: str,
        timing: Timing | dict[str, JsonValue],
        *,
        now: datetime | None = None,
        time_zone: str = "Asia/Shanghai",
        valid_until: datetime | None = None,
        request_key: str | None = None,
        scope: dict[str, JsonValue] | None = None,
        budget: dict[str, JsonValue] | None = None,
        acceptance_criteria: list[str] | None = None,
    ) -> Schedule:
        now = utc(now or datetime.now(UTC))
        _text(title, "title")
        _text(goal, "goal")
        if request_key is not None:
            _text(request_key, "request_key")
        zone(time_zone)
        rule = self._timing(timing, now)
        scope = (
            scope
            if scope is not None
            else {
                "allowed_group_ids": [actor.group_id],
                "delivery_target": "origin_group",
                "external_account_changes": False,
            }
        )
        with self._mutation(actor) as tx:
            if request_key is not None:
                old = tx.conn.execute(
                    "SELECT * FROM ga_schedules WHERE group_id = %s AND request_key = %s",
                    (tx.group_id, request_key),
                ).fetchone()
                if old:
                    tx._controller(old, "creator_id")
                    return Schedule.model_validate(old)
            if isinstance(rule, AtTiming) and rule.at < now:
                raise DomainError(
                    "invalid_input", "A new one-shot schedule cannot start in the past"
                )
            end = (
                utc(valid_until)
                if valid_until
                else (
                    rule.at + self.one_shot_grace
                    if isinstance(rule, AtTiming)
                    else now + self.default_validity
                )
            )
            following = next_due(rule, time_zone, now, inclusive=True)
            if end < now or following is None or following > end:
                raise DomainError(
                    "invalid_input", "No occurrence fits within the authorized validity"
                )
            tx._scope(scope)
            if scope.get("group_id", actor.group_id) != actor.group_id:
                raise DomainError("scope_violation", "Schedule scope belongs to another group")
            self._quota(tx)
            row = tx.conn.execute(
                "INSERT INTO ga_schedules (id, group_id, creator_id, creator_role, request_key, "
                "title, goal, timing, time_zone, starts_at, valid_until, next_due_at, policy_revision, "
                "scope, budget, acceptance_criteria, created_at, updated_at) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING *",
                (
                    str(uuid4()),
                    actor.group_id,
                    actor.member_id,
                    actor.role.value,
                    request_key,
                    title,
                    goal,
                    Jsonb(rule.model_dump(mode="json")),
                    time_zone,
                    now,
                    end,
                    following,
                    actor.policy_revision,
                    Jsonb(scope),
                    Jsonb(budget or {}),
                    Jsonb(acceptance_criteria or []),
                    now,
                    now,
                ),
            ).fetchone()
            assert row is not None
            tx._audit(
                "schedule_created",
                row["id"],
                timing_kind=rule.kind,
                next_due_at=following.isoformat(),
                valid_until=end.isoformat(),
            )
            return Schedule.model_validate(row)

    def get(self, actor: ActorContext, schedule_id: str) -> Schedule:
        with self._transaction(actor) as tx:
            return Schedule.model_validate(self._row(tx, schedule_id))

    def list(self, actor: ActorContext, *, state: ScheduleState | None = None) -> list[Schedule]:
        with self._transaction(actor) as tx:
            state_value = state.value if state is not None else None
            rows = tx.conn.execute(
                "SELECT * FROM ga_schedules WHERE group_id = %s "
                "AND (%s::text IS NULL OR state = %s) ORDER BY created_at, id LIMIT 1000",
                (actor.group_id, state_value, state_value),
            ).fetchall()
            return [Schedule.model_validate(row) for row in rows]

    def occurrences(
        self, actor: ActorContext, schedule_id: str, *, limit: int = 100
    ) -> builtins.list[ScheduleOccurrence]:
        with self._transaction(actor) as tx:
            self._row(tx, schedule_id)
            rows = tx.conn.execute(
                "SELECT * FROM ga_schedule_occurrences WHERE group_id = %s AND schedule_id = %s "
                "ORDER BY scheduled_at DESC, id LIMIT %s",
                (actor.group_id, schedule_id, min(max(limit, 1), 1000)),
            ).fetchall()
            return [ScheduleOccurrence.model_validate(row) for row in rows]

    @staticmethod
    def _skip_pending(
        tx: UnitOfWork, schedule_id: str, reason: str
    ) -> builtins.list[ScheduleOccurrence]:
        rows = tx.conn.execute(
            "UPDATE ga_schedule_occurrences SET state = 'skipped', reason = %s, "
            "revision = revision + 1, updated_at = clock_timestamp() "
            "WHERE group_id = %s AND schedule_id = %s AND state = 'pending' RETURNING *",
            (reason, tx.group_id, schedule_id),
        ).fetchall()
        return [ScheduleOccurrence.model_validate(row) for row in rows]

    def update(
        self,
        actor: ActorContext,
        schedule_id: str,
        *,
        expected_revision: int,
        title: str | None = None,
        goal: str | None = None,
        timing: Timing | dict[str, JsonValue] | None = None,
        time_zone: str | None = None,
        valid_until: datetime | None = None,
        now: datetime | None = None,
    ) -> Schedule:
        now = utc(now or datetime.now(UTC))
        with self._mutation(actor) as tx:
            row = self._row(tx, schedule_id)
            tx._controller(row, "creator_id")
            tx._revision(row, expected_revision)
            if row["state"] in (ScheduleState.CANCELLED, ScheduleState.ENDED):
                raise DomainError("invalid_transition", "Closed schedules cannot be rewritten")
            if (
                row["policy_revision"] != actor.policy_revision
                and row["creator_id"] != actor.member_id
            ):
                raise DomainError(
                    "policy_changed", "Only the current creator may reauthorize this rule"
                )
            if title is not None:
                _text(title, "title")
            if goal is not None:
                _text(goal, "goal")
            rule = self._timing(timing if timing is not None else row["timing"], now)
            time_zone = time_zone if time_zone is not None else row["time_zone"]
            zone(time_zone)
            end = utc(valid_until) if valid_until is not None else row["valid_until"]
            if end < now:
                raise DomainError("invalid_input", "The new validity must not already have expired")
            changed_timing = timing is not None or time_zone != row["time_zone"]
            following = next_due(rule, time_zone, now) if changed_timing else row["next_due_at"]
            if following is not None and following > end:
                following = None
            if changed_timing:
                self._skip_pending(tx, schedule_id, "rule_updated")
            target = row["state"]
            if (
                target == ScheduleState.ACTIVE
                and following is None
                and self._pending(tx, schedule_id) is None
            ):
                target = ScheduleState.ENDED.value
            updated = tx.conn.execute(
                "UPDATE ga_schedules SET title = %s, goal = %s, timing = %s, time_zone = %s, "
                "valid_until = %s, next_due_at = %s, state = %s, policy_revision = %s, "
                "creator_role = %s, revision = revision + 1, updated_at = clock_timestamp() "
                "WHERE group_id = %s AND id = %s RETURNING *",
                (
                    title if title is not None else row["title"],
                    goal if goal is not None else row["goal"],
                    Jsonb(rule.model_dump(mode="json")),
                    time_zone,
                    end,
                    following,
                    target,
                    actor.policy_revision,
                    actor.role.value
                    if row["creator_id"] == actor.member_id
                    else row["creator_role"],
                    actor.group_id,
                    schedule_id,
                ),
            ).fetchone()
            assert updated is not None
            tx._audit("schedule_updated", schedule_id, revision=updated["revision"])
            return Schedule.model_validate(updated)

    def _control_work(self, tx: UnitOfWork, schedule_id: str, *, cancel: bool) -> None:
        rows = tx.conn.execute(
            "SELECT w.* FROM ga_work_orders w JOIN ga_schedule_occurrences o "
            "ON o.group_id = w.group_id AND o.work_id = w.id "
            "WHERE o.group_id = %s AND o.schedule_id = %s "
            "AND w.state NOT IN ('completed','failed','cancelled') ORDER BY w.id FOR UPDATE OF w",
            (tx.group_id, schedule_id),
        ).fetchall()
        for work in rows:
            if not cancel and work["state"] == WorkState.PAUSED:
                continue
            tx.transition_work(
                work["id"],
                WorkState.CANCELLED if cancel else WorkState.PAUSED,
                expected_revision=work["revision"],
                reason="schedule_cancelled" if cancel else "schedule_paused",
            )

    def pause(self, actor: ActorContext, schedule_id: str, *, expected_revision: int) -> Schedule:
        return self._stop(actor, schedule_id, expected_revision=expected_revision, cancel=False)

    def cancel(self, actor: ActorContext, schedule_id: str, *, expected_revision: int) -> Schedule:
        return self._stop(actor, schedule_id, expected_revision=expected_revision, cancel=True)

    def _stop(
        self, actor: ActorContext, schedule_id: str, *, expected_revision: int, cancel: bool
    ) -> Schedule:
        with self._mutation(actor) as tx:
            row = self._row(tx, schedule_id)
            tx._controller(row, "creator_id")
            tx._revision(row, expected_revision)
            target = ScheduleState.CANCELLED if cancel else ScheduleState.PAUSED
            if row["state"] == target:
                return Schedule.model_validate(row)
            if row["state"] == ScheduleState.CANCELLED:
                raise DomainError("invalid_transition", "Cancelled schedules cannot be resumed")
            self._skip_pending(
                tx, schedule_id, "schedule_cancelled" if cancel else "schedule_paused"
            )
            self._control_work(tx, schedule_id, cancel=cancel)
            updated = tx.conn.execute(
                "UPDATE ga_schedules SET state = %s, pause_reason = %s, revision = revision + 1, "
                "updated_at = clock_timestamp() WHERE group_id = %s AND id = %s RETURNING *",
                (target.value, "user_request" if not cancel else None, actor.group_id, schedule_id),
            ).fetchone()
            assert updated is not None
            self._sync(tx, schedule_id)
            tx._audit("schedule_cancelled" if cancel else "schedule_paused", schedule_id)
            return Schedule.model_validate(updated)

    def resume(
        self,
        actor: ActorContext,
        schedule_id: str,
        *,
        expected_revision: int,
        now: datetime | None = None,
    ) -> Schedule:
        now = utc(now or datetime.now(UTC))
        with self._mutation(actor) as tx:
            row = self._row(tx, schedule_id)
            tx._controller(row, "creator_id")
            tx._revision(row, expected_revision)
            self._policy(tx, row)
            if row["state"] != ScheduleState.PAUSED:
                raise DomainError("invalid_transition", "Only paused schedules can resume")
            if now > row["valid_until"]:
                raise DomainError("invalid_transition", "Expired schedules cannot resume")
            self._quota(tx, excluding=schedule_id)
            works = tx.conn.execute(
                "SELECT w.* FROM ga_work_orders w JOIN ga_schedule_occurrences o "
                "ON o.group_id = w.group_id AND o.work_id = w.id "
                "WHERE o.group_id = %s AND o.schedule_id = %s AND w.state = 'paused' "
                "AND w.pause_reason = 'schedule_paused' ORDER BY w.id FOR UPDATE OF w",
                (actor.group_id, schedule_id),
            ).fetchall()
            for work in works:
                tx.transition_work(
                    work["id"],
                    WorkState.QUEUED,
                    expected_revision=work["revision"],
                    reason="schedule_resumed",
                )
            following = next_due(parse_timing(row["timing"]), row["time_zone"], now)
            if following is not None and following > row["valid_until"]:
                following = None
            updated = tx.conn.execute(
                "UPDATE ga_schedules SET state = %s, pause_reason = NULL, next_due_at = %s, "
                "revision = revision + 1, updated_at = clock_timestamp() "
                "WHERE group_id = %s AND id = %s RETURNING *",
                (
                    ScheduleState.ACTIVE.value if following else ScheduleState.ENDED.value,
                    following,
                    actor.group_id,
                    schedule_id,
                ),
            ).fetchone()
            assert updated is not None
            tx._audit("schedule_resumed", schedule_id)
            return Schedule.model_validate(updated)

    @staticmethod
    def _sync(tx: UnitOfWork, schedule_id: str) -> bool:
        rows = tx.conn.execute(
            "SELECT o.id, o.state, w.state AS work_state, EXISTS "
            "(SELECT 1 FROM ga_deliveries d WHERE d.group_id = w.group_id AND d.work_id = w.id "
            "AND d.state IN ('dispatching','unknown')) AS uncertain "
            "FROM ga_schedule_occurrences o JOIN ga_work_orders w "
            "ON o.group_id = w.group_id AND o.work_id = w.id "
            "WHERE o.group_id = %s AND o.schedule_id = %s AND "
            "(o.state IN ('enqueued','running','uncertain') OR "
            "w.state NOT IN ('completed','failed','cancelled')) ORDER BY o.id",
            (tx.group_id, schedule_id),
        ).fetchall()
        busy = False
        for row in rows:
            if row["uncertain"]:
                state = OccurrenceState.UNCERTAIN
            elif row["work_state"] == WorkState.COMPLETED:
                state = OccurrenceState.SUCCEEDED
            elif row["work_state"] in (WorkState.FAILED, WorkState.CANCELLED):
                state = OccurrenceState.FAILED
            elif row["work_state"] in (
                WorkState.RUNNING,
                WorkState.VALIDATING,
                WorkState.DELIVERING,
            ):
                state = OccurrenceState.RUNNING
            else:
                state = OccurrenceState.ENQUEUED
            busy = busy or state in (
                OccurrenceState.ENQUEUED,
                OccurrenceState.RUNNING,
                OccurrenceState.UNCERTAIN,
            )
            if row["state"] != state:
                tx.conn.execute(
                    "UPDATE ga_schedule_occurrences SET state = %s, revision = revision + 1, "
                    "updated_at = clock_timestamp() WHERE group_id = %s AND id = %s",
                    (state.value, tx.group_id, row["id"]),
                )
        return busy

    @staticmethod
    def _pending(tx: UnitOfWork, schedule_id: str) -> Row | None:
        return tx.conn.execute(
            "SELECT * FROM ga_schedule_occurrences WHERE group_id = %s AND schedule_id = %s "
            "AND state = 'pending' FOR UPDATE",
            (tx.group_id, schedule_id),
        ).fetchone()

    @staticmethod
    def _occurrence(
        tx: UnitOfWork,
        schedule: Row,
        scheduled_at: datetime,
        *,
        coalesced_from: datetime | None = None,
        reason: str | None = None,
        skipped: bool = False,
    ) -> Row | None:
        return tx.conn.execute(
            "INSERT INTO ga_schedule_occurrences (id, group_id, schedule_id, scheduled_at, "
            "state, coalesced_from, reason) VALUES (%s,%s,%s,%s,%s,%s,%s) "
            "ON CONFLICT (schedule_id, scheduled_at) DO NOTHING RETURNING *",
            (
                str(uuid4()),
                tx.group_id,
                schedule["id"],
                scheduled_at,
                "skipped" if skipped else "pending",
                coalesced_from,
                reason,
            ),
        ).fetchone()

    @staticmethod
    def _admit(tx: UnitOfWork, schedule: Row, occurrence: Row, now: datetime) -> Row:
        original = ActorContext(
            group_id=schedule["group_id"],
            member_id=schedule["creator_id"],
            role=Role(schedule["creator_role"]),
            policy_revision=schedule["policy_revision"],
        )
        origin_tx = UnitOfWork(tx.conn, original, tx.trace_id)
        origin_tx._check_group()
        stream_id = "schedule:" + schedule["id"]
        slot = occurrence["scheduled_at"].astimezone(UTC).isoformat()
        event = GroupEvent(
            id=str(uuid4()),
            source_key=stream_id + ":" + slot,
            group_id=original.group_id,
            actor_id=original.member_id,
            kind=EventKind.SYSTEM,
            text=schedule["goal"],
            occurred_at=occurrence["scheduled_at"],
            received_at=now,
            payload_ref="schedule-occurrence:" + occurrence["id"],
            trusted_actor_origin="authorized_schedule",
            policy_revision=original.policy_revision,
        )
        cursor = origin_tx.get_cursor(stream_id)
        ingested = origin_tx.ingest_event(event, stream_id, slot, cursor.revision if cursor else 0)
        work = origin_tx.create_work(
            schedule["goal"],
            ingested.event.id,
            scope={
                **schedule["scope"],
                "origin_schedule_id": schedule["id"],
                "origin_occurrence_id": occurrence["id"],
                "derived_depth": 1,
                "schedule_creation_allowed": False,
            },
            budget=schedule["budget"],
            acceptance_criteria=schedule["acceptance_criteria"],
        )
        # Compatible with coordinator replay results, and already decided so a
        # channel recovery scan cannot classify the scheduler's system event.
        origin_tx.record_decision(
            ingested.event.id,
            {
                "kind": "start_work",
                "schedule_id": schedule["id"],
                "occurrence_id": occurrence["id"],
                "result": {
                    "event_id": ingested.event.id,
                    "decision": {
                        "kind": "start_work",
                        "reason_code": "schedule_occurrence",
                        "goal": schedule["goal"],
                        "acceptance_criteria": schedule["acceptance_criteria"],
                    },
                    "work_id": work.id,
                    "delivery_id": None,
                    "duplicate": False,
                },
            },
        )
        updated = tx.conn.execute(
            "UPDATE ga_schedule_occurrences SET work_id = %s, state = 'enqueued', "
            "revision = revision + 1, updated_at = clock_timestamp() "
            "WHERE group_id = %s AND id = %s AND state = 'pending' RETURNING *",
            (work.id, tx.group_id, occurrence["id"]),
        ).fetchone()
        assert updated is not None
        origin_tx._audit(
            "schedule_occurrence_enqueued",
            occurrence["id"],
            schedule_id=schedule["id"],
            work_id=work.id,
            scheduled_at=slot,
        )
        return updated

    def due(
        self, actor: ActorContext, now: datetime | None = None, *, limit: int = 100
    ) -> builtins.list[ScheduleOccurrence]:
        now = utc(now or datetime.now(UTC))
        changed: builtins.list[ScheduleOccurrence] = []
        with self._mutation(actor) as tx:
            schedules = tx.conn.execute(
                "SELECT s.* FROM ga_schedules s WHERE s.group_id = %s "
                "AND (%s OR s.creator_id = %s) AND (s.state IN ('active','paused') OR "
                "(s.state IN ('ended','cancelled') "
                "AND EXISTS (SELECT 1 FROM ga_schedule_occurrences o WHERE o.group_id = s.group_id "
                "AND o.schedule_id = s.id AND o.state IN ('enqueued','running','uncertain')))) "
                "ORDER BY CASE WHEN s.state = 'active' THEN 0 ELSE 1 END, "
                "s.next_due_at NULLS LAST, s.id LIMIT %s FOR UPDATE",
                (
                    actor.group_id,
                    actor.role == Role.OWNER,
                    actor.member_id,
                    min(max(limit, 1), 1000),
                ),
            ).fetchall()
            for schedule in schedules:
                busy = self._sync(tx, schedule["id"])
                if schedule["state"] in (ScheduleState.ENDED, ScheduleState.CANCELLED):
                    continue
                rule = parse_timing(schedule["timing"])
                expired = now > schedule["valid_until"] or (
                    isinstance(rule, AtTiming) and now - rule.at > self.one_shot_grace
                )
                if expired:
                    changed.extend(self._skip_pending(tx, schedule["id"], "validity_expired"))
                    if schedule["next_due_at"] is not None:
                        missed = latest_due(
                            rule,
                            schedule["time_zone"],
                            schedule["next_due_at"],
                            min(now, schedule["valid_until"]),
                        )
                        if missed is not None:
                            skipped = self._occurrence(
                                tx,
                                schedule,
                                missed,
                                coalesced_from=schedule["next_due_at"],
                                skipped=True,
                                reason="one_shot_grace_expired"
                                if isinstance(rule, AtTiming)
                                else "validity_expired",
                            )
                            if skipped:
                                changed.append(ScheduleOccurrence.model_validate(skipped))
                    tx.conn.execute(
                        "UPDATE ga_schedules SET state = 'ended', next_due_at = NULL, "
                        "revision = revision + 1, updated_at = clock_timestamp() "
                        "WHERE group_id = %s AND id = %s",
                        (actor.group_id, schedule["id"]),
                    )
                    tx._audit("schedule_ended", schedule["id"], reason="validity_expired")
                    continue
                if schedule["state"] == ScheduleState.PAUSED:
                    continue
                if schedule["policy_revision"] != actor.policy_revision:
                    tx.conn.execute(
                        "UPDATE ga_schedules SET state = 'paused', pause_reason = 'policy_changed', "
                        "revision = revision + 1, updated_at = clock_timestamp() "
                        "WHERE group_id = %s AND id = %s",
                        (actor.group_id, schedule["id"]),
                    )
                    tx._audit("schedule_paused", schedule["id"], reason="policy_changed")
                    continue
                following = schedule["next_due_at"]
                advanced = False
                if following is not None and following <= now:
                    latest = latest_due(rule, schedule["time_zone"], following, now)
                    if latest is not None:
                        changed.extend(
                            self._skip_pending(tx, schedule["id"], "coalesced_by_newer_slot")
                        )
                        occurrence = self._occurrence(
                            tx,
                            schedule,
                            latest,
                            coalesced_from=following if latest > following else None,
                            reason="latest_only" if latest > following else None,
                        )
                        if occurrence:
                            if not busy:
                                occurrence = self._admit(tx, schedule, occurrence, now)
                                busy = True
                            changed.append(ScheduleOccurrence.model_validate(occurrence))
                    following = next_due(rule, schedule["time_zone"], now)
                    if following is not None and following > schedule["valid_until"]:
                        following = None
                    advanced = True
                pending = self._pending(tx, schedule["id"])
                if pending is not None and pending["scheduled_at"] <= now and not busy:
                    admitted = self._admit(tx, schedule, pending, now)
                    changed.append(ScheduleOccurrence.model_validate(admitted))
                    pending = None
                if advanced or (following is None and pending is None):
                    target = (
                        ScheduleState.ACTIVE
                        if following is not None or pending is not None
                        else ScheduleState.ENDED
                    )
                    tx.conn.execute(
                        "UPDATE ga_schedules SET next_due_at = %s, state = %s, revision = revision + 1, "
                        "updated_at = clock_timestamp() WHERE group_id = %s AND id = %s",
                        (following, target.value, actor.group_id, schedule["id"]),
                    )
                    tx._audit(
                        "schedule_advanced",
                        schedule["id"],
                        next_due_at=following.isoformat() if following else None,
                    )
            return changed
