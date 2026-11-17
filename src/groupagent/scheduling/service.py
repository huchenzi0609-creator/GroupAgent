"""PostgreSQL scheduler for explicit, time-bounded rules.

The trusted control process supplies current ActorContext and clock values. A
due scan performs no network or tool calls. It atomically enqueues a prepared
message in the outbox or opens a WorkOrder for the regular execution path.
"""

from __future__ import annotations

import builtins
import hashlib
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import uuid4
from zoneinfo import ZoneInfo

from psycopg.errors import LockNotAvailable, QueryCanceled
from psycopg.types.json import Jsonb
from pydantic import JsonValue

from groupagent.domain import ActorContext, DomainError, EventKind, GroupEvent, Role, WorkState
from groupagent.memory.common import memory_lock, source_rows
from groupagent.persistence import PostgresStore, UnitOfWork
from groupagent.scheduling.models import (
    AtTiming,
    EveryTiming,
    OccurrenceState,
    PreparedMessage,
    Schedule,
    ScheduleDeliveryFacts,
    ScheduleFacts,
    ScheduleKind,
    ScheduleOccurrence,
    ScheduleOccurrenceFacts,
    ScheduleState,
    Timing,
    parse_timing,
)
from groupagent.scheduling.recurrence import latest_due, next_due, utc, zone
from groupagent.scheduling.reminders import occurrence_reminder_contract
from groupagent.scheduling.temporal import (
    InstantProposal,
    ScheduleTimingProposal,
    resolve_instant,
    resolve_schedule_timing,
)
from groupagent.security import redact_credentials

Row = dict[str, Any]
VERIFIED_SCHEDULE_ORIGINS = {
    "verified_channel_metadata",
    "agentwechat_authenticated_message_metadata",
    "authenticated_audio_transcription",
}
_DELIVERY_STATES = {
    "pending": OccurrenceState.ENQUEUED,
    "dispatching": OccurrenceState.UNCERTAIN,
    "unknown": OccurrenceState.UNCERTAIN,
    "accepted": OccurrenceState.SUCCEEDED,
    "rejected": OccurrenceState.FAILED,
}


def _text(value: str, field: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise DomainError("invalid_input", f"{field} must be nonempty")


def _source_fingerprints(scope: dict[str, JsonValue]) -> dict[str, str]:
    values = scope.get("source_fingerprints", {})
    if not isinstance(values, dict) or any(not isinstance(value, str) for value in values.values()):
        raise DomainError("scope_violation", "Schedule evidence is not a fingerprint map")
    return {key: str(value) for key, value in values.items()}


def _verify_human_source(tx: UnitOfWork, event_id: str) -> GroupEvent:
    event = tx.get_event(event_id)
    if (
        event.actor_id != tx.actor.member_id
        or event.content_state != "active"
        or event.trusted_actor_origin not in VERIFIED_SCHEDULE_ORIGINS
        or event.kind not in {EventKind.TEXT, EventKind.QUOTE}
    ):
        raise DomainError("permission_denied", "A verified request from the author is required")
    source_rows(tx, [event.id], shared=True, require_complete=True)
    return event


def _prepared_payload(
    tx: UnitOfWork,
    message: PreparedMessage,
    scope: dict[str, JsonValue],
    authorization_event_id: str | None,
) -> tuple[str, ScheduleKind]:
    if authorization_event_id is None:
        raise DomainError("permission_denied", "Prepared messages require a verified request")
    event = _verify_human_source(tx, authorization_event_id)
    text = message.text
    if not text.strip() or "\x00" in text or redact_credentials(text) != text:
        raise DomainError("invalid_input", "Prepared text contains unsupported content")
    kind = ScheduleKind(message.purpose)
    if kind == ScheduleKind.EXPLICIT_REMINDER and text not in event.text:
        raise DomainError(
            "reminder_text_mismatch", "An exact reminder must quote the author's wording"
        )
    scope["prepared_message_source_id"] = event.id
    return text, kind


def _schedule_evidence(
    tx: UnitOfWork, scope: dict[str, JsonValue], *, include_legacy_sources: bool = True
) -> dict[str, str]:
    """Check durable grants; return only current evidence required by future work."""
    values = _source_fingerprints(scope)
    required = scope.get("schedule_fact_source_ids")
    if required is None:
        return values if include_legacy_sources else {}
    if not isinstance(required, list) or any(
        not isinstance(key, str) or key not in values for key in required
    ):
        raise DomainError("scope_violation", "Schedule required evidence is invalid")
    if required:
        source_rows(tx, [str(key) for key in required], shared=True, require_complete=True)
    work_sources: dict[str, str] = {}
    for key, digest in values.items():
        source = tx.get_event(key)
        if source.content_state in {"forgotten", "recalled"}:
            raise DomainError("source_unavailable", "Schedule authorization was removed")
        if source.content_state == "active" and tx.event_fingerprint(key) != digest:
            raise DomainError("source_unavailable", "Schedule evidence changed")
        if key in required:
            if source.content_state != "active":
                raise DomainError("source_unavailable", "Required schedule evidence expired")
            work_sources[key] = digest
    return work_sources


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
            self._lock_scheduler(tx)
            yield tx

    @staticmethod
    def _lock_scheduler(tx: UnitOfWork) -> None:
        tx._writer()
        memory_lock(tx)
        tx.conn.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
            ("groupagent:scheduler:" + tx.group_id,),
        )

    @contextmanager
    def _timer_transaction(
        self, actor: ActorContext, fence: Callable[[UnitOfWork], None] | None
    ) -> Iterator[UnitOfWork]:
        if self._bound_transaction is not None:
            raise ValueError("The scheduling clock requires its own transaction")
        try:
            with self.store.connect() as conn:
                conn.execute("SET LOCAL lock_timeout='40ms'")
                conn.execute("SET LOCAL statement_timeout='750ms'")
                tx = UnitOfWork(conn, actor, str(uuid4()))
                tx._check_group()
                tx._writer()
                if fence is not None:
                    fence(tx)
                yield tx
        except (LockNotAvailable, QueryCanceled):
            raise DomainError(
                "resource_deferred",
                "The scheduling clock will retry a busy database",
                retryable=True,
            ) from None

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

    def _timing(self, rule: Timing, now: datetime) -> Timing:
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

    @staticmethod
    def _check_new_instant(
        rule: Timing, now: datetime, time_zone: str, event: GroupEvent | None
    ) -> None:
        if isinstance(rule, AtTiming) and rule.at < now:
            if event is None:
                raise DomainError(
                    "invalid_input", "A new one-shot schedule cannot start in the past"
                )
            target = rule.at.astimezone(zone(time_zone)).isoformat(timespec="seconds")
            raise DomainError(
                "schedule_time_missed", f"The requested instant {target} ({time_zone}) has passed"
            )

    def create(
        self,
        actor: ActorContext,
        title: str,
        goal: str | None,
        timing: ScheduleTimingProposal | dict[str, JsonValue],
        *,
        now: datetime | None = None,
        time_zone: str = "Asia/Shanghai",
        valid_until: InstantProposal | None = None,
        request_key: str | None = None,
        scope: dict[str, JsonValue] | None = None,
        budget: dict[str, JsonValue] | None = None,
        acceptance_criteria: list[str] | None = None,
        prepared_message: PreparedMessage | None = None,
        authorization_event_id: str | None = None,
        allow_silent_result: bool = False,
    ) -> Schedule:
        now = utc(now or datetime.now(UTC))
        _text(title, "title")
        if (goal is None) == (prepared_message is None):
            raise DomainError("invalid_input", "Provide one future goal or prepared message")
        if allow_silent_result and prepared_message is not None:
            raise DomainError("invalid_input", "Only a future goal may allow a silent result")
        if allow_silent_result and authorization_event_id is None:
            raise DomainError("permission_denied", "Silent results require a verified request")
        if request_key is not None:
            _text(request_key, "request_key")
        scope = (
            dict(scope)
            if scope is not None
            else {
                "allowed_group_ids": [actor.group_id],
                "delivery_target": "origin_group",
                "external_account_changes": False,
            }
        )
        if allow_silent_result and scope.get("reminder_only") is True:
            raise DomainError("invalid_input", "Only a future goal may allow a silent result")
        scope.pop("allow_silent_result", None)
        scope.pop("allow_silent_result_source_id", None)
        scope.pop("default_one_shot_validity", None)
        with self._mutation(actor) as tx:
            if request_key is not None:
                old = tx.conn.execute(
                    "SELECT * FROM ga_schedules WHERE group_id = %s AND request_key = %s",
                    (tx.group_id, request_key),
                ).fetchone()
                if old:
                    tx._controller(old, "creator_id")
                    return Schedule.model_validate(old)
            execution_kind = ScheduleKind.GOAL
            event = None
            if authorization_event_id is not None:
                event = _verify_human_source(tx, authorization_event_id)
                fingerprints = _source_fingerprints(scope)
                fingerprints[event.id] = tx.event_fingerprint(event.id)
                scope.update(
                    request_source_id=event.id,
                    schedule_timing_source_id=event.id,
                    schedule_payload_source_id=event.id,
                    source_event_ids=list(fingerprints),
                    source_fingerprints={key: value for key, value in fingerprints.items()},
                )
                scope.setdefault("schedule_fact_source_ids", [])
                _schedule_evidence(tx, scope)
                if allow_silent_result:
                    scope["allow_silent_result"] = True
                    scope["allow_silent_result_source_id"] = event.id
            if prepared_message is not None:
                goal, execution_kind = _prepared_payload(
                    tx, prepared_message, scope, authorization_event_id
                )
            assert goal is not None
            _text(goal, "goal")
            occurred_at = event.occurred_at if event is not None else None
            source_text = event.text if event is not None else None
            rule, time_zone = resolve_schedule_timing(
                timing, occurred_at=occurred_at, time_zone=time_zone, source_text=source_text
            )
            rule = self._timing(rule, now)
            self._check_new_instant(rule, now, time_zone, event)
            if isinstance(rule, AtTiming):
                scope["default_one_shot_validity"] = valid_until is None
            if valid_until is not None:
                end = resolve_instant(
                    valid_until,
                    occurred_at=occurred_at,
                    time_zone=time_zone,
                    source_text=source_text,
                )
            else:
                end = (
                    rule.at + self.one_shot_grace
                    if isinstance(rule, AtTiming)
                    else now + self.default_validity
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
                "scope, budget, acceptance_criteria, created_at, updated_at, execution_kind) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING *",
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
                    execution_kind.value,
                ),
            ).fetchone()
            assert row is not None
            tx._audit(
                "schedule_created",
                row["id"],
                timing_kind=rule.kind,
                next_due_at=following.isoformat(),
                valid_until=end.isoformat(),
                execution_kind=execution_kind.value,
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

    def facts(
        self, actor: ActorContext, *, schedule_id: str | None = None, limit: int = 20
    ) -> builtins.list[ScheduleFacts]:
        """Read scoped rules and their occurrence and delivery evidence."""
        if actor.role not in {Role.OWNER, Role.MEMBER}:
            return []
        with self._transaction(actor) as tx:
            rows = tx.conn.execute(
                "SELECT s.*,e.occurred_at AS timing_source_at "
                "FROM ga_schedules s LEFT JOIN ga_memory_topics t "
                "ON t.group_id=s.group_id AND t.id=s.scope->>'topic_id' "
                "LEFT JOIN ga_events e ON e.group_id=s.group_id "
                "AND e.id=s.scope->>'schedule_timing_source_id' AND e.memory_state='active' "
                "WHERE s.group_id=%s AND (%s OR s.creator_id=%s) "
                "AND (%s::text IS NULL OR s.id=%s) "
                "AND (s.scope->>'topic_id' IS NULL OR t.visibility='group') "
                "ORDER BY (s.state IN ('active','paused')) DESC, s.updated_at DESC,s.id LIMIT %s",
                (
                    tx.group_id,
                    actor.role == Role.OWNER,
                    actor.member_id,
                    schedule_id,
                    schedule_id,
                    min(max(limit, 1), 20),
                ),
            ).fetchall()
            result = []
            for row in rows:
                timing_source_at = row.pop("timing_source_at")
                rule = Schedule.model_validate(row)
                time_zone = zone(rule.time_zone)
                history = tx.conn.execute(
                    "SELECT o.id,o.scheduled_at,o.state,o.reason,o.work_id,o.delivery_id "
                    "FROM ga_schedule_occurrences o WHERE o.group_id=%s AND o.schedule_id=%s "
                    "ORDER BY o.scheduled_at DESC,o.id LIMIT 5",
                    (tx.group_id, rule.id),
                ).fetchall()
                occurrences = []
                for occurrence in history:
                    projection = self._occurrence_facts(tx, occurrence, time_zone)
                    occurrences.append(projection)
                result.append(
                    ScheduleFacts(
                        id=rule.id,
                        title=rule.title,
                        goal=rule.goal,
                        state=rule.state,
                        execution_kind=rule.execution_kind,
                        revision=rule.revision,
                        timing=rule.timing,
                        time_zone=rule.time_zone,
                        valid_until=rule.valid_until,
                        next_due_at=rule.next_due_at,
                        next_due_local=(
                            rule.next_due_at.astimezone(time_zone).isoformat()
                            if rule.next_due_at is not None
                            else None
                        ),
                        timing_source_local=(
                            timing_source_at.astimezone(time_zone).isoformat()
                            if timing_source_at is not None
                            else None
                        ),
                        pause_reason=rule.pause_reason,
                        owned_by_requester=rule.creator_id == actor.member_id,
                        allow_silent_result=rule.scope.get("allow_silent_result") is True,
                        occurrences=occurrences,
                    )
                )
            return result

    @staticmethod
    def _occurrence_facts(
        tx: UnitOfWork, occurrence: Row, time_zone: ZoneInfo
    ) -> ScheduleOccurrenceFacts:
        work_id, delivery_id = occurrence["work_id"], occurrence["delivery_id"]
        state = OccurrenceState(occurrence["state"])
        deliveries = []
        if work_id or delivery_id:
            rows = tx.conn.execute(
                "SELECT id,state,reason,accepted_at,platform_message_id,platform_job_id "
                "FROM ga_deliveries WHERE group_id=%s AND (work_id=%s OR id=%s) "
                "ORDER BY created_at DESC,id LIMIT 10",
                (tx.group_id, work_id, delivery_id),
            ).fetchall()
            deliveries = [ScheduleDeliveryFacts.model_validate(row) for row in rows]
            if delivery_id and deliveries:
                state = _DELIVERY_STATES[deliveries[0].state.value]
            elif work_id:
                work = tx.get_work(work_id)
                state = Scheduler._work_occurrence_state(
                    work.state,
                    any(item.state.value in {"dispatching", "unknown"} for item in deliveries),
                )
        return ScheduleOccurrenceFacts(
            id=occurrence["id"],
            scheduled_at=occurrence["scheduled_at"],
            scheduled_local=occurrence["scheduled_at"].astimezone(time_zone).isoformat(),
            state=state,
            reason=occurrence["reason"],
            work_id=work_id,
            deliveries=deliveries,
        )

    @staticmethod
    def _skip_pending(
        tx: UnitOfWork, schedule_id: str, reason: str, *, except_slot: datetime | None = None
    ) -> builtins.list[ScheduleOccurrence]:
        rows = tx.conn.execute(
            "UPDATE ga_schedule_occurrences SET state = 'skipped', reason = %s, "
            "revision = revision + 1, updated_at = clock_timestamp() "
            "WHERE group_id = %s AND schedule_id = %s AND state = 'pending' "
            "AND scheduled_at IS DISTINCT FROM %s::timestamptz RETURNING *",
            (reason, tx.group_id, schedule_id, except_slot),
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
        timing: ScheduleTimingProposal | dict[str, JsonValue] | None = None,
        time_zone: str | None = None,
        valid_until: InstantProposal | None = None,
        now: datetime | None = None,
        prepared_message: PreparedMessage | None = None,
        authorization_event_id: str | None = None,
        fact_source_ids: builtins.list[str] | None = None,
        allow_silent_result: bool | None = None,
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
                if prepared_message is not None:
                    raise DomainError(
                        "invalid_input", "Provide one future goal or prepared message"
                    )
            scope = dict(row["scope"])
            execution_kind = ScheduleKind(row["execution_kind"])
            replaces_payload = goal is not None or prepared_message is not None
            if replaces_payload and (
                authorization_event_id is not None
                or execution_kind != ScheduleKind.GOAL
                or goal != row["goal"]
            ):
                execution_kind = ScheduleKind.GOAL
                scope.pop("reminder_only", None)
                scope.pop("reminder_body", None)
                scope.pop("reminder_body_literal", None)
                scope.pop("reminder_body_source_event_id", None)
                scope.pop("prepared_message_source_id", None)
            if prepared_message is not None:
                if (
                    prepared_message.purpose == "explicit_reminder"
                    and row["creator_id"] != actor.member_id
                ):
                    raise DomainError(
                        "permission_denied", "Only the creator may grant their reminder"
                    )
                goal, execution_kind = _prepared_payload(
                    tx, prepared_message, scope, authorization_event_id
                )
            if allow_silent_result is True:
                if execution_kind != ScheduleKind.GOAL or scope.get("reminder_only") is True:
                    raise DomainError(
                        "invalid_input", "Only a future goal may allow a silent result"
                    )
                if authorization_event_id is None:
                    raise DomainError(
                        "permission_denied", "Silent results require a verified request"
                    )
                scope["allow_silent_result"] = True
                scope["allow_silent_result_source_id"] = authorization_event_id
            elif allow_silent_result is False or prepared_message is not None:
                scope.pop("allow_silent_result", None)
                scope.pop("allow_silent_result_source_id", None)
            event = None
            if authorization_event_id is not None:
                event = _verify_human_source(tx, authorization_event_id)
                fingerprints = _source_fingerprints(scope)
                was_modern = "schedule_fact_source_ids" in row["scope"]
                if replaces_payload and not was_modern:
                    original_request_key = row["request_key"] or ""
                    if original_request_key.startswith("chat:"):
                        original_id = original_request_key.removeprefix("chat:")
                        if original_id in fingerprints:
                            scope.setdefault("request_source_id", original_id)
                            scope.setdefault("schedule_timing_source_id", original_id)
                if fact_source_ids is not None:
                    if fact_source_ids:
                        source_rows(tx, fact_source_ids, shared=True, require_complete=True)
                    for key in fact_source_ids:
                        fingerprints[key] = tx.event_fingerprint(key)
                    scope["schedule_fact_source_ids"] = list(fact_source_ids)
                elif replaces_payload:
                    scope["schedule_fact_source_ids"] = []
                fingerprints[event.id] = tx.event_fingerprint(event.id)
                if timing is not None or time_zone is not None or valid_until is not None:
                    scope["schedule_timing_source_id"] = event.id
                if replaces_payload:
                    scope["schedule_payload_source_id"] = event.id
                if title is not None:
                    scope["schedule_title_source_id"] = event.id
                # Historical occurrences keep their frozen references.
                if was_modern or replaces_payload:
                    keep = {
                        scope.get("request_source_id"),
                        scope.get("schedule_timing_source_id"),
                        scope.get("schedule_payload_source_id"),
                        scope.get("schedule_title_source_id"),
                        scope.get("allow_silent_result_source_id"),
                        *scope.get("schedule_fact_source_ids", []),
                    }
                    fingerprints = {
                        key: digest for key, digest in fingerprints.items() if key in keep
                    }
                scope.update(source_event_ids=list(fingerprints), source_fingerprints=fingerprints)
                _schedule_evidence(tx, scope)
            reference = parse_timing(row["timing"])
            occurred_at = event.occurred_at if event is not None else None
            source_text = event.text if event is not None else None
            rule, time_zone = resolve_schedule_timing(
                timing if timing is not None else reference,
                occurred_at=occurred_at,
                time_zone=time_zone if time_zone is not None else row["time_zone"],
                reference_timing=reference,
                source_text=source_text,
            )
            rule = self._timing(rule, now)
            if timing is not None and event is not None:
                self._check_new_instant(rule, now, time_zone, event)
            end = row["valid_until"]
            if valid_until is not None:
                end = resolve_instant(
                    valid_until,
                    occurred_at=occurred_at,
                    time_zone=time_zone,
                    source_text=source_text,
                )
                scope["default_one_shot_validity"] = False
            elif (
                isinstance(rule, AtTiming)
                and isinstance(reference, AtTiming)
                and timing is not None
                and scope.get(
                    "default_one_shot_validity", end == reference.at + self.one_shot_grace
                )
            ):
                end = rule.at + self.one_shot_grace
                scope["default_one_shot_validity"] = True
            if not isinstance(rule, AtTiming):
                scope.pop("default_one_shot_validity", None)
            if end < now:
                raise DomainError("invalid_input", "The new validity must not already have expired")
            changed_timing = timing is not None or time_zone != row["time_zone"]
            following = (
                next_due(rule, time_zone, now, inclusive=isinstance(rule, AtTiming))
                if changed_timing
                else row["next_due_at"]
            )
            if following is not None and following > end:
                if timing is not None and event is not None:
                    raise DomainError(
                        "invalid_input", "No occurrence fits within the authorized validity"
                    )
                following = None
            if changed_timing:
                self._skip_pending(tx, schedule_id, "rule_updated")
                if rule != reference or time_zone != row["time_zone"]:
                    self._retire_prepared(tx, schedule_id, "rule_updated")
                    self._sync(tx, schedule_id)
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
                "creator_role = %s, scope = %s, execution_kind = %s, "
                "revision = revision + 1, updated_at = clock_timestamp() "
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
                    Jsonb(scope),
                    execution_kind.value,
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

    @staticmethod
    def _retire_prepared(tx: UnitOfWork, schedule_id: str, reason: str) -> None:
        from groupagent.conversation.policy import retire_delivery_in_tx

        for row in tx.conn.execute(
            "SELECT d.id FROM ga_schedule_occurrences o JOIN ga_deliveries d "
            "ON d.group_id=o.group_id AND d.id=o.delivery_id "
            "WHERE o.group_id=%s AND o.schedule_id=%s "
            "AND d.state IN ('pending','dispatching','unknown') ORDER BY d.id",
            (tx.group_id, schedule_id),
        ).fetchall():
            retire_delivery_in_tx(tx, row["id"], reason=reason)

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
            if cancel:
                self._retire_prepared(tx, schedule_id, "schedule_cancelled")
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
    def _work_occurrence_state(work_state: str, uncertain: bool) -> OccurrenceState:
        if uncertain:
            return OccurrenceState.UNCERTAIN
        if work_state == WorkState.COMPLETED:
            return OccurrenceState.SUCCEEDED
        if work_state in {WorkState.FAILED, WorkState.CANCELLED}:
            return OccurrenceState.FAILED
        if work_state in {WorkState.RUNNING, WorkState.VALIDATING, WorkState.DELIVERING}:
            return OccurrenceState.RUNNING
        return OccurrenceState.ENQUEUED

    @staticmethod
    def _sync(tx: UnitOfWork, schedule_id: str) -> bool:
        rows = tx.conn.execute(
            "SELECT o.id, o.state, w.state AS work_state, d.state AS delivery_state, EXISTS "
            "(SELECT 1 FROM ga_deliveries d WHERE d.group_id = w.group_id AND d.work_id = w.id "
            "AND d.state IN ('dispatching','unknown')) AS uncertain "
            "FROM ga_schedule_occurrences o LEFT JOIN ga_work_orders w "
            "ON o.group_id = w.group_id AND o.work_id = w.id "
            "LEFT JOIN ga_deliveries d ON o.group_id=d.group_id AND o.delivery_id=d.id "
            "WHERE o.group_id = %s AND o.schedule_id = %s AND "
            "(o.state IN ('enqueued','running','uncertain') OR "
            "w.state NOT IN ('completed','failed','cancelled')) ORDER BY o.id",
            (tx.group_id, schedule_id),
        ).fetchall()
        busy = False
        for row in rows:
            if row["delivery_state"] is not None:
                state = _DELIVERY_STATES[row["delivery_state"]]
            else:
                state = Scheduler._work_occurrence_state(row["work_state"], row["uncertain"])
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
    def _coalesce_quiet_delivery(tx: UnitOfWork, schedule_id: str) -> bool:
        """A newer slot replaces a prior held send only if no native submit started."""
        rows = tx.conn.execute(
            "SELECT d.id FROM ga_schedule_occurrences o JOIN ga_deliveries d "
            "ON d.group_id=o.group_id AND d.id=o.delivery_id "
            "WHERE o.group_id=%s AND o.schedule_id=%s AND d.state='pending' "
            "AND d.reason IN ('quiet_hold','group_quiet_hold','group_paused','group_disabled') AND NOT EXISTS "
            "(SELECT 1 FROM ga_delivery_parts p WHERE p.group_id=d.group_id "
            "AND p.intent_id=d.id AND p.state<>'pending') ORDER BY d.id FOR UPDATE OF d",
            (tx.group_id, schedule_id),
        ).fetchall()
        if rows:
            from groupagent.conversation.policy import retire_delivery_in_tx

            for row in rows:
                retire_delivery_in_tx(tx, row["id"], reason="coalesced_by_newer_slot")
        return bool(rows)

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
    def _execution_kind(tx: UnitOfWork, schedule: Row) -> ScheduleKind:
        kind = ScheduleKind(schedule["execution_kind"])
        scope = schedule["scope"]
        if kind != ScheduleKind.GOAL or scope.get("reminder_body_literal") is not True:
            return kind
        source_id = scope.get("reminder_body_source_event_id")
        fingerprints = _source_fingerprints(scope)
        if not isinstance(source_id, str) or source_id not in fingerprints:
            return kind
        source = tx.get_event(source_id)
        if source.content_state in {"forgotten", "recalled"}:
            raise DomainError("source_unavailable", "Legacy reminder authorization was removed")
        if (
            source.actor_id == schedule["creator_id"]
            and source.trusted_actor_origin in VERIFIED_SCHEDULE_ORIGINS
            and redact_credentials(schedule["goal"]) == schedule["goal"]
            and (
                source.content_state == "expired"
                or (
                    schedule["goal"] in source.text
                    and tx.event_fingerprint(source_id) == fingerprints[source_id]
                )
            )
        ):
            return ScheduleKind.EXPLICIT_REMINDER
        return kind

    @staticmethod
    def _admit_goal(
        origin_tx: UnitOfWork,
        schedule: Row,
        occurrence: Row,
        now: datetime,
        source_fingerprints: dict[str, str],
    ) -> str:
        original = origin_tx.actor
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
        fingerprints = {
            **source_fingerprints,
            ingested.event.id: origin_tx.event_fingerprint(ingested.event.id),
        }
        reminder_scope, criteria = occurrence_reminder_contract(
            schedule["goal"], schedule["scope"], schedule["acceptance_criteria"]
        )
        work = origin_tx.create_work(
            schedule["goal"],
            ingested.event.id,
            scope={
                **reminder_scope,
                "origin_schedule_id": schedule["id"],
                "origin_occurrence_id": occurrence["id"],
                "derived_depth": 1,
                "schedule_creation_allowed": False,
                "source_fingerprints": {key: digest for key, digest in fingerprints.items()},
            },
            budget=schedule["budget"],
            acceptance_criteria=criteria,
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
                        "acceptance_criteria": [value for value in criteria],
                    },
                    "work_id": work.id,
                    "delivery_id": None,
                    "duplicate": False,
                },
            },
        )
        return work.id

    def _admit(self, tx: UnitOfWork, schedule: Row, occurrence: Row, now: datetime) -> Row:
        original = ActorContext(
            group_id=schedule["group_id"],
            member_id=schedule["creator_id"],
            role=Role(schedule["creator_role"]),
            policy_revision=schedule["policy_revision"],
        )
        origin_tx = UnitOfWork(tx.conn, original, tx.trace_id)
        work_id = delivery_id = delivery_purpose = reason = None
        delivery_sources: list[str] = []
        try:
            with tx.conn.transaction():
                origin_tx._check_group()
                kind = self._execution_kind(origin_tx, schedule)
                sources = _schedule_evidence(
                    origin_tx, schedule["scope"], include_legacy_sources=kind == ScheduleKind.GOAL
                )
                if kind == ScheduleKind.GOAL:
                    work_id = self._admit_goal(origin_tx, schedule, occurrence, now, sources)
                else:
                    text = schedule["goal"]
                    delivery = origin_tx.enqueue_delivery(
                        None,
                        "text:" + text,
                        hashlib.sha256(text.encode("utf-8")).hexdigest(),
                        "schedule-occurrence:" + occurrence["id"],
                    )
                    delivery_id, delivery_purpose = delivery.id, kind.value
                    scope = schedule["scope"]
                    if "schedule_fact_source_ids" in scope or "prepared_message_source_id" in scope:
                        delivery_sources = sorted(_source_fingerprints(scope))
                    else:
                        body_source = scope.get("reminder_body_source_event_id")
                        created_by = schedule["request_key"] or ""
                        delivery_sources = sorted(
                            {body_source, created_by.removeprefix("chat:")}
                            if created_by.startswith("chat:")
                            else {body_source}
                        )
        except DomainError as error:
            if error.code not in {
                "source_unavailable",
                "not_found",
                "member_left",
                "permission_denied",
            }:
                raise
            reason = error.code
        updated = tx.conn.execute(
            "UPDATE ga_schedule_occurrences SET work_id=%s, delivery_id=%s, delivery_purpose=%s, "
            "delivery_sources=%s, "
            "state=%s, reason=COALESCE(%s,reason), "
            "revision = revision + 1, updated_at = clock_timestamp() "
            "WHERE group_id = %s AND id = %s AND state = 'pending' RETURNING *",
            (
                work_id,
                delivery_id,
                delivery_purpose,
                delivery_sources,
                "skipped" if reason else "enqueued",
                reason,
                tx.group_id,
                occurrence["id"],
            ),
        ).fetchone()
        assert updated is not None
        tx._audit(
            "schedule_occurrence_skipped" if reason else "schedule_occurrence_enqueued",
            occurrence["id"],
            schedule_id=schedule["id"],
            work_id=work_id,
            delivery_id=delivery_id,
            scheduled_at=occurrence["scheduled_at"].astimezone(UTC).isoformat(),
            reason=reason,
        )
        return updated

    def due(
        self, actor: ActorContext, now: datetime | None = None, *, limit: int = 100
    ) -> builtins.list[ScheduleOccurrence]:
        now = utc(now or datetime.now(UTC))
        with self._mutation(actor) as tx:
            return self._scan(tx, now, limit, due_only=False)

    _DUE_PREDICATE = (
        "((s.state='active' AND s.next_due_at<=%(now)s) "
        "OR (s.state IN ('active','paused') AND s.valid_until<%(now)s) "
        "OR EXISTS (SELECT 1 FROM ga_schedule_occurrences o "
        "WHERE o.group_id=s.group_id AND o.schedule_id=s.id AND "
        "((s.state='active' AND o.state='pending' AND o.scheduled_at<=%(now)s) "
        "OR o.state IN ('enqueued','running','uncertain'))))"
    )

    def wake(
        self,
        actor: ActorContext,
        now: datetime | None = None,
        *,
        limit: int = 20,
        fence: Callable[[UnitOfWork], None] | None = None,
    ) -> builtins.list[ScheduleOccurrence]:
        """Use an independent, bounded database wakeup from the trusted clock."""
        now = utc(now or datetime.now(UTC))
        with self._timer_transaction(actor, fence) as tx:
            ready = tx.conn.execute(
                "SELECT 1 FROM ga_schedules s WHERE s.group_id=%(group)s "
                "AND (%(owner)s OR s.creator_id=%(creator)s) AND "
                + self._DUE_PREDICATE
                + " LIMIT 1",
                {
                    "group": tx.group_id,
                    "owner": actor.role == Role.OWNER,
                    "creator": actor.member_id,
                    "now": now,
                },
            ).fetchone()
            if ready is None:
                return []
            self._lock_scheduler(tx)
            return self._scan(tx, now, min(limit, 20), due_only=True)

    def next_wake(
        self, actor: ActorContext, *, fence: Callable[[UnitOfWork], None] | None = None
    ) -> datetime | None:
        """Nearest known wall-clock slot; periodic polling also checks pending work."""
        with self._timer_transaction(actor, fence) as tx:
            row = tx.conn.execute(
                "SELECT min(CASE WHEN EXISTS (SELECT 1 FROM ga_schedule_occurrences o "
                "WHERE o.group_id=s.group_id AND o.schedule_id=s.id AND o.state='pending') "
                "THEN least(s.next_due_at,clock_timestamp()) ELSE s.next_due_at END) AS next_due_at "
                "FROM ga_schedules s WHERE s.group_id=%s AND s.state='active' "
                "AND (%s OR s.creator_id=%s)",
                (tx.group_id, actor.role == Role.OWNER, actor.member_id),
            ).fetchone()
            return row["next_due_at"] if row else None

    def _scan(
        self, tx: UnitOfWork, now: datetime, limit: int, *, due_only: bool
    ) -> builtins.list[ScheduleOccurrence]:
        actor = tx.actor
        changed: builtins.list[ScheduleOccurrence] = []
        schedules = tx.conn.execute(
            "SELECT s.* FROM ga_schedules s WHERE s.group_id = %(group)s "
            "AND (%(owner)s OR s.creator_id = %(creator)s) AND (s.state IN ('active','paused') OR "
            "(s.state IN ('ended','cancelled') "
            "AND EXISTS (SELECT 1 FROM ga_schedule_occurrences o WHERE o.group_id = s.group_id "
            "AND o.schedule_id = s.id AND o.state IN ('enqueued','running','uncertain')))) "
            + ("AND " + self._DUE_PREDICATE + " " if due_only else "")
            + "ORDER BY CASE WHEN s.next_due_at<=%(now)s AND s.state='active' THEN 0 ELSE 1 END, "
            "s.next_due_at NULLS LAST, s.id LIMIT %(limit)s FOR UPDATE",
            {
                "group": actor.group_id,
                "owner": actor.role == Role.OWNER,
                "creator": actor.member_id,
                "now": now,
                "limit": min(max(limit, 1), 1000),
            },
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
                        self._skip_pending(
                            tx, schedule["id"], "coalesced_by_newer_slot", except_slot=latest
                        )
                    )
                    occurrence = self._occurrence(
                        tx,
                        schedule,
                        latest,
                        coalesced_from=following if latest > following else None,
                        reason="latest_only" if latest > following else None,
                    )
                    if occurrence:
                        if busy and self._coalesce_quiet_delivery(tx, schedule["id"]):
                            busy = self._sync(tx, schedule["id"])
                        if not busy:
                            occurrence = self._admit(tx, schedule, occurrence, now)
                            busy = occurrence["state"] == OccurrenceState.ENQUEUED
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
