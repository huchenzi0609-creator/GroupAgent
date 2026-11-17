"""Finite scheduling tools bound to one trusted source work and current lease.

No listener code, cron, member identity, source locator or delivery destination is
accepted as tool input. Durable receipts contain IDs/state, never copied goals.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import Any, Literal

from pydantic import AwareDatetime, Field, JsonValue, model_validator

from groupagent.domain import ActorContext, DomainError, ExecutionLease, Role, WorkOrder
from groupagent.memory.common import memory_lock, source_rows
from groupagent.persistence import PostgresStore, UnitOfWork
from groupagent.scheduling import Scheduler
from groupagent.scheduling.models import PreparedMessage, Schedule
from groupagent.scheduling.temporal import (
    CreateTimingProposal,
    InstantProposal,
    ScheduleTimingProposal,
    resolve_instant,
)
from groupagent.scheduling.triggers import EventTriggerService, TriggerRule, TriggerType
from groupagent.tools import IdempotencyMode, ToolContext, ToolData, ToolDefinition, ToolGateway
from groupagent.tools.models import ToolFailure


class ListRules(ToolData):
    kind: Literal["time", "event", "all"] = "all"
    limit: int = Field(default=20, ge=1, le=50, strict=True)


class CreateSchedule(ToolData):
    title: str = Field(min_length=1, max_length=200)
    goal: str | None = Field(default=None, min_length=1, max_length=4096)
    prepared_message: PreparedMessage | None = None
    fact_source_ids: list[str] = Field(default_factory=list, max_length=20)
    allow_silent_result: bool = Field(
        default=False,
        strict=True,
        description="Enable only if the requester explicitly allows future work to finish silently "
        "when there is nothing to report.",
    )
    timing: CreateTimingProposal
    time_zone: str = Field(default="Asia/Shanghai", min_length=1, max_length=100)
    valid_until: InstantProposal | None = None

    @model_validator(mode="after")
    def single_payload(self) -> CreateSchedule:
        if (self.goal is None) == (self.prepared_message is None):
            raise ValueError("Provide one future goal or prepared message")
        if self.allow_silent_result and self.prepared_message is not None:
            raise ValueError("Only a future goal may allow a silent result")
        return self


class UpdateSchedule(ToolData):
    schedule_id: str = Field(min_length=1, max_length=200)
    expected_revision: int = Field(ge=1, strict=True)
    title: str | None = Field(default=None, min_length=1, max_length=200)
    goal: str | None = Field(default=None, min_length=1, max_length=4096)
    prepared_message: PreparedMessage | None = None
    fact_source_ids: list[str] | None = Field(default=None, max_length=20)
    allow_silent_result: bool | None = Field(
        default=None,
        strict=True,
        description="Omit to keep the existing setting. Set false to require a delivered result.",
    )
    timing: ScheduleTimingProposal | None = None
    time_zone: str | None = Field(default=None, min_length=1, max_length=100)
    valid_until: InstantProposal | None = None


class ControlRule(ToolData):
    rule_id: str = Field(min_length=1, max_length=200)
    expected_revision: int = Field(ge=1, strict=True)
    command: Literal["pause", "resume", "cancel"]


class CreateTrigger(ToolData):
    event_type: TriggerType
    resource_id: str = Field(min_length=1, max_length=4096)
    goal: str = Field(min_length=1, max_length=4096)
    max_occurrences: int = Field(default=1, ge=1, le=10, strict=True)
    expires_at: AwareDatetime | None = None


class RuleReceipt(ToolData):
    kind: Literal["time", "event"]
    id: str
    revision: int
    state: str
    next_due_at: AwareDatetime | None = None
    time_zone: str | None = None
    valid_until: AwareDatetime
    recorded_at: AwareDatetime
    replayed: bool = False
    allow_silent_result: bool | None = None


class RuleList(ToolData):
    items: tuple[dict[str, JsonValue], ...]
    untrusted_content: Literal[True] = True


def _ttl(value: datetime | None, now: datetime) -> datetime:
    end = value.astimezone(UTC) if value is not None else now + timedelta(days=30)
    if not now < end <= now + timedelta(days=30):
        raise DomainError("invalid_ttl", "Tool-created follow-ups must expire within thirty days")
    return end


def _receipt(value: Schedule | TriggerRule, now: datetime) -> RuleReceipt:
    return RuleReceipt(
        kind="time" if isinstance(value, Schedule) else "event",
        id=value.id,
        revision=value.revision,
        state=value.state,
        next_due_at=value.next_due_at if isinstance(value, Schedule) else None,
        time_zone=value.time_zone if isinstance(value, Schedule) else None,
        valid_until=value.valid_until if isinstance(value, Schedule) else value.expires_at,
        recorded_at=now,
        allow_silent_result=value.scope.get("allow_silent_result") is True
        if isinstance(value, Schedule)
        else None,
    )


def _rule_sources(tx: UnitOfWork, value: Schedule | TriggerRule) -> None:
    if value.creator_id != tx.actor.member_id and tx.actor.role != Role.OWNER:
        raise DomainError("permission_denied", "This rule is outside the requester scope")
    topic = value.scope.get("topic_id")
    if isinstance(topic, str):
        row = tx.conn.execute(
            "SELECT visibility FROM ga_memory_topics WHERE group_id=%s AND id=%s",
            (tx.group_id, topic),
        ).fetchone()
        if row is None or row["visibility"] != "group":
            raise DomainError(
                "scope_violation", "Private-topic rules are not exposed by these tools"
            )
    if isinstance(value, Schedule) and "schedule_fact_source_ids" in value.scope:
        # Timer grants survive expiry of the original chat text. A tool control
        # requires its new human request and the current rule revision.
        tx._scope(value.scope)
        return
    fingerprints = value.scope.get("source_fingerprints", {})
    if not isinstance(fingerprints, dict):
        raise DomainError("source_unavailable", "Rule source scope is invalid")
    if fingerprints:
        source_rows(tx, list(fingerprints), shared=True, require_complete=True)
        if any(tx.event_fingerprint(key) != digest for key, digest in fingerprints.items()):
            raise DomainError("source_unavailable", "Rule source content changed")
    tx._scope(value.scope)


def register_scheduling_tools(
    gateway: ToolGateway,
    store: PostgresStore,
    scheduler: Scheduler,
    triggers: EventTriggerService,
    work: WorkOrder,
    lease: ExecutionLease,
    actor: ActorContext,
) -> None:
    """Register six guarded tools; callers still explicitly grant their names."""
    if (work.group_id, work.id, work.requester_id, work.goal_revision) != (
        actor.group_id,
        lease.work_id,
        actor.member_id,
        lease.goal_revision,
    ) or lease.group_id != actor.group_id:
        raise DomainError(
            "scope_violation", "Scheduling tools require the exact requester work lease"
        )

    def guard(tx: UnitOfWork, context: ToolContext) -> WorkOrder:
        if (
            context.actor != actor
            or context.group_id != work.group_id
            or context.work_id != work.id
        ):
            raise DomainError(
                "scope_violation", "Tool context cannot replace source-work authority"
            )
        if context.cancelled.is_set():
            raise DomainError("cancelled", "Scheduling action was cancelled")
        tx._writer()
        memory_lock(tx)
        tx.conn.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended(%s,0))",
            ("groupagent:scheduler:" + actor.group_id,),
        )
        tx.assert_lease(lease)
        current = tx.get_work(work.id)
        depth = current.scope.get("derived_depth", 0)
        if (
            type(depth) is not int
            or depth != 0
            or current.scope.get("schedule_creation_allowed") is not True
            or any(
                current.scope.get(key) for key in ("origin_schedule_id", "origin_trigger_rule_id")
            )
        ):
            raise DomainError(
                "derivation_limit", "Only explicitly authorized human-source work may manage rules"
            )
        source = tx.get_event(current.request_event_id)
        if (
            source.actor_id != actor.member_id
            or source.trusted_actor_origin
            not in {
                "verified_channel_metadata",
                "agentwechat_authenticated_message_metadata",
                "authenticated_audio_transcription",
            }
            or source.kind.value in {"system", "unsupported_video"}
        ):
            raise DomainError(
                "permission_denied", "A human source request must authorize follow-ups"
            )
        fingerprints = current.scope.get("source_fingerprints", {})
        if not isinstance(fingerprints, dict):
            raise DomainError("source_unavailable", "Source-work evidence is invalid")
        sources = sorted({current.request_event_id, *fingerprints})
        source_rows(tx, sources, shared=True, require_complete=True)
        if any(tx.event_fingerprint(key) != digest for key, digest in fingerprints.items()):
            raise DomainError("source_unavailable", "Source-work evidence changed")
        tx._scope(current.scope)
        return current

    def mutation(
        name: str,
        value: ToolData,
        context: ToolContext,
        action: Callable[[UnitOfWork, WorkOrder, datetime, str], RuleReceipt],
    ) -> RuleReceipt:
        fingerprint = hashlib.sha256(
            json.dumps(
                [name, lease.goal_revision, value.model_dump(mode="json")],
                sort_keys=True,
                separators=(",", ":"),
            ).encode()
        ).hexdigest()
        with store.transaction(actor) as tx:
            current = guard(tx, context)
            old = tx.conn.execute(
                "SELECT details FROM ga_audit_events WHERE group_id=%s AND resource_ref=%s "
                "AND action='tool_scheduling_receipt' AND details->>'idempotency_key'=%s "
                "ORDER BY id DESC LIMIT 1",
                (actor.group_id, work.id, context.idempotency_key),
            ).fetchone()
            if old:
                if old["details"]["fingerprint"] != fingerprint:
                    raise DomainError(
                        "idempotency_conflict", "A scheduling key cannot change its arguments"
                    )
                return RuleReceipt.model_validate(old["details"]["receipt"]).model_copy(
                    update={"replayed": True}
                )
            now = datetime.now(UTC)
            key = (
                "tool-followup:"
                + hashlib.sha256(
                    json.dumps([actor.group_id, work.id, context.idempotency_key]).encode()
                ).hexdigest()
            )
            result = action(tx, current, now, key)
            tx.assert_lease(lease)
            if context.cancelled.is_set():
                raise DomainError("cancelled", "Scheduling action was cancelled before commit")
            tx._audit(
                "tool_scheduling_receipt",
                work.id,
                idempotency_key=context.idempotency_key,
                fingerprint=fingerprint,
                receipt=result.model_dump(mode="json"),
            )
            return result

    async def mutate(
        name: str,
        value: ToolData,
        context: ToolContext,
        action: Callable[[UnitOfWork, WorkOrder, datetime, str], RuleReceipt],
    ) -> RuleReceipt:
        try:
            return await asyncio.to_thread(mutation, name, value, context, action)
        except DomainError as error:
            # The transaction context has rolled back every business write.
            # A connection/commit loss is not a DomainError and remains UNKNOWN.
            raise ToolFailure(error.code, error.message, no_effect=True) from None

    async def create_schedule(value: CreateSchedule, context: ToolContext) -> RuleReceipt:
        def action(tx: UnitOfWork, current: WorkOrder, now: datetime, key: str) -> RuleReceipt:
            raw = current.scope.get("source_fingerprints", {})
            assert isinstance(raw, dict)
            if len(set(value.fact_source_ids)) != len(value.fact_source_ids) or not set(
                value.fact_source_ids
            ) <= set(raw):
                raise DomainError("source_unavailable", "Schedule evidence was not in this work")
            end = (
                resolve_instant(
                    value.valid_until,
                    occurred_at=tx.get_event(current.request_event_id).occurred_at,
                    time_zone=value.time_zone,
                )
                if value.valid_until is not None
                else None
            )
            result = scheduler.in_tx(tx).create(
                actor,
                value.title,
                value.goal,
                value.timing,
                now=now,
                time_zone=value.time_zone,
                valid_until=_ttl(end, now),
                request_key=key,
                budget=current.budget,
                prepared_message=value.prepared_message,
                authorization_event_id=current.request_event_id,
                allow_silent_result=value.allow_silent_result,
                scope={
                    "allowed_group_ids": [actor.group_id],
                    "delivery_target": "origin_group",
                    "external_account_changes": False,
                    "source_fingerprints": {key: raw[key] for key in value.fact_source_ids},
                    "schedule_fact_source_ids": [
                        identifier for identifier in value.fact_source_ids
                    ],
                    "origin_source_work_id": current.id,
                    "derived_depth": 1,
                    "schedule_creation_allowed": False,
                },
            )
            return _receipt(result, now)

        return await mutate("schedule_create", value, context, action)

    async def update_schedule(value: UpdateSchedule, context: ToolContext) -> RuleReceipt:
        def action(tx: UnitOfWork, current: WorkOrder, now: datetime, key: str) -> RuleReceipt:
            service = scheduler.in_tx(tx)
            original = service.get(actor, value.schedule_id)
            _rule_sources(tx, original)
            raw = current.scope.get("source_fingerprints", {})
            assert isinstance(raw, dict)
            sources = value.fact_source_ids
            if sources is not None and (
                len(set(sources)) != len(sources) or not set(sources) <= set(raw)
            ):
                raise DomainError("source_unavailable", "Schedule evidence was not in this work")
            end = (
                resolve_instant(
                    value.valid_until,
                    occurred_at=tx.get_event(current.request_event_id).occurred_at,
                    time_zone=value.time_zone or original.time_zone,
                )
                if value.valid_until is not None
                else original.valid_until
            )
            result = service.update(
                actor,
                value.schedule_id,
                expected_revision=value.expected_revision,
                title=value.title,
                goal=value.goal,
                prepared_message=value.prepared_message,
                authorization_event_id=current.request_event_id,
                fact_source_ids=sources,
                allow_silent_result=value.allow_silent_result,
                timing=value.timing,
                time_zone=value.time_zone,
                valid_until=_ttl(end, now),
                now=now,
            )
            return _receipt(result, now)

        return await mutate("schedule_update", value, context, action)

    async def create_trigger(value: CreateTrigger, context: ToolContext) -> RuleReceipt:
        def action(tx: UnitOfWork, current: WorkOrder, now: datetime, key: str) -> RuleReceipt:
            result = triggers.in_tx(tx).create(
                actor,
                value.event_type,
                value.resource_id,
                value.goal,
                source_event_id=current.request_event_id,
                source_work_id=current.id,
                request_key=key,
                expires_at=_ttl(value.expires_at, now),
                max_occurrences=value.max_occurrences,
                now=now,
            )
            return _receipt(result, now)

        return await mutate("trigger_create", value, context, action)

    def control(kind: Literal["time", "event"]) -> Callable:
        async def invoke(value: ControlRule, context: ToolContext) -> RuleReceipt:
            def action(tx: UnitOfWork, current: WorkOrder, now: datetime, key: str) -> RuleReceipt:
                service: Any = scheduler.in_tx(tx) if kind == "time" else triggers.in_tx(tx)
                original = service.get(actor, value.rule_id)
                _rule_sources(tx, original)
                if value.command == "resume":
                    _ttl(original.valid_until if kind == "time" else original.expires_at, now)
                    result = service.resume(
                        actor, value.rule_id, expected_revision=value.expected_revision, now=now
                    )
                else:
                    result = getattr(service, value.command)(
                        actor, value.rule_id, expected_revision=value.expected_revision
                    )
                return _receipt(result, now)

            return await mutate(
                "schedule_control" if kind == "time" else "trigger_control",
                value,
                context,
                action,
            )

        return invoke

    async def list_rules(value: ListRules, context: ToolContext) -> RuleList:
        def read() -> RuleList:
            with store.transaction(actor) as tx:
                guard(tx, context)
                values: list[Schedule | TriggerRule] = []
                if value.kind in {"time", "all"}:
                    values.extend(scheduler.in_tx(tx).list(actor))
                if value.kind in {"event", "all"}:
                    values.extend(triggers.in_tx(tx).list(actor))
                items: list[dict[str, JsonValue]] = []
                for item in values:
                    try:
                        _rule_sources(tx, item)
                    except DomainError:
                        continue
                    summary = _receipt(item, datetime.now(UTC)).model_dump(mode="json")
                    summary["label"] = (item.title if isinstance(item, Schedule) else item.goal)[
                        :200
                    ]
                    summary["owned_by_requester"] = item.creator_id == actor.member_id
                    items.append(summary)
                    if len(items) >= value.limit:
                        break
                return RuleList(items=tuple(items))

        return await asyncio.to_thread(read)

    for name, input_model, handler, description in (
        (
            "schedule_create",
            CreateSchedule,
            create_schedule,
            "Create a finite time rule. Relative after timing uses the verified human message time. "
            "Supply prepared_message for text ready now, "
            "or goal for work that requires a future execution. Mark explicit_reminder only when the "
            "source directly grants the reminder and contains its exact wording.",
        ),
        (
            "schedule_update",
            UpdateSchedule,
            update_schedule,
            "Update an authorized finite time rule using its current revision. Use clock to change "
            "only the local time of an existing daily or weekly rule.",
        ),
        (
            "schedule_control",
            ControlRule,
            control("time"),
            "Pause, resume or cancel an authorized time rule with a version check.",
        ),
        (
            "trigger_create",
            CreateTrigger,
            create_trigger,
            "Create a finite task/work/attachment follow-up; 1-10 occurrences within 30 days.",
        ),
        (
            "trigger_control",
            ControlRule,
            control("event"),
            "Pause, resume or cancel an authorized event rule with a version check.",
        ),
    ):
        gateway.register(
            ToolDefinition(
                name=name,
                description=description,
                input_model=input_model,
                output_model=RuleReceipt,
                handler=handler,
                idempotency_mode=IdempotencyMode.MANAGED_TRANSACTION,
                resource_scopes=("group", "work"),
                timeout_seconds=15,
                max_input_bytes=32_000,
                max_output_bytes=12_000,
            )
        )
    gateway.register(
        ToolDefinition(
            name="schedule_list",
            description="List authorized finite time/event rules; private-topic rules are omitted.",
            input_model=ListRules,
            output_model=RuleList,
            handler=list_rules,
            idempotency_mode=IdempotencyMode.READ_ONLY,
            resource_scopes=("group", "work"),
            risk_class="read_only",
            timeout_seconds=15,
            max_input_bytes=2000,
            max_output_bytes=48_000,
        )
    )
