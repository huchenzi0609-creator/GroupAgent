"""Bounded scheduling contracts. Natural-language parsing is a separate layer."""

from __future__ import annotations

import re
from datetime import time
from enum import StrEnum
from typing import Annotated, Literal

from pydantic import AwareDatetime, Field, JsonValue, TypeAdapter, ValidationError, field_validator

from groupagent.domain import DeliveryState, DomainError, DomainModel, Identifier, Role


class AtTiming(DomainModel):
    kind: Literal["at"] = "at"
    at: AwareDatetime


class EveryTiming(DomainModel):
    kind: Literal["every"] = "every"
    interval_seconds: int = Field(strict=True, ge=1)
    start_at: AwareDatetime | None = None


_LOCAL_TIME_PATTERN = r"^(?:[01][0-9]|2[0-3]):[0-5][0-9](?::[0-5][0-9])?$"
LocalTimeInput = Annotated[str, Field(pattern=_LOCAL_TIME_PATTERN)]


class WallClockTiming(DomainModel):
    local_time: time = Field(
        description="Local clock time in HH:MM or HH:MM:SS, without an offset."
    )

    @field_validator("local_time", mode="before", json_schema_input_type=LocalTimeInput)
    @classmethod
    def wall_clock(cls, value: object) -> time:
        if isinstance(value, str) and re.fullmatch(_LOCAL_TIME_PATTERN, value):
            return time.fromisoformat(value)
        if not isinstance(value, time):
            raise ValueError("Use a local time in HH:MM or HH:MM:SS form")
        if value.tzinfo is not None or value.microsecond:
            raise ValueError("Local time must have whole seconds and no UTC offset")
        return value


class DailyTiming(WallClockTiming):
    kind: Literal["daily"] = "daily"


class WeeklyTiming(WallClockTiming):
    kind: Literal["weekly"] = "weekly"
    weekdays: list[Annotated[int, Field(strict=True, ge=0, le=6)]] = Field(
        min_length=1,
        max_length=7,
        description="Distinct weekday indices counting from Monday=0 through Sunday=6.",
        json_schema_extra={"uniqueItems": True},
    )

    @field_validator("weekdays")
    @classmethod
    def canonical_weekdays(cls, value: list[int]) -> list[int]:
        if len(set(value)) != len(value):
            raise ValueError("Weekdays must not repeat")
        return sorted(value)


Timing = Annotated[AtTiming | EveryTiming | DailyTiming | WeeklyTiming, Field(discriminator="kind")]
_timing_adapter: TypeAdapter[Timing] = TypeAdapter(Timing)


def parse_timing(value: Timing | dict[str, JsonValue]) -> Timing:
    if isinstance(value, dict) and (
        not isinstance(value.get("kind"), str)
        or value.get("kind") not in {"at", "every", "daily", "weekly"}
    ):
        raise DomainError(
            "unsupported_schedule", "Only at, every, daily and weekly rules are supported"
        )
    try:
        return _timing_adapter.validate_python(value)
    except ValidationError as error:
        raise DomainError("invalid_input", "Schedule timing is invalid or ambiguous") from error


class ScheduleState(StrEnum):
    ACTIVE = "active"
    PAUSED = "paused"
    ENDED = "ended"
    CANCELLED = "cancelled"


class ScheduleKind(StrEnum):
    GOAL = "goal"
    NOTIFICATION = "notification"
    EXPLICIT_REMINDER = "explicit_reminder"


class PreparedMessage(DomainModel):
    text: str = Field(min_length=1, max_length=3000)
    purpose: Literal["notification", "explicit_reminder"] = "notification"


class OccurrenceState(StrEnum):
    PENDING = "pending"
    ENQUEUED = "enqueued"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    SKIPPED = "skipped"
    UNCERTAIN = "uncertain"


class Schedule(DomainModel):
    id: Identifier
    group_id: Identifier
    creator_id: Identifier
    creator_role: Role
    request_key: str | None = None
    title: Identifier
    goal: Identifier
    execution_kind: ScheduleKind = ScheduleKind.GOAL
    timing: Timing
    time_zone: str = "Asia/Shanghai"
    starts_at: AwareDatetime
    valid_until: AwareDatetime
    next_due_at: AwareDatetime | None = None
    state: ScheduleState = ScheduleState.ACTIVE
    revision: int = Field(ge=1)
    policy_revision: int = Field(ge=1)
    scope: dict[str, JsonValue]
    budget: dict[str, JsonValue]
    acceptance_criteria: list[str]
    pause_reason: str | None = None
    misfire_policy: Literal["latest_only"] = "latest_only"
    overlap_policy: Literal["coalesce"] = "coalesce"
    created_at: AwareDatetime
    updated_at: AwareDatetime


class ScheduleOccurrence(DomainModel):
    id: Identifier
    group_id: Identifier
    schedule_id: Identifier
    scheduled_at: AwareDatetime
    state: OccurrenceState
    work_id: str | None = None
    delivery_id: str | None = None
    delivery_purpose: ScheduleKind | None = None
    delivery_sources: list[str] = Field(default_factory=list, exclude=True)
    coalesced_from: AwareDatetime | None = None
    reason: str | None = None
    revision: int = Field(ge=1)
    created_at: AwareDatetime
    updated_at: AwareDatetime


class ScheduleDeliveryFacts(DomainModel):
    id: str
    state: DeliveryState
    reason: str | None = None
    accepted_at: AwareDatetime | None = None
    platform_message_id: str | None = None
    platform_job_id: str | None = None


class ScheduleOccurrenceFacts(DomainModel):
    id: str
    scheduled_at: AwareDatetime
    scheduled_local: str | None = None
    state: OccurrenceState
    reason: str | None = None
    work_id: str | None = None
    deliveries: list[ScheduleDeliveryFacts]


class ScheduleFacts(DomainModel):
    id: str
    title: str
    goal: str
    execution_kind: ScheduleKind
    state: ScheduleState
    revision: int
    time_zone: str
    timing: Timing
    valid_until: AwareDatetime
    next_due_at: AwareDatetime | None
    next_due_local: str | None = None
    timing_source_local: str | None = None
    pause_reason: str | None
    owned_by_requester: bool
    allow_silent_result: bool = False
    occurrences: list[ScheduleOccurrenceFacts]
