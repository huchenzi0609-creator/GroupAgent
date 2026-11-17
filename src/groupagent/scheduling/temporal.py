"""Resolve typed time proposals from trusted clocks and revision-checked rules."""

from __future__ import annotations

from datetime import date as Date
from datetime import datetime, time, timedelta
from typing import Annotated, Literal, TypeAlias, cast

from pydantic import AwareDatetime, Field, JsonValue, TypeAdapter, ValidationError, field_validator

from groupagent.domain import DomainError, DomainModel
from groupagent.scheduling.models import (
    AtTiming,
    DailyTiming,
    EveryTiming,
    Timing,
    WallClockTiming,
    WeeklyTiming,
    parse_timing,
)
from groupagent.scheduling.recurrence import utc, wall_instant, zone


class AfterTiming(DomainModel):
    kind: Literal["after"]
    seconds: int = Field(
        strict=True, gt=0, description="Positive elapsed seconds from the source message timestamp."
    )


class WallTiming(WallClockTiming):
    kind: Literal["wall"]
    date: Date
    time_zone: str | None = Field(default=None, description="IANA time zone for this local time.")
    fold: Annotated[int, Field(strict=True, ge=0, le=1)] | None = Field(
        default=None,
        description="For a repeated local time, 0 selects the first instant and 1 the second.",
    )
    fold_evidence: str | None = Field(
        default=None,
        min_length=1,
        max_length=160,
        description="Exact words from the current requester selecting the earlier or later instant.",
    )

    @field_validator("date", mode="before")
    @classmethod
    def calendar_date(cls, value: object) -> Date:
        if type(value) is Date:
            return value
        if isinstance(value, str):
            parsed = Date.fromisoformat(value)
            if parsed.isoformat() == value:
                return parsed
        raise ValueError("Use a calendar date in YYYY-MM-DD form")


class ClockTiming(WallClockTiming):
    """Change only the local clock time of an existing daily or weekly rule."""

    kind: Literal["clock"]


class ShiftTiming(DomainModel):
    kind: Literal["shift"]
    seconds: int = Field(
        strict=True,
        description="Signed seconds to move the selected schedule. Negative moves it earlier.",
    )

    @field_validator("seconds")
    @classmethod
    def nonzero_shift(cls, value: int) -> int:
        if value == 0:
            raise ValueError("A shift must move the schedule")
        return value


CreateTimingProposal: TypeAlias = Annotated[
    AtTiming | DailyTiming | EveryTiming | WeeklyTiming | AfterTiming | WallTiming,
    Field(discriminator="kind"),
]
ScheduleTimingProposal: TypeAlias = Annotated[
    AtTiming
    | DailyTiming
    | EveryTiming
    | WeeklyTiming
    | AfterTiming
    | WallTiming
    | ClockTiming
    | ShiftTiming,
    Field(discriminator="kind"),
]
InstantTimingProposal: TypeAlias = Annotated[
    AtTiming | AfterTiming | WallTiming, Field(discriminator="kind")
]
InstantProposal: TypeAlias = AwareDatetime | InstantTimingProposal
SCHEDULE_TIMING_ADAPTER = TypeAdapter(cast(type[ScheduleTimingProposal], ScheduleTimingProposal))
_instant_adapter = TypeAdapter(cast(type[InstantProposal], InstantProposal))


def clock_snapshot(*, occurred_at: datetime, now: datetime, time_zone: str) -> dict[str, str]:
    """Expose the group clock and the immutable source clock with explicit offsets."""
    tz = zone(time_zone)
    now_utc = utc(now)
    occurred_at_utc = utc(occurred_at)
    return {
        "time_zone": time_zone,
        "now": now_utc.astimezone(tz).isoformat(),
        "now_utc": now_utc.isoformat(),
        "occurred_at": occurred_at_utc.astimezone(tz).isoformat(),
        "occurred_at_utc": occurred_at_utc.isoformat(),
    }


def _plus(instant: datetime, seconds: int) -> datetime:
    try:
        return utc(instant) + timedelta(seconds=seconds)
    except OverflowError as error:
        raise DomainError(
            "invalid_schedule_time", "The proposed instant is out of range"
        ) from error


def _instant(
    proposal: InstantProposal,
    occurred_at: datetime | None,
    time_zone: str,
    source_text: str | None,
) -> tuple[datetime, str]:
    if isinstance(proposal, datetime):
        return utc(proposal), time_zone
    if isinstance(proposal, AtTiming):
        return utc(proposal.at), time_zone
    if isinstance(proposal, AfterTiming):
        if occurred_at is None:
            raise DomainError(
                "invalid_schedule_time", "A relative time requires a trusted source timestamp"
            )
        return _plus(occurred_at, proposal.seconds), time_zone
    time_zone = proposal.time_zone if proposal.time_zone is not None else time_zone
    tz = zone(time_zone)
    try:
        first = wall_instant(proposal.date, proposal.local_time, tz)
        second = wall_instant(proposal.date, proposal.local_time, tz, fold=1)
    except OverflowError as error:
        raise DomainError(
            "invalid_schedule_time", "The proposed instant is out of range"
        ) from error
    if first is None or second is None:
        raise DomainError(
            "nonexistent_local_time", "The local time was skipped when the clocks changed"
        )
    selection = (proposal.fold_evidence or "").strip()
    if first != second and (
        proposal.fold is None
        or source_text is not None
        and (not selection or selection not in source_text)
    ):
        raise DomainError(
            "ambiguous_dst_fold", "The local time occurs twice. Choose the first or second instant"
        )
    return (second if proposal.fold == 1 else first), time_zone


def resolve_instant(
    value: InstantProposal | str | dict[str, object],
    *,
    occurred_at: datetime | None,
    time_zone: str,
    source_text: str | None = None,
) -> datetime:
    """Resolve a one-shot proposal for a deadline, quiet end or validity bound."""
    try:
        proposal = _instant_adapter.validate_python(value)
    except (ValidationError, OverflowError) as error:
        raise DomainError(
            "invalid_schedule_time", "An aware instant or one-shot proposal is required"
        ) from error
    return _instant(proposal, occurred_at, time_zone, source_text)[0]


def _shift(rule: Timing, seconds: int) -> Timing:
    if isinstance(rule, AtTiming):
        return AtTiming(at=_plus(rule.at, seconds))
    if isinstance(rule, EveryTiming):
        if rule.start_at is None:
            raise DomainError(
                "invalid_schedule_time", "The selected interval rule has no start time"
            )
        return EveryTiming(
            interval_seconds=rule.interval_seconds, start_at=_plus(rule.start_at, seconds)
        )
    old = rule.local_time
    days, slot = divmod(old.hour * 3600 + old.minute * 60 + old.second + seconds, 86400)
    hours, remainder = divmod(slot, 3600)
    minutes, seconds = divmod(remainder, 60)
    shifted = time(hours, minutes, seconds)
    if isinstance(rule, WeeklyTiming):
        return WeeklyTiming(
            local_time=shifted, weekdays=[(day + days) % 7 for day in rule.weekdays]
        )
    return DailyTiming(local_time=shifted)


def resolve_schedule_timing(
    value: ScheduleTimingProposal | dict[str, JsonValue],
    *,
    occurred_at: datetime | None,
    time_zone: str,
    reference_timing: Timing | None = None,
    source_text: str | None = None,
) -> tuple[Timing, str]:
    """Return existing stored timing and its display zone from a typed proposal."""
    if isinstance(value, dict) and value.get("kind") not in ("after", "wall", "clock", "shift"):
        zone(time_zone)
        return parse_timing(value), time_zone
    try:
        proposal = SCHEDULE_TIMING_ADAPTER.validate_python(value)
    except (ValidationError, OverflowError) as error:
        raise DomainError("invalid_schedule_time", "Schedule timing is invalid") from error
    if isinstance(proposal, (AfterTiming, WallTiming)):
        instant, time_zone = _instant(proposal, occurred_at, time_zone, source_text)
        zone(time_zone)
        return AtTiming(at=instant), time_zone
    zone(time_zone)
    if isinstance(proposal, ClockTiming):
        if not isinstance(reference_timing, (DailyTiming, WeeklyTiming)):
            raise DomainError(
                "invalid_schedule_time", "A clock-only change requires a daily or weekly schedule"
            )
        return reference_timing.model_copy(update={"local_time": proposal.local_time}), time_zone
    if isinstance(proposal, ShiftTiming):
        if reference_timing is None:
            raise DomainError("invalid_schedule_time", "A shift requires a selected schedule")
        return _shift(reference_timing, proposal.seconds), time_zone
    return proposal, time_zone
