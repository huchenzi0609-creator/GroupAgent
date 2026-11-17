"""Resolve typed time proposals from trusted clocks and revision-checked rules."""

from __future__ import annotations

from datetime import date as Date
from datetime import datetime, time, timedelta
from decimal import Decimal, InvalidOperation
from typing import Annotated, Literal, TypeAlias, cast
from zoneinfo import ZoneInfo

from pydantic import (
    AwareDatetime,
    Field,
    JsonValue,
    TypeAdapter,
    ValidationError,
    field_validator,
    model_validator,
)

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

_UNIT_SECONDS = {"seconds": 1, "minutes": 60, "hours": 3600, "days": 86400, "weeks": 604800}
DurationAmount = Annotated[
    Decimal,
    Field(allow_inf_nan=False, description="Copy the numeric amount in the request's stated unit."),
]


class DurationTiming(DomainModel):
    amount: DurationAmount
    unit: Literal["seconds", "minutes", "hours", "days", "weeks"] = Field(
        description="The unit stated by the requester. The host converts this amount."
    )

    @model_validator(mode="before")
    @classmethod
    def parse_amount(cls, value: object) -> object:
        if not isinstance(value, dict):
            return value
        duration = dict(value)
        if "seconds" in duration:
            seconds = duration.pop("seconds")
            if type(seconds) is not int or "amount" in duration or "unit" in duration:
                raise ValueError(
                    "Specify either a duration amount and unit or legacy integer seconds"
                )
            return {**duration, "amount": Decimal(seconds), "unit": "seconds"}
        amount = duration.get("amount")
        if type(amount) in (int, float, str):
            try:
                duration["amount"] = Decimal(str(amount))
            except InvalidOperation as error:
                raise ValueError("The stated duration requires a decimal number") from error
        return duration

    @property
    def seconds(self) -> int:
        # Integer-ratio arithmetic preserves the stated decimal exactly.
        magnitude = self.amount.copy_abs()
        if magnitude and magnitude < Decimal("0.000001"):
            raise ValueError("The stated duration must resolve to whole seconds")
        if magnitude > Decimal(timedelta.max.days * 86400 + 86399):
            raise ValueError("The stated duration is outside the supported calendar range")
        numerator, denominator = self.amount.as_integer_ratio()
        seconds, remainder = divmod(numerator * _UNIT_SECONDS[self.unit], denominator)
        if remainder:
            raise ValueError("The stated duration must resolve to whole seconds")
        return seconds

    @model_validator(mode="after")
    def whole_nonzero_duration(self) -> DurationTiming:
        if not self.seconds:
            raise ValueError("The duration must move the scheduled time")
        return self


class AfterTiming(DurationTiming):
    kind: Literal["after"]
    amount: DurationAmount = Field(gt=0)


class WallTimeInput(WallClockTiming):
    date: Date
    time_zone: str | None = Field(default=None, description="IANA time zone for this local time.")

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


class WallTiming(WallTimeInput):
    kind: Literal["wall"]
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


class WallTimeCandidate(DomainModel):
    fold: Literal[0, 1]
    local: str
    utc: str


class WallTimePreview(DomainModel):
    date: Date
    local_time: time
    time_zone: str
    status: Literal["unique", "ambiguous", "nonexistent"]
    requires_author_choice: bool
    candidates: tuple[WallTimeCandidate, ...]


class ClockTiming(WallClockTiming):
    """Change only the local clock time of an existing daily or weekly rule."""

    kind: Literal["clock"]


class ShiftTiming(DurationTiming):
    kind: Literal["shift"]
    amount: DurationAmount = Field(
        description="Copy the stated amount. Use a negative sign to move the rule earlier."
    )


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


def _wall_candidates(wall: WallTimeInput, time_zone: str) -> tuple[ZoneInfo, tuple[datetime, ...]]:
    tz = zone(wall.time_zone if wall.time_zone is not None else time_zone)
    try:
        first = wall_instant(wall.date, wall.local_time, tz)
        second = wall_instant(wall.date, wall.local_time, tz, fold=1)
    except OverflowError as error:
        raise DomainError(
            "invalid_schedule_time", "The proposed instant is out of range"
        ) from error
    if first is None or second is None:
        return tz, ()
    return tz, (first,) if first == second else (first, second)


def preview_wall_time(wall: WallTimeInput, *, time_zone: str) -> WallTimePreview:
    """Observe possible instants for a local clock time without choosing one."""
    tz, instants = _wall_candidates(wall, time_zone)
    return WallTimePreview(
        date=wall.date,
        local_time=wall.local_time,
        time_zone=tz.key,
        status=("nonexistent", "unique", "ambiguous")[len(instants)],
        requires_author_choice=len(instants) != 1,
        candidates=tuple(
            WallTimeCandidate(
                fold=1 if index else 0,
                local=instant.astimezone(tz).isoformat(),
                utc=instant.isoformat(),
            )
            for index, instant in enumerate(instants)
        ),
    )


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
    tz, candidates = _wall_candidates(proposal, time_zone)
    if not candidates:
        raise DomainError(
            "nonexistent_local_time", "The local time was skipped when the clocks changed"
        )
    selection = (proposal.fold_evidence or "").strip()
    if len(candidates) == 2 and (
        proposal.fold is None
        or source_text is not None
        and (not selection or selection not in source_text)
    ):
        raise DomainError(
            "ambiguous_dst_fold", "The local time occurs twice. Choose the first or second instant"
        )
    return candidates[-1 if proposal.fold == 1 else 0], tz.key


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
