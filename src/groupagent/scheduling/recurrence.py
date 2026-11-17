"""Calendar calculations bounded independently of the amount of missed history.

Daily/weekly rules skip nonexistent local wall times and choose the first fold
when clocks repeat. Aware one-shot timestamps already identify a unique instant.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, time, timedelta
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from groupagent.domain import DomainError
from groupagent.scheduling.models import AtTiming, DailyTiming, EveryTiming, Timing, WeeklyTiming


def utc(value: datetime) -> datetime:
    if not isinstance(value, datetime) or value.utcoffset() is None:
        raise DomainError("invalid_input", "Scheduling timestamps must include a timezone")
    return value.astimezone(UTC)


def zone(name: str) -> ZoneInfo:
    try:
        return ZoneInfo(name)
    except (ZoneInfoNotFoundError, ValueError, TypeError) as error:
        raise DomainError("invalid_timezone", "Time zone must be a known IANA name") from error


def wall_instant(day: date, local_time: time, tz: ZoneInfo, *, fold: int = 0) -> datetime | None:
    naive = datetime.combine(day, local_time)
    candidate = naive.replace(tzinfo=tz, fold=fold).astimezone(UTC)
    if candidate.astimezone(tz).replace(tzinfo=None) != naive:
        return None
    return candidate


def _wall(day: date, timing: DailyTiming | WeeklyTiming, tz: ZoneInfo) -> datetime | None:
    return wall_instant(day, timing.local_time, tz)


def next_due(
    timing: Timing, time_zone: str, after: datetime, *, inclusive: bool = False
) -> datetime | None:
    after = utc(after)
    if isinstance(timing, AtTiming):
        return timing.at if (timing.at >= after if inclusive else timing.at > after) else None
    if isinstance(timing, EveryTiming):
        if timing.start_at is None:
            raise DomainError("invalid_input", "An interval schedule requires a stored start time")
        anchor = timing.start_at
        if after < anchor or (inclusive and after == anchor):
            return anchor
        interval = timedelta(seconds=timing.interval_seconds)
        candidate = anchor + ((after - anchor) // interval) * interval
        if candidate < after or not inclusive:
            candidate += interval
        return candidate
    tz = zone(time_zone)
    local_date = after.astimezone(tz).date()
    for offset in range(15):
        day = local_date + timedelta(days=offset)
        if isinstance(timing, WeeklyTiming) and day.weekday() not in timing.weekdays:
            continue
        wall_candidate = _wall(day, timing, tz)
        if wall_candidate is not None and (
            wall_candidate >= after if inclusive else wall_candidate > after
        ):
            return wall_candidate
    raise DomainError(
        "invalid_schedule_time", "No valid local occurrence exists in the next two weeks"
    )


def latest_due(
    timing: Timing, time_zone: str, not_before: datetime, at_or_before: datetime
) -> datetime | None:
    floor, ceiling = utc(not_before), utc(at_or_before)
    if ceiling < floor:
        return None
    if isinstance(timing, AtTiming):
        return timing.at if floor <= timing.at <= ceiling else None
    if isinstance(timing, EveryTiming):
        if timing.start_at is None:
            raise DomainError("invalid_input", "An interval schedule requires a stored start time")
        if ceiling < timing.start_at:
            return None
        interval = timedelta(seconds=timing.interval_seconds)
        candidate = timing.start_at + ((ceiling - timing.start_at) // interval) * interval
        return candidate if candidate >= floor else None
    tz = zone(time_zone)
    local_date = ceiling.astimezone(tz).date()
    for offset in range(15):
        day = local_date - timedelta(days=offset)
        if isinstance(timing, WeeklyTiming) and day.weekday() not in timing.weekdays:
            continue
        wall_candidate = _wall(day, timing, tz)
        if wall_candidate is not None and wall_candidate <= ceiling:
            return wall_candidate if wall_candidate >= floor else None
    return None
