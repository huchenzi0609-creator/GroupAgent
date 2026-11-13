from datetime import UTC, datetime, time, timedelta

import pytest

from groupagent.domain import DomainError
from groupagent.scheduling import AtTiming, DailyTiming, EveryTiming, WeeklyTiming
from groupagent.scheduling.models import parse_timing
from groupagent.scheduling.recurrence import latest_due, next_due


def test_daily_shanghai_is_explicit_and_utc_persistable() -> None:
    rule = DailyTiming(local_time=time(20))
    assert next_due(rule, "Asia/Shanghai", datetime(2030, 1, 1, 13, tzinfo=UTC)) == datetime(
        2030, 1, 2, 12, tzinfo=UTC
    )


def test_nonexistent_dst_wall_time_is_skipped() -> None:
    rule = DailyTiming(local_time=time(2, 30))
    assert next_due(rule, "America/New_York", datetime(2024, 3, 10, tzinfo=UTC)) == datetime(
        2024, 3, 11, 6, 30, tzinfo=UTC
    )


def test_repeated_dst_wall_time_runs_once_at_first_fold() -> None:
    rule = DailyTiming(local_time=time(1, 30))
    first = datetime(2024, 11, 3, 5, 30, tzinfo=UTC)
    assert next_due(rule, "America/New_York", datetime(2024, 11, 3, tzinfo=UTC)) == first
    assert next_due(rule, "America/New_York", first) == datetime(2024, 11, 4, 6, 30, tzinfo=UTC)
    assert (
        latest_due(rule, "America/New_York", first, datetime(2024, 11, 3, 6, 45, tzinfo=UTC))
        == first
    )


def test_weekly_preserves_local_weekdays_and_one_fold_rule() -> None:
    rule = WeeklyTiming(local_time=time(20), weekdays=[4, 0])
    assert rule.weekdays == [0, 4]
    # 2030-01-01 is Tuesday: Friday at 20:00 Shanghai is the next slot.
    assert next_due(rule, "Asia/Shanghai", datetime(2030, 1, 1, tzinfo=UTC)) == datetime(
        2030, 1, 4, 12, tzinfo=UTC
    )


def test_interval_catchup_uses_arithmetic_not_historical_iteration() -> None:
    start = datetime(2000, 1, 1, tzinfo=UTC)
    rule = EveryTiming(interval_seconds=3600, start_at=start)
    end = datetime(2200, 12, 31, 23, 42, tzinfo=UTC)
    assert latest_due(rule, "Asia/Shanghai", start, end) == end.replace(minute=0)
    assert next_due(rule, "Asia/Shanghai", end) == datetime(2201, 1, 1, tzinfo=UTC)


def test_calendar_catchup_checks_at_most_two_weeks(monkeypatch: pytest.MonkeyPatch) -> None:
    from groupagent.scheduling import recurrence

    calls = 0
    original = recurrence._wall

    def counted(*args):
        nonlocal calls
        calls += 1
        return original(*args)

    monkeypatch.setattr(recurrence, "_wall", counted)
    rule = WeeklyTiming(local_time=time(20), weekdays=[0])
    result = latest_due(
        rule,
        "Asia/Shanghai",
        datetime(2000, 1, 1, tzinfo=UTC),
        datetime(2200, 12, 31, tzinfo=UTC),
    )
    assert result is not None and result.weekday() == 0
    assert calls <= 15


@pytest.mark.parametrize("kind", ["cron", "rrule", "event", "monthly", "unknown"])
def test_unsupported_rules_are_rejected(kind: str) -> None:
    with pytest.raises(DomainError) as caught:
        parse_timing({"kind": kind})
    assert caught.value.code == "unsupported_schedule"


def test_non_string_rule_kind_is_rejected_as_domain_error() -> None:
    with pytest.raises(DomainError) as caught:
        parse_timing({"kind": ["daily"]})
    assert caught.value.code == "unsupported_schedule"


@pytest.mark.parametrize(
    "timing",
    [
        {"kind": "at", "at": "2030-01-01T20:00:00"},
        {"kind": "every", "interval_seconds": True},
        {"kind": "daily", "local_time": "20:00:00+08:00"},
        {"kind": "weekly", "local_time": "20:00", "weekdays": [0, 0]},
        {"kind": "weekly", "local_time": "20:00", "weekdays": [7]},
    ],
)
def test_ambiguous_or_invalid_timing_is_rejected(timing) -> None:
    with pytest.raises(DomainError) as caught:
        parse_timing(timing)
    assert caught.value.code == "invalid_input"


def test_one_shot_and_clock_rollback_do_not_create_new_slots() -> None:
    at = datetime(2030, 1, 1, tzinfo=UTC)
    rule = AtTiming(at=at)
    assert next_due(rule, "Asia/Shanghai", at - timedelta(seconds=1)) == at
    assert next_due(rule, "Asia/Shanghai", at) is None
    assert latest_due(rule, "Asia/Shanghai", at, at - timedelta(seconds=1)) is None
