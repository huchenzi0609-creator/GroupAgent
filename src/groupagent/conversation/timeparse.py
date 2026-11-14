"""Small deterministic Chinese time grammar; unsupported expressions stay explicit.

This module resolves evidence, not intent or permission. Its anchor is the actual
message timestamp, never the processing clock. It does not create schedules.
"""

from __future__ import annotations

import re
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime, time, timedelta
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

_NUM = r"(?:\d{1,4}|[零〇一二两三四五六七八九十百]+)"
_CLOCK = re.compile(
    rf"(?P<period>凌晨|午夜|早上|上午|中午|下午|傍晚|晚上|早|晚)?\s*(?P<hour>{_NUM})(?:[：:](?P<minute>\d{{2}})(?P<offset>[+-]\d{{2}}:\d{{2}})?|[点时](?P<part>半|一刻|三刻|{_NUM}分?)?)"
)
_DATE = re.compile(r"(?:(?P<year>\d{4})年)?(?P<month>\d{1,2})月(?P<day>\d{1,2})[日号]?")
_ISO_DATE = re.compile(r"(?P<year>\d{4})-(?P<month>\d{2})-(?P<day>\d{2})")
_DAY = re.compile(r"今天|明天|后天|昨天|前天|今晚|今早|明晚|明早")
_WEEK = re.compile(r"(?P<prefix>下下|下|本|这)?(?:周|星期)(?P<day>[一二三四五六日天])")
_WEEKDAYS = {char: index for index, char in enumerate("一二三四五六日")}
_WEEKDAYS["天"] = 6
_DURATION = re.compile(rf"(?P<amount>半|{_NUM})(?P<unit>小时|分钟|秒)(?P<after>后)?")
_ZONES = {
    "上海": "Asia/Shanghai",
    "北京": "Asia/Shanghai",
    "纽约": "America/New_York",
    "伦敦": "Europe/London",
    "东京": "Asia/Tokyo",
}


@dataclass(frozen=True)
class TimeResolution:
    status: str
    anchor: str
    time_zone: str
    timing: dict[str, Any] | None = None
    instant: str | None = None
    local_date: str | None = None
    local_time: str | None = None
    reason: str | None = None
    shift_seconds: int | None = None
    recurrence_kind: str | None = None
    weekdays: tuple[int, ...] | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def chinese_number(value: str) -> int:
    if value.isascii() and value.isdecimal():
        return int(value)
    digits = {char: index for index, char in enumerate("零一二三四五六七八九")}
    digits.update({"〇": 0, "两": 2})
    if value in digits:
        return digits[value]
    if re.fullmatch(r"[一二两三四五六七八九]?十[一二三四五六七八九]?", value):
        tens, ones = value.split("十")
        return digits.get(tens, 1) * 10 + digits.get(ones, 0)
    raise ValueError("Unsupported numeral")


def _seconds(match: re.Match[str]) -> int:
    multiplier = {"小时": 3600, "分钟": 60, "秒": 1}[match["unit"]]
    return (
        int(multiplier / 2)
        if match["amount"] == "半"
        else chinese_number(match["amount"]) * multiplier
    )


def _wall(
    day: date, clock: time, zone: ZoneInfo, offset: str | None
) -> tuple[datetime | None, str | None]:
    naive = datetime.combine(day, clock)
    possibilities: list[datetime] = []
    for fold in (0, 1):
        candidate = naive.replace(tzinfo=zone, fold=fold)
        roundtrip = candidate.astimezone(UTC).astimezone(zone)
        if roundtrip.replace(tzinfo=None) == naive and all(
            candidate.utcoffset() != prior.utcoffset() for prior in possibilities
        ):
            possibilities.append(candidate)
    if offset:
        sign = -1 if offset[0] == "-" else 1
        hours, minutes = map(int, offset[1:].split(":"))
        if hours > 14 or minutes > 59 or hours == 14 and minutes:
            return None, "invalid_offset"
        wanted = timedelta(minutes=sign * (hours * 60 + minutes))
        possibilities = [
            candidate for candidate in possibilities if candidate.utcoffset() == wanted
        ]
        if not possibilities:
            return None, "offset_zone_mismatch"
    if len(possibilities) != 1:
        return None, "ambiguous_dst_fold" if possibilities else "nonexistent_local_time"
    return possibilities[0], None


def normalize_time(
    text: str, occurred_at: datetime, time_zone: str = "Asia/Shanghai"
) -> TimeResolution:
    if occurred_at.tzinfo is None:
        raise ValueError("Time normalization requires an aware source timestamp")
    if len(text) > 16000:
        return TimeResolution("unresolved", occurred_at.isoformat(), time_zone, reason="text_limit")
    for name, identifier in _ZONES.items():
        if name + "时间" in text:
            time_zone = identifier
            break
    try:
        zone = ZoneInfo(time_zone)
    except ZoneInfoNotFoundError:
        return TimeResolution(
            "unresolved", occurred_at.isoformat(), time_zone, reason="unknown_time_zone"
        )
    anchor = occurred_at.astimezone(zone)
    # Explicit replacement clauses identify the proposed time; do not reuse the
    # old frequency/date appearing before “change to”. This still grants no action.
    replacements = re.split(r"改成|改为|改到|换成|调整为|更新为", text)
    if len(replacements) > 1:
        text = replacements[-1]

    def result(status: str, **fields: Any) -> TimeResolution:
        return TimeResolution(status, anchor.isoformat(), time_zone, **fields)

    try:
        durations = list(_DURATION.finditer(text))
        clocks = list(_CLOCK.finditer(text))
        dates = list(_DATE.finditer(text)) + list(_ISO_DATE.finditer(text))
        days = list(_DAY.finditer(text))
        weeks = list(_WEEK.finditer(text))
        if len(clocks) > 1 or len(dates) > 1 or len(days) > 1 or dates and days:
            return result("ambiguous", reason="multiple_time_references")
        if durations:
            duration = durations[0]
            seconds = _seconds(duration)
            if seconds <= 0 or seconds > 366 * 24 * 3600 or len(durations) > 1:
                return result("ambiguous", reason="invalid_duration")
            before = text[max(0, duration.start() - 6) : duration.start()]
            if re.search(r"往后(?:挪|推|移)|延后", before):
                return result("partial", shift_seconds=seconds, reason="existing_schedule_required")
            if re.search(r"往前(?:挪|推|移)|提前", before):
                return result(
                    "partial", shift_seconds=-seconds, reason="existing_schedule_required"
                )
            if re.search(r"每(?:隔)?$", before):
                return result("resolved", timing={"kind": "every", "interval_seconds": seconds})
            if duration["after"] or "接下来" in before or "安静" in before or "静默" in before:
                if clocks or dates or days or weeks:
                    return result("ambiguous", reason="multiple_time_references")
                instant_text = (
                    (anchor.astimezone(UTC) + timedelta(seconds=seconds))
                    .astimezone(zone)
                    .isoformat()
                )
                return result(
                    "resolved", timing={"kind": "at", "at": instant_text}, instant=instant_text
                )
        clock = None
        offset = None
        if clocks:
            match = clocks[0]
            hour = chinese_number(match["hour"])
            minute = int(match["minute"]) if match["minute"] else 0
            part = match["part"]
            if part:
                minute = {"半": 30, "一刻": 15, "三刻": 45}.get(part, -1)
                if minute == -1:
                    minute = chinese_number(part.rstrip("分"))
            period = match["period"]
            if period in {"下午", "傍晚", "晚上", "晚"} and hour < 12:
                hour += 12
            elif period == "中午" and hour < 11:
                hour += 12
            elif period in {"凌晨", "午夜"} and hour == 12:
                hour = 0
            if period in {"晚上", "晚"} and hour == 12:
                return result("ambiguous", reason="midnight_date_ambiguous")
            clock = time(hour, minute)
            offset = match["offset"]
        clock_text = clock.strftime("%H:%M") if clock else None
        recurring_daily = bool(re.search(r"每天|每日", text))
        recurring_weekly = (
            "每周" in text
            or "每星期" in text
            or (len(weeks) > 1 and all(item["prefix"] is None for item in weeks))
        )
        if recurring_daily or recurring_weekly:
            if clock is None:
                return result(
                    "partial",
                    reason="clock_required",
                    recurrence_kind="daily" if recurring_daily else "weekly",
                    weekdays=tuple(sorted({_WEEKDAYS[item["day"]] for item in weeks})) or None,
                )
            if offset or dates or days:
                return result("ambiguous", reason="recurrence_has_date_or_offset")
            if recurring_daily:
                return result(
                    "resolved",
                    timing={"kind": "daily", "local_time": clock_text},
                    local_time=clock_text,
                )
            if not weeks:
                return result("partial", local_time=clock_text, reason="weekday_required")
            return result(
                "resolved",
                timing={
                    "kind": "weekly",
                    "local_time": clock_text,
                    "weekdays": sorted({_WEEKDAYS[item["day"]] for item in weeks}),
                },
                local_time=clock_text,
            )
        day = None
        if dates:
            match = dates[0]
            day = date(int(match["year"] or anchor.year), int(match["month"]), int(match["day"]))
        elif days:
            relative = {
                "今天": 0,
                "今早": 0,
                "今晚": 0,
                "明天": 1,
                "明早": 1,
                "明晚": 1,
                "后天": 2,
                "昨天": -1,
                "前天": -2,
            }
            day = anchor.date() + timedelta(days=relative[days[0].group()])
        elif weeks:
            if len(weeks) != 1:
                return result(
                    "ambiguous", local_time=clock_text, reason="recurrence_or_multiple_dates"
                )
            item = weeks[0]
            weekday = _WEEKDAYS[item["day"]]
            prefix = item["prefix"]
            if prefix:
                day = (
                    anchor.date()
                    - timedelta(days=anchor.weekday())
                    + timedelta(days=weekday + {"下下": 14, "下": 7, "本": 0, "这": 0}[prefix])
                )
            else:
                day = anchor.date() + timedelta(days=(weekday - anchor.weekday()) % 7)
        if day is None:
            return result(
                "partial" if clock else "unrecognized",
                local_time=clock_text,
                reason="date_required" if clock else None,
            )
        if clock is None:
            return result("partial", local_date=day.isoformat(), reason="clock_required")
        instant, reason = _wall(day, clock, zone, offset)
        if instant is None:
            return result(
                "ambiguous", local_date=day.isoformat(), local_time=clock_text, reason=reason
            )
        canonical = instant.isoformat()
        return result(
            "resolved",
            timing={"kind": "at", "at": canonical},
            instant=canonical,
            local_date=day.isoformat(),
            local_time=clock_text,
        )
    except (ValueError, OverflowError):
        return result("ambiguous", reason="invalid_time_expression")


def shift_timing(timing: dict[str, Any], seconds: int) -> dict[str, Any] | None:
    """Apply an explicit displacement to a selected existing rule only."""
    try:
        kind = timing.get("kind")
        if kind == "at":
            instant = datetime.fromisoformat(timing["at"].replace("Z", "+00:00"))
            if instant.tzinfo is None:
                return None
            return {"kind": "at", "at": (instant + timedelta(seconds=seconds)).isoformat()}
        if kind in {"daily", "weekly"}:
            clock = time.fromisoformat(timing["local_time"])
            current = clock.hour * 3600 + clock.minute * 60 + clock.second
            shifted = current + seconds
            if not 0 <= shifted < 86400:
                return None  # Day/weekday rollover needs explicit user intent.
            return {**timing, "local_time": str(timedelta(seconds=shifted)).zfill(8)}
    except (ValueError, KeyError, TypeError):
        pass
    return None
