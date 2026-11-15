"""Shared quiet-window semantics for participation and autonomous delivery."""

from __future__ import annotations

import re
from datetime import datetime
from zoneinfo import ZoneInfo


def validate_windows(values: list[str]) -> list[str]:
    if len(values) > 10 or any(
        not re.fullmatch(r"(?:[01]\d|2[0-3]):[0-5]\d-(?:[01]\d|2[0-3]):[0-5]\d", value)
        or value[:5] == value[6:]
        for value in values
    ):
        raise ValueError("Quiet windows use distinct HH:MM-HH:MM local times")
    return list(dict.fromkeys(values))


def in_window(now: datetime, zone: str, windows: list[str]) -> bool:
    clock = now.astimezone(ZoneInfo(zone)).strftime("%H:%M")
    return any(
        (start <= clock < end if start < end else clock >= start or clock < end)
        for start, end in (value.split("-") for value in validate_windows(windows))
    )


def is_quiet(
    policies: list[dict], now: datetime, zone: str, defaults: list[str] | None = None
) -> bool:
    if not any(row["subject_id"] == "*" for row in policies) and in_window(
        now, zone, defaults or []
    ):
        return True
    for row in policies:
        if row["quiet"] and (row["quiet_until"] is None or row["quiet_until"] > now):
            if not row.get("quiet_hours") or in_window(now, zone, row["quiet_hours"]):
                return True
    return False
