"""Literal reminder data derived only from explicit authorized source wording."""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from typing import Any

from groupagent.domain import DomainError
from groupagent.identity import DEFAULT_IDENTITY, BotIdentity

MAX_REMINDER_BODY = 3000
_TIME_SHELL = re.compile(
    r"[\s在于到从每逢周星期天日年月号今明后上下本个点时分秒钟小一二两三四五六七八九十百零〇早晚晨午凌晨隔工作休息末之和及至只仅，,:：./+TZ0-9-]*"
)
_NOTICE = re.compile(r"(?:提醒|通知)(?:一下)?(?:我们|大家|我)(?:一下)?\s*[:：]?\s*")
_REPLACEMENT = re.compile(
    r"(?:提醒|通知)(?:的)?(?:正文|内容|文案|文字|事项)\s*(?:改成|改为|换成|换为|更新为|调整为)\s*[:：]?\s*"
)
_ONCE = re.compile(
    r"(?:仍然|仍|还是|继续|也|就)?(?:只|仅)(?:提醒|通知)?(?:我们|大家|我)?(?:一|1)次|(?:不要|不用)(?:再|重复)(?:提醒|通知)?"
)


def _unwrapped_body(value: str, identity: BotIdentity) -> str | None:
    value = value.strip()
    # Frequency/one-shot wording belongs to the rule, not its notification.
    value = re.sub(r"[,，;；]\s*(?:" + _ONCE.pattern + r")[。.!！]?\s*$", "", value).strip()
    quotes = {"“": "”", "‘": "’", "「": "」", "『": "』", '"': '"', "'": "'"}
    if value[:1] in quotes:
        closing = quotes[value[0]]
        end = value.find(closing, 1)
        if end < 0 or value[end + 1 :].strip(" 。.!！"):
            return None
        body = value[1:end]
    else:
        body = value.rstrip("。.!！").strip()
        if re.search(r"[，,；;\n]", body) or identity.additional_request(body):
            return None
        # Postposed timing may be the command shell rather than the body.
        if re.match(
            r"(?:在|于)?(?:[0-9]|[一二两三四五六七八九十]+(?:点|时|分钟|小时)|今天|明天|后天|星期|周[一二三四五六日天]|上午|下午|晚上|中午|凌晨)",
            body,
        ):
            return None
    if not body.strip() or body != body.strip() or len(body) > MAX_REMINDER_BODY or "\x00" in body:
        return None
    return body


def extract_reminder_body(
    text: str, *, replacement: bool = False, identity: BotIdentity = DEFAULT_IDENTITY
) -> str | None:
    """Return an explicit body without timing/receiver/frequency wrappers.

    Ambiguous prose is left to the existing semantic path. Quoted bodies retain
    their internal characters; unquoted trailing sentence punctuation is omitted.
    """
    if not text or len(text) > 6000:
        return None
    text = identity.strip_address(text)
    if replacement:
        match = _REPLACEMENT.search(text)
        if match is None:
            match = re.search(
                r"(?:改成|改为|换成|换为|调整为)(?:只|仅|继续|还是)?"
                r"(?:提醒|通知)(?:我们|大家|我)(?:一下)?\s*[:：]?\s*",
                text,
            )
        if match and not re.search(r"引用|转发|假设|如果|解释|不要|别", text[: match.start()]):
            return _unwrapped_body(text[match.end() :], identity)
    match = _NOTICE.search(text)
    if match is None:
        return None
    prefix = text[: match.start()].strip()
    prefix = re.sub(r"^(?:(?:请|麻烦你?|帮我|替我|为我|能否|能不能)\s*)+", "", prefix)
    if not _TIME_SHELL.fullmatch(prefix):
        return None
    return _unwrapped_body(text[match.end() :], identity)


def is_time_only_reminder_change(text: str, *, identity: BotIdentity = DEFAULT_IDENTITY) -> bool:
    """A narrow timing change cannot authorize a model-generated body rewrite."""
    text = identity.strip_address(text)
    head = re.split(r"[，,。；;]", text, maxsplit=1)[0]
    if re.search(r"解释|怎么|如何|如果|假如|假设|引用|转发|不要|别", head):
        return False
    if extract_reminder_body(text, replacement=True, identity=identity) is not None:
        return False
    parts = [x.strip() for x in re.split(r"[，,。；;]", text) if x.strip()]
    if not parts or any(not _ONCE.fullmatch(x) for x in parts[1:]):
        return False
    match = re.fullmatch(
        r"(?:请)?(?:把|将)?[^，。]{0,100}?(?:提醒|通知|安排)(?:的?(?:时间|时刻|周期|频率|间隔))?(?:改到|改至|改为|改成|调整到|调整为|推迟到?|提前到?|延后到?|顺延到?)\s*(.+)",
        parts[0],
    )
    return bool(
        match
        and _TIME_SHELL.fullmatch(match[1])
        and re.search(r"点|时|分|秒|天|周|日|月|年|星期", match[1])
    )


def get_reminder_body(scope: Mapping[str, Any]) -> str | None:
    """Read a controller-owned Work body; never accept an unbound text hint."""
    body = scope.get("reminder_body")
    if body is None:
        return None
    source = scope.get("reminder_body_source_event_id")
    fingerprints = scope.get("source_fingerprints")
    if (
        scope.get("reminder_only") is not True
        or scope.get("reminder_body_literal") is not True
        or not isinstance(body, str)
        or not body.strip()
        or len(body) > MAX_REMINDER_BODY
        or "\x00" in body
        or not isinstance(source, str)
        or not source
        or not isinstance(fingerprints, dict)
        or source not in fingerprints
    ):
        raise DomainError("scope_violation", "Literal reminder body lacks its authorized source")
    return body


def occurrence_reminder_contract(
    goal: str, scope: Mapping[str, Any], criteria: Sequence[str]
) -> tuple[dict[str, Any], list[str]]:
    """Materialize body only on Work; Schedule.goal remains its sole text store."""
    copied = dict(scope)
    copied.pop("reminder_body", None)
    checks = list(criteria)
    if copied.get("reminder_body_literal") is True:
        copied["reminder_body"] = goal
        get_reminder_body(copied)
        # The literal body is notification data, never another semantic work
        # criterion (which could accidentally request scheduling/compute tools).
    return copied, checks
