"""Trusted tool authority and a small, semantic initial description set.

Authorized tools come from the host's registered, feature-filtered catalog and
source-bound business grants. Goal text, acceptance prose, files and model output
never grant or remove authority. The agent can discover every remaining authorized
tool with the catalog loader instead of receiving every schema at once.
"""

from __future__ import annotations

import re
from collections.abc import Iterable, Mapping, Sequence
from typing import Any

from pydantic import TypeAdapter, ValidationError

from groupagent.control import WorkCapability
from groupagent.conversation.image_requirements import is_public_image_work
from groupagent.conversation.intent import (
    main_request,
    requests_speech_reply,
    requires_research,
    task_write_request,
)
from groupagent.domain import ActorContext, DomainError, Role, WorkOrder
from groupagent.evolution import EVOLUTION_OWNER_TOOLS, EVOLUTION_TOOLS
from groupagent.identity import DEFAULT_IDENTITY, BotIdentity
from groupagent.scheduling.reminders import get_reminder_body

from .tool_profiles import names_for_capabilities

_IMAGE = frozenset({"png", "jpg", "jpeg", "webp", "gif"})
_AUDIO = frozenset({"wav", "mp3", "ogg", "flac", "m4a", "aac"})
_DOCUMENT_FORMATS = frozenset({"md", "docx", "pdf", "pptx"})
_TEAM_WRITES = frozenset({"team_task_create", "team_task_claim", "team_task_update"})
_SCHEDULE_WRITES = frozenset(
    {
        "schedule_create",
        "schedule_update",
        "schedule_control",
        "trigger_create",
        "trigger_control",
    }
)
_NOTIFICATION_TOOLS = frozenset({"request_input"})


_COMPUTE = re.compile(
    r"(?:运行|执行|调用|启动|使用|采用|通过|用)\s*(?:受控的?\s*)?(?:python|shell)"
    r"(?!\s*(?:官方|文档|教程|手册|语法|docs|documentation|manual))"
    r"|(?:python|shell)\s*(?:实际)?(?:代码|脚本|程序|计算|算|检查|核验|验证|运行|执行|处理|绘制|绘图)"
    r"|(?:run|execute|invoke|use)\s+(?:the\s+)?(?:python|shell)\b"
    r"(?!\s*(?:official|docs|documentation|manual))"
    r"|运行.{0,12}(?:代码|进程|程序)|执行.{0,12}(?:代码|命令|进程)|用.{0,8}程序",
    re.I,
)
_FILE_OUTPUT = re.compile(
    r"(?:生成|制作|保存|导出|写成|整理成|转换成|转换为|做成|发来|发给我|给我一份).{0,30}"
    r"(?:文件|文档|报告|表格|图表|流程图|图片|语音|音频|pdf|word|docx|pptx|ppt|markdown|\.md|\.txt|\.csv|\.png|\.wav)"
    r"|(?:create|generate|save|export|write).{0,30}(?:file|document|report|chart|diagram|audio|pdf|docx|pptx|markdown)",
    re.I,
)
_MEMORY = re.compile(
    r"记忆|记住|记下|忘记|更正记录|历史记录|聊天记录|群聊|(?:昨天|昨日|前天|昨晚).{0,6}(?:聊天|讨论)"
    r"|上次|刚才|之前|以前|remember|memory|previous|earlier",
    re.I,
)
_MEMORY_SOURCE_PROHIBITION = re.compile(
    r"(?:不要|不得|不能|不许|禁止|不)\s*(?:仅|只)?\s*(?:凭|依赖)\s*(?:仅|只)?\s*记忆"
)
_SCHEDULING = re.compile(
    r"提醒|通知|日程|定时|到期|安排.{0,12}(?:时间|任务)|(?:每天|每周|每月|每小时).{0,20}(?:执行|运行|搜索|生成|发来)"
    r"|(?:完成|失败|收到|上传).{0,20}(?:后|时|触发)|schedule|remind|recurr|follow.up",
    re.I,
)


def message_only_research_request(text: str, *, identity: BotIdentity = DEFAULT_IDENTITY) -> bool:
    """A canonical information request cannot implicitly authorize a new file.

    URLs, code and quoted examples are data. Explicit file/report/format clauses
    remain file requests; ambiguous or operational goals keep their existing plan.
    """
    if not text.strip() or len(text) > 12_000:
        return False
    outside = re.sub(
        r'```.*?```|~~~.*?~~~|“[^”]*”|「[^」]*」|『[^』]*』|《[^》]*》|"[^"\n]*"|\'[^\'\n]*\'',
        " <quoted data> ",
        text,
        flags=re.S,
    )
    if not requires_research(outside):
        return False
    outside = re.sub(r'https?://[^\s<>，。；！？“”「」『』"\'\u4e00-\u9fff]+', " <url> ", outside)
    if requests_speech_reply(outside):
        return False
    if (
        _COMPUTE.search(outside)
        or task_write_request(outside, identity=identity)
        or _SCHEDULING.search(outside)
        or _requires_memory(outside)
    ):
        return False
    for clause in re.split(
        r"[，,。；;\n]|\b(?:and|then|also)\b|并且|同时|然后|最后|另外|"
        r"并(?=生成|制作|保存|导出|写成|输出|交付|提供|撰写|形成)",
        outside,
        flags=re.I,
    ):
        clause = main_request(
            re.sub(r"^(?:并且|并|同时|然后|最后|另外)\s*", "", clause.strip()),
            identity=identity,
        )
        if _FILE_OUTPUT.search(clause) or re.search(
            r"(?:输出|交付|提供|撰写|写一份|形成).{0,24}(?:文件|报告|文档|附件)"
            r"|(?:结果|输出|交付|格式).{0,12}(?:Markdown|PDF|Word|DOCX|PPTX?|CSV|\.md\b)"
            r"|(?:以|采用|用)\s*(?:Markdown|PDF|Word|DOCX|PPTX?|CSV)\s*(?:文件|文档|格式)"
            r"|\b(?:as|in|using)\s+(?:a\s+)?(?:PDF|Markdown|Word|DOCX|PPTX|CSV)\s+(?:file|report|format)\b"
            r"|(?:保存|导出|写入)(?:为|成|到)?\s*<quoted data>",
            clause,
            re.I,
        ):
            return False
    return bool(
        re.search(
            r"说明|解释|回答|比较|对比|区别|差异|简短|简要|总结|概括|摘要|summari[sz]|explain|difference|compare|brief|answer",
            outside,
            re.I,
        )
    )


def _requires_memory(text: str) -> bool:
    # Used only by message_only_research_request's existing output-admission
    # compatibility rule, never by authority or schema visibility selection.
    return bool(_MEMORY.search(_MEMORY_SOURCE_PROHIBITION.sub("", text)))


def _names(values: Iterable[str]) -> frozenset[str]:
    names = frozenset(values)
    if any(not isinstance(name, str) or not name for name in names):
        raise DomainError("invalid_input", "Eligible tool names must be nonempty strings")
    return names


def _required(work: WorkOrder, available: frozenset[str]) -> frozenset[str]:
    raw = work.scope.get("required_tools", [])
    if (
        not isinstance(raw, list)
        or len(raw) > 100
        or any(not isinstance(name, str) or not name.strip() for name in raw)
    ):
        raise DomainError("invalid_input", "Required tools must be a bounded list of names")
    required = frozenset(str(name) for name in raw)
    if not required.issubset(available):
        raise DomainError(
            "tool_not_registered", "A required tool is absent from the authorized catalog"
        )
    return required


def _derived(work: WorkOrder) -> bool:
    depth = work.scope.get("derived_depth", 0)
    if type(depth) is not int or depth < 0:
        raise DomainError(
            "scope_violation", "Derived work depth must be a trusted nonnegative integer"
        )
    return bool(
        depth or work.scope.get("origin_schedule_id") or work.scope.get("origin_trigger_rule_id")
    )


def _notification_only(work: WorkOrder) -> bool:
    """Match the controller's actual occurrence intent, never an orphan hint."""
    origin = work.scope.get("origin_schedule_id") or work.scope.get("origin_trigger_rule_id")
    return (
        work.scope.get("reminder_only") is True
        and isinstance(origin, str)
        and bool(origin.strip())
        and type(work.scope.get("derived_depth")) is int
        and work.scope["derived_depth"] == 1
    )


def _literal_notification(work: WorkOrder) -> bool:
    return (
        _notification_only(work)
        and work.scope.get("reminder_body_literal") is True
        and get_reminder_body(work.scope) == work.goal
    )


def authorized_tools(
    work: WorkOrder,
    registered_names: Iterable[str],
    *,
    actor: ActorContext,
) -> frozenset[str]:
    """Intersect trusted registration with actual source/role business grants.

    The caller first filters disabled features and unavailable routes. Native
    gateway handlers still check actor, lease, group and individual source IDs on
    every invocation. A semantic capability or required-tool plan is not a grant.
    """
    if (actor.group_id, actor.member_id) != (work.group_id, work.requester_id):
        raise DomainError(
            "scope_violation", "Tool authority requires the exact requester work scope"
        )
    if actor.role not in {Role.MEMBER, Role.OWNER}:
        raise DomainError("permission_denied", "A read-only actor cannot execute background tools")
    eligible = _names(registered_names)
    selected = set(eligible)
    derived = _derived(work)
    if derived:
        selected.difference_update(EVOLUTION_TOOLS)
    elif actor.role != Role.OWNER:
        selected.difference_update(EVOLUTION_OWNER_TOOLS)
    if derived or work.scope.get("team_task_write_allowed") is not True:
        selected.difference_update(_TEAM_WRITES)
    if derived or work.scope.get("schedule_creation_allowed") is not True:
        selected.difference_update(_SCHEDULE_WRITES)
    if _notification_only(work):
        # The notification body is the recipient's job. Neither words such as
        # Python nor separate prose criteria turn it into agent delegation.
        selected.intersection_update(_NOTIFICATION_TOOLS)
        if _literal_notification(work):
            selected.clear()
    authorized = frozenset(selected)
    required = _required(work, eligible)
    if not required.issubset(authorized):
        raise DomainError(
            "permission_denied", "Required tools exceed the source's actual tool grant"
        )
    return authorized


def _planned_capabilities(work: WorkOrder) -> frozenset[WorkCapability]:
    raw = work.scope.get("work_capabilities", [])
    if not isinstance(raw, list) or len(raw) > 8:
        raise DomainError("invalid_input", "Stored work capability plan is invalid")
    try:
        return frozenset(TypeAdapter(list[WorkCapability]).validate_python(raw, strict=True))
    except ValidationError:
        raise DomainError("invalid_input", "Stored work capability plan is invalid") from None


def research_evidence_required(work: WorkOrder) -> bool:
    """Use the semantic plan, retaining the old flag only for no-plan work.

    Older records always stored default empty capability arrays. A nonempty valid
    plan is the authoritative execution intent; stale lexical flags cannot turn a
    compute/document task into a web-research task. This changes evidence duties,
    never the source/role tool allowlist.
    """
    capabilities = _planned_capabilities(work)
    if capabilities:
        return bool(capabilities & {"public_research", "public_images"})
    return work.scope.get("research_required") is True


def select_tools(
    work: WorkOrder,
    input_refs: Sequence[Mapping[str, Any]],
    registered_names: Iterable[str],
    *,
    identity: BotIdentity = DEFAULT_IDENTITY,
) -> frozenset[str]:
    """Choose initial descriptions only, from an already authorized catalog.

    Missing/old plans start with a few navigation tools and can discover another
    family. Imported formats and structured outputs may add relevant descriptions;
    their names, textual contents, goal spelling and criteria never select tools.
    ``identity`` remains accepted for callers upgrading from the old planner.
    """
    available = _names(registered_names)
    required = _required(work, available)
    planned = _planned_capabilities(work)
    capabilities = set(planned)
    outputs = work.scope.get("outputs", [])
    if isinstance(outputs, list) and any(
        isinstance(item, dict) and item.get("format") in _DOCUMENT_FORMATS for item in outputs
    ):
        capabilities.add("documents")
    for reference in input_refs:
        format_name = reference.get("format") if isinstance(reference, Mapping) else None
        if not isinstance(format_name, str):
            continue
        format_name = format_name.casefold()
        if format_name in _IMAGE:
            capabilities.add("image_analysis")
        elif format_name in _AUDIO:
            capabilities.add("audio_transcription")
        elif format_name in _DOCUMENT_FORMATS:
            capabilities.add("documents")
    if not planned and research_evidence_required(work):
        capabilities.add("public_research")
    if is_public_image_work(work):
        capabilities.add("public_images")
    if work.scope.get("team_task_write_allowed") is True:
        capabilities.add("team_tasks")
    return names_for_capabilities(capabilities, available) | required
