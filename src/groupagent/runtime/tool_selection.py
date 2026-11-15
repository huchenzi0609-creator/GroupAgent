"""Small conservative catalogs for already imported inputs; not an authority layer.

Call after input import and feature filtering, immediately before ExecutionRequest.
Only controller Work fields and validated input formats are inspected. Input text,
filenames and provider output never choose tools. Ambiguous/complex work keeps the
general catalog; required tools must already exist in the supplied eligible set.
"""

from __future__ import annotations

import re
from collections.abc import Iterable, Mapping, Sequence
from typing import Any

from groupagent.conversation.intent import (
    main_request,
    requests_speech_reply,
    requires_research,
    task_write_request,
)
from groupagent.domain import DomainError, WorkOrder
from groupagent.identity import DEFAULT_IDENTITY, BotIdentity
from groupagent.scheduling.reminders import get_reminder_body

_IMAGE = frozenset({"png", "jpg", "jpeg", "webp", "gif"})
_AUDIO = frozenset({"wav", "mp3", "ogg", "flac", "m4a", "aac"})
_IMAGE_TOOLS = frozenset({"request_input", "image_analyze", "media_ocr", "provider_ocr"})
_AUDIO_TOOLS = frozenset({"request_input", "audio_transcribe"})
_WEB_TOOLS = frozenset({"request_input", "web_search", "web_fetch", "browser_open"})
_REMINDER_TOOLS = frozenset({"request_input"})
_DOCUMENT_FORMATS = frozenset({"md", "docx", "pdf", "pptx"})
_DOCUMENT_TOOLS = frozenset(
    {
        "request_input",
        "document_generate",
        "document_read",
        "document_search",
        "document_edit",
        "document_validate",
        "document_extract_image",
        "pdf_combine",
        "chart_render",
        "artifact_search",
        "artifact_import",
        "artifact_export",
        "artifact_from_workspace",
        "artifact_deliver",
        "workspace_read_text",
        "workspace_write_text",
        "python_compute",
        "process_run",
        "shell_command",
    }
)
_MEDIA_REQUIREMENT = re.compile(
    r"图片|图像|照片|插图|插画|语音|音频|转写|转录|视频|浏览器|image|photo|illustrat|audio|speech|transcrib|ocr|video|browser",
    re.I,
)
_TEAM_WRITES = frozenset({"team_task_create", "team_task_claim", "team_task_update"})
_SCHEDULE_WRITES = frozenset(
    {"schedule_create", "schedule_update", "schedule_control", "trigger_create", "trigger_control"}
)

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
_BUSINESS = re.compile(
    r"登记|认领|分配|分工|落库|负责人|群成员|群昵称|团队|查(?:询|看).{0,12}(?:任务|成员|名单|日程|提醒)"
    r"|更新.{0,12}(?:任务|进度)|标记.{0,12}完成|team\s*task|assign|claim\s*(?:a\s*)?task|roster",
    re.I,
)
_SCHEDULING = re.compile(
    r"提醒|通知|日程|定时|到期|安排.{0,12}(?:时间|任务)|(?:每天|每周|每月|每小时).{0,20}(?:执行|运行|搜索|生成|发来)"
    r"|(?:完成|失败|收到|上传).{0,20}(?:后|时|触发)|schedule|remind|recurr|follow.up",
    re.I,
)
_TRANSCRIPTION = re.compile(r"转写|转录|逐字|语音识别|听写|transcrib|speech.to.text|\basr\b", re.I)
_REMINDER_GOAL = re.compile(
    r"^(?:(?:请|到时|届时|到点后|按时|定时|定期)[，,:：\s]*)*(?:提醒(?:我|我们|大家|成员)?|通知(?:我|我们|大家)|remind\b|notify\b)",
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
            "tool_not_registered", "A required tool is absent from the eligible catalog"
        )
    return required


def _text_only(work: WorkOrder) -> bool:
    outputs = work.scope.get("outputs", [])
    return (
        isinstance(outputs, list)
        and all(isinstance(item, dict) and item.get("format") == "text" for item in outputs)
        and not any(check.startswith("artifact:") for check in work.required_checks)
        and not work.scope.get("speech_requested")
    )


def _requires_memory(text: str) -> bool:
    # Only suppress this narrow ban on an unsupported knowledge source. Other
    # mentions of remembered facts or actual conversation history remain needs.
    return bool(_MEMORY.search(_MEMORY_SOURCE_PROHIBITION.sub("", text)))


def select_tools(
    work: WorkOrder,
    input_refs: Sequence[Mapping[str, Any]],
    registered_names: Iterable[str],
    *,
    identity: BotIdentity = DEFAULT_IDENTITY,
) -> frozenset[str]:
    """Return only existing eligible names; never alter grants or budgets.

    reminder_only is a controller hint for a currently authorized reminder goal.
    Rule updates must recompute it. It must not come from file/OCR content.
    """
    available = frozenset(registered_names)
    if any(not isinstance(name, str) or not name for name in available):
        raise DomainError("invalid_input", "Eligible tool names must be nonempty strings")
    required = _required(work, available)
    goal = work.goal.strip()
    criteria = "\n".join(work.acceptance_criteria)
    requirements = "\n".join((goal, criteria))
    derived = bool(work.scope.get("origin_schedule_id") or work.scope.get("origin_trigger_rule_id"))
    team_write = work.scope.get("team_task_write_allowed") is True
    research = work.scope.get("research_required") is True
    business = team_write or bool(_BUSINESS.search(requirements)) or _requires_memory(requirements)
    text_only = _text_only(work)
    raw_formats = [ref.get("format") if isinstance(ref, Mapping) else None for ref in input_refs]
    formats = [value.casefold() if isinstance(value, str) else None for value in raw_formats]
    profile: frozenset[str] | None = None
    if work.scope.get("research_message_only") is True and work.goal_revision == 1:
        if (
            not research
            or not text_only
            or input_refs
            or work.input_versions
            or work.scope.get("attachment_event_ids")
            or derived
            or team_write
        ):
            raise DomainError("scope_violation", "Message-only research scope is inconsistent")
        if not required.issubset(_WEB_TOOLS):
            raise DomainError(
                "permission_denied", "Message-only research cannot require file or execution tools"
            )
        return available & _WEB_TOOLS

    # A reminder tells the requester to act. Its quoted/action text is not an
    # instruction for the agent to execute Python, inspect files or do that job.
    reminder = derived and (
        work.scope.get("reminder_only") is True or bool(_REMINDER_GOAL.search(goal))
    )
    literal_reminder = (
        reminder
        and type(work.scope.get("derived_depth")) is int
        and work.scope["derived_depth"] == 1
        and work.scope.get("reminder_body_literal") is True
        and get_reminder_body(work.scope) == goal
    )
    if (
        reminder
        and text_only
        and not research
        and not team_write
        and (literal_reminder or not identity.additional_request(goal))
        and not _requires_memory(criteria)
        and not requires_research(criteria)
        # Criteria describe work the agent must actually satisfy, even when
        # the short goal only asks it to remind the requester.
        and not any(
            pattern.search(criteria)
            for pattern in (
                _COMPUTE,
                _FILE_OUTPUT,
                _BUSINESS,
                _SCHEDULING,
                _TRANSCRIPTION,
            )
        )
        and required.issubset(_REMINDER_TOOLS)
    ):
        # A source-bound literal notification already has its complete body and
        # recipient authorization. Offering request_input here can turn an
        # optional follow-up question into a blocking material requirement.
        profile = frozenset() if literal_reminder and not required else _REMINDER_TOOLS
    elif (
        text_only
        and not business
        and not _COMPUTE.search(requirements)
        and not _FILE_OUTPUT.search(requirements)
        and not _SCHEDULING.search(requirements)
    ):
        if (
            formats
            and all(format in _IMAGE for format in formats)
            and not research
            and not requires_research(requirements)
        ):
            profile = _IMAGE_TOOLS
        elif (
            formats
            and all(format in _AUDIO for format in formats)
            and _TRANSCRIPTION.search(requirements)
            and not research
            and not requires_research(requirements)
        ):
            profile = _AUDIO_TOOLS
        elif (
            research
            and not input_refs
            and not work.input_versions
            and not work.scope.get("attachment_event_ids")
        ):
            profile = _WEB_TOOLS

    outputs = work.scope.get("outputs", [])
    if (
        profile is None
        and isinstance(outputs, list)
        and bool(outputs)
        and all(
            isinstance(item, dict) and item.get("format") in _DOCUMENT_FORMATS for item in outputs
        )
        and all(format in _DOCUMENT_FORMATS for format in formats)
        and not (not input_refs and (work.input_versions or work.scope.get("attachment_event_ids")))
        and not business
        and not research
        and not work.scope.get("speech_requested")
        and not _MEDIA_REQUIREMENT.search(requirements)
        and not requires_research(requirements)
        and not _SCHEDULING.search(requirements)
        and required.issubset(_DOCUMENT_TOOLS)
    ):
        # This retains all native editing/version routes, scoped workspace access,
        # Python/process/shell, charts and PDF composition. It only removes
        # unrelated catalogs; an extra required capability keeps the general set.
        profile = _DOCUMENT_TOOLS

    if profile is not None and required.issubset(profile):
        return (available & profile) | required

    # Complex workflows retain media/file/computation/retrieval routes. These
    # exclusions only reduce irrelevant writes; actual invocation still fences
    # scope, identity and lease in the existing gateway/backend.
    selected = set(available)
    if not team_write or derived:
        selected.difference_update(_TEAM_WRITES)
    # An eligible authorized event follow-up need not use our schedule keywords.
    # Keep its existing tools in the general catalog; the backend still checks
    # the creator, source, lease, quota and derived-depth restriction.
    if derived or work.scope.get("schedule_creation_allowed") is not True:
        selected.difference_update(_SCHEDULE_WRITES)
    return frozenset(selected | required)
