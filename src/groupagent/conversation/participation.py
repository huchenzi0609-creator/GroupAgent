"""Small, shared participation guards over trusted roster/context evidence.

No role is inferred from text. Ambiguous names or outer requests fall back to
normal interpretation; a source quotation never authorizes a business write.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

from groupagent.coordinator import Decision
from groupagent.domain import EventKind, GroupEvent

from .timeparse import normalize_time

_EXPLANATION = re.compile(
    r"^(?:请|麻烦(?:你)?|帮我)?\s*(?:解释|翻译|说明|分析|总结|评价|判断|比较|如何|怎么|为什么)|是什么意思|有什么(?:区别|不同)"
)
_QUOTED = re.compile(r"```.*?```|『[^』]*』|「[^」]*」|“[^”]*”|\"[^\"]*\"|(?m:^>[^\n]*)", re.S)
_OUTER_ASK = re.compile(r"解释|翻译|分析|总结|评价|判断|比较|核对|为什么|如何|怎么|[?？]")
_GROUP = re.compile(r"全群|本群|整个群|这个群|当前群|群内|群里所有|群的|群参与|群讨论")
_PARTICIPATION = re.compile(
    r"安静|静默|插话|打扰|参与|回应|回复|处理|助手|机器人|主动.{0,12}(?:回应|回复|发言|补充|提醒|交流|建议)|(?:恢复|正常|自然).{0,5}(?:交流|参与)|(?:处理|启用|停用).{0,12}(?:群|消息)"
)
_SOURCE_LABEL = re.compile(r"^(?:引用|引述|转述|转发)(?:的)?(?:原文|原话|内容|消息)?\s*[：:]")
_SOURCE_DISCLAIMER = re.compile(
    r"(?:(?:这|以上|上述)(?:句话|段话|内容)?(?:并|都|也)?)?"
    r"(?:不是|不代表|不表示|并非)(?:我|本人|我们)(?:的)?(?:在|要|已经)?"
    r"(?:认领|接单|承诺|授权|分工|安排|请求|指令)(?:任务|工作)?"
)
_PLAIN_STATE = re.compile(
    r"[^，,。.!！?？:：;；@/\n\"“”『』「」`]{0,60}"
    r"(?:有点|有些|有一点|挺|比较|很|太|真)"
    r"(?:冷|热|忙|累|困|难|快|慢|多|少|紧|赶|乱|吵|复杂|辛苦|吃力|疲惫|紧张|费劲)"
    r"(?:了|的|啊|呀|呢)?[。！!\s]*"
)
_NON_CASUAL_STATE = re.compile(
    r"请|麻烦|拜托|帮|求|建议|推荐|解释|翻译|分析|总结|整理|计算|生成|制作|画|写|改|换|调整|撤|删|"
    r"停止|取消|暂停|继续|恢复|重试|记住|记下|忘记|分配|安排|分工|认领|负责|承担|接手|来做|准备|"
    r"我来|我接|我做|由我|算我|进度|完成|提交|汇报|"
    r"截止|延期|推迟|提醒|需要|希望|想要|能否|可以|能不能|是不是|是否|难道|吗|么|谁|什么|哪|为何|你|您|"
    r"任务|报告|文档|幻灯|表格|摘要|图表|稿|PPT|PDF|Word|"
    r"活着|活下去|自杀|想死|伤害|救命|自残|受伤|疼|痛|报警|急救|危险|绝望",
    re.I,
)


def _outside(text: str) -> str:
    return _QUOTED.sub(" ", text).strip()


def _agent_request(text: str, context: dict[str, Any]) -> bool:
    aliases = context.get("agent_aliases", ["机器人", "机器人组长", "组长", "小伴"])
    aliases = (
        [value for value in aliases if isinstance(value, str) and value]
        if isinstance(aliases, list)
        else []
    )
    if text.startswith("/组长"):
        return True
    for name in aliases:
        literal = re.escape(name)
        if re.search(r"(?:请|麻烦|拜托)\s*@?" + literal, text) or re.search(
            literal + r"[，,:：\s]*(?:请|帮|麻烦|能否|可以|解释|翻译|分析|总结|回答|继续|把)", text
        ):
            return True
    return False


def _source_quote_only(text: str, outside: str) -> bool:
    """Match the entire source wrapper, never an instruction after the quote."""
    if not _QUOTED.search(text):
        return False
    label = _SOURCE_LABEL.match(outside)
    if label is None:
        return False
    rest = outside[label.end() :].strip(" \t\n，,。.!！;；")
    return not rest or _SOURCE_DISCLAIMER.fullmatch(rest) is not None


def _has_outer_action(text: str) -> bool:
    """An outer command needs interpretation; quoted instructions grant nothing."""
    # A removed quote is a clause boundary, so a following command need not have
    # punctuation. Keep whitespace inside clauses to preserve "不要 执行".
    outside = _QUOTED.sub("，", text)
    for clause in re.split(r"[，,。;；:：\n!?！？]", outside):
        clause = re.sub(
            r"^(?:(?:但是|但|不过|然后|现在|还是|请|麻烦(?:你)?|拜托|你|您)\s*)+",
            "",
            clause.strip(),
        )
        if re.match(r"^(?:不要|别|不用|无需|不能|不必|不得|不应|不是让你|没有让你)", clause):
            continue
        if re.match(
            r"(?:把|将).{0,100}(?:整理|生成|制作|转换|改|写|存|导出|删|登记|给我|发给)"
            r"|(?:按|照|按照|依照).{0,100}(?:做|执行|办理|处理|给我|修改|登记|安排|分配)"
            r"|(?:帮我|替我|为我|给我|整理|生成|制作|保存|导出|执行|办理|登记|修改|删除|撤销|安排|提醒|发给我)"
            r"|(?:这(?:个|项|部分).{0,8})?我(?:来|负责|认领|接手|承担)",
            clause,
        ):
            return True
    return False


def _standalone_state(event: GroupEvent, text: str, context: dict[str, Any]) -> bool:
    """A bounded everyday observation does not itself ask for help or a check-in.

    Replies, artifacts, work feedback, questions and uncertain context remain for
    interpretation. This is deliberately narrower than a general intent parser.
    """
    if (
        event.kind != EventKind.TEXT
        or event.reply_to
        or event.attachments
        or context.get("quoted_work_id")
        or context.get("context_incomplete")
        or not _PLAIN_STATE.fullmatch(text)
        or _NON_CASUAL_STATE.search(text)
    ):
        return False
    aliases = context.get("agent_aliases", ["机器人", "机器人组长", "组长", "小伴"])
    if isinstance(aliases, list) and any(
        isinstance(alias, str) and alias and alias in text for alias in aliases
    ):
        return False
    # An unthreaded answer to a recent question can still continue a request.
    recent = context.get("recent_messages", [])
    if isinstance(recent, list) and any(
        isinstance(row, dict)
        and row.get("id") != event.id
        and isinstance(row.get("text"), str)
        and _OUTER_ASK.search(row["text"])
        for row in recent[-2:]
    ):
        return False
    for name in ("works", "tasks", "schedules"):
        rows = context.get(name, [])
        if isinstance(rows, list) and any(
            isinstance(row, dict)
            and isinstance(row.get("title"), str)
            and len(row["title"]) >= 2
            and row["title"] in text
            for row in rows
        ):
            return False
    return True


def fast_observe(event: GroupEvent, context: dict[str, Any]) -> Decision | None:
    """Recognize only high-certainty no-intervention windows, without a model call."""
    if (
        event.kind not in {EventKind.TEXT, EventKind.QUOTE, EventKind.FORWARDED}
        or len(event.text) > 16000
    ):
        return None
    text = event.text.strip()
    outside = _outside(text)
    explicit_data = bool(
        re.search(
            r"(?:只是|仅仅|仅|单纯).{0,4}(?:引用|转发|例子|示例)|假例子|(?:举的|中的)(?:例子|示例)|(?:这是|下面是).{0,4}(?:引用|转发)",
            outside,
        )
    )
    if (
        (event.kind == EventKind.FORWARDED or explicit_data or _source_quote_only(text, outside))
        and not _OUTER_ASK.search(outside)
        and not _has_outer_action(text)
        and not _agent_request(outside, context)
    ):
        return Decision(
            kind="observe",
            reason_code="reference_data_without_outer_request",
            directness="none",
            confidence=1,
        )
    if _EXPLANATION.search(outside) or _agent_request(outside, context):
        return None
    if _standalone_state(event, text, context):
        return Decision(
            kind="observe",
            reason_code="standalone_state_without_request",
            directness="none",
            confidence=1,
        )
    # One-off refusal to involve the bot is not a persistent quiet policy and is
    # not a cancellation of a quoted, already accepted work.
    if (
        not event.reply_to
        and not context.get("quoted_work_id")
        and not _OUTER_ASK.search(outside)
        and not re.search(
            r"主动|以后|每天|接下来|安静|静默|模式|全群|恢复|开启|关闭|停止|取消|暂停", outside
        )
    ):
        if re.search(
            r"(?:机器人|组长|小伴)[，,:：\s]*(?:先)?(?:别|不要|不用)(?:接|接手|参与这事|回复这条)",
            outside,
        ):
            return Decision(
                kind="observe",
                reason_code="explicit_one_off_nonintervention",
                directness="none",
                confidence=1,
            )
    if context.get("role") not in {"member", "observer", "maintainer"}:
        return None  # An owner's explicit human assignments are preauthorized.
    requester = context.get("requester_member_id")
    if not isinstance(requester, str):
        return None
    names: dict[str, set[str]] = {}
    members = context.get("members", [])
    for member in members if isinstance(members, list) else ():
        if (
            not isinstance(member, dict)
            or member.get("name_source") not in {"roster", "roomDisplayName", "owner_confirmed"}
            or member.get("membership_state") not in {"active", "present"}
        ):
            continue
        name, identifier = member.get("name"), member.get("member_id")
        if isinstance(name, str) and name and isinstance(identifier, str):
            names.setdefault(name, set()).add(identifier)
    aliases = context.get("agent_aliases", [])
    excluded = {
        "我",
        "你",
        "他",
        "她",
        "它",
        "大家",
        "同学",
        "群昵称待补充",
        "机器人",
        "机器人组长",
        "组长",
        "小伴",
    }
    if isinstance(aliases, list):
        excluded.update(value for value in aliases if isinstance(value, str))
    for name, identities in names.items():
        if len(identities) != 1 or requester in identities or name in excluded:
            continue
        if re.match(
            r"^(?:(?:请|麻烦|拜托)\s*)?@?" + re.escape(name) + r"(?=[，,：:\s]|先|帮|来|你)",
            outside,
        ):
            return Decision(
                kind="observe",
                reason_code="addressed_to_verified_other_member",
                directness="other",
                confidence=1,
            )
    return None


def owner_assignment_request(text: str, context: dict[str, Any]) -> bool:
    """An owner may need an assignee clarification before an action can exist."""
    outside = _outside(text)
    if (
        context.get("role") != "owner"
        or _EXPLANATION.search(outside)
        or re.search(r"如果|假如|假设|举例|假例子|仅供参考|不要登记", outside)
    ):
        return False
    return bool(
        re.search(
            r"^(?:请|麻烦|拜托)?(?:我(?:决定|安排)|负责人(?:安排|决定)[：:]?)?(?:让.{1,80}负责|给.{1,80}(?:安排|分配)|把.{1,80}(?:交给|分配给)|安排.{1,80}负责|由.{1,80}负责)",
            outside,
        )
    )


@dataclass(frozen=True)
class PolicyIntent:
    operation: str
    subject: str
    transition: str
    until: str | None = None
    hours: tuple[str, ...] | None = None

    def action(self) -> dict[str, Any]:
        if self.operation == "participation":
            return {"op": "participation", "mode": self.transition}
        value: dict[str, Any] = {
            "op": "quiet",
            "scope": self.subject,
            "enabled": self.transition == "pause_proactive",
        }
        if self.until is not None:
            value["until"] = self.until
        if self.hours is not None:
            value["hours"] = list(self.hours)
        return value


def policy_intent(event: GroupEvent, context: dict[str, Any]) -> PolicyIntent | None:
    """Resolve explicit participation-policy semantics before mapping boolean fields."""
    if event.kind not in {EventKind.TEXT, EventKind.QUOTE} or len(event.text) > 2000:
        return None
    text = _outside(event.text)
    if not text or _EXPLANATION.search(text) or not _PARTICIPATION.search(text):
        return None
    if re.search(
        r"假设|如果|举例|只是.{0,4}(?:引用|转发)|(?:同时|顺便|另外).{0,8}(?:制作|生成|整理|安排|提醒|计算)|(?:帮我|请你).{0,5}(?:做|制作|生成|分析|计算)",
        text,
    ):
        return None
    subject = "group" if _GROUP.search(text) else "self"
    if subject == "group" and re.search(
        r"(?:停止|停用|关闭).{0,10}(?:处理|助手)|停止处理.{0,8}消息", text
    ):
        return PolicyIntent("participation", "group", "disabled")
    if subject == "group" and re.search(
        r"重新启用|(?:恢复|开启|启用).{0,8}(?:处理|助手|机器人)|恢复.{0,8}自然参与模式|切换.{0,8}自然(?:参与)?模式",
        text,
    ):
        return PolicyIntent("participation", "group", "natural")
    if (
        subject == "group"
        and not re.search(r"安静|静默", text)
        and re.search(r"以后|模式|只有|仅在|只在", text)
        and re.search(r"明确.{0,8}(?:叫|呼叫|点名)|(?:被|收到)@", text)
    ):
        return PolicyIntent("participation", "group", "on_request")
    resume = bool(
        re.search(
            r"不用再静默|不再静默|解除静默|结束静默|别再?安静|恢复.{0,8}(?:正常交流|正常参与|主动|回应)|(?:继续|开启|打开|可以).{0,8}主动|不要关闭.{0,8}主动",
            text,
        )
    )
    pause = bool(
        re.search(
            r"保持安静|先安静|请安静|静默|(?:不要|不用|别|停止|暂停|关闭).{0,12}(?:主动|插话|打扰)|主动.{0,8}(?:关闭|暂停|停止)|没叫你.{0,8}(?:别|不)回应|不要恢复.{0,8}主动",
            text,
        )
    )
    if re.search(r"不要恢复.{0,8}主动", text):
        resume, pause = False, True
    if resume:
        return PolicyIntent("quiet", subject, "resume_proactive")
    if not pause:
        return None
    if re.search(r"(?:后|之后).{0,4}(?:再|开始).{0,8}(?:安静|静默)", text):
        return None  # A delayed start is not an immediate quiet-until deadline.
    zone = str(context.get("group_time_zone", "Asia/Shanghai"))
    # Daily windows are separate from a one-shot until. Both clock expressions
    # are resolved by the same deterministic parser, without borrowing dates.
    window = re.search(
        r"(?:每天|每日)\s*(.+?)\s*(?:到|至)\s*(.+?)(?=保持|安静|静默|不要|别|不主动|$)", text
    )
    if window:
        left = normalize_time("今天" + window[1], event.occurred_at, zone)
        right = normalize_time("今天" + window[2], event.occurred_at, zone)
        if left.local_time and right.local_time and left.local_time != right.local_time:
            return PolicyIntent(
                "quiet",
                subject,
                "pause_proactive",
                hours=(left.local_time + "-" + right.local_time,),
            )
        return None
    temporal = normalize_time(text, event.occurred_at, zone)
    if temporal.instant:
        return PolicyIntent("quiet", subject, "pause_proactive", until=temporal.instant)
    duration = re.search(r"(?:半|\d+|[一二两三四五六七八九十]+)(?:小时|分钟)", text)
    if duration and not re.search(r"(?:小时|分钟)后", text):
        temporal = normalize_time(duration.group() + "后", event.occurred_at, zone)
        if temporal.instant:
            return PolicyIntent("quiet", subject, "pause_proactive", until=temporal.instant)
    if re.search(r"直到|安静到|静默到|持续到|今晚|明天|明晚|分钟|小时", text):
        return None  # Do not discard an unresolved explicit time restriction.
    return PolicyIntent("quiet", subject, "pause_proactive")
