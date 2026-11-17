"""Small, shared participation guards over trusted roster/context evidence.

No role is inferred from text. Ambiguous names or outer requests fall back to
normal interpretation; a source quotation never authorizes a business write.
"""

from __future__ import annotations

import re
from typing import Any

from groupagent.coordinator import Decision
from groupagent.domain import EventKind, GroupEvent
from groupagent.identity import BotIdentity

from .policy import PolicyIntent as PolicyIntent
from .policy import policy_intent as policy_intent
from .text import EXPLANATION as _EXPLANATION
from .text import QUOTED as _QUOTED
from .text import outside_quotes as outside_quotes
from .turns import verified_human_addressee as verified_human_addressee


def explicit_policy_intent(event: GroupEvent, context: dict[str, Any]) -> PolicyIntent | None:
    """Reserve synchronous handling for standalone participation controls."""
    if event.kind not in {EventKind.TEXT, EventKind.QUOTE} or _QUOTED.search(event.text):
        return None
    text = BotIdentity.from_context(context).strip_address(event.text).strip(" 。！!\t\n")
    if not re.fullmatch(
        r"(?:(?:请|先|暂时|对我|我这边)\s*)*"
        r"(?:(?:保持(?:安静|静默)|静默|安静|别说话|别插话)(?:[，,]\s*等我叫你)?|解除(?:安静|静默)|"
        r"没叫你就别(?:回应|回复)[，,]\s*(?:先)?(?:安静|静默)|"
        r"(?:不用|无需|不必)再?(?:保持)?(?:安静|静默)(?:了)?|"
        r"恢复正常(?:参与|交流)|(?:暂停|停止|关闭|启用|恢复|开启)全群处理|"
        r"重新启用(?:本群|这个群|当前群|全群))",
        text,
    ):
        return None
    return policy_intent(event, context)


_OUTER_ASK = re.compile(r"解释|翻译|分析|总结|评价|判断|比较|核对|为什么|如何|怎么|[?？]")
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


def _agent_request(text: str, context: dict[str, Any]) -> bool:
    return BotIdentity.from_context(context).request_addressed(text)


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
    if BotIdentity.from_context(context).mentioned(text):
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
    outside = outside_quotes(text)
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
            BotIdentity.from_context(context).name_pattern
            + r"[，,:：\s]*(?:先)?(?:别|不要|不用)(?:接|接手|参与这事|回复这条)",
            outside,
            re.I,
        ):
            return Decision(
                kind="observe",
                reason_code="explicit_one_off_nonintervention",
                directness="none",
                confidence=1,
            )
    return None


def owner_assignment_request(text: str, context: dict[str, Any]) -> bool:
    """An owner may need an assignee clarification before an action can exist."""
    outside = BotIdentity.from_context(context).strip_address(outside_quotes(text))
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
