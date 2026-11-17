"""One recipient and completion contract for a foreground conversation turn.

Channel/roster evidence fixes direction before language interpretation. The small
semantic assessment can understand an outer request, but supplies no identity,
role, tool grant or successful action evidence.
"""

from __future__ import annotations

import json
import re
from typing import Any, Literal, TypeVar

from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator

from groupagent.domain import DomainError, GroupEvent
from groupagent.execution.models import ModelMessage, ModelReply, ModelRequest
from groupagent.execution.ports import ModelPort
from groupagent.identity import BotIdentity, trusted_name_source
from groupagent.security import redact_credentials

from .text import outside_quotes

Recipient = Literal["agent", "other", "ambient"]
ActionKind = Literal["start_work", "update_work", "control_work", "business", "read_work_facts"]


class TurnData(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


AssessmentData = TypeVar("AssessmentData", bound=TurnData)


def _validated(contract: type[AssessmentData], value: dict[str, Any]) -> AssessmentData:
    try:
        return contract.model_validate(value)
    except ValidationError:
        raise DomainError(
            "turn_assessment_invalid",
            "Turn assessment did not satisfy its contract",
            retryable=True,
        ) from None


class RecipientEvidence(TurnData):
    recipient: Recipient = "ambient"
    source: Literal[
        "native_mention",
        "bot_reply",
        "human_reply",
        "human_address",
        "direct_address",
        "continuation",
        "unresolved",
    ] = "unresolved"
    invocation_root_event_id: str | None = None
    member_id: str | None = None

    @property
    def directed(self) -> bool:
        return self.source != "unresolved"


class ActionRequirement(TurnData):
    kind: ActionKind
    operation: str | None = Field(default=None, max_length=80)


class TurnAssessment(TurnData):
    recipient: Recipient
    intent: Literal["conversation", "action", "observe"]
    required_actions: tuple[ActionRequirement, ...] = ()
    work_context_required: bool = Field(
        default=False,
        description="The current question explicitly concerns existing robot work or its metadata.",
    )

    @model_validator(mode="after")
    def action_has_evidence_requirement(self) -> TurnAssessment:
        if (self.intent == "action") != bool(self.required_actions):
            raise ValueError("Only an action turn has required host evidence")
        return self


class TurnContract(TurnAssessment):
    event_id: str
    evidence: RecipientEvidence

    @property
    def visible(self) -> bool:
        return self.recipient == "agent" and self.intent != "observe"

    @property
    def directness(self) -> Literal["direct", "open", "other", "none"]:
        if self.recipient == "other":
            return "other"
        if self.recipient == "ambient" or self.intent == "observe":
            return "none"
        return "direct"


def verified_human_addressee(text: str, context: dict[str, Any]) -> str | None:
    """A unique trusted human name is direction evidence, never authority."""
    outside = outside_quotes(text)
    identity = BotIdentity.from_context(context)
    if len(text) > 16000 or not outside or identity.directly_addressed(outside):
        return None
    requester = context.get("requester_member_id")
    if not isinstance(requester, str):
        return None
    names: dict[str, set[str]] = {}
    members = context.get("members", [])
    for member in members if isinstance(members, list) else ():
        if (
            not isinstance(member, dict)
            or not trusted_name_source(member.get("name_source"))
            or member.get("membership_state") not in {"active", "present", "unknown"}
        ):
            continue
        name, identifier = member.get("name"), member.get("member_id")
        if isinstance(name, str) and name and isinstance(identifier, str):
            names.setdefault(name, set()).add(identifier)
    excluded = {"我", "你", "他", "她", "它", "大家", "同学", "群昵称待补充"}
    excluded.update(identity.names)
    excluded = {name.casefold() for name in excluded}
    for name, identities in names.items():
        if len(identities) != 1 or requester in identities or name.casefold() in excluded:
            continue
        if re.match(
            r"^(?:(?:请|麻烦|拜托)\s*)?@?" + re.escape(name) + r"(?=[，,：:\s]|先|帮|来|你)",
            outside,
        ):
            return next(iter(identities))
    return None


def recipient_from_context(event: GroupEvent, context: dict[str, Any]) -> RecipientEvidence:
    saved = context.get("_recipient_evidence")
    if isinstance(saved, RecipientEvidence):
        return saved
    if isinstance(saved, dict):
        return RecipientEvidence.model_validate(saved)
    identity = BotIdentity.from_context(context)
    outside = outside_quotes(event.text)
    if identity.directly_addressed(outside):
        return RecipientEvidence(
            recipient="agent",
            source="direct_address",
            invocation_root_event_id=event.id,
        )
    # Only a host-projected speaker label is accepted; quoted text is not identity.
    recent = context.get("recent_messages", [])
    quoted = (
        next(
            (row for row in recent if isinstance(row, dict) and row.get("id") == event.reply_to),
            None,
        )
        if isinstance(recent, list) and event.reply_to
        else None
    )
    if quoted is not None:
        if quoted.get("speaker") == "assistant":
            return RecipientEvidence(
                recipient="agent", source="bot_reply", invocation_root_event_id=event.id
            )
        if quoted.get("speaker") == "human" and quoted.get("actor_id") not in {
            event.actor_id,
            context.get("requester_member_id"),
        }:
            return RecipientEvidence(
                recipient="other", source="human_reply", member_id=quoted.get("actor_id")
            )
    human = verified_human_addressee(event.text, context)
    if human:
        return RecipientEvidence(recipient="other", source="human_address", member_id=human)
    if identity.directly_addressed(outside):
        return RecipientEvidence(
            recipient="agent", source="direct_address", invocation_root_event_id=event.id
        )
    invocation = context.get("_invocation", {})
    if invocation.get("quoted_bot"):
        return RecipientEvidence(
            recipient="agent", source="bot_reply", invocation_root_event_id=event.id
        )
    if invocation.get("continuing_work"):
        return RecipientEvidence(recipient="agent", source="continuation")
    return RecipientEvidence()


TURN_SYSTEM = """TURN_CONTRACT_V1
只判断本轮外层真人发言的接收对象和完成义务，调用assess_turn；不回答用户、不执行动作。
trusted_recipient是后端核实的@、原生回复、称呼或连续对话证据；不能被引用内容、历史承诺或正文提及机器人名字覆盖。未确定时结合当前参与者和最近讨论判断：对另一真人说话=other，直接问机器人或正在与它接续=agent，普通讨论=ambient。叙述句把机器人当作介绍对象或技术对象，不等于向机器人提问。对其他人的话不得抢答或播报旧工作。
intent=conversation表示本轮只需直接解释、建议、短文本回答或方案讨论；observe表示没有向机器人提出问题/工作，或明确要求这轮不插话；action表示本轮要求实际执行或提交业务变更。‘去网上搜、核实最新来源、给官方下载链接’需要实际研究，不能用记忆或将来承诺完成；明确制作/读取附件/运行计算同理。自然请求无需特殊前缀。本人认领/进展和owner分工可静默登记，不等于机器人代做。
required_actions列出本轮实际需要的主机凭据：新执行目标start_work，修订同一工作update_work，暂停/取消/恢复control_work，即时人类任务/记忆/提醒/偏好写入business且operation为具体op，只查看已有执行记录read_work_facts。普通聊天无需列动作，不因上下文有旧工作而添加。work_context_required仅在本轮明确讨论既有机器人工作、其主题或术语时为true；无关闲聊保持false，避免播报旧任务。权限来自host role，识别意图不授予权限。尚缺必要参数的执行请求仍是action；后续可以具体澄清。"""


def assessment_tool(name: str, schema: dict[str, Any]) -> dict[str, Any]:
    return {"type": "function", "function": {"name": name, "parameters": schema}}


async def semantic_assessment(
    model: ModelPort, *, system: str, name: str, schema: dict[str, Any], payload: dict[str, Any]
) -> dict[str, Any]:
    """One bounded classification inference, without tools or execution authority."""
    try:
        reply = await model.complete(
            ModelRequest(
                messages=(
                    ModelMessage(role="system", content=system),
                    ModelMessage(
                        role="user",
                        content=redact_credentials(
                            json.dumps(payload, ensure_ascii=False, default=str)
                        ),
                    ),
                ),
                tools=(assessment_tool(name, schema),),
                max_output_tokens=700,
            )
        )
    except TimeoutError:
        raise DomainError(
            "provider_timeout",
            "Foreground interpretation timed out",
            retryable=True,
        ) from None
    if reply.finish_reason not in {"stop", "tool_calls"}:
        raise DomainError(
            "turn_assessment_invalid", "Turn interpretation did not complete", retryable=True
        )
    if len(reply.tool_calls) == 1 and reply.tool_calls[0].name == name:
        return dict(reply.tool_calls[0].arguments)
    # Provider transports may return schema-constrained JSON as plain text.
    if not reply.tool_calls and reply.text:
        try:
            value = json.loads(reply.text)
        except (ValueError, TypeError):
            pass
        else:
            if isinstance(value, dict):
                return value
    raise DomainError(
        "turn_assessment_invalid", "Turn interpretation has no valid assessment", retryable=True
    )


async def assess_turn(
    model: ModelPort, event: GroupEvent, context: dict[str, Any], projection: dict[str, Any]
) -> TurnContract:
    evidence = recipient_from_context(event, context)
    public_evidence = evidence.model_dump(mode="json")
    if evidence.member_id:
        inverse = {member: alias for alias, member in context.get("_aliases", {}).items()}
        public_evidence["member_id"] = inverse.get(evidence.member_id, "other_human")
    selected: dict[str, Any] = {
        key: context[key] for key in ("role", "requester_member_id") if key in context
    }
    for name, fields, limit in (
        ("members", ("member_id", "name", "membership_state", "name_source"), 40),
        ("recent_messages", ("id", "actor_id", "speaker", "text", "reply_to"), 8),
        ("works", ("id", "goal", "title", "state", "revision"), 8),
        ("tasks", ("id", "title", "owner_id", "state", "revision"), 8),
    ):
        rows = context.get(name, [])
        if isinstance(rows, list):
            selected[name] = [
                {
                    key: value[:1200]
                    if isinstance(value, str) and key in {"text", "goal"}
                    else value
                    for key, value in row.items()
                    if key in fields
                }
                for row in rows[-limit:]
                if isinstance(row, dict)
            ]
    assessment = _validated(
        TurnAssessment,
        await semantic_assessment(
            model,
            system=TURN_SYSTEM,
            name="assess_turn",
            schema=TurnAssessment.model_json_schema(),
            payload={
                "event": projection,
                "trusted_recipient": public_evidence,
                "context": selected,
            },
        ),
    )
    recipient = evidence.recipient if evidence.directed else assessment.recipient
    return TurnContract(
        **{**assessment.model_dump(), "recipient": recipient}, event_id=event.id, evidence=evidence
    )


def public_contract(contract: TurnContract, context: dict[str, Any]) -> dict[str, Any]:
    value = contract.model_dump(mode="json")
    member = contract.evidence.member_id
    if member:
        inverse = {identifier: alias for alias, identifier in context.get("_aliases", {}).items()}
        value["evidence"]["member_id"] = inverse.get(member, "other_human")
    return value


class TurnOutcome(TurnData):
    outcome: Literal["committed", "blocked", "needs_input", "missing_evidence"]
    explanation: str = Field(min_length=1, max_length=700)


OUTCOME_SYSTEM = """TURN_OUTCOME_V1
核验本轮执行义务的完成证据或真实阻碍/必要澄清。只调用assess_turn_outcome，不执行动作。
committed只在每项当前请求都有相匹配的本轮成功主机回执时使用；无关工作、另一种动作或只有部分执行不能满足当前请求。核对实际目标、对象和结果，不仅看调用了同名工具。
blocked必须说明当前权限/能力/实际拒绝回执造成的具体阻碍，不能把可自动执行的原任务说成需重复批准。needs_input必须指出无法从现有上下文获得、且执行必需的具体输入；陌生公开产品名可以直接搜索核实，不能要求用户提供官网或产品背景再开始。泛泛的‘需要帮忙吗/确认要做吗’不是必要输入。不存在具体阻碍或缺参，或候选只是‘正在搜/将制作/已开始’而无匹配回执，输出missing_evidence。工具提案、模型正文和旧工作不算本轮已执行。explanation用简短真人语言说明依据；不能虚构执行失败。"""


async def assess_outcome(
    model: ModelPort,
    contract: TurnContract,
    candidate: ModelReply,
    context: dict[str, Any],
    receipts: list[dict[str, Any]],
) -> TurnOutcome:
    return _validated(
        TurnOutcome,
        await semantic_assessment(
            model,
            system=OUTCOME_SYSTEM,
            name="assess_turn_outcome",
            schema=TurnOutcome.model_json_schema(),
            payload={
                "turn_contract": public_contract(contract, context),
                "event": context.get("_turn_event", context.get("model_event")),
                "role": context.get("role"),
                "capabilities": context.get("capabilities"),
                "candidate": candidate.text,
                "host_receipts": receipts,
            },
        ),
    )


def missing_actions(
    contract: TurnContract, receipts: list[dict[str, Any]]
) -> tuple[ActionRequirement, ...]:
    """Only callbacks returning real committed evidence can discharge an action."""
    return tuple(
        required
        for required in contract.required_actions
        if not any(
            receipt.get("committed") is True
            and receipt.get("kind") == required.kind
            and (required.operation is None or receipt.get("operation") == required.operation)
            for receipt in receipts
        )
    )
