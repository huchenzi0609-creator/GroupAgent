"""Synthetic failures cross the real foreground graph and host receipt boundary.

Semantic responses below are explicit fixtures. These checks prove orchestration,
silence, evidence and replay contracts; they do not claim live intent accuracy.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime

import pytest

from groupagent.conversation.policy import (
    delivery_admission_in_tx,
    ingress_admission,
    recipient_evidence_in_tx,
)
from groupagent.conversation.provider import ModelDecisionProvider
from groupagent.conversation.turns import RecipientEvidence, TurnContract, assess_turn
from groupagent.domain import DomainError, EventKind, GroupEvent
from groupagent.execution.models import ModelReply, ModelToolCall, TokenUsage
from groupagent.identity import BotIdentity

from .test_agent_dialogue import source

USAGE = TokenUsage(input_tokens=1, output_tokens=1)
GOAL = "Pot，去网上核实Polaris Studio的当前功能，给出官方下载入口。"


def reply(text):
    return ModelReply(text=text, usage=USAGE)


def start(goal=GOAL, identifier="synthetic-start"):
    return ModelReply(
        tool_calls=(
            ModelToolCall(
                id=identifier,
                name="start_work",
                arguments={
                    "goal": goal,
                    "work_capabilities": ["public_research"],
                    "outputs": [{"format": "text"}],
                },
            ),
        ),
        finish_reason="tool_calls",
        usage=USAGE,
    )


def event(text=GOAL, **changes):
    stamp = datetime.now(UTC)
    return GroupEvent(
        id="synthetic-turn",
        source_key="synthetic-turn",
        group_id="synthetic-group",
        actor_id="synthetic-owner",
        kind=EventKind.TEXT,
        text=text,
        occurred_at=stamp,
        received_at=stamp,
        **changes,
    )


def context(**changes):
    async def persist_unit_contract(_):
        return {"status": "persisted"}  # Protocol fixture; actual persistence is tested with PG.

    return {
        "role": "owner",
        "requester_member_id": "synthetic-owner",
        "members": [],
        "recent_messages": [],
        "works": [],
        "tasks": [],
        "schedules": [],
        "_persist_turn_contract": persist_unit_contract,
        **changes,
    }


class ContractModel:
    """A scripted actor plus an independent, explicitly declared semantic verdict."""

    name = "synthetic-turn-contract"

    def __init__(self, *steps, assessment=None, outcomes=(), expected_goal=GOAL):
        self.steps = list(steps)
        self.assessment = assessment or {
            "recipient": "agent",
            "intent": "action",
            "required_actions": [{"kind": "start_work"}],
        }
        self.outcomes, self.requests, self.assessments = list(outcomes), [], []
        self.expected_goal = expected_goal

    async def complete(self, request):
        marker = request.messages[0].content or ""
        if marker.startswith("TURN_CONTRACT_V1"):
            self.assessments.append(request)
            return reply(json.dumps(self.assessment))
        if marker.startswith("TURN_OUTCOME_V1"):
            self.assessments.append(request)
            payload = json.loads(request.messages[1].content)
            if self.outcomes:
                outcome = self.outcomes.pop(0)
            else:
                matching = any(
                    receipt.get("committed")
                    and receipt.get("result", {}).get("decision", {}).get("goal")
                    == self.expected_goal
                    for receipt in payload["host_receipts"]
                )
                ordinary = self.assessment.get("intent") == "conversation"
                outcome = "committed" if matching or ordinary else "missing_evidence"
            required = self.assessment.get("required_actions", [])
            if not required and self.assessment.get("information_basis") == "public_verification":
                required = [{"kind": "start_work"}]
            return reply(
                json.dumps(
                    {
                        "outcome": outcome,
                        "explanation": "当前公开资料请求需要匹配的实际研究工作回执。",
                        "required_actions": required,
                        "reply_required": payload["trusted_recipient"]["recipient"] == "agent",
                    }
                )
            )
        self.requests.append(request)
        step = self.steps.pop(0)
        if isinstance(step, BaseException):
            raise step
        return step(request) if callable(step) else step


@pytest.mark.parametrize(
    "text",
    [
        "我用框架重做了Pot的内核，刚才在验证。",
        "这是Pot的内核。",
        "Pot的内核改过了。",
        "我计划请Pot帮忙，但现在只是和同学讨论。",
    ],
)
def test_narrative_mentions_are_not_natural_calls(text):
    assert BotIdentity().mentioned(text)
    assert not BotIdentity().directly_addressed(text)


def test_absent_native_mentions_preserve_legacy_canonical_event_fingerprints():
    legacy = event().model_dump(mode="json")
    assert "mentioned_member_ids" not in legacy
    assert GroupEvent.model_validate(legacy).model_dump(mode="json") == legacy
    native = event().model_copy(update={"mentioned_member_ids": ["synthetic-peer"]})
    assert native.model_dump(mode="json")["mentioned_member_ids"] == ["synthetic-peer"]


@pytest.mark.parametrize(
    "recipient,intent,visible",
    [
        ("agent", "conversation", True),
        ("agent", "observe", False),
        ("other", "conversation", False),
        ("ambient", "conversation", False),
        ("ambient", "observe", False),
    ],
)
def test_only_addressed_agent_turns_are_visible(recipient, intent, visible):
    contract = TurnContract(
        event_id="synthetic-visibility",
        recipient=recipient,
        intent=intent,
        evidence=RecipientEvidence(),
    )
    assert contract.visible is visible
    if recipient == "ambient":
        assert contract.directness == "none"


async def test_ambient_conversation_assessment_does_not_generate_a_reply_or_delegate_work():
    model = ContractModel(
        start(),
        reply("不应出现的插话。"),
        assessment={"recipient": "ambient", "intent": "conversation", "required_actions": []},
    )
    result = await ModelDecisionProvider(model).decide(event("换个配色也挺好。"), context())
    assert result.kind == "observe" and result.response is None
    assert not model.requests


async def test_ambient_self_declaration_can_be_recorded_without_bot_work_or_visible_ack():
    committed = []

    async def apply(candidate, identifier):
        committed.append(candidate)
        assert candidate.kind == "command" and candidate.action["op"] == "claim_task"
        return {"status": "committed", "committed": True}

    claim = ModelReply(
        tool_calls=(
            ModelToolCall(
                id="synthetic-human-declaration",
                name="apply_business_action",
                arguments={"action": {"op": "claim_task", "title": "演示配色"}},
            ),
        ),
        finish_reason="tool_calls",
        usage=USAGE,
    )
    model = ContractModel(
        start(),
        claim,
        reply("不应发出的确认。"),
        outcomes=["committed"],
        assessment={
            "recipient": "ambient",
            "intent": "action",
            "required_actions": [
                {"kind": "business", "operation": "claim_task"},
            ],
        },
    )
    result = await ModelDecisionProvider(model).decide(
        event("这次演示配色由我负责。"),
        context(_apply_business_action=apply),
    )
    assert len(committed) == 1
    assert result.kind == "observe" and result.response is None


async def test_action_promise_repairs_inside_same_graph_and_commits_before_reply():
    committed = []

    async def apply(candidate, identifier):
        committed.append((candidate, identifier))
        return {
            "status": "committed",
            "committed": True,
            "result": {"work_id": "synthetic-work", "decision": candidate.model_dump(mode="json")},
        }

    def finish(request):
        assert committed
        assert any(
            "turn_completion_repair" in (message.content or "") for message in request.messages
        )
        return reply("研究工作已经排队，核实后给你下载入口。")

    model = ContractModel(reply("我现在去搜。"), start(), finish)
    result = await ModelDecisionProvider(model).decide(
        event(), context(_apply_business_action=apply)
    )
    assert result.response == "研究工作已经排队，核实后给你下载入口。"
    assert len(committed) == 1 and len(model.requests) == 3 and not model.steps


def public_information_assessment():
    # Reproduce the coarse conversational classification while making the
    # semantic evidence requirement explicit. No product/verb parser is used.
    return {
        "recipient": "agent",
        "intent": "conversation",
        "required_actions": [],
        "information_basis": "public_verification",
    }


async def test_public_information_basis_establishes_research_without_explicit_search_wording():
    goal = "Pot，请说明如何获取和配置Aurora Desk。"
    committed = []

    async def apply(candidate, identifier):
        committed.append(candidate)
        assert candidate.kind == "start_work" and candidate.work_capabilities == ["public_research"]
        return {
            "status": "committed",
            "committed": True,
            "result": {
                "work_id": "synthetic-research",
                "decision": candidate.model_dump(mode="json"),
            },
        }

    model = ContractModel(
        reply("请先告诉我产品公司或官网，我再给一般步骤。"),
        start(goal),
        reply("已安排核实官方资料，再给出准确步骤。"),
        assessment=public_information_assessment(),
        expected_goal=goal,
    )
    current = context(
        capabilities={"web_research": True}, read_only=False, _apply_business_action=apply
    )
    result = await ModelDecisionProvider(model).decide(event(goal), current)
    assessment_request = model.assessments[0]
    assessment_context = json.loads(assessment_request.messages[1].content)["context"]
    assert assessment_context["capabilities"] == {"web_research": True}
    assert assessment_context["read_only"] is False
    assert "information_basis" in assessment_request.tools[0]["function"]["parameters"]["required"]
    assert current["_turn_contract"].intent == "action"
    assert current["_turn_contract"].information_basis == "public_verification"
    assert len(committed) == 1 and len(model.requests) == 3
    assert result.response == "已安排核实官方资料，再给出准确步骤。"


@pytest.mark.parametrize("question", ["Pot，什么是RAG？", "Pot，用一句话解释什么是人。"])
async def test_stable_concepts_remain_conversation_with_research_available(question):
    model = ContractModel(
        reply("这是稳定概念的简短解释。"),
        assessment={
            "recipient": "agent",
            "intent": "conversation",
            "required_actions": [],
            "information_basis": "stable_knowledge",
        },
    )
    current = context(capabilities={"web_research": True}, read_only=False)
    result = await ModelDecisionProvider(model).decide(event(question), current)
    assert result.response == "这是稳定概念的简短解释。"
    assert current["_turn_contract"].intent == "conversation"
    assert len(model.requests) == 1 and len(model.assessments) == 2


@pytest.mark.parametrize("basis", ["provided_context", "user_input"])
async def test_supplied_evidence_or_true_user_only_input_does_not_force_research(basis):
    model = ContractModel(
        reply("基于已有材料回答，或只补充真正缺少的个人输入。"),
        assessment={
            "recipient": "agent",
            "intent": "conversation",
            "required_actions": [],
            "information_basis": basis,
        },
    )
    current = context(capabilities={"web_research": True}, read_only=False)
    result = await ModelDecisionProvider(model).decide(
        event("Pot，请按本轮具体材料说明。"), current
    )
    assert result.response and current["_turn_contract"].intent == "conversation"
    assert not current["_turn_contract"].required_actions
    assert len(model.requests) == 1 and len(model.assessments) == 2


@pytest.mark.parametrize("kind", ["update_work", "control_work", "read_work_facts"])
async def test_public_basis_preserves_selected_existing_work_obligations(kind):
    model = ContractModel(
        assessment={
            "recipient": "agent",
            "intent": "action",
            "required_actions": [{"kind": kind}],
            "information_basis": "public_verification",
        }
    )
    value = event("Pot，核对已有工作的资料。")
    contract = await assess_turn(
        model,
        value,
        context(capabilities={"web_research": True}),
        {
            "id": value.id,
            "text": value.text,
        },
    )
    assert contract.intent == "action" and [item.kind for item in contract.required_actions] == [
        kind
    ]


@pytest.mark.parametrize(
    "role,read_only,enabled,reason",
    [
        ("owner", False, False, "public_research_unavailable"),
        ("observer", True, True, "read_only_scope"),
    ],
)
async def test_public_verification_uses_actual_host_boundaries_without_granting_research(
    role, read_only, enabled, reason
):
    model = ContractModel(assessment=public_information_assessment())
    current = context(role=role, read_only=read_only, capabilities={"web_research": enabled})
    result = await ModelDecisionProvider(model).decide(
        event("Pot，说明这个公开产品的当前用法。"), current
    )
    assert result.reason_code == reason and result.response
    assert current["_turn_outcome"]["outcome"] == "blocked"
    assert not model.requests


@pytest.mark.postgres
async def test_implicit_public_information_commits_research_in_its_first_turn(conversation_env):
    env = conversation_env
    value = source(env, "Pot，请说明如何获取和配置Aurora Desk。")
    model = ContractModel(
        reply("请先给我官网地址。"),
        start(value.text),
        reply("已安排核实官方资料，再给出准确步骤。"),
        assessment=public_information_assessment(),
        expected_goal=value.text,
    )
    env.models[value.id] = model
    result = await env.coordinator.process(value)
    assert result.work_id
    with env.store.transaction(env.actor) as tx:
        assert len(tx.list_work()) == 1 and len(tx.list_delivery()) == 1
        assert tx.get_work(result.work_id).scope["research_required"] is True
        saved = tx.get_decision(value.id)["turn_contract"]
        assert saved["intent"] == "action" and saved["information_basis"] == "public_verification"
    assert result.decision.response == "已安排核实官方资料，再给出准确步骤。"


@pytest.mark.parametrize("outcomes", [(), ("committed", "committed")])
async def test_repeated_unexecuted_promises_defer_without_hot_loop_or_forged_receipt(outcomes):
    model = ContractModel(reply("我正在搜。"), reply("我接着查。"), outcomes=outcomes)
    with pytest.raises(DomainError) as caught:
        await ModelDecisionProvider(model).decide(event(), context())
    assert caught.value.code == "turn_execution_pending" and caught.value.retryable
    assert len(model.requests) == 2 and not model.steps


async def test_repeated_rejected_tool_proposals_defer_without_repeating_host_calls():
    model = ContractModel(start(identifier="first"), start(identifier="second"))
    with pytest.raises(DomainError) as caught:
        await ModelDecisionProvider(model).decide(event(), context(role="observer"))
    assert caught.value.code == "turn_execution_pending" and caught.value.retryable
    assert len(model.requests) == 2 and not model.steps


async def test_human_reply_direction_survives_a_wrong_semantic_agent_label():
    source_id = "synthetic-human-source"
    model = ContractModel(
        reply("不该出现的旧工作播报。"),
        assessment={"recipient": "agent", "intent": "conversation", "required_actions": []},
    )
    result = await ModelDecisionProvider(model).decide(
        event("我用框架重做了Pot的内核，刚才在验证。", reply_to=source_id),
        context(
            recent_messages=[{"id": source_id, "speaker": "human", "actor_id": "synthetic-peer"}]
        ),
    )
    assert result.kind == "observe" and result.response is None and result.directness == "other"
    assert not model.requests


async def test_unrelated_work_is_omitted_from_ordinary_chat_and_style_is_configured():
    def answer(request):
        public = json.loads(request.messages[1].content)["context"]
        assert "works" not in public and "work_facts_overview" not in public
        assert "short_reply_style" in public
        assert "短句" in request.messages[0].content
        assert "不用Decision JSON" in request.messages[0].content
        return reply("列表可变，元组不能逐项修改。")

    model = ContractModel(
        answer,
        assessment={
            "recipient": "agent",
            "intent": "conversation",
            "required_actions": [],
        },
    )
    result = await ModelDecisionProvider(model).decide(
        event("Pot，列表和元组有何区别？"),
        context(
            works=[{"id": "old", "goal": "无关旧任务"}],
            work_facts_overview={"facts": [{"id": "old"}]},
            short_reply_style="one_to_three_short_sentences",
        ),
    )
    assert result.response == "列表可变，元组不能逐项修改。"


@pytest.mark.postgres
async def test_native_peer_reply_with_middle_bot_name_stays_silent_and_retains_memory(
    conversation_env,
):
    env = conversation_env
    peer = source(env, "合成同学间的技术讨论。", actor="synthetic-peer")
    value = source(env, "我用框架重做了Pot的内核，刚才在验证。", reply_to=peer.id)
    model = ContractModel(
        assessment={
            "recipient": "ambient",
            "intent": "observe",
            "required_actions": [],
        }
    )
    env.models[value.id] = model
    result = await env.coordinator.process(value)
    with env.store.transaction(env.actor) as tx:
        assert recipient_evidence_in_tx(tx, value).source == "human_reply"
        assert not tx.list_work() and not tx.list_delivery()
        assert tx.get_event(value.id).text == value.text
        assert tx.get_decision(value.id)["turn_contract"]["recipient"] == "other"
    assert result.decision.kind == "observe" and not model.requests


@pytest.mark.postgres
async def test_ambient_conversation_remains_in_memory_and_blocks_persisted_foreground_delivery(
    conversation_env,
):
    env = conversation_env
    value = source(env, "换个配色也挺好。")
    model = ContractModel(
        reply("不应发送的环境讨论插话。"),
        assessment={
            "recipient": "ambient",
            "intent": "conversation",
            "required_actions": [],
        },
    )
    env.models[value.id] = model
    result = await env.coordinator.process(value)
    with env.store.transaction(env.actor) as tx:
        assert tx.get_event(value.id).text == value.text
        assert not tx.list_work() and not tx.list_delivery()
        saved = TurnContract.model_validate(tx.get_decision(value.id)["turn_contract"])
        assert saved.recipient == "ambient" and saved.intent == "conversation" and not saved.visible
        intrusive = tx.enqueue_reply(value.id, "不应发送的环境讨论插话。")
        admission = delivery_admission_in_tx(tx, intrusive)
        assert not admission.allowed and admission.retire
        assert admission.reason_code == "turn_not_addressed_to_agent"
    assert result.decision.kind == "observe" and result.delivery_id is None and not model.requests


@pytest.mark.postgres
def test_shared_account_native_human_mention_cannot_impersonate_pot(conversation_env):
    env = conversation_env
    value = source(env, "请你看看Pot的框架。", actor="synthetic-peer")
    value = value.model_copy(
        update={
            "mentioned_member_ids": [env.actor.member_id],
            "trusted_actor_origin": "agentwechat_authenticated_message_metadata",
        }
    )
    with env.store.transaction(env.actor) as tx:
        recipient = recipient_evidence_in_tx(
            tx,
            value,
            bot_member_id=env.actor.member_id,
            shared_account=True,
        )
        assert recipient.recipient == "other" and recipient.source == "native_mention"
        assert (
            ingress_admission(
                tx,
                value,
                bot_member_id=env.actor.member_id,
                shared_account=True,
                default_mode="on_request",
            ).allowed
            is False
        )
        called = value.model_copy(update={"text": "@Pot 请解释一下框架。"})
        assert (
            recipient_evidence_in_tx(
                tx,
                called,
                bot_member_id=env.actor.member_id,
                shared_account=True,
            ).recipient
            == "agent"
        )


@pytest.mark.postgres
async def test_partial_commits_survive_deferral_and_replay_without_duplicate_work(conversation_env):
    env = conversation_env
    value = source(env, GOAL + "并记下我负责核验下载结果。")
    assessment = {
        "recipient": "agent",
        "intent": "action",
        "required_actions": [
            {"kind": "start_work"},
            {"kind": "business", "operation": "claim_task"},
        ],
    }
    env.models[value.id] = ContractModel(
        start(value.text),
        reply("研究已排队，我会登记你的分工。"),
        reply("我继续登记。"),
        assessment=assessment,
        expected_goal=value.text,
    )
    with pytest.raises(DomainError, match="host progress"):
        await env.coordinator.process(value)
    with env.store.transaction(env.actor) as tx:
        body = tx.get_decision(value.id)
        assert body["agent_needs_execution"] and body["agent_pending_response"]
        assert len(tx.list_work()) == 1 and not tx.list_delivery()
    # A new call ID for the accepted work reuses its durable receipt; the
    # separate missing declaration still needs its own committed host action.
    claim = ModelReply(
        tool_calls=(
            ModelToolCall(
                id="requested-declaration",
                name="apply_business_action",
                arguments={"action": {"op": "claim_task", "title": "核验官方下载结果"}},
            ),
        ),
        finish_reason="tool_calls",
        usage=USAGE,
    )
    model = ContractModel(
        start(value.text, "replayed"),
        claim,
        reply("研究工作已排队，也记下了你的分工。"),
        assessment=assessment,
        expected_goal=value.text,
    )
    env.models[value.id] = model
    result = await env.coordinator.process(value)
    with env.store.transaction(env.actor) as tx:
        assert len(tx.list_work()) == len(tx.list_team_tasks()) == len(tx.list_delivery()) == 1
        assert not tx.get_decision(value.id)["agent_needs_execution"]
        assert not tx.get_decision(value.id)["agent_pending_response"]
    assert result.delivery_id and (await env.coordinator.process(value)).duplicate


@pytest.mark.postgres
async def test_quiet_closes_a_deferred_foreground_turn_without_cancelling_accepted_work(
    conversation_env,
):
    env = conversation_env
    value = source(env, GOAL + "并记下我的核验分工。")
    env.models[value.id] = ContractModel(
        start(value.text),
        reply("我会再登记分工。"),
        reply("我继续登记。"),
        expected_goal=value.text,
        assessment={
            "recipient": "agent",
            "intent": "action",
            "required_actions": [
                {"kind": "start_work"},
                {"kind": "business", "operation": "claim_task"},
            ],
        },
    )
    with pytest.raises(DomainError):
        await env.coordinator.process(value)
    await env.coordinator.process(source(env, "Pot，先保持安静。"))
    # The empty script proves no new model or execution is needed to retire
    # this older foreground reply at the quiet barrier.
    env.models[value.id] = ContractModel()
    result = await env.coordinator.process(value)
    with env.store.transaction(env.actor) as tx:
        assert len(tx.list_work()) == 1 and not tx.list_delivery()
        assert not tx.get_decision(value.id)["agent_pending_response"]
        assert not tx.get_decision(value.id)["agent_needs_execution"]
    assert result.decision.kind == "observe"


@pytest.mark.postgres
async def test_semantically_addressed_dialogue_establishes_a_durable_followup_root(
    conversation_env,
):
    env = conversation_env
    value = source(env, "请解释一下列表和元组的区别。")
    env.models[value.id] = ContractModel(
        reply("列表可以逐项修改，元组不能。"),
        assessment={"recipient": "agent", "intent": "conversation", "required_actions": []},
    )
    await env.coordinator.process(value)
    followup = source(env, "哪一种适合固定记录呢？")
    with env.store.transaction(env.actor) as tx:
        admitted = ingress_admission(tx, followup, default_mode="on_request")
        assert admitted.allowed and admitted.invocation_root_event_id == value.id


@pytest.mark.postgres
async def test_own_request_quote_continues_bot_work_without_implying_a_bot_speaker(
    conversation_env,
):
    env = conversation_env
    requested = source(env, GOAL)
    env.models[requested.id] = ContractModel(start(), reply("研究已排队。"))
    started = await env.coordinator.process(requested)
    followup = source(env, "你搜出来了吗？", reply_to=requested.id)
    read = ModelReply(
        tool_calls=(
            ModelToolCall(
                id="read-requested-work",
                name="read_work_facts",
                arguments={
                    "selector": {
                        "basis": "ids",
                        "ids": [started.work_id],
                        "states": [],
                        "quantifier": "one",
                    },
                    "include_evidence": True,
                },
            ),
        ),
        finish_reason="tool_calls",
        usage=USAGE,
    )
    env.models[followup.id] = ContractModel(
        read,
        reply("还没有可交付的结果，研究工作仍在排队。"),
        outcomes=["committed"],
        assessment={
            "recipient": "agent",
            "intent": "action",
            "required_actions": [{"kind": "read_work_facts"}],
        },
    )
    result = await env.coordinator.process(followup)
    with env.store.transaction(env.actor) as tx:
        recipient = recipient_evidence_in_tx(tx, followup)
        assert recipient.recipient == "agent" and recipient.source == "continuation"
        assert tx.get_decision(followup.id)["turn_contract"]["evidence"]["source"] != "bot_reply"
        assert len(tx.list_work()) == 1
    assert result.work_id == started.work_id and result.delivery_id
