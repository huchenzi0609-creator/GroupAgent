"""Authenticated routing and durable continuation with the shared foreground agent."""

from datetime import UTC, datetime

import pytest

from groupagent.conversation.policy import (
    delivery_admission_in_tx,
    ingress_admission,
    recipient_evidence_in_tx,
)
from groupagent.conversation.turns import RecipientEvidence, TurnContract
from groupagent.domain import EventKind, GroupEvent
from groupagent.identity import BotIdentity

from .test_agent_dialogue import source
from .test_react_turn import ModelScript, call, finish

GOAL = "Pot，去网上核实Polaris Studio的当前功能，给出官方下载入口。"


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
async def test_native_human_reply_keeps_data_and_fences_unsolicited_delivery(conversation_env):
    env = conversation_env
    peer_message = source(env, "合成同学间的技术讨论。", actor="synthetic-peer")
    event = source(env, "我用框架重做了Pot的内核，刚才在验证。", reply_to=peer_message.id)
    model = ModelScript(finish("插话不应送出。"))
    env.models[event.id] = model
    result = await env.coordinator.process(event)
    assert len(model.requests) == 1 and result.decision.kind == "observe"
    with env.store.transaction(env.actor) as tx:
        assert recipient_evidence_in_tx(tx, event).source == "human_reply"
        assert not tx.list_work() and not tx.list_delivery()
        assert tx.get_event(event.id).text == event.text
        unsolicited = tx.enqueue_reply(event.id, "插话不应送出。")
        policy = delivery_admission_in_tx(tx, unsolicited)
        assert not policy.allowed and policy.retire
        assert policy.reason_code == "turn_not_addressed_to_agent"


@pytest.mark.postgres
async def test_open_question_to_agent_establishes_continuation_for_same_member(conversation_env):
    env = conversation_env
    event = source(env, "请解释一下列表和元组的区别。")
    env.models[event.id] = ModelScript(
        finish(
            "列表可修改，元组固定。",
            open_request=True,
            open_request_evidence="请解释一下列表和元组的区别。",
        )
    )
    result = await env.coordinator.process(event)
    assert result.delivery_id and result.decision.directness == "open"
    followup = source(env, "哪一种适合固定记录呢？")
    with env.store.transaction(env.actor) as tx:
        admitted = ingress_admission(tx, followup, default_mode="on_request")
        assert admitted.allowed and admitted.invocation_root_event_id == event.id
    unrelated = source(env, "我也问个问题。", actor="another-human")
    with env.store.transaction(env.actor) as tx:
        assert not ingress_admission(tx, unrelated, default_mode="on_request").allowed


@pytest.mark.postgres
async def test_own_request_quote_resolves_work_without_impersonating_bot_receipt(conversation_env):
    env = conversation_env
    request = source(env, GOAL)
    start = call(
        "start_work",
        "start",
        goal=GOAL,
        work_capabilities=["public_research"],
        outputs=[{"format": "text"}],
    )
    env.models[request.id] = ModelScript(start, finish(reports=[("start", "queued")]))
    started = await env.coordinator.process(request)
    followup = source(env, "你搜出来了吗？", reply_to=request.id)
    read = call(
        "read_work_facts",
        "evidence",
        selector={"basis": "ids", "ids": [started.work_id]},
        include_evidence=True,
    )
    env.models[followup.id] = ModelScript(
        read, finish("目前还在排队，没有可交付结果。", reports=[("evidence", "observed")])
    )
    result = await env.coordinator.process(followup)
    assert result.work_id == started.work_id and result.delivery_id
    with env.store.transaction(env.actor) as tx:
        evidence = recipient_evidence_in_tx(tx, followup)
        assert evidence.recipient == "agent" and evidence.source == "continuation"
        assert len(tx.list_work()) == 1


@pytest.mark.postgres
async def test_shadow_candidate_records_observation_without_delivery(conversation_env):
    env = conversation_env
    cfg = env.coordinator.config
    env.coordinator.config = cfg.model_copy(
        update={"conversation": cfg.conversation.model_copy(update={"proactive_mode": "shadow"})}
    )
    event = source(env, "组内技术讨论发现一个缺口。")
    env.models[event.id] = ModelScript(
        finish("这个缺口可以用小样本复核。", disposition="proactive")
    )
    result = await env.coordinator.process(event)
    assert result.decision.kind == "observe" and result.decision.reason_code == "proactive_shadow"
    assert result.delivery_id is None
