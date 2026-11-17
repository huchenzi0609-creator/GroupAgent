"""Typed time proposals through the real foreground graph and durable scheduler."""

import json
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest

from groupagent.domain import GroupEvent
from groupagent.execution.models import ModelReply

from .test_react_turn import USAGE, ModelScript, call, finish, tool_receipt


def ingest(env, text, occurred_at, received_at):
    event = GroupEvent(
        id=uuid4().hex,
        source_key=uuid4().hex,
        group_id=env.actor.group_id,
        actor_id=env.actor.member_id,
        kind="text",
        text=text,
        occurred_at=occurred_at,
        received_at=received_at,
    )
    with env.store.transaction(env.actor) as tx:
        tx.ingest_event(event, "fixture:" + event.id, "1", 0)
    return event


@pytest.mark.postgres
@pytest.mark.parametrize(("delay", "mixed"), [(10, False), (10, True), (360, False)])
async def test_relative_reminder_uses_verified_event_anchor_and_canonical_receipt(
    conversation_env, delay, mixed
):
    env = conversation_env
    occurred = datetime(2026, 11, 17, 20, 35, 22, tzinfo=UTC)
    received = occurred + timedelta(seconds=delay)
    due = occurred + timedelta(minutes=5)
    env.coordinator.clock = lambda: received
    text = "Pot，5分钟以后提醒我取快递可以吗？"
    if mixed:
        text += "顺便告诉我3加4是多少。"
    event = ingest(env, text, occurred, received)

    def propose(request):
        assert request.tool_choice == "required"
        payload = json.loads(request.messages[-1].content)
        assert payload["event"]["occurred_at"] == "2026-11-18T04:35:22+08:00"
        context = payload["context"]
        assert context["clock"]["occurred_at"] == "2026-11-18T04:35:22+08:00"
        assert context["clock"]["now_utc"] == received.isoformat()
        assert "time_resolution" not in context
        return call(
            "apply_business_action",
            "reminder",
            action={
                "op": "create_schedule",
                "title": "取快递",
                "timing": {"kind": "after", "seconds": 300},
                "prepared_message": {"purpose": "explicit_reminder", "text": "取快递"},
            },
        )

    def close(request):
        receipt = tool_receipt(request)
        if delay > 300:
            assert request.tool_choice == "required"
            assert receipt["code"] == "schedule_time_missed"
            assert not receipt["committed"]
            assert "04:40:22" in receipt["host_message"]
            return finish("原来的提醒时刻04:40:22已经过去，未创建提醒。")
        assert request.tool_choice == "auto"
        assert receipt["committed"], receipt
        assert datetime.fromisoformat(receipt["result"]["action_result"]["next_due_at"]) == due
        if mixed:
            return finish("3加4等于7。")
        return ModelReply(text="我会在五分钟后提醒你。", usage=USAGE)

    model = ModelScript(propose, close)
    env.models[event.id] = model
    result = await env.coordinator.process(event)
    assert len(model.requests) == 2
    assert result.delivery_id
    assert result.decision.response.count("04:40:22") == 1
    if mixed:
        assert "3加4等于7。" in result.decision.response
    assert "我会在五分钟后提醒你" not in result.decision.response
    with env.store.transaction(env.actor) as tx:
        assert len(tx.list_delivery()) == 1
        assert tx.get_decision(event.id)["turn_outcome"]["actions"] == [
            {"tool": "apply_business_action", "status": "rejected", "code": "schedule_time_missed"}
            if delay > 300
            else {"tool": "apply_business_action", "status": "committed"}
        ]
    rules = env.coordinator.scheduler.list(env.actor)
    if delay > 300:
        assert not rules and result.action_result is None
    else:
        assert len(rules) == 1 and rules[0].next_due_at == due
        assert (await env.coordinator.process(event)).duplicate


@pytest.mark.postgres
async def test_model_can_refresh_clock_then_answer_in_one_completion(conversation_env):
    env = conversation_env
    occurred = datetime(2026, 11, 17, 20, 35, 22, tzinfo=UTC)
    now = occurred
    env.coordinator.clock = lambda: now
    event = ingest(env, "Pot，北京时间现在几点？", occurred, now)

    def request_clock(request):
        nonlocal now
        now += timedelta(seconds=15)
        return call("read_group_context", "clock", resource="clock")

    def answer(request):
        observation = tool_receipt(request)
        assert observation["status"] == "observed" and not observation["committed"]
        assert observation["facts"]["now"] == "2026-11-18T04:35:37+08:00"
        assert observation["facts"]["occurred_at"] == "2026-11-18T04:35:22+08:00"
        return finish("北京时间现在是凌晨04:35:37。")

    model = ModelScript(request_clock, answer)
    env.models[event.id] = model
    result = await env.coordinator.process(event)
    assert len(model.requests) == 2
    assert result.decision.response == "北京时间现在是凌晨04:35:37。"
    assert result.delivery_id
