"""Pot catalog replays through the actual foreground graph, PostgreSQL and spool.

Model replies and the clock are deterministic. Delivery receipts are read back
from LocalChannel after reopening it. Real-model and WeChat latency are scored
in their own layers.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from zoneinfo import ZoneInfo

import pytest

from groupagent.channels.local import LocalChannel
from groupagent.domain import DeliveryState
from groupagent.runtime.dispatch import Dispatcher
from groupagent.scheduling import Scheduler

from .test_bot_identity import outgoing
from .test_group_conversation_contract_replay import DialogueModel, Replay, observation, reply, tool

pytestmark = pytest.mark.postgres
SHANGHAI = ZoneInfo("Asia/Shanghai")


async def test_name_and_simple_followup_are_one_useful_delivery_per_turn(runtime_env):
    replay = Replay(runtime_env)
    name, first, _ = await replay.say(
        "你好，你在群里叫什么？一句话。", DialogueModel(reply("我叫Pot。"))
    )
    _, followup, _ = await replay.say(
        "那三角形有几条边？", DialogueModel(reply("三角形有三条边。"))
    )
    assert first.delivery_id and followup.delivery_id and first.delivery_id != followup.delivery_id
    assert replay.works() == []
    sent = outgoing(runtime_env)
    assert {item["payload"]["text"] for item in sent} == {
        "【Pot】我叫Pot。",
        "【Pot】三角形有三条边。",
    }
    assert len(sent) == 2 and all(item["receipt"]["state"] == "accepted" for item in sent)
    assert all(item["payload"]["text"].count("【Pot】") == 1 for item in sent)
    with runtime_env.store.transaction(runtime_env.actor) as tx:
        assert tx.get_event(name.id)
        assert {item.state for item in tx.list_delivery()} == {DeliveryState.ACCEPTED}


def create_reminder(seconds, body):
    return tool(
        "apply_business_action",
        {
            "action": {
                "op": "create_schedule",
                "title": "取件提醒",
                "timing": {"kind": "after", "seconds": seconds},
                "prepared_message": {"text": body, "purpose": "explicit_reminder"},
            },
        },
    )


def dropped_notice(env):
    return Dispatcher(
        env.state,
        env.service,
        lambda _: env.actor,
        env.channel,
        env.registry,
        after_submit=lose_acceptance,
    )


async def lose_acceptance(*_):
    raise RuntimeError("replay lost local acceptance response")


async def test_natural_literal_reminder_reconciles_lost_ack_after_restart(runtime_env):
    env = runtime_env
    replay = Replay(env)
    anchor = datetime(2031, 11, 18, 4, 35, 22, tzinfo=SHANGHAI)
    now = anchor + timedelta(seconds=2)
    replay.coordinator.clock = lambda: now
    due = anchor + timedelta(minutes=5)

    await replay.say(
        "Pot，凌晨的快递柜还可以取件吗？", DialogueModel(reply("以柜机的营业提示为准。"))
    )
    request = replay.source("5分钟以后提醒我取快递可以吗", persist=False).model_copy(
        update={"occurred_at": anchor, "received_at": now}
    )
    with env.store.transaction(env.actor) as tx:
        tx.ingest_event(request, "pot-reminder:" + request.id, "1", 0)

    def saved(req):
        shown = observation(req)
        assert shown["status"] == "committed", shown
        assert datetime.fromisoformat(shown["result"]["action_result"]["next_due_at"]) == due
        return tool("finish_turn", {"disposition": "reply"})

    model = DialogueModel(create_reminder(300, "取快递"), saved)
    replay.models[request.id] = model
    result = await replay.coordinator.process(request)
    assert not model.steps and not result.work_id
    await replay.drain()
    scheduler = Scheduler(env.store)
    rules = scheduler.list(env.actor)
    assert len(rules) == 1
    rule = rules[0]
    assert rule.timing.at == datetime(2031, 11, 17, 20, 40, 22, tzinfo=UTC)
    assert rule.time_zone == "Asia/Shanghai" and rule.goal == "取快递"
    assert (
        len([row for row in outgoing(env) if "2031-11-18 04:40:22" in row["payload"]["text"]]) == 1
    )
    assert scheduler.occurrences(env.actor, rule.id) == []
    assert scheduler.wake(env.actor, due - timedelta(microseconds=1)) == []
    occurrence = scheduler.wake(env.actor, due)[0]
    assert occurrence.work_id is None and occurrence.delivery_id and replay.works() == []

    with pytest.raises(RuntimeError, match="lost local acceptance response"):
        await dropped_notice(env).tick()
    waiting = scheduler.facts(env.actor, schedule_id=rule.id)[0].occurrences[0].deliveries[0]
    assert waiting.state == DeliveryState.UNKNOWN
    restarted_channel = LocalChannel(env.root / "channel", env.config)
    restarted = Dispatcher(
        env.state, env.service, lambda _: env.actor, restarted_channel, env.registry
    )
    assert await restarted.tick() == 1 and await restarted.tick() == 0
    assert scheduler.wake(env.actor, due + timedelta(seconds=2)) == []
    accepted = scheduler.facts(env.actor, schedule_id=rule.id)[0].occurrences[0].deliveries[0]
    assert accepted.id == waiting.id == occurrence.delivery_id
    assert accepted.state == DeliveryState.ACCEPTED
    wire = [item for item in outgoing(env) if item["payload"]["text"] == "【Pot】取快递"]
    assert len(wire) == 1
    receipt_data = wire[0]["receipt"]
    receipt = await restarted_channel.lookup_delivery(
        env.actor.group_id, receipt_data["idempotency_key"]
    )
    assert (
        receipt
        and receipt.state == "accepted"
        and receipt.platform_message_id == receipt_data["platform_message_id"]
    )
    assert replay.works() == []


async def test_natural_schedule_time_is_anchored_even_when_late(runtime_env):
    env = runtime_env
    replay = Replay(env)
    anchor = datetime(2031, 11, 18, 23, 58, tzinfo=SHANGHAI)
    due = anchor + timedelta(minutes=5)
    received = due + timedelta(minutes=2)
    replay.coordinator.clock = lambda: received
    request = replay.source("Pot，过五分钟提醒我锁窗。", persist=False).model_copy(
        update={"occurred_at": anchor, "received_at": received}
    )
    with env.store.transaction(env.actor) as tx:
        tx.ingest_event(request, "pot-reminder:" + request.id, "1", 0)

    def missed(req):
        payload = json.loads(req.messages[1].content)
        assert datetime.fromisoformat(payload["event"]["occurred_at"]) == anchor
        decision = observation(req)
        assert decision["status"] == "rejected" and decision["committed"] is False
        return reply("原定北京时间次日00:03，消息到达时已经过了，未创建提醒。")

    model = DialogueModel(create_reminder(300, "锁窗"), missed)
    replay.models[request.id] = model
    result = await replay.coordinator.process(request)
    await replay.drain()
    assert result.delivery_id and not model.steps
    assert Scheduler(env.store).list(env.actor) == [] and replay.works() == []
    [sent] = outgoing(env)
    assert "次日00:03" in sent["payload"]["text"] and "已经过了" in sent["payload"]["text"]
