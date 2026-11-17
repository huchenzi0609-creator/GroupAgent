"""Substantive chat responses through the real router; no placeholder latency credit."""

from __future__ import annotations

import asyncio
import json

import pytest

from groupagent.conversation.provider import ModelDecisionProvider
from groupagent.coordinator import Decision
from groupagent.domain import DeliveryState, WorkState
from groupagent.execution.models import ModelReply, TokenUsage

from .foreground_model_fixture import semantic_fixture_reply
from .test_app import app_for, event
from .test_application_controls import admit

pytestmark = pytest.mark.postgres


class ForegroundProbe:
    def __init__(self):
        self.counts, self.closed = [], False

    def foreground_pending(self, count):
        self.counts.append(count)

    def ready(self, memory_mb):
        return True

    async def aclose(self):
        self.closed = True


class StructuredModelFixture:
    name = "synthetic-structured-chat-fixture"

    def __init__(self, text="变量是给值起的名称，值可以改变。", *, waiting=False):
        self.text, self.requests = text, []
        self.entered, self.release = asyncio.Event(), asyncio.Event()
        if not waiting:
            self.release.set()

    async def complete(self, request):
        assessed = semantic_fixture_reply(request)
        if assessed is not None:
            return assessed
        self.requests.append(request)
        self.entered.set()
        await self.release.wait()
        return ModelReply(
            text=self.text,
            usage=TokenUsage(input_tokens=10, output_tokens=10),
        )


def setup_chat(env, *, waiting=False):
    app, _ = app_for(env, {})
    # An old config may still enable ACKs; the chat path must remain inert.
    app.config = app.config.model_copy(
        update={
            "runtime": app.config.runtime.model_copy(
                update={"early_ack_seconds": 0.001, "early_ack_enabled": True}
            )
        }
    )
    app.coordinator.config = app.config
    model = StructuredModelFixture(waiting=waiting)
    app.coordinator.provider_factory = lambda *_: ModelDecisionProvider(model)
    probe = ForegroundProbe()
    app.worker_governor = probe
    return app, model, probe


async def intake(app, env, value):
    env.channel.append_incoming(value)
    await app.poll_intake()


def intents(env):
    with env.store.transaction(env.actor) as tx:
        return tx.list_delivery()


def sent(env):
    return [
        json.loads(path.read_text())["payload"]["text"]
        for path in (env.root / "channel/outgoing").glob("*.json")
    ]


async def test_exact_name_question_returns_configured_identity_without_model_or_placeholder(
    runtime_env,
):
    env = runtime_env
    app, model, _ = setup_chat(env)
    request = event(env, "Pot，请告诉我你叫什么名字，用一句话回答")
    try:
        await intake(app, env, request)
        assert await app.process_pending(batched=True) == 1
        assert model.requests == []
        assert await app.early_ack_tick() == 0
        assert await app.dispatcher.tick() == 1
        assert sent(env) == ["【Pot】我叫Pot。"]
        assert [row.idempotency_key for row in intents(env)] == ["reply:" + request.id + ":1"]
    finally:
        await app.close()


async def test_direct_light_question_bypasses_idle_batch_and_delivers_real_router_json(runtime_env):
    env = runtime_env
    app, model, probe = setup_chat(env)
    request = event(env, "Pot，请解释变量是什么？")
    try:
        await intake(app, env, request)
        assert await app.process_pending(batched=True) == 1
        assert len(model.requests) == 1
        assert await app.dispatcher.tick() == 1
        assert sent(env) == ["【Pot】" + model.text]
        assert not any(row.idempotency_key.startswith("early_ack:") for row in intents(env))
        assert max(probe.counts) == 1 and probe.counts[-1] == 0
    finally:
        await app.close()


async def test_slow_model_never_substitutes_placeholder_for_its_substantive_final(runtime_env):
    env = runtime_env
    app, model, probe = setup_chat(env, waiting=True)
    request = event(env, "Pot，请解释变量是什么？")
    await intake(app, env, request)
    route = asyncio.create_task(app.process_pending(batched=True))
    try:
        await asyncio.wait_for(model.entered.wait(), 3)
        await asyncio.sleep(0.01)
        assert await app.early_ack_tick() == 0 and intents(env) == []
        assert probe.counts[-1] == 1
        model.release.set()
        await route
        assert await app.dispatcher.tick() == 1
        assert sent(env) == ["【Pot】" + model.text]
        assert len(model.requests) == 1
    finally:
        model.release.set()
        await asyncio.gather(route, return_exceptions=True)
        await app.close()


async def test_historical_accepted_ack_does_not_suppress_final_through_real_router_and_gate(
    runtime_env,
):
    env = runtime_env
    app, model, _ = setup_chat(env, waiting=True)
    request = event(env, "Pot，请解释变量是什么？")
    await intake(app, env, request)
    route = asyncio.create_task(app.process_pending(batched=True))
    try:
        await asyncio.wait_for(model.entered.wait(), 3)
        with env.store.transaction(env.actor) as tx:
            old_ack = tx.enqueue_early_ack(request.id, "旧版本占位消息。")
            assert tx.get_decision(request.id) is None
        assert await app.dispatcher.tick() == 1
        with env.store.transaction(env.actor) as tx:
            assert tx.get_delivery(old_ack.id).state == DeliveryState.ACCEPTED
        model.release.set()
        await route
        final = next(
            row for row in intents(env) if row.idempotency_key == "reply:" + request.id + ":1"
        )
        assert final.payload_ref == "text:" + model.text and final.id != old_ack.id
        assert await app.dispatcher.tick() == 1
        with env.store.transaction(env.actor) as tx:
            assert tx.get_delivery(final.id).state == DeliveryState.ACCEPTED
            decision = tx.get_decision(request.id)["result"]["decision"]
            assert decision["kind"] == "reply" and decision["response"] == model.text
        assert "【Pot】" + model.text in sent(env)
    finally:
        model.release.set()
        await asyncio.gather(route, return_exceptions=True)
        await app.close()


async def test_name_question_uses_control_lane_while_another_real_router_is_waiting(runtime_env):
    env = runtime_env
    app, model, probe = setup_chat(env, waiting=True)
    slow = event(env, "Pot，请解释变量是什么？")
    await intake(app, env, slow)
    route = asyncio.create_task(app.process_pending(batched=True))
    try:
        await asyncio.wait_for(model.entered.wait(), 3)
        name = event(env, "Pot，请告诉我你叫什么名字，用一句话回答")
        await intake(app, env, name)
        assert await app.process_pending(controls_only=True) == 1
        assert not route.done() and len(model.requests) == 1
        assert await app.dispatcher.tick() == 1
        assert sent(env) == ["【Pot】我叫Pot。"]
        assert max(probe.counts) == 2 and probe.counts[-1] == 1
    finally:
        model.release.set()
        await asyncio.gather(route, return_exceptions=True)
        await app.close()


async def test_start_retires_only_old_ack_parts_that_were_never_submitted(runtime_env):
    env = runtime_env
    app, _, _ = setup_chat(env)
    sources = [event(env, "Pot，请解释这个问题。") for _ in range(2)]
    with env.store.transaction(env.actor) as tx:
        values = []
        for source in sources:
            tx.ingest_event(source, source.id, "1", 0)
            values.append(tx.enqueue_early_ack(source.id, "旧版本占位。"))
        tx.conn.execute(
            "INSERT INTO ga_delivery_parts(group_id,intent_id,position,idempotency_key,payload_hash,state) "
            "VALUES(%s,%s,0,'synthetic-submitted-part',%s,'submitted')",
            (env.actor.group_id, values[1].id, "0" * 64),
        )
    try:
        await app.start()
        with env.store.transaction(env.actor) as tx:
            assert tx.get_delivery(values[0].id).state == DeliveryState.REJECTED
            assert tx.get_delivery(values[1].id).state == DeliveryState.PENDING
    finally:
        await app.close()


async def test_quoted_or_compound_identity_wording_stays_with_real_router(runtime_env):
    env = runtime_env
    app, model, _ = setup_chat(env)
    samples = [
        event(env, "Pot，请解释‘你叫什么名字’的含义。"),
        event(env, "Pot，告诉我你叫什么名字，并帮我研究一个问题。"),
        event(env, "你叫什么名字？"),
        event(env, "Pot，告诉我你叫什么名字‘并帮我研究另一个问题’"),
    ]
    model.text = "这两种说法需要结合各自语境理解。"
    try:
        for value in samples:
            await intake(app, env, value)
            await app.process_pending(batched=False)
        assert len(model.requests) == 4
        assert all(row.payload_ref != "text:我叫Pot。" for row in intents(env))
    finally:
        await app.close()


async def test_cancel_control_keeps_normal_work_reply_without_placeholder(runtime_env):
    env = runtime_env
    request = event(env, "Pot，请准备报告。")
    app, _ = app_for(
        env,
        {
            request.id: Decision(
                kind="start_work",
                reason_code="synthetic_work",
                goal="准备报告",
                directness="direct",
                confidence=1,
            )
        },
    )
    try:
        result = await admit(app, env, request)
        stop = event(env, "Pot停止这个任务")
        await intake(app, env, stop)
        assert await app.process_pending(controls_only=True) == 1
        assert await app.early_ack_tick() == 0
        with env.store.transaction(env.actor) as tx:
            assert tx.get_work(result["result"]["work_id"]).state == WorkState.CANCELLED
        assert not any(row.idempotency_key.startswith("early_ack:") for row in intents(env))
    finally:
        await app.close()


async def test_closing_runtime_stops_pending_chat_and_preserves_foreground_close(runtime_env):
    env = runtime_env
    app, model, probe = setup_chat(env, waiting=True)
    request = event(env, "Pot，请解释变量是什么？")
    await intake(app, env, request)
    route = asyncio.create_task(app.process_pending(batched=True))
    app._tasks.append(route)
    await asyncio.wait_for(model.entered.wait(), 3)
    await app.close()
    assert route.cancelled() and probe.closed and probe.counts[-1] == 0
    assert await app.early_ack_tick() == 0 and intents(env) == []
