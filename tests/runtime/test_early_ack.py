"""Substantive chat responses through the real router; no placeholder latency credit."""

from __future__ import annotations

import asyncio
import json

import pytest
from conversation.turn_model_fixture import finish_fixture_reply

from groupagent.conversation.provider import ModelDecisionProvider
from groupagent.coordinator import Decision
from groupagent.domain import DeliveryState, DomainError, WorkState
from groupagent.execution.kernel import KernelTool
from groupagent.execution.models import ModelReply, ModelToolCall, TokenUsage

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
        self.requests.append(request)
        self.entered.set()
        await self.release.wait()
        return finish_fixture_reply(
            request, ModelReply(text=self.text, usage=TokenUsage(input_tokens=10, output_tokens=10))
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


async def test_exact_name_question_uses_configured_identity_in_one_model_turn(
    runtime_env,
):
    env = runtime_env
    app, model, _ = setup_chat(env)
    request = event(env, "Pot，请告诉我你叫什么名字，用一句话回答")
    model.text = "我叫Pot。"
    try:
        await intake(app, env, request)
        assert await app.process_pending(batched=True) == 1
        assert len(model.requests) == 1
        assert json.loads(model.requests[0].messages[1].content)["context"]["agent_name"] == "Pot"
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


async def test_explicit_quiet_uses_control_lane_while_foreground_model_is_waiting(runtime_env):
    env = runtime_env
    app, model, probe = setup_chat(env, waiting=True)
    slow = event(env, "Pot，请解释变量是什么？")
    await intake(app, env, slow)
    route = asyncio.create_task(app.process_pending(batched=True))
    try:
        await asyncio.wait_for(model.entered.wait(), 3)
        quiet = event(env, "Pot，先保持安静。")
        await intake(app, env, quiet)
        assert await app.process_pending(controls_only=True) == 1
        assert not route.done() and len(model.requests) == 1
        assert await app.dispatcher.tick() == 0 and not sent(env)
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


@pytest.mark.parametrize(
    ("preliminary_search", "deferred_lookup"), [(False, False), (True, False), (True, True)]
)
async def test_real_admission_is_deliverable_while_foreground_still_waits(
    runtime_env, preliminary_search, deferred_lookup
):
    env = runtime_env
    app, _, _ = setup_chat(env)
    request = event(
        env,
        "Pot，请核实杉径阅读器的官方安装说明。"
        if preliminary_search
        else "Pot，请运行 Python 计算一个合成数值。",
    )
    waiting, proceed = asyncio.Event(), asyncio.Event()
    first_action_round = 2 if preliminary_search else 1

    async def search_fixture(call):
        assert call.arguments == {"query": "杉径阅读器 官方安装说明"}
        if deferred_lookup:
            raise DomainError("foreground_research_deferred", "The inline lookup window ended.")
        return '{"status":"observed","evidence":{"sources":[]}}'

    if preliminary_search:
        app.coordinator.foreground_tools = lambda *_: (
            KernelTool("web_search", "Read a synthetic result.", {}, search_fixture),
        )

    class AdmittingFixture:
        calls = 0

        async def complete(self, model_request):
            self.calls += 1
            if preliminary_search and self.calls == 1:
                action = ModelToolCall(
                    id="preliminary-search",
                    name="web_search",
                    arguments={"query": "杉径阅读器 官方安装说明"},
                )
            elif self.calls == first_action_round:
                if preliminary_search:
                    lookup = json.loads(model_request.messages[-1].content)
                    if deferred_lookup:
                        assert lookup["code"] == "foreground_research_deferred"
                    else:
                        assert lookup["status"] == "observed"
                action = ModelToolCall(
                    id="start",
                    name="start_work",
                    arguments={
                        "goal": "核实杉径阅读器的官方安装说明"
                        if preliminary_search
                        else "Calculate the requested synthetic value",
                        "admission_ack": "deferred" if deferred_lookup else "immediate",
                        "work_capabilities": ["public_research"]
                        if preliminary_search
                        else ["compute"],
                        "required_work_tools": ["web_search"]
                        if preliminary_search
                        else ["python_compute"],
                        "outputs": [{"format": "text"}],
                        **({"response": "我稍后核对再发。"} if preliminary_search else {}),
                    },
                )
            else:
                waiting.set()
                await proceed.wait()
                action = ModelToolCall(
                    id="close",
                    name="finish_turn",
                    arguments={
                        "disposition": "reply",
                        **(
                            {
                                "response": "我把安装资料的核对交给后台了。等查到权威出处，我会再把步骤发给你。"
                            }
                            if preliminary_search
                            else {}
                        ),
                    },
                )
            return ModelReply(
                finish_reason="tool_calls",
                tool_calls=(action,),
                usage=TokenUsage(input_tokens=1, output_tokens=1),
            )

    app.coordinator.provider_factory = lambda *_: ModelDecisionProvider(AdmittingFixture())
    await app.start()
    old_request = event(env, "Pot，旧占位消息测试。")
    with env.store.transaction(env.actor) as tx:
        tx.ingest_event(old_request, old_request.id, "1", 0)
        old = tx.enqueue_early_ack(old_request.id, "Legacy placeholder.")
        tx.record_decision(
            old_request.id,
            {
                "kind": "observe",
                "result": {
                    "event_id": old_request.id,
                    "decision": {"kind": "observe", "reason_code": "fixture"},
                },
            },
        )
    await intake(app, env, request)
    foreground = asyncio.create_task(app.process_pending(batched=False))
    try:
        await asyncio.wait_for(waiting.wait(), 3)
        assert app._retire_pending_acknowledgements() == 1
        with env.store.transaction(env.actor) as tx:
            decision = tx.get_decision(request.id)
            marker = decision["work_admission"]
            current = tx.get_delivery(marker["delivery_id"])
            assert current.state == DeliveryState.PENDING and decision["agent_pending_response"]
            assert tx.get_delivery(old.id).state == DeliveryState.REJECTED
            work = tx.get_work(marker["work_id"])
            assert work.state == WorkState.QUEUED
            assert work.goal in marker["text"]
            assert "我稍后核对再发。" not in marker["text"]
        assert await app.dispatcher.tick() == 1
        assert not foreground.done()
        assert sent(env) == ["【Pot】" + marker["text"]]
        proceed.set()
        await foreground
        assert await app.dispatcher.tick() == 0
        assert sent(env) == ["【Pot】" + marker["text"]]
        with env.store.transaction(env.actor) as tx:
            final = tx.get_decision(request.id)
            assert not final["agent_pending_response"]
            assert final["result"]["delivery_id"] == current.id
            assert tx.get_delivery(current.id).state == DeliveryState.ACCEPTED
            assert [row.id for row in tx.list_delivery() if row.request_event_id == request.id] == [
                current.id
            ]
    finally:
        proceed.set()
        await asyncio.gather(foreground, return_exceptions=True)
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
