"""Independent application boundary tests: real PG/local spool, synthetic models.

Faults deliberately exercise accepted business events and lifecycle transitions,
not implementation mirrors or paid/provider/channel integration.
"""

from __future__ import annotations

import asyncio
from uuid import NAMESPACE_URL, uuid5

import pytest

from groupagent.channels.models import stable_key
from groupagent.coordinator import Decision
from groupagent.domain import DeliveryState, DomainError, EventKind, WorkState
from groupagent.execution import (
    ExecutionRequest,
    ModelReply,
    ModelToolCall,
    ScopedToolEngine,
    ScriptedFakeModel,
    TokenUsage,
)
from groupagent.runtime.contracts import PreparedExecution
from groupagent.tools import ToolGateway, register_request_input_tool

from .test_app import app_for, drain, event

pytestmark = pytest.mark.postgres


def work_decision(goal="Synthetic work"):
    return Decision(
        kind="start_work",
        reason_code="synthetic_work",
        goal=goal,
        directness="direct",
        confidence=1,
    )


async def admit(app, env, value):
    env.channel.append_incoming(value)
    await app.poll_intake()
    await app.process_pending(batched=False)
    with env.store.transaction(env.actor) as tx:
        return tx.get_decision(value.id)


def work_by_source(env, source_id):
    with env.store.transaction(env.actor) as tx:
        return next(work for work in tx.list_work() if work.request_event_id == source_id)


def waiting_work(env, app, *, question="Which reporting period?"):
    value = event(env, "Synthetic source for a question")
    with env.store.transaction(env.actor) as tx:
        tx.ingest_event(value, value.id, "1", 0)
        work = tx.create_work(
            "Prepare a report",
            value.id,
            scope={
                "source_fingerprints": {value.id: tx.event_fingerprint(value.id)},
                "_runtime_input_request": {
                    "question": question,
                    "required_inputs": ["period"],
                    "goal_revision": 1,
                },
            },
            state=WorkState.WAITING_INPUT,
        )
    assert app.notices.tick() == 1
    with env.store.transaction(env.actor) as tx:
        identifier = tx.conn.execute(
            "SELECT intent_id FROM ga_progress_notices WHERE group_id=%s AND work_id=%s",
            (env.actor.group_id, work.id),
        ).fetchone()["intent_id"]
        return work, value, tx.get_delivery(identifier)


@pytest.mark.parametrize("stop_text", ["停止", "@组长 停止", "机器人组长，暂停"])
async def test_control_preempts_a_different_slow_conversation_route(runtime_env, stop_text):
    env = runtime_env
    request = event(env, "请准备一个报告")
    app, _ = app_for(env, {request.id: work_decision()})
    await admit(app, env, request)
    slow = event(env, "我们先聊聊报告背景")
    entered, release = asyncio.Event(), asyncio.Event()

    class ExplicitSyntheticSlowProvider:
        async def decide(self, value, context):
            assert value.id == slow.id
            entered.set()
            await release.wait()
            return Decision(kind="observe", reason_code="synthetic_slow_dialogue")

    app.coordinator.provider_factory = lambda *_: ExplicitSyntheticSlowProvider()
    env.channel.append_incoming(slow)
    await app.poll_intake()
    route = asyncio.create_task(app.process_pending(batched=False))
    try:
        await asyncio.wait_for(entered.wait(), 2)
        stop = event(env, stop_text)
        env.channel.append_incoming(stop)
        await app.poll_intake()
        assert await app.process_pending(controls_only=True) == 1
        assert work_by_source(env, request.id).state in {WorkState.CANCELLED, WorkState.PAUSED}
        assert not route.done(), "Control should not need the unrelated model response"
    finally:
        release.set()
        await asyncio.gather(route, return_exceptions=True)
        await app.close()


async def test_application_lease_blocks_a_second_instance(runtime_env):
    env = runtime_env
    app, models = app_for(env, {})
    held = env.state.claim_process(
        env.service, "application", "synthetic-other-instance", ttl_seconds=30
    )
    assert held
    try:
        with pytest.raises(DomainError, match="Another application"):
            await app.run()
        assert not models
    finally:
        env.state.release_process(env.service, held)
        await app.close()


async def test_lost_application_lease_prevents_next_route_before_heartbeat(runtime_env):
    env = runtime_env
    incoming = event(env, "请解释一下这个词")
    app, _ = app_for(env, {})
    app.config = app.config.model_copy(
        update={
            "runtime": app.config.runtime.model_copy(update={"poll_seconds": 0.02}),
            "conversation": app.config.conversation.model_copy(
                update={"idle_batch_seconds": 1, "max_batch_wait_seconds": 1}
            ),
        }
    )
    app.coordinator.config = app.config
    called, entered, release = asyncio.Event(), asyncio.Event(), asyncio.Event()

    class RecordingSyntheticProvider:
        async def decide(self, value, context):
            called.set()
            return Decision(
                kind="reply",
                reason_code="synthetic_reply",
                response="Synthetic explanation",
                directness="direct",
                confidence=1,
            )

    app.coordinator.provider_factory = lambda *_: RecordingSyntheticProvider()
    original_poll = app.poll_intake

    async def gated_poll():
        entered.set()
        await release.wait()
        return await original_poll()

    app.poll_intake = gated_poll
    running = asyncio.create_task(app.run())
    observed = asyncio.create_task(called.wait())
    replacement = None
    try:
        await asyncio.wait_for(entered.wait(), 3)
        with env.store.transaction(env.service) as tx:
            tx.conn.execute(
                "UPDATE ga_process_leases SET expires_at=clock_timestamp()-interval '1 second' WHERE group_id=%s AND name='application'",
                (env.actor.group_id,),
            )
        replacement = env.state.claim_process(
            env.service, "application", "synthetic-takeover", ttl_seconds=30
        )
        assert replacement and replacement.owner_id != app.instance_id
        env.channel.append_incoming(incoming)
        release.set()
        await asyncio.wait({running, observed}, timeout=3, return_when=asyncio.FIRST_COMPLETED)
        assert not called.is_set(), (
            "A stale application invoked another model before checking its lease"
        )
    finally:
        release.set()
        running.cancel()
        observed.cancel()
        await asyncio.gather(running, observed, return_exceptions=True)
        if replacement:
            env.state.release_process(env.service, replacement)
        await app.close()


async def test_background_failure_stops_active_execution_and_releases_application_lease(
    runtime_env,
):
    env = runtime_env
    request = event(env, "请执行一项慢任务")
    app, models = app_for(env, {request.id: work_decision()}, delay=30)
    await admit(app, env, request)

    async def failing_maintenance(*, force=False):
        for _ in range(100):
            if models:
                await models[0].started.wait()
                raise RuntimeError("Synthetic background failure")
            await asyncio.sleep(0.01)
        raise AssertionError("Synthetic execution did not start")

    app.maintenance = failing_maintenance
    try:
        with pytest.raises(RuntimeError, match="Synthetic background failure"):
            await asyncio.wait_for(app.run(), 4)
        assert models[0].cancelled.is_set()
        assert work_by_source(env, request.id).state == WorkState.PAUSED
        replacement = env.state.claim_process(
            env.service, "application", "synthetic-restart", ttl_seconds=30
        )
        assert replacement
        env.state.release_process(env.service, replacement)
    finally:
        await app.close()


async def test_waiting_input_notice_is_durable_but_stale_state_never_sends(
    runtime_env, monkeypatch
):
    env = runtime_env
    app, _ = app_for(env, {})
    work, _, notice = waiting_work(env, app)
    assert notice.payload_ref == "text:Which reporting period?"
    assert app.notices.tick() == 0
    calls = []
    original = env.channel.send_text

    async def send(*args):
        calls.append(args)
        return await original(*args)

    monkeypatch.setattr(env.channel, "send_text", send)
    try:
        with env.store.transaction(env.actor) as tx:
            tx.transition_work(
                work.id, WorkState.QUEUED, expected_revision=work.revision, reason="input_received"
            )
        assert await app.dispatcher.tick() == 0
        with env.store.transaction(env.actor) as tx:
            assert tx.get_delivery(notice.id).state == DeliveryState.REJECTED
        assert calls == []
    finally:
        await app.close()


async def test_quiet_defers_notices_but_allows_explicit_replies(runtime_env):
    env = runtime_env
    app, _ = app_for(env, {})
    _, _, notice = waiting_work(env, app)
    direct = event(env, "请直接回答我的问题")
    with env.store.transaction(env.actor) as tx:
        tx.ingest_event(direct, direct.id, "1", 0)
        reply = tx.enqueue_reply(direct.id, "Synthetic direct reply")
        tx.conn.execute(
            "INSERT INTO ga_conversation_policy(group_id,subject_id,quiet) VALUES(%s,'*',true)",
            (env.actor.group_id,),
        )
    try:
        assert await app.dispatcher.tick() == 1
        with env.store.transaction(env.actor) as tx:
            assert tx.get_delivery(reply.id).state == DeliveryState.ACCEPTED
            assert tx.get_delivery(notice.id).state == DeliveryState.PENDING
            tx.conn.execute(
                "UPDATE ga_conversation_policy SET quiet=false WHERE group_id=%s",
                (env.actor.group_id,),
            )
        assert await app.dispatcher.tick() == 1
        with env.store.transaction(env.actor) as tx:
            assert tx.get_delivery(notice.id).state == DeliveryState.ACCEPTED
    finally:
        await app.close()


async def test_quiet_applies_to_already_queued_proactive_reply(runtime_env):
    env = runtime_env
    value = event(env, "今天的讨论里有几个可整理的想法")
    app, _ = app_for(
        env,
        {
            value.id: Decision(
                kind="reply",
                reason_code="synthetic_proactive",
                response="我可以帮忙整理成清单。",
                directness="proactive",
                confidence=1,
            )
        },
    )
    result = await admit(app, env, value)
    intent = result["result"]["delivery_id"]
    assert intent
    try:
        with env.store.transaction(env.actor) as tx:
            tx.conn.execute(
                "INSERT INTO ga_conversation_policy(group_id,subject_id,quiet) VALUES(%s,'*',true) ON CONFLICT(group_id,subject_id) DO UPDATE SET quiet=true",
                (env.actor.group_id,),
            )
        assert await app.dispatcher.tick() == 0
        with env.store.transaction(env.actor) as tx:
            assert tx.get_delivery(intent).state == DeliveryState.PENDING
    finally:
        await app.close()


@pytest.mark.parametrize("echo_present", [True, False])
async def test_quote_of_accepted_bot_receipt_stops_only_its_work(runtime_env, echo_present):
    env = runtime_env
    first, second = event(env, "准备第一份报告"), event(env, "准备第二份报告")
    app, _ = app_for(env, {first.id: work_decision("First"), second.id: work_decision("Second")})
    await admit(app, env, first)
    await admit(app, env, second)
    await app.dispatcher.tick()
    with env.store.transaction(env.actor) as tx:
        receipt = next(d for d in tx.list_delivery() if d.request_event_id == first.id)
        assert receipt.state == DeliveryState.ACCEPTED and receipt.platform_message_id
    bot_source = stable_key(env.config, receipt.platform_message_id, server=True)
    bot_id = str(uuid5(NAMESPACE_URL, bot_source))
    if echo_present:
        echo = event(
            env,
            "Synthetic acknowledged request",
            id=bot_id,
            source_key=bot_source,
            actor_id="synthetic-bot",
            platform_message_id=receipt.platform_message_id,
            trusted_actor_origin="agentwechat_authenticated_message_metadata",
        )
        with env.store.transaction(env.actor) as tx:
            tx.ingest_event(echo, echo.id, "1", 0)
    stop = event(
        env,
        "停止",
        kind=EventKind.QUOTE,
        reply_to=bot_id,
        trusted_actor_origin="agentwechat_authenticated_message_metadata",
        channel_account_id=env.config.account_id,
        attachments=[
            {
                "kind": "quote",
                "server_id": receipt.platform_message_id,
                "sender_id": "synthetic-bot",
                "state": "embedded",
                "text": "Synthetic acknowledged request",
            }
        ],
    )
    try:
        await admit(app, env, stop)
        assert work_by_source(env, first.id).state == WorkState.CANCELLED
        assert work_by_source(env, second.id).state == WorkState.QUEUED
    finally:
        await app.close()


async def test_input_update_resumes_same_application_work_and_keeps_usage(runtime_env):
    env = runtime_env
    request = event(env, "请计算并说明结果")
    decisions = {request.id: work_decision("Calculate 2+3 for a reporting period")}
    app, models = app_for(env, decisions)
    normal_factory = app.supervisor.factory
    first = True

    async def question_factory(work, lease, actor, workspace):
        nonlocal first
        if not first:
            return await normal_factory(work, lease, actor, workspace)
        first = False
        gateway = ToolGateway()
        register_request_input_tool(gateway)
        model = ScriptedFakeModel(
            [
                ModelReply(
                    tool_calls=(
                        ModelToolCall(
                            id="period",
                            name="request_input",
                            arguments={"question": "Which reporting period?"},
                        ),
                    ),
                    finish_reason="tool_calls",
                    usage=TokenUsage(input_tokens=20, output_tokens=10),
                )
            ]
        )
        models.append(model)
        return PreparedExecution(
            ScopedToolEngine(model, gateway),
            ExecutionRequest(
                work_id=work.id,
                attempt_id=lease.attempt_id,
                actor=actor,
                objective=work.goal,
                workspace=workspace,
                revision=work.goal_revision,
                allowed_tools=gateway.registered_names,
                trace_id=work.request_event_id,
            ),
        )

    app.supervisor.factory = question_factory
    env.channel.append_incoming(request)
    try:
        works = await drain(app)
        waiting = works[0]
        assert waiting.state == WorkState.WAITING_INPUT
        before = env.state.load_checkpoint(env.actor, waiting.id)
        answer = event(env, "使用2030年第四季度", kind=EventKind.QUOTE, reply_to=request.id)
        decisions[answer.id] = Decision(
            kind="update_work",
            reason_code="synthetic_answer",
            goal="Calculate 2+3; accepted period Q4 2030",
            target_work_id=waiting.id,
            expected_revision=waiting.revision,
            directness="direct",
            confidence=1,
        )
        env.channel.append_incoming(answer)
        completed = await drain(app)
        assert (
            len(completed) == 1
            and completed[0].id == waiting.id
            and completed[0].state == WorkState.COMPLETED
        )
        after = env.state.load_checkpoint(env.actor, waiting.id)
        assert (
            after.usage.rounds == before.usage.rounds + 1
            and after.usage.tokens > before.usage.tokens
        )
        assert (
            after.usage.tool_calls == before.usage.tool_calls
            and after.usage.active_seconds >= before.usage.active_seconds
        )
        assert "Q4 2030" in models[-1].requests[0].model_dump_json()
    finally:
        await app.close()


async def test_batch_rejection_rolls_back_earlier_human_task_and_derived_sources(runtime_env):
    env = runtime_env
    value = event(env, "第一项我负责，再把那项任务标记完成")
    app, _ = app_for(
        env,
        {
            value.id: Decision(
                kind="reply",
                reason_code="synthetic_batch",
                response="两项已处理",
                directness="direct",
                confidence=1,
                action={
                    "op": "task_batch",
                    "actions": [
                        {"op": "claim_task", "title": "Must roll back"},
                        {
                            "op": "update_task",
                            "task_id": "synthetic-missing-task",
                            "expected_revision": 1,
                            "status": "completed",
                        },
                    ],
                },
            )
        },
    )
    try:
        result = await admit(app, env, value)
        with env.store.transaction(env.actor) as tx:
            assert tx.list_team_tasks() == []
            assert not tx.conn.execute(
                "SELECT 1 FROM ga_events WHERE group_id=%s AND source_key LIKE 'action-part:%%'",
                (env.actor.group_id,),
            ).fetchone()
        assert result["result"]["decision"]["response"] != "两项已处理"
    finally:
        await app.close()


async def test_router_cannot_claim_execution_or_send_without_effect_evidence(runtime_env):
    env = runtime_env
    value = event(env, "请用Python实际运行2加3，并发回文件")
    false = "已运行Python，文件也已生成并发送。"
    app, _ = app_for(
        env,
        {
            value.id: Decision(
                kind="reply",
                reason_code="synthetic_false_claim",
                response=false,
                directness="direct",
                confidence=1,
            )
        },
    )
    try:
        await admit(app, env, value)
        with env.store.transaction(env.actor) as tx:
            assert all(d.payload_ref != "text:" + false for d in tx.list_delivery()), (
                "Unexecuted tool claim entered the outbox"
            )
    finally:
        await app.close()


async def test_privacy_removal_during_routing_blocks_late_work_and_notice(runtime_env):
    env = runtime_env
    app, _ = app_for(env, {})
    value = event(env, "Prepare the synthetic private report")
    entered, release = asyncio.Event(), asyncio.Event()

    class StalledSyntheticProvider:
        async def decide(self, source, context):
            entered.set()
            await release.wait()
            return work_decision("Must not use removed evidence")

    app.coordinator.provider_factory = lambda *_: StalledSyntheticProvider()
    env.channel.append_incoming(value)
    await app.poll_intake()
    processing = asyncio.create_task(app.process_pending(batched=False))
    try:
        await asyncio.wait_for(entered.wait(), 2)
        app.coordinator.memory.forget(env.actor, event_ids=[value.id])
        release.set()
        await processing
        with env.store.transaction(env.actor) as tx:
            assert tx.get_event(value.id).content_state == "forgotten"
            assert not tx.list_work() and not tx.list_delivery()
        work, source, notice = waiting_work(
            env, app, question="Synthetic question from removable source"
        )
        app.coordinator.memory.forget(env.actor, event_ids=[source.id])
        assert await app.dispatcher.tick() == 0
        with env.store.transaction(env.actor) as tx:
            assert tx.get_work(work.id).state == WorkState.CANCELLED
            assert tx.get_delivery(notice.id).state == DeliveryState.REJECTED
    finally:
        release.set()
        await asyncio.gather(processing, return_exceptions=True)
        await app.close()


@pytest.mark.parametrize(
    "request_text",
    [
        "为什么使用Python生成PDF会乱码？请解释原因。",
        "请解释运行Python脚本时cwd是什么意思。",
        "如果用Python制作PPT，一般有哪些步骤？",
        "请把句子“请用Python计算2加3”翻译成英语。",
        "不要运行Python，解释sum函数的含义即可。",
        "组长，为什么用Python生成PDF会乱码？",
        "帮我解释一下如何用Python生成PDF。",
    ],
)
async def test_execution_fallback_does_not_turn_explanation_or_quoted_data_into_work(
    runtime_env, request_text
):
    env = runtime_env
    value = event(env, request_text)
    response = "这是一个概念解释或文本翻译，不涉及本次实际操作。"
    app, _ = app_for(
        env,
        {
            value.id: Decision(
                kind="reply",
                reason_code="synthetic_explanation",
                response=response,
                directness="direct",
                confidence=1,
            )
        },
    )
    try:
        result = await admit(app, env, value)
        with env.store.transaction(env.actor) as tx:
            assert tx.list_work() == [], (
                "Conceptual or quoted text was promoted to an executable work"
            )
        assert result["result"]["decision"]["kind"] == "reply"
    finally:
        await app.close()


async def test_unrelated_negative_constraint_does_not_hide_explicit_execution_request(runtime_env):
    env = runtime_env
    value = event(env, "请用Python实际计算2加3，不要联网。")
    app, _ = app_for(
        env,
        {
            value.id: Decision(
                kind="reply",
                reason_code="synthetic_misroute",
                response="我会按这个要求处理。",
                directness="direct",
                confidence=1,
            )
        },
    )
    try:
        result = await admit(app, env, value)
        with env.store.transaction(env.actor) as tx:
            assert len(tx.list_work()) == 1
        assert result["result"]["decision"]["kind"] == "start_work"
    finally:
        await app.close()


async def test_lease_loss_during_router_cannot_acknowledge_request_via_failure_reply(runtime_env):
    env = runtime_env
    value = event(env, "请准备一份合成报告")
    app, _ = app_for(env, {})
    lease = env.state.claim_process(env.service, "application", app.instance_id, ttl_seconds=30)
    assert lease
    app._application_lease = lease
    entered, release = asyncio.Event(), asyncio.Event()

    class StalledSyntheticProvider:
        async def decide(self, source, context):
            entered.set()
            await release.wait()
            return work_decision("Authorized request must remain available for the new application")

    app.coordinator.provider_factory = lambda *_: StalledSyntheticProvider()
    env.channel.append_incoming(value)
    await app.poll_intake()
    processing = asyncio.create_task(app.process_pending(batched=False))
    replacement = None
    try:
        await asyncio.wait_for(entered.wait(), 2)
        with env.store.transaction(env.service) as tx:
            tx.conn.execute(
                "UPDATE ga_process_leases SET expires_at=clock_timestamp()-interval '1 second' WHERE group_id=%s AND name='application'",
                (env.actor.group_id,),
            )
        replacement = env.state.claim_process(
            env.service, "application", "synthetic-new-owner", ttl_seconds=30
        )
        assert replacement
        release.set()
        await asyncio.gather(processing, return_exceptions=True)
        with env.store.transaction(env.actor) as tx:
            assert tx.get_decision(value.id) is None, (
                "Stale error handling consumed the new owner's pending request"
            )
            assert tx.list_work() == [] and tx.list_delivery() == []
    finally:
        release.set()
        await asyncio.gather(processing, return_exceptions=True)
        await app.close()
        if replacement:
            env.state.release_process(env.service, replacement)
