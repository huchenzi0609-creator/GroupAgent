from __future__ import annotations

import pytest

from groupagent.coordinator import Decision
from groupagent.domain import WorkState

from .test_app import app_for, event

pytestmark = pytest.mark.postgres


async def test_group_disable_fences_work_skips_messages_and_resumes_without_model(runtime_env):
    env = runtime_env
    request = event(env, "请准备报告")
    stop = event(env, "请停止本群助手处理")
    resume = event(env, "恢复全群处理")
    decisions = {
        request.id: Decision(
            kind="start_work",
            reason_code="work",
            goal="准备报告",
            directness="direct",
            confidence=1,
        ),
        stop.id: Decision(
            kind="reply",
            reason_code="stop",
            response="暂停",
            directness="direct",
            confidence=1,
            action={"op": "participation", "mode": "disabled"},
        ),
        resume.id: Decision(
            kind="reply",
            reason_code="resume",
            response="恢复",
            directness="direct",
            confidence=1,
            action={"op": "participation", "mode": "natural"},
        ),
    }
    app, models = app_for(env, decisions)
    for item in (request, stop):
        env.channel.append_incoming(item)
        await app.poll_intake()
        await app.process_pending(batched=False)
    with env.store.transaction(env.actor) as tx:
        assert tx.list_work()[0].state == WorkState.PAUSED
    ordinary = event(env, "正常讨论里不应保存的正文")
    env.channel.append_incoming(ordinary)
    await app.poll_intake()
    await app.execute_tick()
    assert not models
    with env.store.transaction(env.actor) as tx:
        assert (
            tx.conn.execute(
                "SELECT 1 FROM ga_events WHERE group_id=%s AND id=%s", (tx.group_id, ordinary.id)
            ).fetchone()
            is None
        )
    env.channel.append_incoming(resume)
    await app.poll_intake()
    await app.process_pending(batched=False)
    assert app.coordinator.current_mode() == "natural"
    with env.store.transaction(env.actor) as tx:
        assert tx.list_work()[0].state == WorkState.QUEUED
    await app.close()


async def test_quiet_keeps_accepted_work_running(runtime_env):
    env = runtime_env
    value = event(env, "先别插话")
    app, _ = app_for(
        env,
        {
            value.id: Decision(
                kind="reply",
                reason_code="quiet",
                response="安静",
                directness="direct",
                confidence=1,
                action={"op": "quiet", "scope": "group", "enabled": True},
            )
        },
    )
    source = event(env, "已接受的工作")
    with env.store.transaction(env.actor) as tx:
        tx.ingest_event(source, source.id, "1", 0)
        work = tx.create_work("照常执行", source.id)
    env.channel.append_incoming(value)
    await app.poll_intake()
    await app.process_pending(batched=False)
    with env.store.transaction(env.actor) as tx:
        assert tx.get_work(work.id).state == WorkState.QUEUED
        policy = tx.conn.execute(
            "SELECT quiet,quiet_since_sequence FROM ga_conversation_policy "
            "WHERE group_id=%s AND subject_id='*'",
            (tx.group_id,),
        ).fetchone()
        sequence = tx.conn.execute(
            "SELECT sequence FROM ga_events WHERE group_id=%s AND id=%s",
            (tx.group_id, value.id),
        ).fetchone()["sequence"]
        assert policy["quiet"] and policy["quiet_since_sequence"] == sequence
        decision = tx.get_decision(value.id)["result"]
        assert decision["decision"]["kind"] == "observe"
        assert decision["action_result"] == {"policy_subject": "*"}
        assert tx.list_delivery() == []
    assert app.coordinator.current_mode() == "natural"
    assert await app.dispatcher.tick() == 0
    assert not list((env.channel.root / "outgoing").glob("*.json"))
    await app.close()
