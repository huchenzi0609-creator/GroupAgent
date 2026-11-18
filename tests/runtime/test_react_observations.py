"""Real persistence/outbox checks. The observation model is an explicit fixture."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta

import pytest

from groupagent.coordinator import Decision
from groupagent.domain import DomainError, TeamTaskState
from groupagent.runtime.observers import ProactiveObserver
from groupagent.scheduling.models import PreparedMessage

from .conftest import source
from .test_app import app_for, event

pytestmark = pytest.mark.postgres


def configure(app, mode):
    conversation = app.config.conversation.model_copy(update={"proactive_mode": mode})
    return app.config.model_copy(update={"conversation": conversation})


def task_with_deadline(env, now):
    origin = source(env, "展台海报还缺馆方许可，今晚提交。")
    with env.store.transaction(env.actor) as tx:
        task = tx.create_team_task(
            "确认海报可张贴范围", due_at=now + timedelta(minutes=50), source_event_id=origin.id
        )
    return origin, task


def contributions() -> Decision:
    return Decision(
        kind="reply",
        directness="proactive",
        response="海报提交前最好核实一下馆方的张贴许可。",
        reason_code="fixture_model_suggestion",
    )


async def test_shadow_replays_only_fresh_facts_after_interrupted_react(runtime_env):
    env = runtime_env
    app, _ = app_for(env, {})
    now = datetime.now(UTC)
    origin, task = task_with_deadline(env, now)
    seen = []

    async def propose(event, observation, _):
        assert event.id == origin.id and observation["task"]["id"] == task.id
        assert observation["task"]["revision"] == task.revision
        assert observation["signal"] == "deadline_approaching"
        assert datetime.fromisoformat(observation["observed_at"]) == now
        assert datetime.fromisoformat(observation["signal_at"]) == task.due_at
        assert observation["seconds_until_due"] == 46 * 60
        seen.append(observation)
        return contributions()

    observer = ProactiveObserver(
        env.store,
        env.actor,
        configure(app, "shadow"),
        propose,
        clock=lambda: now,
        fence=lambda tx: None,
    )
    assert observer._claim() is not None  # Simulates a process lost before the model ran.
    assert await observer.tick() is None and seen == []
    now += timedelta(minutes=4)
    restarted = ProactiveObserver(
        env.store,
        env.actor,
        configure(app, "shadow"),
        propose,
        clock=lambda: now,
        fence=lambda tx: None,
    )
    assert await restarted.tick() == "shadow"
    assert await restarted.tick() is None and len(seen) == 1
    with env.store.transaction(env.actor) as tx:
        row = tx.conn.execute(
            "SELECT to_jsonb(o) AS body FROM ga_react_observations o WHERE group_id=%s",
            (tx.group_id,),
        ).fetchone()["body"]
        assert row["proposal_sha256"] and row["delivery_id"] is None
        assert "张贴许可" not in str(row) and not tx.list_delivery()
    await app.close()


async def test_observer_rechecks_state_after_model_and_again_at_the_sender(runtime_env):
    env = runtime_env
    app, _ = app_for(env, {})
    app.dispatcher.proactive_mode = "active"
    now = datetime.now(UTC)
    _, task = task_with_deadline(env, now)
    calls = []

    async def propose(event, observation, _):
        calls.append(observation)
        if len(calls) == 1:
            with env.store.transaction(env.actor) as tx:
                tx.update_team_task(task.id, expected_revision=task.revision, progress="待核实")
        return contributions()

    observer = ProactiveObserver(
        env.store,
        env.actor,
        configure(app, "active"),
        propose,
        clock=lambda: now,
        fence=lambda tx: None,
    )
    assert await observer.tick() == "silent"  # More recent human progress wins.
    assert await observer.tick() == "published"
    assert await observer.tick() is None
    with env.store.transaction(env.actor) as tx:
        current = tx.get_team_task(task.id)
        tx.update_team_task(
            task.id, expected_revision=current.revision, status=TeamTaskState.COMPLETED
        )
    assert await app.dispatcher.tick() == 0
    with env.store.transaction(env.actor) as tx:
        deliveries = tx.list_delivery()
        assert len(deliveries) == 1 and deliveries[0].state.value == "rejected"
    await app.close()


async def test_transient_observer_failure_retries_original_observation(runtime_env):
    env = runtime_env
    app, _ = app_for(env, {})
    now = datetime.now(UTC)
    task_with_deadline(env, now)
    calls = 0

    async def propose(*_):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise DomainError(
                "model_unavailable", "synthetic temporary provider outage", retryable=True
            )
        return Decision(kind="observe", reason_code="fixture_already_addressed")

    observer = ProactiveObserver(
        env.store,
        env.actor,
        configure(app, "active"),
        propose,
        clock=lambda: now,
        fence=lambda tx: None,
    )
    assert await observer.tick() == "deferred"
    assert await observer.tick() is None and calls == 1
    now += timedelta(seconds=5)
    assert await observer.tick() == "silent" and calls == 2
    with env.store.transaction(env.actor) as tx:
        result = tx.conn.execute(
            "SELECT state,failures FROM ga_react_observations WHERE group_id=%s",
            (tx.group_id,),
        ).fetchall()
        assert result == [{"state": "silent", "failures": 1}]
        assert not tx.list_delivery()
    await app.close()


async def test_clock_enqueues_prepared_send_while_maintenance_is_marked_active(runtime_env):
    env = runtime_env
    app, work_models = app_for(env, {})
    request = event(
        env,
        "倒计时结束后请提醒大家「把展柜照明调暗」。",
        trusted_actor_origin="agentwechat_authenticated_message_metadata",
    )
    with env.store.transaction(env.actor) as tx:
        tx.ingest_event(request, "react-clock-held-out", "1", 0)
    now = datetime.now(UTC)
    rule = app.coordinator.scheduler.create(
        env.actor,
        "展柜照明",
        None,
        {"kind": "at", "at": (now - timedelta(seconds=1)).isoformat()},
        now=now - timedelta(seconds=2),
        authorization_event_id=request.id,
        prepared_message=PreparedMessage(text="把展柜照明调暗", purpose="explicit_reminder"),
    )
    started, release = asyncio.Event(), asyncio.Event()

    async def maintenance_fixture(_=None):
        # Observe the real runtime branch without holding a DB transaction open.
        started.set()
        await release.wait()

    async def run_fixture():
        app._maintenance_lane._active = asyncio.get_running_loop().create_future()
        try:
            await maintenance_fixture()
        finally:
            app._maintenance_lane._active.set_result(None)
            app._maintenance_lane._active = None

    running = asyncio.create_task(run_fixture())
    await started.wait()
    try:
        await asyncio.wait_for(app.schedule_tick(), timeout=2)
        assert work_models == [] and app._maintenance_lane.busy
        occurrence = app.coordinator.scheduler.occurrences(env.actor, rule.id)[0]
        assert occurrence.delivery_id is not None and occurrence.work_id is None
        await app.schedule_tick()
        assert len(app.coordinator.scheduler.occurrences(env.actor, rule.id)) == 1
    finally:
        release.set()
        await running
        await app.close()
