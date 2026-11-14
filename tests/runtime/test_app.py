from __future__ import annotations

import asyncio
import hashlib
from datetime import UTC, datetime
from uuid import uuid4

import pytest

from groupagent.config import AppConfig, GroupConfig
from groupagent.conversation.service import ConversationCoordinator
from groupagent.coordinator import Decision
from groupagent.domain import EventKind, GroupEvent, WorkState
from groupagent.execution import (
    ExecutionRequest,
    ModelReply,
    ScopedToolEngine,
    ScriptedFakeModel,
    TokenUsage,
)
from groupagent.memory import DeterministicFixtureSummarizer, MemberService, MemoryService
from groupagent.runtime.app import RuntimeApp
from groupagent.runtime.contracts import DeliveryDraft, PreparedExecution, ValidatedCandidate
from groupagent.runtime.dispatch import Dispatcher
from groupagent.runtime.executor import WorkSupervisor
from groupagent.scheduling import Scheduler
from groupagent.tools import ToolGateway

pytestmark = pytest.mark.postgres


def event(env, text, **changes):
    now = datetime.now(UTC)
    data = dict(
        id=uuid4().hex,
        source_key=uuid4().hex,
        group_id=env.actor.group_id,
        actor_id=env.actor.member_id,
        kind=EventKind.TEXT,
        text=text,
        occurred_at=now,
        received_at=now,
    )
    data.update(changes)
    return GroupEvent(**data)


def app_for(env, decisions, *, delay=0):
    config = AppConfig(
        group=GroupConfig(
            enabled=True,
            internal_id=env.actor.group_id,
            external_group_id=env.config.external_group_id,
            owner_ids=[env.actor.member_id],
        )
    )

    class Provider:
        async def decide(self, event, context):
            return decisions.get(event.id, Decision(kind="observe", reason_code="fixture_unmapped"))

    provider = Provider()
    memory = MemoryService(env.store, DeterministicFixtureSummarizer())
    coordinator = ConversationCoordinator(
        config,
        env.store,
        lambda actor, event: provider,
        memory=memory,
        members=MemberService(env.store),
        scheduler=Scheduler(env.store),
        artifacts=env.registry,
    )
    models = []

    async def factory(work, lease, actor, workspace):
        model = ScriptedFakeModel(
            [
                ModelReply(
                    text="合成结果：已计算 2 + 3 = 5。",
                    usage=TokenUsage(input_tokens=1, output_tokens=1),
                )
            ],
            delay_seconds=delay,
        )
        models.append(model)
        return PreparedExecution(
            ScopedToolEngine(model, ToolGateway()),
            ExecutionRequest(
                work_id=work.id,
                attempt_id=lease.attempt_id,
                actor=actor,
                objective=work.goal,
                workspace=workspace,
                revision=work.goal_revision,
                trace_id=work.request_event_id,
            ),
        )

    class Validator:
        async def validate(self, work, lease, actor, snapshot, workspace):
            assert snapshot.candidate_text == "合成结果：已计算 2 + 3 = 5。"
            return ValidatedCandidate(
                {"result": "synthetic-content-check"},
                (
                    DeliveryDraft(
                        "text:" + snapshot.candidate_text,
                        hashlib.sha256(snapshot.candidate_text.encode()).hexdigest(),
                        "verified:" + work.id,
                    ),
                ),
            )

    def resolve(identifier):
        return app.resolve_actor(identifier)

    supervisor = WorkSupervisor(
        env.store, env.state, env.service, resolve, factory, Validator(), env.root / "workspaces"
    )
    dispatcher = Dispatcher(env.state, env.service, resolve, env.channel, env.registry)
    app = RuntimeApp(coordinator, env.channel, supervisor, dispatcher)
    return app, models


async def drain(app, *, steps=60):
    for _ in range(steps):
        await app.step()
        await asyncio.sleep(0.01)
        with app.store.transaction(app.owner) as tx:
            works = tx.list_work()
            pending = tx.list_pending_events()
        if (
            not pending
            and works
            and all(
                w.state
                in {
                    WorkState.COMPLETED,
                    WorkState.CANCELLED,
                    WorkState.FAILED,
                    WorkState.PAUSED,
                    WorkState.WAITING_INPUT,
                }
                for w in works
            )
        ):
            return works
    raise AssertionError("Application did not reach an observable durable result")


async def test_end_to_end_local_spool_actual_database_and_restart(runtime_env):
    env = runtime_env
    request = event(env, "帮我计算2加3")
    decision = Decision(
        kind="start_work",
        reason_code="synthetic_work",
        goal="计算2加3",
        directness="direct",
        confidence=1,
    )
    app, models = app_for(env, {request.id: decision})
    await app.start()
    env.channel.append_incoming(request)
    works = await drain(app)
    assert works[0].state == WorkState.COMPLETED
    assert len(models) == 1
    with env.store.transaction(env.actor) as tx:
        assert len(tx.list_delivery()) == 2
        assert all(d.state.value == "accepted" for d in tx.list_delivery())
    await app.close()
    restarted, models2 = app_for(env, {})
    await restarted.step()
    assert not models2
    with env.store.transaction(env.actor) as tx:
        assert len(tx.list_work()) == 1
        assert len(tx.list_delivery()) == 2
    await restarted.close()


async def test_self_claim_commits_once_without_second_confirmation(runtime_env):
    env = runtime_env
    request = event(env, "我想负责做PPT")
    app, models = app_for(
        env,
        {
            request.id: Decision(
                kind="reply",
                response="已记下",
                reason_code="self_claim",
                directness="direct",
                confidence=1,
                action={"op": "claim_task", "title": "制作PPT"},
            )
        },
    )
    env.channel.append_incoming(request)
    await app.step()
    await app.step()
    with env.store.transaction(env.actor) as tx:
        tasks = tx.list_team_tasks()
        assert len(tasks) == 1 and tasks[0].owner_id == env.actor.member_id
        assert not tx.list_work()
        assert len(tx.list_delivery()) == 1
    assert not models
    await app.close()


async def test_fast_control_quote_never_cancels_other_own_work(runtime_env):
    env = runtime_env
    first = event(env, "帮我计算")
    second = event(env, "停止", kind=EventKind.QUOTE, reply_to="different-source")
    app, _ = app_for(
        env,
        {
            first.id: Decision(
                kind="start_work",
                reason_code="work",
                goal="计算",
                directness="direct",
                confidence=1,
            )
        },
    )
    with env.store.transaction(env.actor) as tx:
        tx.ingest_event(first, first.id, "1", 0)
    await app.coordinator.process(first)
    context = app.coordinator._context(env.actor, first)
    control = app.coordinator._fast_control(second, context)
    assert control.kind == "clarify"
    with env.store.transaction(env.actor) as tx:
        assert tx.list_work()[0].state == WorkState.QUEUED
    await app.close()


async def test_stop_runs_while_execution_is_waiting_for_model(runtime_env):
    env = runtime_env
    request = event(env, "帮我计算2加3")
    app, models = app_for(
        env,
        {
            request.id: Decision(
                kind="start_work",
                reason_code="work",
                goal="计算2加3",
                directness="direct",
                confidence=1,
            )
        },
        delay=30,
    )
    env.channel.append_incoming(request)
    await app.step()
    for _ in range(40):
        if models:
            break
        await asyncio.sleep(0.01)
    stop = event(env, "停止")
    env.channel.append_incoming(stop)
    await app.poll_intake()
    assert await app.process_pending(controls_only=True) == 1
    with env.store.transaction(env.actor) as tx:
        assert tx.list_work()[0].state == WorkState.CANCELLED
    await app.close()


async def test_batch_self_tasks_atomic_and_duplicate_safe(runtime_env):
    env = runtime_env
    request = event(env, "PPT和现场拍照我都负责")
    app, _ = app_for(
        env,
        {
            request.id: Decision(
                kind="reply",
                response="已记下",
                reason_code="self_tasks",
                directness="direct",
                confidence=1,
                action={
                    "op": "task_batch",
                    "actions": [
                        {"op": "claim_task", "title": "PPT"},
                        {"op": "claim_task", "title": "现场拍照"},
                    ],
                },
            )
        },
    )
    env.channel.append_incoming(request)
    await app.step()
    await app.step()
    with env.store.transaction(env.actor) as tx:
        assert {task.title for task in tx.list_team_tasks()} == {"PPT", "现场拍照"}
        assert len(tx.list_delivery()) == 1
        assert not tx.list_pending_events()
    await app.close()
