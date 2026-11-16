from __future__ import annotations

import time

import pytest

from groupagent.domain import DomainError, WorkState

from .test_app import app_for, event
from .test_performance_acceptance import performance

__all__ = ["performance"]
pytestmark = pytest.mark.postgres


class NeverProvider:
    async def decide(self, event, context):
        raise AssertionError("A proven immediate decision must not invoke the model")


async def ready(env):
    app, _ = app_for(env, {})
    app.coordinator.provider_factory = lambda actor, event: NeverProvider()
    await app.start()
    return app


def counts(env):
    with env.store.connect() as conn:
        return tuple(
            conn.execute(
                f"SELECT count(*) AS n FROM {table} WHERE group_id=%s", (env.actor.group_id,)
            ).fetchone()["n"]
            for table in ["ga_work_orders", "ga_deliveries", "ga_budget_reservations"]
        )


async def ingest(env, app, text, **changes):
    value = event(env, text, **changes)
    env.channel.append_incoming(value)
    assert await app.poll_intake() == 1
    return value


async def test_fast_admission_keeps_full_goal_pages_title_and_duplicate_identity(runtime_env):
    env = runtime_env
    app = await ready(env)
    text = "请制作3页Word，介绍哈希表，标题为“哈希表说明”。"
    request = await ingest(env, app, text)
    result = await app.coordinator.process(request)
    replay = await app.coordinator.process(request)
    assert replay.duplicate and replay.work_id == result.work_id
    assert counts(env) == (1, 1, 0)
    with env.store.transaction(env.actor) as tx:
        work = tx.get_work(result.work_id)
    assert work.goal == text
    assert work.scope["admission_path"] == "deterministic_current_request"
    assert work.scope["outputs"] == [
        {"format": "docx", "exact_units": 3, "expected_text": ["哈希表说明"]}
    ]
    assert work.required_checks == ["result", "artifact:docx"]
    assert work.request_event_id == request.id
    assert request.id in work.scope["source_fingerprints"]
    assert work.budget == app.config.execution.model_dump(mode="json")


async def test_actual_compute_admission_keeps_required_tool(runtime_env):
    env = runtime_env
    app = await ready(env)
    request = await ingest(env, app, "帮我计算1到100的平方和。")
    result = await app.coordinator.process(request)
    with env.store.transaction(env.actor) as tx:
        work = tx.get_work(result.work_id)
    assert work.goal == request.text and work.scope["required_tools"] == ["python_compute"]
    assert counts(env) == (1, 1, 0)


@pytest.mark.parametrize("gate", ["quiet", "paused", "natural_disabled", "document_disabled"])
async def test_fast_candidate_still_obeys_shared_gates(runtime_env, gate):
    env = runtime_env
    app = await ready(env)
    if gate in {"quiet", "paused"}:
        with env.store.transaction(env.actor) as tx:
            tx.conn.execute(
                "INSERT INTO ga_conversation_policy(group_id,subject_id,quiet,mode) VALUES(%s,'*',%s,%s)",
                (env.actor.group_id, gate == "quiet", "paused" if gate == "paused" else "natural"),
            )
    else:
        field = "natural_participation" if gate == "natural_disabled" else "document_editing"
        app.coordinator.config = app.config.model_copy(
            update={"features": app.config.features.model_copy(update={field: False})}
        )
    request = await ingest(env, app, "请生成一份合成报告并交付Word。")
    result = await app.coordinator.process(request)
    assert result.work_id is None and counts(env)[0] == 0
    if gate == "document_disabled":
        assert result.decision.reason_code == "feature_disabled"


@pytest.mark.parametrize("change", ["source", "policy"])
async def test_fast_candidate_rejects_source_or_policy_change_after_context(
    runtime_env, monkeypatch, change
):
    env = runtime_env
    app = await ready(env)
    request = await ingest(env, app, "请生成一份合成报告并交付Markdown。")
    original = app.coordinator._context

    def changed(actor, value):
        context = original(actor, value)
        if change == "source":
            app.coordinator.memory.forget(env.actor, event_ids=[request.id])
        else:
            env.store.register_group(env.actor.group_id, 2)
        return context

    monkeypatch.setattr(app.coordinator, "_context", changed)
    with pytest.raises(DomainError) as caught:
        await app.coordinator.process(request)
    assert caught.value.code in {"memory_changed", "policy_changed", "source_unavailable"}
    assert counts(env) == (0, 0, 0)


async def test_discussion_waits_but_new_work_and_controls_keep_priority(runtime_env):
    env = runtime_env
    app = await ready(env)
    assert app.config.conversation.idle_batch_seconds == 2
    ordinary = await ingest(env, app, "大家正在讨论JSON的背景。")
    assert await app.process_pending() == 0
    # Clear the earlier ordinary test event using the unchanged provider path.
    with env.store.transaction(env.actor) as tx:
        tx.record_decision(
            ordinary.id,
            {
                "kind": "observe",
                "result": {
                    "event_id": ordinary.id,
                    "decision": {"kind": "observe", "reason_code": "fixture"},
                },
            },
        )
    request = await ingest(env, app, "请生成一份合成报告并交付Markdown。")
    assert time.monotonic() - app._last_arrival < 2
    assert await app.process_pending() == 1
    with env.store.transaction(env.actor) as tx:
        work = tx.list_work()[0]
    await ingest(env, app, "请生成一份合成报告并交付Markdown。")
    await ingest(env, app, "停止这个任务", reply_to=request.id)
    assert await app.process_pending() == 1
    with env.store.transaction(env.actor) as tx:
        assert tx.get_work(work.id).state == WorkState.CANCELLED
        assert len(tx.list_work()) == 1


async def test_performance_proof_uses_real_work_ack_and_rejects_goal_tampering(
    runtime_env, performance
):
    env = runtime_env
    app = await ready(env)
    request = await ingest(env, app, "请生成一份合成报告并交付Markdown。")
    result = await app.coordinator.process(request)
    with env.store.connect() as conn:
        proof = performance.deterministic_admission_proof(
            conn,
            env.actor,
            request,
            result.work_id,
            result.delivery_id,
            result.decision.model_dump(mode="json"),
            expected_format="md",
        )
    assert proof["valid"] and proof["kind"] == "real_app_deterministic_admission"
    assert counts(env)[2] == 0
    with env.store.transaction(env.actor) as tx:
        tx.conn.execute("UPDATE ga_work_orders SET goal='changed' WHERE id=%s", (result.work_id,))
    with env.store.connect() as conn:
        proof = performance.deterministic_admission_proof(
            conn,
            env.actor,
            request,
            result.work_id,
            result.delivery_id,
            result.decision.model_dump(mode="json"),
            expected_format="md",
        )
    assert not proof["valid"] and not proof["checks"]["original_goal"]
