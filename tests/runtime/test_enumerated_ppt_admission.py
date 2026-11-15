"""Real PostgreSQL control-plane checks; no model/API or execution worker runs."""

import pytest

from groupagent.coordinator import Decision
from groupagent.domain import DomainError, EventKind

from .test_fast_admission import counts, ingest, ready

pytestmark = pytest.mark.postgres
SPEC = (
    "请做成并发送恰好2页PPT，主题《合成气象记录》。"
    "第1页标题“记录目的”，写“比较两次观测”；"
    "第2页标题“记录结果”，写“清晨12度”“午间18度”。"
    "只用这些内容，不生成插画。"
)
VISIBLE = ["记录目的", "比较两次观测", "记录结果", "清晨12度", "午间18度"]


async def test_enumerated_ppt_admission_keeps_goal_literals_budget_and_duplicate_identity(
    runtime_env,
):
    env = runtime_env
    app = await ready(env)  # NeverProvider fails if the router is invoked.
    request = await ingest(env, app, SPEC)
    result = await app.coordinator.process(request)
    replay = await app.coordinator.process(request)
    assert result.work_id and replay.duplicate and replay.work_id == result.work_id
    assert replay.delivery_id == result.delivery_id and counts(env) == (1, 1, 0)
    with env.store.transaction(env.actor) as tx:
        work = tx.get_work(result.work_id)
    assert work.goal == SPEC and work.request_event_id == request.id
    assert work.scope["admission_path"] == "deterministic_current_request"
    assert work.scope["outputs"] == [{"format": "pptx", "exact_units": 2, "expected_text": VISIBLE}]
    assert "合成气象记录" not in work.scope["outputs"][0]["expected_text"]
    assert work.input_versions == work.scope["attachment_event_ids"] == []
    assert request.id in work.scope["source_fingerprints"]
    assert work.required_checks == ["result", "artifact:pptx"]
    assert work.budget == app.config.execution.model_dump(mode="json")


@pytest.mark.parametrize("gate", ["quiet", "paused", "document_disabled", "observer"])
async def test_enumerated_ppt_still_obeys_existing_final_gates(runtime_env, gate):
    env = runtime_env
    app = await ready(env)
    actor = env.actor.member_id
    if gate in {"quiet", "paused"}:
        with env.store.transaction(env.actor) as tx:
            tx.conn.execute(
                "INSERT INTO ga_conversation_policy(group_id,subject_id,quiet,mode) VALUES(%s,'*',%s,%s)",
                (env.actor.group_id, gate == "quiet", "paused" if gate == "paused" else "natural"),
            )
    elif gate == "document_disabled":
        app.coordinator.config = app.config.model_copy(
            update={"features": app.config.features.model_copy(update={"document_editing": False})}
        )
    else:
        actor = "synthetic-observer"
        app.coordinator.config = app.config.model_copy(
            update={"group": app.config.group.model_copy(update={"observer_ids": [actor]})}
        )
    request = await ingest(env, app, SPEC, actor_id=actor)
    result = await app.coordinator.process(request)
    assert result.work_id is None and counts(env)[0] == counts(env)[2] == 0
    if gate == "document_disabled":
        assert result.decision.reason_code == "feature_disabled"


async def test_enumerated_ppt_rechecks_source_after_context_snapshot(runtime_env, monkeypatch):
    env = runtime_env
    app = await ready(env)
    request = await ingest(env, app, SPEC)
    original = app.coordinator._context

    def withdrawn(actor, value):
        context = original(actor, value)
        app.coordinator.memory.forget(env.actor, event_ids=[request.id])
        return context

    monkeypatch.setattr(app.coordinator, "_context", withdrawn)
    with pytest.raises(DomainError) as failure:
        await app.coordinator.process(request)
    assert failure.value.code in {"memory_changed", "source_unavailable"}
    assert counts(env) == (0, 0, 0)


@pytest.mark.parametrize(
    "text,changes",
    [(SPEC + "请发邮件给外部账号。", {}), (SPEC, {"kind": EventKind.QUOTE})],
)
async def test_unknown_or_quoted_request_keeps_provider_clarification(runtime_env, text, changes):
    env = runtime_env
    app = await ready(env)
    called = []

    class ExplicitClarifyingFixture:
        async def decide(self, value, context):
            called.append(value.id)
            return Decision(
                kind="clarify",
                reason_code="explicit-fixture-clarification",
                response="请说明还需要处理的范围。",
                directness="direct",
            )

    app.coordinator.provider_factory = lambda actor, value: ExplicitClarifyingFixture()
    request = await ingest(env, app, text, **changes)
    result = await app.coordinator.process(request)
    assert called == [request.id] and result.work_id is None
    assert result.decision.reason_code == "explicit-fixture-clarification"
    assert result.decision.response == "请说明还需要处理的范围。"
    assert counts(env) == (0, 1, 0)
