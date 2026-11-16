"""Read outcomes prove their scope without starting or reviving executions."""

import pytest

from groupagent.control import WorkSelector
from groupagent.conversation.work_queries import render_work_query
from groupagent.coordinator import Decision
from groupagent.domain import WorkState

from .test_app import app_for, event
from .test_early_ack import intake

pytestmark = [pytest.mark.postgres, pytest.mark.work_awareness]


@pytest.mark.parametrize("state,label", [("cancelled", "已取消"), ("failed", "执行失败")])
def test_late_accepted_delivery_does_not_overwrite_a_terminal_work_state(state, label):
    snapshot = {
        "complete": True,
        "status": "matched",
        "facts": [
            {
                "goal": "合成图片工作",
                "state": state,
                "delivery": {"phase": "accepted", "accepted": 3, "image_accepted": 2},
            }
        ],
    }
    response = render_work_query(snapshot)
    assert label in response and "此前已发出2张图片" in response
    assert "已完成" not in response


@pytest.mark.parametrize("existing", [False, True])
async def test_empty_overview_never_substitutes_for_a_complete_historical_lookup(
    runtime_env, existing
):
    env = runtime_env
    decisions = {}
    app, models = app_for(env, decisions)
    try:
        original = event(env, "归档的合成调研")
        await intake(app, env, original)
        if existing:
            with env.store.transaction(env.actor) as tx:
                work = tx.create_work("归档的合成调研", original.id)
                tx.transition_work(
                    work.id,
                    WorkState.PAUSED,
                    expected_revision=work.revision,
                    reason="user_request",
                )
        query = event(env, "Pot，归档的合成调研怎么样了？")
        await intake(app, env, query)
        assert not app.coordinator._context(env.actor, query)["work_facts_overview"]["facts"]
        decisions[query.id] = Decision(
            kind="query_work",
            reason_code="synthetic_historical_lookup",
            work_selector=WorkSelector(basis="query", title_query="归档 合成调研"),
            directness="direct",
            confidence=1,
        )
        result = await app.coordinator.process(query)
        snapshot = result.action_result["work_facts"]
        assert snapshot["complete"] and snapshot["observed_at"]
        assert snapshot["status"] == ("matched" if existing else "empty")
        assert snapshot["query_scope"]["visibility"] == "public"
        if existing:
            assert result.work_id == work.id
            assert "按你的要求暂停" in result.decision.response
            with env.store.transaction(env.actor) as tx:
                assert tx.get_work(work.id).state == WorkState.PAUSED
        else:
            assert result.work_id is None and "本群当前可见" in result.decision.response
        assert not models
    finally:
        await app.close()


async def test_partial_query_reports_coverage_instead_of_claiming_a_complete_list(runtime_env):
    env = runtime_env
    decisions = {}
    app, models = app_for(env, decisions)
    try:
        original = event(env, "合成队列")
        await intake(app, env, original)
        for index in range(23):
            request = event(env, f"合成队列工作{index}")
            await intake(app, env, request)
            with env.store.transaction(env.actor) as tx:
                tx.create_work(f"合成队列工作{index}", request.id)
        query = event(env, "Pot，排队工作有哪些？")
        await intake(app, env, query)
        decisions[query.id] = Decision(
            kind="query_work",
            reason_code="synthetic_partial_lookup",
            work_selector=WorkSelector(basis="query", states=(WorkState.QUEUED,), quantifier="all"),
            directness="direct",
            confidence=1,
        )
        result = await app.coordinator.process(query)
        snapshot = result.action_result["work_facts"]
        assert snapshot["status"] == "partial" and not snapshot["complete"]
        assert snapshot["query_scope"]["matched_count"] == 23
        assert len(snapshot["facts"]) == 20 and "部分记录" in result.decision.response
        assert not result.controlled_work_ids and not models
        with env.store.transaction(env.actor) as tx:
            assert len(tx.list_work(state=WorkState.QUEUED)) == 23
            assert not tx.conn.execute(
                "SELECT 1 FROM ga_execution_attempts WHERE group_id=%s", (env.actor.group_id,)
            ).fetchone()
    finally:
        await app.close()
