"""Real PG reminder source/occurrence contracts; the decision model is a fixture."""

from datetime import UTC, datetime, timedelta

import pytest

from groupagent.coordinator import Decision
from groupagent.runtime.tool_selection import authorized_tools, select_tools

from .test_app import app_for, event
from .test_tool_selection import CATALOG

pytestmark = pytest.mark.postgres


@pytest.mark.parametrize("replace_body", [False, True])
async def test_literal_body_survives_router_paraphrase_and_schedule_updates(
    runtime_env, replace_body
):
    env = runtime_env
    original_body = "检查合成实验清单"
    request = event(env, "请在十分钟后提醒我检查合成实验清单，只提醒一次。")
    decisions = {
        request.id: Decision(
            kind="reply",
            reason_code="synthetic_create_literal_reminder",
            response="已安排提醒。",
            directness="direct",
            confidence=1,
            action={
                "op": "create_schedule",
                "title": "合成提醒",
                "goal": "检查一下合成实验清单",
                "timing": {
                    "kind": "at",
                    "at": (datetime.now(UTC) + timedelta(minutes=10)).isoformat(),
                },
            },
        )
    }
    app, execution_models = app_for(env, decisions)
    try:
        await app.start()
        await app.coordinator.receive(
            request, stream_id="literal-create", cursor="1", verified_channel=True
        )
        scheduler = app.coordinator.scheduler
        rule = scheduler.list(env.actor)[0]
        assert rule.goal == original_body
        assert rule.scope["reminder_body_source_event_id"] == request.id
        # Schedule.goal is the sole stored body; privacy cleanup already clears it.
        assert "reminder_body" not in rule.scope
        text = (
            "把提醒内容改为“核对合成评分表”。"
            if replace_body
            else "把提醒改到二十分钟后，仍然只提醒一次。"
        )
        update = event(env, text, reply_to=request.id)
        decisions[update.id] = Decision(
            kind="reply",
            reason_code="synthetic_update_literal_reminder",
            response="已更新提醒。",
            directness="direct",
            confidence=1,
            action={
                "op": "change_schedule",
                "schedule_id": rule.id,
                "expected_revision": rule.revision,
                "goal": "路由器不应自行替换的正文",
                "timing": {
                    "kind": "at",
                    "at": (datetime.now(UTC) + timedelta(minutes=20)).isoformat(),
                },
            },
        )
        await app.coordinator.receive(
            update, stream_id="literal-update", cursor="1", verified_channel=True
        )
        revised = scheduler.get(env.actor, rule.id)
        expected = "核对合成评分表" if replace_body else original_body
        body_source = update.id if replace_body else request.id
        assert revised.goal == expected and revised.scope["reminder_body_literal"] is True
        assert revised.scope["reminder_body_source_event_id"] == body_source
        assert body_source in revised.scope["source_fingerprints"]
        assert "reminder_body" not in revised.scope
        occurrence = scheduler.due(env.actor, revised.next_due_at)[0]
        with env.store.transaction(env.actor) as tx:
            work = tx.get_work(occurrence.work_id)
        assert work.goal == expected and work.scope["reminder_body"] == expected
        assert work.scope["reminder_body_source_event_id"] == body_source
        allowed = authorized_tools(work, CATALOG, actor=env.actor)
        assert allowed == frozenset()
        assert select_tools(work, [], allowed) == frozenset()
        assert len(scheduler.list(env.actor)) == 1 and not execution_models
    finally:
        await app.close()
