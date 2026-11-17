"""An acknowledgement must describe canonical, current committed facts."""

import json

import pytest

from groupagent.domain import DomainError

from .test_agent_dialogue import source
from .test_react_turn import ModelScript, call, finish


@pytest.mark.postgres
async def test_old_pending_tool_receipt_is_read_but_never_applied_again(conversation_env):
    env = conversation_env
    event = source(env, "Pot，实际使用 Python 核对题目的结果。")
    start = call(
        "start_work",
        "queued",
        goal=event.text,
        work_capabilities=["compute"],
        required_work_tools=["python_compute"],
        outputs=[{"format": "text"}],
    )
    env.models[event.id] = ModelScript(start, DomainError("offline", "interrupted", retryable=True))
    with pytest.raises(DomainError, match="interrupted"):
        await env.coordinator.process(event)
    from psycopg.types.json import Jsonb

    with env.store.transaction(env.actor) as tx:
        body = tx.get_decision(event.id)
        body["turn_contract"] = {
            "event_id": event.id,
            "recipient": "agent",
            "intent": "action",
            "evidence": {"recipient": "agent", "source": "direct_address"},
            "required_actions": [{"kind": "start_work"}],
            "reviewed_reply_required": True,
        }
        body["agent_matched_effects"] = []
        tx.conn.execute(
            "UPDATE ga_decisions SET body=%s WHERE group_id=%s AND event_id=%s",
            (Jsonb(body), tx.group_id, event.id),
        )

    def resume(request):
        payload = json.loads(request.messages[1].content)
        assert payload["prior_receipts"][0]["tool_call_id"] == "queued"
        assert "turn_contract" not in payload["context"]
        return finish(reports=[("queued", "queued")])

    model = ModelScript(resume)
    env.models[event.id] = model
    result = await env.coordinator.process(event)
    assert "实际运行 Python" in result.decision.response
    assert len(model.requests) == 1
    with env.store.transaction(env.actor) as tx:
        assert len(tx.list_work()) == len(tx.list_delivery()) == 1
        assert "turn_contract" not in tx.get_decision(event.id)


@pytest.mark.postgres
async def test_distinct_effects_require_distinct_per_turn_receipts(conversation_env):
    env = conversation_env
    event = source(env, "Pot，整理泥炭地的公开资料，再记下我负责演示。")
    start = call(
        "start_work",
        "work",
        goal="整理泥炭地的公开资料",
        work_capabilities=["public_research"],
        outputs=[{"format": "text"}],
    )
    claim = call("apply_business_action", "claim", action={"op": "claim_task", "title": "演示"})

    def correct(request):
        assert (
            json.loads(request.messages[-1].content)["code"] == "acknowledge_each_committed_effect"
        )
        return finish(reports=[("work", "queued"), ("claim", "committed")])

    env.models[event.id] = ModelScript(start, claim, finish(reports=[("work", "queued")]), correct)
    result = await env.coordinator.process(event)
    assert result.delivery_id and "演示" in result.decision.response
    with env.store.transaction(env.actor) as tx:
        assert len(tx.list_work()) == len(tx.list_team_tasks()) == len(tx.list_delivery()) == 1
