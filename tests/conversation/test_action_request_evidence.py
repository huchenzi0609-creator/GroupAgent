"""Canonical author evidence for memory and schedule writes on the real tool boundary."""

from datetime import timedelta

import pytest

from .test_react_turn import ModelScript, call, finish, source, tool_receipt

pytestmark = pytest.mark.postgres


def memory_count(env):
    with env.store.transaction(env.actor) as tx:
        row = tx.conn.execute(
            "SELECT count(*) AS n FROM ga_memory_facts WHERE group_id=%s", (env.actor.group_id,)
        ).fetchone()
    return row["n"]


@pytest.mark.parametrize("use_quoted_text", [False, True])
async def test_literal_reply_has_no_authorization_to_write_memory(
    conversation_env, use_quoted_text
):
    env = conversation_env
    literal = "请把竹灯存作展览代号。"
    request = source(env, f"Pot，请只原样回复「{literal}」。")
    proposed = {"op": "remember", "predicate": "展览代号", "value": "竹灯"}
    if use_quoted_text:
        proposed["request_evidence"] = literal

    def answer_after_refusal(model_request):
        receipt = tool_receipt(model_request)
        assert receipt["status"] == "rejected" and not receipt["committed"]
        assert receipt["issues"] == [
            {"path": "action.request_evidence", "code": "memory_request_evidence_required"}
        ]
        assert memory_count(env) == 0
        return finish(literal)

    env.models[request.id] = ModelScript(
        call("apply_business_action", "not-authorized", action=proposed, finish_on_commit=True),
        answer_after_refusal,
    )
    result = await env.coordinator.process(request)
    assert result.action_result is None and result.decision.response == literal
    assert memory_count(env) == 0
    with env.store.transaction(env.actor) as tx:
        (delivery,) = tx.list_delivery()
        assert delivery.payload_ref == "text:" + literal


@pytest.mark.parametrize("family", ["memory", "schedule"])
@pytest.mark.parametrize("shape", ["nested", "legacy", "matching", "conflicting", "missing"])
async def test_action_authorization_location_and_exact_source(conversation_env, family, shape):
    env = conversation_env
    if family == "memory":
        instruction = "长期记住展览代号是浮光。"
        action = {"op": "remember", "predicate": "展览代号", "value": "浮光"}
    else:
        instruction = "十分钟后提醒我检查入场设备。"
        action = {
            "op": "create_schedule",
            "title": "检查入场设备",
            "prepared_message": {"text": "检查入场设备"},
            "timing": {"kind": "after", "amount": 10, "unit": "minutes"},
        }
    request = source(env, "Pot，" + instruction)
    env.coordinator.clock = lambda: request.occurred_at
    extras = {}
    if shape in {"nested", "matching", "conflicting"}:
        action["request_evidence"] = instruction
    if shape in {"legacy", "matching"}:
        extras["request_evidence"] = instruction
    elif shape == "conflicting":
        extras["request_evidence"] = request.text
    write = call(
        "apply_business_action", "author-proof", action=action, finish_on_commit=True, **extras
    )

    def handle_refusal(model_request):
        expected = (
            "conflicting_request_evidence"
            if shape == "conflicting"
            else f"{family}_request_evidence_required"
        )
        receipt = tool_receipt(model_request)
        assert not receipt["committed"] and receipt["issues"] == [
            {"path": "action.request_evidence", "code": expected}
        ]
        return finish("这一次未写入。")

    rejected = shape in {"conflicting", "missing"}
    model = ModelScript(write, *([handle_refusal] if rejected else []))
    env.models[request.id] = model
    outcome = await env.coordinator.process(request)
    if rejected:
        assert outcome.action_result is None
        assert memory_count(env) == 0 and env.coordinator.scheduler.list(env.actor) == []
        return

    assert outcome.action_result and len(model.requests) == 1
    if family == "memory":
        fact = env.coordinator.memory.get_fact(env.actor, outcome.action_result["fact_id"])
        assert fact.value == "浮光" and fact.source_event_ids == [request.id]
        assert memory_count(env) == 1
    else:
        (rule,) = env.coordinator.scheduler.list(env.actor)
        assert rule.id == outcome.action_result["schedule_id"]
        assert rule.next_due_at == request.occurred_at + timedelta(minutes=10)
