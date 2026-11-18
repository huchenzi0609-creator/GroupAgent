"""Canonical author evidence for memory and schedule writes on the real tool boundary."""

import json
from datetime import timedelta

import pytest

from groupagent.domain import DomainError

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
    model = ModelScript(write)
    if rejected:
        model.steps.append(handle_refusal)
    elif family == "memory":
        model.steps.append(call("review_memory_consent", "verdict", authorized=True))
    env.models[request.id] = model
    outcome = await env.coordinator.process(request)
    if rejected:
        assert outcome.action_result is None
        assert memory_count(env) == 0 and env.coordinator.scheduler.list(env.actor) == []
        return

    assert outcome.action_result
    if family == "memory":
        assert len(model.requests) == 2
        fact = env.coordinator.memory.get_fact(env.actor, outcome.action_result["fact_id"])
        assert fact.value == "浮光" and fact.source_event_ids == [request.id]
        assert memory_count(env) == 1
    else:
        assert len(model.requests) == 1
        (rule,) = env.coordinator.scheduler.list(env.actor)
        assert rule.id == outcome.action_result["schedule_id"]
        assert rule.next_due_at == request.occurred_at + timedelta(minutes=10)


@pytest.mark.parametrize("verdict", [True, False, "true", None])
async def test_verbatim_instruction_must_authorize_durable_consent(conversation_env, verdict):
    env = conversation_env
    statement = "展览代号是杉汀。"
    utterance = (
        "Pot，请把展览代号是杉汀这件事留在项目记录中，以后还会用。"
        if verdict is True
        else "Pot，这次只用一句展览代号是杉汀来答复我。"
    )
    request = source(env, utterance)
    action = {"op": "remember", "predicate": "展览代号", "value": "杉汀"}

    def review(model_request):
        assert [tool["function"]["name"] for tool in model_request.tools] == [
            "review_memory_consent"
        ]
        assert model_request.tool_choice == "required"
        assert len(model_request.messages) == 2
        assert json.loads(model_request.messages[1].content) == {
            "current_utterance": utterance,
            "proposed_change": action,
        }
        assert memory_count(env) == 0
        if verdict is None:
            raise TimeoutError()
        return call("review_memory_consent", "verdict", authorized=verdict)

    def answer(model_request):
        expected = "memory_consent_denied" if verdict is False else "memory_consent_unverified"
        assert tool_receipt(model_request)["code"] == expected
        assert memory_count(env) == 0
        return finish(statement)

    model = ModelScript(
        call(
            "apply_business_action",
            "memory-write",
            action={**action, "request_evidence": utterance},
            finish_on_commit=True,
        ),
        review,
        *([] if verdict is True else [answer]),
    )
    env.models[request.id] = model
    result = await env.coordinator.process(request)
    if verdict is not True:
        assert result.decision.response == statement and result.action_result is None
    assert not model.steps and len(model.requests) == (2 if verdict is True else 3)
    with env.store.transaction(env.actor) as tx:
        trace = tx.get_decision(request.id)["turn_outcome"]["actions"]
        (review_step,) = (row for row in trace if row["tool"] == "review_memory_consent")
        assert review_step["elapsed_ms"] >= 0
        assert len(tx.list_delivery()) == 1
    if verdict is True:
        assert memory_count(env) == 1 and result.action_result["fact_id"]
        assert review_step["code"] == "memory_consent_authorized"
    else:
        assert memory_count(env) == 0
        assert review_step["code"] == (
            "memory_consent_denied" if verdict is False else "memory_consent_unverified"
        )


async def test_explicit_correction_can_update_the_verified_existing_fact(conversation_env):
    env = conversation_env
    original = source(env, "现有展览代号是杉汀。")
    fact = env.coordinator.memory.record_fact(
        env.actor, "展览代号", "杉汀", source_event_ids=[original.id], explicit=True
    )
    request = source(env, "Pot，请把长期记录里的展览代号更正为月桥。")

    def edit_observed_fact(model_request):
        assert any(row["id"] == fact.id for row in tool_receipt(model_request)["facts"])
        return call(
            "apply_business_action",
            "correct-fact",
            finish_on_commit=True,
            action={
                "op": "correct_memory",
                "fact_id": fact.id,
                "expected_revision": fact.revision,
                "value": "月桥",
                "request_evidence": request.text,
            },
        )

    def approve_correction(model_request):
        payload = json.loads(model_request.messages[1].content)
        assert payload["current_utterance"] == request.text
        assert payload["proposed_change"]["op"] == "correct_memory"
        return call("review_memory_consent", "verdict", authorized=True)

    model = ModelScript(
        call("read_group_context", "record", resource="memory", term="展览代号"),
        edit_observed_fact,
        approve_correction,
    )
    env.models[request.id] = model
    result = await env.coordinator.process(request)
    assert result.action_result and result.action_result["fact_id"] == fact.id
    current = env.coordinator.memory.get_fact(env.actor, fact.id)
    assert current.value == "月桥" and current.revision == fact.revision + 1


async def test_memory_consent_is_fenced_if_its_source_is_withdrawn_during_review(conversation_env):
    env = conversation_env
    request = source(env, "Pot，以后请记住展览代号是钟塔。")

    def revoke_source(model_request):
        assert json.loads(model_request.messages[1].content)["current_utterance"] == request.text
        env.coordinator.memory.forget(env.actor, event_ids=[request.id])
        return call("review_memory_consent", "verdict", authorized=True)

    model = ModelScript(
        call(
            "apply_business_action",
            "memory-write",
            action={
                "op": "remember",
                "predicate": "展览代号",
                "value": "钟塔",
                "request_evidence": request.text,
            },
        ),
        revoke_source,
    )
    env.models[request.id] = model
    with pytest.raises(DomainError) as error:
        await env.coordinator.process(request)
    assert error.value.code == "memory_changed"
    assert len(model.requests) == 2 and memory_count(env) == 0
    with env.store.transaction(env.actor) as tx:
        assert not tx.list_delivery()
