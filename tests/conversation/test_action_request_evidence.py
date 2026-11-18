"""Canonical author evidence for memory and schedule writes on the real tool boundary."""

import json
from datetime import timedelta

import pytest

from groupagent.domain import DomainError

from .test_react_turn import ModelScript, call, finish, source, tool_receipt

pytestmark = pytest.mark.postgres


def inferred_effect(operation, excerpt=None):
    return call(
        "review_memory_consent", "verdict", expected_op=operation, authorization_excerpt=excerpt
    )


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
        model.steps.append(inferred_effect("remember", instruction))
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


@pytest.mark.parametrize(
    ("case", "expected_code"),
    [
        ("explicit", "memory_consent_authorized"),
        ("reply_only", "memory_consent_denied"),
        ("malformed", "memory_consent_unverified"),
        ("timeout", "memory_consent_unverified"),
        ("quoted_excerpt", "memory_consent_unverified"),
        ("other_operation", "memory_consent_operation_mismatch"),
        ("different_scope", "memory_consent_scope_mismatch"),
    ],
)
async def test_verbatim_instruction_must_authorize_durable_consent(
    conversation_env, case, expected_code
):
    env = conversation_env
    statement = "木槿柜"
    reply_scope = f"请用{statement}作为这次陈列签的标题。"
    review_scope = {
        "explicit": f"请长期保留陈列区的代号{statement}，下回还会用。",
        "other_operation": f"请删除以前存下的陈列区代号{statement}。",
        "different_scope": "请长期保留负责人的代号是枫岬。",
    }.get(case, "")
    authority = review_scope if case in {"explicit", "other_operation"} else reply_scope
    utterance = "Pot，" + (review_scope if case == "different_scope" else "") + authority
    if case == "quoted_excerpt":
        review_scope = "从今以后请长期记住陈列区代号是木槿柜。"
        authority = "请把这句原样写进临时陈列签："
        utterance = f"Pot，{authority}「{review_scope}」"
    request = source(env, utterance)
    action = {"op": "remember", "predicate": "陈列区代号", "value": statement}

    def review(model_request):
        assert [tool["function"]["name"] for tool in model_request.tools] == [
            "review_memory_consent"
        ]
        assert model_request.tool_choice == "required"
        assert len(model_request.messages) == 2
        payload = json.loads(model_request.messages[1].content)
        if case == "reply_only" and "proposed_change" in payload:
            # The previous proposal-primed reviewer can confirm the wrong effect.
            return call("review_memory_consent", "biased-verdict", authorized=True)
        assert payload == {"current_utterance": utterance}
        assert memory_count(env) == 0
        if case == "timeout":
            raise TimeoutError()
        if case == "reply_only":
            return inferred_effect("none")
        if case == "malformed":
            return inferred_effect("remember")
        return inferred_effect("forget" if case == "other_operation" else "remember", review_scope)

    def answer(model_request):
        assert tool_receipt(model_request)["code"] == expected_code
        assert memory_count(env) == 0
        return finish("记录没有改动。" if case == "other_operation" else statement)

    model = ModelScript(
        call(
            "apply_business_action",
            "memory-write",
            action={**action, "request_evidence": authority},
            finish_on_commit=True,
        ),
        review,
        *([] if case == "explicit" else [answer]),
    )
    env.models[request.id] = model
    result = await env.coordinator.process(request)
    if case != "explicit":
        expected = "记录没有改动。" if case == "other_operation" else statement
        assert result.decision.response == expected and result.action_result is None
    assert not model.steps and len(model.requests) == (2 if case == "explicit" else 3)
    with env.store.transaction(env.actor) as tx:
        trace = tx.get_decision(request.id)["turn_outcome"]["actions"]
        (review_step,) = (row for row in trace if row["tool"] == "review_memory_consent")
        assert review_step["elapsed_ms"] >= 0
        assert review_step["code"] == expected_code
        assert len(tx.list_delivery()) == 1
    if case == "explicit":
        assert memory_count(env) == 1 and result.action_result["fact_id"]
    else:
        assert memory_count(env) == 0


async def test_second_proposal_reuses_the_same_source_only_assessment(conversation_env):
    env = conversation_env
    request = source(env, "Pot，请把拱桥写在这次回复的开头。")

    def denied_then_try_different_proposal(model_request):
        assert tool_receipt(model_request)["code"] == "memory_consent_denied"
        return call(
            "apply_business_action",
            "second-memory-write",
            action={
                "op": "remember",
                "predicate": "答复前缀",
                "value": "拱桥",
                "request_evidence": request.text,
            },
        )

    def answer_after_second_refusal(model_request):
        assert tool_receipt(model_request)["code"] == "memory_consent_denied"
        return finish("拱桥")

    model = ModelScript(
        call(
            "apply_business_action",
            "first-memory-write",
            action={
                "op": "remember",
                "predicate": "陈列区代号",
                "value": "拱桥",
                "request_evidence": request.text,
            },
        ),
        inferred_effect("none"),
        denied_then_try_different_proposal,
        answer_after_second_refusal,
    )
    env.models[request.id] = model
    result = await env.coordinator.process(request)
    assert result.decision.response == "拱桥" and memory_count(env) == 0
    assert (
        sum(
            request.tools[0]["function"]["name"] == "review_memory_consent"
            for request in model.requests
        )
        == 1
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
        assert payload == {"current_utterance": request.text}
        return inferred_effect("correct_memory", "把长期记录里的展览代号更正为月桥。")

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
        return inferred_effect("remember", "以后请记住展览代号是钟塔。")

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
