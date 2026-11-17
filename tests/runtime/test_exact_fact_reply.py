"""Exact authorized record reads; all model responses and group data are synthetic."""

from __future__ import annotations

import json

import pytest

from groupagent.domain import DomainError

from .test_app import event
from .test_early_ack import intake, intents, setup_chat

pytestmark = pytest.mark.postgres


def remember(env, app, predicate, value, **scope):
    source = event(env, "明确记录一条合成群事实。")
    with env.store.transaction(env.actor) as tx:
        tx.ingest_event(source, source.id, "1", 0)
    fact = app.coordinator.memory.record_fact(
        env.actor, predicate, value, source_event_ids=[source.id], explicit=True, **scope
    )
    return fact, source


async def ask(env, app, text):
    request = event(env, text)
    await intake(app, env, request)
    await app.coordinator.process(request)
    with env.store.transaction(env.actor) as tx:
        return request, tx.get_decision(request.id)["result"]["decision"]


def answer_from_memory(model, predicate, *, before_finish=None):
    complete = model.complete

    async def use_fact(request):
        payload = json.loads(request.messages[1].content)
        matches = [row for row in payload["context"]["memory"] if row.get("predicate") == predicate]
        assert len(matches) == 1
        fact = matches[0]
        assert fact["subject_id"] == payload["context"]["requester_member_id"]
        model.text = f"我记下的{predicate}是{fact['value']}。"
        reply = await complete(request)
        if before_finish is not None:
            before_finish(fact)
        return reply

    model.complete = use_fact


@pytest.mark.parametrize(
    "predicate,value,question",
    [
        ("测试代号", "合成青原", "Pot，测试代号是什么？一句话回答。"),
        ("席位数", 12, "Pot，席位数是多少？"),
    ],
)
async def test_unique_exact_groupwide_scalar_is_grounded_in_one_model_turn(
    runtime_env, predicate, value, question
):
    env = runtime_env
    app, model, _ = setup_chat(env)
    try:
        fact, source = remember(env, app, predicate, value)
        assert fact.subject_id == env.actor.member_id and fact.topic_id is None
        answer_from_memory(model, predicate)
        request, result = await ask(env, app, question)
        assert result["response"] == f"我记下的{predicate}是{value}。"
        assert len(model.requests) == 1
        with env.store.transaction(env.actor) as tx:
            assert source.id in tx.get_decision(request.id)["source_fingerprints"]
        assert len(intents(env)) == 1 and intents(env)[0].request_event_id == request.id
    finally:
        await app.close()


async def test_corrected_record_uses_current_value_revision_and_source(runtime_env):
    env = runtime_env
    app, model, _ = setup_chat(env)
    try:
        fact, _ = remember(env, app, "项目简称", "合成旧值")
        answer_from_memory(model, "项目简称")
        _, old = await ask(env, app, "Pot，项目简称是什么？")
        assert old["response"] == "我记下的项目简称是合成旧值。"
        replacement = event(env, "明确更正合成事实。")
        with env.store.transaction(env.actor) as tx:
            tx.ingest_event(replacement, replacement.id, "1", 0)
        app.coordinator.memory.correct_fact(
            env.actor,
            fact.id,
            "合成新值",
            expected_revision=1,
            source_event_ids=[replacement.id],
            explicit=True,
        )
        request, result = await ask(env, app, "Pot，项目简称是啥？请用一句话回答。")
        assert result["response"] == "我记下的项目简称是合成新值。"
        assert len(model.requests) == 2
        with env.store.transaction(env.actor) as tx:
            assert replacement.id in tx.get_decision(request.id)["source_fingerprints"]
    finally:
        await app.close()


async def test_conflict_other_person_and_topic_scopes_fall_back_to_real_router(runtime_env):
    env = runtime_env
    app, model, _ = setup_chat(env)
    model.text = "需要结合具体对象核对记录。"
    try:
        remember(env, app, "方案编号", "合成甲")
        remember(env, app, "方案编号", "合成乙")
        remember(env, app, "职责名称", "合成职责", subject_id="another-member")
        topic = app.coordinator.memory.create_topic(env.actor, "合成话题")
        remember(env, app, "话题简称", "合成话题值", topic_id=topic.id)
        for predicate in ("方案编号", "职责名称", "话题简称"):
            _, result = await ask(env, app, f"Pot，{predicate}是什么？")
            assert result["reason_code"] != "exact_known_fact" and result["response"] == model.text
        assert len(model.requests) == 3
    finally:
        await app.close()


async def test_incomplete_quoted_and_compound_questions_fall_back(runtime_env, monkeypatch):
    env = runtime_env
    app, model, _ = setup_chat(env)
    model.text = "需要根据完整上下文核对。"
    remember(env, app, "项目简称", "合成青原")
    original_context = app.coordinator._context

    def incomplete(actor, request):
        context = original_context(actor, request)
        context["context_incomplete"] = True
        return context

    try:
        monkeypatch.setattr(app.coordinator, "_context", incomplete)
        _, result = await ask(env, app, "Pot，项目简称是什么？")
        assert result["reason_code"] != "exact_known_fact"
        monkeypatch.setattr(app.coordinator, "_context", original_context)
        for text in ("Pot，解释‘项目简称是什么’。", "Pot，项目简称是什么，并帮我研究另一个问题。"):
            _, result = await ask(env, app, text)
            assert result["reason_code"] != "exact_known_fact"
        assert len(model.requests) == 3
    finally:
        await app.close()


async def test_withdrawn_source_is_not_fast_answered_or_exposed_to_model(runtime_env):
    env = runtime_env
    app, model, _ = setup_chat(env)
    model.text = "目前没有可用的记录。"
    try:
        _, source = remember(env, app, "项目简称", "合成隐私旧值")
        app.coordinator.memory.forget(env.actor, event_ids=[source.id])
        _, result = await ask(env, app, "Pot，项目简称是什么？")
        assert result["reason_code"] != "exact_known_fact" and result["response"] == model.text
        assert "合成隐私旧值" not in model.requests[0].messages[1].content
    finally:
        await app.close()


async def test_correction_during_model_response_blocks_stale_reply(runtime_env):
    env = runtime_env
    app, model, _ = setup_chat(env)
    fact, source = remember(env, app, "项目简称", "合成旧值")
    request = event(env, "Pot，项目简称是什么？")
    await intake(app, env, request)

    def racing(observed):
        if observed["revision"] == 1:
            assert observed["value"] == "合成旧值"
            # Reuse the source event so only the logical fact revision changes.
            app.coordinator.memory.correct_fact(
                env.actor,
                fact.id,
                "合成新值",
                expected_revision=1,
                source_event_ids=[source.id],
                explicit=True,
            )

    answer_from_memory(model, "项目简称", before_finish=racing)
    try:
        with pytest.raises(DomainError) as error:
            await app.coordinator.process(request)
        assert error.value.code == "memory_changed" and intents(env) == []
        assert len(model.requests) == 1
        result = await app.coordinator.process(request)
        assert result.decision.response == "我记下的项目简称是合成新值。"
        assert len(model.requests) == 2
    finally:
        await app.close()
