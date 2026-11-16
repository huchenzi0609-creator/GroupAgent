"""Registered facts and explicitly requested archives have different read plans."""

import json
from datetime import UTC, datetime, timedelta

import pytest

from groupagent.domain import DomainError, Role
from groupagent.memory import DeterministicFixtureSummarizer, MemoryService

from .test_context_privacy import emit, harness, public_context

pytestmark = pytest.mark.postgres


async def archive_history(env, memory):
    memory.summarizer = DeterministicFixtureSummarizer()
    sources = []
    for batch in range(2):
        for index in range(12):
            sources.append(
                emit(
                    env,
                    f"Python archive-only-marker {batch}/{index}: " + "historical discussion " * 50,
                    at=datetime.now(UTC) - timedelta(hours=2),
                )
            )
        assert await memory.compact(env.actor, force=True, token_budget=64000) is not None
    return sources


async def test_plain_qa_does_not_load_matching_archive_but_explicit_recall_does(
    runtime_env, monkeypatch
):
    env = runtime_env
    h = harness(env)
    sources = await archive_history(env, h.memory)
    assert (
        len(
            [
                hit
                for hit in h.memory.retrieve(env.actor, "Python", include_tasks=False)
                if hit.kind == "summary"
            ]
        )
        == 2
    )
    calls = []
    original = MemoryService.retrieve

    def observed(self, actor, query="", **options):
        calls.append((query, options.get("exact_predicate", False)))
        return original(self, actor, query, **options)

    monkeypatch.setattr(MemoryService, "retrieve", observed)
    plain = emit(env, "Pot，用一句话解释Python元组是什么")
    context = h.coordinator._context(env.actor, plain)
    assert context["memory"] == []
    assert calls == [("用一句话解释Python元组", True)]
    assert "archive-only-marker" not in json.dumps(public_context(context))
    assert not {source.id for source in sources} & set(context["_snapshot"]["source_fingerprints"])
    calls.clear()
    recall = emit(env, "Pot，之前讨论的Python方案有哪些？")
    context = h.coordinator._context(env.actor, recall)
    assert calls == [("Python", False)]
    assert {row["kind"] for row in context["memory"]} == {"event", "summary"}
    assert "archive-only-marker" in json.dumps(public_context(context))


@pytest.mark.parametrize("predicate", ["项目代号", "测试代号", "Python项目代号"])
def test_exact_registered_predicate_survives_similar_newer_records(runtime_env, predicate):
    env = runtime_env
    h = harness(env)
    original = emit(env, "明确登记合成代号。", at=datetime.now(UTC) - timedelta(hours=2))
    wanted = h.memory.record_fact(
        env.actor, predicate, "合成青原", source_event_ids=[original.id], explicit=True
    )
    for index in range(12):
        h.memory.record_fact(
            env.actor,
            predicate + str(index),
            "other",
            source_event_ids=[original.id],
            explicit=True,
        )
    request = emit(env, f"Pot，{predicate}是什么？")
    context = h.coordinator._context(env.actor, request)
    assert [(row["id"], row["predicate"]) for row in context["memory"]] == [(wanted.id, predicate)]
    assert (
        h.coordinator._fast_fact_reply(request, context, env.actor).response
        == f"我记下的{predicate}是合成青原。"
    )
    assert original.id in context["_snapshot"]["source_fingerprints"]


def test_exact_fact_source_withdrawal_and_causal_boundary_still_apply(runtime_env):
    env = runtime_env
    h = harness(env)
    original = emit(env, "明确登记合成项目代号。", at=datetime.now(UTC) - timedelta(hours=2))
    h.memory.record_fact(
        env.actor, "项目代号", "retained-only-marker", source_event_ids=[original.id], explicit=True
    )
    request = emit(env, "Pot，项目代号是什么？")
    context = h.coordinator._context(env.actor, request)
    h.memory.forget(env.actor, event_ids=[original.id])
    with pytest.raises(DomainError) as caught:
        with env.store.transaction(env.actor) as tx:
            h.memory.assert_snapshot_in_tx(tx, context["_snapshot"])
    assert caught.value.code == "memory_changed"
    replacement = emit(env, "较晚登记的事实来源。")
    h.memory.record_fact(
        env.actor,
        "项目代号",
        "future-only-marker",
        source_event_ids=[replacement.id],
        explicit=True,
    )
    context = h.coordinator._context(env.actor, request)
    assert context["memory"] == []
    assert "retained-only-marker" not in json.dumps(public_context(context))
    assert "future-only-marker" not in json.dumps(public_context(context))


@pytest.mark.parametrize("viewer", ["owner", "member"])
def test_exact_fact_lookup_cannot_expose_participant_only_records_to_group(runtime_env, viewer):
    env = runtime_env
    h = harness(env)
    private = h.memory.create_topic(
        env.actor, "participant-only exact fact", visibility="participants"
    )
    original = emit(env, "私有登记来源。")
    h.memory.link_event(env.actor, original.id, private.id, evidence_event_ids=[original.id])
    h.memory.record_fact(
        env.actor,
        "测试代号",
        "private-exact-marker",
        source_event_ids=[original.id],
        explicit=True,
        topic_id=private.id,
    )
    actor = (
        env.actor
        if viewer == "owner"
        else env.actor.model_copy(update={"member_id": "synthetic-reader", "role": Role.MEMBER})
    )
    request = emit(env, "Pot，测试代号是什么？", actor_id=actor.member_id)
    context = h.coordinator._context(actor, request)
    assert context["memory"] == []
    assert "private-exact-marker" not in json.dumps(public_context(context))
