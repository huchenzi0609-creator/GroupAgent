"""Scoped context integration cases; all data are synthetic, PostgreSQL only."""

import json
from datetime import UTC, datetime, timedelta

import pytest

from groupagent.control import WorkSelector
from groupagent.conversation.context import ContextBuilder, ContextQuery
from groupagent.conversation.references import resolve_work_selection
from groupagent.coordinator import CoordinationResult, Decision
from groupagent.domain import DomainError, EventKind, GroupEvent, Role, WorkState
from groupagent.memory.causality import causal_sources

from .test_app import app_for
from .test_context_privacy import emit, harness, public_context

pytestmark = pytest.mark.postgres


def paused(env, goal="查找合成图片"):
    source = emit(env, goal, at=datetime.now(UTC) - timedelta(days=2))
    with env.store.transaction(env.actor) as tx:
        value = tx.create_work(goal, source.id)
        value = tx.transition_work(
            value.id, WorkState.PAUSED, expected_revision=value.revision, reason="user_pause"
        )
    return source, value


def test_new_goal_omits_old_paused_work_and_its_provenance(runtime_env):
    env = runtime_env
    h = harness(env)
    old, value = paused(env)
    source = emit(env, "Pot，搜索新的合成图片并发两张给我。")
    context = h.coordinator._context(env.actor, source)
    assert context["works"] == [] and context["_works"] == []
    assert context["_focus_work_id"] is None and context["memory"] == []
    assert old.id not in context["_snapshot"]["source_fingerprints"]
    assert value.id in {row["id"] for row in context["_control_works"]}
    assert "scope" not in context["_control_works"][0]
    assert value.goal not in json.dumps(public_context(context), ensure_ascii=False)


def test_native_quoted_followup_consumes_only_relevant_work(runtime_env):
    env = runtime_env
    h = harness(env)
    original, relevant = paused(env, "合成报告甲")
    unrelated, other = paused(env, "合成报告乙")
    source = emit(env, "把这份报告改短。", reply_to=original.id)
    context = h.coordinator._context(env.actor, source)
    assert context["quoted_work_id"] == relevant.id
    assert {row["id"] for row in context["works"]} == {relevant.id}
    assert original.id in context["_snapshot"]["source_fingerprints"]
    assert unrelated.id not in context["_snapshot"]["source_fingerprints"]
    assert other.goal not in json.dumps(public_context(context), ensure_ascii=False)


def test_agent_context_keeps_recent_evidence_across_apparent_topic_changes(runtime_env):
    env = runtime_env
    h = harness(env)
    now = datetime.now(UTC).replace(microsecond=0)
    earlier = emit(env, "合成小组件的作者刚提供了代号雨桥。", at=now - timedelta(seconds=15))
    request = emit(env, "Pot，搜索这个作者的官网。", at=now)
    later = emit(env, "尚未到达当前上下文的后续消息。", at=now + timedelta(seconds=1))

    context = h.coordinator._context(env.actor, request, model_led=True)

    assert context["recent_messages"][0]["id"] == earlier.id
    assert context["conversation_focus"] == "discussion"
    assert context["context_coverage"]["schedules"] == "preview"
    assert later.id not in context["_snapshot"]["source_fingerprints"]
    assert datetime.fromisoformat(context["model_event"]["occurred_at"]) == now
    assert context["model_event"]["occurred_at"].endswith("+08:00")


def test_agent_context_distinguishes_existing_transcript_from_attachment(runtime_env):
    env = runtime_env
    h = harness(env)
    now = datetime.now(UTC)
    audio = emit(env, "", kind=EventKind.AUDIO, at=now - timedelta(seconds=10))
    transcript = emit(
        env,
        "合成语音代号是星湖四。",
        kind=EventKind.QUOTE,
        reply_to=audio.id,
        at=audio.occurred_at,
        trusted_actor_origin="authenticated_audio_transcription",
        attachments=[{"kind": "audio_transcription", "source_event_id": audio.id}],
    )
    request = emit(env, "Pot，刚才说的是哪个代号？", at=now)

    context = h.coordinator._context(env.actor, request, model_led=True)
    observed = next(row for row in context["recent_messages"] if row["id"] == transcript.id)
    assert observed["content_origin"] == "authenticated_audio_transcription"
    assert observed["text"] == transcript.text and observed["reply_to"] == audio.id
    assert transcript.id in context["_snapshot"]["source_fingerprints"]


def test_unrelated_task_revision_is_not_a_chat_dependency(runtime_env):
    env = runtime_env
    h = harness(env)
    old = emit(env, "昨天的人类分工", at=datetime.now(UTC) - timedelta(days=1))
    with env.store.transaction(env.actor) as tx:
        task = tx.create_team_task("无关的人类任务", source_event_id=old.id)
    source = emit(env, "Pot，你好。")
    context = h.coordinator._context(env.actor, source)
    assert context["tasks"] == [] and context["_snapshot"]["source_task_revisions"] == {}
    assert context["_snapshot"]["task_queries"] == []
    with env.store.transaction(env.actor) as tx:
        tx.update_team_task(
            task.id,
            expected_revision=task.revision,
            progress="另一项任务的进度",
            source_event_id=old.id,
        )
    with env.store.transaction(env.actor) as tx:
        h.memory.assert_snapshot_in_tx(tx, context["_snapshot"])


def test_fact_recall_keeps_authorship_and_source_limits_with_aliases(runtime_env):
    env = runtime_env
    h = harness(env)
    original = emit(env, "记住项目代号为合成青原。", at=datetime.now(UTC) - timedelta(days=1))
    fact = h.memory.record_fact(
        env.actor, "项目代号", "合成青原", source_event_ids=[original.id], explicit=True
    )
    source = emit(env, "Pot，项目代号是什么？")
    context = h.coordinator._context(env.actor, source)
    shown = next(row for row in context["memory"] if row["id"] == fact.id)
    assert shown["author_id"] == context["requester_member_id"]
    assert shown["source_refs"][0]["actor_id"] == context["requester_member_id"]
    assert datetime.fromisoformat(shown["source_refs"][0]["occurred_at"]) == original.occurred_at
    assert shown["retrieval_reason"] == "explicit_long_term_record"
    assert original.id in context["_snapshot"]["source_fingerprints"]
    assert env.actor.member_id not in json.dumps(public_context(context))


def test_private_history_never_enters_public_context_or_control_index(runtime_env):
    env = runtime_env
    h = harness(env)
    original, value = paused(env, "仅参与者可见的合成工作")
    private = h.memory.create_topic(env.actor, "私有合成主题", visibility="participants")
    h.memory.link_event(env.actor, original.id, private.id, evidence_event_ids=[original.id])
    source = emit(env, "Pot，你好。")
    context = h.coordinator._context(env.actor, source)
    assert value.id not in {row["id"] for row in context["_control_works"]}
    assert original.id not in context["_snapshot"]["source_fingerprints"]
    assert value.goal not in json.dumps(public_context(context), ensure_ascii=False)


def test_public_metadata_catalog_serves_member_queries_without_control_authority(runtime_env):
    env = runtime_env
    h = harness(env)
    _, value = paused(env, "公开的合成工作")
    member = env.actor.model_copy(update={"member_id": "synthetic-peer", "role": Role.MEMBER})
    request = emit(env, "Pot，你好。", actor_id=member.member_id)
    context = h.coordinator._context(member, request)
    assert context["works"] == []
    assert value.id in {row["id"] for row in context["_control_works"]}
    selector = WorkSelector(basis="query", ownership="group", title_query=value.goal)
    assert resolve_work_selection(
        member, request, selector, context["_control_works"], readonly=True
    ).ids == (value.id,)
    with pytest.raises(DomainError) as caught:
        resolve_work_selection(member, request, selector, context["_control_works"])
    assert caught.value.code == "permission_denied"


def record_work_result(env, request, identifiers):
    result = CoordinationResult(
        event_id=request.id,
        decision=Decision(kind="reply", reason_code="work_status_query", response="合成查询结果"),
        work_id=identifiers[0] if len(identifiers) == 1 else None,
        action_result={"work_ids": list(identifiers)},
    )
    with env.store.transaction(env.actor) as tx:
        tx.record_decision(request.id, {"kind": "reply", "result": result.model_dump(mode="json")})


def test_typed_plural_result_focus_keeps_ids_without_hydrating_paused_contents(runtime_env):
    env = runtime_env
    h = harness(env)
    original_a, first = paused(env, "暂停的合成工作甲")
    original_b, second = paused(env, "暂停的合成工作乙")
    query = emit(env, "列出暂停的工作。")
    record_work_result(env, query, (first.id, second.id))
    chat = emit(env, "Pot，谢谢。")
    context = h.coordinator._context(env.actor, chat)
    assert context["_focus_work_ids"] == (first.id, second.id)
    assert context["focused_work_ids"] == [first.id, second.id]
    assert context["focused_work_id"] is None and context["works"] == []
    sources = context["_snapshot"]["source_fingerprints"]
    assert query.id in sources and original_a.id not in sources and original_b.id not in sources
    followup = emit(env, "就这几项取消。")
    context = h.coordinator._context(env.actor, followup)
    assert context["_focus_work_ids"] == (first.id, second.id)
    assert {row["id"] for row in context["works"]} == {first.id, second.id}


def test_typed_singleton_paused_focus_resumes_and_fresh_goal_resets(runtime_env):
    env = runtime_env
    h = harness(env)
    original, value = paused(env)
    query = emit(env, "查一下这项暂停工作的状态。")
    record_work_result(env, query, (value.id,))
    followup = emit(env, "恢复。")
    context = h.coordinator._context(env.actor, followup)
    assert context["_focus_work_id"] == value.id
    assert context["works"][0]["state"] == "paused"
    fresh = emit(env, "Pot，生成一份介绍太阳系的三页PPT。")
    context = h.coordinator._context(env.actor, fresh)
    assert context["focused_work_ids"] == [] and context["works"] == []
    assert original.id not in context["_snapshot"]["source_fingerprints"]


def test_explicit_member_quote_filters_typed_result_to_public_index(runtime_env):
    env = runtime_env
    h = harness(env)
    _, public = paused(env, "公开合成工作")
    hidden_source, hidden = paused(env, "私有合成工作")
    query = emit(env, "查询这组工作。")
    record_work_result(env, query, (public.id, hidden.id))
    private = h.memory.create_topic(env.actor, "私有合成讨论", visibility="participants")
    h.memory.link_event(
        env.actor, hidden_source.id, private.id, evidence_event_ids=[hidden_source.id]
    )
    member = env.actor.model_copy(update={"member_id": "synthetic-peer", "role": Role.MEMBER})
    request = emit(env, "这些工作的状态呢？", actor_id=member.member_id, reply_to=query.id)
    context = h.coordinator._context(member, request)
    assert context["_focus_work_ids"] == (public.id,)
    assert hidden.id not in json.dumps(public_context(context), ensure_ascii=False)
    assert hidden.goal not in json.dumps(public_context(context), ensure_ascii=False)


def test_context_sequence_excludes_later_ingested_messages_tasks_and_work(runtime_env):
    env = runtime_env
    h = harness(env)
    now = datetime.now(UTC)
    earlier = emit(env, "earlier-ingested-clock-ahead", at=now + timedelta(hours=1))
    request = emit(env, "查询当前相关资料。", at=now)
    later = emit(env, "causal-future-human", at=now - timedelta(hours=1))
    with env.store.transaction(env.actor) as tx:
        tx.create_team_task("causal-future-task", source_event_id=later.id)
        future_work = tx.create_work("causal-future-work", later.id)
    h.memory.record_fact(
        env.actor, "causal-future-fact", "later", source_event_ids=[later.id], explicit=True
    )
    context = ContextBuilder(
        h.coordinator.config, env.store, h.memory, h.coordinator.members, h.scheduler
    ).build(
        env.actor,
        request,
        ContextQuery(resources=frozenset({"tasks", "works"}), memory_terms=("causal",)),
    )
    shown = json.dumps(public_context(context), ensure_ascii=False)
    assert "causal-future" not in shown
    assert earlier.id in {row["id"] for row in context["recent_messages"]}
    assert future_work.id not in {row["id"] for row in context["_control_works"]}
    assert later.id not in context["_snapshot"]["source_fingerprints"]
    another = emit(env, "another later task source")
    with env.store.transaction(env.actor) as tx:
        tx.create_team_task("causal-another-future-task", source_event_id=another.id)
    with env.store.transaction(env.actor) as tx:
        h.memory.assert_snapshot_in_tx(tx, context["_snapshot"])


async def batch_before_queued_query(env):
    parent = emit(env, "两个分工我来负责，先登记。")
    query = emit(env, "请列出现有分工。")
    future = emit(env, "later human task request")
    candidate = Decision(
        kind="reply",
        reason_code="synthetic_atomic_batch",
        response="登记分工。",
        directness="open",
        confidence=1,
        action={
            "op": "task_batch",
            "actions": [
                {"op": "claim_task", "title": "causal-batch-one"},
                {"op": "claim_task", "title": "causal-batch-two"},
            ],
        },
    )
    app, _ = app_for(env, {parent.id: candidate})
    await app.coordinator.process(parent)
    with env.store.transaction(env.actor) as tx:
        tasks = tx.list_team_tasks()
        parts = {source for task in tasks for source in task.source_event_ids}
        query_sequence = tx.conn.execute(
            "SELECT sequence FROM ga_events WHERE id=%s", (query.id,)
        ).fetchone()["sequence"]
        assert all(
            tx.conn.execute("SELECT sequence FROM ga_events WHERE id=%s", (identifier,)).fetchone()[
                "sequence"
            ]
            > query_sequence
            for identifier in parts
        )
    return app, parent, query, future, parts


async def test_atomic_batch_children_inherit_prior_parent_order_and_consumed_closure(runtime_env):
    env = runtime_env
    app, parent, query, future, parts = await batch_before_queued_query(env)
    try:
        with env.store.transaction(env.actor) as tx:
            tx.create_team_task("genuine-future-task", source_event_id=future.id)
        context = app.coordinator._context(env.actor, query)
        assert {row["title"] for row in context["tasks"]} == {
            "causal-batch-one",
            "causal-batch-two",
        }
        sources = set(context["_snapshot"]["source_fingerprints"])
        assert parts | {parent.id} <= sources and future.id not in sources
        with env.store.transaction(env.actor) as tx:
            app.coordinator.memory.assert_snapshot_in_tx(tx, context["_snapshot"])
    finally:
        await app.close()


@pytest.mark.parametrize("change", ["private", "forgotten"])
async def test_atomic_batch_parent_visibility_still_gates_part_task_reads(runtime_env, change):
    env = runtime_env
    app, parent, query, _, _ = await batch_before_queued_query(env)
    try:
        memory = app.coordinator.memory
        if change == "private":
            topic = memory.create_topic(
                env.actor, "restricted batch parent", visibility="participants"
            )
            memory.link_event(env.actor, parent.id, topic.id, evidence_event_ids=[parent.id])
        else:
            memory.forget(env.actor, event_ids=[parent.id])
        context = app.coordinator._context(env.actor, query)
        assert context["tasks"] == []
        assert "causal-batch" not in json.dumps(public_context(context), ensure_ascii=False)
    finally:
        await app.close()


def test_native_quotes_and_unproven_host_markers_keep_their_own_causal_root(runtime_env):
    import hashlib

    env = runtime_env
    h = harness(env)
    parent = emit(env, "earlier human request")
    query = emit(env, "请列出现有分工。")
    native = emit(env, "new human quote", kind=EventKind.QUOTE, reply_to=parent.id)
    identifier = "action-part:" + hashlib.sha256(f"{parent.id}:0".encode()).hexdigest()
    forged = GroupEvent(
        id=identifier,
        source_key=identifier,
        group_id=env.actor.group_id,
        actor_id=env.actor.member_id,
        kind=EventKind.QUOTE,
        text="",
        reply_to=parent.id,
        occurred_at=native.occurred_at,
        received_at=native.received_at,
        attachments=[{"kind": "action_part", "source_event_id": parent.id, "index": 0}],
    )
    with env.store.transaction(env.actor) as tx:
        tx.ingest_event(forged, identifier, "1", 0)
        tx.create_team_task("future-native-quote-task", source_event_id=native.id)
        tx.create_team_task("unproven-marker-task", source_event_id=forged.id)
        assert causal_sources(tx, {native.id, forged.id}) == {native.id, forged.id}
    context = h.coordinator._context(env.actor, query)
    assert context["tasks"] == []
