"""Independent privacy races and bounded context tests; all data/models are synthetic."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from uuid import uuid4

import pytest

from groupagent.config import AppConfig, GroupConfig
from groupagent.conversation.provider import ModelDecisionProvider
from groupagent.conversation.service import ConversationCoordinator
from groupagent.coordinator import Decision
from groupagent.domain import DomainError, EventKind, GroupEvent, WorkState
from groupagent.execution import ModelReply, ScriptedFakeModel, TokenUsage
from groupagent.memory import MemberService, MemoryService
from groupagent.scheduling import EveryTiming, Scheduler

from .foreground_model_fixture import candidate_reply, natural_reply

pytestmark = pytest.mark.postgres


def harness(env, *, response="合成夹具答复"):
    model = ScriptedFakeModel(
        [
            ModelReply(
                text=response,
                usage=TokenUsage(input_tokens=1, output_tokens=1),
            )
            for _ in range(5)
        ]
    )
    provider = ModelDecisionProvider(model)
    memory = MemoryService(env.store)
    scheduler = Scheduler(env.store)
    coordinator = ConversationCoordinator(
        AppConfig(
            group=GroupConfig(
                enabled=True,
                internal_id=env.actor.group_id,
                external_group_id=env.config.external_group_id,
                owner_ids=[env.actor.member_id],
            )
        ),
        env.store,
        lambda actor, event: provider,
        memory=memory,
        members=MemberService(env.store),
        scheduler=scheduler,
    )
    return SimpleNamespace(coordinator=coordinator, model=model, memory=memory, scheduler=scheduler)


def emit(env, text, *, at=None, tx=None, **changes):
    stamp = at or datetime.now(UTC)
    identity = uuid4().hex
    data = dict(
        id=identity,
        source_key="context-privacy:" + identity,
        group_id=env.actor.group_id,
        actor_id=env.actor.member_id,
        kind=EventKind.TEXT,
        text=text,
        occurred_at=stamp,
        received_at=stamp,
    )
    data.update(changes)
    value = GroupEvent(**data)
    if tx is not None:
        return tx.ingest_event(value, identity, "1", 0).event
    with env.store.transaction(env.actor) as unit:
        return unit.ingest_event(value, identity, "1", 0).event


def flood_recent(env, *, count=60):
    with env.store.transaction(env.actor) as tx:
        return [emit(env, "普通合成闲聊" + str(index), tx=tx) for index in range(count)]


def model_payload(model):
    return json.loads(model.requests[-1].messages[1].content)


def public_context(context):
    return {key: value for key, value in context.items() if not key.startswith("_")}


@pytest.mark.parametrize("kind", ["quote", "transcript"])
async def test_source_forgotten_between_initial_read_and_context_never_reaches_model(
    runtime_env, monkeypatch, kind
):
    env = runtime_env
    h = harness(env)
    secret = "synthetic-private-source-" + uuid4().hex
    source = emit(
        env,
        secret,
        kind=EventKind.AUDIO if kind == "transcript" else EventKind.IMAGE,
        attachments=[{"kind": "audio" if kind == "transcript" else "image", "handle": secret}],
    )
    request = emit(
        env,
        secret if kind == "transcript" else "请说明这份引用",
        kind=EventKind.QUOTE,
        reply_to=source.id,
        trusted_actor_origin="authenticated_audio_transcription"
        if kind == "transcript"
        else "verified_channel_metadata",
        attachments=[
            {
                "kind": "audio_transcription" if kind == "transcript" else "quote",
                "text": secret,
                "image_handle": secret,
            }
        ],
    )
    original = h.coordinator._context
    changed = False

    def at_boundary(actor, canonical):
        nonlocal changed
        if not changed:
            assert canonical.text == request.text and secret in json.dumps(canonical.attachments)
            h.memory.forget(env.actor, event_ids=[source.id])
            changed = True
        return original(actor, canonical)

    monkeypatch.setattr(h.coordinator, "_context", at_boundary)
    with pytest.raises(DomainError) as caught:
        await h.coordinator.process(request)
    assert caught.value.code in {"memory_changed", "source_unavailable"}
    assert h.model.requests == []
    with env.store.transaction(env.actor) as tx:
        assert not tx.list_delivery() and tx.get_decision(request.id) is None
    result = await h.coordinator.process(request)
    if kind == "transcript":
        assert result.decision.kind == "ignore" and not h.model.requests
    else:
        assert result.decision.kind == "reply" and len(h.model.requests) == 1
        assert secret not in json.dumps(model_payload(h.model), ensure_ascii=False)
        assert model_payload(h.model)["event"]["text"] == request.text


@pytest.mark.parametrize("target", ["request", "quoted_source"])
async def test_participants_link_after_early_topic_probe_is_rechecked_under_snapshot_lock(
    runtime_env, monkeypatch, target
):
    env = runtime_env
    h = harness(env)
    original_source = emit(env, "synthetic source before topic restriction")
    request = emit(env, "请概括这份引用", kind=EventKind.QUOTE, reply_to=original_source.id)
    private = h.memory.create_topic(env.actor, "synthetic private title", visibility="participants")
    target_id = request.id if target == "request" else original_source.id
    original = h.memory.topics_for_event
    changed = False

    def stale_probe(actor, event_id):
        nonlocal changed
        result = original(actor, event_id)
        if event_id == target_id and not changed:
            assert result == []
            h.memory.link_event(env.actor, target_id, private.id, evidence_event_ids=[target_id])
            changed = True
        return result

    monkeypatch.setattr(h.memory, "topics_for_event", stale_probe)
    with pytest.raises(DomainError) as caught:
        await h.coordinator.process(request)
    assert changed and caught.value.code == "permission_denied"
    assert h.model.requests == []
    with env.store.transaction(env.actor) as tx:
        assert tx.get_decision(request.id) is None and not tx.list_delivery()


@pytest.mark.parametrize("record_kind", ["work", "rule", "task"])
async def test_displayed_old_record_sources_enter_reply_provenance_and_later_forget(
    runtime_env, record_kind
):
    env = runtime_env
    answer = "derived-private-answer-" + uuid4().hex
    h = harness(env, response=answer)
    old = emit(env, "older private business source", at=datetime.now(UTC) - timedelta(days=2))
    secret_title = "private-record-" + uuid4().hex
    with env.store.transaction(env.actor) as tx:
        digest = tx.event_fingerprint(old.id)
        if record_kind == "work":
            record = tx.create_work(
                secret_title, old.id, scope={"source_fingerprints": {old.id: digest}}
            )
        elif record_kind == "task":
            record = tx.create_team_task(secret_title, source_event_id=old.id)
    if record_kind == "rule":
        record = h.scheduler.create(
            env.actor,
            secret_title,
            secret_title,
            EveryTiming(interval_seconds=3600),
            request_key="chat:" + old.id,
            scope={
                "source_event_ids": [old.id],
                "source_fingerprints": {old.id: digest},
                "request_source_id": old.id,
            },
        )
    flood_recent(env)
    request = emit(env, "请概括已有安排")
    result = await h.coordinator.process(request)
    payload = model_payload(h.model)
    assert old.id not in {row["id"] for row in payload["context"]["recent_messages"]}
    collection = {"work": "works", "rule": "schedules", "task": "tasks"}[record_kind]
    assert record.id in {row["id"] for row in payload["context"][collection]}
    assert secret_title in json.dumps(payload, ensure_ascii=False)
    with env.store.transaction(env.actor) as tx:
        decision = tx.get_decision(request.id)
        assert old.id in decision["source_fingerprints"] and old.id in decision["source_event_ids"]
    h.memory.forget(env.actor, event_ids=[old.id])
    with env.store.transaction(env.actor) as tx:
        assert tx.get_decision(request.id)["privacy_redacted"] is True
        delivery = tx.get_delivery(result.delivery_id)
        assert (
            delivery.payload_ref == "text:[content removed]"
            and delivery.reason == "source_forgotten"
        )


@pytest.mark.parametrize("scope_kind", ["request_source_only", "fingerprints_only"])
async def test_rule_provenance_uses_canonical_supported_source_fields(runtime_env, scope_kind):
    env = runtime_env
    h = harness(env)
    old = emit(env, "old schedule authority", at=datetime.now(UTC) - timedelta(days=3))
    with env.store.transaction(env.actor) as tx:
        digest = tx.event_fingerprint(old.id)
    scope = (
        {"request_source_id": old.id}
        if scope_kind == "request_source_only"
        else {"source_fingerprints": {old.id: digest}}
    )
    rule = h.scheduler.create(
        env.actor,
        "synthetic older rule",
        "scope-bound source goal",
        EveryTiming(interval_seconds=3600),
        scope=scope,
        request_key="chat:" + old.id,
    )
    flood_recent(env)
    request = emit(env, "请介绍当前提醒")
    await h.coordinator.process(request)
    assert rule.id in {row["id"] for row in model_payload(h.model)["context"]["schedules"]}
    with env.store.transaction(env.actor) as tx:
        assert old.id in tx.get_decision(request.id)["source_fingerprints"]


async def test_forgotten_task_source_is_not_reintroduced_through_memory_hit(runtime_env):
    env = runtime_env
    h = harness(env)
    original = emit(env, "source that will be explicitly forgotten")
    secret = "withdrawn-task-title-" + uuid4().hex
    with env.store.transaction(env.actor) as tx:
        tx.create_team_task(secret, source_event_id=original.id)
    h.memory.forget(env.actor, event_ids=[original.id])
    request = emit(env, "请列一下现有安排")
    await h.coordinator.process(request)
    assert secret not in json.dumps(model_payload(h.model), ensure_ascii=False)


async def test_independent_legacy_task_without_event_source_does_not_break_context(runtime_env):
    env = runtime_env
    h = harness(env)
    with env.store.transaction(env.actor) as tx:
        task = tx.create_team_task("synthetic independently imported duty")
    request = emit(env, "请列出现有分工")
    result = await h.coordinator.process(request)
    assert result.decision.kind == "reply"
    assert task.id in {row["id"] for row in model_payload(h.model)["context"]["tasks"]}
    with env.store.transaction(env.actor) as tx:
        assert None not in tx.get_decision(request.id)["source_event_ids"]


def dense_sources(env, count):
    stamp = datetime.now(UTC) - timedelta(days=5)
    with env.store.transaction(env.actor) as tx:
        events = [
            emit(env, "synthetic historical fragment " + str(index), at=stamp, tx=tx)
            for index in range(count)
        ]
        fingerprints = {event.id: tx.event_fingerprint(event.id) for event in events}
    return events, fingerprints


async def test_work_and_topic_source_growth_omits_whole_items_with_visible_incompleteness(
    runtime_env,
):
    env = runtime_env
    h = harness(env)
    old, fingerprints = dense_sources(env, 540)
    originals = []
    topics = []
    for index in range(3):
        subset = old[index * 180 : (index + 1) * 180]
        topic = h.memory.create_topic(env.actor, "bounded-topic-title-" + str(index))
        topics.append(topic)
        with env.store.transaction(env.actor) as tx:
            work = tx.create_work(
                "bounded-work-goal-" + str(index),
                subset[0].id,
                scope={
                    "topic_id": topic.id,
                    "source_fingerprints": {item.id: fingerprints[item.id] for item in subset},
                },
            )
            tx.conn.execute(
                "INSERT INTO ga_topic_origins(group_id,topic_id,source_event_ids) VALUES(%s,%s,%s)",
                (env.actor.group_id, topic.id, [item.id for item in subset[:20]]),
            )
        originals.append(work)
    flood_recent(env)
    with env.store.transaction(env.actor) as tx:
        internal = [
            emit(env, "eval-billing-internal-" + str(index), kind=EventKind.SYSTEM, tx=tx)
            for index in range(80)
        ]
    request = emit(env, "请介绍这些工作的进度")
    context = h.coordinator._context(env.actor, request)
    assert context["context_incomplete"] is True
    source_ids = set(context["_snapshot"]["source_fingerprints"])
    assert len(source_ids) <= 400
    public = public_context(context)
    shown = {row["id"]: row for row in public["works"]}
    assert 0 < len(shown) < len(originals)
    for work in originals:
        if work.id in shown:
            assert set(work.scope["source_event_ids"]) | {work.request_event_id} <= source_ids
            assert {
                source: context["_snapshot"]["source_fingerprints"][source]
                for source in work.scope["source_fingerprints"]
            } == work.scope["source_fingerprints"]
        else:
            # A compact public status does not hydrate the task's entire
            # conversation archive or consume its transitive 400+ sources.
            summary = next(
                row for row in public["work_facts_overview"]["facts"] if row["id"] == work.id
            )
            assert summary["goal"] == work.goal
            assert "scope" not in summary and "source_fingerprints" not in summary
    assert "synthetic historical fragment" not in json.dumps(public["work_facts_overview"])
    for topic in public["topics"]:
        index = next(index for index, original in enumerate(topics) if original.id == topic["id"])
        assert {item.id for item in old[index * 180 : index * 180 + 20]} <= source_ids
    assert not {item.id for item in internal} & source_ids
    assert "eval-billing-internal" not in json.dumps(public, ensure_ascii=False)
    await h.coordinator.process(request)
    assert model_payload(h.model)["context"]["context_incomplete"] is True


async def test_deterministic_native_quote_stop_survives_omitted_huge_work_context(runtime_env):
    env = runtime_env
    h = harness(env)
    old, fingerprints = dense_sources(env, 410)
    with env.store.transaction(env.actor) as tx:
        huge = tx.create_work(
            "omitted-huge-sensitive-goal", old[0].id, scope={"source_fingerprints": fingerprints}
        )
    flood_recent(env)
    request = emit(env, "停止这个任务", kind=EventKind.QUOTE, reply_to=old[0].id)
    context = h.coordinator._context(env.actor, request)
    assert context["context_incomplete"] and len(context["_snapshot"]["source_fingerprints"]) <= 400
    assert huge.id not in {row["id"] for row in context["works"]}
    summary = next(row for row in context["work_facts_overview"]["facts"] if row["id"] == huge.id)
    assert summary["goal"] == huge.goal and "scope" not in summary
    assert "synthetic historical fragment" not in json.dumps(context["work_facts_overview"])
    result = await h.coordinator.process(request)
    assert result.work_id == huge.id and result.decision.control == "cancel"
    assert not h.model.requests
    with env.store.transaction(env.actor) as tx:
        assert tx.get_work(huge.id).state == WorkState.CANCELLED


async def test_provider_time_forget_discards_candidate_before_business_or_outbox_commit(
    runtime_env,
):
    env = runtime_env
    h = harness(env, response="candidate must never be delivered")
    request = emit(env, "请概括当前讨论")
    inner = h.coordinator.provider_factory(env.actor, request)

    class ForgetDuringProvider:
        async def decide(self, event, context):
            answer = await inner.decide(event, context)
            h.memory.forget(env.actor, event_ids=[request.id])
            return answer

    h.coordinator.provider_factory = lambda actor, event: ForgetDuringProvider()
    with pytest.raises(DomainError) as caught:
        await h.coordinator.process(request)
    assert caught.value.code == "memory_changed"
    with env.store.transaction(env.actor) as tx:
        assert tx.get_decision(request.id) is None and not tx.list_work() and not tx.list_delivery()


def realistic_history(env, *, task_count=0):
    """Routing fixture: real PG rows and Markdown bytes, not an Office/render acceptance claim."""
    from psycopg.types.json import Jsonb

    from groupagent.artifacts import ArtifactScope

    h = harness(env)
    h.coordinator.artifacts = env.registry
    sources, fingerprints = dense_sources(env, 320)
    tasks = []
    works = []
    versions = []
    file_sources = {}
    with env.store.transaction(env.actor) as tx:
        for index in range(task_count):
            tasks.append(tx.create_team_task(f"独立长期任务{index:03d}：整理合成项目进展"))
        for index in range(30):
            subset = [sources[(index * 9 + offset) % len(sources)] for offset in range(80)]
            request = emit(
                env, f"历史资料整理请求{index:02d}", at=datetime.now(UTC) - timedelta(days=3), tx=tx
            )
            goal = (
                f"历史工作{index:02d}：请整理一份可审阅的合成资料说明，保留关键论点、方法、待解决问题和引用。"
                + ("核对来源并梳理结构。" * 45)
            )
            task_map = {tasks[index % 10].id: tasks[index % 10].revision} if tasks else {}
            if tasks:
                goal = tasks[index % 10].title + "。" + goal
            work = tx.create_work(
                goal,
                request.id,
                scope={
                    "allowed_group_ids": [env.actor.group_id],
                    "delivery_target": "origin_group",
                    "outputs": [{"format": "md"}],
                    "source_fingerprints": {item.id: fingerprints[item.id] for item in subset},
                    "source_task_revisions": task_map,
                },
            )
            # Terminal history is a fixture precondition; this test does not claim
            # a renderer or delivery validator was exercised by these seeded jobs.
            tx.conn.execute(
                "UPDATE ga_work_orders SET state='completed' WHERE group_id=%s AND id=%s",
                (env.actor.group_id, work.id),
            )
            work = tx.get_work(work.id)
            scope = ArtifactScope(env.actor.group_id, work.id, env.actor.member_id)
            version = env.artifacts.create(
                scope,
                f"共享-{tasks[index % 10].title if tasks else subset[0].text}-{index:02d}.md",
                f"# 合成资料{index:02d}\n\n只用于上下文容量回归。".encode(),
                provenance={
                    "source_event_ids": [item.id for item in subset] + [request.id],
                    "source_task_revisions": task_map,
                },
            )
            source_ids = [item.id for item in subset] + [request.id]
            source_map = {
                **{item.id: fingerprints[item.id] for item in subset},
                request.id: tx.event_fingerprint(request.id),
            }
            tx.conn.execute(
                "INSERT INTO ga_artifact_catalog(group_id,version_id,file_id,work_id,created_by,name,format,sha256,byte_length,body,source_event_ids,source_fingerprints,shared,expires_at) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,true,%s)",
                (
                    env.actor.group_id,
                    version.version_id,
                    version.file_id,
                    work.id,
                    env.actor.member_id,
                    version.name,
                    version.format,
                    version.sha256,
                    version.size_bytes,
                    Jsonb(version.to_dict()),
                    source_ids,
                    Jsonb(source_map),
                    datetime.now(UTC) + timedelta(days=90),
                ),
            )
            works.append(work)
            versions.append(version)
            file_sources[version.version_id] = (set(source_ids), set(task_map))
        chats = []
        text = ("合成讨论内容包含项目背景、调查过程、时间安排、观点比较和后续待办。" * 40)[:1000]
        assert len(text) == 1000
        for index in range(24):
            marker = f"历史检索锚点{index:02d}："
            chats.append(emit(env, (marker + text)[:1000], tx=tx))
    h.works, h.tasks, h.versions, h.file_sources, h.chats = (
        works,
        tasks,
        versions,
        file_sources,
        chats,
    )
    return h


def capture_actual_router_payload(monkeypatch):
    """Observe the real provider's serialized boundary, including early size rejection."""
    from groupagent.conversation import provider as module

    original = module.redact_credentials
    captured = []

    def capture(value):
        result = original(value)
        try:
            payload = json.loads(result)
        except (TypeError, ValueError):
            return result
        if isinstance(payload, dict) and set(payload) == {"event", "context"}:
            captured.append((result, payload))
        return result

    monkeypatch.setattr(module, "redact_credentials", capture)
    return captured


def payload_composition(captured):
    if not captured:
        return {"captured": False}
    encoded, payload = captured[-1]

    def size(value):
        return len(json.dumps(value, ensure_ascii=False, default=str).encode("utf-8"))

    return {
        "total_utf8_bytes": len(encoded.encode("utf-8")),
        "event_bytes": size(payload["event"]),
        "context_fields": {
            key: {"bytes": size(value), "items": len(value) if isinstance(value, list) else None}
            for key, value in payload["context"].items()
        },
        "under_target_100kb": len(encoded.encode("utf-8")) <= 100_000,
    }


def assert_visible_stress_provenance(context, h):
    snapshot = context["_snapshot"]
    sources = set(snapshot["source_fingerprints"])
    tasks = set(snapshot.get("source_task_revisions", {}))
    counts = {
        "event_sources": len(sources),
        "logical_tasks": len(tasks),
        "works": len(context["works"]),
        "tasks": len(context["tasks"]),
        "files": len(context.get("files", [])),
    }
    assert len(sources) <= 400, counts
    assert len(tasks) <= 100, counts
    by_work = {work.id: work for work in h.works}
    for row in context["works"]:
        original = by_work[row["id"]]
        assert (
            set(original.scope.get("source_event_ids", [])) | {original.request_event_id} <= sources
        ), ("work_source_gap", row["id"], counts)
        assert set(original.scope.get("source_task_revisions", {})) <= tasks, (
            "work_task_gap",
            row["id"],
            counts,
        )
    by_task = {task.id: task for task in h.tasks}
    for row in context["tasks"]:
        assert row["id"] in tasks, ("task_identity_gap", row["id"], counts)
        assert set(by_task[row["id"]].source_event_ids) <= sources, (
            "task_event_gap",
            row["id"],
            counts,
        )
    for row in context.get("files", []):
        events, logical = h.file_sources[row["version_id"]]
        assert events <= sources, (
            "file_source_gap",
            row["version_id"],
            len(events - sources),
            counts,
        )
        assert logical <= tasks, ("file_task_gap", row["version_id"], counts)
    for row in context["recent_messages"]:
        assert row["id"] in sources, ("chat_source_gap", row["id"], counts)


@pytest.mark.parametrize("task_count", [0, 110])
async def test_realistic_completed_history_keeps_router_payload_bounded_and_new_requests_live(
    runtime_env, monkeypatch, record_property, task_count
):
    env = runtime_env
    h = realistic_history(env, task_count=task_count)
    captured = capture_actual_router_payload(monkeypatch)
    reply = Decision(
        kind="reply",
        reason_code="fixture_reply",
        response="已收到你的问题。",
        directness="direct",
        confidence=1,
    )
    action = Decision(
        kind="start_work",
        reason_code="fixture_new_work",
        goal="整理独立的合成说明",
        directness="direct",
        confidence=1,
    )
    model = ScriptedFakeModel(
        [candidate_reply(reply), candidate_reply(action), natural_reply("独立合成说明已经排队。")]
    )
    provider = ModelDecisionProvider(model)
    h.coordinator.provider_factory = lambda actor, event: provider
    first = emit(env, "请简单说说当前进度和资料目录。")
    second = None
    first_context = None
    for index in range(2):
        request = first if index == 0 else emit(env, "新建独立工作，整理一份新的合成说明。")
        if index == 1:
            second = request
        try:
            result = await h.coordinator.process(request)
        except DomainError as error:
            pytest.fail(
                json.dumps(
                    {"error": error.code, "stage": index, **payload_composition(captured)},
                    ensure_ascii=False,
                )
            )
        measured = payload_composition(captured)
        record_property(f"router_payload_{index}_bytes", measured["total_utf8_bytes"])
        record_property(f"router_payload_{index}_under_100kb", measured["under_target_100kb"])
        record_property(
            f"router_payload_{index}_composition", json.dumps(measured, ensure_ascii=False)
        )
        assert measured["total_utf8_bytes"] <= 140_000, measured
        assert len(model.requests) == (1 if index == 0 else 3)
        assert model_payload(model)["event"]["id"] == request.id
        assert result.decision.kind == ("reply" if index == 0 else "start_work")
        replay = await h.coordinator.process(request)
        assert replay.duplicate and len(model.requests) == (1 if index == 0 else 3)
        context = h.coordinator._context(env.actor, request)
        # Only preexisting work/file rows are checked here; the new job has its own
        # decision provenance, asserted below after the second input commits.
        if index == 0:
            first_context = context
    assert second is not None
    with env.store.transaction(env.actor) as tx:
        record = tx.get_decision(second.id)
        assert len(record["source_fingerprints"]) <= 400
        assert len(record.get("source_task_revisions", {})) <= 100
        assert len(tx.list_work(limit=100)) == 31
        assert tx.get_work(h.works[0].id).goal == h.works[0].goal
        assert tx.get_event(h.chats[0].id).text == h.chats[0].text
    assert any(hit.id == h.chats[0].id for hit in h.memory.retrieve(env.actor, "历史检索锚点00"))
    version, data = env.registry.read(env.actor, h.versions[0].version_id)
    assert version.version_id == h.versions[0].version_id and data.startswith(b"# ")
    assert first_context is not None
    assert_visible_stress_provenance(first_context, h)


def test_realistic_logical_task_and_file_closure_is_bounded_before_paid_routing(runtime_env):
    env = runtime_env
    h = realistic_history(env, task_count=110)
    request = emit(env, "请说说之前各项工作的进度和资料目录。")
    context = h.coordinator._context(env.actor, request)
    assert_visible_stress_provenance(context, h)
    assert context["context_incomplete"] is True
    assert h.model.requests == []
