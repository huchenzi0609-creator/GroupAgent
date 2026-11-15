"""Real PG TeamTask effects; every identity/message/model here is synthetic."""

import asyncio
from datetime import UTC, datetime
from types import SimpleNamespace
from uuid import uuid4

import pytest
from psycopg.types.json import Jsonb

from groupagent.domain import ActorContext, EventKind, GroupEvent, Role, WorkState
from groupagent.execution import (
    ExecutionRequest,
    ModelReply,
    ModelToolCall,
    RunState,
    ScopedToolEngine,
    ScriptedFakeModel,
    TokenUsage,
)
from groupagent.memory import MemberService, MemoryService
from groupagent.persistence import UnitOfWork
from groupagent.runtime.team_tools import register_team_tools
from groupagent.tools import EffectState, ToolContext, ToolGateway, ToolScope

pytestmark = pytest.mark.postgres


def source(
    env,
    actor,
    *,
    text="读取作业、拆解并登记真人任务",
    kind=EventKind.TEXT,
    reply_to=None,
    origin="verified_channel_metadata",
    attachments=None,
):
    now = datetime.now(UTC)
    value = GroupEvent(
        id=uuid4().hex,
        source_key="synthetic-team:" + uuid4().hex,
        group_id=actor.group_id,
        actor_id=actor.member_id,
        kind=kind,
        text=text,
        occurred_at=now,
        received_at=now,
        reply_to=reply_to,
        trusted_actor_origin=origin,
        attachments=attachments or [],
    )
    with env.store.transaction(actor) as tx:
        return tx.ingest_event(value, value.id, "1", 0).event


@pytest.fixture
def team_env(runtime_env):
    env = runtime_env
    env.member = ActorContext(
        group_id=env.actor.group_id, member_id="synthetic-member-a", role=Role.MEMBER
    )
    env.other = ActorContext(
        group_id=env.actor.group_id, member_id="synthetic-member-b", role=Role.MEMBER
    )
    env.members = MemberService(env.store)
    env.memory = MemoryService(env.store)
    with env.store.transaction(env.actor) as tx:
        for actor, name in [(env.actor, "群主"), (env.member, "小林"), (env.other, "小周")]:
            tx.conn.execute(
                "INSERT INTO ga_members(group_id,member_id,room_name,confirmed_name,membership_state) VALUES(%s,%s,%s,'旧称','present') ON CONFLICT(group_id,member_id) DO UPDATE SET room_name=EXCLUDED.room_name,confirmed_name='旧称',membership_state='present'",
                (actor.group_id, actor.member_id, name),
            )
    return env


def grant(env, *, actor=None, write=True, extra_scope=None, request=None):
    actor = actor or env.actor
    request = request or source(env, actor)
    with env.store.transaction(actor) as tx:
        work = tx.create_work(
            "按明确授权登记真人任务",
            request.id,
            scope={
                "team_task_write_allowed": write,
                "derived_depth": 0,
                "source_fingerprints": {request.id: tx.event_fingerprint(request.id)},
                **(extra_scope or {}),
            },
        )
        lease = tx.claim_work(work.id, "synthetic-team-worker", ttl_seconds=300)
        work = tx.get_work(work.id)
    root = env.root / uuid4().hex
    root.mkdir()
    return SimpleNamespace(actor=actor, work=work, lease=lease, request=request, workspace=root)


def client(env, allowed):
    gateway = ToolGateway()
    register_team_tools(
        gateway, env.store, env.members, env.memory, allowed.work, allowed.lease, allowed.actor
    )
    scope = ToolScope(
        allowed.actor.group_id,
        allowed.work.id,
        allowed.actor,
        allowed.workspace,
        gateway.registered_names,
        allowed.request.id,
    )
    gateway.bind_scope(scope)

    async def invoke(name, args, key):
        return await gateway.invoke(name, args, ToolContext(scope, key, key, asyncio.Event()))

    return gateway, invoke


def task(env, identifier):
    with env.store.transaction(env.actor) as tx:
        return tx.get_team_task(identifier)


def scope_patch(env, allowed, **fields):
    with env.store.transaction(allowed.actor) as tx:
        tx.conn.execute(
            "UPDATE ga_work_orders SET scope=scope || %s WHERE group_id=%s AND id=%s",
            (Jsonb(fields), allowed.actor.group_id, allowed.work.id),
        )


@pytest.mark.parametrize(
    "options",
    [
        {"write": False},
        {"extra_scope": {"derived_depth": 1}},
        {"extra_scope": {"origin_schedule_id": "synthetic-rule"}},
    ],
)
async def test_document_or_derived_goals_get_read_tools_only(team_env, options):
    env = team_env
    allowed = grant(env, **options)
    gateway, invoke = client(env, allowed)
    assert gateway.registered_names == frozenset({"team_members", "team_tasks"})
    refused = await invoke("team_task_create", {"title": "must not write"}, "bad")
    assert refused.error_code == "tool_not_registered"
    roster = await invoke("team_members", {}, "roster")
    assert roster.success and roster.output["untrusted_content"]
    assert (
        next(x for x in roster.output["items"] if x["member_id"] == env.member.member_id)[
            "display_name"
        ]
        == "小林"
    )
    with env.store.transaction(env.actor) as tx:
        assert tx.list_team_tasks() == []


async def test_owner_create_has_actual_source_empty_internal_quote_and_durable_replay(team_env):
    env = team_env
    allowed = grant(env)
    _, invoke = client(env, allowed)
    arguments = {"title": "整理合成资料", "owner_id": env.member.member_id, "owner_name": "小林"}
    created = await invoke("team_task_create", arguments, "create-one")
    assert created.success and created.effect_state == EffectState.SUCCEEDED
    value = task(env, created.output["task_id"])
    assert value.owner_id == env.member.member_id and value.assignment_kind == "owner_assigned"
    assert value.created_by == env.actor.member_id and value.status.value == "open"
    with env.store.transaction(env.actor) as tx:
        canonical = tx.get_event(value.source_event_id)
        assert (
            canonical.text == ""
            and canonical.kind == EventKind.QUOTE
            and canonical.platform_message_id is None
        )
        assert (
            canonical.trusted_actor_origin == "internal_work_action"
            and canonical.reply_to == allowed.request.id
        )
        assert canonical.attachments[0]["internal"] is True
        assert tx.get_decision(canonical.id)["kind"] == "observe"
        work = tx.get_work(allowed.work.id)
        assert work.scope["source_task_revisions"] == {value.id: 1}
        assert work.scope["team_task_effects"][0]["task_id"] == value.id
    _, restarted = client(env, allowed)
    replay = await restarted("team_task_create", arguments, "create-one")
    assert replay.success and replay.output["replayed"] and replay.output["task_id"] == value.id
    _, fresh = client(env, allowed)
    conflict = await fresh("team_task_create", {**arguments, "title": "Changed"}, "create-one")
    assert (
        conflict.error_code == "idempotency_conflict"
        and conflict.effect_state == EffectState.NO_EFFECT
    )
    with env.store.transaction(env.actor) as tx:
        assert len(tx.list_team_tasks()) == 1


async def test_member_cannot_assign_or_modify_another_member_even_with_wrong_server_target_grant(
    team_env,
):
    env = team_env
    with env.store.transaction(env.actor) as tx:
        foreign = tx.create_team_task("Other person's task", owner_id=env.other.member_id)
    allowed = grant(env, actor=env.member, extra_scope={"team_task_target_ids": [foreign.id]})
    _, invoke = client(env, allowed)
    for name, args in [
        ("team_task_create", {"title": "Not mine", "owner_id": env.other.member_id}),
        ("team_task_claim", {"task_id": foreign.id, "expected_revision": 1}),
        ("team_task_update", {"task_id": foreign.id, "expected_revision": 1, "progress": "wrong"}),
    ]:
        result = await invoke(name, args, name)
        assert (
            result.error_code == "permission_denied"
            and result.effect_state == EffectState.NO_EFFECT
        )
    spoof = await invoke(
        "team_task_create",
        {"title": "bad", "actor_id": env.actor.member_id, "source_event_id": "forged"},
        "spoof",
    )
    assert spoof.error_code == "invalid_input" and task(env, foreign.id).revision == 1


async def test_claim_then_update_tracks_field_sources_without_fabricating_title_author(team_env):
    env = team_env
    identifier = uuid4().hex
    with env.store.transaction(env.actor) as tx:
        tx.conn.execute(
            "INSERT INTO ga_team_tasks(id,group_id,title,owner_id,assignment_kind) VALUES(%s,%s,'Synthetic unassigned duty',NULL,'unassigned')",
            (identifier, env.actor.group_id),
        )
    allowed = grant(env, actor=env.member, extra_scope={"team_task_target_ids": [identifier]})
    _, invoke = client(env, allowed)
    claimed = await invoke(
        "team_task_claim", {"task_id": identifier, "expected_revision": 1}, "claim"
    )
    assert claimed.success and claimed.output["changed"]
    current = task(env, identifier)
    assert current.owner_id == env.member.member_id and current.field_sources["owner_id"] == [
        claimed.output["source_event_id"]
    ]
    assert "title" not in current.field_sources and current.created_by is None
    updated = await invoke(
        "team_task_update",
        {"task_id": identifier, "expected_revision": 2, "progress": "已整理资料，尚未完成"},
        "progress",
    )
    assert updated.success and task(env, identifier).field_sources["progress"] == [
        updated.output["source_event_id"]
    ]
    _, again = client(env, allowed)
    replay = await again(
        "team_task_update",
        {"task_id": identifier, "expected_revision": 2, "progress": "已整理资料，尚未完成"},
        "progress",
    )
    assert replay.output["replayed"] and task(env, identifier).revision == 3


async def test_reading_task_does_not_grant_write_and_human_completion_needs_approval(team_env):
    env = team_env
    with env.store.transaction(env.member) as tx:
        original = tx.create_team_task("Human work still open")
    allowed = grant(env, actor=env.member)
    _, invoke = client(env, allowed)
    assert (await invoke("team_tasks", {}, "read")).success
    missing = await invoke(
        "team_task_update",
        {"task_id": original.id, "expected_revision": 1, "progress": "wrong"},
        "ungranted",
    )
    assert not missing.success and missing.control and missing.effect_state == EffectState.NO_EFFECT
    scope_patch(env, allowed, team_task_target_ids=[original.id])
    requested = await invoke(
        "team_task_update",
        {"task_id": original.id, "expected_revision": 1, "status": "completed"},
        "complete",
    )
    assert (
        not requested.success
        and requested.control
        and task(env, original.id).status.value == "open"
    )
    approved = source(env, env.member, text="我明确确认这项真人任务已完成，请登记完成。")
    with env.store.transaction(env.member) as tx:
        current = tx.get_work(allowed.work.id)
        fingerprints = {
            **current.scope["source_fingerprints"],
            approved.id: tx.event_fingerprint(approved.id),
        }
    scope_patch(
        env,
        allowed,
        team_task_authorization_event_id=approved.id,
        source_fingerprints=fingerprints,
        team_task_completion_ids=[original.id],
    )
    complete = await invoke(
        "team_task_update",
        {"task_id": original.id, "expected_revision": 1, "status": "completed"},
        "complete",
    )
    assert complete.success and task(env, original.id).status.value == "completed"
    with env.store.transaction(env.member) as tx:
        assert tx.get_event(complete.output["source_event_id"]).reply_to == approved.id


async def test_asr_ambiguity_enters_real_engine_waiting_input_with_zero_task_writes(team_env):
    env = team_env
    audio = source(env, env.actor, kind=EventKind.AUDIO, text="")
    transcript = source(
        env,
        env.actor,
        kind=EventKind.QUOTE,
        reply_to=audio.id,
        origin="authenticated_audio_transcription",
        text="小林整理资料",
        attachments=[
            {
                "kind": "audio_transcription",
                "needs_clarification": True,
                "uncertainty": [{"kind": "name"}],
            }
        ],
    )
    allowed = grant(env, request=transcript)
    gateway, _ = client(env, allowed)
    # A separate engine gateway binds exactly the work grant, avoiding fixture invoke scope reuse.
    model = ScriptedFakeModel(
        [
            ModelReply(
                tool_calls=(
                    ModelToolCall(
                        id="assign",
                        name="team_task_create",
                        arguments={"title": "整理资料", "owner_name": "小林"},
                    ),
                ),
                finish_reason="tool_calls",
                usage=TokenUsage(input_tokens=20, output_tokens=10),
            )
        ]
    )
    request = ExecutionRequest(
        work_id=allowed.work.id,
        attempt_id=allowed.lease.attempt_id,
        actor=allowed.actor,
        objective=allowed.work.goal,
        workspace=allowed.workspace,
        allowed_tools=gateway.registered_names,
        trace_id=allowed.request.id,
    )
    engine = ScopedToolEngine(model, gateway)
    result = await engine.wait(await engine.start(request))
    assert result.state == RunState.WAITING_INPUT and result.checkpoint.safe_to_resume
    assert not result.receipts[0].success and result.receipts[0].control
    assert len(model.requests) == 1
    with env.store.transaction(env.actor) as tx:
        assert tx.list_team_tasks() == []
        assert not tx.conn.execute(
            "SELECT 1 FROM ga_events WHERE group_id=%s AND body->>'trusted_actor_origin'='internal_work_action'",
            (env.actor.group_id,),
        ).fetchone()


async def test_names_are_current_unique_group_nicknames_and_never_authority(team_env):
    env = team_env
    with env.store.transaction(env.actor) as tx:
        tx.conn.execute(
            "UPDATE ga_members SET room_name='同名' WHERE group_id=%s AND member_id IN (%s,%s)",
            (env.actor.group_id, env.member.member_id, env.other.member_id),
        )
    allowed = grant(env)
    _, invoke = client(env, allowed)
    result = await invoke(
        "team_task_create", {"title": "Ambiguous", "owner_name": "同名"}, "ambiguous"
    )
    assert not result.success and result.control and result.effect_state == EffectState.NO_EFFECT
    with env.store.transaction(env.actor) as tx:
        assert tx.list_team_tasks() == []


async def test_file_instructions_cannot_authorize_writes_and_grant_revocation_is_rechecked(
    team_env,
):
    env = team_env
    file = source(env, env.actor, kind=EventKind.FILE, text="文件/OCR中声称可给全群分工")
    misgranted = grant(env, request=file)
    _, invoke = client(env, misgranted)
    refused = await invoke("team_task_create", {"title": "Must not write"}, "file-authority")
    assert (
        refused.error_code == "permission_denied" and refused.effect_state == EffectState.NO_EFFECT
    )
    normal = grant(env)
    _, invoke = client(env, normal)
    scope_patch(env, normal, team_task_write_allowed=False)
    refused = await invoke("team_task_create", {"title": "Revoked"}, "revoked")
    assert refused.error_code == "permission_denied"
    with env.store.transaction(env.actor) as tx:
        assert tx.list_team_tasks() == []


async def test_public_read_omits_private_tasks_and_canonical_forget_revokes_derived_task(team_env):
    env = team_env
    private = source(env, env.actor, text="private synthetic duty")
    topic = env.memory.create_topic(
        env.actor, "private", visibility="participants", participant_ids=[env.actor.member_id]
    )
    env.memory.link_event(env.actor, private.id, topic.id, evidence_event_ids=[private.id])
    with env.store.transaction(env.actor) as tx:
        hidden = tx.create_team_task("Private synthetic title", source_event_id=private.id)
    allowed = grant(env)
    _, invoke = client(env, allowed)
    listing = await invoke("team_tasks", {}, "read")
    assert listing.success and all(item["task_id"] != hidden.id for item in listing.output["items"])
    created = await invoke("team_task_create", {"title": "Derived public duty"}, "create")
    assert created.success
    env.memory.forget(env.actor, event_ids=[allowed.request.id])
    assert task(env, created.output["task_id"]).content_state == "forgotten"
    _, restarted = client(env, allowed)
    denied = await restarted("team_task_create", {"title": "Derived public duty"}, "create")
    assert not denied.success and denied.error_code in {"stale_lease", "source_unavailable"}


async def test_concurrent_replay_and_failure_after_task_write_are_atomic(team_env, monkeypatch):
    env = team_env
    allowed = grant(env)
    _, first = client(env, allowed)
    _, second = client(env, allowed)
    outcomes = await asyncio.gather(
        first("team_task_create", {"title": "One duty"}, "same"),
        second("team_task_create", {"title": "One duty"}, "same"),
    )
    assert all(result.success for result in outcomes)
    assert len({result.output["task_id"] for result in outcomes}) == 1
    original = UnitOfWork.record_decision

    def crash(self, *args, **kwargs):
        raise RuntimeError("Synthetic crash before source/receipt commit")

    monkeypatch.setattr(UnitOfWork, "record_decision", crash)
    _, third = client(env, allowed)
    failed = await third("team_task_create", {"title": "Must roll back"}, "crash")
    assert not failed.success
    with env.store.transaction(env.actor) as tx:
        assert len(tx.list_team_tasks()) == 1
        assert (
            tx.conn.execute(
                "SELECT count(*) AS n FROM ga_events WHERE group_id=%s AND body->>'trusted_actor_origin'='internal_work_action'",
                (env.actor.group_id,),
            ).fetchone()["n"]
            == 1
        )
    monkeypatch.setattr(UnitOfWork, "record_decision", original)
    _, recovered = client(env, allowed)
    assert (await recovered("team_task_create", {"title": "Must roll back"}, "crash")).success


async def test_cancellation_and_revision_compare_and_swap_prevent_late_writes(team_env):
    env = team_env
    allowed = grant(env)
    _, invoke = client(env, allowed)
    created = await invoke("team_task_create", {"title": "Mutable duty"}, "create")
    identifier = created.output["task_id"]
    updated = await invoke(
        "team_task_update",
        {"task_id": identifier, "expected_revision": 1, "progress": "first"},
        "first",
    )
    assert updated.success
    stale = await invoke(
        "team_task_update",
        {"task_id": identifier, "expected_revision": 1, "progress": "late"},
        "late",
    )
    assert stale.error_code == "version_conflict" and stale.effect_state == EffectState.NO_EFFECT
    with env.store.transaction(env.actor) as tx:
        current = tx.get_work(allowed.work.id)
        tx.transition_work(current.id, WorkState.CANCELLED, expected_revision=current.revision)
    cancelled = await invoke(
        "team_task_update",
        {"task_id": identifier, "expected_revision": 2, "progress": "cancelled"},
        "cancelled",
    )
    assert cancelled.error_code == "stale_lease" and task(env, identifier).progress == "first"


async def test_forgetting_consumed_file_clears_generated_duties_without_using_file_as_authority(
    team_env,
):
    env = team_env
    data = source(env, env.actor, kind=EventKind.FILE, text="Synthetic assignment data")
    request = source(env, env.actor, text="读取该作业，拆解并登记真人任务")
    with env.store.transaction(env.actor) as tx:
        fingerprints = {value.id: tx.event_fingerprint(value.id) for value in (request, data)}
    allowed = grant(env, request=request, extra_scope={"source_fingerprints": fingerprints})
    _, invoke = client(env, allowed)
    created = await invoke(
        "team_task_create", {"title": "Duty derived from the synthetic file"}, "file-derived"
    )
    assert created.success
    with env.store.transaction(env.actor) as tx:
        provenance = tx.get_decision(created.output["source_event_id"])
        assert data.id in provenance["source_fingerprints"]
        assert tx.get_event(created.output["source_event_id"]).reply_to == request.id
    env.memory.forget(env.actor, event_ids=[data.id])
    assert task(env, created.output["task_id"]).content_state == "forgotten"
    with env.store.transaction(env.actor) as tx:
        assert tx.get_event(request.id).content_state == "active"
