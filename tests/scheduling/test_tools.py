"""Trusted work-scoped scheduling tool integration with real PostgreSQL."""

import asyncio
import os
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from uuid import uuid4

import pytest

from groupagent.domain import ActorContext, EventKind, GroupEvent, Role, WorkState
from groupagent.persistence import PostgresStore, UnitOfWork
from groupagent.runtime.scheduling_tools import register_scheduling_tools
from groupagent.scheduling import EveryTiming, Scheduler
from groupagent.scheduling.triggers import EventTriggerService
from groupagent.tools import EffectState, ToolContext, ToolGateway, ToolScope

pytestmark = pytest.mark.postgres


@pytest.fixture(scope="module")
def store():
    dsn = os.environ.get("GROUPAGENT_TEST_DSN")
    if not dsn:
        pytest.skip("Requires isolated PostgreSQL")
    result = PostgresStore(dsn)
    result.migrate()
    return result


@pytest.fixture
def env(store, tmp_path):
    group = "scheduling-tools-" + uuid4().hex
    store.register_group(group)
    actor = ActorContext(group_id=group, member_id="synthetic-member", role=Role.MEMBER)
    owner = ActorContext(group_id=group, member_id="synthetic-owner", role=Role.OWNER)
    stamp = datetime.now(UTC)
    request = GroupEvent(
        id=uuid4().hex,
        source_key="synthetic:" + uuid4().hex,
        group_id=group,
        actor_id=actor.member_id,
        kind=EventKind.TEXT,
        text="Create a bounded follow-up for my work",
        occurred_at=stamp,
        received_at=stamp,
    )
    with store.transaction(actor) as tx:
        tx.ingest_event(request, "fixture:" + request.id, "1", 0)
        work = tx.create_work(
            "Create authorized follow-ups",
            request.id,
            scope={
                "source_fingerprints": {request.id: tx.event_fingerprint(request.id)},
                "schedule_creation_allowed": True,
                "derived_depth": 0,
            },
        )
        lease = tx.claim_work(work.id, "synthetic-worker", ttl_seconds=300)
        work = tx.get_work(work.id)
        task = tx.create_team_task("Synthetic personal task")
    return SimpleNamespace(
        store=store,
        actor=actor,
        owner=owner,
        work=work,
        lease=lease,
        request=request,
        task=task,
        root=tmp_path.resolve(),
        scheduler=Scheduler(store),
        triggers=EventTriggerService(
            store, lambda member: actor if member == actor.member_id else owner
        ),
    )


def client(env):
    gateway = ToolGateway()
    register_scheduling_tools(
        gateway, env.store, env.scheduler, env.triggers, env.work, env.lease, env.actor
    )
    scope = ToolScope(
        env.actor.group_id,
        env.work.id,
        env.actor,
        env.root,
        gateway.registered_names,
        env.request.id,
    )
    gateway.bind_scope(scope)

    async def invoke(name, arguments, key):
        return await gateway.invoke(name, arguments, ToolContext(scope, key, key, asyncio.Event()))

    return invoke


def create_args(**extra):
    return {
        "title": "Synthetic follow-up",
        "goal": "Produce a bounded report",
        "timing": {"kind": "every", "interval_seconds": 3600},
        **extra,
    }


async def test_tool_goal_update_cannot_keep_a_stale_reminder_hint(env):
    with env.store.transaction(env.actor) as tx:
        fingerprints = {env.request.id: tx.event_fingerprint(env.request.id)}
    rule = env.scheduler.create(
        env.actor,
        "Synthetic reminder",
        "Check the list",
        EveryTiming(interval_seconds=3600),
        scope={"reminder_only": True, "source_fingerprints": fingerprints},
    )
    receipt = await client(env)(
        "schedule_update",
        {
            "schedule_id": rule.id,
            "expected_revision": rule.revision,
            "goal": "Search public sources and summarize",
        },
        "replace-reminder-goal",
    )
    assert receipt.success and receipt.effect_state == EffectState.SUCCEEDED
    updated = env.scheduler.get(env.actor, rule.id)
    assert updated.goal == "Search public sources and summarize"
    assert "reminder_only" not in updated.scope


async def test_create_update_controls_and_cross_gateway_idempotent_receipts(env):
    invoke = client(env)
    created = await invoke("schedule_create", create_args(), "create")
    assert created.success and created.effect_state == EffectState.SUCCEEDED
    rule = env.scheduler.get(env.actor, created.output["id"])
    assert rule.scope["origin_source_work_id"] == env.work.id
    assert rule.scope["schedule_creation_allowed"] is False and rule.scope["derived_depth"] == 1
    assert rule.valid_until <= datetime.now(UTC) + timedelta(days=30)
    update = {"schedule_id": rule.id, "expected_revision": rule.revision, "title": "Changed label"}
    first = await invoke("schedule_update", update, "update")
    replay = await client(env)("schedule_update", update, "update")
    assert first.success and replay.success and replay.output["replayed"]
    assert env.scheduler.get(env.actor, rule.id).revision == first.output["revision"]
    conflict = await client(env)("schedule_update", {**update, "title": "Different"}, "update")
    assert not conflict.success and conflict.error_code == "idempotency_conflict"
    assert conflict.effect_state == EffectState.NO_EFFECT
    for command, target in [("pause", "paused"), ("resume", "active"), ("cancel", "cancelled")]:
        current = env.scheduler.get(env.actor, rule.id)
        outcome = await invoke(
            "schedule_control",
            {"rule_id": rule.id, "expected_revision": current.revision, "command": command},
            command,
        )
        assert outcome.success and outcome.output["state"] == target


async def test_event_creation_control_and_fixed_source_authority(env):
    invoke = client(env)
    arguments = {
        "event_type": "task_completed",
        "resource_id": env.task.id,
        "goal": "Prepare follow-up",
    }
    created = await invoke("trigger_create", arguments, "trigger")
    assert created.success
    rule = env.triggers.get(env.actor, created.output["id"])
    assert rule.creator_id == env.actor.member_id and rule.derived_depth == 1
    assert rule.scope["request_source_id"] == env.request.id and rule.max_occurrences == 1
    result = await invoke(
        "trigger_control",
        {"rule_id": rule.id, "expected_revision": rule.revision, "command": "pause"},
        "pause-event",
    )
    assert result.success and result.output["state"] == "paused"
    forged = await invoke(
        "trigger_create", {**arguments, "source_event_id": "forged", "member_id": "owner"}, "forged"
    )
    assert forged.error_code == "invalid_input" and forged.effect_state == EffectState.NOT_STARTED


async def test_time_to_live_occurrence_and_listener_limits(env):
    invoke = client(env)
    too_late = (datetime.now(UTC) + timedelta(days=31)).isoformat()
    denied = await invoke("schedule_create", create_args(valid_until=too_late), "long-time")
    assert denied.error_code == "invalid_ttl" and denied.effect_state == EffectState.NO_EFFECT
    for extra in [
        {"max_occurrences": 11},
        {"event_type": "arbitrary_listener"},
        {"expires_at": too_late},
    ]:
        result = await invoke(
            "trigger_create",
            {
                "event_type": "task_completed",
                "resource_id": env.task.id,
                "goal": "Finite follow-up",
                **extra,
            },
            str(extra),
        )
        assert not result.success and result.effect_state in {
            EffectState.NO_EFFECT,
            EffectState.NOT_STARTED,
        }
    cron = await invoke(
        "schedule_create",
        {**create_args(), "timing": {"kind": "cron", "expression": "* * * * *"}},
        "cron",
    )
    assert cron.error_code == "invalid_input"
    assert env.scheduler.list(env.actor) == [] and env.triggers.list(env.actor) == []


@pytest.mark.parametrize(
    "scope_change",
    [
        {"derived_depth": 1},
        {"schedule_creation_allowed": False},
        {"origin_trigger_rule_id": "synthetic-rule"},
    ],
)
async def test_derived_or_revoked_source_work_cannot_create_recursion(env, scope_change):
    with env.store.transaction(env.actor) as tx:
        from psycopg.types.json import Jsonb

        tx.conn.execute(
            "UPDATE ga_work_orders SET scope=scope || %s WHERE group_id=%s AND id=%s",
            (Jsonb(scope_change), env.actor.group_id, env.work.id),
        )
    receipt = await client(env)("schedule_create", create_args(), "recursive")
    assert (
        receipt.error_code == "derivation_limit" and receipt.effect_state == EffectState.NO_EFFECT
    )
    assert env.scheduler.list(env.actor) == []


async def test_stale_lease_source_removal_and_other_creator_rejected(env):
    foreign = env.scheduler.create(
        env.owner, "Owner rule", "Owner follow-up", EveryTiming(interval_seconds=3600)
    )
    result = await client(env)(
        "schedule_control",
        {"rule_id": foreign.id, "expected_revision": foreign.revision, "command": "cancel"},
        "foreign",
    )
    assert (
        result.error_code == "permission_denied"
        and env.scheduler.get(env.owner, foreign.id).state == "active"
    )
    with env.store.transaction(env.actor) as tx:
        tx.conn.execute(
            "UPDATE ga_events SET memory_state='recalled' WHERE group_id=%s AND id=%s",
            (env.actor.group_id, env.request.id),
        )
    result = await client(env)("schedule_create", create_args(), "removed")
    assert (
        result.error_code == "source_unavailable" and result.effect_state == EffectState.NO_EFFECT
    )
    with env.store.transaction(env.actor) as tx:
        current = tx.get_work(env.work.id)
        tx.transition_work(current.id, WorkState.CANCELLED, expected_revision=current.revision)
    result = await client(env)("schedule_create", create_args(), "stale")
    assert result.error_code == "stale_lease"


async def test_lists_omit_other_member_and_private_topic_rules(env):
    public = env.scheduler.create(
        env.actor, "Public own label", "Bounded", EveryTiming(interval_seconds=3600)
    )
    env.scheduler.create(
        env.owner, "Hidden other owner label", "Other", EveryTiming(interval_seconds=3600)
    )
    topic = uuid4().hex
    with env.store.transaction(env.actor) as tx:
        tx.conn.execute(
            "INSERT INTO ga_memory_topics(id,group_id,title,visibility,created_by) VALUES(%s,%s,'Private','participants',%s)",
            (topic, env.actor.group_id, env.actor.member_id),
        )
    env.scheduler.create(
        env.actor,
        "Private topic label",
        "Private",
        EveryTiming(interval_seconds=3600),
        scope={"topic_id": topic},
    )
    result = await client(env)("schedule_list", {}, "list")
    assert result.success
    assert [row["id"] for row in result.output["items"]] == [public.id]
    assert "Private topic label" not in result.model_dump_json()
    assert "Hidden other owner label" not in result.model_dump_json()


async def test_concurrent_duplicate_create_and_atomic_receipt_rollback(env, monkeypatch):
    first, second = client(env), client(env)
    results = await asyncio.gather(
        first("schedule_create", create_args(), "same"),
        second("schedule_create", create_args(), "same"),
    )
    assert all(result.success for result in results)
    assert (
        len({result.output["id"] for result in results}) == 1
        and len(env.scheduler.list(env.actor)) == 1
    )
    audit = UnitOfWork._audit

    def crash(self, action, resource_ref, **details):
        if action == "tool_scheduling_receipt":
            raise RuntimeError("Synthetic crash before transaction receipt commit")
        return audit(self, action, resource_ref, **details)

    monkeypatch.setattr(UnitOfWork, "_audit", crash)
    result = await client(env)("schedule_create", create_args(title="Rollback"), "crash")
    assert not result.success and len(env.scheduler.list(env.actor)) == 1
    monkeypatch.setattr(UnitOfWork, "_audit", audit)
    recovered = await client(env)("schedule_create", create_args(title="Rollback"), "crash")
    assert recovered.success and len(env.scheduler.list(env.actor)) == 2
    with env.store.transaction(env.actor) as tx:
        receipts = tx.conn.execute(
            "SELECT details FROM ga_audit_events WHERE group_id=%s AND action='tool_scheduling_receipt'",
            (env.actor.group_id,),
        ).fetchall()
    assert len(receipts) == 2
    assert all("goal" not in str(row) and "title" not in str(row) for row in receipts)


async def test_tools_share_time_and_event_quota(env):
    for index in range(9):
        env.scheduler.create(
            env.actor, f"Rule {index}", "Finite", EveryTiming(interval_seconds=3600)
        )
    created = await client(env)(
        "trigger_create",
        {"event_type": "task_completed", "resource_id": env.task.id, "goal": "Last quota slot"},
        "last",
    )
    assert created.success
    denied = await client(env)("schedule_create", create_args(), "over-quota")
    assert denied.error_code == "quota_exceeded" and denied.effect_state == EffectState.NO_EFFECT
