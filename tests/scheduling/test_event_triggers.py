"""Actual PostgreSQL event admission; all identities/content are synthetic."""

from __future__ import annotations

import os
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from threading import Barrier
from types import SimpleNamespace
from uuid import uuid4

import pytest

from groupagent.channels.models import AttachmentHandle
from groupagent.domain import (
    ActorContext,
    DeliveryState,
    DomainError,
    EventKind,
    GroupEvent,
    Role,
    TeamTaskState,
    WorkState,
)
from groupagent.persistence import PostgresStore, UnitOfWork
from groupagent.scheduling import EveryTiming, Scheduler
from groupagent.scheduling.triggers import EventTriggerService

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
def env(store):
    group = "triggers-" + uuid4().hex
    store.register_group(group)
    actors = {
        "owner": ActorContext(group_id=group, member_id="owner", role=Role.OWNER),
        "member": ActorContext(group_id=group, member_id="member", role=Role.MEMBER),
        "other": ActorContext(group_id=group, member_id="other", role=Role.MEMBER),
        "service": ActorContext(group_id=group, member_id="service", role=Role.MAINTAINER),
    }
    with store.transaction(actors["owner"]) as tx:
        for member in actors:
            tx.conn.execute(
                "INSERT INTO ga_members(group_id,member_id,membership_state) VALUES(%s,%s,'present')",
                (group, member),
            )
    return SimpleNamespace(
        store=store,
        group=group,
        actors=actors,
        triggers=EventTriggerService(store, actors.__getitem__),
        now=datetime.now(UTC) - timedelta(seconds=1),
    )


def event(
    env,
    actor="member",
    *,
    attachments=None,
    kind=EventKind.TEXT,
    reply_to=None,
    occurred_at=None,
    origin="verified_channel_metadata",
):
    who = env.actors[actor]
    identifier = uuid4().hex
    value = GroupEvent(
        id=identifier,
        source_key="synthetic:" + identifier,
        group_id=env.group,
        actor_id=who.member_id,
        kind=kind,
        text="Synthetic authorization or input",
        occurred_at=occurred_at or datetime.now(UTC),
        received_at=datetime.now(UTC),
        attachments=attachments or [],
        reply_to=reply_to,
        trusted_actor_origin=origin,
        platform_message_id="platform:" + identifier,
        policy_revision=who.policy_revision,
    )
    with env.store.transaction(who) as tx:
        tx.ingest_event(value, "fixture:" + identifier, "1", 0)
    return value


def task(env, actor="member"):
    source = event(env, actor)
    with env.store.transaction(env.actors[actor]) as tx:
        return tx.create_team_task("Synthetic human task", source_event_id=source.id)


def rule(env, resource, actor="member", event_type="task_completed", **kwargs):
    source = kwargs.pop("source_event_id", None) or event(env, actor).id
    return env.triggers.create(
        env.actors[actor],
        event_type,
        resource,
        "Prepare follow-up",
        source_event_id=source,
        request_key=kwargs.pop("request_key", uuid4().hex),
        now=kwargs.pop("now", env.now),
        **kwargs,
    )


def complete_task(env, identifier, *, actor="member"):
    with env.store.transaction(env.actors[actor]) as tx:
        current = tx.get_team_task(identifier)
        return tx.update_team_task(
            identifier, expected_revision=current.revision, status=TeamTaskState.COMPLETED
        )


def scan(env, **kwargs):
    return env.triggers.poll(env.actors["service"], **kwargs)


def get_work(env, identifier):
    with env.store.transaction(env.actors["owner"]) as tx:
        return tx.get_work(identifier)


def handle(env, *, filename="report.pdf", mime="application/pdf", availability="available"):
    return AttachmentHandle(
        account_id="synthetic-account",
        group_id=env.group,
        external_group_id="synthetic@chatroom",
        source_epoch="test",
        message_id=uuid4().hex,
        asset_id=uuid4().hex,
        kind="file",
        variant="original",
        filename=filename,
        content_type=mime,
        availability=availability,
    ).as_attachment()


def test_defaults_idempotency_and_bounded_inputs(env):
    target = task(env)
    source = event(env)
    first = rule(env, target.id, source_event_id=source.id, request_key="once")
    assert first.max_occurrences == 1 and first.count == 0
    assert first.expires_at == env.now + timedelta(days=30)
    again = rule(env, target.id, source_event_id=source.id, request_key="once")
    assert again.id == first.id
    for maximum in [0, 11, True]:
        with pytest.raises(DomainError, match="occurrence"):
            rule(env, target.id, max_occurrences=maximum)
    with pytest.raises(DomainError, match="Unsupported"):
        rule(env, target.id, event_type="run_arbitrary_listener")
    with pytest.raises(DomainError, match="future"):
        rule(env, target.id, expires_at=env.now)
    with pytest.raises(DomainError, match="another rule"):
        rule(env, target.id, source_event_id=source.id, request_key="once", max_occurrences=2)
    assert len(env.triggers.list(env.actors["member"])) == 1


def test_member_owner_and_maintenance_authority(env):
    target = task(env, "other")
    with pytest.raises(DomainError, match="own tasks"):
        rule(env, target.id)
    assert rule(env, target.id, actor="owner").creator_role == Role.OWNER
    with pytest.raises(DomainError, match="Business"):
        rule(env, target.id, actor="service")
    env.actors["other"] = env.actors["other"].model_copy(update={"role": Role.OBSERVER})
    with pytest.raises(DomainError, match="Business"):
        rule(env, target.id, actor="other")
    with pytest.raises(DomainError, match="trusted control"):
        env.triggers.poll(env.actors["member"])


def test_forged_completion_and_concurrent_duplicate_notify(env):
    target = task(env)
    watcher = rule(env, target.id, max_occurrences=2)
    assert (
        env.triggers.notify(
            env.actors["service"], "task_completed", target.id, event_key="business:123456789"
        )
        == []
    )
    assert scan(env) == []
    complete_task(env, target.id)
    barrier = Barrier(2)

    def concurrent():
        barrier.wait()
        return scan(env)

    with ThreadPoolExecutor(max_workers=2) as pool:
        values = list(pool.map(lambda _: concurrent(), range(2)))
    all_values = [item for value in values for item in value]
    assert len(all_values) == 1
    occurrence = all_values[0]
    assert occurrence.state == "enqueued"
    work = get_work(env, occurrence.work_id)
    assert work.state == WorkState.QUEUED
    assert work.requester_id == "member"
    assert work.scope["derived_depth"] == 1
    assert work.scope["schedule_creation_allowed"] is False
    assert len(work.scope["source_fingerprints"]) == 3
    assert (
        env.triggers.notify(
            env.actors["service"], "task_completed", target.id, event_key=occurrence.event_key
        )
        == []
    )
    assert env.triggers.get(env.actors["member"], watcher.id).count == 1
    with env.store.transaction(env.actors["member"]) as tx:
        completed = tx.get_team_task(target.id)
        tx.update_team_task(
            target.id,
            expected_revision=completed.revision,
            status=TeamTaskState.COMPLETED,
            progress="Still complete",
        )
        assert (
            tx.conn.execute(
                "SELECT count(*) AS n FROM ga_trigger_events WHERE group_id=%s", (env.group,)
            ).fetchone()["n"]
            == 1
        )
        assert (
            tx.conn.execute(
                "SELECT count(*) AS n FROM ga_deliveries WHERE group_id=%s", (env.group,)
            ).fetchone()["n"]
            == 0
        )


def test_rolled_back_business_change_produces_no_fact(env):
    target = task(env)
    rule(env, target.id)
    with pytest.raises(RuntimeError):
        with env.store.transaction(env.actors["member"]) as tx:
            tx.update_team_task(
                target.id, expected_revision=target.revision, status=TeamTaskState.COMPLETED
            )
            raise RuntimeError("synthetic crash before commit")
    assert scan(env) == []


def test_atomic_admission_fault_rolls_back_occurrence_source_work(env, monkeypatch):
    target = task(env)
    watcher = rule(env, target.id)
    complete_task(env, target.id)
    original = UnitOfWork.record_decision

    def crash(self, *args, **kwargs):
        raise RuntimeError("synthetic crash after create_work")

    monkeypatch.setattr(UnitOfWork, "record_decision", crash)
    with pytest.raises(RuntimeError, match="synthetic crash"):
        scan(env)
    with env.store.transaction(env.actors["member"]) as tx:
        assert not tx.list_work()
        assert (
            tx.conn.execute(
                "SELECT count(*) AS n FROM ga_events WHERE group_id=%s AND kind='system'",
                (env.group,),
            ).fetchone()["n"]
            == 0
        )
    assert env.triggers.occurrences(env.actors["member"], watcher.id) == []
    monkeypatch.setattr(UnitOfWork, "record_decision", original)
    assert len(scan(env)) == 1


def test_work_failed_fact_and_stale_retry_event(env):
    source = event(env)
    with env.store.transaction(env.actors["member"]) as tx:
        work = tx.create_work("Synthetic workload", source.id)
    watcher = rule(env, work.id, event_type="work_failed", max_occurrences=2)
    with env.store.transaction(env.actors["member"]) as tx:
        failed = tx.transition_work(work.id, WorkState.FAILED, expected_revision=work.revision)
        retry = tx.transition_work(
            work.id, WorkState.QUEUED, expected_revision=failed.revision, reason="retry"
        )
    assert scan(env)[0].reason == "resource_changed"
    with env.store.transaction(env.actors["member"]) as tx:
        tx.transition_work(work.id, WorkState.FAILED, expected_revision=retry.revision)
    assert scan(env)[0].state == "enqueued"
    assert env.triggers.get(env.actors["member"], watcher.id).count == 1


def test_work_completed_requires_real_validation_and_accepted_delivery(env):
    source = event(env)
    with env.store.transaction(env.actors["member"]) as tx:
        work = tx.create_work("Synthetic workload", source.id)
    rule(env, work.id, event_type="work_completed")
    with env.store.transaction(env.actors["member"]) as tx:
        lease = tx.claim_work(work.id, "synthetic-worker")
        current = tx.get_work(work.id)
        current = tx.transition_work(
            work.id, WorkState.VALIDATING, expected_revision=current.revision, lease=lease
        )
        tx.record_validation(work.id, lease, "fixture:validated")
        current = tx.transition_work(
            work.id, WorkState.DELIVERING, expected_revision=current.revision, lease=lease
        )
        intent = tx.enqueue_delivery(
            work.id, "text:Synthetic", "1" * 64, "complete-one", lease=lease
        )
    assert scan(env) == []
    with pytest.raises(DomainError, match="not accepted"):
        with env.store.transaction(env.actors["member"]) as tx:
            tx.transition_work(
                work.id, WorkState.COMPLETED, expected_revision=current.revision, lease=lease
            )
    assert scan(env) == []
    with env.store.transaction(env.actors["member"]) as tx:
        claimed = tx.claim_delivery("synthetic-sender", intent_id=intent.id)
        tx.finish_delivery(
            intent.id,
            claimed.claim_token,
            DeliveryState.ACCEPTED,
            platform_message_id="synthetic-accepted",
            evidence_ref="fixture:receipt",
        )
        tx.transition_work(
            work.id, WorkState.COMPLETED, expected_revision=current.revision, lease=lease
        )
    assert scan(env)[0].state == "enqueued"


def test_overlap_is_explicit_skip_and_count_only_enqueued(env):
    target = task(env)
    watcher = rule(env, target.id, max_occurrences=2)
    complete_task(env, target.id)
    first = scan(env)[0]
    with env.store.transaction(env.actors["member"]) as tx:
        completed = tx.get_team_task(target.id)
        tx.update_team_task(
            target.id, expected_revision=completed.revision, status=TeamTaskState.OPEN
        )
    complete_task(env, target.id)
    second = scan(env)[0]
    assert second.reason == "overlap_busy" and second.work_id is None
    assert env.triggers.get(env.actors["member"], watcher.id).count == 1
    assert get_work(env, first.work_id).state == WorkState.QUEUED
    assert scan(env) == []


def test_pause_resume_cancel_children_and_no_paused_backfill(env):
    watcher = rule(env, "member:member", event_type="attachment_received", max_occurrences=2)
    event(env, kind=EventKind.FILE, attachments=[handle(env)])
    child = scan(env)[0]
    current = env.triggers.get(env.actors["member"], watcher.id)
    paused = env.triggers.pause(
        env.actors["member"], watcher.id, expected_revision=current.revision
    )
    assert get_work(env, child.work_id).pause_reason == "trigger_paused"
    event(env, kind=EventKind.FILE, attachments=[handle(env)])
    assert scan(env) == []
    resumed = env.triggers.resume(
        env.actors["member"], watcher.id, expected_revision=paused.revision
    )
    assert get_work(env, child.work_id).state == WorkState.QUEUED
    assert scan(env) == []
    cancelled = env.triggers.cancel(
        env.actors["member"], watcher.id, expected_revision=resumed.revision
    )
    assert cancelled.state == "cancelled"
    assert get_work(env, child.work_id).state == WorkState.CANCELLED
    with pytest.raises(DomainError, match="paused"):
        env.triggers.resume(env.actors["member"], watcher.id, expected_revision=cancelled.revision)


def test_last_occurrence_can_be_paused_resumed_even_ended(env):
    target = task(env)
    watcher = rule(env, target.id)
    complete_task(env, target.id)
    child = scan(env)[0]
    ended = env.triggers.get(env.actors["member"], watcher.id)
    assert ended.state == "ended"
    paused = env.triggers.pause(env.actors["member"], watcher.id, expected_revision=ended.revision)
    assert scan(env) == []
    assert env.triggers.get(env.actors["member"], watcher.id).state == "paused"
    resumed = env.triggers.resume(
        env.actors["member"], watcher.id, expected_revision=paused.revision
    )
    assert resumed.state == "ended"
    assert get_work(env, child.work_id).state == WorkState.QUEUED


def test_policy_change_requires_creator_reauthorization(env):
    target = task(env)
    watcher = rule(env, target.id)
    complete_task(env, target.id)
    env.store.register_group(env.group, policy_revision=2)
    for key, actor in env.actors.items():
        env.actors[key] = actor.model_copy(update={"policy_revision": 2})
    assert scan(env) == []
    paused = env.triggers.get(env.actors["member"], watcher.id)
    assert paused.state == "paused"
    with pytest.raises(DomainError, match="Only the creator"):
        env.triggers.resume(env.actors["owner"], watcher.id, expected_revision=paused.revision)
    resumed = env.triggers.resume(
        env.actors["member"], watcher.id, expected_revision=paused.revision
    )
    assert resumed.policy_revision == 2 and resumed.creator_role == Role.MEMBER
    assert scan(env) == []  # resume never backfills an earlier-policy event


def test_current_creator_role_and_membership_are_rechecked(env):
    target = task(env, "other")
    watcher = rule(env, target.id, actor="owner")
    env.actors["owner"] = env.actors["owner"].model_copy(update={"role": Role.MEMBER})
    complete_task(env, target.id, actor="other")
    assert scan(env) == []
    assert env.triggers.get(env.actors["member"], watcher.id).state == "paused"
    other = rule(env, "member:member", event_type="attachment_received")
    with env.store.transaction(env.actors["service"]) as tx:
        tx.conn.execute(
            "UPDATE ga_members SET membership_state='left' WHERE group_id=%s AND member_id='member'",
            (env.group,),
        )
    assert scan(env) == []
    assert env.triggers.get(env.actors["service"], other.id).state == "paused"


def test_source_privacy_change_prevents_admission(env):
    target = task(env)
    source = event(env)
    watcher = rule(env, target.id, source_event_id=source.id)
    complete_task(env, target.id)
    with env.store.transaction(env.actors["member"]) as tx:
        tx.conn.execute(
            "UPDATE ga_events SET memory_state='forgotten' WHERE group_id=%s AND id=%s",
            (env.group, source.id),
        )
    assert scan(env) == []
    assert env.triggers.get(env.actors["member"], watcher.id).state == "paused"


def test_task_reassignment_revokes_member_followup(env):
    target = task(env)
    watcher = rule(env, target.id)
    with env.store.transaction(env.actors["owner"]) as tx:
        tx.update_team_task(target.id, expected_revision=target.revision, owner_id="other")
    complete_task(env, target.id, actor="other")
    assert scan(env) == []
    assert env.triggers.get(env.actors["member"], watcher.id).state == "paused"


def test_attachment_filters_require_backend_work_link_and_reject_video(env):
    with pytest.raises(DomainError, match="monitor other"):
        rule(env, "member:other", event_type="attachment_received")
    with pytest.raises(DomainError, match="Only Owner"):
        rule(env, "group:" + env.group, event_type="attachment_received")
    source = event(env)
    with env.store.transaction(env.actors["member"]) as tx:
        work = tx.create_work("Synthetic document work", source.id)
    watcher = rule(env, "work:" + work.id, event_type="attachment_received", max_occurrences=2)
    event(env, "other", kind=EventKind.FILE, attachments=[handle(env)])
    event(
        env,
        "other",
        kind=EventKind.FILE,
        reply_to=source.platform_message_id,
        attachments=[handle(env, filename="clip.mp4", mime="video/mp4")],
    )
    event(env, "other", kind=EventKind.UNSUPPORTED_VIDEO, attachments=[handle(env)])
    valid = event(
        env,
        "other",
        kind=EventKind.FILE,
        reply_to=source.platform_message_id,
        attachments=[handle(env)],
    )
    results = scan(env)
    assert [item.state for item in results] == ["skipped", "skipped", "enqueued"]
    assert results[-1].event_key == "attachment:" + valid.id + ":0"
    assert get_work(env, results[-1].work_id).requester_id == "member"
    assert env.triggers.get(env.actors["member"], watcher.id).count == 1


def test_attachment_never_trusts_fake_event_or_foreign_handle(env):
    rule(env, "member:member", event_type="attachment_received")
    foreign = handle(env)
    foreign["handle"]["group_id"] = "another-group"
    event(env, kind=EventKind.FILE, attachments=[foreign])
    event(env, kind=EventKind.FILE, attachments=[handle(env)], origin="model_claim")
    assert all(item.state == "skipped" for item in scan(env))
    assert (
        env.triggers.notify(
            env.actors["service"],
            "attachment_received",
            "member:member",
            event_key="attachment:made-up:0",
        )
        == []
    )
    with env.store.transaction(env.actors["member"]) as tx:
        assert not tx.list_work()


def test_video_variants_never_admit_even_when_file_kind_is_claimed(env):
    rule(env, "member:member", event_type="attachment_received", max_occurrences=10)
    for filename, mime in [
        ("CLIP.MOV ", "application/octet-stream"),
        ("clip.m2ts", "application/octet-stream"),
        ("innocent.pdf", " VIDEO/MP4 "),
        ("clip.ogv", "application/ogg"),
    ]:
        event(env, kind=EventKind.FILE, attachments=[handle(env, filename=filename, mime=mime)])
    observed = scan(env)
    assert len(observed) == 4
    assert all(item.reason == "unsupported_or_unavailable_attachment" for item in observed)
    with env.store.transaction(env.actors["member"]) as tx:
        assert not tx.list_work()


@pytest.mark.parametrize(
    "availability,accepted",
    [
        ("unresolved", True),
        ("pending", True),
        ("expired", False),
        ("unsupported", False),
        ("failed", False),
        ("recalled", False),
    ],
)
def test_attachment_arrival_is_distinct_from_download_availability(env, availability, accepted):
    rule(env, "member:member", event_type="attachment_received")
    item = handle(env)
    item["handle"]["availability"] = availability
    received = event(env, kind=EventKind.FILE, attachments=[item])
    occurrence = scan(env)[0]
    assert (occurrence.state == "enqueued") is accepted
    if accepted:
        assert get_work(env, occurrence.work_id).scope["attachment_event_ids"] == [received.id]


def test_derived_work_cannot_create_or_trigger_recursion(env):
    target = task(env)
    watcher = rule(env, target.id)
    complete_task(env, target.id)
    occurrence = scan(env)[0]
    child = get_work(env, occurrence.work_id)
    with pytest.raises(DomainError, match="human request"):
        rule(env, target.id, source_event_id=child.request_event_id)
    with pytest.raises(DomainError, match="Generated work"):
        rule(env, child.id, event_type="work_completed")
    source = event(env)
    with env.store.transaction(env.actors["member"]) as tx:
        authorized = tx.create_work(
            "Create one followup rule", source.id, scope={"schedule_creation_allowed": True}
        )
    allowed = rule(env, target.id, source_event_id=source.id, source_work_id=authorized.id)
    assert allowed.derived_depth == 1 and watcher.derived_depth == 0


def test_combined_scheduler_quota_and_concurrent_admission(env):
    scheduler = Scheduler(env.store)
    for index in range(9):
        scheduler.create(
            env.actors["member"],
            f"Synthetic {index}",
            "Follow up",
            EveryTiming(interval_seconds=3600),
        )
    target = task(env)
    sources = [event(env), event(env)]
    barrier = Barrier(2)

    def create(index):
        barrier.wait()
        try:
            rule(env, target.id, source_event_id=sources[index].id)
            return "created"
        except DomainError as error:
            return error.code

    with ThreadPoolExecutor(max_workers=2) as pool:
        assert sorted(pool.map(create, range(2))) == ["created", "quota_exceeded"]
    with pytest.raises(DomainError, match="limit|quota"):
        scheduler.create(
            env.actors["member"], "Another", "Followup", EveryTiming(interval_seconds=3600)
        )


def test_expiry_and_before_creation_events_do_not_admit(env):
    target = task(env)
    complete_task(env, target.id)
    watcher = rule(env, target.id, now=datetime.now(UTC))
    assert scan(env) == []
    assert scan(env, now=watcher.expires_at + timedelta(seconds=1)) == []
    assert env.triggers.get(env.actors["member"], watcher.id).state == "ended"


def test_bound_transaction_rolls_back_creation_and_rejects_authority_swap(env):
    target = task(env)
    source = event(env)
    with pytest.raises(RuntimeError):
        with env.store.transaction(env.actors["member"]) as tx:
            env.triggers.in_tx(tx).create(
                env.actors["member"],
                "task_completed",
                target.id,
                "Followup",
                source_event_id=source.id,
                request_key="atomic",
            )
            raise RuntimeError("synthetic outer transaction crash")
    assert env.triggers.list(env.actors["member"]) == []
    with pytest.raises(DomainError, match="authority"):
        with env.store.transaction(env.actors["member"]) as tx:
            env.triggers.in_tx(tx).create(
                env.actors["owner"],
                "task_completed",
                target.id,
                "Followup",
                source_event_id=source.id,
                request_key="forbidden",
            )


def test_forgetting_actual_completion_source_revokes_already_enqueued_followup(env):
    from groupagent.memory import MemoryService

    original = task(env)
    rule(env, original.id)
    completion = event(env)
    with env.store.transaction(env.actors["member"]) as tx:
        done = tx.update_team_task(
            original.id,
            expected_revision=original.revision,
            status=TeamTaskState.COMPLETED,
            source_event_id=completion.id,
        )
    occurrence = scan(env)[0]
    derived = get_work(env, occurrence.work_id)
    assert completion.id in derived.scope["source_fingerprints"]
    assert derived.scope["source_task_revisions"] == {original.id: done.revision}
    MemoryService(env.store).forget(env.actors["member"], event_ids=[completion.id])
    with env.store.transaction(env.actors["member"]) as tx:
        assert tx.get_team_task(original.id).status == TeamTaskState.OPEN
        assert tx.get_work(derived.id).state == WorkState.CANCELLED


def test_independent_task_deletion_revokes_trigger_work_without_fabricated_message_sources(env):
    from groupagent.memory import MemoryService

    with env.store.transaction(env.actors["member"]) as tx:
        original = tx.create_team_task("Synthetic independent business task")
    rule(env, original.id)
    complete_task(env, original.id)
    occurrence = scan(env)[0]
    derived = get_work(env, occurrence.work_id)
    assert original.id in derived.scope["source_task_revisions"]
    MemoryService(env.store).forget(env.actors["member"], task_ids=[original.id])
    assert get_work(env, derived.id).state == WorkState.CANCELLED
