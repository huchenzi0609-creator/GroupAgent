"""Scheduling integration tests use real PostgreSQL and synthetic unique groups."""

from __future__ import annotations

import os
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, time, timedelta
from threading import Barrier
from uuid import uuid4

import pytest

from groupagent.domain import ActorContext, DeliveryState, DomainError, Role, WorkState
from groupagent.persistence import PostgresStore, UnitOfWork
from groupagent.scheduling import (
    AtTiming,
    DailyTiming,
    EveryTiming,
    OccurrenceState,
    Scheduler,
    ScheduleState,
    WeeklyTiming,
)

pytestmark = pytest.mark.postgres
START = datetime(2030, 1, 1, tzinfo=UTC)


@pytest.fixture(scope="module")
def store() -> PostgresStore:
    dsn = os.environ.get("GROUPAGENT_TEST_DSN")
    if not dsn:
        pytest.skip("Set GROUPAGENT_TEST_DSN for real PostgreSQL scheduling tests")
    result = PostgresStore(dsn)
    result.migrate()
    return result


@pytest.fixture
def actor(store: PostgresStore) -> ActorContext:
    group = "schedule-test-" + uuid4().hex
    store.register_group(group)
    return ActorContext(group_id=group, member_id="member-a", role=Role.MEMBER)


@pytest.fixture
def scheduler(store: PostgresStore) -> Scheduler:
    return Scheduler(store)


def hourly(scheduler: Scheduler, actor: ActorContext, **kwargs):
    return scheduler.create(
        actor,
        "Hourly synthetic report",
        "Prepare a synthetic report",
        EveryTiming(interval_seconds=3600),
        now=START,
        **kwargs,
    )


def test_all_supported_rules_and_defaults(scheduler: Scheduler, actor: ActorContext) -> None:
    schedules = [
        scheduler.create(
            actor,
            "Once",
            "A synthetic reminder",
            AtTiming(at=START + timedelta(hours=1)),
            now=START,
        ),
        hourly(scheduler, actor),
        scheduler.create(
            actor, "Daily", "Daily report", DailyTiming(local_time=time(20)), now=START
        ),
        scheduler.create(
            actor,
            "Weekly",
            "Weekly report",
            WeeklyTiming(local_time=time(20), weekdays=[4]),
            now=START,
        ),
    ]
    assert [schedule.next_due_at for schedule in schedules] == [
        START + timedelta(hours=1),
        START + timedelta(hours=1),
        START + timedelta(hours=12),
        datetime(2030, 1, 4, 12, tzinfo=UTC),
    ]
    assert all(schedule.time_zone == "Asia/Shanghai" for schedule in schedules)
    assert schedules[0].valid_until == START + timedelta(hours=25)
    assert schedules[1].valid_until == START + timedelta(days=30)
    assert schedules[1].overlap_policy == "coalesce"
    assert schedules[1].misfire_policy == "latest_only"


def test_latest_only_catchup_creates_one_work_atomically(
    store: PostgresStore, scheduler: Scheduler, actor: ActorContext
) -> None:
    schedule = hourly(scheduler, actor)
    changed = scheduler.due(actor, START + timedelta(hours=10, minutes=42))
    assert len(changed) == 1
    occurrence = changed[0]
    assert occurrence.state == OccurrenceState.ENQUEUED
    assert occurrence.scheduled_at == START + timedelta(hours=10)
    assert occurrence.coalesced_from == START + timedelta(hours=1)
    assert scheduler.get(actor, schedule.id).next_due_at == START + timedelta(hours=11)
    with store.transaction(actor) as tx:
        assert occurrence.work_id is not None
        work = tx.get_work(occurrence.work_id)
        source = tx.get_event(work.request_event_id)
        assert work.requester_id == actor.member_id and source.actor_id == actor.member_id
        assert source.occurred_at == occurrence.scheduled_at
        assert tx.get_decision(source.id) is not None
        assert tx.list_pending_events() == []


def test_restart_concurrent_scanners_and_clock_rollback_do_not_duplicate(
    store: PostgresStore, scheduler: Scheduler, actor: ActorContext
) -> None:
    schedule = hourly(scheduler, actor)
    barrier = Barrier(2)

    def wake():
        barrier.wait()
        return Scheduler(store).due(actor, START + timedelta(hours=1))

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(lambda _: wake(), range(2)))
    assert sum(len(result) for result in results) == 1
    assert Scheduler(store).due(actor, START + timedelta(minutes=30)) == []
    assert Scheduler(store).due(actor, START + timedelta(hours=1)) == []
    assert len(scheduler.occurrences(actor, schedule.id)) == 1
    with store.transaction(actor) as tx:
        assert len(tx.list_work()) == 1


def test_crash_rolls_back_occurrence_work_event_and_next_due(
    store: PostgresStore, scheduler: Scheduler, actor: ActorContext, monkeypatch: pytest.MonkeyPatch
) -> None:
    schedule = hourly(scheduler, actor)
    original = UnitOfWork.create_work

    def crash_after_work(self, *args, **kwargs):
        original(self, *args, **kwargs)
        raise RuntimeError("crash after work insert")

    with monkeypatch.context() as patch:
        patch.setattr(UnitOfWork, "create_work", crash_after_work)
        with pytest.raises(RuntimeError, match="crash after work"):
            scheduler.due(actor, START + timedelta(hours=1))
    assert scheduler.get(actor, schedule.id).next_due_at == START + timedelta(hours=1)
    assert scheduler.occurrences(actor, schedule.id) == []
    with store.transaction(actor) as tx:
        assert tx.list_work() == []
        assert tx.list_pending_events() == []
        assert tx.get_cursor("schedule:" + schedule.id) is None
    assert len(Scheduler(store).due(actor, START + timedelta(hours=1))) == 1


def test_overlap_coalesces_to_one_pending_then_admits_latest(
    store: PostgresStore, scheduler: Scheduler, actor: ActorContext
) -> None:
    schedule = hourly(scheduler, actor)
    first = scheduler.due(actor, START + timedelta(hours=1))[0]
    second = scheduler.due(actor, START + timedelta(hours=2))[0]
    assert second.state == OccurrenceState.PENDING and second.work_id is None
    newer = scheduler.due(actor, START + timedelta(hours=4))
    assert {item.state for item in newer} == {OccurrenceState.SKIPPED, OccurrenceState.PENDING}
    pending = [
        item
        for item in scheduler.occurrences(actor, schedule.id)
        if item.state == OccurrenceState.PENDING
    ]
    assert len(pending) == 1 and pending[0].scheduled_at == START + timedelta(hours=4)
    with store.transaction(actor) as tx:
        assert first.work_id is not None
        work = tx.get_work(first.work_id)
        tx.transition_work(
            work.id, WorkState.FAILED, expected_revision=work.revision, reason="synthetic_failure"
        )
    admitted = scheduler.due(actor, START + timedelta(hours=4, minutes=10))
    assert len(admitted) == 1 and admitted[0].id == pending[0].id
    assert admitted[0].work_id and admitted[0].state == OccurrenceState.ENQUEUED
    with store.transaction(actor) as tx:
        assert len(tx.list_work()) == 2


@pytest.mark.parametrize(
    "late_hours, expected", [(24, OccurrenceState.ENQUEUED), (25, OccurrenceState.SKIPPED)]
)
def test_one_shot_catchup_has_exact_bounded_grace(
    scheduler: Scheduler, actor: ActorContext, late_hours: int, expected: OccurrenceState
) -> None:
    at = START + timedelta(hours=1)
    schedule = scheduler.create(actor, "Once", "A reminder", AtTiming(at=at), now=START)
    changed = scheduler.due(actor, at + timedelta(hours=late_hours))
    assert len(changed) == 1 and changed[0].state == expected
    assert scheduler.get(actor, schedule.id).state == ScheduleState.ENDED
    assert scheduler.due(actor, at + timedelta(hours=late_hours)) == []


def test_expired_periodic_scope_cannot_admit_late_work(
    store: PostgresStore, scheduler: Scheduler, actor: ActorContext
) -> None:
    schedule = hourly(scheduler, actor, valid_until=START + timedelta(hours=3))
    changes = scheduler.due(actor, START + timedelta(days=3650))
    assert len(changes) == 1 and changes[0].state == OccurrenceState.SKIPPED
    assert changes[0].scheduled_at == START + timedelta(hours=3)
    assert scheduler.get(actor, schedule.id).state == ScheduleState.ENDED
    with store.transaction(actor) as tx:
        assert tx.list_work() == []


def test_pause_resume_and_cancel_revoke_work_leases(
    store: PostgresStore, scheduler: Scheduler, actor: ActorContext
) -> None:
    schedule = hourly(scheduler, actor)
    occurrence = scheduler.due(actor, START + timedelta(hours=1))[0]
    assert occurrence.work_id is not None
    with store.transaction(actor) as tx:
        lease = tx.claim_work(occurrence.work_id, "synthetic-worker")
    current = scheduler.get(actor, schedule.id)
    paused = scheduler.pause(actor, schedule.id, expected_revision=current.revision)
    assert scheduler.due(actor, START + timedelta(hours=20)) == []
    with pytest.raises(DomainError) as caught:
        with store.transaction(actor) as tx:
            tx.assert_lease(lease)
    assert caught.value.code == "stale_lease"
    resumed = scheduler.resume(
        actor, schedule.id, expected_revision=paused.revision, now=START + timedelta(hours=20)
    )
    assert resumed.next_due_at == START + timedelta(hours=21)
    with store.transaction(actor) as tx:
        assert tx.get_work(occurrence.work_id).state == WorkState.QUEUED
    cancelled = scheduler.cancel(actor, schedule.id, expected_revision=resumed.revision)
    assert cancelled.state == ScheduleState.CANCELLED
    with store.transaction(actor) as tx:
        assert tx.get_work(occurrence.work_id).state == WorkState.CANCELLED
    assert scheduler.due(actor, START + timedelta(hours=21)) == []


def test_revision_update_preserves_occurrence_history(
    scheduler: Scheduler, actor: ActorContext
) -> None:
    schedule = hourly(scheduler, actor)
    original = scheduler.due(actor, START + timedelta(hours=1))[0]
    current = scheduler.get(actor, schedule.id)
    updated = scheduler.update(
        actor,
        schedule.id,
        expected_revision=current.revision,
        timing=DailyTiming(local_time=time(20)),
        time_zone="Asia/Tokyo",
        now=START + timedelta(hours=1, minutes=30),
    )
    assert updated.next_due_at == START + timedelta(hours=11)
    history = scheduler.occurrences(actor, schedule.id)
    assert history[0].id == original.id and history[0].scheduled_at == START + timedelta(hours=1)
    with pytest.raises(DomainError) as caught:
        scheduler.pause(actor, schedule.id, expected_revision=current.revision)
    assert caught.value.code == "version_conflict"


def test_request_replay_and_concurrent_quota_are_serialized(
    store: PostgresStore, actor: ActorContext
) -> None:
    scheduler = Scheduler(store, max_active_per_group=1)
    first = hourly(scheduler, actor, request_key="source:one")
    assert hourly(scheduler, actor, request_key="source:one").id == first.id
    scheduler.pause(actor, first.id, expected_revision=first.revision)
    barrier = Barrier(2)

    def create(key: str):
        barrier.wait()
        try:
            return hourly(scheduler, actor, request_key=key).id
        except DomainError as error:
            return error.code

    with ThreadPoolExecutor(max_workers=2) as executor:
        outcomes = list(executor.map(create, ["source:two", "source:three"]))
    assert outcomes.count("quota_exceeded") == 1
    assert len(scheduler.list(actor, state=ScheduleState.ACTIVE)) == 1


def test_owner_scanner_preserves_creator_and_group_permissions(
    store: PostgresStore, scheduler: Scheduler, actor: ActorContext
) -> None:
    schedule = hourly(scheduler, actor)
    owner = actor.model_copy(update={"member_id": "group-owner", "role": Role.OWNER})
    occurrence = scheduler.due(owner, START + timedelta(hours=1))[0]
    with store.transaction(owner) as tx:
        assert occurrence.work_id is not None
        assert tx.get_work(occurrence.work_id).requester_id == actor.member_id
    other = actor.model_copy(update={"member_id": "member-b"})
    with pytest.raises(DomainError) as caught:
        scheduler.cancel(
            other, schedule.id, expected_revision=scheduler.get(actor, schedule.id).revision
        )
    assert caught.value.code == "permission_denied"
    outsider = actor.model_copy(update={"group_id": "outside-" + uuid4().hex})
    store.register_group(outsider.group_id)
    with pytest.raises(DomainError) as caught:
        scheduler.get(outsider, schedule.id)
    assert caught.value.code == "not_found"
    for role in (Role.OBSERVER, Role.MAINTAINER):
        with pytest.raises(DomainError) as caught:
            hourly(scheduler, actor.model_copy(update={"role": role}))
        assert caught.value.code == "permission_denied"
    with pytest.raises(DomainError) as caught:
        hourly(scheduler, actor, scope={"allowed_group_ids": ["another-group"]})
    assert caught.value.code == "scope_violation"


def test_policy_change_pauses_until_creator_reauthorizes(
    store: PostgresStore, scheduler: Scheduler, actor: ActorContext
) -> None:
    schedule = hourly(scheduler, actor)
    store.register_group(actor.group_id, 2)
    current_actor = actor.model_copy(update={"policy_revision": 2})
    assert scheduler.due(current_actor, START + timedelta(hours=1)) == []
    paused = scheduler.get(current_actor, schedule.id)
    assert paused.state == ScheduleState.PAUSED and paused.pause_reason == "policy_changed"
    with pytest.raises(DomainError) as caught:
        scheduler.resume(
            current_actor,
            schedule.id,
            expected_revision=paused.revision,
            now=START + timedelta(hours=1),
        )
    assert caught.value.code == "policy_changed"
    reauthorized = scheduler.update(
        current_actor,
        schedule.id,
        expected_revision=paused.revision,
        goal="Current authorized report",
        now=START + timedelta(hours=1),
    )
    scheduler.resume(
        current_actor,
        schedule.id,
        expected_revision=reauthorized.revision,
        now=START + timedelta(hours=1),
    )
    occurrence = scheduler.due(current_actor, START + timedelta(hours=2))[0]
    with store.transaction(current_actor) as tx:
        assert occurrence.work_id is not None
        assert tx.get_work(occurrence.work_id).policy_revision == 2


def test_uncertain_prior_delivery_keeps_new_occurrence_pending(
    store: PostgresStore, scheduler: Scheduler, actor: ActorContext
) -> None:
    schedule = hourly(scheduler, actor)
    occurrence = scheduler.due(actor, START + timedelta(hours=1))[0]
    assert occurrence.work_id is not None
    with store.transaction(actor) as tx:
        lease = tx.claim_work(occurrence.work_id, "worker")
        work = tx.get_work(occurrence.work_id)
        work = tx.transition_work(
            work.id, WorkState.VALIDATING, expected_revision=work.revision, lease=lease
        )
        tx.record_validation(work.id, lease, "synthetic:validation")
        tx.transition_work(
            work.id, WorkState.DELIVERING, expected_revision=work.revision, lease=lease
        )
        intent = tx.enqueue_delivery(
            work.id, "artifact:synthetic", "a" * 64, "output:v1", lease=lease
        )
        claimed = tx.claim_delivery("synthetic-dispatcher")
        assert claimed is not None
        tx.finish_delivery(
            intent.id, claimed.claim_token, DeliveryState.UNKNOWN, reason="synthetic-timeout"
        )
    changed = scheduler.due(actor, START + timedelta(hours=2))
    assert len(changed) == 1 and changed[0].state == OccurrenceState.PENDING
    occurrences = scheduler.occurrences(actor, schedule.id)
    assert {item.state for item in occurrences} == {
        OccurrenceState.UNCERTAIN,
        OccurrenceState.PENDING,
    }
    with store.transaction(actor) as tx:
        assert len(tx.list_work()) == 1
    current = scheduler.get(actor, schedule.id)
    scheduler.cancel(actor, schedule.id, expected_revision=current.revision)
    with store.transaction(actor) as tx:
        tx.reconcile_delivery(
            intent.id,
            DeliveryState.ACCEPTED,
            evidence_ref="synthetic:late-query",
            platform_message_id="synthetic:late-receipt",
        )
    assert scheduler.due(actor, START + timedelta(hours=2, minutes=5)) == []
    assert {item.state for item in scheduler.occurrences(actor, schedule.id)} == {
        OccurrenceState.FAILED,
        OccurrenceState.SKIPPED,
    }


def test_invalid_timezone_interval_and_naive_clock_are_rejected(
    scheduler: Scheduler, actor: ActorContext
) -> None:
    with pytest.raises(DomainError) as caught:
        hourly(scheduler, actor, time_zone="Planet/Mars")
    assert caught.value.code == "invalid_timezone"
    with pytest.raises(DomainError) as caught:
        scheduler.create(actor, "Too often", "Goal", EveryTiming(interval_seconds=30), now=START)
    assert caught.value.code == "quota_exceeded"
    with pytest.raises(DomainError) as caught:
        scheduler.due(actor, datetime(2030, 1, 1))
    assert caught.value.code == "invalid_input"


def test_one_shot_creation_replay_still_resolves_after_due_time(
    scheduler: Scheduler, actor: ActorContext
) -> None:
    at = START + timedelta(hours=1)
    original = scheduler.create(
        actor, "Once", "Reminder", AtTiming(at=at), now=START, request_key="request:one"
    )
    scheduler.due(actor, at)
    replay = scheduler.create(
        actor,
        "Once",
        "Reminder",
        AtTiming(at=at),
        now=at + timedelta(days=3),
        request_key="request:one",
    )
    assert replay.id == original.id and replay.state == ScheduleState.ENDED


def test_paused_rule_expires_without_creating_work(
    store: PostgresStore, scheduler: Scheduler, actor: ActorContext
) -> None:
    schedule = hourly(scheduler, actor, valid_until=START + timedelta(hours=3))
    scheduler.pause(actor, schedule.id, expected_revision=schedule.revision)
    changes = scheduler.due(actor, START + timedelta(days=1))
    assert len(changes) == 1 and changes[0].state == OccurrenceState.SKIPPED
    assert scheduler.get(actor, schedule.id).state == ScheduleState.ENDED
    with store.transaction(actor) as tx:
        assert tx.list_work() == []


def test_goal_update_does_not_strand_last_coalesced_occurrence(
    store: PostgresStore, scheduler: Scheduler, actor: ActorContext
) -> None:
    schedule = hourly(scheduler, actor, valid_until=START + timedelta(hours=2, minutes=30))
    first = scheduler.due(actor, START + timedelta(hours=1))[0]
    pending = scheduler.due(actor, START + timedelta(hours=2))[0]
    current = scheduler.get(actor, schedule.id)
    assert current.next_due_at is None and pending.state == OccurrenceState.PENDING
    updated = scheduler.update(
        actor,
        schedule.id,
        expected_revision=current.revision,
        goal="Updated future report",
        now=START + timedelta(hours=2, minutes=5),
    )
    assert updated.state == ScheduleState.ACTIVE
    with store.transaction(actor) as tx:
        assert first.work_id is not None
        work = tx.get_work(first.work_id)
        tx.transition_work(work.id, WorkState.FAILED, expected_revision=work.revision)
    admitted = scheduler.due(actor, START + timedelta(hours=2, minutes=10))
    assert len(admitted) == 1 and admitted[0].id == pending.id and admitted[0].work_id
    with store.transaction(actor) as tx:
        assert tx.get_work(admitted[0].work_id).goal == "Updated future report"
