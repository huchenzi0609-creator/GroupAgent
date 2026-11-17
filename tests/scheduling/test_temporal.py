"""Typed temporal proposals exercised with a fixed clock and an isolated store."""

from __future__ import annotations

import os
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, time, timedelta
from threading import Barrier
from uuid import uuid4

import pytest

from groupagent.conversation.policy import delivery_admission_in_tx
from groupagent.domain import ActorContext, DeliveryState, DomainError, EventKind, GroupEvent, Role
from groupagent.persistence import PostgresStore
from groupagent.scheduling import (
    AtTiming,
    OccurrenceState,
    PreparedMessage,
    Scheduler,
    WeeklyTiming,
)
from groupagent.scheduling.temporal import (
    SCHEDULE_TIMING_ADAPTER,
    clock_snapshot,
    resolve_instant,
    resolve_schedule_timing,
)

SHANGHAI = "Asia/Shanghai"
SOURCE_TIME = datetime(2026, 11, 17, 20, 35, 22, tzinfo=UTC)
DUE = datetime(2026, 11, 17, 20, 40, 22, tzinfo=UTC)


@pytest.mark.parametrize(
    ("amount", "unit", "elapsed"),
    [(7, "minutes", 420), (0.5, "minutes", 30), (1.5, "minutes", 90), (0.5, "hours", 1800)],
)
def test_stated_duration_uses_exact_host_units_for_instant_and_shift(amount, unit, elapsed):
    proposal = {"kind": "after", "amount": amount, "unit": unit}
    assert resolve_instant(
        proposal, occurred_at=SOURCE_TIME, time_zone=SHANGHAI
    ) == SOURCE_TIME + timedelta(seconds=elapsed)
    shifted, _ = resolve_schedule_timing(
        {"kind": "shift", "amount": -amount, "unit": unit},
        occurred_at=SOURCE_TIME,
        time_zone=SHANGHAI,
        reference_timing=AtTiming(at=DUE),
    )
    assert shifted == AtTiming(at=DUE - timedelta(seconds=elapsed))


def test_advertised_duration_schema_uses_amount_and_unit_while_old_seconds_load():
    definitions = SCHEDULE_TIMING_ADAPTER.json_schema()["$defs"]
    for name in ("AfterTiming", "ShiftTiming"):
        assert set(definitions[name]["properties"]) == {"kind", "amount", "unit"}
    legacy = SCHEDULE_TIMING_ADAPTER.validate_json('{"kind":"after","seconds":300}', strict=True)
    assert legacy.kind == "after" and legacy.seconds == 300


def test_snapshot_and_one_shot_consume_the_same_event_clock() -> None:
    occurred_at = datetime(2026, 11, 17, 15, 58, 42, tzinfo=UTC)
    now = occurred_at + timedelta(seconds=67)
    snapshot = clock_snapshot(occurred_at=occurred_at, now=now, time_zone=SHANGHAI)
    assert snapshot == {
        "time_zone": SHANGHAI,
        "now": "2026-11-17T23:59:49+08:00",
        "now_utc": "2026-11-17T15:59:49+00:00",
        "occurred_at": "2026-11-17T23:58:42+08:00",
        "occurred_at_utc": "2026-11-17T15:58:42+00:00",
    }
    proposed = {"kind": "after", "seconds": 300}
    deadline = resolve_instant(proposed, occurred_at=occurred_at, time_zone=SHANGHAI)
    schedule, display_zone = resolve_schedule_timing(
        proposed, occurred_at=occurred_at, time_zone=SHANGHAI
    )
    assert deadline == datetime(2026, 11, 17, 16, 3, 42, tzinfo=UTC)
    assert schedule == AtTiming(at=deadline) and display_zone == SHANGHAI


@pytest.mark.parametrize(
    ("local_date", "fold", "expected"),
    [
        ("2026-03-08", None, "nonexistent_local_time"),
        ("2026-11-01", None, "ambiguous_dst_fold"),
        ("2026-11-01", 0, datetime(2026, 11, 1, 5, 30, tzinfo=UTC)),
        ("2026-11-01", 1, datetime(2026, 11, 1, 6, 30, tzinfo=UTC)),
    ],
)
def test_one_shot_wall_clock_uses_named_zone_and_explicit_fold(local_date, fold, expected) -> None:
    proposed = {
        "kind": "wall",
        "date": local_date,
        "local_time": "02:30" if local_date == "2026-03-08" else "01:30",
        "time_zone": "America/New_York",
        "fold": fold,
    }
    if isinstance(expected, str):
        with pytest.raises(DomainError) as error:
            resolve_instant(proposed, occurred_at=SOURCE_TIME, time_zone=SHANGHAI)
        assert error.value.code == expected
    else:
        result, display_zone = resolve_schedule_timing(
            proposed, occurred_at=SOURCE_TIME, time_zone=SHANGHAI
        )
        assert result == AtTiming(at=expected) and display_zone == "America/New_York"


@pytest.mark.parametrize("seconds", [True, "300", -1, 0, 10**20])
def test_invalid_or_unrepresentable_relative_duration_is_rejected(seconds) -> None:
    with pytest.raises(DomainError) as error:
        resolve_instant(
            {"kind": "after", "seconds": seconds}, occurred_at=SOURCE_TIME, time_zone=SHANGHAI
        )
    assert error.value.code == "invalid_schedule_time"


@pytest.fixture
def temporal_db():
    dsn = os.environ.get("GROUPAGENT_TEST_DSN")
    if not dsn:
        pytest.skip("Requires isolated PostgreSQL")
    store = PostgresStore(dsn)
    store.migrate()
    group_id = "temporal-" + uuid4().hex
    store.register_group(group_id)
    return store, ActorContext(group_id=group_id, member_id="synthetic-owner", role=Role.OWNER)


def ingest(store, actor, text, occurred_at=SOURCE_TIME, *, arrival_delay=45):
    source = GroupEvent(
        id=uuid4().hex,
        source_key="synthetic-time:" + uuid4().hex,
        group_id=actor.group_id,
        actor_id=actor.member_id,
        kind=EventKind.TEXT,
        text=text,
        occurred_at=occurred_at,
        received_at=occurred_at + timedelta(seconds=arrival_delay),
    )
    with store.transaction(actor) as tx:
        tx.ingest_event(source, source.id, "1", 0)
    return source


@pytest.mark.postgres
def test_shanghai_five_minutes_has_one_outbox_receipt_despite_busy_work_and_replay(
    temporal_db,
) -> None:
    store, actor = temporal_db
    scheduler = Scheduler(store)
    request = ingest(store, actor, "5分钟以后提醒我取快递可以吗")
    with store.transaction(actor) as tx:
        work = tx.create_work("Long unrelated calculation", request.id)
        tx.claim_work(work.id, "synthetic-busy-worker")
        tx.conn.execute(
            "INSERT INTO ga_conversation_policy(group_id,subject_id,quiet) VALUES(%s,%s,true)",
            (actor.group_id, actor.member_id),
        )
    authorization = {
        "authorization_event_id": request.id,
        "request_key": "chat:" + request.id,
        "prepared_message": PreparedMessage(text="取快递", purpose="explicit_reminder"),
    }
    timing = {"kind": "after", "seconds": 300}
    rule = scheduler.create(actor, "取快递", None, timing, now=request.received_at, **authorization)
    assert rule.timing == AtTiming(at=DUE) and rule.next_due_at == DUE
    assert clock_snapshot(occurred_at=SOURCE_TIME, now=DUE, time_zone=SHANGHAI)["now"] == (
        "2026-11-18T04:40:22+08:00"
    )
    registered = scheduler.facts(actor, schedule_id=rule.id)[0]
    assert registered.occurrences == []
    assert registered.next_due_local == "2026-11-18T04:40:22+08:00"
    assert registered.timing_source_local == "2026-11-18T04:35:22+08:00"
    assert scheduler.wake(actor, DUE - timedelta(microseconds=1)) == []

    barrier = Barrier(2)

    def scan(_):
        barrier.wait()
        return Scheduler(store).due(actor, DUE)

    with ThreadPoolExecutor(max_workers=2) as threads:
        first, second = threads.map(scan, range(2))
    (occurrence,) = first + second
    assert occurrence.scheduled_at == DUE and occurrence.work_id is None
    scheduler = Scheduler(store)
    facts = scheduler.facts(actor, schedule_id=rule.id)[0]
    (delivery,) = facts.occurrences[0].deliveries
    assert delivery.state == DeliveryState.PENDING and delivery.platform_message_id is None
    assert scheduler.wake(actor, DUE) == []

    with store.transaction(actor) as tx:
        intent = tx.get_delivery(delivery.id)
        assert intent.payload_ref == "text:取快递"
        assert delivery_admission_in_tx(tx, intent, now=DUE).allowed
        claimed = tx.claim_delivery("synthetic-sender", intent_id=delivery.id)
        assert claimed is not None
        tx.finish_delivery(
            claimed.id,
            claimed.claim_token,
            DeliveryState.ACCEPTED,
            platform_message_id="synthetic-receipt:pick-up-parcel",
        )
        assert [item.id for item in tx.list_work()] == [work.id]
        assert [item.id for item in tx.list_delivery()] == [intent.id]

    replay = scheduler.create(
        actor, "取快递", None, timing, now=DUE + timedelta(minutes=1), **authorization
    )
    assert replay.id == rule.id
    scheduler.wake(actor, DUE + timedelta(minutes=1))
    (occurrence_facts,) = scheduler.facts(actor, schedule_id=rule.id)[0].occurrences
    assert occurrence_facts.state == OccurrenceState.SUCCEEDED
    assert occurrence_facts.scheduled_local == "2026-11-18T04:40:22+08:00"
    (delivered,) = occurrence_facts.deliveries
    assert delivered.platform_message_id == "synthetic-receipt:pick-up-parcel"


@pytest.mark.postgres
def test_late_relative_request_reports_missed_local_instant_and_leaves_no_effect(
    temporal_db,
) -> None:
    store, actor = temporal_db
    request = ingest(store, actor, "过五分钟提醒我打印会议资料", arrival_delay=301)
    scheduler = Scheduler(store)
    with pytest.raises(DomainError) as error:
        scheduler.create(
            actor,
            "打印会议资料",
            None,
            {"kind": "after", "seconds": 300},
            authorization_event_id=request.id,
            prepared_message=PreparedMessage(text="打印会议资料", purpose="explicit_reminder"),
            now=request.received_at,
        )
    assert error.value.code == "schedule_time_missed"
    assert "2026-11-18T04:40:22+08:00" in error.value.message
    assert scheduler.list(actor) == []
    with store.transaction(actor) as tx:
        assert tx.list_work() == [] and tx.list_delivery() == []


@pytest.mark.postgres
def test_shift_uses_current_rule_revision_and_retires_unsubmitted_prior_slot(temporal_db) -> None:
    store, actor = temporal_db
    source = ingest(store, actor, "纽约时间每周日23:58提醒我核对原文")
    scheduler = Scheduler(store)
    original = scheduler.create(
        actor,
        "周末检查",
        None,
        WeeklyTiming(local_time=time(23, 58), weekdays=[6]),
        time_zone="America/New_York",
        authorization_event_id=source.id,
        prepared_message=PreparedMessage(text="核对原文", purpose="explicit_reminder"),
        now=SOURCE_TIME,
    )
    old_slot = datetime(2026, 11, 23, 4, 58, tzinfo=UTC)
    assert original.next_due_at == old_slot
    (old_occurrence,) = scheduler.wake(actor, old_slot)
    change = ingest(store, actor, "把这条安排推迟五分钟", old_slot + timedelta(minutes=1))
    edit = {
        "timing": {"kind": "shift", "seconds": 300},
        "authorization_event_id": change.id,
        "now": change.received_at,
    }
    with pytest.raises(DomainError) as error:
        scheduler.update(actor, original.id, expected_revision=original.revision, **edit)
    assert error.value.code == "version_conflict"
    current = scheduler.get(actor, original.id)
    shifted = scheduler.update(actor, original.id, expected_revision=current.revision, **edit)
    assert shifted.id == original.id and shifted.goal == original.goal
    assert shifted.valid_until == original.valid_until and shifted.time_zone == original.time_zone
    assert shifted.timing == WeeklyTiming(local_time=time(0, 3), weekdays=[0])
    assert shifted.next_due_at == old_slot + timedelta(minutes=5)
    (old_facts,) = scheduler.facts(actor, schedule_id=original.id)[0].occurrences
    assert old_facts.id == old_occurrence.id
    (retired,) = old_facts.deliveries
    assert retired.state == DeliveryState.REJECTED and retired.reason == "rule_updated"
    scheduler.cancel(actor, original.id, expected_revision=shifted.revision)
    assert Scheduler(store).wake(actor, shifted.next_due_at) == []
    assert len(scheduler.occurrences(actor, original.id)) == 1
