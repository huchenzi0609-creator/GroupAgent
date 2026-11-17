"""Synthetic scheduling turns checked against PostgreSQL and a local outbox channel."""

from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest

from groupagent.coordinator import Decision
from groupagent.domain import DeliveryState, DomainError
from groupagent.memory import MemoryService
from groupagent.memory.common import memory_lock
from groupagent.runtime.dispatch import Dispatcher
from groupagent.scheduling import AtTiming, EveryTiming, PreparedMessage, Scheduler

from .test_app import app_for, event

pytestmark = pytest.mark.postgres


def verified_source(env, text, *, occurred_at=None):
    request = event(
        env,
        text,
        trusted_actor_origin="verified_channel_metadata",
        **({"occurred_at": occurred_at} if occurred_at else {}),
    )
    with env.store.transaction(env.actor) as tx:
        tx.ingest_event(request, "verified-schedule:" + request.id, "1", 0)
    return request


def prepared(env, scheduler, request, *, text, at, now, exact=True, timing=None):
    return scheduler.create(
        env.actor,
        "合成提醒",
        None,
        timing or AtTiming(at=at),
        now=now,
        authorization_event_id=request.id,
        prepared_message=PreparedMessage(
            text=text, purpose="explicit_reminder" if exact else "notification"
        ),
    )


def quiet(env, subject, enabled):
    with env.store.transaction(env.actor) as tx:
        tx.conn.execute(
            "INSERT INTO ga_conversation_policy(group_id,subject_id,quiet) VALUES(%s,%s,%s) "
            "ON CONFLICT(group_id,subject_id) DO UPDATE SET quiet=EXCLUDED.quiet",
            (tx.group_id, subject, enabled),
        )


def sender(env, **kwargs):
    return Dispatcher(
        env.state, env.service, lambda _: env.actor, env.channel, env.registry, **kwargs
    )


@pytest.mark.parametrize("replace_body", [False, True])
async def test_explicit_message_and_timing_updates_never_create_work(runtime_env, replace_body):
    env = runtime_env
    original_body = "检查合成实验清单"
    request = event(env, f"请在十分钟后提醒我‘{original_body}’，只提醒一次。")
    first_due = request.occurred_at + timedelta(minutes=10)
    decisions = {
        request.id: Decision(
            kind="reply",
            reason_code="synthetic_typed_reminder",
            response="已安排提醒。",
            directness="direct",
            confidence=1,
            action={
                "op": "create_schedule",
                "title": "合成提醒",
                "prepared_message": {"text": original_body, "purpose": "explicit_reminder"},
                "timing": {"kind": "at", "at": first_due.isoformat()},
            },
        )
    }
    app, models = app_for(env, decisions)
    try:
        await app.start()
        received = await app.coordinator.receive(
            request, stream_id="literal-create", cursor="1", verified_channel=True
        )
        scheduler = app.coordinator.scheduler
        rule = scheduler.list(env.actor)[0]
        assert received.action_result["next_due_at"] == first_due.isoformat()
        assert rule.goal == original_body and rule.scope["prepared_message_source_id"] == request.id
        assert set(rule.scope["source_fingerprints"]) == {request.id}
        expected = "核对合成评分表" if replace_body else original_body
        text = (
            f"把提醒内容改为“{expected}”。"
            if replace_body
            else "把提醒改到二十分钟后，仍然只提醒一次。"
        )
        update = event(env, text, reply_to=request.id)
        second_due = update.occurred_at + timedelta(minutes=20)
        decisions[update.id] = Decision(
            kind="reply",
            reason_code="synthetic_update_reminder",
            response="已更新提醒。",
            directness="direct",
            confidence=1,
            action={
                "op": "change_schedule",
                "schedule_id": rule.id,
                "expected_revision": rule.revision,
                "timing": {"kind": "at", "at": second_due.isoformat()},
                **(
                    {"prepared_message": {"text": expected, "purpose": "explicit_reminder"}}
                    if replace_body
                    else {}
                ),
            },
        )
        await app.coordinator.receive(
            update, stream_id="literal-update", cursor="1", verified_channel=True
        )
        revised = scheduler.get(env.actor, rule.id)
        assert revised.goal == expected and revised.execution_kind.value == "explicit_reminder"
        assert revised.scope["prepared_message_source_id"] == (
            update.id if replace_body else request.id
        )
        assert "reminder_body" not in revised.scope
        assert scheduler.wake(env.actor, second_due - timedelta(microseconds=1)) == []
        occurrence = scheduler.wake(env.actor, second_due)[0]
        with env.store.transaction(env.actor) as tx:
            delivery = tx.get_delivery(occurrence.delivery_id)
            assert tx.list_work() == []
        assert delivery.payload_ref == "text:" + expected and occurrence.work_id is None
        assert occurrence.scheduled_at == second_due and not models
    finally:
        await app.close()


async def test_personal_quiet_only_allows_exact_reminder_and_group_quiet_holds(runtime_env):
    env = runtime_env
    now = datetime.now(UTC)
    call = verified_source(env, "十分钟后提醒我：核对 **原文**，并在同一时刻发送预先拟好的通知。")
    scheduler = Scheduler(env.store)
    at = now + timedelta(minutes=10)
    reminder = prepared(env, scheduler, call, text="核对 **原文**", at=at, now=now)
    notification = prepared(env, scheduler, call, text="普通预拟通知", at=at, now=now, exact=False)
    future = scheduler.create(
        env.actor,
        "延迟运算",
        "十分钟后读取最新数值并计算总和",
        AtTiming(at=at),
        now=now,
        authorization_event_id=call.id,
    )
    assert scheduler.next_wake(env.actor) == at
    wake = Scheduler(env.store).wake(env.actor, at + timedelta(seconds=54))
    assert len(wake) == 3 and all(occurrence.scheduled_at == at for occurrence in wake)
    assert scheduler.wake(env.actor, at + timedelta(seconds=54)) == []
    quiet(env, env.actor.member_id, True)
    quiet(env, "*", True)
    dispatch = sender(env)
    assert await dispatch.tick() == 0
    for rule in (reminder, notification):
        facts = scheduler.facts(env.actor, schedule_id=rule.id)[0]
        delivery = facts.occurrences[0].deliveries[0]
        assert delivery.state == DeliveryState.PENDING and delivery.reason == "group_quiet_hold"
        assert delivery.platform_message_id is None
    assert scheduler.facts(env.actor, schedule_id=future.id)[0].occurrences[0].work_id
    quiet(env, "*", False)
    assert await dispatch.tick() == 1
    delivery = scheduler.facts(env.actor, schedule_id=reminder.id)[0].occurrences[0].deliveries[0]
    assert delivery.state == DeliveryState.ACCEPTED and delivery.platform_message_id
    with env.store.transaction(env.actor) as tx:
        part = tx.conn.execute(
            "SELECT rendered_text FROM ga_delivery_parts WHERE group_id=%s AND intent_id=%s",
            (tx.group_id, delivery.id),
        ).fetchone()
        assert part["rendered_text"].endswith("核对 **原文**")
    delivery = (
        scheduler.facts(env.actor, schedule_id=notification.id)[0].occurrences[0].deliveries[0]
    )
    assert delivery.state == DeliveryState.PENDING and delivery.reason == "quiet_hold"
    rule = scheduler.get(env.actor, notification.id)
    scheduler.cancel(env.actor, rule.id, expected_revision=rule.revision)
    quiet(env, env.actor.member_id, False)
    assert await dispatch.tick() == 0
    delivery = (
        scheduler.facts(env.actor, schedule_id=notification.id)[0].occurrences[0].deliveries[0]
    )
    assert delivery.state == DeliveryState.REJECTED and delivery.reason == "schedule_cancelled"


async def test_retention_keeps_grant_but_explicit_erase_removes_delivery(runtime_env):
    env = runtime_env
    now = datetime.now(UTC)
    auth = verified_source(env, "到时提醒检查名单", occurred_at=now - timedelta(days=2))
    scheduler = Scheduler(env.store)
    at = now + timedelta(minutes=10)
    rule = prepared(env, scheduler, auth, text="检查名单", at=at, now=now)
    memory = MemoryService(env.store)
    memory.expire(env.service, before=now - timedelta(days=1), now=now)
    with env.store.transaction(env.actor) as tx:
        assert tx.get_event(auth.id).content_state == "expired"
    occurrence = scheduler.wake(env.actor, at)[0]
    assert occurrence.delivery_id and occurrence.work_id is None
    memory.forget(env.actor, event_ids=[auth.id], now=now)
    facts = scheduler.facts(env.actor, schedule_id=rule.id)[0]
    assert facts.state.value == "cancelled" and facts.goal == "[source removed]"
    delivery = facts.occurrences[0].deliveries[0]
    assert delivery.state == DeliveryState.REJECTED and delivery.reason == "source_forgotten"
    assert await sender(env).tick() == 0


async def test_latest_periodic_slot_and_crash_reconcile_use_one_native_key(runtime_env):
    env = runtime_env
    now = datetime.now(UTC)
    auth = verified_source(env, "每小时提醒一下检查名单")
    at = now + timedelta(hours=1)
    scheduler = Scheduler(env.store)
    rule = prepared(
        env,
        scheduler,
        auth,
        text="检查名单",
        at=at,
        now=now,
        timing=EveryTiming(interval_seconds=3600),
    )
    occurrence = scheduler.wake(env.actor, now + timedelta(hours=4, seconds=54))[0]
    assert occurrence.scheduled_at == now + timedelta(hours=4)
    assert occurrence.delivery_id and occurrence.coalesced_from == at
    with pytest.raises(DomainError, match="quote the author's wording"):
        prepared(env, scheduler, auth, text="模型自行补写的文字", at=at, now=now)

    async def crash_after_submit(*_):
        raise RuntimeError("synthetic lost response")

    with pytest.raises(RuntimeError, match="synthetic lost response"):
        await sender(env, after_submit=crash_after_submit).tick()
    assert (
        scheduler.facts(env.actor, schedule_id=rule.id)[0].occurrences[0].deliveries[0].state
        == DeliveryState.UNKNOWN
    )
    assert await sender(env).tick() == 1
    assert await sender(env).tick() == 0
    facts = scheduler.facts(env.actor, schedule_id=rule.id)[0]
    assert len(facts.occurrences) == 1 and len(facts.occurrences[0].deliveries) == 1
    accepted = facts.occurrences[0].deliveries[0]
    assert accepted.id == occurrence.delivery_id and accepted.state == DeliveryState.ACCEPTED


def test_clock_retries_a_locked_existing_slot_without_replacing_it(runtime_env):
    env = runtime_env
    now = datetime.now(UTC)
    at = now + timedelta(minutes=10)
    auth = verified_source(env, "十分钟后提醒我检查名单")
    scheduler = Scheduler(env.store)
    rule = prepared(env, scheduler, auth, text="检查名单", at=at, now=now)
    existing_id = uuid4().hex
    with env.store.transaction(env.actor) as tx:
        tx.conn.execute(
            "INSERT INTO ga_schedule_occurrences (id,group_id,schedule_id,scheduled_at,state) "
            "VALUES (%s,%s,%s,%s,'pending')",
            (existing_id, tx.group_id, rule.id, at),
        )
    with env.store.transaction(env.actor) as maintenance:
        memory_lock(maintenance)
        with pytest.raises(DomainError) as caught:
            scheduler.wake(env.actor, at)
        assert caught.value.code == "resource_deferred"
    admitted = scheduler.wake(env.actor, at)
    assert len(admitted) == 1 and admitted[0].id == existing_id and admitted[0].delivery_id
    assert scheduler.wake(env.actor, at) == []
    assert [item.id for item in scheduler.occurrences(env.actor, rule.id)] == [existing_id]


async def test_quiet_coalesces_only_unsubmitted_periodic_delivery(runtime_env):
    env = runtime_env
    now = datetime.now(UTC)
    source = verified_source(env, "每小时提醒我检查名单")
    scheduler = Scheduler(env.store)
    first_due = now + timedelta(hours=1)
    rule = prepared(
        env,
        scheduler,
        source,
        text="检查名单",
        at=first_due,
        now=now,
        timing=EveryTiming(interval_seconds=3600),
    )
    quiet(env, "*", True)
    scheduler.wake(env.actor, first_due)
    dispatch = sender(env)
    assert await dispatch.tick() == 0
    scheduler.wake(env.actor, first_due + timedelta(hours=1))
    history = scheduler.facts(env.actor, schedule_id=rule.id)[0].occurrences
    assert len(history) == 2
    assert history[0].deliveries[0].state == DeliveryState.PENDING
    assert history[1].deliveries[0].reason == "coalesced_by_newer_slot"
    quiet(env, "*", False)
    assert await dispatch.tick() == 1
    assert await dispatch.tick() == 0


def test_erasing_a_replaced_body_retires_its_delivery_not_the_current_rule(runtime_env):
    env = runtime_env
    now = datetime.now(UTC)
    scheduler = Scheduler(env.store)
    request = verified_source(env, "每小时提醒我检查旧名单")
    due = now + timedelta(hours=1)
    rule = prepared(
        env,
        scheduler,
        request,
        text="检查旧名单",
        at=due,
        now=now,
        timing=EveryTiming(interval_seconds=3600),
    )
    old = verified_source(env, "把提醒文案改为检查中期名单")
    rule = scheduler.update(
        env.actor,
        rule.id,
        expected_revision=rule.revision,
        authorization_event_id=old.id,
        prepared_message=PreparedMessage(text="检查中期名单", purpose="explicit_reminder"),
        now=now,
    )
    occurrence = scheduler.wake(env.actor, due)[0]
    fresh = verified_source(env, "把提醒文案改为检查最终名单")
    current = scheduler.get(env.actor, rule.id)
    scheduler.update(
        env.actor,
        rule.id,
        expected_revision=current.revision,
        authorization_event_id=fresh.id,
        prepared_message=PreparedMessage(text="检查最终名单", purpose="explicit_reminder"),
        now=due,
    )
    MemoryService(env.store).forget(env.actor, event_ids=[old.id])
    facts = scheduler.facts(env.actor, schedule_id=rule.id)[0]
    assert facts.state.value == "active" and facts.goal == "检查最终名单"
    obsolete = facts.occurrences[0].deliveries[0]
    assert obsolete.id == occurrence.delivery_id and obsolete.reason == "source_forgotten"
