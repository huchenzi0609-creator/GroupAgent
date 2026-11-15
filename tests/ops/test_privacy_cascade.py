from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from uuid import uuid4

import pytest

from groupagent.domain import DeliveryState, EventKind, GroupEvent, Role
from groupagent.memory import MemoryService
from groupagent.scheduling import EveryTiming, Scheduler

pytestmark = pytest.mark.postgres


def emit(store, actor, text, **extra):
    stamp = datetime.now(UTC)
    identifier = uuid4().hex
    event = GroupEvent(
        id=identifier,
        source_key="cascade:" + identifier,
        group_id=actor.group_id,
        actor_id=actor.member_id,
        kind=EventKind.TEXT,
        text=text,
        occurred_at=stamp,
        received_at=stamp,
        **extra,
    )
    with store.transaction(actor) as tx:
        result = tx.ingest_event(event, identifier, "1", 0)
    return result.event


def reply(store, member, question, secret, *, accept=True):
    with store.transaction(member) as tx:
        tx.record_decision(
            question.id,
            {
                "kind": "reply",
                "source_event_ids": [question.id, secret.id],
                "source_fingerprints": {
                    question.id: tx.event_fingerprint(question.id),
                    secret.id: tx.event_fingerprint(secret.id),
                },
                "result": {"response": "private derived answer"},
            },
        )
        intent = tx.enqueue_reply(question.id, "private derived answer")
        claimed = tx.claim_delivery("synthetic-channel", intent_id=intent.id)
        if accept:
            tx.finish_delivery(
                intent.id,
                claimed.claim_token,
                DeliveryState.ACCEPTED,
                platform_message_id="bot:" + intent.id,
            )
    return intent, claimed


def test_rag_reply_and_verified_echo_are_removed_without_touching_same_account_human(
    store, actor, member
):
    memory = MemoryService(store)
    secret = emit(store, member, "private source")
    question = emit(store, member, "what did I say earlier")
    intent, _ = reply(store, member, question, secret)
    shared = actor.model_copy(update={"member_id": "shared-bot-and-human", "role": Role.OWNER})
    echo = emit(store, shared, "private derived answer", platform_message_id="bot:" + intent.id)
    human = emit(store, shared, "private derived answer", platform_message_id="human:independent")
    memory.forget(member, event_ids=[secret.id])
    with store.transaction(member) as tx:
        assert tx.get_event(question.id).text == question.text
        assert tx.get_event(echo.id).text == ""
        assert tx.get_event(human.id).text == human.text
        decision = tx.get_decision(question.id)
        assert decision["privacy_redacted"] and secret.id in decision["source_fingerprints"]
        assert "private derived answer" not in json.dumps(decision)
        receipt = tx.get_delivery(intent.id)
        assert (
            receipt.state == DeliveryState.ACCEPTED
            and receipt.platform_message_id == "bot:" + intent.id
        )
        assert receipt.payload_ref == "text:[content removed]"


def test_late_send_receipt_does_not_restore_echo_or_quote_content(store, actor, member):
    memory = MemoryService(store)
    secret = emit(store, member, "private source")
    question = emit(store, member, "summarize earlier")
    intent, claimed = reply(store, member, question, secret, accept=False)
    bot = actor.model_copy(update={"member_id": "bot-account", "role": Role.OWNER})
    echo = emit(store, bot, "private derived answer", platform_message_id="late:receipt")
    memory.forget(member, event_ids=[secret.id])
    with store.transaction(member) as tx:
        tx.finish_delivery(
            intent.id,
            claimed.claim_token,
            DeliveryState.ACCEPTED,
            platform_message_id="late:receipt",
        )
        assert tx.get_event(echo.id).content_state == "forgotten"
    assert not any(
        hit.id == echo.id for hit in memory.retrieve(member, "private derived", include_tasks=False)
    )
    stamp = datetime.now(UTC)
    quoted = GroupEvent(
        id=uuid4().hex,
        source_key="quote:" + uuid4().hex,
        group_id=member.group_id,
        actor_id=member.member_id,
        kind=EventKind.QUOTE,
        text="my own words",
        occurred_at=stamp,
        received_at=stamp,
        reply_to=echo.id,
        attachments=[{"kind": "quote", "text": "private derived answer"}],
    )
    with store.transaction(member) as tx:
        canonical = tx.ingest_event(quoted, quoted.id, "1", 0).event
        assert canonical.text == "my own words" and canonical.attachments[0]["privacy_redacted"]


def test_partial_send_receipts_and_progress_notices_follow_work_privacy(store, actor, member):
    memory = MemoryService(store)
    secret = emit(store, member, "private goal")
    with store.transaction(member) as tx:
        work = tx.create_work("private goal", secret.id)
        notice = tx.enqueue_delivery(
            None,
            "text:private progress",
            hashlib.sha256(b"private progress").hexdigest(),
            "notice:" + work.id,
        )
        tx.conn.execute(
            "INSERT INTO ga_progress_notices(group_id,work_id,notice_key,intent_id) VALUES (%s,%s,'progress',%s)",
            (member.group_id, work.id, notice.id),
        )
        tx.conn.execute(
            "INSERT INTO ga_delivery_parts(group_id,intent_id,position,idempotency_key,payload_hash,state,platform_message_id) VALUES (%s,%s,0,%s,%s,'accepted','part:accepted')",
            (member.group_id, notice.id, "notice-part:" + work.id, "a" * 64),
        )
    bot = actor.model_copy(update={"member_id": "bot", "role": Role.OWNER})
    echo = emit(store, bot, "private progress", platform_message_id="part:accepted")
    memory.forget(member, event_ids=[secret.id])
    with store.transaction(member) as tx:
        assert tx.get_delivery(notice.id).state == DeliveryState.REJECTED
        assert tx.get_event(echo.id).text == ""


def test_forget_cancels_derived_time_and_event_rules_without_new_work(store, actor, member):
    memory = MemoryService(store)
    secret = emit(store, member, "private schedule goal")
    scheduler = Scheduler(store)
    scheduled = scheduler.create(
        member,
        "private schedule goal",
        "private schedule goal",
        EveryTiming(interval_seconds=3600),
        request_key="chat:" + secret.id,
    )
    trigger_id = uuid4().hex
    with store.transaction(member) as tx:
        tx.conn.execute(
            "INSERT INTO ga_trigger_rules(id,group_id,creator_id,creator_role,request_key,event_type,resource_id,goal,max_occurrences,expires_at,policy_revision,scope) VALUES (%s,%s,%s,'member',%s,'task_completed','fixture-resource','private schedule goal',1,clock_timestamp()+interval '1 day',1,jsonb_build_object('request_source_id',%s::text))",
            (trigger_id, member.group_id, member.member_id, "chat:" + secret.id, secret.id),
        )
    memory.forget(member, event_ids=[secret.id])
    assert scheduler.get(member, scheduled.id).state.value == "cancelled"
    assert scheduler.get(member, scheduled.id).goal == "[source removed]"
    with store.transaction(member) as tx:
        row = tx.conn.execute(
            "SELECT state,goal FROM ga_trigger_rules WHERE group_id=%s AND id=%s",
            (member.group_id, trigger_id),
        ).fetchone()
        assert row["state"] == "cancelled" and row["goal"] == "[source removed]"
        assert tx.list_work() == []


def test_later_retention_cannot_downgrade_shared_echo_privacy_tombstone(store, actor, member):
    from datetime import timedelta

    memory = MemoryService(store)
    secret = emit(store, member, "explicitly forgotten source")
    old = emit(store, member, "second dependency")
    question = emit(store, member, "answer from both")
    with store.transaction(member) as tx:
        tx.record_decision(
            question.id,
            {
                "kind": "reply",
                "source_event_ids": [secret.id, old.id],
                "source_fingerprints": {
                    secret.id: tx.event_fingerprint(secret.id),
                    old.id: tx.event_fingerprint(old.id),
                },
            },
        )
        intent = tx.enqueue_reply(question.id, "derived answer from both")
        claim = tx.claim_delivery("fixture", intent_id=intent.id)
        tx.finish_delivery(
            intent.id,
            claim.claim_token,
            DeliveryState.ACCEPTED,
            platform_message_id="shared:" + intent.id,
        )
    echo = emit(
        store, member, "derived answer from both", platform_message_id="shared:" + intent.id
    )
    memory.forget(member, event_ids=[secret.id])
    with store.transaction(actor) as tx:
        tx.conn.execute(
            "UPDATE ga_events SET occurred_at=clock_timestamp()-interval '40 days' WHERE group_id=%s AND id=%s",
            (member.group_id, old.id),
        )
    memory.expire(actor, before=datetime.now(UTC) - timedelta(days=30))
    with store.transaction(member) as tx:
        assert tx.get_event(echo.id).content_state == "forgotten"
        assert tx.get_delivery(intent.id).reason == "source_forgotten"
        assert (
            tx.get_decision(question.id)["result"]["decision"]["reason_code"] == "source_forgotten"
        )


def test_forget_after_raw_expiry_enqueues_new_event_cleanup_for_retained_artifacts(
    store, actor, member
):
    from datetime import timedelta

    memory = MemoryService(store)
    event = emit(store, member, "old source with retained output")
    with store.transaction(actor) as tx:
        tx.conn.execute(
            "UPDATE ga_events SET occurred_at=clock_timestamp()-interval '40 days' WHERE group_id=%s AND id=%s",
            (member.group_id, event.id),
        )
    memory.expire(actor, before=datetime.now(UTC) - timedelta(days=30))
    with store.transaction(actor) as tx:
        tx.conn.execute(
            "UPDATE ga_memory_cleanup SET state='complete',evidence_ref='synthetic:expired' WHERE group_id=%s",
            (actor.group_id,),
        )
    memory.forget(member, event_ids=[event.id])
    pending = memory.pending_cleanup(actor)
    assert any(
        job.event_id == event.id and job.resource_kind == "event_attachments" for job in pending
    )
