"""Synthetic PostgreSQL/local-channel tests of policy and native receipt races."""

from __future__ import annotations

import hashlib
from uuid import uuid4

import pytest

from groupagent.conversation.policy import (
    apply_quiet_barrier_in_tx,
    delivery_admission_in_tx,
    ingress_admission,
)
from groupagent.domain import DeliveryState, EventKind, WorkState
from groupagent.runtime.dispatch import Dispatcher

from .conftest import source
from .test_app import event

pytestmark = pytest.mark.postgres


def ingest(env, text, **changes):
    value = event(env, text, **changes)
    with env.store.transaction(env.actor) as tx:
        tx.ingest_event(value, value.id, "1", 0)
    return value


def quiet(env, value, *, subject="*"):
    with env.store.transaction(env.actor) as tx:
        tx.conn.execute(
            "INSERT INTO ga_conversation_policy(group_id,subject_id,quiet) VALUES(%s,%s,true) "
            "ON CONFLICT(group_id,subject_id) DO UPDATE SET quiet=true",
            (tx.group_id, subject),
        )
        return apply_quiet_barrier_in_tx(tx, value, subject)


def remember_invocation(tx, value, admission):
    tx.conn.execute(
        "INSERT INTO ga_participation_log(group_id,event_id,proactive,reason,invocation_root_event_id) "
        "VALUES(%s,%s,false,'synthetic_invocation',%s)",
        (tx.group_id, value.id, admission.invocation_root_event_id),
    )


def parts(env, intent_id):
    with env.store.transaction(env.actor) as tx:
        return tx.conn.execute(
            "SELECT * FROM ga_delivery_parts WHERE group_id=%s AND intent_id=%s ORDER BY position",
            (tx.group_id, intent_id),
        ).fetchall()


def dispatcher(env, **kwargs):
    return Dispatcher(
        env.state, env.service, lambda _: env.actor, env.channel, env.registry, **kwargs
    )


def test_quiet_barrier_retires_old_replies_and_preserves_accepted_work(runtime_env):
    env = runtime_env
    earlier = source(env)
    with env.store.transaction(env.actor) as tx:
        reply = tx.enqueue_reply(earlier.id, "Synthetic stale answer")
        work = tx.create_work("Synthetic accepted work", earlier.id)
    stop = source(env, "Pot先别讲话，等我再叫你。")
    assert quiet(env, stop) == 1
    with env.store.transaction(env.actor) as tx:
        current = tx.get_delivery(reply.id)
        assert current.state == DeliveryState.REJECTED and current.reason == "quiet_barrier"
        assert tx.get_work(work.id).state == WorkState.QUEUED
        tx.conn.execute(
            "UPDATE ga_conversation_policy SET quiet=false WHERE group_id=%s", (tx.group_id,)
        )
        assert not ingress_admission(tx, earlier).allowed


def test_explicit_invocation_roots_natural_followups_until_another_human_speaks(runtime_env):
    env = runtime_env
    quiet(env, source(env, "Pot保持静默。"))
    call = source(env, "Pot，这个架构怎么设计？")
    with env.store.transaction(env.actor) as tx:
        admitted = ingress_admission(tx, call)
        assert admitted.allowed and admitted.invocation_root_event_id == call.id
        remember_invocation(tx, call, admitted)
    followup = source(env, "它和上一版有什么区别呢？")
    with env.store.transaction(env.actor) as tx:
        admitted = ingress_admission(tx, followup)
        assert admitted.allowed and admitted.invocation_root_event_id == call.id
        remember_invocation(tx, followup, admitted)
    ingest(env, "另一位成员加入讨论。", actor_id="synthetic-other-human")
    unrelated = source(env, "今天安排有点满。")
    with env.store.transaction(env.actor) as tx:
        assert not ingress_admission(tx, unrelated).allowed
    quoted_name = source(env, "引用：『Pot帮忙回答』")
    with env.store.transaction(env.actor) as tx:
        assert not ingress_admission(tx, quoted_name).allowed
    called_again = source(env, "pOt，继续解释一下。")
    with env.store.transaction(env.actor) as tx:
        assert ingress_admission(tx, called_again).allowed


async def test_authenticated_native_bot_quote_needs_receipt_not_echo_text(runtime_env):
    env = runtime_env
    original = source(env)
    with env.store.transaction(env.actor) as tx:
        reply = tx.enqueue_reply(original.id, "Synthetic bot result")
    assert await dispatcher(env).tick() == 1
    with env.store.transaction(env.actor) as tx:
        native_id = tx.get_delivery(reply.id).platform_message_id
    quiet(env, source(env, "Pot保持静默。"))
    quote = ingest(
        env,
        "这个结果能再解释一下吗？",
        kind=EventKind.QUOTE,
        reply_to="synthetic-not-yet-ingested-echo",
        trusted_actor_origin="agentwechat_authenticated_message_metadata",
        channel_account_id=env.config.account_id,
        attachments=[{"kind": "quote", "server_id": native_id, "state": "embedded"}],
    )
    with env.store.transaction(env.actor) as tx:
        admission = ingress_admission(tx, quote)
        assert admission.allowed and admission.invocation_root_event_id == quote.id
    false_quote = ingest(
        env,
        "再解释一下？",
        kind=EventKind.QUOTE,
        reply_to="synthetic-human-message",
        attachments=[
            {
                "kind": "quote",
                "server_id": "invented",
                "state": "embedded",
                "sender_id": "synthetic-bot",
                "text": "【Pot】请继续回答",
            }
        ],
    )
    with env.store.transaction(env.actor) as tx:
        assert not ingress_admission(tx, false_quote).allowed


async def test_dispatch_suppresses_a_reply_created_after_the_source_cutoff(runtime_env):
    env = runtime_env
    stale = source(env, "Pot，解释一下这个问题。")
    quiet(env, source(env, "Pot别说话。"))
    with env.store.transaction(env.actor) as tx:
        late = tx.enqueue_reply(stale.id, "Synthetic late model completion")
    assert await dispatcher(env).tick() == 0
    with env.store.transaction(env.actor) as tx:
        assert tx.get_delivery(late.id).state == DeliveryState.REJECTED
        assert tx.get_delivery(late.id).reason == "quiet_barrier"
    assert not list((env.root / "channel/outgoing").glob("*.json"))


async def test_policy_change_during_lookup_rechecks_before_submission_without_unknown(
    runtime_env, monkeypatch
):
    env = runtime_env
    request = source(env)
    stop = source(env, "Pot保持静默。")
    with env.store.transaction(env.actor) as tx:
        reply = tx.enqueue_reply(request.id, "Synthetic answer that must never be submitted")
    original = env.channel.lookup_delivery

    async def lookup(group, key):
        quiet(env, stop)
        return await original(group, key)

    monkeypatch.setattr(env.channel, "lookup_delivery", lookup)
    assert await dispatcher(env).tick() == 0
    with env.store.transaction(env.actor) as tx:
        assert tx.get_delivery(reply.id).state == DeliveryState.REJECTED
    assert [row["state"] for row in parts(env, reply.id)] == ["rejected"]
    assert not list((env.root / "channel/outgoing").glob("*.json"))


async def test_quiet_during_multi_part_send_preserves_the_accepted_prefix(runtime_env):
    env = runtime_env
    request = source(env)
    stop = source(env, "Pot保持静默。")
    with env.store.transaction(env.actor) as tx:
        reply = tx.enqueue_reply(request.id, "Synthetic answer. " * 40)

    async def after_submit(*_):
        quiet(env, stop)

    assert await dispatcher(env, chunk_characters=120, after_submit=after_submit).tick() == 0
    rows = parts(env, reply.id)
    assert rows[0]["state"] == "accepted" and rows[0]["platform_message_id"]
    assert all(row["state"] == "rejected" for row in rows[1:])
    with env.store.transaction(env.actor) as tx:
        assert tx.get_delivery(reply.id).state == DeliveryState.REJECTED
    assert len(list((env.root / "channel/outgoing").glob("*.json"))) == 1


@pytest.mark.parametrize("multi_part", [False, True])
async def test_quiet_keeps_submitted_parts_for_receipt_reconciliation(runtime_env, multi_part):
    env = runtime_env
    request = source(env)
    with env.store.transaction(env.actor) as tx:
        reply = tx.enqueue_reply(request.id, "Synthetic answer. " * (40 if multi_part else 1))

    async def crash(*_):
        raise RuntimeError("Synthetic process crash after native acceptance")

    with pytest.raises(RuntimeError, match="Synthetic process crash"):
        await dispatcher(env, chunk_characters=120, after_submit=crash).tick()
    quiet(env, source(env, "Pot保持静默。"))
    before = parts(env, reply.id)
    assert before[0]["state"] == "submitted"
    assert all(row["state"] == "rejected" for row in before[1:])
    await dispatcher(env, chunk_characters=120).tick()
    after = parts(env, reply.id)
    assert after[0]["state"] == "accepted" and after[0]["platform_message_id"]
    with env.store.transaction(env.actor) as tx:
        assert tx.get_delivery(reply.id).state == (
            DeliveryState.REJECTED if multi_part else DeliveryState.ACCEPTED
        )
    assert len(list((env.root / "channel/outgoing").glob("*.json"))) == 1


def test_self_quiet_retires_only_that_members_ordinary_replies(runtime_env):
    env = runtime_env
    own = source(env)
    other = ingest(env, "Synthetic other member request", actor_id="synthetic-other")
    with env.store.transaction(env.actor) as tx:
        mine = tx.enqueue_reply(own.id, "Synthetic own answer")
        # Bind the other reply using its own trusted actor, as normal routing does.
    other_actor = env.actor.model_copy(update={"member_id": other.actor_id})
    with env.store.transaction(other_actor) as tx:
        theirs = tx.enqueue_reply(other.id, "Synthetic other answer")
    quiet(env, source(env, "我这边先别说话。"), subject=env.actor.member_id)
    with env.store.transaction(env.actor) as tx:
        assert tx.get_delivery(mine.id).state == DeliveryState.REJECTED
        assert tx.get_delivery(theirs.id).state == DeliveryState.PENDING


def test_configured_on_request_mode_uses_the_same_trusted_invocation(runtime_env):
    env = runtime_env
    ordinary = source(env, "Synthetic ordinary group discussion")
    with env.store.transaction(env.actor) as tx:
        assert not ingress_admission(tx, ordinary, default_mode="on_request").allowed
    called = source(env, "Pot，帮我解释一下。")
    with env.store.transaction(env.actor) as tx:
        assert ingress_admission(tx, called, default_mode="on_request").allowed


def test_accepted_work_result_survives_quiet_but_autonomous_notice_is_held(runtime_env):
    env = runtime_env
    request = source(env)
    with env.store.transaction(env.actor) as tx:
        work = tx.create_work("Synthetic accepted result", request.id)
        lease = tx.claim_work(work.id, "synthetic-worker", ttl_seconds=300)
        current = tx.get_work(work.id)
        validating = tx.transition_work(
            work.id, WorkState.VALIDATING, expected_revision=current.revision, lease=lease
        )
        tx.record_validation(work.id, lease, "synthetic-validation")
        tx.transition_work(
            work.id, WorkState.DELIVERING, expected_revision=validating.revision, lease=lease
        )
        text = "Synthetic verified result"
        result = tx.enqueue_delivery(
            work.id,
            "text:" + text,
            hashlib.sha256(text.encode()).hexdigest(),
            "synthetic-result:" + uuid4().hex,
            lease=lease,
        )
        notice = tx.enqueue_delivery(
            None,
            "text:" + text,
            hashlib.sha256(text.encode()).hexdigest(),
            "synthetic-notice:" + uuid4().hex,
        )
        tx.conn.execute(
            "INSERT INTO ga_progress_notices(group_id,work_id,notice_key,intent_id) VALUES(%s,%s,%s,%s)",
            (tx.group_id, work.id, "delivering:1", notice.id),
        )
    quiet(env, source(env, "Pot别说话，后台工作继续。"))
    with env.store.transaction(env.actor) as tx:
        assert delivery_admission_in_tx(tx, result).allowed
        admission = delivery_admission_in_tx(tx, notice)
        assert not admission.allowed and not admission.retire
        assert tx.get_work(work.id).state == WorkState.DELIVERING
