"""Real PostgreSQL, synthetic intents and local receipts; no live WeChat calls."""

from __future__ import annotations

import hashlib
import json
import re
from datetime import UTC, datetime, timedelta

import pytest

from groupagent.channels.local import LocalChannel
from groupagent.domain import DeliveryState, WorkState
from groupagent.runtime.contracts import DeliveryDraft
from groupagent.runtime.dispatch import Dispatcher, split_text
from groupagent.runtime.text import render_wechat_text
from groupagent.scheduling import EveryTiming, PreparedMessage, Scheduler

from .conftest import source

pytestmark = pytest.mark.postgres


def sender(env, **kwargs):
    return Dispatcher(
        env.state, env.service, lambda _: env.actor, env.channel, env.registry, **kwargs
    )


def parts(env, intent):
    with env.store.transaction(env.actor) as tx:
        return tx.conn.execute(
            "SELECT * FROM ga_delivery_parts WHERE group_id=%s AND intent_id=%s ORDER BY position",
            (env.actor.group_id, intent.id),
        ).fetchall()


def spool(env):
    return {
        body["receipt"]["idempotency_key"]: body
        for path in (env.root / "channel/outgoing").glob("*.json")
        if (body := json.loads(path.read_text()))
    }


def legacy_parts(env, intent, *, maximum):
    chunks = split_text(intent.payload_ref[5:], maximum=maximum)
    with env.store.transaction(env.actor) as tx:
        for index, chunk in enumerate(chunks):
            text = "【Pot】" + (f"（{index + 1}/{len(chunks)}）" if len(chunks) > 1 else "") + chunk
            key = (
                "part:" + hashlib.sha256(intent.idempotency_key.encode()).hexdigest() + f":{index}"
            )
            tx.conn.execute(
                "INSERT INTO ga_delivery_parts(group_id,intent_id,position,idempotency_key,"
                "payload_hash) VALUES(%s,%s,%s,%s,%s)",
                (env.actor.group_id, intent.id, index, key, Dispatcher._hash(("text", text, None))),
            )
    return parts(env, intent)


async def test_all_text_delivery_origins_render_at_the_shared_sender(runtime_env):
    env = runtime_env
    markdown = "## 提醒事项\n\n**提交** `report_file`：[资料](https://example.test/a_b)"
    event = source(env)
    with env.store.transaction(env.actor) as tx:
        foreground = tx.enqueue_reply(event.id, markdown)
        background = tx.enqueue_delivery(
            None, "text:" + markdown, hashlib.sha256(markdown.encode()).hexdigest(), "background:1"
        )
        reminder = tx.enqueue_delivery(
            None, "text:" + markdown, hashlib.sha256(markdown.encode()).hexdigest(), "reminder:1"
        )
    dispatcher = sender(env)
    assert await dispatcher.tick() == 3
    outputs = spool(env)
    for intent in (foreground, background, reminder):
        frozen = parts(env, intent)
        assert len(frozen) == 1
        expected = "【Pot】提醒事项\n\n提交 report_file：资料（https://example.test/a_b）"
        assert frozen[0]["rendered_text"] == expected
        assert frozen[0]["payload_hash"] == Dispatcher._hash(("text", expected, None))
        assert outputs[frozen[0]["idempotency_key"]]["payload"]["text"] == expected
        with env.store.transaction(env.actor) as tx:
            persisted = tx.get_delivery(intent.id)
            assert persisted.payload_ref == "text:" + markdown
            assert persisted.payload_hash == hashlib.sha256(markdown.encode()).hexdigest()
        assert dispatcher.is_bot_message(env.actor, frozen[0]["platform_message_id"])
    assert await dispatcher.tick() == 0


async def test_prepared_reminder_and_background_work_share_native_text_delivery(
    runtime_env,
):
    env = runtime_env
    body = "**检查** `report_file`：[资料](https://example.test/source_a)"
    event = source(env, "一小时后只提醒我：" + body)
    scheduler = Scheduler(env.store)
    now = datetime.now(UTC)
    with env.store.transaction(env.actor) as tx:
        background = tx.create_work("Synthetic background work", event.id)
    scheduler.create(
        env.actor,
        "Synthetic literal reminder",
        None,
        EveryTiming(interval_seconds=3600),
        now=now,
        authorization_event_id=event.id,
        prepared_message=PreparedMessage(text=body, purpose="explicit_reminder"),
    )
    occurrence = scheduler.due(env.actor, now + timedelta(hours=1))[0]
    assert occurrence.work_id is None and occurrence.delivery_id is not None
    output = "# 结果\n\n" + body
    with env.store.transaction(env.actor) as tx:
        assert [work.id for work in tx.list_work()] == [background.id]
        reminder = tx.get_delivery(occurrence.delivery_id)
        lease = tx.claim_work(background.id, "synthetic-output-validator", ttl_seconds=300)
        current = tx.get_work(background.id)
        validating = tx.transition_work(
            background.id, WorkState.VALIDATING, expected_revision=current.revision, lease=lease
        )
        tx.record_validation(background.id, lease, "synthetic:body-validated")
        tx.transition_work(
            background.id, WorkState.DELIVERING, expected_revision=validating.revision, lease=lease
        )
        (work_intent,) = env.state.enqueue_bundle_in_tx(
            tx,
            lease,
            (
                DeliveryDraft(
                    "text:" + output,
                    hashlib.sha256(output.encode()).hexdigest(),
                    "synthetic:final-text:" + background.id,
                ),
            ),
        )
    dispatcher = sender(env)
    assert await dispatcher.tick() == 2
    outputs = spool(env)
    rendered_output = "【Pot】结果\n\n检查 report_file：资料（https://example.test/source_a）"
    for intent, expected in ((work_intent, rendered_output), (reminder, "【Pot】" + body)):
        (part,) = parts(env, intent)
        assert part["rendered_text"] == expected
        assert outputs[part["idempotency_key"]]["payload"]["text"] == expected
        assert dispatcher.is_bot_message(env.actor, part["platform_message_id"])
    assert await dispatcher.tick() == 0


async def test_render_once_before_chunking_with_one_badge_per_frozen_part(runtime_env, monkeypatch):
    env = runtime_env
    markdown = "【Pot】# 合成标题\n\n" + "**完整结论**与 `item_name`。\n" * 35
    event = source(env)
    with env.store.transaction(env.actor) as tx:
        intent = tx.enqueue_reply(event.id, markdown)
    calls = []

    def render(text):
        calls.append(text)
        return render_wechat_text(text)

    monkeypatch.setattr("groupagent.runtime.dispatch.render_wechat_text", render)
    dispatcher = sender(env, chunk_characters=80)
    assert await dispatcher.tick() == 1
    assert len(calls) == 1
    frozen = parts(env, intent)
    assert len(frozen) > 2
    bodies = []
    for index, part in enumerate(frozen):
        prefix = f"【Pot】（{index + 1}/{len(frozen)}）"
        assert part["rendered_text"].startswith(prefix)
        assert part["rendered_text"].count("【Pot】") == 1
        bodies.append(part["rendered_text"][len(prefix) :])
    assert "".join(bodies) == render_wechat_text(markdown.removeprefix("【Pot】"))
    assert all("**" not in body and "`" not in body for body in bodies)


async def test_restart_reuses_frozen_text_after_render_and_chunk_settings_change(
    runtime_env, monkeypatch
):
    env = runtime_env
    markdown = "# 合成报告\n\n" + "**完整内容**与 snake_case。\n" * 40
    event = source(env)
    with env.store.transaction(env.actor) as tx:
        intent = tx.enqueue_reply(event.id, markdown)

    async def crash(*_):
        raise RuntimeError("synthetic crash after spool acceptance")

    dispatcher = sender(env, chunk_characters=100, after_submit=crash)
    with pytest.raises(RuntimeError, match="synthetic crash"):
        await dispatcher.tick()
    before = parts(env, intent)
    assert before[0]["state"] == "submitted" and len(spool(env)) == 1

    def forbidden(_):
        raise AssertionError("Frozen text must never be rendered again")

    monkeypatch.setattr("groupagent.runtime.dispatch.render_wechat_text", forbidden)
    env.channel = LocalChannel(env.root / "channel", env.config)
    restarted = sender(env, chunk_characters=50)
    assert await restarted.tick() == 0  # Native receipt accepts only the first part.
    assert await restarted.tick() == 1  # Remaining proven unsubmitted frozen parts continue.
    after = parts(env, intent)
    assert [p["rendered_text"] for p in after] == [p["rendered_text"] for p in before]
    assert [p["payload_hash"] for p in after] == [p["payload_hash"] for p in before]
    assert [p["idempotency_key"] for p in after] == [p["idempotency_key"] for p in before]
    assert all(part["state"] == "accepted" for part in after)
    assert len(spool(env)) == len(after)
    assert await restarted.tick() == 0


async def test_legacy_frozen_parts_keep_original_text_hashes_and_accepted_receipt(
    runtime_env, monkeypatch
):
    env = runtime_env
    markdown = "**Legacy original** item_name。\n" * 9
    event = source(env)
    with env.store.transaction(env.actor) as tx:
        intent = tx.enqueue_reply(event.id, markdown)
    old = legacy_parts(env, intent, maximum=100)
    chunks = split_text(markdown, maximum=100)
    accepted_text = f"【Pot】（1/{len(chunks)}）" + chunks[0]
    receipt = await env.channel.send_text(
        env.actor.group_id, accepted_text, old[0]["idempotency_key"]
    )
    with env.store.transaction(env.actor) as tx:
        tx.conn.execute(
            "UPDATE ga_delivery_parts SET state='accepted',platform_message_id=%s "
            "WHERE group_id=%s AND intent_id=%s AND position=0",
            (receipt.platform_message_id, env.actor.group_id, intent.id),
        )

    def forbidden(_):
        raise AssertionError("Legacy parts must use their original recipe")

    monkeypatch.setattr("groupagent.runtime.dispatch.render_wechat_text", forbidden)
    assert await sender(env, chunk_characters=100).tick() == 1
    after = parts(env, intent)
    assert all(part["rendered_text"] is None for part in after)
    assert [p["payload_hash"] for p in after] == [p["payload_hash"] for p in old]
    assert [p["idempotency_key"] for p in after] == [p["idempotency_key"] for p in old]
    assert after[0]["platform_message_id"] == receipt.platform_message_id
    outputs = spool(env)
    assert len(outputs) == len(chunks)
    combined = "".join(
        re.sub(r"^【Pot】（\d+/\d+）", "", outputs[p["idempotency_key"]]["payload"]["text"])
        for p in after
    )
    assert combined == markdown


async def test_legacy_recipe_mismatch_fails_closed_without_rewriting_parts(runtime_env):
    env = runtime_env
    event = source(env)
    with env.store.transaction(env.actor) as tx:
        intent = tx.enqueue_reply(event.id, "**Legacy original** item_name。\n" * 9)
    before = legacy_parts(env, intent, maximum=100)
    assert await sender(env, chunk_characters=50).tick() == 0
    assert parts(env, intent) == before
    assert not spool(env)
    with env.store.transaction(env.actor) as tx:
        assert tx.get_delivery(intent.id).state == DeliveryState.PENDING


async def test_unknown_receipt_never_resends_or_rewrites_frozen_text(runtime_env, monkeypatch):
    env = runtime_env
    event = source(env)
    with env.store.transaction(env.actor) as tx:
        intent = tx.enqueue_reply(event.id, "**Unknown result**")
    dispatcher = sender(env)
    dispatcher._prepare(env.actor, intent, dispatcher._payloads(env.actor, intent))
    with env.store.transaction(env.actor) as tx:
        claimed = tx.claim_delivery(
            "synthetic-earlier-sender", ttl_seconds=300, intent_id=intent.id
        )
        tx.conn.execute(
            "UPDATE ga_delivery_parts SET state='unknown' WHERE group_id=%s AND intent_id=%s",
            (env.actor.group_id, intent.id),
        )
        tx.finish_delivery(intent.id, claimed.claim_token, DeliveryState.UNKNOWN)
    before = parts(env, intent)
    calls = []

    async def lookup(group, key):
        calls.append(key)
        return None

    async def forbidden(*_):
        raise AssertionError("Uncertain external effects must not be submitted again")

    monkeypatch.setattr(env.channel, "lookup_delivery", lookup)
    monkeypatch.setattr(env.channel, "send_text", forbidden)
    for _ in range(2):
        assert await sender(env, chunk_characters=50).tick() == 0
    assert calls == [before[0]["idempotency_key"]] * 2
    assert parts(env, intent) == before
    assert not spool(env)


async def test_corrupt_frozen_text_cannot_be_sent_or_silently_replaced(runtime_env):
    env = runtime_env
    event = source(env)
    with env.store.transaction(env.actor) as tx:
        intent = tx.enqueue_reply(event.id, "**Original source**")
    dispatcher = sender(env)
    dispatcher._prepare(env.actor, intent, dispatcher._payloads(env.actor, intent))
    with env.store.transaction(env.actor) as tx:
        tx.conn.execute(
            "UPDATE ga_delivery_parts SET rendered_text=%s WHERE group_id=%s AND intent_id=%s",
            ("【Pot】Corrupt synthetic value", env.actor.group_id, intent.id),
        )
    before = parts(env, intent)
    assert await dispatcher.tick() == 0
    assert parts(env, intent) == before
    assert not spool(env)
