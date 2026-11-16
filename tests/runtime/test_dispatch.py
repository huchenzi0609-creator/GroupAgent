from __future__ import annotations

import hashlib
import json

import pytest

from groupagent.artifacts import ArtifactScope, DocumentService, Report, Section
from groupagent.channels import ChannelReceipt
from groupagent.channels.local import LocalChannel
from groupagent.domain import DeliveryState, DomainError, WorkState
from groupagent.runtime.contracts import DeliveryDraft
from groupagent.runtime.dispatch import Dispatcher, split_text

from .conftest import source

pytestmark = pytest.mark.postgres


async def test_auth_loss_backs_off_the_channel_and_preserves_unsent_intents(
    runtime_env, monkeypatch
):
    env = runtime_env
    events = [source(env, "Synthetic auth-loss request " + str(index)) for index in range(2)]
    with env.store.transaction(env.actor) as tx:
        for event in events:
            tx.enqueue_reply(event.id, "Synthetic retained reply")
    calls = []

    async def unavailable(*args):
        calls.append(args)
        raise DomainError("auth_required", "Synthetic expired channel session")

    monkeypatch.setattr(env.channel, "lookup_delivery", unavailable)
    clock = [0.0]
    dispatcher = Dispatcher(
        env.state,
        env.service,
        lambda _: env.actor,
        env.channel,
        env.registry,
        clock=lambda: clock[0],
    )
    assert await dispatcher.tick() == 0
    assert len(calls) == 1
    assert await dispatcher.tick() == 0
    assert len(calls) == 1
    clock[0] = 31
    await dispatcher.tick()
    assert len(calls) == 2
    with env.store.transaction(env.actor) as tx:
        assert len(tx.list_delivery()) == 2
        assert all(row.state != DeliveryState.ACCEPTED for row in tx.list_delivery())


def test_split_preserves_all_content_and_combining_sequences():
    text = ("一段足够长的中文说明。\n" * 500) + ("a\u0301👨\u200d👩\u200d👧\u200d👦" * 30)
    parts = split_text(text, maximum=200)
    assert len(parts) > 2 and "".join(parts) == text
    assert all(not p.startswith("\u0301") and not p.endswith("\u200d") for p in parts)


async def test_chat_arriving_during_file_send_precedes_remaining_files(runtime_env, monkeypatch):
    env = runtime_env
    event = source(env, "Produce a synthetic background bundle")
    with env.store.transaction(env.actor) as tx:
        work = tx.create_work("Produce two reports", event.id)
        lease = tx.claim_work(work.id, "fixture-worker", ttl_seconds=300)
    scope = ArtifactScope(env.actor.group_id, work.id, env.actor.member_id)
    docs = DocumentService(env.artifacts)
    drafts = []
    for name in ("first.md", "second.md"):
        version = docs.generate(
            scope, name, Report("Example", (Section("Result", ("A verified synthetic result",)),))
        )
        validation = docs.validate(
            scope, version.file_id, version.version_id, expected_text=("verified",)
        )
        env.registry.register(env.actor, lease, version, validation=validation.to_dict())
        drafts.append(
            DeliveryDraft("artifact:" + version.version_id, version.sha256, "file:" + name)
        )
    closing = "Bundle complete"
    drafts.append(
        DeliveryDraft("text:" + closing, hashlib.sha256(closing.encode()).hexdigest(), "bundle:end")
    )
    with env.store.transaction(env.actor) as tx:
        current = tx.get_work(work.id)
        validating = tx.transition_work(
            work.id, WorkState.VALIDATING, expected_revision=current.revision, lease=lease
        )
        tx.record_validation(work.id, lease, "synthetic:verified")
        tx.transition_work(
            work.id, WorkState.DELIVERING, expected_revision=validating.revision, lease=lease
        )
        bundle = env.state.enqueue_bundle_in_tx(tx, lease, tuple(drafts))
    order = []
    original_file, original_text = env.channel.send_file, env.channel.send_text

    async def send_file(group, name, content, key):
        order.append(("file", name))
        if name == "first.md":
            # The reply does not exist when the first file is selected. Admit
            # it while that transport call is in progress, before its receipt.
            question = source(env, "A new foreground chat question")
            background = "Other background text"
            with env.store.transaction(env.actor) as tx:
                tx.enqueue_delivery(
                    None,
                    "text:" + background,
                    hashlib.sha256(background.encode()).hexdigest(),
                    "background:text",
                )
                tx.enqueue_reply(question.id, "Foreground answer")
        return await original_file(group, name, content, key)

    async def send_text(group, text, key):
        order.append(("text", text))
        return await original_text(group, text, key)

    monkeypatch.setattr(env.channel, "send_file", send_file)
    monkeypatch.setattr(env.channel, "send_text", send_text)
    dispatcher = Dispatcher(env.state, env.service, lambda _: env.actor, env.channel, env.registry)
    assert await dispatcher.tick() == 5
    assert order == [
        ("file", "first.md"),
        ("text", dispatcher.badge + "Foreground answer"),
        ("text", dispatcher.badge + "Other background text"),
        ("file", "second.md"),
        ("text", dispatcher.badge + closing),
    ]
    with env.store.transaction(env.actor) as tx:
        assert all(tx.get_delivery(intent.id).state == DeliveryState.ACCEPTED for intent in bundle)


async def test_unresolved_reply_is_checked_once_per_tick_without_blocking_other_text(
    runtime_env, monkeypatch
):
    env = runtime_env
    event = source(env)
    with env.store.transaction(env.actor) as tx:
        unresolved = tx.enqueue_reply(event.id, "Receipt still pending")
        background = "Independent background text"
        other = tx.enqueue_delivery(
            None,
            "text:" + background,
            hashlib.sha256(background.encode()).hexdigest(),
            "independent:text",
        )
    queued = {}
    lookups, sends = [], []
    original_lookup, original_send = env.channel.lookup_delivery, env.channel.send_text

    async def lookup(group, key):
        lookups.append(key)
        return queued.get(key) or await original_lookup(group, key)

    async def send_text(group, text, key):
        assert key in lookups  # Every send still follows a receipt lookup.
        sends.append(key)
        if text.endswith("Receipt still pending"):
            queued[key] = ChannelReceipt(
                account_id="local-account",
                group_id=group,
                idempotency_key=key,
                wire_idempotency_key=key,
                state="queued",
                observed_at=event.occurred_at,
            )
            return queued[key]
        return await original_send(group, text, key)

    monkeypatch.setattr(env.channel, "lookup_delivery", lookup)
    monkeypatch.setattr(env.channel, "send_text", send_text)
    dispatcher = Dispatcher(env.state, env.service, lambda _: env.actor, env.channel, env.registry)
    assert await dispatcher.tick() == 1
    unresolved_key = next(iter(queued))
    assert lookups.count(unresolved_key) == 1 and sends.count(unresolved_key) == 1
    with env.store.transaction(env.actor) as tx:
        assert tx.get_delivery(unresolved.id).state == DeliveryState.UNKNOWN
        assert tx.get_delivery(other.id).state == DeliveryState.ACCEPTED
    assert await dispatcher.tick() == 0
    assert lookups.count(unresolved_key) == 2 and sends.count(unresolved_key) == 1


async def test_long_reply_crash_reconciles_each_part_without_duplicate(runtime_env):
    env = runtime_env
    event = source(env)
    text = "完整回答不会被截成200字。\n" * 80
    with env.store.transaction(env.actor) as tx:
        intent = tx.enqueue_reply(event.id, text)

    async def crash(*args):
        raise RuntimeError("injected crash after actual local acceptance")

    dispatcher = Dispatcher(
        env.state,
        env.service,
        lambda _: env.actor,
        env.channel,
        env.registry,
        chunk_characters=120,
        after_submit=crash,
    )
    with pytest.raises(RuntimeError, match="injected"):
        await dispatcher.tick()
    with env.store.transaction(env.actor) as tx:
        assert tx.get_delivery(intent.id).state == DeliveryState.UNKNOWN
    restarted = Dispatcher(
        env.state,
        env.service,
        lambda _: env.actor,
        LocalChannel(env.root / "channel", env.config),
        env.registry,
        chunk_characters=120,
    )
    await restarted.tick()
    await restarted.tick()
    with env.store.transaction(env.actor) as tx:
        assert tx.get_delivery(intent.id).state == DeliveryState.ACCEPTED
    manifests = list((env.root / "channel/outgoing").glob("*.json"))
    assert len(manifests) == len(split_text(text, maximum=120))
    for path in manifests:
        receipt = json.loads(path.read_text())["receipt"]
        assert restarted.is_bot_message(env.actor, receipt["platform_message_id"])
    assert not restarted.is_bot_message(env.actor, "a-human-with-the-same-text")
    assert await restarted.tick() == 0


async def test_file_bundle_preserves_partial_success_and_actual_bytes(runtime_env):
    env = runtime_env
    event = source(env)
    with env.store.transaction(env.actor) as tx:
        work = tx.create_work("Produce two reports", event.id)
        lease = tx.claim_work(work.id, "fixture-worker", ttl_seconds=300)
    scope = ArtifactScope(env.actor.group_id, work.id, env.actor.member_id)
    docs = DocumentService(env.artifacts)
    versions = []
    for name in ("blocked.md", "good.md"):
        version = docs.generate(
            scope, name, Report("Example", (Section("Result", ("A verified synthetic result",)),))
        )
        validation = docs.validate(
            scope, version.file_id, version.version_id, expected_text=("verified",)
        )
        env.registry.register(env.actor, lease, version, validation=validation.to_dict())
        versions.append(version)
    with env.store.transaction(env.actor) as tx:
        current = tx.get_work(work.id)
        current = tx.transition_work(
            work.id, WorkState.VALIDATING, expected_revision=current.revision, lease=lease
        )
        tx.record_validation(work.id, lease, "synthetic:verified")
        current = tx.transition_work(
            work.id, WorkState.DELIVERING, expected_revision=current.revision, lease=lease
        )
        intents = env.state.enqueue_bundle_in_tx(
            tx,
            lease,
            tuple(
                DeliveryDraft("artifact:" + v.version_id, v.sha256, "file:" + v.version_id)
                for v in versions
            ),
        )

    class PartialChannel:
        async def lookup_delivery(self, group, key):
            return await env.channel.lookup_delivery(group, key)

        async def send_text(self, group, text, key):
            return await env.channel.send_text(group, text, key)

        async def send_file(self, group, name, content, key):
            if name == "blocked.md":
                return ChannelReceipt(
                    account_id="local-account",
                    group_id=group,
                    idempotency_key=key,
                    wire_idempotency_key=key,
                    state="failed",
                    observed_at=event.occurred_at,
                    reason="synthetic rejection",
                )
            return await env.channel.send_file(group, name, content, key)

    dispatcher = Dispatcher(
        env.state, env.service, lambda _: env.actor, PartialChannel(), env.registry
    )
    await dispatcher.tick()
    with env.store.transaction(env.actor) as tx:
        assert tx.get_delivery(intents[0].id).state == DeliveryState.REJECTED
        assert tx.get_delivery(intents[0].id).reason == "synthetic rejection"
        rejected_part = tx.conn.execute(
            "SELECT reason FROM ga_delivery_parts WHERE group_id=%s AND intent_id=%s",
            (env.actor.group_id, intents[0].id),
        ).fetchone()
        assert rejected_part["reason"] == "synthetic rejection"
        assert tx.get_delivery(intents[1].id).state == DeliveryState.ACCEPTED
        with pytest.raises(DomainError):
            tx.transition_work(
                work.id, WorkState.COMPLETED, expected_revision=current.revision, lease=lease
            )
    payloads = list((env.root / "channel/outgoing").glob("*.payload"))
    assert len(payloads) == 1
    assert payloads[0].read_bytes() == env.artifacts.read(scope, versions[1].file_id)
    assert await dispatcher.tick() == 0


async def test_cancellation_before_submission_prevents_send(runtime_env):
    env = runtime_env
    event = source(env)
    with env.store.transaction(env.actor) as tx:
        intent = tx.enqueue_reply(event.id, "Sensitive source result")

    class RevokeBeforeSend:
        async def lookup_delivery(self, group, key):
            from groupagent.memory import MemoryService

            MemoryService(env.store).forget(env.actor, event_ids=[event.id])
            return None

        async def send_text(self, *args):
            raise AssertionError("Must not send after source withdrawal")

        async def send_file(self, *args):
            raise AssertionError("Must not send after source withdrawal")

    dispatcher = Dispatcher(
        env.state, env.service, lambda _: env.actor, RevokeBeforeSend(), env.registry
    )
    await dispatcher.tick()
    with env.store.transaction(env.actor) as tx:
        assert tx.get_delivery(intent.id).state != DeliveryState.ACCEPTED


async def test_local_file_restart_and_purpose_conflict(runtime_env):
    env = runtime_env
    one = await env.channel.send_file(env.actor.group_id, "a.md", b"original", "key")
    restarted = LocalChannel(env.root / "channel", env.config)
    assert (
        await restarted.send_file(env.actor.group_id, "a.md", b"original", "key")
    ).platform_message_id == one.platform_message_id
    with pytest.raises(DomainError) as caught:
        await restarted.send_file(env.actor.group_id, "a.md", b"changed", "key")
    assert caught.value.code == "idempotency_conflict"
