from __future__ import annotations

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
