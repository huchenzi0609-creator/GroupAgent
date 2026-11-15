"""Real PG/local-spool expiry races; no provider, Docker or production channel."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from uuid import uuid4

import pytest
from psycopg.conninfo import make_conninfo

from groupagent.artifacts import ArtifactError
from groupagent.channels.local import LocalChannel
from groupagent.channels.models import AgentWechatConfig
from groupagent.config import AppConfig, GroupConfig
from groupagent.conversation.service import ConversationCoordinator
from groupagent.coordinator import Decision
from groupagent.domain import DeliveryState
from groupagent.memory import MemberService, MemoryService
from groupagent.ops import CleanupRunner, DeletionJournal
from groupagent.persistence import PostgresStore
from groupagent.runtime.cleanup import RuntimeCleanupHandler
from groupagent.scheduling import Scheduler

from .test_runtime_cleanup import catalog, seed

pytestmark = pytest.mark.postgres


class SlowLocalPurge:
    def __init__(self, channel):
        self.channel = channel
        self.entered, self.release = asyncio.Event(), asyncio.Event()
        self.calls = []

    async def purge_event(self, *args, **kwargs):
        self.calls.append(kwargs["delivery_keys"])
        self.entered.set()
        await self.release.wait()
        return await self.channel.purge_event(*args, **kwargs)


@pytest.fixture
async def expiry_case(store, actor, member, tmp_path):
    root, artifacts, event, work, _, _ = seed(store, member, tmp_path)
    scope, value = catalog(store, member, artifacts, work, event, lifetime=-1)
    channel = LocalChannel(
        root / "channel",
        AgentWechatConfig(
            base_url="http://127.0.0.1",
            account_id="synthetic-account",
            external_group_id="synthetic@chatroom",
            internal_group_id=member.group_id,
            source_epoch="test",
        ),
    )
    with store.transaction(member) as tx:
        tx.conn.execute(
            "UPDATE ga_work_orders SET state='completed' WHERE group_id=%s AND id=%s",
            (member.group_id, work.id),
        )
        intent = tx.enqueue_delivery(
            None, "artifact:" + value.version_id, value.sha256, "expiry:" + value.version_id
        )
        claim = tx.claim_delivery("synthetic-channel", intent_id=intent.id)
    receipt = await channel.send_file(
        member.group_id, value.name, artifacts.read(scope, value.file_id), intent.idempotency_key
    )
    with store.transaction(member) as tx:
        tx.finish_delivery(
            intent.id,
            claim.claim_token,
            DeliveryState.ACCEPTED,
            platform_message_id=receipt.platform_message_id,
        )
    # A broken implementation must fail promptly at the server rather than
    # deadlock pytest's event loop while its synchronous connection waits.
    bounded = PostgresStore(make_conninfo(store.dsn, options="-c lock_timeout=500ms"))
    slow = SlowLocalPurge(channel)
    quiesced = []

    async def quiesce(works, events):
        quiesced.append((works, events))

    handler = RuntimeCleanupHandler(
        bounded,
        artifacts,
        root,
        technical=actor,
        channel=slow,
        quiesce=quiesce,
        is_busy=lambda works, events: False,
    )
    return SimpleNamespace(
        store=bounded,
        actor=actor,
        member=member,
        root=root,
        artifacts=artifacts,
        event=event,
        work=work,
        scope=scope,
        value=value,
        intent=intent,
        receipt=receipt,
        channel=channel,
        slow=slow,
        handler=handler,
        quiesced=quiesced,
    )


async def start_expiry(env):
    task = asyncio.create_task(env.handler.expire_artifacts(env.actor))
    await asyncio.wait_for(env.slow.entered.wait(), timeout=2)
    return task


async def finish_expiry(env, task):
    env.slow.release.set()
    return await asyncio.wait_for(task, timeout=3)


async def test_chat_commits_while_expiry_waits_for_slow_channel_purge(expiry_case):
    env = expiry_case
    calls = []

    class OfflineReply:
        async def decide(self, event, context):
            calls.append(event.id)
            return Decision(
                kind="reply",
                reason_code="synthetic_chat_progress",
                response="这条消息已处理。",
                directness="direct",
                confidence=1,
            )

    coordinator = ConversationCoordinator(
        AppConfig(
            group=GroupConfig(
                enabled=True,
                internal_id=env.actor.group_id,
                external_group_id="synthetic@chatroom",
                owner_ids=[env.member.member_id],
            )
        ),
        env.store,
        lambda *_: OfflineReply(),
        memory=MemoryService(env.store),
        members=MemberService(env.store),
        scheduler=Scheduler(env.store),
    )
    task = await start_expiry(env)
    try:
        now = datetime.now(UTC)
        request = env.event.model_copy(
            update={
                "id": uuid4().hex,
                "source_key": "expiry-chat:" + uuid4().hex,
                "text": "Pot，请解释这条消息。",
                "occurred_at": now,
                "received_at": now,
            }
        )
        result = await asyncio.wait_for(
            coordinator.receive(
                request, stream_id="expiry-chat", cursor="1", verified_channel=True
            ),
            timeout=2,
        )
        assert result.delivery_id and calls == [request.id]
        assert not env.slow.release.is_set() and not task.done()
        with env.store.transaction(coordinator.actor(request)) as tx:
            assert tx.get_decision(request.id) is not None
        assert env.artifacts.read(env.scope, env.value.file_id)
    finally:
        expired = await finish_expiry(env, task)
    assert expired["expired_artifact_versions"] == [env.value.version_id]
    assert expired["errors"] == {} and env.quiesced == [((env.work.id,), ())]
    assert (
        await env.channel.lookup_delivery(env.actor.group_id, env.intent.idempotency_key)
        == env.receipt
    )


async def test_new_retained_sibling_during_purge_is_not_deleted_from_stale_keep_set(expiry_case):
    env = expiry_case
    task = await start_expiry(env)
    try:
        sibling = env.artifacts.revise(
            env.scope,
            env.value.file_id,
            b"new retained sibling",
            expected_version_id=env.value.version_id,
        )
        with env.store.transaction(env.member) as tx:
            tx.conn.execute(
                "INSERT INTO ga_artifact_catalog(group_id,version_id,file_id,work_id,created_by,name,format,sha256,byte_length,body,validation,source_event_ids,expires_at) SELECT group_id,%s,file_id,work_id,created_by,name,format,%s,%s,body,validation,source_event_ids,clock_timestamp()+interval '10 days' FROM ga_artifact_catalog WHERE group_id=%s AND version_id=%s",
                (
                    sibling.version_id,
                    sibling.sha256,
                    sibling.size_bytes,
                    env.actor.group_id,
                    env.value.version_id,
                ),
            )
    finally:
        expired = await finish_expiry(env, task)
    assert expired["errors"] == {} and expired["expired_artifact_versions"] == [
        env.value.version_id
    ]
    assert (
        env.artifacts.read(env.scope, sibling.file_id, sibling.version_id)
        == b"new retained sibling"
    )
    with pytest.raises(ArtifactError):
        env.artifacts.read(env.scope, env.value.file_id, env.value.version_id)


@pytest.mark.parametrize(
    "change,code",
    [
        ("work_restarted", "cleanup_busy"),
        ("busy", "cleanup_busy"),
        ("lease", "cleanup_busy"),
        ("new_delivery", "version_conflict"),
        ("source_changed", "version_conflict"),
    ],
)
async def test_expiry_rechecks_writers_sources_and_delivery_keys_after_purge(
    expiry_case, change, code
):
    env = expiry_case
    if change == "lease":
        env.handler.quiesce = None
    task = await start_expiry(env)
    try:
        if change == "busy":
            env.handler.is_busy = lambda works, events: True
        else:
            with env.store.transaction(env.member) as tx:
                if change == "work_restarted":
                    tx.conn.execute(
                        "UPDATE ga_work_orders SET state='running',revision=revision+1 WHERE group_id=%s AND id=%s",
                        (env.actor.group_id, env.work.id),
                    )
                elif change == "lease":
                    tx.conn.execute(
                        "INSERT INTO ga_process_leases(group_id,name,owner_id,expires_at) VALUES(%s,'application','synthetic-writer',clock_timestamp()+interval '1 minute')",
                        (env.actor.group_id,),
                    )
                elif change == "new_delivery":
                    tx.enqueue_delivery(
                        None,
                        "artifact:" + env.value.version_id,
                        env.value.sha256,
                        "new-expiry-delivery:" + env.value.version_id,
                    )
                else:
                    tx.conn.execute(
                        "UPDATE ga_events SET body=jsonb_set(body,'{text}',to_jsonb(%s::text)) WHERE group_id=%s AND id=%s",
                        ("synthetic changed source", env.actor.group_id, env.event.id),
                    )
    finally:
        expired = await finish_expiry(env, task)
    assert expired["expired_artifact_versions"] == []
    assert expired["errors"] == {"artifact:" + env.value.file_id: code}
    assert env.artifacts.read(env.scope, env.value.file_id)
    with env.store.transaction(env.actor) as tx:
        assert tx.conn.execute(
            "SELECT available FROM ga_artifact_catalog WHERE group_id=%s AND version_id=%s",
            (env.actor.group_id, env.value.version_id),
        ).fetchone()["available"]
        assert tx.get_delivery(env.intent.id).reason is None


async def test_receipt_becoming_uncertain_during_purge_blocks_deletion_until_reconciled(
    expiry_case,
):
    env = expiry_case
    with env.store.transaction(env.member) as tx:
        pending = tx.enqueue_delivery(
            None,
            "artifact:" + env.value.version_id,
            env.value.sha256,
            "pending-expiry-delivery:" + env.value.version_id,
        )
    task = await start_expiry(env)
    try:
        with env.store.transaction(env.member) as tx:
            claim = tx.claim_delivery("synthetic-late-sender", intent_id=pending.id)
    finally:
        expired = await finish_expiry(env, task)
    assert expired["errors"] == {"artifact:" + env.value.file_id: "delivery_unknown"}
    assert not expired["expired_artifact_versions"] and env.artifacts.read(
        env.scope, env.value.file_id
    )
    with env.store.transaction(env.member) as tx:
        tx.finish_delivery(pending.id, claim.claim_token, DeliveryState.REJECTED, reason="not sent")
    retried = await env.handler.expire_artifacts(env.actor)
    assert retried["errors"] == {} and retried["expired_artifact_versions"] == [
        env.value.version_id
    ]
    assert (
        await env.channel.lookup_delivery(env.actor.group_id, env.intent.idempotency_key)
        == env.receipt
    )


async def test_source_forget_during_purge_keeps_its_journal_and_privacy_cleanup_authority(
    expiry_case,
):
    env = expiry_case
    journal = DeletionJournal(env.root / "deletion-journal")
    memory = MemoryService(env.store, deletion_sink=journal)
    env.handler.deletion_sink = journal
    task = await start_expiry(env)
    try:
        memory.forget(env.member, event_ids=[env.event.id])
        assert journal.load(env.actor.group_id)[0].reason == "forgotten"
        assert env.artifacts.read(env.scope, env.value.file_id)
    finally:
        expired = await finish_expiry(env, task)
    assert expired["expired_artifact_versions"] == []
    assert memory.pending_cleanup(env.actor)
    with env.store.transaction(env.actor) as tx:
        assert tx.get_event(env.event.id).content_state == "forgotten"
    cleaned = await CleanupRunner(memory, env.handler).run(env.actor)
    assert cleaned.completed and not cleaned.failed
    with pytest.raises(ArtifactError):
        env.artifacts.read(env.scope, env.value.file_id)
    assert journal.load(env.actor.group_id)[0].reason == "forgotten"
    assert (
        await env.channel.lookup_delivery(env.actor.group_id, env.intent.idempotency_key)
        == env.receipt
    )


async def test_extended_deadline_during_purge_does_not_delete_local_version(expiry_case):
    env = expiry_case
    task = await start_expiry(env)
    try:
        with env.store.transaction(env.member) as tx:
            tx.conn.execute(
                "UPDATE ga_artifact_catalog SET expires_at=%s WHERE group_id=%s AND version_id=%s",
                (datetime.now(UTC) + timedelta(days=10), env.actor.group_id, env.value.version_id),
            )
    finally:
        expired = await finish_expiry(env, task)
    assert expired["errors"] == {} and not expired["expired_artifact_versions"]
    assert env.artifacts.read(env.scope, env.value.file_id)


async def test_unverified_channel_erasure_retries_without_premature_local_deletion(expiry_case):
    env = expiry_case

    class NoEvidence:
        async def purge_event(self, *args, **kwargs):
            return ""

    env.handler.channel = NoEvidence()
    failed = await env.handler.expire_artifacts(env.actor)
    assert failed["errors"] == {"artifact:" + env.value.file_id: "upstream_cleanup_unverified"}
    assert env.artifacts.read(env.scope, env.value.file_id)
    env.handler.channel = env.channel
    retried = await env.handler.expire_artifacts(env.actor)
    assert retried["errors"] == {} and retried["expired_artifact_versions"] == [
        env.value.version_id
    ]
    assert not (await env.handler.expire_artifacts(env.actor))["expired_artifact_versions"]
