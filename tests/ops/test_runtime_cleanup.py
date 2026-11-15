from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from psycopg.types.json import Jsonb

from groupagent.artifacts import ArtifactError, ArtifactScope, ArtifactStore
from groupagent.channels.local import LocalChannel
from groupagent.channels.models import AgentWechatConfig
from groupagent.domain import DomainError, EventKind, GroupEvent
from groupagent.memory import MemoryService
from groupagent.ops import CleanupRunner
from groupagent.runtime.cleanup import RuntimeCleanupHandler

pytestmark = pytest.mark.postgres


def seed(store, member, tmp_path, *, expired=False, channel=False):
    root = tmp_path / "runtime"
    root.mkdir()
    artifacts = ArtifactStore(root / "artifacts")
    stamp = datetime.now(UTC) - timedelta(days=40 if expired else 0)
    identity = uuid4().hex
    event = GroupEvent(
        id=identity,
        source_key="runtime-cleanup:" + identity,
        group_id=member.group_id,
        actor_id=member.member_id,
        kind=EventKind.TEXT,
        text="synthetic source private content",
        occurred_at=stamp,
        received_at=stamp,
        platform_message_id="platform:" + identity if channel else None,
        channel_account_id="synthetic-account" if channel else None,
    )
    with store.transaction(member) as tx:
        tx.ingest_event(event, identity, "1", 0)
        work = tx.create_work(
            "derived synthetic goal",
            event.id,
            scope={"source_fingerprints": {event.id: tx.event_fingerprint(event.id)}},
        )
        tx.conn.execute(
            "INSERT INTO ga_runtime_checkpoints(group_id,work_id,attempt_id,goal_revision,sequence,body,source_event_ids) VALUES(%s,%s,'attempt',1,1,%s,%s)",
            (
                member.group_id,
                work.id,
                Jsonb({"input_request": {"question": event.text}}),
                [event.id],
            ),
        )
        tx.conn.execute(
            "INSERT INTO ga_runtime_events(group_id,work_id,attempt_id,sequence,kind,body) VALUES(%s,%s,'attempt',1,'needs_input',%s)",
            (member.group_id, work.id, Jsonb({"question": event.text})),
        )
    workspace = (
        root
        / "workspaces"
        / hashlib.sha256(member.group_id.encode()).hexdigest()
        / hashlib.sha256(work.id.encode()).hexdigest()
    )
    workspace.mkdir(parents=True)
    (workspace / "scratch.txt").write_text(event.text)
    input_name = hashlib.sha256(
        json.dumps(
            [member.group_id, event.id], sort_keys=True, ensure_ascii=False, separators=(",", ":")
        ).encode()
    ).hexdigest()
    input_dir = root / "inputs" / ("input-" + input_name)
    input_dir.mkdir(parents=True)
    (input_dir / "audio.bin").write_bytes(b"synthetic raw")
    return root, artifacts, event, work, workspace, input_dir


def catalog(store, member, artifacts, work, event, *, passed=True, lifetime=90):
    scope = ArtifactScope(member.group_id, work.id, member.member_id)
    value = artifacts.create(
        scope,
        "result.md",
        b"synthetic retained document",
        provenance={"source_event_ids": [event.id]},
    )
    with store.transaction(member) as tx:
        tx.conn.execute(
            "INSERT INTO ga_artifact_catalog(group_id,version_id,file_id,work_id,created_by,name,format,sha256,byte_length,body,validation,source_event_ids,expires_at) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            (
                member.group_id,
                value.version_id,
                value.file_id,
                work.id,
                member.member_id,
                value.name,
                value.format,
                value.sha256,
                value.size_bytes,
                Jsonb({"synthetic": True}),
                Jsonb({"passed": passed}),
                [event.id],
                datetime.now(UTC) + timedelta(days=lifetime),
            ),
        )
    return scope, value


async def test_forget_removes_managed_files_and_runtime_bodies_without_following_symlinks(
    store, actor, member, tmp_path
):
    root, artifacts, event, work, workspace, inputs = seed(store, member, tmp_path)
    scope, value = catalog(store, member, artifacts, work, event)
    sentinel = tmp_path / "outside.txt"
    sentinel.write_text("untouched unrelated content")
    (workspace / "link").symlink_to(sentinel)
    memory = MemoryService(store)
    memory.forget(member, event_ids=[event.id])
    result = await CleanupRunner(
        memory, RuntimeCleanupHandler(store, artifacts, root, technical=actor)
    ).run(actor)
    assert result.completed and not result.failed
    assert not workspace.exists() and not inputs.exists()
    assert sentinel.read_text() == "untouched unrelated content"
    with pytest.raises(ArtifactError) as error:
        artifacts.read(scope, value.file_id)
    assert error.value.code == "artifact_deleted"
    with store.transaction(actor) as tx:
        assert tx.conn.execute(
            "SELECT body,invalidated FROM ga_runtime_checkpoints"
        ).fetchone() == {"body": {}, "invalidated": True}
        assert tx.conn.execute("SELECT body FROM ga_runtime_events").fetchone()["body"] == {}
        row = tx.conn.execute("SELECT body,name,available FROM ga_artifact_catalog").fetchone()
        assert row == {"body": {}, "name": "[source removed]", "available": False}
    assert (
        await CleanupRunner(
            memory, RuntimeCleanupHandler(store, artifacts, root, technical=actor)
        ).run(actor)
    ).completed == []


@pytest.mark.parametrize(
    "passed,lifetime,retained", [(True, 90, True), (False, 90, False), (True, -1, False)]
)
async def test_raw_expiry_preserves_only_validated_unexpired_artifact(
    store, actor, member, tmp_path, passed, lifetime, retained
):
    root, artifacts, event, work, workspace, inputs = seed(store, member, tmp_path, expired=True)
    scope, value = catalog(store, member, artifacts, work, event, passed=passed, lifetime=lifetime)
    raw = artifacts.create(
        ArtifactScope(member.group_id, "input:" + event.id, member.member_id),
        "raw.md",
        b"synthetic raw",
        provenance={"source_event_id": event.id},
    )
    memory = MemoryService(store)
    memory.expire(actor, before=datetime.now(UTC) - timedelta(days=30))
    result = await CleanupRunner(
        memory, RuntimeCleanupHandler(store, artifacts, root, technical=actor)
    ).run(actor)
    assert result.completed and not result.failed
    assert not workspace.exists() and not inputs.exists()
    if retained:
        assert artifacts.read(scope, value.file_id) == b"synthetic retained document"
    else:
        with pytest.raises(ArtifactError):
            artifacts.read(scope, value.file_id)
    with pytest.raises(ArtifactError):
        artifacts.read(
            ArtifactScope(member.group_id, "input:" + event.id, member.member_id), raw.file_id
        )
    with pytest.raises(ArtifactError) as error:
        artifacts.create(
            ArtifactScope(member.group_id, "input:" + event.id, member.member_id),
            "late.md",
            b"late",
            provenance={"source_event_ids": [event.id]},
        )
    assert error.value.code == "source_deleted"
    if retained:
        reused = artifacts.create(
            ArtifactScope(member.group_id, "new-independent-work", member.member_id),
            "reused.md",
            artifacts.read(scope, value.file_id),
            provenance={"source_event_ids": [event.id], "input_versions": [value.version_id]},
        )
        assert reused.size_bytes == value.size_bytes


async def test_busy_writer_and_unsupported_upstream_remain_pending_with_specific_codes(
    store, actor, member, tmp_path
):
    root, artifacts, event, work, workspace, _ = seed(store, member, tmp_path, channel=True)
    memory = MemoryService(store)
    memory.forget(member, event_ids=[event.id])
    handler = RuntimeCleanupHandler(
        store, artifacts, root, technical=actor, is_busy=lambda works, events: True
    )
    busy = await CleanupRunner(memory, handler).run(actor)
    assert set(busy.errors.values()) == {"cleanup_busy"} and workspace.exists()
    handler.is_busy = lambda works, events: False
    result = await CleanupRunner(memory, handler).run(actor)
    assert "upstream_cleanup_unsupported" in result.errors.values()
    assert not workspace.exists() and memory.pending_cleanup(actor)


async def test_local_spool_deletion_preserves_receipts_and_stream_cursor_and_blocks_replay(
    store, actor, member, tmp_path
):
    root, artifacts, event, _, _, _ = seed(store, member, tmp_path, channel=True)
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
    channel.append_incoming(event)
    handle = channel.add_attachment(
        event.platform_message_id, "input.md", b"raw private", kind="file"
    )
    with store.transaction(member) as tx:
        intent = tx.enqueue_reply(event.id, "derived private")
        claim = tx.claim_delivery("local", intent_id=intent.id)
    receipt = await channel.send_text(member.group_id, "derived private", intent.idempotency_key)
    with store.transaction(member) as tx:
        from groupagent.domain import DeliveryState

        tx.finish_delivery(
            intent.id,
            claim.claim_token,
            DeliveryState.ACCEPTED,
            platform_message_id=receipt.platform_message_id,
        )
    memory = MemoryService(store)
    memory.forget(member, event_ids=[event.id])
    result = await CleanupRunner(
        memory, RuntimeCleanupHandler(store, artifacts, root, technical=actor, channel=channel)
    ).run(actor)
    assert not result.failed
    batch = await channel.consume()
    assert not batch.entries and len(batch.skipped) == 1 and batch.next_cursor.value == "1"
    await channel.ack_committed(batch.next_cursor)
    assert await channel.lookup_delivery(member.group_id, intent.idempotency_key) == receipt
    record = json.loads(channel._receipt_path(member.group_id, intent.idempotency_key).read_text())
    assert record["payload"] == {} and record["deleted"]
    with pytest.raises(DomainError) as error:
        channel.append_incoming(event)
    assert error.value.code == "source_unavailable"
    with pytest.raises(DomainError):
        channel.add_attachment(event.platform_message_id, "input.md", b"raw private", kind="file")
    with pytest.raises(DomainError):
        await channel.fetch_attachment(handle)


async def test_forged_cleanup_job_and_foreign_identity_cannot_delete_files(
    store, actor, member, tmp_path
):
    root, artifacts, event, _, workspace, _ = seed(store, member, tmp_path)
    memory = MemoryService(store)
    memory.forget(member, event_ids=[event.id])
    job = memory.pending_cleanup(actor)[0]
    handler = RuntimeCleanupHandler(store, artifacts, root, technical=actor)
    with pytest.raises(DomainError) as error:
        await handler.purge(job.model_copy(update={"event_id": "another-event"}))
    assert error.value.code == "version_conflict" and workspace.exists()
    handler.technical = member
    with pytest.raises(DomainError) as error:
        await handler.purge(job)
    assert error.value.code == "scope_violation"


async def test_expiry_removes_unvalidated_history_but_forget_later_removes_retained_version(
    store, actor, member, tmp_path
):
    root, artifacts, event, work, _, _ = seed(store, member, tmp_path, expired=True)
    scope, first = catalog(store, member, artifacts, work, event, passed=False)
    latest = artifacts.revise(
        scope,
        first.file_id,
        b"validated independent document",
        expected_version_id=first.version_id,
    )
    with store.transaction(member) as tx:
        tx.conn.execute(
            "INSERT INTO ga_artifact_catalog(group_id,version_id,file_id,work_id,created_by,name,format,sha256,byte_length,body,validation,source_event_ids,expires_at) SELECT group_id,%s,file_id,work_id,created_by,name,format,%s,%s,body,%s,source_event_ids,expires_at FROM ga_artifact_catalog WHERE group_id=%s AND version_id=%s",
            (
                latest.version_id,
                latest.sha256,
                latest.size_bytes,
                Jsonb({"passed": True}),
                member.group_id,
                first.version_id,
            ),
        )
    memory = MemoryService(store)
    runner = CleanupRunner(memory, RuntimeCleanupHandler(store, artifacts, root, technical=actor))
    memory.expire(actor, before=datetime.now(UTC) - timedelta(days=30))
    result = await runner.run(actor)
    assert not result.failed
    assert artifacts.read(scope, first.file_id) == b"validated independent document"
    assert not (artifacts.root / first.file_id / "versions" / first.version_id).exists()
    with store.transaction(actor) as tx:
        assert (
            tx.conn.execute(
                "SELECT available FROM ga_artifact_catalog WHERE group_id=%s AND version_id=%s",
                (actor.group_id, first.version_id),
            ).fetchone()["available"]
            is False
        )
    memory.forget(member, event_ids=[event.id])
    assert not (await runner.run(actor)).failed
    with pytest.raises(ArtifactError) as caught:
        artifacts.read(scope, first.file_id, latest.version_id)
    assert caught.value.code == "artifact_deleted"


async def test_catalog_deadline_physically_expires_only_due_version_without_raw_cleanup_job(
    store, actor, member, tmp_path
):
    root, artifacts, event, work, _, _ = seed(store, member, tmp_path)
    scope, first = catalog(store, member, artifacts, work, event, lifetime=-1)
    sibling = artifacts.revise(
        scope, first.file_id, b"sibling with later deadline", expected_version_id=first.version_id
    )
    with store.transaction(member) as tx:
        tx.conn.execute(
            "INSERT INTO ga_artifact_catalog(group_id,version_id,file_id,work_id,created_by,name,format,sha256,byte_length,body,validation,source_event_ids,expires_at) SELECT group_id,%s,file_id,work_id,created_by,name,format,%s,%s,body,validation,source_event_ids,clock_timestamp()+interval '10 days' FROM ga_artifact_catalog WHERE group_id=%s AND version_id=%s",
            (
                sibling.version_id,
                sibling.sha256,
                sibling.size_bytes,
                member.group_id,
                first.version_id,
            ),
        )
    memory = MemoryService(store)
    assert not memory.pending_cleanup(actor)
    runner = CleanupRunner(memory, RuntimeCleanupHandler(store, artifacts, root, technical=actor))
    result = await runner.run(actor)
    assert not result.errors and result.expired_artifact_versions == [first.version_id]
    assert not (artifacts.root / first.file_id / "versions" / first.version_id).exists()
    assert (
        artifacts.read(scope, first.file_id, sibling.version_id) == b"sibling with later deadline"
    )
    later = await runner.run(actor, now=datetime.now(UTC) + timedelta(days=11))
    assert not later.errors and later.expired_artifact_versions == [sibling.version_id]
    with pytest.raises(ArtifactError):
        artifacts.read(scope, first.file_id)
    assert not (
        await runner.run(actor, now=datetime.now(UTC) + timedelta(days=12))
    ).expired_artifact_versions


async def test_catalog_expiry_respects_writer_and_group_permissions(store, actor, member, tmp_path):
    root, artifacts, event, work, _, _ = seed(store, member, tmp_path)
    scope, value = catalog(store, member, artifacts, work, event, lifetime=-1)
    handler = RuntimeCleanupHandler(
        store, artifacts, root, technical=actor, is_busy=lambda works, events: True
    )
    result = await handler.expire_artifacts(actor)
    assert set(result["errors"].values()) == {"cleanup_busy"}
    assert artifacts.read(scope, value.file_id)
    with pytest.raises(DomainError) as caught:
        await handler.expire_artifacts(member)
    assert caught.value.code == "permission_denied"


@pytest.mark.parametrize("quiesce_fails", [False, True])
async def test_privacy_quiesces_stopped_work_before_deleting_managed_payload(
    store, actor, member, tmp_path, quiesce_fails
):
    root, artifacts, event, work, workspace, _ = seed(store, member, tmp_path)
    memory = MemoryService(store)
    memory.forget(member, event_ids=[event.id])
    calls = []

    async def quiesce(works, events):
        assert workspace.exists()
        assert work.id in works
        with store.transaction(actor) as tx:
            assert tx.get_work(work.id).state.value == "cancelled"
        calls.append((works, events))
        if quiesce_fails:
            raise DomainError("work_busy", "synthetic writer still running")

    handler = RuntimeCleanupHandler(
        store,
        artifacts,
        root,
        technical=actor,
        quiesce=quiesce,
        is_busy=lambda works, events: False,
    )
    job = next(job for job in memory.pending_cleanup(actor) if job.event_id == event.id)
    if quiesce_fails:
        with pytest.raises(DomainError) as caught:
            await handler.purge(job)
        assert caught.value.code == "work_busy" and workspace.exists()
    else:
        assert await handler.purge(job)
        assert not workspace.exists()
    assert len(calls) == 1


async def test_missing_quiesce_does_not_trust_only_local_idle_flag(store, actor, member, tmp_path):
    root, artifacts, event, _, workspace, _ = seed(store, member, tmp_path)
    memory = MemoryService(store)
    memory.forget(member, event_ids=[event.id])
    with store.transaction(actor) as tx:
        tx.conn.execute(
            "INSERT INTO ga_process_leases(group_id,name,owner_id,expires_at) VALUES(%s,'application','synthetic-runtime',clock_timestamp()+interval '1 minute')",
            (actor.group_id,),
        )
    result = await CleanupRunner(
        memory,
        RuntimeCleanupHandler(
            store, artifacts, root, technical=actor, is_busy=lambda works, events: False
        ),
    ).run(actor)
    assert set(result.errors.values()) == {"cleanup_busy"} and workspace.exists()


async def test_audit_retention_is_group_scoped_and_preserves_human_tasks(
    store, actor, member, tmp_path
):
    root, artifacts, event, _, _, _ = seed(store, member, tmp_path)
    with store.transaction(member) as tx:
        task = tx.create_team_task("independent long term duty", source_event_id=event.id)
        tx.conn.execute(
            "UPDATE ga_audit_events SET created_at=clock_timestamp()-interval '91 days' WHERE group_id=%s",
            (actor.group_id,),
        )
    result = await CleanupRunner(
        MemoryService(store),
        RuntimeCleanupHandler(store, artifacts, root, technical=actor, audit_days=90),
    ).run(actor)
    assert result.deleted_audit_events > 0
    with store.transaction(member) as tx:
        assert tx.get_team_task(task.id).title == task.title
        assert not tx.conn.execute(
            "SELECT 1 FROM ga_audit_events WHERE group_id=%s AND created_at<clock_timestamp()-interval '90 days'",
            (actor.group_id,),
        ).fetchone()


async def test_artifact_deadline_purges_exact_local_delivery_copy_and_preserves_receipt(
    store, actor, member, tmp_path
):
    from groupagent.domain import DeliveryState

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
        intent = tx.enqueue_delivery(
            None,
            "artifact:" + value.version_id,
            value.sha256,
            "synthetic-artifact:" + value.version_id,
        )
        claim = tx.claim_delivery("local", intent_id=intent.id)
    receipt = await channel.send_file(
        member.group_id, "result.md", artifacts.read(scope, value.file_id), intent.idempotency_key
    )
    with store.transaction(member) as tx:
        tx.finish_delivery(
            intent.id,
            claim.claim_token,
            DeliveryState.ACCEPTED,
            platform_message_id=receipt.platform_message_id,
        )
    payload = channel._receipt_path(member.group_id, intent.idempotency_key).with_suffix(".payload")
    assert payload.exists()
    result = await CleanupRunner(
        MemoryService(store),
        RuntimeCleanupHandler(store, artifacts, root, technical=actor, channel=channel),
    ).run(actor)
    assert not result.errors and result.expired_artifact_versions == [value.version_id]
    assert not payload.exists()
    assert await channel.lookup_delivery(member.group_id, intent.idempotency_key) == receipt
    with store.transaction(member) as tx:
        assert tx.get_delivery(intent.id).reason == "artifact_expired"


async def test_old_raw_cleanup_does_not_remove_new_work_derived_from_retained_document(
    store, actor, member, tmp_path
):
    root, artifacts, event, original, _, _ = seed(store, member, tmp_path, expired=True)
    _, retained = catalog(store, member, artifacts, original, event)
    memory = MemoryService(store)
    memory.expire(actor, before=datetime.now(UTC) - timedelta(days=30))
    now = datetime.now(UTC)
    request = GroupEvent(
        id=uuid4().hex,
        source_key="reuse:" + uuid4().hex,
        group_id=member.group_id,
        actor_id=member.member_id,
        kind=EventKind.TEXT,
        text="summarize independent document",
        occurred_at=now,
        received_at=now,
    )
    with store.transaction(member) as tx:
        tx.ingest_event(request, request.id, "1", 0)
        new = tx.create_work(
            "reuse retained output",
            request.id,
            input_versions=[retained.version_id],
            scope={
                "source_fingerprints": {event.id: tx.event_fingerprint(event.id)},
                "retained_fact_source_ids": [event.id],
            },
        )
        tx.conn.execute(
            "INSERT INTO ga_runtime_checkpoints(group_id,work_id,attempt_id,goal_revision,sequence,body,source_event_ids) VALUES(%s,%s,'new-attempt',1,1,%s,%s)",
            (member.group_id, new.id, Jsonb({"retained_document": "new safe context"}), [event.id]),
        )
    scope = ArtifactScope(member.group_id, new.id, member.member_id)
    middle = artifacts.create(
        scope,
        "in-progress.md",
        b"legitimate new draft",
        provenance={"source_event_ids": [event.id]},
    )
    seen = []

    async def quiesce(works, events):
        seen.extend(works)
        assert new.id not in works

    result = await CleanupRunner(
        memory,
        RuntimeCleanupHandler(
            store,
            artifacts,
            root,
            technical=actor,
            quiesce=quiesce,
            is_busy=lambda works, events: False,
        ),
    ).run(actor)
    assert not result.errors
    assert artifacts.read(scope, middle.file_id) == b"legitimate new draft"
    assert original.id in seen and new.id not in seen
    with store.transaction(member) as tx:
        assert tx.get_work(new.id).state.value == "queued"
        row = tx.conn.execute(
            "SELECT body,invalidated FROM ga_runtime_checkpoints WHERE group_id=%s AND work_id=%s",
            (member.group_id, new.id),
        ).fetchone()
        assert (
            row["body"] == {"retained_document": "new safe context"} and row["invalidated"] is False
        )


async def test_work_row_stays_locked_through_physical_payload_removal(
    store, actor, member, tmp_path, monkeypatch
):
    from psycopg.errors import LockNotAvailable

    root, artifacts, event, work, _, _ = seed(store, member, tmp_path)
    memory = MemoryService(store)
    memory.forget(member, event_ids=[event.id])
    handler = RuntimeCleanupHandler(store, artifacts, root, technical=actor)
    remove = handler._remove_payloads
    checked = []

    def verify_lock(identity, job, plan):
        with pytest.raises(LockNotAvailable):
            with store.connect() as concurrent:
                concurrent.execute("SET LOCAL lock_timeout='50ms'")
                concurrent.execute(
                    "SELECT id FROM ga_work_orders WHERE group_id=%s AND id=%s FOR UPDATE",
                    (member.group_id, work.id),
                )
        checked.append(work.id)
        remove(identity, job, plan)

    monkeypatch.setattr(handler, "_remove_payloads", verify_lock)
    job = next(job for job in memory.pending_cleanup(actor) if job.event_id == event.id)
    await handler.purge(job)
    assert checked == [work.id]
