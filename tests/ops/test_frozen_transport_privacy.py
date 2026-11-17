"""Actual PostgreSQL erasure/upgrade of transport copies, with synthetic data."""

from __future__ import annotations

import hashlib
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest

from groupagent.domain import DomainError, EventKind, GroupEvent
from groupagent.memory import MemoryService
from groupagent.ops import BackupManager, CleanupRunner
from groupagent.ops.cleanup_proof import local_cleanup_proof
from groupagent.persistence.text_parts import has_rendered_text
from groupagent.runtime.cleanup import RuntimeCleanupHandler

from .test_runtime_cleanup import seed

pytestmark = pytest.mark.postgres


def frozen_reply(store, member, event, *, state="accepted"):
    with store.transaction(member) as tx:
        intent = tx.enqueue_reply(event.id, "**Synthetic** private answer")
        digest = hashlib.sha256(b"synthetic wire fingerprint").hexdigest()
        tx.conn.execute(
            "INSERT INTO ga_delivery_parts(group_id,intent_id,position,idempotency_key,"
            "payload_hash,state,platform_message_id,rendered_text) "
            "VALUES(%s,%s,0,%s,%s,%s,%s,%s)",
            (
                member.group_id,
                intent.id,
                "synthetic-part:" + intent.id,
                digest,
                state,
                "synthetic-native:" + intent.id if state == "accepted" else None,
                "【Pot】Synthetic private answer",
            ),
        )
    return intent, digest


@pytest.mark.parametrize("operation", ["expire", "forget", "recall"])
def test_privacy_removes_frozen_plaintext_and_preserves_receipt_identity(
    store, actor, member, tmp_path, operation
):
    root, artifacts, event, *_ = seed(
        store, member, tmp_path, expired=operation == "expire", channel=True
    )
    try:
        intent, digest = frozen_reply(store, member, event)
        memory = MemoryService(store)
        if operation == "expire":
            memory.expire(actor, before=datetime.now(UTC) - timedelta(days=30))
        elif operation == "forget":
            memory.forget(member, event_ids=[event.id])
        else:
            stamp = datetime.now(UTC)
            recall = GroupEvent(
                id=uuid4().hex,
                source_key=uuid4().hex,
                group_id=actor.group_id,
                actor_id=member.member_id,
                kind=EventKind.RECALL,
                reply_to=event.id,
                occurred_at=stamp,
                received_at=stamp,
                trusted_actor_origin="agentwechat_authenticated_recall_metadata",
            )
            with store.transaction(member) as tx:
                tx.ingest_event(recall, recall.id, "1", 0)
            memory.recall(actor, recall.id)
        with store.transaction(actor) as tx:
            part = tx.conn.execute(
                "SELECT rendered_text,payload_hash,idempotency_key,state,platform_message_id "
                "FROM ga_delivery_parts WHERE intent_id=%s",
                (intent.id,),
            ).fetchone()
            assert part == {
                "rendered_text": None,
                "payload_hash": digest,
                "idempotency_key": "synthetic-part:" + intent.id,
                "state": "accepted",
                "platform_message_id": "synthetic-native:" + intent.id,
            }
    finally:
        artifacts.close()


async def test_late_transport_copy_invalidates_local_proof_until_canonical_erasure(
    store, actor, member, tmp_path, pg_bin
):
    root, artifacts, event, *_ = seed(store, member, tmp_path, expired=True, channel=True)
    try:
        intent, digest = frozen_reply(store, member, event)
        memory = MemoryService(store)
        memory.expire(actor, before=datetime.now(UTC) - timedelta(days=30))
        handler = RuntimeCleanupHandler(store, artifacts, root, technical=actor)
        runner = CleanupRunner(memory, handler)
        await runner.run(actor)
        job = next(job for job in memory.pending_cleanup(actor) if job.event_id == event.id)
        with store.transaction(actor) as tx:
            assert local_cleanup_proof(tx.conn, job) is not None
            tx.conn.execute(
                "UPDATE ga_delivery_parts SET rendered_text=%s WHERE intent_id=%s",
                ("late synthetic private content", intent.id),
            )
        manager = BackupManager(store, tmp_path / "backups", pg_bin)
        with pytest.raises(DomainError) as caught:
            manager.create(actor)
        assert caught.value.code == "privacy_cleanup_pending"
        assert not list(manager.root.glob("*/archive.pgdump"))
        await runner.run(actor)
        with store.transaction(actor) as tx:
            assert local_cleanup_proof(tx.conn, job) is not None
            part = tx.conn.execute(
                "SELECT rendered_text,payload_hash,state FROM ga_delivery_parts WHERE intent_id=%s",
                (intent.id,),
            ).fetchone()
            assert part == {"rendered_text": None, "payload_hash": digest, "state": "accepted"}
        backup = manager.create(actor)
        assert manager.verify(actor, backup.id).archive_bytes > 0
    finally:
        artifacts.close()


async def test_candidate_maintenance_reader_supports_pre16_backup_and_additive_upgrade(
    store, actor, member, tmp_path, pg_bin
):
    root, artifacts, event, *_ = seed(store, member, tmp_path, expired=True, channel=True)
    try:
        intent, digest = frozen_reply(store, member, event, state="unknown")
        with store.transaction(actor) as tx:
            tx.conn.execute("ALTER TABLE ga_delivery_parts DROP COLUMN rendered_text")
            tx.conn.execute("DELETE FROM ga_schema_migrations WHERE version=16")
            before = tx.conn.execute(
                "SELECT * FROM ga_delivery_parts WHERE intent_id=%s", (intent.id,)
            ).fetchone()
            old_checksums = tx.conn.execute(
                "SELECT * FROM ga_schema_migrations WHERE version<=15 ORDER BY version"
            ).fetchall()
            assert not has_rendered_text(tx.conn)
        memory = MemoryService(store)
        memory.expire(actor, before=datetime.now(UTC) - timedelta(days=30))
        await CleanupRunner(
            memory, RuntimeCleanupHandler(store, artifacts, root, technical=actor)
        ).run(actor)
        manager = BackupManager(store, tmp_path / "before-migration", pg_bin)
        backup = manager.create(actor)
        assert manager.verify(actor, backup.id).archive_bytes > 0
        assert store.migrate() == 19
        with store.transaction(actor) as tx:
            after = tx.conn.execute(
                "SELECT * FROM ga_delivery_parts WHERE intent_id=%s", (intent.id,)
            ).fetchone()
            assert after.pop("rendered_text") is None
            assert after == before
            assert after["payload_hash"] == digest and after["state"] == "unknown"
            assert (
                tx.conn.execute(
                    "SELECT * FROM ga_schema_migrations WHERE version<=15 ORDER BY version"
                ).fetchall()
                == old_checksums
            )
    finally:
        artifacts.close()
