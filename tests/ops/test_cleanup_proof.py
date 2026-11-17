from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from psycopg.conninfo import make_conninfo
from psycopg.types.json import Jsonb

from groupagent.artifacts import ArtifactStore
from groupagent.domain import DomainError, EventKind, GroupEvent
from groupagent.memory import MemoryService
from groupagent.ops import BackupManager, CleanupRunner
from groupagent.ops.cleanup_proof import (
    LOCAL_CLEANUP_ACTION,
    local_cleanup_proof,
)
from groupagent.persistence import PostgresStore
from groupagent.runtime.cleanup import RuntimeCleanupHandler

from .test_runtime_cleanup import catalog, seed

pytestmark = pytest.mark.postgres


def assert_blocked(manager, actor):
    with pytest.raises(DomainError) as caught:
        manager.create(actor)
    assert caught.value.code == "privacy_cleanup_pending"
    assert not list(manager.root.glob("*/archive.pgdump"))


async def expire_and_purge(store, actor, member, tmp_path):
    root, artifacts, event, work, workspace, inputs = seed(
        store, member, tmp_path, expired=True, channel=True
    )
    memory = MemoryService(store)
    memory.expire(actor, before=datetime.now(UTC) - timedelta(days=30))
    handler = RuntimeCleanupHandler(store, artifacts, root, technical=actor)
    result = await CleanupRunner(memory, handler).run(actor)
    assert set(result.errors.values()) == {"upstream_cleanup_unsupported"}
    job = next(job for job in memory.pending_cleanup(actor) if job.event_id == event.id)
    return root, artifacts, event, work, workspace, inputs, memory, job


async def test_local_proof_allows_real_archive_while_external_erasure_stays_pending(
    store, actor, member, tmp_path, pg_bin
):
    root, artifacts, event, work, workspace, inputs = seed(
        store, member, tmp_path, expired=True, channel=True
    )
    scope, retained = catalog(store, member, artifacts, work, event)
    before = artifacts.read(scope, retained.file_id, retained.version_id)
    memory = MemoryService(store)
    memory.expire(actor, before=datetime.now(UTC) - timedelta(days=30))
    result = await CleanupRunner(
        memory, RuntimeCleanupHandler(store, artifacts, root, technical=actor)
    ).run(actor)
    assert not result.completed and set(result.errors.values()) == {"upstream_cleanup_unsupported"}
    assert not workspace.exists() and not inputs.exists()
    job = memory.pending_cleanup(actor)[0]
    with store.transaction(actor) as tx:
        proof = local_cleanup_proof(tx.conn, job)
        assert proof is not None
        assert proof["details"]["retained_versions"] == [retained.version_id]
        upstream = tx.conn.execute(
            "SELECT details FROM ga_audit_events WHERE action='runtime_upstream_cleanup_pending' AND resource_ref=%s",
            (job.id,),
        ).fetchone()["details"]
        assert upstream["local_proof_id"] == str(proof["id"])
        assert upstream["limitation"] == "channel_cache_erasure_not_verified"
        assert tx.conn.execute(
            "SELECT body,invalidated FROM ga_runtime_checkpoints WHERE work_id=%s", (work.id,)
        ).fetchone() == {"body": {}, "invalidated": True}
    manager = BackupManager(store, tmp_path / "backups", pg_bin)
    backup = manager.create(actor)
    assert manager.verify(actor, backup.id).archive_bytes > 0
    with store.transaction(actor) as tx:
        assert tx.conn.execute(
            "SELECT state,evidence_ref,revision FROM ga_memory_cleanup WHERE id=%s", (job.id,)
        ).fetchone() == {"state": "pending", "evidence_ref": None, "revision": job.revision}
        retained_row = tx.conn.execute(
            "SELECT available,validation FROM ga_artifact_catalog WHERE version_id=%s",
            (retained.version_id,),
        ).fetchone()
        assert retained_row == {"available": True, "validation": {"passed": True}}
    assert artifacts.read(scope, retained.file_id, retained.version_id) == before
    assert hashlib.sha256(before).hexdigest() == retained.sha256


@pytest.mark.parametrize("change", ["job", "source", "runtime", "foreign_job_proof"])
async def test_stale_or_wrong_local_proof_blocks_archive(
    store, actor, member, tmp_path, pg_bin, change
):
    _, _, event, work, _, _, _, job = await expire_and_purge(store, actor, member, tmp_path)
    with store.transaction(actor) as tx:
        assert local_cleanup_proof(tx.conn, job) is not None
        if change == "job":
            tx.conn.execute(
                "UPDATE ga_memory_cleanup SET revision=revision+1,resource_refs=%s WHERE id=%s",
                (Jsonb(["synthetic:new-generation"]), job.id),
            )
        elif change == "source":
            tx.conn.execute(
                "UPDATE ga_events SET body=body || %s WHERE id=%s",
                (Jsonb({"text": "synthetic late source payload"}), event.id),
            )
        elif change == "runtime":
            tx.conn.execute(
                "UPDATE ga_runtime_events SET body=%s WHERE work_id=%s",
                (Jsonb({"late": "synthetic private payload"}), work.id),
            )
        else:
            proof = local_cleanup_proof(tx.conn, job)
            tx._audit(
                LOCAL_CLEANUP_ACTION,
                job.id,
                **{**proof["details"], "job_sha256": "0" * 64},
            )
    manager = BackupManager(store, tmp_path / "backups", pg_bin)
    assert_blocked(manager, actor)


async def test_legacy_audit_and_real_local_removal_failure_do_not_establish_proof(
    store, actor, member, tmp_path, pg_bin
):
    root, artifacts, event, _, workspace, inputs = seed(
        store, member, tmp_path, expired=True, channel=True
    )
    memory = MemoryService(store)
    memory.expire(actor, before=datetime.now(UTC) - timedelta(days=30))
    job = memory.pending_cleanup(actor)[0]
    with store.transaction(actor) as tx:
        tx._audit("runtime_privacy_payloads_removed", job.id, reason="expired", work_ids=[])
    manager = BackupManager(store, tmp_path / "backups", pg_bin)
    assert_blocked(manager, actor)
    outside = tmp_path / "outside"
    outside.mkdir()
    sentinel = outside / "keep.txt"
    sentinel.write_text("synthetic unrelated payload")
    inputs.parent.rename(root / "original-inputs")
    inputs.parent.symlink_to(outside, target_is_directory=True)
    result = await CleanupRunner(
        memory, RuntimeCleanupHandler(store, artifacts, root, technical=actor)
    ).run(actor)
    assert set(result.errors.values()) == {"unsafe_path"}
    assert workspace.exists() and sentinel.read_text() == "synthetic unrelated payload"
    with store.transaction(actor) as tx:
        assert local_cleanup_proof(tx.conn, job) is None
        assert tx.conn.execute("SELECT body FROM ga_runtime_events").fetchone()["body"] != {}
        deferred = tx.conn.execute(
            "SELECT details FROM ga_audit_events WHERE action='memory_cleanup_attempt_deferred' AND resource_ref=%s",
            (job.id,),
        ).fetchone()["details"]
        assert deferred["pending_phase"] == "local" and deferred["local_proof_id"] is None
    assert_blocked(manager, actor)


async def test_no_op_database_redaction_cannot_sign_local_proof(
    store, actor, member, tmp_path, pg_bin, monkeypatch
):
    root, artifacts, _, _, workspace, inputs = seed(
        store, member, tmp_path, expired=True, channel=True
    )
    memory = MemoryService(store)
    memory.expire(actor, before=datetime.now(UTC) - timedelta(days=30))
    job = memory.pending_cleanup(actor)[0]
    handler = RuntimeCleanupHandler(store, artifacts, root, technical=actor)
    monkeypatch.setattr(handler, "_redact_rows", lambda *args: None)
    result = await CleanupRunner(memory, handler).run(actor)
    assert set(result.errors.values()) == {"version_conflict"}
    assert not workspace.exists() and not inputs.exists()
    with store.transaction(actor) as tx:
        assert local_cleanup_proof(tx.conn, job) is None
        assert tx.conn.execute("SELECT body FROM ga_runtime_events").fetchone()["body"] != {}
    assert_blocked(BackupManager(store, tmp_path / "backups", pg_bin), actor)


async def test_more_than_one_batch_of_unsupported_upstream_jobs_does_not_starve_local_purge(
    store, actor, member, tmp_path, pg_bin
):
    root = tmp_path / "runtime"
    root.mkdir()
    artifacts = ArtifactStore(root / "artifacts")
    stamp = datetime.now(UTC) - timedelta(days=40)
    inputs = []
    with store.transaction(member) as tx:
        for _ in range(107):
            identity = uuid4().hex
            event = GroupEvent(
                id=identity,
                source_key="synthetic-phase-batch:" + identity,
                group_id=member.group_id,
                actor_id=member.member_id,
                kind=EventKind.TEXT,
                text="synthetic raw source",
                occurred_at=stamp,
                received_at=stamp,
                platform_message_id="synthetic-platform:" + identity,
                channel_account_id="synthetic-account",
            )
            tx.ingest_event(event, identity, "1", 0)
            input_hash = hashlib.sha256(
                json.dumps(
                    [member.group_id, identity],
                    sort_keys=True,
                    ensure_ascii=False,
                    separators=(",", ":"),
                ).encode()
            ).hexdigest()
            path = root / "inputs" / ("input-" + input_hash)
            path.mkdir(parents=True)
            (path / "raw.bin").write_bytes(b"synthetic raw cache")
            inputs.append(path)
    memory = MemoryService(store)
    memory.expire(actor, before=datetime.now(UTC) - timedelta(days=30))
    runner = CleanupRunner(memory, RuntimeCleanupHandler(store, artifacts, root, technical=actor))
    first = await runner.run(actor, limit=100)
    assert len(first.failed) == 100 and not first.completed
    assert set(first.errors.values()) == {"upstream_cleanup_unsupported"}
    assert sum(path.exists() for path in inputs) == 7
    manager = BackupManager(store, tmp_path / "backups", pg_bin)
    assert_blocked(manager, actor)
    second = await runner.run(actor, limit=100)
    assert len(second.failed) == 100 and not second.completed
    assert set(second.errors.values()) == {"upstream_cleanup_unsupported"}
    assert not any(path.exists() for path in inputs)
    jobs = memory.pending_cleanup(actor, limit=1000)
    assert len(jobs) == 107 and all(job.state == "pending" for job in jobs)
    with store.transaction(actor) as tx:
        assert all(local_cleanup_proof(tx.conn, job) is not None for job in jobs)
        assert (
            tx.conn.execute(
                "SELECT count(*) AS n FROM ga_memory_cleanup WHERE resource_kind='backup_tombstone' AND state='pending'"
            ).fetchone()["n"]
            == 107
        )
    backup = manager.create(actor)
    assert manager.verify(actor, backup.id).archive_bytes > 0


async def test_overlapping_media_cleanup_preserves_proof_when_heap_order_changes(
    store, actor, member, tmp_path
):
    # PostgreSQL is free to choose a sequential scan. Updating an already
    # redacted sibling must not make a list with tied event_id ordering differ.
    store = PostgresStore(
        make_conninfo(store.dsn, options="-c enable_indexscan=off -c enable_bitmapscan=off")
    )
    root, artifacts, first, _, _, _ = seed(store, member, tmp_path, expired=True, channel=True)
    now = datetime.now(UTC)
    events = {}
    with store.transaction(member) as tx:
        for name in ("overlap", "media_owner"):
            identity = uuid4().hex
            stamp = now if name == "media_owner" else now - timedelta(days=40)
            event = GroupEvent(
                id=identity,
                source_key="synthetic-media-order:" + identity,
                group_id=member.group_id,
                actor_id=member.member_id,
                kind=EventKind.TEXT,
                text="synthetic source",
                occurred_at=stamp,
                received_at=stamp,
                platform_message_id="synthetic-platform:" + identity,
                channel_account_id="synthetic-account",
            )
            tx.ingest_event(event, identity, "1", 0)
            events[name] = event
        for attachment, sources in (
            ("first", [first.id, events["overlap"].id]),
            ("second", [first.id]),
        ):
            tx.conn.execute(
                "INSERT INTO ga_media_inputs(group_id,event_id,attachment_id,body,source_fingerprints) VALUES(%s,%s,%s,%s,%s)",
                (
                    actor.group_id,
                    events["media_owner"].id,
                    attachment,
                    Jsonb({"raw": "synthetic media payload"}),
                    Jsonb({source: "synthetic-fingerprint" for source in sources}),
                ),
            )
    memory = MemoryService(store)
    memory.expire(actor, before=now - timedelta(days=30))
    jobs = {job.event_id: job for job in memory.pending_cleanup(actor)}
    handler = RuntimeCleanupHandler(store, artifacts, root, technical=actor)
    with pytest.raises(DomainError) as caught:
        await handler.purge(jobs[first.id])
    assert caught.value.code == "upstream_cleanup_unsupported"
    with store.transaction(actor) as tx:
        assert local_cleanup_proof(tx.conn, jobs[first.id]) is not None
        before = tx.conn.execute("SELECT * FROM ga_media_inputs ORDER BY attachment_id").fetchall()
        before_heap = tx.conn.execute(
            "SELECT attachment_id FROM ga_media_inputs ORDER BY ctid"
        ).fetchall()
    with pytest.raises(DomainError) as caught:
        await handler.purge(jobs[events["overlap"].id])
    assert caught.value.code == "upstream_cleanup_unsupported"
    with store.transaction(actor) as tx:
        after = tx.conn.execute("SELECT * FROM ga_media_inputs ORDER BY attachment_id").fetchall()
        after_heap = tx.conn.execute(
            "SELECT attachment_id FROM ga_media_inputs ORDER BY ctid"
        ).fetchall()
        assert before == after and before_heap != after_heap
        assert local_cleanup_proof(tx.conn, jobs[first.id]) is not None
        assert local_cleanup_proof(tx.conn, jobs[events["overlap"].id]) is not None
