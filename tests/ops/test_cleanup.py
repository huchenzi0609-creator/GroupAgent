from __future__ import annotations

from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest

from groupagent.domain import DomainError, EventKind, GroupEvent
from groupagent.memory import MemoryService
from groupagent.ops import CleanupRunner, DeletionJournal, DeletionMarker

pytestmark = pytest.mark.postgres


def source(store, member):
    stamp = datetime.now(UTC)
    identity = uuid4().hex
    event = GroupEvent(
        id=identity,
        source_key="cleanup:" + identity,
        group_id=member.group_id,
        actor_id=member.member_id,
        kind=EventKind.TEXT,
        text="synthetic delete marker",
        occurred_at=stamp,
        received_at=stamp,
    )
    with store.transaction(member) as tx:
        tx.ingest_event(event, "cleanup", "1", 0)
    return event


async def test_cleanup_acks_only_actual_callback_receipts_and_syncs_journal(
    store, actor, member, tmp_path
):
    event = source(store, member)
    memory = MemoryService(store)
    memory.forget(member, event_ids=[event.id])
    journal = DeletionJournal(tmp_path / "deletions")
    removed = tmp_path / "synthetic-payload"
    removed.write_text("synthetic bytes", encoding="utf-8")

    async def purge(job):
        assert job.group_id == actor.group_id and job.event_id == event.id
        removed.unlink(missing_ok=True)
        return "synthetic-filesystem-removal:" + job.id

    result = await CleanupRunner(memory, purge, journal=journal).run(actor)
    assert result.completed and not result.failed and not removed.exists()
    assert (
        result.tombstones_synced == 1
        and journal.load(actor.group_id)[0].source_key == event.source_key
    )
    assert memory.pending_cleanup(actor) == []  # Backup job remains delayed, not falsely completed.


@pytest.mark.parametrize("outcome", ["failure", "no_evidence"])
async def test_cleanup_failures_remain_pending(store, actor, member, outcome):
    event = source(store, member)
    memory = MemoryService(store)
    memory.forget(member, event_ids=[event.id])

    async def purge(job):
        if outcome == "failure":
            raise RuntimeError("synthetic deletion failure")
        return ""

    result = await CleanupRunner(memory, purge).run(actor)
    assert not result.completed and result.failed
    assert memory.pending_cleanup(actor)


def test_journal_strongest_tombstone_survives_stale_writer(tmp_path, actor):
    journal = DeletionJournal(tmp_path / "journal")
    marker = DeletionMarker(
        group_id=actor.group_id,
        source_key="event:key",
        event_id="event",
        platform_message_id=None,
        channel_account_id=None,
        reason="forgotten",
    )
    journal.append([marker])
    journal.append([marker.model_copy(update={"reason": "expired"})])
    assert journal.load(actor.group_id) == [marker]
    assert journal.append([marker]) == 0


def test_corrupt_or_partial_deletion_journal_blocks_recovery(tmp_path, actor):
    journal = DeletionJournal(tmp_path / "journal")
    marker = DeletionMarker(
        group_id=actor.group_id,
        source_key="key",
        event_id="id",
        platform_message_id=None,
        channel_account_id=None,
        reason="forgotten",
    )
    journal.append([marker])
    with journal._path(actor.group_id).open("ab") as target:
        target.write(b'{"incomplete":')
    with pytest.raises(DomainError) as caught:
        journal.load(actor.group_id)
    assert caught.value.code == "invalid_deletion_journal"


def test_backup_cleanup_job_is_not_due_before_retention_deadline(store, actor, member):
    event = source(store, member)
    memory = MemoryService(store)
    stamp = datetime.now(UTC)
    result = memory.forget(member, event_ids=[event.id], now=stamp)
    assert all(
        job.resource_kind != "backup_tombstone"
        for job in memory.pending_cleanup(actor, now=stamp + timedelta(days=6))
    )
    assert any(
        job.resource_kind == "backup_tombstone"
        for job in memory.pending_cleanup(actor, now=result.backups_delete_after)
    )


async def test_delayed_backup_job_cannot_be_acknowledged_by_generic_callback(store, actor, member):
    event = source(store, member)
    memory = MemoryService(store)
    stamp = datetime.now(UTC)
    forgotten = memory.forget(member, event_ids=[event.id], now=stamp)
    called = []

    async def generic(job):
        called.append(job.resource_kind)
        return "synthetic:generic"

    result = await CleanupRunner(memory, generic).run(actor, now=forgotten.backups_delete_after)
    assert "backup_tombstone" not in called
    assert "dependency_unavailable" in result.errors.values()


def test_pending_derived_cleanup_blocks_new_backup(store, actor, member, pg_bin, tmp_path):
    from groupagent.ops import BackupManager

    event = source(store, member)
    memory = MemoryService(store)
    memory.forget(member, event_ids=[event.id])
    manager = BackupManager(store, tmp_path / "backups", pg_bin)
    with pytest.raises(DomainError) as caught:
        manager.create(actor)
    assert caught.value.code == "privacy_cleanup_pending"
    assert not list(manager.root.glob("*/archive.pgdump"))
