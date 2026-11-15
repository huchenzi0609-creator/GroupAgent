from __future__ import annotations

import pytest

from groupagent.domain import DomainError, Role
from groupagent.memory import MemoryService
from groupagent.ops import BackupManager, LegacyImporter, RestorationService
from groupagent.persistence import PostgresStore

from .test_legacy import legacy

__all__ = ["legacy"]
pytestmark = pytest.mark.postgres


@pytest.mark.parametrize("archive_before_import", [False, True])
def test_external_task_tombstone_overlays_real_backup_and_blocks_legacy_reimport(
    store, actor, legacy, database_factory, pg_bin, tmp_path, archive_before_import
):
    manager = BackupManager(store, tmp_path / "backups", pg_bin)
    importer = LegacyImporter(store)
    if archive_before_import:
        backup = manager.create(actor)
    plan = importer.plan(actor, legacy, "legacy-group")
    importer.apply(actor, legacy, plan.id, expected_source_hash=plan.source_sha256)
    task = plan.tasks[0]
    if not archive_before_import:
        backup = manager.create(actor)
    owner = actor.model_copy(update={"role": Role.OWNER, "member_id": "synthetic-owner"})
    memory = MemoryService(store, deletion_sink=manager.journal)
    memory.forget(owner, task_ids=[task.target_id])
    assert any(marker.task_id == task.target_id for marker in manager.journal.load(actor.group_id))
    with store.transaction(actor) as tx:
        review = tx.conn.execute(
            "SELECT body FROM ga_legacy_review WHERE group_id=%s AND kind='reminder'",
            (actor.group_id,),
        ).fetchone()
        assert review is not None and review["body"] == {}
    target_dsn = database_factory()
    report = manager.restore(actor, backup.id, target_dsn)
    target = PostgresStore(target_dsn)
    recovery = RestorationService(target)
    with recovery.maintenance(actor) as tx:
        restored = tx.get_team_task(task.target_id)
        assert restored.content_state == "forgotten" and restored.title == "[task removed]"
        assert task.target_id not in {item.id for item in tx.list_team_tasks()}
        assert tx.conn.execute(
            "SELECT 1 FROM ga_task_tombstones WHERE group_id=%s AND task_id=%s",
            (actor.group_id, task.target_id),
        ).fetchone()
        # Exercise importer replay under an explicitly released synthetic recovery
        # guard; no app process, sender, or provider is running against this DB.
        tx.conn.execute("UPDATE groupagent_recovery.guard SET blocked=false WHERE singleton")
        tx.conn.execute(
            "UPDATE ga_restore_guards SET blocked=false WHERE group_id=%s", (actor.group_id,)
        )
    replay = LegacyImporter(target)
    refreshed = replay.plan(actor, legacy, "legacy-group")
    if archive_before_import:
        with pytest.raises(DomainError) as caught:
            replay.apply(actor, legacy, refreshed.id, expected_source_hash=refreshed.source_sha256)
        assert caught.value.code == "migration_conflict"
    else:
        assert replay.apply(
            actor, legacy, refreshed.id, expected_source_hash=refreshed.source_sha256
        ).already_applied
    with target.transaction(actor) as tx:
        assert tx.get_team_task(task.target_id).content_state == "forgotten"
    assert report.quarantined
