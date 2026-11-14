from __future__ import annotations

import subprocess
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

import pytest

from groupagent.domain import DeliveryState, DomainError, EventKind, GroupEvent
from groupagent.memory import MemoryService
from groupagent.ops import BackupManager, RestorationService
from groupagent.persistence import PostgresStore

pytestmark = pytest.mark.postgres


def seed(store, actor, *, reply=False):
    stamp = datetime.now(UTC)
    identity = uuid4().hex
    event = GroupEvent(
        id=identity,
        source_key="ops-source:" + identity,
        group_id=actor.group_id,
        actor_id=actor.member_id,
        kind=EventKind.TEXT,
        text="synthetic recoverable content",
        occurred_at=stamp,
        received_at=stamp,
    )
    with store.transaction(actor) as tx:
        tx.ingest_event(event, "stream", "1", 0)
        work = tx.create_work("synthetic goal", event.id)
        task = tx.create_team_task("synthetic human duty", source_event_id=event.id)
        intent = tx.enqueue_reply(event.id, "synthetic reply") if reply else None
        tx.conn.execute(
            "INSERT INTO ga_budget_policy(group_id,currency,daily_limit,total_limit,time_zone) VALUES (%s,'CNY',50,50,'Asia/Shanghai')",
            (actor.group_id,),
        )
    return event, work, task, intent


def test_real_consistent_dump_restore_checks_hashes_and_quarantines(
    store, actor, member, database_factory, pg_bin, tmp_path
):
    event, work, task, intent = seed(store, member, reply=True)
    manager = BackupManager(store, tmp_path / "backups", pg_bin)
    backup = manager.create(actor, build_ref="synthetic-test-build")
    manifest = manager.verify(actor, backup.id)
    assert manifest.tables["ga_events"].count == 1
    assert manifest.archive_bytes > 0 and len(manifest.archive_sha256) == 64
    assert (manager.root / backup.id / "archive.pgdump").stat().st_mode & 0o077 == 0
    target_dsn = database_factory()
    report = manager.restore(actor, backup.id, target_dsn)
    assert report.quarantined and report.verified_tables == len(manifest.tables)
    target = PostgresStore(target_dsn)
    with pytest.raises(DomainError) as caught:
        with target.transaction(member):
            pytest.fail("Restored business transaction was admitted before reconciliation")
    assert caught.value.code == "restore_pending"
    restoration = RestorationService(target)
    with restoration.maintenance(actor) as tx:
        assert tx.get_event(event.id).text == event.text
        assert tx.get_work(work.id).state.value == "paused"
        assert tx.get_team_task(task.id).owner_id == member.member_id
        assert tx.get_delivery(intent.id).state == DeliveryState.UNKNOWN
    with pytest.raises(DomainError) as caught:
        restoration.release(
            actor,
            report.id,
            evidence_ref="synthetic-reconciliation",
            privacy_verified=True,
            channel_verified=True,
        )
    assert caught.value.code == "delivery_unknown"
    restoration.reconcile_delivery(
        actor, intent.id, DeliveryState.REJECTED, evidence_ref="synthetic-conclusive-rejection"
    )
    restoration.release(
        actor,
        report.id,
        evidence_ref="synthetic-reviewed-cutover",
        privacy_verified=True,
        channel_verified=True,
    )
    with target.transaction(member) as tx:
        assert tx.get_work(work.id).state.value == "paused"
        budget = tx.conn.execute(
            "SELECT blocked FROM ga_budget_policy WHERE group_id=%s", (actor.group_id,)
        ).fetchone()
        assert budget is not None
        assert budget["blocked"] is True


def test_restore_overlays_newer_deletions_from_outside_archive(
    store, actor, member, database_factory, pg_bin, tmp_path
):
    event, work, _, _ = seed(store, member)
    manager = BackupManager(store, tmp_path / "backups", pg_bin)
    backup = manager.create(actor)
    MemoryService(store).forget(member, event_ids=[event.id])
    target_dsn = database_factory()
    report = manager.restore(actor, backup.id, target_dsn)
    assert report.applied_tombstones >= 1
    restored = RestorationService(PostgresStore(target_dsn))
    with restored.maintenance(actor) as tx:
        assert tx.get_event(event.id).content_state == "forgotten"
        assert tx.get_event(event.id).text == ""
        assert "synthetic goal" not in tx.get_work(work.id).goal
    assert manager.journal.load(actor.group_id)[0].reason == "forgotten"
    with pytest.raises(DomainError) as caught:
        restored.release(
            actor,
            report.id,
            evidence_ref="synthetic:not-clean-yet",
            privacy_verified=True,
            channel_verified=True,
        )
    assert caught.value.code == "privacy_cleanup_pending"


def test_offline_restore_uses_managed_manifest_and_external_journal(
    store, actor, member, database_factory, pg_bin, tmp_path, monkeypatch
):
    event, _, task, _ = seed(store, member)
    manager = BackupManager(store, tmp_path / "backups", pg_bin)
    backup = manager.create(actor)
    MemoryService(store).forget(member, event_ids=[event.id])
    manager.journal.sync(store, actor)
    target_dsn = database_factory()

    def unavailable():
        raise RuntimeError("source database unavailable")

    monkeypatch.setattr(store, "connect", unavailable)
    report = manager.restore(actor, backup.id, target_dsn, offline=True)
    assert report.quarantined and report.applied_tombstones == 2  # Event and durable Task identity.
    with RestorationService(PostgresStore(target_dsn)).maintenance(actor) as tx:
        assert tx.get_event(event.id).text == ""
        assert tx.get_team_task(task.id).content_state == "forgotten"


def test_nonempty_target_is_never_overwritten(
    store, actor, member, database_factory, pg_bin, tmp_path
):
    seed(store, member)
    manager = BackupManager(store, tmp_path / "backups", pg_bin)
    backup = manager.create(actor)
    target = PostgresStore(database_factory())
    with target.connect() as conn:
        conn.execute("CREATE TABLE preserved(value text)")
        conn.execute("INSERT INTO preserved VALUES ('do not overwrite')")
    with pytest.raises(DomainError) as caught:
        manager.restore(actor, backup.id, target.dsn)
    assert caught.value.code == "target_not_empty"
    with target.connect() as conn:
        preserved = conn.execute("SELECT value FROM preserved").fetchone()
        assert preserved is not None and preserved["value"] == "do not overwrite"


def test_corrupt_archive_fails_before_target_mutation(
    store, actor, member, database_factory, pg_bin, tmp_path
):
    seed(store, member)
    manager = BackupManager(store, tmp_path / "backups", pg_bin)
    backup = manager.create(actor)
    archive = manager.root / backup.id / "archive.pgdump"
    with archive.open("ab") as archive_output:
        archive_output.write(b"corrupt")
    target = PostgresStore(database_factory())
    with pytest.raises(DomainError) as caught:
        manager.restore(actor, backup.id, target.dsn)
    assert caught.value.code == "invalid_backup"
    with target.connect() as conn:
        result = conn.execute("SELECT to_regclass('public.ga_groups') AS found").fetchone()
        assert result is not None and result["found"] is None


def test_failed_pg_restore_leaves_persistent_quarantine(
    store, actor, member, database_factory, pg_bin, tmp_path, monkeypatch
):
    seed(store, member)
    manager = BackupManager(store, tmp_path / "backups", pg_bin)
    backup = manager.create(actor)
    original = manager._run

    def failure(name, args, env):
        if name == "pg_restore":
            raise DomainError("database_tool_failed", "synthetic restore failure")
        return original(name, args, env)

    monkeypatch.setattr(manager, "_run", failure)
    target = PostgresStore(database_factory())
    with pytest.raises(DomainError):
        manager.restore(actor, backup.id, target.dsn)
    with target.connect() as conn:
        guard = conn.execute("SELECT blocked FROM groupagent_recovery.guard").fetchone()
        assert guard is not None and guard["blocked"]
    with pytest.raises(DomainError) as caught:
        with target.transaction(member):
            pass
    assert caught.value.code == "restore_pending"
    status = RestorationService(target).status(actor)
    assert status["schema_available"] is False and status["guard"]["blocked"]


def test_expired_backup_is_removed_after_policy_deadline(store, actor, member, pg_bin, tmp_path):
    seed(store, member)
    manager = BackupManager(store, tmp_path / "backups", pg_bin)
    stamp = datetime.now(UTC)
    backup = manager.create(actor, now=stamp)
    assert manager.expire(actor, now=stamp + timedelta(days=6)) == []
    assert manager.expire(actor, now=stamp + timedelta(days=8)) == [backup.id]
    assert not (manager.root / backup.id).exists()


def test_credentials_never_enter_arguments_or_diagnostics(
    store, actor, pg_bin, tmp_path, monkeypatch
):
    manager = BackupManager(store, tmp_path / "backups", pg_bin)
    secret = "synthetic-secret:with\\slash"
    seen = []

    def fake(args, **kwargs):
        seen.append((args, kwargs))
        assert secret not in " ".join(args)
        assert "PGPASSWORD" not in kwargs["env"]
        password_file = Path(kwargs["env"]["PGPASSFILE"])
        assert password_file.stat().st_mode & 0o077 == 0
        return subprocess.CompletedProcess(args, 2, stdout=secret, stderr=secret)

    monkeypatch.setattr(subprocess, "run", fake)
    from psycopg.conninfo import make_conninfo

    with manager._environment(
        make_conninfo(host="localhost", dbname="synthetic", password=secret)
    ) as env:
        path = Path(env["PGPASSFILE"])
        with pytest.raises(DomainError) as caught:
            manager._run("pg_dump", ["--no-password"], env)
        assert secret not in str(caught.value)
    assert not path.exists() and len(seen) == 1


def test_backup_rejects_other_groups_by_default(store, actor, member, pg_bin, tmp_path):
    seed(store, member)
    store.register_group("foreign-group")
    manager = BackupManager(store, tmp_path / "backups", pg_bin)
    with pytest.raises(DomainError) as caught:
        manager.create(actor)
    assert caught.value.code == "scope_violation"
    assert not list(manager.root.glob("*/archive.pgdump"))


def test_regular_member_cannot_create_backup(store, member, pg_bin, tmp_path):
    manager = BackupManager(store, tmp_path / "backups", pg_bin)
    with pytest.raises(DomainError) as caught:
        manager.create(member)
    assert caught.value.code == "permission_denied"
