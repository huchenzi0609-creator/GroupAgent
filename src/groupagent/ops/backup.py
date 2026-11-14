"""Consistent PostgreSQL archives, verified empty-target restore and quarantine.

Only managed archives produced from a trusted application database are accepted.
Credentials are supplied through a private temporary libpq password file, never
command arguments, manifests, captured diagnostics or application logs.
"""

from __future__ import annotations

import hashlib
import os
import re
import shutil
import subprocess
import tempfile
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import timedelta
from pathlib import Path
from uuid import uuid4

import psycopg
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict, make_conninfo
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from groupagent.domain import ActorContext, DomainError, Role
from groupagent.memory import MemoryService
from groupagent.memory.common import memory_lock
from groupagent.ops.common import (
    RESTORE_LOCK_KEY,
    atomic_json,
    clock,
    digest,
    file_hash,
    managed_directory,
    private_root,
    require_ops,
)
from groupagent.ops.journal import DeletionJournal
from groupagent.ops.models import (
    BackupManifest,
    BackupRecord,
    RestoreReport,
    TableDigest,
)
from groupagent.persistence import PostgresStore, UnitOfWork

_ENV_FIELDS = {
    "host": "PGHOST",
    "hostaddr": "PGHOSTADDR",
    "port": "PGPORT",
    "dbname": "PGDATABASE",
    "user": "PGUSER",
    "sslmode": "PGSSLMODE",
    "sslrootcert": "PGSSLROOTCERT",
    "sslcert": "PGSSLCERT",
    "sslkey": "PGSSLKEY",
    "connect_timeout": "PGCONNECT_TIMEOUT",
    "options": "PGOPTIONS",
    "application_name": "PGAPPNAME",
    "client_encoding": "PGCLIENTENCODING",
}


class BackupManager:
    def __init__(
        self,
        store: PostgresStore,
        root: Path,
        pg_bin_dir: Path | None = None,
        *,
        authorized_group_ids: set[str] | None = None,
        retention_days: int = 7,
        timeout_seconds: int = 180,
        max_bytes: int = 512 * 1024**2,
    ) -> None:
        self.store = store
        self.root = private_root(root)
        self.journal = DeletionJournal(self.root / "deletions")
        binary = shutil.which("pg_dump")
        self.bin_dir = (
            pg_bin_dir.resolve()
            if pg_bin_dir
            else (Path(binary).resolve().parent if binary else None)
        )
        if self.bin_dir is None or any(
            not (self.bin_dir / name).is_file() for name in ("pg_dump", "pg_restore")
        ):
            raise DomainError("dependency_unavailable", "PostgreSQL client binaries are required")
        if not 1 <= retention_days <= 7 or not 1 <= timeout_seconds <= 1800 or max_bytes <= 0:
            raise DomainError("invalid_input", "Backup limits are invalid")
        self.authorized_groups = authorized_group_ids
        self.retention = timedelta(days=retention_days)
        self.timeout = timeout_seconds
        self.max_bytes = max_bytes

    @staticmethod
    def _identity(conn) -> str:
        return digest(
            {"database": conn.info.dbname, "host": conn.info.host, "port": conn.info.port}
        )

    def _scope(self, actor: ActorContext, groups: list[str]) -> None:
        require_ops(actor)
        allowed = self.authorized_groups if self.authorized_groups is not None else {actor.group_id}
        if not set(groups).issubset(allowed) or (
            set(groups) - {actor.group_id} and actor.role != Role.MAINTAINER
        ):
            raise DomainError(
                "scope_violation", "Backup contains groups outside the configured maintenance scope"
            )

    @contextmanager
    def _environment(self, dsn: str, *, effective: dict | None = None) -> Iterator[dict[str, str]]:
        params = conninfo_to_dict(dsn)
        if set(params) - set(_ENV_FIELDS) - {"password", "passfile"}:
            raise DomainError(
                "unsupported_database_config",
                "Backup needs an explicit supported libpq connection configuration",
            )
        env = {
            "PATH": os.defpath,
            "LC_ALL": "C",
            "PGAPPNAME": "groupagent-operations",
            "PGCONNECT_TIMEOUT": "10",
        }
        for key, value in {**(effective or {}), **params}.items():
            if key in _ENV_FIELDS:
                env[_ENV_FIELDS[key]] = str(value)
        with tempfile.TemporaryDirectory(prefix=".auth-", dir=self.root) as directory:
            password = str(params["password"]) if params.get("password") is not None else ""
            if "\n" in password or "\r" in password or "\0" in password:
                raise DomainError(
                    "unsupported_database_config",
                    "Password-file credentials cannot contain line separators",
                )
            auth = Path(directory) / "pgpass"
            descriptor = os.open(auth, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            with os.fdopen(descriptor, "w", encoding="utf-8") as target:
                if password:
                    target.write(
                        "*:*:*:*:" + password.replace("\\", "\\\\").replace(":", "\\:") + "\n"
                    )
            env["PGPASSFILE"] = str(params["passfile"]) if params.get("passfile") else str(auth)
            # An empty private file also prevents unintended ~/.pgpass lookup.
            yield env

    def _run(self, name: str, args: list[str], env: dict[str, str]) -> str:
        assert self.bin_dir is not None
        try:
            result = subprocess.run(
                [str(self.bin_dir / name), *args],
                env=env,
                stdin=subprocess.DEVNULL,
                capture_output=True,
                text=True,
                timeout=self.timeout,
                check=False,
            )
        except (subprocess.TimeoutExpired, OSError):
            raise DomainError(
                "database_tool_unavailable", "Database operation timed out or could not start"
            ) from None
        if result.returncode:
            raise DomainError(
                "database_tool_failed",
                f"Database operation failed with exit code {result.returncode}",
            ) from None
        return result.stdout

    def _version(self) -> str:
        value = self._run("pg_dump", ["--version"], {"PATH": os.defpath, "LC_ALL": "C"})
        match = re.match(r"\Apg_dump \(PostgreSQL\) ([0-9]+(?:\.[0-9]+){1,2})(?:\s|$)", value)
        if match is None:
            raise DomainError(
                "dependency_unavailable", "PostgreSQL client version was not recognized"
            )
        return match.group(1)

    def _tables(self, conn) -> dict[str, TableDigest]:
        names = conn.execute(
            "SELECT schemaname,tablename FROM pg_tables WHERE schemaname NOT LIKE 'pg_%' AND schemaname<>'information_schema' ORDER BY schemaname,tablename"
        ).fetchall()
        names = [
            row
            for row in names
            if not (row["schemaname"] == "groupagent_recovery" and row["tablename"] == "guard")
        ]
        if any(
            row["schemaname"] != "public" or not row["tablename"].startswith("ga_") for row in names
        ):
            raise DomainError(
                "unsupported_database_layout",
                "Only a dedicated GroupAgent public schema can be backed up",
            )
        result = {}
        total = 0
        for item in names:
            table = item["tablename"]
            checksum = hashlib.sha256()
            count = 0
            with conn.cursor(name="digest_" + uuid4().hex) as cursor:
                cursor.execute(
                    sql.SQL(
                        'SELECT to_jsonb(t)::text AS value FROM {} t ORDER BY to_jsonb(t)::text COLLATE "C"'
                    ).format(sql.Identifier("public", table))
                )
                for row in cursor:
                    encoded = row["value"].encode() + b"\n"
                    total += len(encoded)
                    if total > self.max_bytes * 4:
                        raise DomainError(
                            "quota_exceeded", "Database snapshot exceeds the verification bound"
                        )
                    checksum.update(encoded)
                    count += 1
            result[table] = TableDigest(count=count, sha256=checksum.hexdigest())
        return result

    @contextmanager
    def _snapshot(self, actor: ActorContext):
        """Freeze a read snapshot after queued erasures, without holding their lock during dump."""
        with self.store.connect() as conn:
            conn.autocommit = True
            locks = sorted(
                self.authorized_groups if self.authorized_groups is not None else {actor.group_id}
            )
            held = []
            try:
                conn.execute(
                    "SELECT set_config('statement_timeout',%s,false)", (str(self.timeout * 1000),)
                )
                for group in locks:
                    conn.execute(
                        "SELECT pg_advisory_lock(hashtextextended(%s,0))", ("memory:" + group,)
                    )
                    held.append(group)
                conn.autocommit = False
                conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
                conn.execute("SET LOCAL TIME ZONE 'UTC'")
                groups = [
                    row["id"]
                    for row in conn.execute("SELECT id FROM ga_groups ORDER BY id").fetchall()
                ]
                self._scope(actor, groups)
                pending = conn.execute(
                    "SELECT 1 FROM ga_memory_cleanup WHERE group_id=ANY(%s) AND state='pending' AND resource_kind<>'backup_tombstone' LIMIT 1",
                    (groups,),
                ).fetchone()
                if pending:
                    raise DomainError(
                        "privacy_cleanup_pending",
                        "Finish source-derived cleanup before creating a fresh archive",
                    )
                snapshot = conn.execute("SELECT pg_export_snapshot() AS id").fetchone()
                assert snapshot is not None
                for group in held:
                    conn.execute(
                        "SELECT pg_advisory_unlock(hashtextextended(%s,0))", ("memory:" + group,)
                    )
                held.clear()
                yield conn, groups, snapshot["id"]
            finally:
                if held:
                    conn.rollback()
                    conn.autocommit = True
                    for group in held:
                        conn.execute(
                            "SELECT pg_advisory_unlock(hashtextextended(%s,0))",
                            ("memory:" + group,),
                        )

    def create(
        self, actor: ActorContext, *, now=None, build_ref: str = "local-unreleased"
    ) -> BackupRecord:
        require_ops(actor)
        stamp = clock(now)
        identifier = uuid4().hex
        folder = managed_directory(self.root, identifier, create=True)
        partial = folder / "archive.pending"
        archive = folder / "archive.pgdump"
        try:
            with self.store.transaction(actor) as tx:
                tx.conn.execute(
                    "INSERT INTO ga_ops_backups(id,group_id,state,expires_at,created_by) VALUES (%s,%s,'creating',%s,%s)",
                    (identifier, actor.group_id, stamp + self.retention, actor.member_id),
                )
            self.journal.sync(self.store, actor)
            with self._snapshot(actor) as (conn, groups, snapshot):
                tables = self._tables(conn)
                schemas = {
                    str(row["version"]): row["sha256"]
                    for row in conn.execute(
                        "SELECT version,sha256 FROM ga_schema_migrations ORDER BY version"
                    ).fetchall()
                }
                effective = {
                    "host": conn.info.host,
                    "port": conn.info.port,
                    "dbname": conn.info.dbname,
                    "user": conn.info.user,
                }
                descriptor = os.open(partial, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
                os.close(descriptor)
                with self._environment(self.store.dsn, effective=effective) as env:
                    self._run(
                        "pg_dump",
                        [
                            "--no-password",
                            "--format=custom",
                            "--no-owner",
                            "--no-acl",
                            "--no-tablespaces",
                            "--exclude-schema=groupagent_recovery",
                            "--snapshot=" + snapshot,
                            "--file=" + str(partial),
                        ],
                        env,
                    )
                checksum, size = file_hash(partial, max_bytes=self.max_bytes)
                manifest = BackupManifest(
                    id=identifier,
                    group_ids=groups,
                    created_at=stamp,
                    expires_at=stamp + self.retention,
                    source_identity=self._identity(conn),
                    build_ref=build_ref,
                    postgres_version=str(conn.info.server_version),
                    pg_dump_version=self._version(),
                    archive_sha256=checksum,
                    archive_bytes=size,
                    schema_versions=schemas,
                    tables=tables,
                )
            with partial.open("rb") as completed:
                os.fsync(completed.fileno())
            partial.replace(archive)
            atomic_json(folder / "manifest.json", manifest.model_dump(mode="json"))
            with self.store.transaction(actor) as tx:
                tx.conn.execute(
                    "UPDATE ga_ops_backups SET state='complete',manifest=%s WHERE id=%s AND group_id=%s",
                    (Jsonb(manifest.model_dump(mode="json")), identifier, actor.group_id),
                )
                tx._audit("backup_created", identifier, archive_sha256=checksum, archive_bytes=size)
            return BackupRecord(
                id=identifier,
                state="complete",
                manifest=manifest,
                archive_ref="backup:" + identifier,
            )
        except BaseException:
            # A fully sealed archive remains useful if the catalog database went
            # down at the final write. Never discard that last good backup.
            if not (folder / "manifest.json").exists():
                shutil.rmtree(folder)
            try:
                with self.store.connect() as conn:
                    conn.execute(
                        "UPDATE ga_ops_backups SET state='failed',error_code='backup_incomplete' WHERE id=%s AND group_id=%s",
                        (identifier, actor.group_id),
                    )
            except psycopg.Error:
                pass
            raise

    def verify(
        self, actor: ActorContext, backup_id: str, *, offline: bool = False
    ) -> BackupManifest:
        require_ops(actor)
        if offline and actor.role != Role.MAINTAINER:
            raise DomainError(
                "permission_denied", "Offline recovery requires a maintenance identity"
            )
        if not offline:
            with self.store.transaction(actor) as tx:
                row = tx.conn.execute(
                    "SELECT state FROM ga_ops_backups WHERE group_id=%s AND id=%s",
                    (actor.group_id, backup_id),
                ).fetchone()
                if row is None or row["state"] not in {"complete", "failed"}:
                    raise DomainError("not_found", "Complete backup was not found in this scope")
        folder = managed_directory(self.root, backup_id)
        path = folder / "manifest.json"
        file_hash(path, max_bytes=1024 * 1024)
        try:
            manifest = BackupManifest.model_validate_json(path.read_text(encoding="utf-8"))
        except ValueError:
            raise DomainError("invalid_backup", "Backup manifest is invalid") from None
        if manifest.id != backup_id:
            raise DomainError("invalid_backup", "Backup manifest identity mismatch")
        self._scope(actor, manifest.group_ids)
        checksum, size = file_hash(folder / "archive.pgdump", max_bytes=self.max_bytes)
        if (checksum, size) != (manifest.archive_sha256, manifest.archive_bytes):
            raise DomainError("invalid_backup", "Backup archive checksum or size does not match")
        return manifest

    @staticmethod
    def _empty_target(conn) -> None:
        count = conn.execute(
            "SELECT count(*) AS n FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname NOT LIKE 'pg_%' AND n.nspname<>'information_schema' AND c.relkind IN ('r','p','v','m','S','f')"
        ).fetchone()["n"]
        if count:
            raise DomainError(
                "target_not_empty", "Restore refuses to overwrite any nonempty target database"
            )
        objects = conn.execute(
            "SELECT (SELECT count(*) FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname NOT LIKE 'pg_%' AND n.nspname<>'information_schema') + (SELECT count(*) FROM pg_type t JOIN pg_namespace n ON n.oid=t.typnamespace WHERE n.nspname NOT LIKE 'pg_%' AND n.nspname<>'information_schema' AND t.typtype IN ('d','e')) AS n"
        ).fetchone()["n"]
        if objects:
            raise DomainError(
                "target_not_empty", "Restore refuses an existing user-defined schema object"
            )

    def restore(
        self,
        actor: ActorContext,
        backup_id: str,
        target_dsn: str,
        *,
        now=None,
        offline: bool = False,
    ) -> RestoreReport:
        manifest = self.verify(actor, backup_id, offline=offline)
        stamp = clock(now)
        if stamp > manifest.expires_at:
            raise DomainError(
                "backup_expired",
                "Backup retention period has elapsed; this archive cannot be activated",
            )
        if not offline:
            for group in manifest.group_ids:
                if group != actor.group_id:
                    raise DomainError(
                        "scope_violation",
                        "Multi-group restore needs separate per-group deletion journal synchronization",
                    )
                self.journal.sync(self.store, actor)
        markers = [marker for group in manifest.group_ids for marker in self.journal.load(group)]
        identifier = uuid4().hex
        with psycopg.connect(target_dsn, row_factory=dict_row) as target:
            lock = target.execute(
                "SELECT pg_try_advisory_lock(%s) AS held", (RESTORE_LOCK_KEY,)
            ).fetchone()
            if not lock or not lock["held"]:
                raise DomainError("restore_busy", "Another session is using the restore boundary")
            try:
                self._empty_target(target)
                target_identity = self._identity(target)
                if target_identity == manifest.source_identity:
                    raise DomainError(
                        "scope_violation", "Restore target must be a separate empty database"
                    )
                other_row = target.execute(
                    "SELECT count(*) AS n FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid()",
                    (),
                ).fetchone()
                assert other_row is not None
                other = other_row["n"]
                if other:
                    raise DomainError(
                        "restore_busy", "Restore target must have no other active clients"
                    )
                target.execute("CREATE SCHEMA groupagent_recovery")
                target.execute(
                    "CREATE TABLE groupagent_recovery.guard(singleton boolean PRIMARY KEY DEFAULT true CHECK(singleton),blocked boolean NOT NULL,restore_id text NOT NULL,evidence_ref text,updated_at timestamptz NOT NULL DEFAULT clock_timestamp())"
                )
                target.execute(
                    "INSERT INTO groupagent_recovery.guard(singleton,blocked,restore_id) VALUES (true,true,%s)",
                    (identifier,),
                )
                target.commit()
                effective = {
                    "host": target.info.host,
                    "port": target.info.port,
                    "dbname": target.info.dbname,
                    "user": target.info.user,
                }
                with self._environment(target_dsn, effective=effective) as env:
                    self._run(
                        "pg_restore",
                        [
                            "--no-password",
                            "--single-transaction",
                            "--exit-on-error",
                            "--no-owner",
                            "--no-acl",
                            "--no-tablespaces",
                            "--dbname=" + make_conninfo(dbname=target.info.dbname),
                            str(managed_directory(self.root, backup_id) / "archive.pgdump"),
                        ],
                        env,
                    )
                target.execute("SET LOCAL TIME ZONE 'UTC'")
                restored = self._tables(target)
                if restored != manifest.tables:
                    raise DomainError(
                        "restore_verification_failed",
                        "Restored table counts or hashes do not match the consistent snapshot",
                    )
                task_schema = target.execute(
                    "SELECT to_regclass('ga_task_tombstones') AS present"
                ).fetchone()
                if any(
                    marker.task_id or marker.reason in {"forgotten", "recalled"}
                    for marker in markers
                ) and (task_schema is None or task_schema["present"] is None):
                    raise DomainError(
                        "restore_schema_incompatible",
                        "Apply a compatible task provenance recovery build before overlaying deletion records",
                    )
                for group in manifest.group_ids:
                    target.execute(
                        "INSERT INTO ga_restore_guards(group_id,restore_id,blocked) VALUES (%s,%s,true) ON CONFLICT(group_id) DO UPDATE SET restore_id=EXCLUDED.restore_id,blocked=true,evidence_ref=NULL,released_at=NULL",
                        (group, identifier),
                    )
                target.commit()
                # Overlay deletion knowledge newer than the archive before any
                # ordinary application transaction may consume restored content.
                for group in manifest.group_ids:
                    revision_row = target.execute(
                        "SELECT policy_revision FROM ga_groups WHERE id=%s", (group,)
                    ).fetchone()
                    assert revision_row is not None
                    revision = revision_row["policy_revision"]
                    technical = ActorContext(
                        group_id=group,
                        member_id=actor.member_id,
                        role=Role.MAINTAINER,
                        policy_revision=revision,
                    )
                    unit = UnitOfWork(target, technical, "restore:" + identifier)
                    memory_lock(unit)
                    for marker in (item for item in markers if item.group_id == group):
                        if marker.task_id:
                            task_schema = target.execute(
                                "SELECT to_regclass('ga_task_tombstones') AS present"
                            ).fetchone()
                            if task_schema is None or task_schema["present"] is None:
                                raise DomainError(
                                    "restore_schema_incompatible",
                                    "Task deletion journal requires task provenance migration before release",
                                )
                            MemoryService(PostgresStore(target_dsn))._purge(
                                unit,
                                [],
                                [],
                                reason="forgotten",
                                now=stamp,
                                forget_facts=True,
                                task_ids=[marker.task_id],
                            )
                            continue
                        target.execute(
                            "INSERT INTO ga_source_tombstones(group_id,source_key,event_id,platform_message_id,channel_account_id,reason) VALUES (%s,%s,%s,%s,%s,%s) ON CONFLICT(group_id,source_key) DO UPDATE SET reason=EXCLUDED.reason",
                            (
                                group,
                                marker.source_key,
                                marker.event_id,
                                marker.platform_message_id,
                                marker.channel_account_id,
                                marker.reason,
                            ),
                        )
                        rows = target.execute(
                            "SELECT * FROM ga_events WHERE group_id=%s AND (source_key=%s OR id=%s OR (body->>'platform_message_id'=%s AND body->>'channel_account_id' IS NOT DISTINCT FROM %s)) FOR UPDATE",
                            (
                                group,
                                marker.source_key,
                                marker.event_id,
                                marker.platform_message_id,
                                marker.channel_account_id,
                            ),
                        ).fetchall()
                        MemoryService(PostgresStore(target_dsn))._purge(
                            unit,
                            rows,
                            [],
                            reason=marker.reason,
                            now=stamp,
                            forget_facts=marker.reason != "expired",
                            referenced_event_ids=[marker.event_id] if marker.event_id else [],
                        )
                    target.execute(
                        "UPDATE ga_work_orders SET state='paused',pause_reason='restore_reconciliation_required',lease_owner=NULL,lease_expires_at=NULL,revision=revision+1 WHERE group_id=%s AND state NOT IN ('completed','cancelled','failed')",
                        (group,),
                    )
                    target.execute(
                        "UPDATE ga_execution_attempts SET state='revoked' WHERE group_id=%s AND state='active'",
                        (group,),
                    )
                    target.execute(
                        "UPDATE ga_deliveries SET state='unknown',reason='restore_reconciliation_required',claim_expires_at=NULL,revision=revision+1 WHERE group_id=%s AND state IN ('pending','dispatching')",
                        (group,),
                    )
                    target.execute(
                        "UPDATE ga_schedules SET state='paused',pause_reason='restore_reconciliation_required',revision=revision+1 WHERE group_id=%s AND state='active'",
                        (group,),
                    )
                    target.execute(
                        "UPDATE ga_trigger_rules SET state='paused',revision=revision+1 WHERE group_id=%s AND state='active'",
                        (group,),
                    )
                    target.execute(
                        "UPDATE ga_budget_policy SET blocked=true,revision=revision+1 WHERE group_id=%s",
                        (group,),
                    )
                    target.execute(
                        "UPDATE ga_budget_reservations SET state='unknown',reason='restore_reconciliation_required' WHERE group_id=%s AND state IN ('reserved','inflight')",
                        (group,),
                    )
                    target.execute(
                        "UPDATE ga_runtime_checkpoints SET invalidated=true WHERE group_id=%s",
                        (group,),
                    )
                report = RestoreReport(
                    id=identifier,
                    backup_id=backup_id,
                    target_identity=target_identity,
                    group_ids=manifest.group_ids,
                    verified_tables=len(restored),
                    applied_tombstones=len(markers),
                    backup_created_at=manifest.created_at,
                    restored_at=stamp,
                )
                for group in manifest.group_ids:
                    target.execute(
                        "INSERT INTO ga_ops_restores(id,group_id,backup_id,report) VALUES (%s,%s,%s,%s)",
                        (
                            identifier if len(manifest.group_ids) == 1 else uuid4().hex,
                            group,
                            backup_id,
                            Jsonb(report.model_dump(mode="json")),
                        ),
                    )
                target.commit()
                return report
            finally:
                target.rollback()
                target.execute("SELECT pg_advisory_unlock(%s)", (RESTORE_LOCK_KEY,))

    def expire(self, actor: ActorContext, *, now=None) -> list[str]:
        require_ops(actor)
        stamp = clock(now)
        with self.store.transaction(actor) as tx:
            rows = tx.conn.execute(
                "SELECT id,manifest FROM ga_ops_backups WHERE group_id=%s AND expires_at<=%s AND state<>'expired' ORDER BY id",
                (actor.group_id, stamp),
            ).fetchall()
        expired = []
        for row in rows:
            if row["manifest"]:
                self._scope(actor, row["manifest"]["group_ids"])
            folder = self.root / row["id"]
            if folder.exists():
                managed_directory(self.root, row["id"])
                shutil.rmtree(folder)
            with self.store.transaction(actor) as tx:
                tx.conn.execute(
                    "UPDATE ga_ops_backups SET state='expired' WHERE group_id=%s AND id=%s",
                    (actor.group_id, row["id"]),
                )
                tx._audit("backup_expired", row["id"])
            expired.append(row["id"])
        return expired
