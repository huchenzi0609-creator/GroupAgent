"""Control-plane erasure of managed caches; never exposed as a model tool."""

from __future__ import annotations

import asyncio
import hashlib
import inspect
import json
import os
import stat
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from groupagent.artifacts import ArtifactScope, ArtifactStore
from groupagent.domain import ActorContext, DomainError, Role
from groupagent.memory import CleanupJob, MemoryService
from groupagent.memory.common import memory_lock
from groupagent.memory.deletions import DeletionSink
from groupagent.persistence import PostgresStore, UnitOfWork


def _digest(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


class RuntimeCleanupHandler:
    def __init__(
        self,
        store: PostgresStore,
        artifacts: ArtifactStore,
        root: Path,
        *,
        technical: ActorContext | None = None,
        channel: Any = None,
        is_busy: Callable[[tuple[str, ...], tuple[str, ...]], bool] | None = None,
        deletion_sink: DeletionSink | None = None,
        quiesce: Callable[[tuple[str, ...], tuple[str, ...]], Awaitable[None]] | None = None,
        audit_days: int = 90,
    ) -> None:
        self.store, self.artifacts, self.channel = store, artifacts, channel
        self.technical, self.is_busy = technical, is_busy
        self.deletion_sink = deletion_sink
        self.quiesce = quiesce
        if not isinstance(audit_days, int) or isinstance(audit_days, bool) or audit_days < 1:
            raise DomainError("invalid_input", "Audit retention must be a positive number of days")
        self.audit_days = audit_days
        self.root = root.absolute()
        if self.root != self.root.resolve() or self.root.is_symlink() or not self.root.is_dir():
            raise DomainError("unsafe_path", "Cleanup needs a canonical managed runtime directory")
        if self.root.stat().st_uid != os.getuid():
            raise DomainError("unsafe_path", "Runtime directory must belong to this service")

    def _actor(self, job: CleanupJob) -> ActorContext:
        if self.technical is not None:
            if self.technical.group_id != job.group_id or self.technical.role != Role.MAINTAINER:
                raise DomainError(
                    "scope_violation", "Cleanup needs this group's technical identity"
                )
            return self.technical
        with self.store.connect() as conn:
            row = conn.execute(
                "SELECT policy_revision FROM ga_groups WHERE id=%s", (job.group_id,)
            ).fetchone()
        if row is None:
            raise DomainError("not_found", "Cleanup group no longer exists")
        return ActorContext(
            group_id=job.group_id,
            member_id="service:cleanup",
            role=Role.MAINTAINER,
            policy_revision=row["policy_revision"],
        )

    def _plan(
        self, actor: ActorContext, job: CleanupJob, *, check_busy: bool = True
    ) -> dict[str, Any]:
        with self.store.transaction(actor) as tx:
            memory_lock(tx)
            row = tx.conn.execute(
                "SELECT * FROM ga_memory_cleanup WHERE group_id=%s AND id=%s FOR SHARE",
                (job.group_id, job.id),
            ).fetchone()
            if row is None or CleanupJob.model_validate(row) != job:
                raise DomainError(
                    "version_conflict", "Cleanup must use the canonical queued operation"
                )
            if job.state != "pending" or job.not_before > datetime.now(UTC):
                raise DomainError("cleanup_pending", "Cleanup is complete or not due")
            if job.resource_kind == "backup_tombstone":
                raise DomainError(
                    "dependency_unavailable", "Backup erasure belongs to the backup manager"
                )
            event = tx.get_event(job.event_id) if job.event_id else None
            reason: str = event.content_state if event else "forgotten"
            if event and reason == "active":
                reasons = {
                    part.get("reason") for part in event.attachments if part.get("privacy_redacted")
                }
                reason = next(
                    (value for value in ("forgotten", "recalled", "expired") if value in reasons),
                    "active",
                )
            if reason not in {"expired", "forgotten", "recalled"}:
                raise DomainError("source_available", "Active content has no cleanup authority")
            works = tx.conn.execute(
                "SELECT id,state,scope,created_at FROM ga_work_orders WHERE group_id=%s AND "
                "(id=%s OR request_event_id=%s OR COALESCE(scope->'source_fingerprints','{}'::jsonb) ? %s) ORDER BY id",
                (job.group_id, job.work_id, job.event_id, job.event_id or ""),
            ).fetchall()
            if job.work_id and (not works or not works[0]["scope"].get("privacy_redacted")):
                raise DomainError("source_available", "Work has no durable privacy tombstone")
            if reason == "expired":
                works = [work for work in works if work["created_at"] <= job.created_at]
            if any(
                work["state"] not in {"paused", "cancelled", "failed", "completed"}
                for work in works
            ):
                raise DomainError(
                    "cleanup_busy", "Related work must already be stopped before privacy cleanup"
                )
            work_ids = tuple(work["id"] for work in works)
            events = (job.event_id,) if job.event_id else ()
            if not check_busy:
                busy = False
            elif self.is_busy is not None:
                busy = self.is_busy(work_ids, events)
            else:
                busy = (
                    tx.conn.execute(
                        "SELECT 1 FROM ga_process_leases WHERE group_id=%s AND expires_at>clock_timestamp() LIMIT 1",
                        (job.group_id,),
                    ).fetchone()
                    is not None
                )
            if check_busy and self.quiesce is None:
                busy = (
                    busy
                    or tx.conn.execute(
                        "SELECT 1 FROM ga_process_leases WHERE group_id=%s AND expires_at>clock_timestamp() LIMIT 1",
                        (job.group_id,),
                    ).fetchone()
                    is not None
                )
            if busy:
                raise DomainError(
                    "cleanup_busy", "A runtime writer must stop before its caches are erased"
                )
            catalog = tx.conn.execute(
                "SELECT * FROM ga_artifact_catalog WHERE group_id=%s AND (work_id=ANY(%s) OR %s=ANY(source_event_ids)) ORDER BY file_id,version_id",
                (job.group_id, list(work_ids), job.event_id),
            ).fetchall()
            if reason == "expired":
                catalog = [item for item in catalog if item["work_id"] in work_ids]
            preserve = set()
            preserve_versions = set()
            if reason == "expired":
                now = datetime.now(UTC)
                preserve = {
                    item["file_id"]
                    for item in catalog
                    if item["available"]
                    and isinstance(item["validation"], dict)
                    and item["validation"].get("passed") is True
                    and (item["expires_at"] is None or item["expires_at"] > now)
                }
                preserve_versions = {
                    item["version_id"]
                    for item in catalog
                    if item["file_id"] in preserve
                    and isinstance(item["validation"], dict)
                    and item["validation"].get("passed") is True
                    and item["available"]
                    and (item["expires_at"] is None or item["expires_at"] > now)
                }
            deliveries = tx.conn.execute(
                "SELECT d.id,d.idempotency_key,d.state,d.platform_message_id FROM ga_deliveries d WHERE d.group_id=%s AND d.reason IN ('source_expired','source_forgotten','source_recalled') AND "
                "(d.request_event_id=%s OR d.work_id=ANY(%s) OR d.request_event_id IN (SELECT event_id FROM ga_decisions WHERE group_id=%s AND (body->'source_fingerprints' ? %s OR body->'source_event_ids' ? %s)) "
                "OR d.id IN (SELECT intent_id FROM ga_progress_notices WHERE group_id=%s AND work_id=ANY(%s)))",
                (
                    job.group_id,
                    job.event_id,
                    list(work_ids),
                    job.group_id,
                    job.event_id or "",
                    job.event_id or "",
                    job.group_id,
                    list(work_ids),
                ),
            ).fetchall()
            parts = tx.conn.execute(
                "SELECT idempotency_key,state,platform_message_id FROM ga_delivery_parts WHERE group_id=%s AND intent_id=ANY(%s)",
                (job.group_id, [item["id"] for item in deliveries]),
            ).fetchall()
            platform_ids = [
                item["platform_message_id"]
                for item in [*deliveries, *parts]
                if item["state"] == "accepted" and item["platform_message_id"]
            ]
            late = tx.conn.execute(
                "SELECT * FROM ga_events WHERE group_id=%s AND memory_state='active' AND body->>'platform_message_id'=ANY(%s) ORDER BY id FOR UPDATE",
                (job.group_id, platform_ids),
            ).fetchall()
            if late:
                MemoryService(self.store, deletion_sink=self.deletion_sink)._purge(
                    tx,
                    late,
                    [],
                    reason=reason,
                    now=datetime.now(UTC),
                    forget_facts=reason != "expired",
                )
            return {
                "event": event,
                "reason": reason,
                "works": work_ids,
                "catalog": catalog,
                "uncertain_delivery": any(
                    item["state"] in {"dispatching", "submitted", "unknown"}
                    for item in [*deliveries, *parts]
                ),
                "preserve": preserve,
                "preserve_versions": preserve_versions,
                "keys": tuple(sorted({item["idempotency_key"] for item in [*deliveries, *parts]})),
            }

    def _remove(self, components: tuple[str, ...]) -> None:
        """Traverse only fixed/hash components with nofollow dirfds; unlink symlinks themselves."""

        def tree(parent: int, name: str) -> None:
            try:
                info = os.stat(name, dir_fd=parent, follow_symlinks=False)
            except FileNotFoundError:
                return
            if stat.S_ISDIR(info.st_mode):
                child = os.open(name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=parent)
                try:
                    for entry in os.listdir(child):
                        tree(child, entry)
                    os.fsync(child)
                finally:
                    os.close(child)
                os.rmdir(name, dir_fd=parent)
            else:
                os.unlink(name, dir_fd=parent)

        fd = os.open(self.root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            for component in components[:-1]:
                try:
                    child = os.open(
                        component, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=fd
                    )
                except FileNotFoundError:
                    return
                os.close(fd)
                fd = child
            tree(fd, components[-1])
            os.fsync(fd)
        except OSError as error:
            raise DomainError(
                "unsafe_path", "Managed cleanup path is unavailable or redirected"
            ) from error
        finally:
            os.close(fd)

    async def purge(self, job: CleanupJob) -> str:
        actor = self._actor(job)
        if self.quiesce is not None:
            preliminary = self._plan(actor, job, check_busy=False)
            await self.quiesce(preliminary["works"], (job.event_id,) if job.event_id else ())
        plan = self._plan(actor, job)
        reason, work_ids, event = plan["reason"], plan["works"], plan["event"]
        with self.store.transaction(actor) as tx:
            memory_lock(tx)
            for work_id in sorted(work_ids):
                current = tx._work_row(work_id)
                if current["state"] not in {"paused", "cancelled", "failed", "completed"}:
                    raise DomainError("cleanup_busy", "Work restarted after writer shutdown")
            self._remove_payloads(actor, job, plan)
            self._redact_rows(tx, job, plan)
            tx._audit(
                "runtime_privacy_payloads_removed",
                job.id,
                reason=reason,
                work_ids=list(work_ids),
                retained_files=len(plan["preserve"]),
            )
        purge = getattr(self.channel, "purge_event", None)
        if purge is None:
            if self.channel is not None or (
                event and (event.channel_account_id or event.platform_message_id)
            ):
                raise DomainError(
                    "upstream_cleanup_unsupported",
                    "The channel has no reliable cache erasure interface",
                )
        else:
            receipt = purge(
                job.group_id,
                event.id if event else None,
                source_key=event.source_key if event else None,
                platform_message_id=event.platform_message_id if event else None,
                replacement=event,
                delivery_keys=plan["keys"],
                reason=reason,
            )
            evidence = (
                await asyncio.wait_for(receipt, timeout=30)
                if inspect.isawaitable(receipt)
                else receipt
            )
            if not isinstance(evidence, str) or not evidence.strip():
                raise DomainError(
                    "upstream_cleanup_unverified", "Channel erasure returned no evidence"
                )
        if plan["uncertain_delivery"]:
            raise DomainError(
                "delivery_unknown", "Late delivery receipts still need privacy reconciliation"
            )
        return "runtime-cache-purge:" + job.id

    def _remove_payloads(self, actor: ActorContext, job: CleanupJob, plan: dict[str, Any]) -> None:
        reason, work_ids, event = plan["reason"], plan["works"], plan["event"]
        if event:
            # Explicit withdrawal closes every derivative. Raw expiry is scoped
            # to the old work snapshot: a later authorized work can independently
            # reuse a retained document without inheriting this pending cleanup.
            if reason != "expired":
                self.artifacts.purge_event(job.group_id, event.id, actor.member_id, reason=reason)
            self.artifacts.purge_work(
                ArtifactScope(job.group_id, "input:" + event.id, actor.member_id), reason=reason
            )
            input_hash = _digest(
                json.dumps(
                    [job.group_id, event.id],
                    sort_keys=True,
                    ensure_ascii=False,
                    separators=(",", ":"),
                )
            )
            self._remove(("inputs", "input-" + input_hash))
        for work_id in work_ids:
            scope = ArtifactScope(job.group_id, work_id, actor.member_id)
            if reason != "expired":
                self.artifacts.purge_work(scope, reason=reason)
            else:
                for version in self.artifacts.list(scope):
                    if version.file_id in plan["preserve"]:
                        self.artifacts.retain_versions(
                            scope,
                            version.file_id,
                            {
                                item["version_id"]
                                for item in plan["catalog"]
                                if item["file_id"] == version.file_id
                                and item["version_id"] in plan["preserve_versions"]
                            },
                            reason=reason,
                        )
                    else:
                        self.artifacts.purge(scope, version.file_id, reason=reason)
            self._remove(("workspaces", _digest(job.group_id), _digest(work_id)))

    @staticmethod
    def _redact_rows(tx, job: CleanupJob, plan: dict[str, Any]) -> None:
        work_ids = list(plan["works"])
        tx.conn.execute(
            "UPDATE ga_runtime_checkpoints SET body='{}',invalidated=true WHERE group_id=%s AND (work_id=ANY(%s) OR ((%s=ANY(source_event_ids) OR source_fingerprints ? %s) AND (%s<>'expired' OR saved_at<=%s)))",
            (
                job.group_id,
                work_ids,
                job.event_id,
                job.event_id or "",
                plan["reason"],
                job.created_at,
            ),
        )
        tx.conn.execute(
            "UPDATE ga_runtime_events SET body='{}' WHERE group_id=%s AND work_id=ANY(%s)",
            (job.group_id, work_ids),
        )
        tx.conn.execute(
            "UPDATE ga_media_inputs SET body='{}',available=false WHERE group_id=%s AND (event_id=%s OR source_fingerprints ? %s)",
            (job.group_id, job.event_id, job.event_id or ""),
        )
        removed = [
            item["version_id"]
            for item in plan["catalog"]
            if item["version_id"] not in plan["preserve_versions"]
        ]
        tx.conn.execute(
            "UPDATE ga_artifact_catalog SET body='{}',validation=NULL,name='[source removed]',format='deleted',available=false WHERE group_id=%s AND version_id=ANY(%s)",
            (job.group_id, removed),
        )
        tx.conn.execute(
            "UPDATE ga_work_outputs SET validation=NULL WHERE group_id=%s AND version_id=ANY(%s)",
            (job.group_id, removed),
        )
        # Outcome problems and suggested corrections can quote raw user content.
        tx.conn.execute(
            "UPDATE ga_work_validations SET evidence_ref='privacy-redacted' WHERE group_id=%s AND work_id=ANY(%s)",
            (job.group_id, work_ids),
        )

    def _artifact_expiry_plan(
        self, tx: UnitOfWork, candidate: dict[str, Any], stamp: datetime
    ) -> dict[str, Any] | None:
        """A locked snapshot; callers must release this transaction before channel I/O."""
        memory_lock(tx)
        work = tx._work_row(candidate["work_id"])
        if self.quiesce is not None and work["state"] not in {
            "paused",
            "cancelled",
            "failed",
            "completed",
        }:
            raise DomainError("cleanup_busy", "Work restarted after writer shutdown")
        if self.is_busy is not None:
            busy = self.is_busy((candidate["work_id"],), ())
        else:
            busy = (
                tx.conn.execute(
                    "SELECT 1 FROM ga_process_leases WHERE group_id=%s AND expires_at>clock_timestamp() LIMIT 1",
                    (tx.group_id,),
                ).fetchone()
                is not None
            )
        if self.quiesce is None:
            busy = (
                busy
                or tx.conn.execute(
                    "SELECT 1 FROM ga_process_leases WHERE group_id=%s AND expires_at>clock_timestamp() LIMIT 1",
                    (tx.group_id,),
                ).fetchone()
                is not None
            )
        if busy:
            raise DomainError("cleanup_busy", "A runtime writer still owns this file's work")
        rows = tx.conn.execute(
            "SELECT * FROM ga_artifact_catalog WHERE group_id=%s AND file_id=%s ORDER BY version_id FOR UPDATE",
            (tx.group_id, candidate["file_id"]),
        ).fetchall()
        due = [
            row
            for row in rows
            if row["available"] and row["expires_at"] is not None and row["expires_at"] <= stamp
        ]
        if not due:
            return None
        due_ids = {row["version_id"] for row in due}
        keep = {
            row["version_id"]
            for row in rows
            if row["available"] and row["version_id"] not in due_ids
        }
        intents = tx.conn.execute(
            "SELECT * FROM ga_deliveries WHERE group_id=%s AND payload_ref=ANY(%s) ORDER BY id FOR UPDATE",
            (
                tx.group_id,
                [
                    reference
                    for row in due
                    for reference in (
                        "artifact:" + row["version_id"],
                        "artifact:" + row["file_id"] + ":" + row["version_id"],
                    )
                ],
            ),
        ).fetchall()
        parts = tx.conn.execute(
            "SELECT * FROM ga_delivery_parts WHERE group_id=%s AND intent_id=ANY(%s) ORDER BY intent_id,position",
            (tx.group_id, [row["id"] for row in intents]),
        ).fetchall()
        if any(
            row["state"] in {"dispatching", "submitted", "unknown"} for row in [*intents, *parts]
        ):
            raise DomainError("delivery_unknown", "Artifact delivery receipt is not conclusive")
        sources = {work["request_event_id"], *work["scope"].get("source_fingerprints", {})}
        for row in due:
            sources.update(row["source_event_ids"])
            sources.update(row["source_fingerprints"])
        return {
            "due": due,
            "keep": keep,
            "intents": intents,
            "parts": parts,
            "keys": tuple(sorted({row["idempotency_key"] for row in [*intents, *parts]})),
            "work": {key: value for key, value in work.items() if key != "db_now"},
            "sources": {source: tx.event_fingerprint(source) for source in sorted(sources)},
        }

    async def expire_artifacts(
        self, actor: ActorContext, *, now: datetime | None = None, limit: int = 100
    ) -> dict[str, Any]:
        """Physically enforce catalog version deadlines without deleting retained siblings."""
        if actor.role not in {Role.MAINTAINER, Role.OWNER} or (
            self.technical is not None and self.technical.group_id != actor.group_id
        ):
            raise DomainError(
                "permission_denied", "Artifact retention needs a scoped control identity"
            )
        stamp = now or datetime.now(UTC)
        if stamp.utcoffset() is None or not 1 <= limit <= 1000:
            raise DomainError(
                "invalid_input", "Artifact retention requires an aware clock and bounded batch"
            )
        with self.store.transaction(actor) as tx:
            candidates = tx.conn.execute(
                "SELECT DISTINCT file_id,work_id FROM ga_artifact_catalog WHERE group_id=%s AND available AND expires_at<=%s ORDER BY file_id LIMIT %s",
                (actor.group_id, stamp, limit),
            ).fetchall()
        expired: list[str] = []
        errors: dict[str, str] = {}
        for candidate in candidates:
            key = "artifact:" + candidate["file_id"]
            try:
                if self.quiesce is not None:
                    with self.store.transaction(actor) as tx:
                        work = tx._work_row(candidate["work_id"])
                        if work["state"] not in {"paused", "cancelled", "failed", "completed"}:
                            raise DomainError(
                                "cleanup_busy",
                                "Active independent work must finish before artifact cache erasure",
                            )
                    await self.quiesce((candidate["work_id"],), ())
                with self.store.transaction(actor) as tx:
                    plan = self._artifact_expiry_plan(tx, candidate, stamp)
                if plan is None:
                    continue
                if plan["keys"]:
                    purge = getattr(self.channel, "purge_event", None)
                    if purge is None:
                        raise DomainError(
                            "upstream_cleanup_unsupported",
                            "Expired delivery cache has no reliable erasure interface",
                        )
                    receipt = purge(
                        actor.group_id, None, delivery_keys=plan["keys"], reason="expired"
                    )
                    evidence = (
                        await asyncio.wait_for(receipt, timeout=30)
                        if inspect.isawaitable(receipt)
                        else receipt
                    )
                    if not isinstance(evidence, str) or not evidence.strip():
                        raise DomainError(
                            "upstream_cleanup_unverified",
                            "Artifact delivery cache returned no erasure evidence",
                        )
                with self.store.transaction(actor) as tx:
                    # No transaction or memory/row lock survives the channel await.
                    # Recheck every authority/source/receipt before touching local
                    # payloads; new siblings must use the current retention set.
                    current = self._artifact_expiry_plan(tx, candidate, stamp)
                    if current is None:
                        continue
                    if any(
                        current[field] != plan[field]
                        for field in ("work", "sources", "due", "intents", "parts")
                    ):
                        raise DomainError(
                            "version_conflict",
                            "Artifact expiry inputs changed during channel erasure",
                        )
                    due_ids = [row["version_id"] for row in current["due"]]
                    self.artifacts.retain_versions(
                        ArtifactScope(actor.group_id, candidate["work_id"], actor.member_id),
                        candidate["file_id"],
                        current["keep"],
                        reason="expired",
                    )
                    tx.conn.execute(
                        "UPDATE ga_artifact_catalog SET available=false,body='{}',validation=NULL,name='[expired]',format='deleted' WHERE group_id=%s AND version_id=ANY(%s)",
                        (actor.group_id, due_ids),
                    )
                    tx.conn.execute(
                        "UPDATE ga_work_outputs SET validation=NULL WHERE group_id=%s AND version_id=ANY(%s)",
                        (actor.group_id, due_ids),
                    )
                    tx.conn.execute(
                        "UPDATE ga_deliveries SET state=CASE WHEN state='pending' THEN 'rejected' ELSE state END,reason=CASE WHEN reason IN ('source_forgotten','source_recalled') THEN reason ELSE 'artifact_expired' END,revision=revision+1 WHERE group_id=%s AND id=ANY(%s)",
                        (actor.group_id, [row["id"] for row in current["intents"]]),
                    )
                    tx._audit(
                        "artifact_versions_expired", candidate["file_id"], version_ids=due_ids
                    )
                    expired.extend(due_ids)
            except Exception as error:
                errors[key] = error.code if isinstance(error, DomainError) else "cleanup_failed"
        with self.store.transaction(actor) as tx:
            removed = tx.conn.execute(
                "DELETE FROM ga_audit_events WHERE group_id=%s AND id IN (SELECT id FROM ga_audit_events WHERE group_id=%s AND created_at<%s ORDER BY created_at,id LIMIT 10000)",
                (actor.group_id, actor.group_id, stamp - timedelta(days=self.audit_days)),
            ).rowcount
            if removed:
                tx._audit(
                    "audit_retention_applied",
                    None,
                    deleted_count=removed,
                    retention_days=self.audit_days,
                )
        return {
            "expired_artifact_versions": expired,
            "errors": errors,
            "deleted_audit_events": removed,
        }
