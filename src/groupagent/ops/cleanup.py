from __future__ import annotations

import inspect
from collections.abc import Awaitable, Callable
from typing import Any, Protocol

from groupagent.domain import ActorContext, DomainError
from groupagent.memory import CleanupJob, MemoryService
from groupagent.ops.backup import BackupManager
from groupagent.ops.journal import DeletionJournal
from groupagent.ops.models import CleanupResult


class CleanupHandler(Protocol):
    async def purge(self, job: CleanupJob) -> str: ...


class CleanupRunner:
    def __init__(
        self,
        memory: MemoryService,
        handler: CleanupHandler | Callable[[CleanupJob], str | Awaitable[str]],
        *,
        journal: DeletionJournal | None = None,
        backups: BackupManager | None = None,
    ) -> None:
        self.memory, self.handler, self.journal = memory, handler, journal
        self.backups = backups

    async def run(self, actor: ActorContext, *, now=None, limit: int = 100) -> CleanupResult:
        synced = self.journal.sync(self.memory.store, actor) if self.journal else 0
        completed = []
        failed = []
        errors = {}
        expired_versions: list[str] = []
        deleted_audits = 0
        for job in self.memory.pending_cleanup(actor, now=now, limit=limit):
            try:
                if job.resource_kind == "backup_tombstone":
                    if self.backups is None:
                        raise DomainError(
                            "dependency_unavailable",
                            "Backup erasure needs the managed backup retention handler",
                        )
                    expired = self.backups.expire(actor, now=now)
                    with self.memory.store.transaction(actor) as tx:
                        remaining = tx.conn.execute(
                            "SELECT 1 FROM ga_ops_backups WHERE group_id=%s AND created_at<=%s AND state<>'expired' LIMIT 1",
                            (actor.group_id, job.created_at),
                        ).fetchone()
                    if remaining:
                        raise DomainError(
                            "cleanup_pending",
                            "Older managed archives still require retention cleanup",
                        )
                    evidence = "backup-retention-verified:" + job.id + ":" + str(len(expired))
                else:
                    result = (
                        self.handler(job) if callable(self.handler) else self.handler.purge(job)
                    )
                    evidence = await result if inspect.isawaitable(result) else result
                if not isinstance(evidence, str) or not evidence.strip():
                    raise ValueError("No concrete purge receipt")
                with self.memory.store.transaction(actor) as tx:
                    self.memory.prepare_in_tx(tx)
                    if job.event_id:
                        event = tx.get_event(job.event_id)
                        private = event.content_state in {"forgotten", "recalled"} or any(
                            item.get("privacy_redacted")
                            and item.get("reason") in {"forgotten", "recalled"}
                            for item in event.attachments
                        )
                        if private:
                            tx.conn.execute(
                                "UPDATE ga_memory_topics SET title='[source removed]',revision=revision+1 WHERE group_id=%s AND id IN (SELECT topic_id FROM ga_topic_origins WHERE group_id=%s AND %s=ANY(source_event_ids))",
                                (actor.group_id, actor.group_id, job.event_id),
                            )
                    self.memory.in_tx(tx).complete_cleanup(
                        actor, job.id, expected_revision=job.revision, evidence_ref=evidence
                    )
                completed.append(job.id)
            except Exception as error:
                failed.append(job.id)
                errors[job.id] = error.code if isinstance(error, DomainError) else "cleanup_failed"
        retention = getattr(self.handler, "expire_artifacts", None)
        if retention is not None:
            retention_result: Any = retention(actor, now=now, limit=limit)
            retained: Any = (
                await retention_result
                if inspect.isawaitable(retention_result)
                else retention_result
            )
            expired_versions = retained["expired_artifact_versions"]
            errors.update(retained["errors"])
            deleted_audits = retained.get("deleted_audit_events", 0)
        return CleanupResult(
            completed=completed,
            failed=failed,
            tombstones_synced=synced,
            errors=errors,
            expired_artifact_versions=expired_versions,
            deleted_audit_events=deleted_audits,
        )
