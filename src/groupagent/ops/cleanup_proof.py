"""Trusted local erasure evidence, separate from pending channel erasure.

Only the runtime cleanup control path writes this proof, after physical removal
and row redaction. It is deliberately not a cleanup completion receipt. Archive
readers verify the exact queued operation, its sources and its derived database
scope in their own snapshot; legacy audit messages and handler return values do
not establish this phase.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from groupagent.domain import DomainError, GroupEvent
from groupagent.memory import CleanupJob
from groupagent.ops.common import digest
from groupagent.persistence import UnitOfWork
from groupagent.persistence.text_parts import removed_text_is_redacted

LOCAL_CLEANUP_ACTION = "runtime_local_cleanup_verified"
LOCAL_CLEANUP_CONTRACT = "runtime-local-cleanup-v1"
DEFERRED_CLEANUP_ACTION = "memory_cleanup_attempt_deferred"


def _json(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.astimezone(UTC).isoformat()
    if isinstance(value, dict):
        return {key: _json(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [_json(item) for item in value]
    return value


def _rows(conn, query: str, params: tuple) -> list[dict[str, Any]]:
    return [_json(row) for row in conn.execute(query, params).fetchall()]


def local_cleanup_binding(conn, job: CleanupJob) -> dict[str, str] | None:
    """Bind current authority and payload rows; missing or changed input fails closed."""
    queued = conn.execute(
        "SELECT * FROM ga_memory_cleanup WHERE group_id=%s AND id=%s",
        (job.group_id, job.id),
    ).fetchone()
    if (
        queued is None
        or CleanupJob.model_validate(queued) != job
        or job.state != "pending"
        or job.resource_kind == "backup_tombstone"
    ):
        return None
    if not removed_text_is_redacted(conn, job.group_id):
        return None
    event = conn.execute(
        "SELECT body,memory_state FROM ga_events WHERE group_id=%s AND id=%s",
        (job.group_id, job.event_id),
    ).fetchone()
    if job.event_id and event is None:
        return None
    reason = event["memory_state"] if event else "forgotten"
    if event and reason == "active":
        source = GroupEvent.model_validate(event["body"])
        reason = source.content_state
        if reason == "active":
            reasons = {
                part.get("reason") for part in source.attachments if part.get("privacy_redacted")
            }
            reason = next(
                (value for value in ("forgotten", "recalled", "expired") if value in reasons),
                "active",
            )
    if reason not in {"expired", "forgotten", "recalled"}:
        return None
    works = conn.execute(
        "SELECT * FROM ga_work_orders WHERE group_id=%s AND "
        "(id=%s OR request_event_id=%s OR COALESCE(scope->'source_fingerprints','{}'::jsonb) ? %s) "
        "AND (%s<>'expired' OR created_at<=%s) ORDER BY id",
        (
            job.group_id,
            job.work_id,
            job.event_id,
            job.event_id or "",
            reason,
            job.created_at,
        ),
    ).fetchall()
    if job.work_id and not any(
        row["id"] == job.work_id and row["scope"].get("privacy_redacted") for row in works
    ):
        return None
    if any(row["state"] not in {"paused", "cancelled", "failed", "completed"} for row in works):
        return None
    work_ids = [row["id"] for row in works]
    sources = {job.event_id} if job.event_id else set()
    for row in works:
        sources.add(row["request_event_id"])
        sources.update(row["scope"].get("source_fingerprints", {}))
        sources.update(row["scope"].get("source_event_ids", []))
    source_rows = _rows(
        conn,
        "SELECT id,source_key,body,memory_state FROM ga_events WHERE group_id=%s AND id=ANY(%s) ORDER BY id",
        (job.group_id, sorted(sources)),
    )
    if len(source_rows) != len(sources):
        return None
    if any(
        row["memory_state"] != "active"
        and (
            row["body"].get("text", "") != ""
            or row["body"].get("attachments", []) != []
            or row["body"].get("payload_ref") is not None
        )
        for row in source_rows
    ):
        return None
    # Select exactly the old source/work scope. Later independent work derived
    # from a retained document is outside an expired raw source's cleanup job.
    checkpoints = _rows(
        conn,
        "SELECT * FROM ga_runtime_checkpoints WHERE group_id=%s AND (work_id=ANY(%s) OR ((%s=ANY(source_event_ids) OR source_fingerprints ? %s) AND (%s<>'expired' OR saved_at<=%s))) ORDER BY work_id,attempt_id",
        (job.group_id, work_ids, job.event_id, job.event_id or "", reason, job.created_at),
    )
    runtime_events = _rows(
        conn,
        "SELECT * FROM ga_runtime_events WHERE group_id=%s AND work_id=ANY(%s) ORDER BY work_id,attempt_id,sequence",
        (job.group_id, work_ids),
    )
    media = _rows(
        conn,
        "SELECT * FROM ga_media_inputs WHERE group_id=%s AND (event_id=%s OR source_fingerprints ? %s) ORDER BY event_id,attachment_id",
        (job.group_id, job.event_id, job.event_id or ""),
    )
    catalog = _rows(
        conn,
        "SELECT * FROM ga_artifact_catalog WHERE group_id=%s AND (work_id=ANY(%s) OR %s=ANY(source_event_ids)) AND (%s<>'expired' OR work_id=ANY(%s)) ORDER BY version_id",
        (job.group_id, work_ids, job.event_id, reason, work_ids),
    )
    outputs = _rows(
        conn,
        "SELECT * FROM ga_work_outputs WHERE group_id=%s AND version_id=ANY(%s) ORDER BY work_id,goal_revision,version_id",
        (job.group_id, [row["version_id"] for row in catalog]),
    )
    validations = _rows(
        conn,
        "SELECT * FROM ga_work_validations WHERE group_id=%s AND work_id=ANY(%s) ORDER BY work_id,goal_revision,check_key",
        (job.group_id, work_ids),
    )
    # Proof is about the actual database state, not a handler's assertion that
    # it ran these writes. A partial/no-op redaction cannot sign dirty rows.
    if (
        any(row["body"] != {} or not row["invalidated"] for row in checkpoints)
        or any(row["body"] != {} for row in runtime_events)
        or any(row["body"] != {} or row["available"] for row in media)
        or any(row["evidence_ref"] != "privacy-redacted" for row in validations)
    ):
        return None
    now = datetime.now(UTC)
    retained = {
        row["version_id"]
        for row in catalog
        if reason == "expired"
        and row["available"]
        and isinstance(row["validation"], dict)
        and row["validation"].get("passed") is True
        and (row["expires_at"] is None or datetime.fromisoformat(row["expires_at"]) > now)
    }
    if any(
        row["version_id"] not in retained
        and (
            row["available"]
            or row["body"] != {}
            or row["validation"] is not None
            or row["name"] != "[source removed]"
            or row["format"] != "deleted"
        )
        for row in catalog
    ) or any(
        row["version_id"] not in retained and row["validation"] is not None for row in outputs
    ):
        return None
    return {
        "contract": LOCAL_CLEANUP_CONTRACT,
        "job_sha256": digest(_json(job.model_dump())),
        "source_sha256": digest(source_rows),
        "scope_sha256": digest(
            {
                "reason": reason,
                "works": _json(works),
                "checkpoints": checkpoints,
                "runtime_events": runtime_events,
                "media": media,
                "catalog": catalog,
                "outputs": outputs,
                "validations": validations,
            }
        ),
    }


def local_cleanup_proof(conn, job: CleanupJob) -> dict[str, Any] | None:
    proof = conn.execute(
        "SELECT id,details FROM ga_audit_events WHERE group_id=%s AND resource_ref=%s AND action=%s ORDER BY id DESC LIMIT 1",
        (job.group_id, job.id, LOCAL_CLEANUP_ACTION),
    ).fetchone()
    if (
        proof is None
        or not isinstance(proof["details"], dict)
        or proof["details"].get("contract") != LOCAL_CLEANUP_CONTRACT
    ):
        return None
    binding = local_cleanup_binding(conn, job)
    if binding is None or any(proof["details"].get(key) != value for key, value in binding.items()):
        return None
    return proof


def record_local_cleanup(tx: UnitOfWork, job: CleanupJob, **details: Any) -> None:
    """Called only after the runtime's physical purge and database redaction."""
    binding = local_cleanup_binding(tx.conn, job)
    if binding is None:
        raise DomainError("version_conflict", "Local erasure authority changed during cleanup")
    tx._audit(LOCAL_CLEANUP_ACTION, job.id, **details, **binding)
