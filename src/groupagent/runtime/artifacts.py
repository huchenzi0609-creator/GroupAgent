"""Scoped artifact catalog and provenance gates between workers and dispatch."""

from __future__ import annotations

import builtins
import hashlib
from dataclasses import asdict
from datetime import UTC, datetime, timedelta
from typing import Any

from psycopg.types.json import Jsonb

from groupagent.artifacts import ArtifactScope, ArtifactStore, ArtifactVersion
from groupagent.domain import ActorContext, DomainError, ExecutionLease, Role
from groupagent.persistence import PostgresStore, UnitOfWork

from .state import source_fingerprint


class ArtifactRegistry:
    def __init__(
        self, store: PostgresStore, artifacts: ArtifactStore, *, retention_days: int = 90
    ) -> None:
        self.store, self.artifacts = store, artifacts
        if not 1 <= retention_days <= 36500:
            raise ValueError("Artifact retention must be bounded")
        self.retention_days = retention_days

    @staticmethod
    def source_fingerprints(
        tx: UnitOfWork,
        sources: tuple[str, ...],
        *,
        expected: dict[str, str] | None = None,
        retained_sources: tuple[str, ...] = (),
    ) -> dict[str, str]:
        result = {}
        for identifier in sorted(set(sources)):
            event = tx.get_event(identifier)
            state = getattr(event, "content_state", "active")
            if state != "active" and not (state == "expired" and identifier in retained_sources):
                raise DomainError("source_unavailable", "Artifact input is no longer available")
            result[identifier] = source_fingerprint(event)
            if expected is not None and expected.get(identifier) != result[identifier]:
                raise DomainError(
                    "source_unavailable", "Artifact input changed since it was authorized"
                )
        return result

    def register(
        self,
        actor: ActorContext,
        lease: ExecutionLease,
        version: ArtifactVersion,
        *,
        source_event_ids: tuple[str, ...] = (),
        validation: dict[str, Any] | None = None,
        purpose: str = "result",
        expires_at: datetime | None = None,
    ) -> None:
        if (version.group_id, version.work_id, version.created_by) != (
            actor.group_id,
            lease.work_id,
            actor.member_id,
        ):
            raise DomainError("scope_violation", "Artifact does not match its producing work")
        scope = ArtifactScope(version.group_id, version.work_id, version.created_by)
        payload = self.artifacts.read(scope, version.file_id, version.version_id)
        if (
            len(payload) != version.size_bytes
            or hashlib.sha256(payload).hexdigest() != version.sha256
        ):
            raise DomainError("validation_failed", "Artifact bytes do not match their manifest")
        if validation is not None and (
            validation.get("passed") is not True or validation.get("sha256") != version.sha256
        ):
            raise DomainError("validation_failed", "Artifact validation does not match its bytes")
        with self.store.transaction(actor) as tx:
            tx.assert_lease(lease)
            work = tx.get_work(lease.work_id)
            expected_raw = work.scope.get("source_fingerprints")
            if expected_raw is not None and (
                not isinstance(expected_raw, dict)
                or any(not isinstance(value, str) for value in expected_raw.values())
            ):
                raise DomainError("invalid_input", "Work source fingerprints are invalid")
            expected: dict[str, str] | None = (
                {key: str(value) for key, value in expected_raw.items()}
                if isinstance(expected_raw, dict)
                else None
            )
            sources = tuple(
                sorted(set(source_event_ids) | {work.request_event_id} | set(expected or {}))
            )
            retained = work.scope.get("retained_fact_source_ids", [])
            fingerprints = self.source_fingerprints(
                tx,
                sources,
                expected=expected,
                retained_sources=tuple(value for value in retained if isinstance(value, str))
                if isinstance(retained, list)
                else (),
            )
            body = asdict(version)
            tx.conn.execute(
                "INSERT INTO ga_artifact_catalog(group_id,version_id,file_id,work_id,created_by,name,format,sha256,byte_length,body,validation,source_event_ids,source_fingerprints,expires_at) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING",
                (
                    actor.group_id,
                    version.version_id,
                    version.file_id,
                    version.work_id,
                    version.created_by,
                    version.name,
                    version.format,
                    version.sha256,
                    version.size_bytes,
                    Jsonb(body),
                    Jsonb(validation) if validation else None,
                    list(sources),
                    Jsonb(fingerprints),
                    expires_at,
                ),
            )
            old = tx.conn.execute(
                "SELECT body,available FROM ga_artifact_catalog WHERE group_id=%s AND version_id=%s",
                (actor.group_id, version.version_id),
            ).fetchone()
            if old is None or old["body"] != body or not old["available"]:
                raise DomainError(
                    "idempotency_conflict", "Artifact version cannot be replaced or resurrected"
                )
            tx.conn.execute(
                "INSERT INTO ga_work_outputs(group_id,work_id,goal_revision,version_id,purpose,validation) VALUES(%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING",
                (
                    actor.group_id,
                    lease.work_id,
                    lease.goal_revision,
                    version.version_id,
                    purpose,
                    Jsonb(validation) if validation else None,
                ),
            )
            tx._audit(
                "artifact_catalogued",
                version.version_id,
                work_id=lease.work_id,
                goal_revision=lease.goal_revision,
            )

    def _row(
        self, tx: UnitOfWork, version_id: str, *, shared_required: bool = False
    ) -> dict[str, Any]:
        row = tx.conn.execute(
            "SELECT *,clock_timestamp() AS now FROM ga_artifact_catalog WHERE group_id=%s AND version_id=%s",
            (tx.group_id, version_id),
        ).fetchone()
        if row is None:
            raise DomainError("not_found", "Artifact version is not available in this group")
        if not row["available"] or (
            row["expires_at"] is not None and row["expires_at"] <= row["now"]
        ):
            raise DomainError("source_unavailable", "Artifact version is unavailable or expired")
        if not row["shared"] and (
            shared_required
            or (
                row["created_by"] != tx.actor.member_id
                and tx.actor.role not in {Role.OWNER, Role.MAINTAINER}
            )
        ):
            raise DomainError("permission_denied", "Draft artifact is not shared with this member")
        producing = tx.get_work(row["work_id"])
        if producing.scope.get("privacy_redacted"):
            raise DomainError("source_unavailable", "Producing work was withdrawn")
        task_sources = producing.scope.get("source_task_revisions", {})
        if isinstance(task_sources, dict):
            for identifier in task_sources:
                if tx.get_team_task(identifier).content_state != "active":
                    raise DomainError("source_unavailable", "Artifact task evidence was forgotten")
        for identifier, digest in row["source_fingerprints"].items():
            event = tx.get_event(identifier)
            state = getattr(event, "content_state", "active")
            withdrawn_quote = any(
                item.get("privacy_redacted") and item.get("reason") in {"forgotten", "recalled"}
                for item in event.attachments
            )
            expired_quote = any(
                item.get("privacy_redacted") and item.get("reason") == "expired"
                for item in event.attachments
            )
            independently_retained = bool(
                row["validation"]
                and row["validation"].get("passed")
                and (state == "expired" or expired_quote)
            )
            if (
                state in {"forgotten", "recalled"}
                or withdrawn_quote
                or (source_fingerprint(event) != digest and not independently_retained)
            ):
                raise DomainError(
                    "source_unavailable", "Artifact source has changed or was withdrawn"
                )
        return row

    def get(self, actor: ActorContext, version_id: str) -> ArtifactVersion:
        with self.store.transaction(actor) as tx:
            return ArtifactVersion(**self._row(tx, version_id)["body"])

    def read(
        self, actor: ActorContext, version_id: str, *, require_validated: bool = False
    ) -> tuple[ArtifactVersion, bytes]:
        with self.store.transaction(actor) as tx:
            row = self._row(tx, version_id)
            if require_validated and not (
                row["validation"] and row["validation"].get("passed") is True
            ):
                raise DomainError("validation_failed", "Artifact is not validated for delivery")
            version = ArtifactVersion(**row["body"])
        payload = self.artifacts.read(
            ArtifactScope(version.group_id, version.work_id, version.created_by),
            version.file_id,
            version.version_id,
        )
        if hashlib.sha256(payload).hexdigest() != version.sha256:
            raise DomainError("validation_failed", "Artifact was modified after cataloging")
        return version, payload

    def list(
        self,
        actor: ActorContext,
        *,
        query: str = "",
        limit: int = 50,
        public_context: bool = False,
        work_id: str | None = None,
    ) -> builtins.list[dict[str, Any]]:
        with self.store.transaction(actor) as tx:
            rows = tx.conn.execute(
                "SELECT version_id FROM ga_artifact_catalog WHERE group_id=%s AND available AND (shared OR created_by=%s OR %s) AND name ILIKE %s AND (%s::text IS NULL OR work_id=%s) ORDER BY created_at DESC LIMIT %s",
                (
                    actor.group_id,
                    actor.member_id,
                    actor.role in {Role.OWNER, Role.MAINTAINER},
                    "%" + query + "%",
                    work_id,
                    work_id,
                    min(max(limit, 1), 200),
                ),
            ).fetchall()
            results = []
            for candidate in rows:
                try:
                    row = self._row(tx, candidate["version_id"])
                    if public_context:
                        self.public_sources_in_tx(tx, row)
                    results.append(
                        {
                            key: row[key]
                            for key in (
                                "version_id",
                                "file_id",
                                "work_id",
                                "name",
                                "format",
                                "sha256",
                                "byte_length",
                                "created_at",
                                "shared",
                            )
                        }
                    )
                except DomainError as error:
                    if error.code not in {"source_unavailable", "permission_denied"}:
                        raise
            return results

    @staticmethod
    def public_sources_in_tx(tx: UnitOfWork, row: dict[str, Any]) -> None:
        # Owner maintenance authority is not permission to quote another
        # member's private draft or restricted topic into a group conversation.
        if not row["shared"] and row["created_by"] != tx.actor.member_id:
            raise DomainError("permission_denied", "Another member's draft is private")
        if tx.conn.execute(
            "SELECT 1 FROM ga_topic_events e JOIN ga_memory_topics t ON "
            "t.group_id=e.group_id AND t.id=e.topic_id WHERE e.group_id=%s "
            "AND e.event_id=ANY(%s) AND t.visibility='participants' LIMIT 1",
            (tx.group_id, row["source_event_ids"]),
        ).fetchone():
            raise DomainError("permission_denied", "Artifact contains restricted group evidence")

    def consume_in_tx(
        self, tx: UnitOfWork, version_id: str
    ) -> tuple[dict[str, str], builtins.list[str]]:
        row = self._row(tx, version_id)
        self.public_sources_in_tx(tx, row)
        fingerprints = {
            identifier: tx.event_fingerprint(identifier) for identifier in row["source_event_ids"]
        }
        retained = [
            identifier
            for identifier in fingerprints
            if tx.get_event(identifier).content_state == "expired"
        ]
        return fingerprints, retained

    def task_sources_in_tx(self, tx: UnitOfWork, version_id: str) -> dict[str, int]:
        row = self._row(tx, version_id)
        self.public_sources_in_tx(tx, row)
        raw = tx.get_work(row["work_id"]).scope.get("source_task_revisions", {})
        return (
            {identifier: tx.get_team_task(identifier).revision for identifier in raw}
            if isinstance(raw, dict)
            else {}
        )

    def outputs(
        self, actor: ActorContext, work_id: str, goal_revision: int
    ) -> builtins.list[dict[str, Any]]:
        with self.store.transaction(actor) as tx:
            work = tx.get_work(work_id)
            if work.requester_id != actor.member_id and actor.role not in {
                Role.OWNER,
                Role.MAINTAINER,
            }:
                raise DomainError("permission_denied", "Outputs belong to another work")
            rows = tx.conn.execute(
                "SELECT version_id,purpose,validation FROM ga_work_outputs WHERE group_id=%s AND work_id=%s AND goal_revision=%s ORDER BY version_id",
                (actor.group_id, work_id, goal_revision),
            ).fetchall()
            return [{**row, "artifact": self._row(tx, row["version_id"])["body"]} for row in rows]

    def validate_output(
        self,
        actor: ActorContext,
        lease: ExecutionLease,
        version_id: str,
        validation: dict[str, Any],
    ) -> None:
        with self.store.transaction(actor) as tx:
            tx.assert_lease(lease)
            row = self._row(tx, version_id)
            if not validation.get("passed") or validation.get("sha256") != row["sha256"]:
                raise DomainError("validation_failed", "Validation does not match output")
            changed = tx.conn.execute(
                "UPDATE ga_work_outputs SET validation=%s WHERE group_id=%s AND work_id=%s AND goal_revision=%s AND version_id=%s RETURNING version_id",
                (Jsonb(validation), actor.group_id, lease.work_id, lease.goal_revision, version_id),
            ).fetchone()
            if changed is None:
                raise DomainError(
                    "scope_violation", "Artifact is not an output of this goal revision"
                )
            tx.conn.execute(
                "UPDATE ga_artifact_catalog SET validation=%s,expires_at=COALESCE(expires_at,%s) WHERE group_id=%s AND version_id=%s",
                (
                    Jsonb(validation),
                    datetime.now(UTC) + timedelta(days=self.retention_days),
                    actor.group_id,
                    version_id,
                ),
            )

    def mark_shared_in_tx(self, tx: UnitOfWork, version_id: str) -> None:
        row = self._row(tx, version_id)
        if not row["validation"] or not row["validation"].get("passed"):
            raise DomainError("validation_failed", "Only validated output can be published")
        tx.conn.execute(
            "UPDATE ga_artifact_catalog SET shared=true WHERE group_id=%s AND version_id=%s",
            (tx.group_id, version_id),
        )
