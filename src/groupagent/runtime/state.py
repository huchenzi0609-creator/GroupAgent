from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from typing import Any

from psycopg.types.json import Jsonb

from groupagent.domain import (
    ActorContext,
    DeliveryIntent,
    DomainError,
    ExecutionLease,
    Role,
    WorkOrder,
    WorkState,
)
from groupagent.execution.models import EngineCheckpoint, EngineEvent
from groupagent.persistence import PostgresStore, UnitOfWork

from .contracts import DeliveryDraft, ProcessLease


def source_fingerprint(event: Any) -> str:
    return hashlib.sha256(
        json.dumps(
            event.model_dump(mode="json"), sort_keys=True, ensure_ascii=False, separators=(",", ":")
        ).encode()
    ).hexdigest()


class RuntimeState:
    """Trusted control service storage; never passed to model or tool workers."""

    def __init__(self, store: PostgresStore) -> None:
        self.store = store

    def claim_process(
        self, actor: ActorContext, name: str, owner_id: str, *, ttl_seconds: int = 30
    ) -> ProcessLease | None:
        if actor.role not in {Role.OWNER, Role.MAINTAINER}:
            raise DomainError(
                "permission_denied", "Only the control service may own a process lease"
            )
        if not name or not owner_id or not 1 <= ttl_seconds <= 300:
            raise DomainError("invalid_input", "Invalid process lease parameters")
        with self.store.transaction(actor) as tx:
            tx.conn.execute(
                "INSERT INTO ga_process_leases(group_id,name,owner_id,expires_at) VALUES(%s,%s,%s,clock_timestamp()+%s*interval '1 second') ON CONFLICT DO NOTHING",
                (actor.group_id, name, owner_id, ttl_seconds),
            )
            row = tx.conn.execute(
                "SELECT *,clock_timestamp() AS now FROM ga_process_leases WHERE group_id=%s AND name=%s FOR UPDATE",
                (actor.group_id, name),
            ).fetchone()
            assert row is not None
            if row["owner_id"] != owner_id and row["expires_at"] > row["now"]:
                return None
            if row["owner_id"] != owner_id or row["expires_at"] <= row["now"]:
                row = tx.conn.execute(
                    "UPDATE ga_process_leases SET owner_id=%s,fencing_token=fencing_token+1,expires_at=clock_timestamp()+%s*interval '1 second' WHERE group_id=%s AND name=%s RETURNING *",
                    (owner_id, ttl_seconds, actor.group_id, name),
                ).fetchone()
            assert row is not None
            return ProcessLease(**{key: row[key] for key in ProcessLease.__dataclass_fields__})

    def assert_process(self, actor: ActorContext, lease: ProcessLease) -> None:
        if actor.role not in {Role.OWNER, Role.MAINTAINER}:
            raise DomainError(
                "permission_denied", "Control process leases are not member capabilities"
            )
        with self.store.transaction(actor) as tx:
            row = tx.conn.execute(
                "SELECT 1 FROM ga_process_leases WHERE group_id=%s AND name=%s AND owner_id=%s AND fencing_token=%s AND expires_at>clock_timestamp()",
                (actor.group_id, lease.name, lease.owner_id, lease.fencing_token),
            ).fetchone()
            if lease.group_id != actor.group_id or row is None:
                raise DomainError("stale_lease", "Control process lost its lease")

    def heartbeat_process(
        self, actor: ActorContext, lease: ProcessLease, *, ttl_seconds: int = 30
    ) -> ProcessLease:
        if actor.role not in {Role.OWNER, Role.MAINTAINER}:
            raise DomainError(
                "permission_denied", "Control process leases are not member capabilities"
            )
        if lease.group_id != actor.group_id or not 1 <= ttl_seconds <= 300:
            raise DomainError("scope_violation", "Invalid control process lease")
        with self.store.transaction(actor) as tx:
            row = tx.conn.execute(
                "UPDATE ga_process_leases SET expires_at=clock_timestamp()+%s*interval '1 second' WHERE group_id=%s AND name=%s AND owner_id=%s AND fencing_token=%s AND expires_at>clock_timestamp() RETURNING *",
                (ttl_seconds, actor.group_id, lease.name, lease.owner_id, lease.fencing_token),
            ).fetchone()
            if row is None:
                raise DomainError("stale_lease", "Control process lost its lease")
            return ProcessLease(**{key: row[key] for key in ProcessLease.__dataclass_fields__})

    def release_process(self, actor: ActorContext, lease: ProcessLease) -> None:
        if actor.role not in {Role.OWNER, Role.MAINTAINER}:
            raise DomainError(
                "permission_denied", "Control process leases are not member capabilities"
            )
        if actor.group_id != lease.group_id:
            raise DomainError("scope_violation", "Control process belongs to another group")
        with self.store.transaction(actor) as tx:
            tx.conn.execute(
                "UPDATE ga_process_leases SET expires_at=clock_timestamp() WHERE group_id=%s AND name=%s AND owner_id=%s AND fencing_token=%s",
                (actor.group_id, lease.name, lease.owner_id, lease.fencing_token),
            )

    def queued(self, actor: ActorContext, *, limit: int = 100) -> list[WorkOrder]:
        with self.store.transaction(actor) as tx:
            rows = tx.conn.execute(
                "SELECT * FROM ga_work_orders WHERE group_id=%s AND (state='queued' OR (state IN ('running','validating','delivering') AND (lease_expires_at IS NULL OR lease_expires_at<=clock_timestamp()))) ORDER BY created_at,id LIMIT %s",
                (actor.group_id, min(max(limit, 1), 1000)),
            ).fetchall()
            return [
                WorkOrder.model_validate({k: row[k] for k in WorkOrder.model_fields})
                for row in rows
            ]

    def recover_phase(
        self, actor: ActorContext, work_id: str, *, expected_revision: int, evidence_ref: str
    ) -> WorkOrder:
        if not evidence_ref.strip():
            raise DomainError("recovery_required", "Recovery requires durable evidence")
        with self.store.transaction(actor) as tx:
            row = tx._work_row(work_id)
            tx._controller(row)
            tx._revision(row, expected_revision)
            if row["state"] not in {"validating", "delivering"} or (
                row["lease_expires_at"] is not None and row["lease_expires_at"] > row["db_now"]
            ):
                raise DomainError("lease_active", "Finalization is still owned or not recoverable")
            uncertain = tx.conn.execute(
                "SELECT 1 FROM ga_deliveries WHERE group_id=%s AND work_id=%s AND state IN ('dispatching','unknown') LIMIT 1",
                (actor.group_id, work_id),
            ).fetchone()
            paused = tx.transition_work(
                work_id,
                WorkState.PAUSED,
                expected_revision=row["revision"],
                reason="delivery_uncertain" if uncertain else "worker_lost",
            )
            tx._audit(
                "finalization_recovered", work_id, previous=row["state"], evidence_ref=evidence_ref
            )
            if uncertain:
                return paused
            return tx.transition_work(
                work_id, WorkState.QUEUED, expected_revision=paused.revision, reason="recovery"
            )

    @staticmethod
    def enqueue_bundle_in_tx(
        tx: UnitOfWork,
        lease: ExecutionLease,
        drafts: tuple[DeliveryDraft, ...],
        *,
        bundle_id: str | None = None,
    ) -> tuple[DeliveryIntent, ...]:
        if not drafts or len(drafts) > 50:
            raise DomainError(
                "validation_failed", "A result needs a bounded nonempty delivery bundle"
            )
        tx.assert_lease(lease)
        bundle_id = bundle_id or f"result:{lease.work_id}:{lease.goal_revision}"
        result = []
        for position, draft in enumerate(drafts):
            intent = tx.enqueue_delivery(
                lease.work_id,
                draft.payload_ref,
                draft.payload_hash,
                draft.idempotency_key,
                lease=lease,
            )
            tx.conn.execute(
                "INSERT INTO ga_delivery_bundles(group_id,bundle_id,intent_id,position) VALUES(%s,%s,%s,%s) ON CONFLICT DO NOTHING",
                (tx.group_id, bundle_id, intent.id, position),
            )
            stored = tx.conn.execute(
                "SELECT bundle_id,position FROM ga_delivery_bundles WHERE group_id=%s AND intent_id=%s",
                (tx.group_id, intent.id),
            ).fetchone()
            if stored is None or stored["bundle_id"] != bundle_id or stored["position"] != position:
                raise DomainError("idempotency_conflict", "Delivery bundle ordering changed")
            result.append(intent)
        existing = tx.conn.execute(
            "SELECT count(*) AS n FROM ga_delivery_bundles WHERE group_id=%s AND bundle_id=%s",
            (tx.group_id, bundle_id),
        ).fetchone()
        if existing is None or existing["n"] != len(result):
            raise DomainError("idempotency_conflict", "Delivery bundle membership changed")
        return tuple(result)

    def save_checkpoint(
        self,
        actor: ActorContext,
        lease: ExecutionLease,
        checkpoint: EngineCheckpoint,
        *,
        source_event_ids: tuple[str, ...] = (),
    ) -> None:
        if (
            checkpoint.request.work_id != lease.work_id
            or checkpoint.request.attempt_id != lease.attempt_id
            or checkpoint.revision != lease.goal_revision
        ):
            raise DomainError("scope_violation", "Checkpoint does not match its execution lease")
        body = checkpoint.model_dump(mode="json")
        if len(checkpoint.model_dump_json().encode()) > 8 * 1024 * 1024:
            raise DomainError("quota_exceeded", "Checkpoint exceeds the persisted context limit")
        with self.store.transaction(actor) as tx:
            tx.assert_lease(lease)
            work = tx.get_work(lease.work_id)
            expected = work.scope.get("source_fingerprints", {})
            sources = sorted(
                set(source_event_ids)
                | {work.request_event_id}
                | (set(expected) if isinstance(expected, dict) else set())
            )
            retained = work.scope.get("retained_fact_source_ids", [])
            fingerprints = {}
            for source in sources:
                event = tx.get_event(source)
                state = getattr(event, "content_state", "active")
                if state != "active" and not (
                    state == "expired"
                    and source != work.request_event_id
                    and isinstance(retained, list)
                    and source in retained
                ):
                    raise DomainError("source_unavailable", "Checkpoint source was removed")
                fingerprints[source] = source_fingerprint(event)
                if (
                    isinstance(expected, dict)
                    and source in expected
                    and expected[source] != fingerprints[source]
                ):
                    raise DomainError(
                        "source_unavailable", "Checkpoint input changed during execution"
                    )
            old = tx.conn.execute(
                "SELECT sequence,body FROM ga_runtime_checkpoints WHERE group_id=%s AND work_id=%s AND attempt_id=%s FOR UPDATE",
                (actor.group_id, lease.work_id, lease.attempt_id),
            ).fetchone()
            if old and (
                old["sequence"] > checkpoint.sequence
                or (old["sequence"] == checkpoint.sequence and old["body"] != body)
            ):
                raise DomainError(
                    "version_conflict", "Checkpoint sequence cannot regress or change"
                )
            tx.conn.execute(
                "INSERT INTO ga_runtime_checkpoints(group_id,work_id,attempt_id,goal_revision,sequence,body,source_event_ids,source_fingerprints) VALUES(%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT(group_id,work_id,attempt_id) DO UPDATE SET sequence=EXCLUDED.sequence,body=EXCLUDED.body,source_event_ids=EXCLUDED.source_event_ids,source_fingerprints=EXCLUDED.source_fingerprints,saved_at=clock_timestamp()",
                (
                    actor.group_id,
                    lease.work_id,
                    lease.attempt_id,
                    lease.goal_revision,
                    checkpoint.sequence,
                    Jsonb(body),
                    sources,
                    Jsonb(fingerprints),
                ),
            )

    def load_checkpoint(self, actor: ActorContext, work_id: str) -> EngineCheckpoint | None:
        with self.store.transaction(actor) as tx:
            work = tx.get_work(work_id)
            if work.requester_id != actor.member_id and actor.role not in {
                Role.OWNER,
                Role.MAINTAINER,
            }:
                raise DomainError("permission_denied", "Checkpoint is private to its work")
            row = tx.conn.execute(
                "SELECT * FROM ga_runtime_checkpoints WHERE group_id=%s AND work_id=%s ORDER BY saved_at DESC,sequence DESC LIMIT 1",
                (actor.group_id, work_id),
            ).fetchone()
            if row is None:
                return None
            retained = work.scope.get("retained_fact_source_ids", [])
            if row["invalidated"] or any(
                getattr(tx.get_event(source), "content_state", "active") != "active"
                and not (
                    getattr(tx.get_event(source), "content_state", "active") == "expired"
                    and source != work.request_event_id
                    and isinstance(retained, list)
                    and source in retained
                )
                for source in row["source_event_ids"]
            ):
                raise DomainError(
                    "source_unavailable", "Checkpoint sources are no longer available"
                )
            if any(
                source_fingerprint(tx.get_event(source)) != digest
                for source, digest in row["source_fingerprints"].items()
            ):
                raise DomainError("source_unavailable", "Checkpoint source snapshots changed")
            return EngineCheckpoint.model_validate(row["body"])

    def append_event(self, actor: ActorContext, lease: ExecutionLease, event: EngineEvent) -> None:
        if event.work_id != lease.work_id or event.attempt_id != lease.attempt_id:
            raise DomainError("scope_violation", "Execution event does not match its lease")
        with self.store.transaction(actor) as tx:
            tx.assert_lease(lease)
            tx.conn.execute(
                "INSERT INTO ga_runtime_events(group_id,work_id,attempt_id,sequence,kind,body) VALUES(%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING",
                (
                    actor.group_id,
                    lease.work_id,
                    lease.attempt_id,
                    event.sequence,
                    event.kind,
                    Jsonb(event.model_dump(mode="json")),
                ),
            )
            row = tx.conn.execute(
                "SELECT body FROM ga_runtime_events WHERE group_id=%s AND work_id=%s AND attempt_id=%s AND sequence=%s",
                (actor.group_id, lease.work_id, lease.attempt_id, event.sequence),
            ).fetchone()
            if row is None or row["body"] != event.model_dump(mode="json"):
                raise DomainError("idempotency_conflict", "Execution event sequence was reused")

    def alert(self, actor: ActorContext, code: str, *, resource_id: str = "group") -> None:
        if not code.replace("_", "").isalnum() or len(code) > 100:
            code = "runtime_failure"
        fingerprint = hashlib.sha256((code + ":" + resource_id).encode()).hexdigest()
        with self.store.transaction(actor) as tx:
            tx.conn.execute(
                "INSERT INTO ga_runtime_alerts(group_id,fingerprint,code) VALUES(%s,%s,%s) ON CONFLICT(group_id,fingerprint) DO UPDATE SET occurrences=ga_runtime_alerts.occurrences+1,last_seen=clock_timestamp(),state='open'",
                (actor.group_id, fingerprint, code),
            )

    def health(self, actor: ActorContext) -> dict[str, Any]:
        with self.store.transaction(actor) as tx:
            queues = tx.conn.execute(
                "SELECT state,count(*) AS count FROM ga_work_orders WHERE group_id=%s GROUP BY state",
                (actor.group_id,),
            ).fetchall()
            deliveries = tx.conn.execute(
                "SELECT state,count(*) AS count FROM ga_deliveries WHERE group_id=%s GROUP BY state",
                (actor.group_id,),
            ).fetchall()
            alerts = tx.conn.execute(
                "SELECT code,occurrences,last_seen FROM ga_runtime_alerts WHERE group_id=%s AND state='open' ORDER BY last_seen DESC LIMIT 20",
                (actor.group_id,),
            ).fetchall()
            processes = tx.conn.execute(
                "SELECT name,expires_at FROM ga_process_leases WHERE group_id=%s AND expires_at>clock_timestamp() ORDER BY name",
                (actor.group_id,),
            ).fetchall()
            return {
                "status": "stopped"
                if not any(row["name"] == "application" for row in processes)
                else "degraded"
                if alerts
                else "ready",
                "checked_at": datetime.now(UTC).isoformat(),
                "works": queues,
                "deliveries": deliveries,
                "alerts": alerts,
                "active_processes": processes,
            }
