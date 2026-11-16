"""Host-only reconciliation of isolated drafts; never a second execution loop."""

from __future__ import annotations

from groupagent.config import AppConfig
from groupagent.domain import ActorContext, WorkOrder, WorkState
from groupagent.execution import EngineCheckpoint
from groupagent.persistence import PostgresStore
from groupagent.sandbox import DockerSandbox
from groupagent.tools import ToolGateway
from groupagent.tools.compute import register_compute_tools
from groupagent.tools.models import EffectState

from .recovery import (
    RecoveryEvidence,
    checkpoint_digest,
    reconcile_unpublished_compute_checkpoint,
)
from .resource_profile import worker_limits


class UnpublishedDraftReconciler:
    def __init__(self, config: AppConfig, store: PostgresStore) -> None:
        self.config, self.store = config, store

    async def __call__(
        self, work: WorkOrder, checkpoint: EngineCheckpoint, actor: ActorContext
    ) -> EngineCheckpoint | None:
        request = checkpoint.request
        if (
            not self.config.runtime.worker_image
            or work.state != WorkState.PAUSED
            or (actor.group_id, actor.member_id) != (work.group_id, work.requester_id)
            or request.actor != actor
            or request.work_id != work.id
            or checkpoint.revision != work.goal_revision
        ):
            return None
        unknown = [r for r in checkpoint.receipts if r.effect_state == EffectState.UNKNOWN]
        if not unknown:
            return None
        runner = DockerSandbox(
            self.config.runtime.worker_image,
            docker_host=self.config.runtime.docker_host,
            limits=worker_limits(self.config),
        )
        gateway = ToolGateway()
        register_compute_tools(gateway, runner)
        if any(not gateway.is_isolated_draft(r.tool_name) for r in unknown):
            return None
        with self.store.transaction(actor) as tx:
            current = tx.get_work(work.id)
            row = tx._work_row(work.id)
            if (
                current.revision != work.revision
                or current.state != WorkState.PAUSED
                or (row["lease_expires_at"] and row["lease_expires_at"] > row["db_now"])
            ):
                return None
            requested = tx.conn.execute(
                "SELECT created_at,body->'payload' AS payload "
                "FROM ga_runtime_events WHERE group_id=%s AND work_id=%s AND attempt_id=%s "
                "AND kind='tool_requested' AND body->'payload'->>'call_id'=ANY(%s)",
                (tx.group_id, work.id, request.attempt_id, [r.call_id for r in unknown]),
            ).fetchall()
            receipts = {receipt.call_id: receipt for receipt in unknown}
            observed = set()
            for event in requested:
                payload = event["payload"]
                identifier = payload.get("call_id") if isinstance(payload, dict) else None
                receipt = receipts.get(identifier) if isinstance(identifier, str) else None
                if (
                    receipt is None
                    or payload.get("name") != receipt.tool_name
                    or payload.get("idempotency_key") != receipt.idempotency_key
                    or type(payload.get("revision")) is not int
                    or not 1 <= payload["revision"] <= checkpoint.revision
                ):
                    return None
                observed.add(receipt.call_id)
            if observed != receipts.keys():
                return None
            boundary = min(event["created_at"] for event in requested)
            last_request = max(event["created_at"] for event in requested)
            stopped = tx.conn.execute(
                "SELECT id FROM ga_audit_events WHERE group_id=%s AND resource_ref=%s "
                "AND action IN ('execution_stopped','work_recovery_host_quiesced') "
                "AND details->>'attempt_id'=%s AND created_at>=%s ORDER BY id DESC LIMIT 1",
                (tx.group_id, work.id, request.attempt_id, last_request),
            ).fetchone()
            if not stopped:
                # Lease expiry alone cannot attest that the previous controller
                # stopped publishing. A process/host stop acknowledgment is needed.
                return None
            published = tx.conn.execute(
                "SELECT 1 FROM ga_artifact_catalog WHERE group_id=%s AND work_id=%s "
                "AND created_at>=%s LIMIT 1",
                (tx.group_id, work.id, boundary),
            ).fetchone()
            artifact_event = tx.conn.execute(
                "SELECT 1 FROM ga_runtime_events WHERE group_id=%s AND work_id=%s "
                "AND kind='artifact_candidate' AND created_at>=%s LIMIT 1",
                (tx.group_id, work.id, boundary),
            ).fetchone()
            sent = tx.conn.execute(
                "SELECT 1 FROM ga_deliveries d LEFT JOIN ga_delivery_parts p "
                "ON p.group_id=d.group_id AND p.intent_id=d.id WHERE d.group_id=%s "
                "AND d.work_id=%s AND ("
                "d.created_at>=%s OR d.updated_at>=%s OR d.accepted_at>=%s OR p.updated_at>=%s) AND ("
                "d.state IN ('dispatching','accepted','unknown') OR d.platform_message_id IS NOT NULL "
                "OR d.platform_job_id IS NOT NULL OR p.state IN ('submitted','accepted','unknown')) LIMIT 1",
                (tx.group_id, work.id, boundary, boundary, boundary, boundary),
            ).fetchone()
            if published or artifact_event or sent:
                return None
            proof = f"audit:{stopped['id']}:unpublished-draft:{checkpoint_digest(checkpoint)}"
        await DockerSandbox.quiesce_work(actor.group_id, work.id)
        return reconcile_unpublished_compute_checkpoint(
            checkpoint,
            gateway=gateway,
            evidence=RecoveryEvidence(
                checkpoint_digest=checkpoint_digest(checkpoint),
                controller_stopped=True,
                workers_absent=True,
                publication_history_clear=True,
                native_delivery_clear=True,
                evidence_ref=proof,
            ),
        )
