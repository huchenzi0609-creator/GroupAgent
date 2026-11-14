"""Explicit, audited maintenance operations for a quarantined restored database."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager

from groupagent.domain import ActorContext, DeliveryIntent, DeliveryState, DomainError
from groupagent.ops.common import require_ops
from groupagent.persistence import PostgresStore, UnitOfWork


class RestorationService:
    def __init__(
        self, store: PostgresStore, *, authorized_group_ids: set[str] | None = None
    ) -> None:
        self.store = store
        self.authorized_groups = authorized_group_ids

    @contextmanager
    def maintenance(self, actor: ActorContext) -> Iterator[UnitOfWork]:
        """Manual control-plane recovery only; never register as an agent tool."""
        require_ops(actor)
        with self.store.connect() as conn:
            tx = UnitOfWork(conn, actor, "restore-maintenance", recovery_maintenance=True)
            tx._check_group()
            yield tx

    def status(self, actor: ActorContext) -> dict:
        require_ops(actor)
        with self.store.connect() as conn:
            schema = conn.execute("SELECT to_regclass('public.ga_groups') AS present").fetchone()
            if not schema or schema["present"] is None:
                private = conn.execute(
                    "SELECT to_regclass('groupagent_recovery.guard') AS present"
                ).fetchone()
                guard = (
                    conn.execute(
                        "SELECT blocked,restore_id,evidence_ref FROM groupagent_recovery.guard WHERE singleton"
                    ).fetchone()
                    if private and private["present"] is not None
                    else None
                )
                return {
                    "guard": dict(guard) if guard else None,
                    "schema_available": False,
                    "unknown_deliveries": None,
                    "budget_blocked": None,
                }
        with self.maintenance(actor) as tx:
            gate = tx.conn.execute(
                "SELECT blocked,restore_id,evidence_ref FROM groupagent_recovery.guard WHERE singleton"
            ).fetchone()
            delivery_count = tx.conn.execute(
                "SELECT count(*) AS n FROM ga_deliveries WHERE group_id=%s AND state IN ('unknown','dispatching')",
                (actor.group_id,),
            ).fetchone()
            assert delivery_count is not None
            deliveries = delivery_count["n"]
            budget = tx.conn.execute(
                "SELECT blocked FROM ga_budget_policy WHERE group_id=%s", (actor.group_id,)
            ).fetchone()
            return {
                "guard": dict(gate) if gate else None,
                "schema_available": True,
                "unknown_deliveries": deliveries,
                "budget_blocked": None if budget is None else budget["blocked"],
            }

    def reconcile_delivery(
        self,
        actor: ActorContext,
        intent_id: str,
        state: DeliveryState,
        *,
        evidence_ref: str,
        platform_message_id: str | None = None,
        platform_job_id: str | None = None,
    ) -> DeliveryIntent:
        if not evidence_ref.strip() or state not in {
            DeliveryState.ACCEPTED,
            DeliveryState.REJECTED,
        }:
            raise DomainError(
                "invalid_input", "Recovery needs a conclusive original-key receipt and evidence"
            )
        with self.maintenance(actor) as tx:
            preview = tx._delivery_row(intent_id, lock=False)
            if preview["work_id"]:
                tx._work_row(preview["work_id"])
            row = tx._delivery_row(intent_id)
            if row["state"] != DeliveryState.UNKNOWN:
                raise DomainError(
                    "invalid_transition", "Only an uncertain original intent can be reconciled"
                )
            result = tx._delivery_outcome(
                row,
                state,
                platform_message_id,
                platform_job_id,
                evidence_ref,
                "restore_reconciliation",
            )
            tx._audit("restore_delivery_reconciled", intent_id, evidence_ref=evidence_ref)
            return result

    def release(
        self,
        actor: ActorContext,
        restore_id: str,
        *,
        evidence_ref: str,
        privacy_verified: bool = False,
        channel_verified: bool = False,
    ) -> None:
        if privacy_verified is not True or channel_verified is not True or not evidence_ref.strip():
            raise DomainError(
                "reconciliation_required",
                "Release requires verified privacy and channel-cutover evidence",
            )
        with self.maintenance(actor) as tx:
            groups = {row["id"] for row in tx.conn.execute("SELECT id FROM ga_groups").fetchall()}
            authorized = (
                self.authorized_groups if self.authorized_groups is not None else {actor.group_id}
            )
            if not groups.issubset(authorized) or (
                groups - {actor.group_id} and actor.role.value != "maintainer"
            ):
                raise DomainError(
                    "scope_violation",
                    "Global recovery release needs every restored group's authorization",
                )
            gate = tx.conn.execute(
                "SELECT * FROM groupagent_recovery.guard WHERE singleton FOR UPDATE"
            ).fetchone()
            if gate is None or gate["restore_id"] != restore_id:
                raise DomainError(
                    "not_found", "Recovery guard does not match this restore operation"
                )
            unknown = tx.conn.execute(
                "SELECT 1 FROM ga_deliveries WHERE state IN ('unknown','dispatching') LIMIT 1"
            ).fetchone()
            if unknown:
                raise DomainError(
                    "delivery_unknown", "Original delivery intents still need reconciliation"
                )
            pending = tx.conn.execute(
                "SELECT 1 FROM ga_memory_cleanup WHERE state='pending' AND resource_kind<>'backup_tombstone' LIMIT 1"
            ).fetchone()
            if pending:
                raise DomainError(
                    "privacy_cleanup_pending",
                    "Restored derived payloads still need verified cleanup",
                )
            missing = tx.conn.execute(
                "SELECT 1 FROM ga_groups g LEFT JOIN ga_budget_policy b ON b.group_id=g.id WHERE b.group_id IS NULL OR NOT b.blocked LIMIT 1"
            ).fetchone()
            if missing:
                raise DomainError(
                    "reconciliation_required",
                    "Recovery must retain a blocked budget policy for every group",
                )
            tx.conn.execute(
                "UPDATE ga_restore_guards SET blocked=false,evidence_ref=%s,released_at=clock_timestamp() WHERE restore_id=%s",
                (evidence_ref, restore_id),
            )
            tx.conn.execute(
                "UPDATE groupagent_recovery.guard SET blocked=false,evidence_ref=%s,updated_at=clock_timestamp() WHERE singleton",
                (evidence_ref,),
            )
            tx._audit("restore_guard_released", restore_id, evidence_ref=evidence_ref)
            # Budget blocks, paused work and paused schedules deliberately stay
            # in place. Their own authorized recovery operations are separate.
