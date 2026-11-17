"""Durable scheduling and evidence-based adjudication of interrupted work.

Retry scheduling never changes execution usage, monetary reservations, tool
receipts, authorization or checkpoint safety. The supervisor validates those
independently before allowing a new attempt.
"""

from __future__ import annotations

import hashlib
import json
import os
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, NoReturn

from pydantic import ValidationError

from groupagent.domain import DomainError, WorkOrder, WorkState
from groupagent.execution import EngineCheckpoint, RunState, accounting_recovery_checkpoint
from groupagent.tools import ToolGateway, ToolScope
from groupagent.tools.gateway import tool_call_fingerprint
from groupagent.tools.models import EffectState, ToolFailureDisposition, ToolReceipt
from groupagent.tools.paths import open_parent, scoped_path

from .checkpoint_codec import serialize_runtime_model

RECOVERY_SCOPE_KEY = "_runtime_recovery"
RECONCILIATION_SCOPE_KEY = "_runtime_reconciliation"
AUTOMATIC_RECOVERY_CODES = frozenset(
    {
        "resource_deferred",
        "dependency_unavailable",
        "dependency_timeout",
        "model_timeout",
        "provider_timeout",
        "provider_unavailable",
        "rate_limited",
        "invalid_model_response",
        "model_output_truncated",
        "execution_stalled",
        "execution_error_loop",
        "worker_stopped",
        "worker_lost",
        "preparation_timeout",
        "execution_failed",
        "validation_failed",
        "activation_yielded",
        "billing_unavailable",
        "input_inference_retry",
    }
)

RECONCILIATION_CODES = frozenset(
    {
        "effect_unknown",
        "publication_unknown",
        "delivery_uncertain",
        "provider_reconciliation_required",
        "preparation_reconciliation_required",
        "reconciliation_required",
    }
)


def checkpoint_digest(checkpoint: EngineCheckpoint) -> str:
    """Bind host evidence to exactly one preserved checkpoint, without exposing it."""
    return hashlib.sha256(serialize_runtime_model(checkpoint)).hexdigest()


@dataclass(frozen=True)
class RecoveryEvidence:
    """Host attestations; never construct these from model/tool output fields.

    Absence of current files alone is insufficient. ``publication_history_clear``
    must cover immutable artifact/catalog/output/audit history, including deleted
    or invalidated versions. ``native_delivery_clear`` must cover submitted sends,
    not merely pending rows. The supervisor separately fences the old controller.
    """

    checkpoint_digest: str
    controller_stopped: bool
    workers_absent: bool
    publication_history_clear: bool
    native_delivery_clear: bool
    evidence_ref: str


def reconcile_unpublished_compute_checkpoint(
    checkpoint: EngineCheckpoint, *, gateway: ToolGateway, evidence: RecoveryEvidence
) -> EngineCheckpoint:
    """Adjudicate failed isolated drafts, without rerunning code or changing usage.

    A matching proposal/ledger/tool observation and positive host evidence are
    all required. Unknown sends, publication history, missing observations and
    in-flight calls remain fenced. The caller preserves the original checkpoint
    and records the host proof before persisting this derived copy.
    """

    def refuse() -> NoReturn:
        raise DomainError("recovery_required", "Unpublished draft proof is incomplete")

    if (
        checkpoint.state != RunState.PAUSED
        or checkpoint.error_code not in {"effect_unknown", "publication_unknown"}
        or evidence.checkpoint_digest != checkpoint_digest(checkpoint)
        or not all(
            (
                evidence.controller_stopped,
                evidence.workers_absent,
                evidence.publication_history_clear,
                evidence.native_delivery_clear,
            )
        )
        or not isinstance(evidence.evidence_ref, str)
        or not 1 <= len(evidence.evidence_ref) <= 512
        or checkpoint.backend_state.get("model_recovery_pending", False)
    ):
        refuse()
    request = checkpoint.request
    scope = ToolScope(
        request.actor.group_id,
        request.work_id,
        request.actor,
        request.workspace,
        request.allowed_tools,
        request.trace_id,
    )
    gateway.bind_scope(scope)
    if not gateway.is_quiescent(scope):
        raise DomainError("work_busy", "A previous tool has not acknowledged termination")
    unknown = {
        receipt.call_id: receipt
        for receipt in checkpoint.receipts
        if receipt.effect_state == EffectState.UNKNOWN
    }
    if not unknown or checkpoint.unresolved_tools != frozenset(
        receipt.tool_name for receipt in unknown.values()
    ):
        refuse()
    failed_codes = {
        "worker_failed",
        "tool_failed",
        "output_contract_failed",
        "invalid_artifact",
        "process_failed",
        "tool_timeout",
        "cancelled",
        "output_too_large",
    }
    for receipt in unknown.values():
        if (
            receipt.success
            or receipt.error_code not in failed_codes
            or not gateway.is_isolated_draft(receipt.tool_name)
            or receipt.tool_name not in request.allowed_tools
            or receipt.idempotency_key != f"{request.work_id}:{receipt.call_id}"
            or sum(item.call_id == receipt.call_id for item in checkpoint.receipts) != 1
            or sum(item.idempotency_key == receipt.idempotency_key for item in checkpoint.receipts)
            != 1
        ):
            refuse()
    ledger = {}
    for entry in checkpoint.ledger:
        key = entry.receipt.idempotency_key
        if key in ledger or entry.receipt not in checkpoint.receipts:
            refuse()
        ledger[key] = entry
    if {
        entry.receipt.call_id
        for entry in ledger.values()
        if entry.receipt.effect_state == EffectState.UNKNOWN
    } != unknown.keys():
        refuse()
    for entry in gateway.export_ledger(scope):
        if (
            entry.receipt.effect_state == EffectState.UNKNOWN
            and ledger.get(entry.receipt.idempotency_key) != entry
        ):
            refuse()
    proposals, observations = set(), set()
    reconciled = {
        identifier: receipt.model_copy(
            update={
                "effect_state": EffectState.NO_EFFECT,
                "failure": receipt.failure.model_copy(
                    update={"disposition": ToolFailureDisposition.REPAIR}
                )
                if receipt.failure is not None
                else None,
            }
        )
        for identifier, receipt in unknown.items()
    }
    messages = []
    for message in checkpoint.messages:
        for call in message.tool_calls:
            original = unknown.get(call.id)
            if original is None:
                continue
            proposal_entry = ledger.get(original.idempotency_key)
            try:
                valid = (
                    message.role == "assistant"
                    and call.id not in proposals
                    and call.name == original.tool_name
                    and proposal_entry is not None
                    and proposal_entry.receipt == original
                    and tool_call_fingerprint(call.name, call.arguments)
                    == proposal_entry.fingerprint
                )
                outputs = call.arguments.get("output_paths", [])
                if not valid or not isinstance(outputs, (list, tuple)) or len(outputs) > 256:
                    refuse()
                for name in outputs:
                    if not isinstance(name, str):
                        refuse()
                    scoped_path(request.workspace, name)
                    with open_parent(request.workspace, name) as (parent, leaf):
                        try:
                            os.stat(leaf, dir_fd=parent, follow_symlinks=False)
                        except FileNotFoundError:
                            pass
                        else:
                            refuse()
            except (OSError, TypeError, ValueError):
                refuse()
            proposals.add(call.id)
        original = unknown.get(message.tool_call_id or "") if message.role == "tool" else None
        if original is None:
            messages.append(message)
            continue
        try:
            recorded = ToolReceipt.model_validate_json(message.content or "")
        except ValidationError:
            refuse()
        if (
            recorded != original
            or original.call_id in observations
            or original.call_id not in proposals
        ):
            refuse()
        observations.add(original.call_id)
        messages.append(
            message.model_copy(update={"content": reconciled[original.call_id].model_dump_json()})
        )
    if proposals != unknown.keys() or observations != unknown.keys():
        refuse()
    return checkpoint.model_copy(
        update={
            "sequence": checkpoint.sequence + 1,
            "safe_to_resume": True,
            "error_code": None,
            "unresolved_tools": frozenset(),
            "receipts": tuple(reconciled.get(item.call_id, item) for item in checkpoint.receipts),
            "ledger": tuple(
                entry.model_copy(
                    update={"receipt": reconciled.get(entry.receipt.call_id, entry.receipt)}
                )
                for entry in checkpoint.ledger
            ),
            "messages": tuple(messages),
            "backend_state": {
                **checkpoint.backend_state,
                "effect_reconciliation": {
                    "checkpoint_digest": evidence.checkpoint_digest,
                    "evidence_ref": evidence.evidence_ref,
                    "classification": "unpublished_isolated_draft",
                },
            },
        }
    )


def is_automatic_recovery_reason(reason: str | None) -> bool:
    """A presentation hint, never authorization to replay an operation."""
    return reason in AUTOMATIC_RECOVERY_CODES


def safe_recovery_checkpoint(checkpoint: EngineCheckpoint) -> bool:
    """Unknown business effects stay fenced even if a malformed safe flag says otherwise."""
    effects_settled = (
        not checkpoint.unresolved_tools
        and all(receipt.effect_state != EffectState.UNKNOWN for receipt in checkpoint.receipts)
        and all(entry.receipt.effect_state != EffectState.UNKNOWN for entry in checkpoint.ledger)
    )
    return effects_settled and (
        accounting_recovery_checkpoint(checkpoint)
        or (
            checkpoint.safe_to_resume
            and not checkpoint.backend_state.get("model_recovery_pending", False)
        )
    )


def recovery_block_reason(checkpoint: EngineCheckpoint) -> str:
    if checkpoint.unresolved_tools or any(
        receipt.effect_state == EffectState.UNKNOWN
        for receipt in (*checkpoint.receipts, *(entry.receipt for entry in checkpoint.ledger))
    ):
        return "effect_unknown"
    if checkpoint.backend_state.get("model_recovery_pending", False):
        return "provider_reconciliation_required"
    if (
        checkpoint.error_code
        and checkpoint.error_code != "cancelled"
        and not is_automatic_recovery_reason(checkpoint.error_code)
    ):
        return checkpoint.error_code
    return "reconciliation_required"


def input_signature(work: WorkOrder) -> str:
    return hashlib.sha256(
        json.dumps(
            [
                work.goal_revision,
                work.input_versions,
                work.scope.get("source_fingerprints", {}),
                work.scope.get("input_event_ids", []),
                work.scope.get("attachment_event_ids", []),
            ],
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
    ).hexdigest()


def recovery_metadata(work: WorkOrder) -> dict[str, Any]:
    value: Any = work.scope.get(RECOVERY_SCOPE_KEY)
    if value is None:
        return {}
    try:
        if (
            not isinstance(value, dict)
            or type(value.get("version")) is not int
            or value["version"] != 1
            or type(value["goal_revision"]) is not int
            or value["goal_revision"] < 1
            or type(value["backoff_step"]) is not int
            or not 0 <= value["backoff_step"] <= 6
            or type(value["consecutive_failures"]) is not int
            or not 1 <= value["consecutive_failures"] <= 1_000_000
            or not is_automatic_recovery_reason(value["error_code"])
        ):
            raise ValueError("Invalid recovery state")
        for key in ("input_signature", "failure_signature"):
            if (
                not isinstance(value[key], str)
                or len(value[key]) != 64
                or any(character not in "0123456789abcdef" for character in value[key])
            ):
                raise ValueError("Invalid recovery signature")
        if not isinstance(value["failure_key"], str) or not 1 <= len(value["failure_key"]) <= 512:
            raise ValueError("Invalid recovery identity")
        if (
            not isinstance(value["strategy_instruction"], str)
            or not 1 <= len(value["strategy_instruction"]) <= 3000
        ):
            raise ValueError("Invalid recovery strategy")
        if datetime.fromisoformat(value["next_retry_at"]).utcoffset() is None:
            raise ValueError("Missing recovery timezone")
    except (KeyError, TypeError, ValueError):
        raise DomainError("reconciliation_required", "Invalid durable recovery state") from None
    return dict(value)


def recovery_due(work: WorkOrder, *, now: datetime | None = None) -> bool:
    metadata = recovery_metadata(work)
    return (
        not metadata
        or metadata["goal_revision"] != work.goal_revision
        or metadata["input_signature"] != input_signature(work)
        or (now or datetime.now(UTC)) >= datetime.fromisoformat(metadata["next_retry_at"])
    )


def automatic_recovery_pending(work: WorkOrder) -> bool:
    """Present scheduled background work without granting execution authority."""
    if work.state not in {WorkState.PAUSED, WorkState.QUEUED}:
        return False
    try:
        reconciliation = reconciliation_metadata(work)
        if (
            reconciliation
            and reconciliation["goal_revision"] == work.goal_revision
            and work.state == WorkState.PAUSED
            and work.pause_reason == reconciliation["reason"]
        ):
            return True
        metadata = recovery_metadata(work)
    except DomainError:
        return False
    return bool(
        metadata
        and metadata["goal_revision"] == work.goal_revision
        and metadata["input_signature"] == input_signature(work)
        and (work.state != WorkState.PAUSED or metadata["error_code"] == work.pause_reason)
    )


def reconciliation_metadata(work: WorkOrder) -> dict[str, Any]:
    value: Any = work.scope.get(RECONCILIATION_SCOPE_KEY)
    if value is None:
        return {}
    try:
        if (
            not isinstance(value, dict)
            or value["version"] != 1
            or type(value["goal_revision"]) is not int
            or type(value["checks"]) is not int
            or not 1 <= value["checks"] <= 1_000_000
            or value["reason"] not in RECONCILIATION_CODES
            or datetime.fromisoformat(value["next_check_at"]).utcoffset() is None
        ):
            raise ValueError("Invalid reconciliation state")
    except (KeyError, TypeError, ValueError):
        raise DomainError(
            "reconciliation_required", "Invalid durable reconciliation state"
        ) from None
    return dict(value)


def plan_reconciliation(
    work: WorkOrder, reason: str, *, now: datetime | None = None
) -> dict[str, Any]:
    """A durable inspection deadline never grants permission to replay an effect."""
    if reason not in RECONCILIATION_CODES:
        raise DomainError("reconciliation_required", "This interruption needs explicit input")
    stamp = now or datetime.now(UTC)
    previous = reconciliation_metadata(work)
    same = previous.get("goal_revision") == work.goal_revision and previous.get("reason") == reason
    if same and stamp < datetime.fromisoformat(previous["next_check_at"]):
        return previous
    checks = min(int(previous.get("checks", 0)) + 1, 1_000_000) if same else 1
    return {
        "version": 1,
        "goal_revision": work.goal_revision,
        "reason": reason,
        "checks": checks,
        "next_check_at": (
            stamp + timedelta(seconds=min(5 * 2 ** min(checks - 1, 6), 300))
        ).isoformat(),
    }


def plan_recovery(
    work: WorkOrder,
    reason: str,
    *,
    failure_key: str,
    diagnostic: str = "",
    now: datetime | None = None,
) -> dict[str, Any]:
    """Keep one retry deadline per failure; saturating counters impose no attempt ceiling."""
    if not is_automatic_recovery_reason(reason) or not 1 <= len(failure_key) <= 512:
        raise DomainError("reconciliation_required", "This failure cannot be retried automatically")
    signature = input_signature(work)
    old = recovery_metadata(work)
    same_input = (
        old.get("goal_revision") == work.goal_revision and old.get("input_signature") == signature
    )
    if same_input and old.get("failure_key") == failure_key and old.get("error_code") == reason:
        return old
    failure_signature = hashlib.sha256((reason + ":" + diagnostic[:1200]).encode()).hexdigest()
    repeated = same_input and old.get("failure_signature") == failure_signature
    step = min(old["backoff_step"] + 1, 6) if same_input else 0
    failures = min(old["consecutive_failures"] + 1, 1_000_000) if repeated else 1
    if reason == "activation_yielded":
        # An activation slice is scheduler fairness, not an accumulating fault.
        step, failures, repeated = 0, 1, False
    specific = {
        "model_output_truncated": (
            "The prior model output was truncated. Break the remaining work into smaller tool "
            "calls and write large artifacts incrementally; return a concise final response."
        ),
        "execution_stalled": (
            "The previous strategy stopped making progress. Inspect existing artifacts and "
            "evidence, choose a different method or smaller concrete step, and verify progress."
        ),
        "execution_error_loop": (
            "The same operation failed repeatedly. Inspect the recorded failure, change its "
            "arguments or method, and do not repeat an unchanged failing operation."
        ),
        "validation_failed": (
            "Inspect the existing candidate and validation evidence. Repair the failing part "
            "with a different concrete strategy and validate it before producing a new candidate."
        ),
    }.get(reason, "Continue from the latest safe checkpoint after the transient interruption.")
    instruction = (
        "Automatic recovery of the same authorized work. Preserve completed work, cumulative "
        "usage, permissions and idempotency evidence. Never replay an uncertain external effect. "
        + specific
    )
    if repeated:
        instruction += (
            " This failure repeated without a changed cause. Diagnose the prerequisite first; "
            "avoid regenerating the same result or issuing the same paid request in a tight loop."
        )
    return {
        "version": 1,
        "goal_revision": work.goal_revision,
        "input_signature": signature,
        "error_code": reason,
        "failure_key": failure_key,
        "failure_signature": failure_signature,
        "consecutive_failures": failures,
        "backoff_step": step,
        "next_retry_at": (
            (now or datetime.now(UTC)) + timedelta(seconds=min(5 * 2**step, 300))
        ).isoformat(),
        "strategy_instruction": instruction,
    }
