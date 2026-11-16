"""Bounded durable scheduling for safe automatic execution recovery.

Retry scheduling never changes execution usage, monetary reservations, tool
receipts, authorization or checkpoint safety. The supervisor validates those
independently before allowing a new attempt.
"""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime, timedelta
from typing import Any

from groupagent.domain import DomainError, WorkOrder, WorkState
from groupagent.execution import EngineCheckpoint
from groupagent.tools.models import EffectState

RECOVERY_SCOPE_KEY = "_runtime_recovery"
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
    }
)


def is_automatic_recovery_reason(reason: str | None) -> bool:
    """A presentation hint, never authorization to replay an operation."""
    return reason in AUTOMATIC_RECOVERY_CODES


def safe_recovery_checkpoint(checkpoint: EngineCheckpoint) -> bool:
    """Unknown business effects stay fenced even if a malformed safe flag says otherwise."""
    return (
        checkpoint.safe_to_resume
        and not checkpoint.unresolved_tools
        and not checkpoint.backend_state.get("model_recovery_pending", False)
        and all(receipt.effect_state != EffectState.UNKNOWN for receipt in checkpoint.receipts)
        and all(entry.receipt.effect_state != EffectState.UNKNOWN for entry in checkpoint.ledger)
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
        metadata = recovery_metadata(work)
    except DomainError:
        return False
    return bool(
        metadata
        and metadata["goal_revision"] == work.goal_revision
        and metadata["input_signature"] == input_signature(work)
        and (work.state != WorkState.PAUSED or metadata["error_code"] == work.pause_reason)
    )


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
