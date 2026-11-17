"""Explicit host-only reconciliation of legacy public-read execution receipts."""

from collections.abc import Mapping
from typing import Any, NoReturn

from pydantic import ValidationError

from groupagent.domain import DomainError
from groupagent.execution.models import EngineCheckpoint, RunState
from groupagent.tools.models import (
    EffectState,
    IdempotencyMode,
    ToolDefinition,
    ToolFailureDisposition,
    ToolReceipt,
)

_RESEARCH_READS = frozenset({"web_search", "web_fetch"})


def reconcile_legacy_research_checkpoint(
    checkpoint: EngineCheckpoint,
    *,
    definitions: Mapping[str, ToolDefinition[Any, Any]],
) -> EngineCheckpoint:
    """Derive a resumable copy; never persist, invoke a tool, or touch fees.

    The trusted operator must first back up/audit the original checkpoint and
    freshly authorize its work against the current gateway. ``definitions``
    must come from that gateway's actual registrations, never from model data.
    Only failed UNKNOWN public-research reads are adjudicated NO_EFFECT; their
    unknown monetary reservations remain a separate, unchanged accounting fact.
    """

    def refuse(message: str) -> NoReturn:
        raise DomainError("recovery_required", message)

    if checkpoint.state != RunState.PAUSED or checkpoint.error_code != "effect_unknown":
        refuse("Only a legacy unknown-effect pause can use read reconciliation")
    if checkpoint.backend_state.get("model_recovery_pending", False):
        refuse("An unresolved model attempt cannot use read reconciliation")

    unknown: dict[str, ToolReceipt] = {}
    call_ids: set[str] = set()
    for receipt in checkpoint.receipts:
        if receipt.effect_state != EffectState.UNKNOWN:
            continue
        definition = definitions.get(receipt.tool_name)
        if (
            receipt.success
            or not receipt.error_code
            or receipt.error_code == "tool_in_progress"
            or receipt.tool_name not in _RESEARCH_READS
            or receipt.tool_name not in checkpoint.request.allowed_tools
            or definition is None
            or definition.name != receipt.tool_name
            or definition.risk_class != "read_only"
            or definition.idempotency_mode != IdempotencyMode.READ_ONLY
        ):
            refuse("An unknown action is not a registered failed public-research read")
        if receipt.idempotency_key in unknown or receipt.call_id in call_ids:
            refuse("Legacy unknown read identities are not unique")
        unknown[receipt.idempotency_key] = receipt
        call_ids.add(receipt.call_id)
    if not unknown or checkpoint.unresolved_tools != frozenset(
        receipt.tool_name for receipt in unknown.values()
    ):
        refuse("Legacy unresolved tools do not match the unknown read receipts")
    if any(
        sum(receipt.idempotency_key == key for receipt in checkpoint.receipts) != 1
        or sum(receipt.call_id == original.call_id for receipt in checkpoint.receipts) != 1
        for key, original in unknown.items()
    ):
        refuse("Legacy failed read identity conflicts with another recorded receipt")

    seen: set[str] = set()
    ledger_unknown: set[str] = set()
    for entry in checkpoint.ledger:
        key = entry.receipt.idempotency_key
        if key in seen or entry.receipt not in checkpoint.receipts:
            refuse("Legacy ledger does not match the recorded receipts")
        seen.add(key)
        if entry.receipt.effect_state == EffectState.UNKNOWN:
            if unknown.get(key) != entry.receipt:
                refuse("Legacy unknown ledger action has no matching failed read")
            ledger_unknown.add(key)
    if ledger_unknown != set(unknown):
        refuse("Legacy failed reads lack matching unknown ledger entries")

    reconciled = {
        key: receipt.model_copy(
            update={
                "effect_state": EffectState.NO_EFFECT,
                "failure": receipt.failure.model_copy(
                    update={"disposition": ToolFailureDisposition.REPAIR}
                )
                if receipt.failure is not None
                else None,
            }
        )
        for key, receipt in unknown.items()
    }
    by_call = {receipt.call_id: receipt for receipt in unknown.values()}
    found_messages: set[str] = set()
    messages = []
    for message in checkpoint.messages:
        original = by_call.get(message.tool_call_id or "") if message.role == "tool" else None
        if original is None:
            messages.append(message)
            continue
        try:
            recorded = ToolReceipt.model_validate_json(message.content or "")
        except ValidationError:
            refuse("Legacy read tool message is not a recorded receipt")
        if recorded != original or original.call_id in found_messages:
            refuse("Legacy read tool message differs from its receipt")
        found_messages.add(original.call_id)
        messages.append(
            message.model_copy(
                update={"content": reconciled[original.idempotency_key].model_dump_json()}
            )
        )
    if found_messages != call_ids:
        refuse("Legacy failed read lacks its matching tool message")

    return checkpoint.model_copy(
        update={
            "sequence": checkpoint.sequence + 1,
            "state": RunState.PAUSED,
            "safe_to_resume": True,
            "error_code": None,
            "unresolved_tools": frozenset(),
            "receipts": tuple(
                reconciled.get(receipt.idempotency_key, receipt) for receipt in checkpoint.receipts
            ),
            "ledger": tuple(
                entry.model_copy(
                    update={"receipt": reconciled.get(entry.receipt.idempotency_key, entry.receipt)}
                )
                for entry in checkpoint.ledger
            ),
            "messages": tuple(messages),
        }
    )
