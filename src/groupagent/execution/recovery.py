"""Host-only adjudication of failed reads recorded as legacy unknown effects."""

from typing import NoReturn

from pydantic import ValidationError

from groupagent.domain import DomainError
from groupagent.tools import EffectState, ToolGateway, ToolScope
from groupagent.tools.gateway import tool_call_fingerprint
from groupagent.tools.models import ToolFailureDisposition, ToolReceipt

from .models import EngineCheckpoint, RunState


def reconcile_read_only_checkpoint(
    checkpoint: EngineCheckpoint, *, gateway: ToolGateway
) -> EngineCheckpoint:
    """Return an auditable copy without executing tools or changing accounting.

    The host must first stop the previous controller/worker, retain the original
    checkpoint, and freshly authorize this work. Only the actual current gateway
    registrations may establish the read contract. Gateway-local quiescence is
    checked here; a new gateway cannot establish that another process stopped.
    Every unknown result must match its exact proposal, ledger and tool message.
    Provider charge uncertainty remains unchanged and never becomes a refund.
    """

    def refuse(message: str) -> NoReturn:
        raise DomainError("recovery_required", message)

    if checkpoint.state != RunState.PAUSED or checkpoint.error_code != "effect_unknown":
        refuse("Only a legacy unknown-effect pause can use read reconciliation")
    if checkpoint.backend_state.get("model_recovery_pending", False):
        refuse("An unresolved model attempt cannot use read reconciliation")

    request = checkpoint.request
    scope = ToolScope(
        group_id=request.actor.group_id,
        work_id=request.work_id,
        actor=request.actor,
        workspace=request.workspace,
        allowed_tools=request.allowed_tools,
        trace_id=request.trace_id,
    )
    gateway.bind_scope(scope)
    if not gateway.is_quiescent(scope):
        raise DomainError("work_busy", "A previous tool has not acknowledged termination")

    unknown: dict[str, ToolReceipt] = {}
    by_call: dict[str, ToolReceipt] = {}
    for receipt in checkpoint.receipts:
        if receipt.effect_state != EffectState.UNKNOWN:
            continue
        if (
            receipt.success
            or not receipt.error_code
            or receipt.error_code == "tool_in_progress"
            or receipt.tool_name not in request.allowed_tools
            or not gateway.is_read_only(receipt.tool_name)
            or receipt.idempotency_key != f"{request.work_id}:{receipt.call_id}"
        ):
            refuse("An unknown action is not a registered failed read")
        if receipt.idempotency_key in unknown or receipt.call_id in by_call:
            refuse("Legacy failed read identities are not unique")
        unknown[receipt.idempotency_key] = receipt
        by_call[receipt.call_id] = receipt
    if not unknown or checkpoint.unresolved_tools != frozenset(
        receipt.tool_name for receipt in unknown.values()
    ):
        refuse("Legacy unresolved tools do not match the failed read receipts")
    if any(
        sum(receipt.idempotency_key == key for receipt in checkpoint.receipts) != 1
        or sum(receipt.call_id == original.call_id for receipt in checkpoint.receipts) != 1
        for key, original in unknown.items()
    ):
        refuse("Legacy failed read identity conflicts with another receipt")

    seen: set[str] = set()
    ledger_unknown: dict[str, str] = {}
    for entry in checkpoint.ledger:
        receipt = entry.receipt
        key = receipt.idempotency_key
        if key in seen or receipt not in checkpoint.receipts:
            refuse("Legacy ledger does not match the recorded receipts")
        seen.add(key)
        if receipt.effect_state == EffectState.UNKNOWN:
            if unknown.get(key) != receipt:
                refuse("Legacy unknown ledger action has no matching failed read")
            ledger_unknown[key] = entry.fingerprint
    if ledger_unknown.keys() != unknown.keys():
        refuse("Legacy failed reads lack matching unknown ledger entries")
    checkpoint_ledger = {entry.receipt.idempotency_key: entry for entry in checkpoint.ledger}
    for entry in gateway.export_ledger(scope):
        if (
            entry.receipt.effect_state == EffectState.UNKNOWN
            and checkpoint_ledger.get(entry.receipt.idempotency_key) != entry
        ):
            refuse("The active gateway has an unrecorded or different unknown action")

    proposals: set[str] = set()
    tool_messages: set[str] = set()
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
    messages = []
    for message in checkpoint.messages:
        for call in message.tool_calls:
            original = by_call.get(call.id)
            if original is None:
                continue
            try:
                fingerprint = tool_call_fingerprint(call.name, call.arguments)
            except (TypeError, ValueError):
                refuse("Legacy failed read proposal has invalid arguments")
            if (
                message.role != "assistant"
                or call.id in proposals
                or call.name != original.tool_name
                or fingerprint != ledger_unknown[original.idempotency_key]
            ):
                refuse("Legacy failed read proposal differs from its ledger")
            proposals.add(call.id)
        original = by_call.get(message.tool_call_id or "") if message.role == "tool" else None
        if original is None:
            messages.append(message)
            continue
        try:
            recorded = ToolReceipt.model_validate_json(message.content or "")
        except ValidationError:
            refuse("Legacy failed read tool message is not a recorded receipt")
        if (
            recorded != original
            or original.call_id in tool_messages
            or original.call_id not in proposals
        ):
            refuse("Legacy failed read tool message differs from its proposal or receipt")
        tool_messages.add(original.call_id)
        messages.append(
            message.model_copy(
                update={"content": reconciled[original.idempotency_key].model_dump_json()}
            )
        )
    if proposals != by_call.keys() or tool_messages != by_call.keys():
        refuse("Legacy failed read lacks its exact proposal and tool message")

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
