"""Read failures, legacy receipt adjudication and active-handler fences."""

import asyncio
from dataclasses import replace

import pytest

from groupagent.domain import DomainError
from groupagent.execution import (
    EngineCheckpoint,
    ModelMessage,
    ModelReply,
    ModelToolCall,
    RunState,
    ScopedToolEngine,
    ScriptedFakeModel,
    TokenUsage,
)
from groupagent.execution.models import UsageTotals
from groupagent.execution.recovery import reconcile_read_only_checkpoint
from groupagent.tools import (
    EffectState,
    IdempotencyMode,
    ToolData,
    ToolDefinition,
    ToolGateway,
)
from groupagent.tools.gateway import tool_call_fingerprint
from groupagent.tools.models import ToolLedgerEntry, ToolReceipt

pytestmark = pytest.mark.public_image_workflow


class InspectionInput(ToolData):
    value: str


class InspectionOutput(ToolData):
    observation: str


async def never_called(arguments, context):
    raise AssertionError("Reconciliation and replay must not invoke a handler")


def register(gateway, handler=never_called, *, mutable=False, timeout=1):
    gateway.register(
        ToolDefinition(
            name="inspect_item",
            description="Synthetic review of an existing item",
            input_model=InspectionInput,
            output_model=InspectionOutput,
            handler=handler,
            risk_class="local_draft" if mutable else "read_only",
            idempotency_mode=(
                IdempotencyMode.RECONCILE_BEFORE_RETRY if mutable else IdempotencyMode.READ_ONLY
            ),
            timeout_seconds=timeout,
        )
    )


def proposal(*values):
    return ModelReply(
        tool_calls=tuple(
            ModelToolCall(id=f"read-{index}", name="inspect_item", arguments={"value": value})
            for index, value in enumerate(values)
        ),
        finish_reason="tool_calls",
        usage=TokenUsage(input_tokens=3, output_tokens=2),
    )


def answer():
    return ModelReply(
        text="Useful available observations", usage=TokenUsage(input_tokens=3, output_tokens=2)
    )


@pytest.fixture
def legacy(execution_request):
    request = execution_request.model_copy(update={"allowed_tools": frozenset({"inspect_item"})})
    call = ModelToolCall(id="failed-inspection", name="inspect_item", arguments={"value": "five"})
    failed = ToolReceipt(
        call_id=call.id,
        tool_name=call.name,
        idempotency_key=f"{request.work_id}:{call.id}",
        success=False,
        effect_state=EffectState.UNKNOWN,
        error_code="invalid_media_request",
        output={"budget_receipt": "synthetic-unknown-fee"},
    )
    checkpoint = EngineCheckpoint(
        request=request,
        revision=request.revision,
        state=RunState.PAUSED,
        sequence=11,
        messages=(
            ModelMessage(role="user", content="Synthetic media inspection"),
            ModelMessage(role="assistant", tool_calls=(call,)),
            ModelMessage(role="tool", tool_call_id=call.id, content=failed.model_dump_json()),
        ),
        usage=UsageTotals(rounds=2, tokens=123, tool_calls=1, active_seconds=7.5),
        receipts=(failed,),
        ledger=(
            ToolLedgerEntry(
                fingerprint=tool_call_fingerprint(call.name, call.arguments), receipt=failed
            ),
        ),
        unresolved_tools=frozenset({call.name}),
        safe_to_resume=False,
        error_code="effect_unknown",
        backend_state={
            "model_recovery_pending": False,
            "uncertain_model_usage": True,
            "provider_reservations": {"synthetic-unknown-fee": "unknown"},
            "continuation_marker": {"preserve": "synthetic-state"},
        },
    )
    gateway = ToolGateway()
    register(gateway)
    return checkpoint, gateway


async def test_legacy_read_reconciliation_preserves_evidence_fees_and_usage_on_resume(legacy):
    original, gateway = legacy
    before = original.model_dump_json()
    derived = reconcile_read_only_checkpoint(original, gateway=gateway)
    assert original.model_dump_json() == before
    assert derived.sequence == original.sequence + 1
    assert derived.safe_to_resume and derived.error_code is None
    assert not derived.unresolved_tools
    assert derived.receipts[0].effect_state == EffectState.NO_EFFECT
    assert (
        derived.receipts[0].model_copy(update={"effect_state": EffectState.UNKNOWN})
        == original.receipts[0]
    )
    assert derived.ledger[0].fingerprint == original.ledger[0].fingerprint
    assert derived.ledger[0].receipt == derived.receipts[0]
    assert derived.messages[:-1] == original.messages[:-1]
    assert ToolReceipt.model_validate_json(derived.messages[-1].content) == derived.receipts[0]
    for field in ("request", "revision", "pending_inputs", "usage", "backend_state"):
        assert getattr(derived, field) == getattr(original, field)

    engine = ScopedToolEngine(ScriptedFakeModel([answer()]), gateway)
    result = await engine.wait(
        await engine.resume(derived, derived.request.actor, attempt_id="recovered")
    )
    assert result.state == RunState.CANDIDATE
    assert (
        result.receipts == derived.receipts and result.usage.tool_calls == original.usage.tool_calls
    )
    assert result.usage.tokens == original.usage.tokens + 5
    assert result.usage.rounds == original.usage.rounds + 1
    assert result.checkpoint.backend_state["provider_reservations"] == {
        "synthetic-unknown-fee": "unknown"
    }


@pytest.mark.parametrize(
    "change",
    [
        "model_unknown",
        "current_write_contract",
        "control_capability",
        "success_unknown",
        "in_progress",
        "identity_mismatch",
        "ledger_missing",
        "fingerprint_mismatch",
        "arguments_mismatch",
        "arguments_not_json",
        "proposal_missing",
        "proposal_duplicate",
        "message_missing",
        "message_mismatch",
        "unresolved_mismatch",
    ],
)
def test_reconciliation_refuses_missing_proof_or_an_unknown_write(legacy, change):
    checkpoint, gateway = legacy
    if change in {"current_write_contract", "control_capability"}:
        definition = gateway._tools["inspect_item"]
        gateway._tools["inspect_item"] = replace(
            definition,
            **(
                {
                    "risk_class": "local_draft",
                    "idempotency_mode": IdempotencyMode.RECONCILE_BEFORE_RETRY,
                }
                if change == "current_write_contract"
                else {"may_request_input": True}
            ),
        )
    elif change == "model_unknown":
        checkpoint = checkpoint.model_copy(
            update={"backend_state": {"model_recovery_pending": True}}
        )
    elif change in {"success_unknown", "in_progress", "identity_mismatch"}:
        changes = {
            "success_unknown": {"success": True},
            "in_progress": {"error_code": "tool_in_progress"},
            "identity_mismatch": {"idempotency_key": "unrelated:key"},
        }
        checkpoint = checkpoint.model_copy(
            update={"receipts": (checkpoint.receipts[0].model_copy(update=changes[change]),)}
        )
    elif change == "ledger_missing":
        checkpoint = checkpoint.model_copy(update={"ledger": ()})
    elif change == "fingerprint_mismatch":
        checkpoint = checkpoint.model_copy(
            update={"ledger": (checkpoint.ledger[0].model_copy(update={"fingerprint": "a" * 64}),)}
        )
    elif change in {"arguments_mismatch", "arguments_not_json"}:
        assistant = checkpoint.messages[1]
        call = assistant.tool_calls[0].model_copy(
            update={
                "arguments": {
                    "value": "changed" if change == "arguments_mismatch" else float("nan")
                }
            }
        )
        checkpoint = checkpoint.model_copy(
            update={
                "messages": (
                    checkpoint.messages[0],
                    assistant.model_copy(update={"tool_calls": (call,)}),
                    checkpoint.messages[2],
                )
            }
        )
    elif change == "proposal_missing":
        checkpoint = checkpoint.model_copy(
            update={"messages": (checkpoint.messages[0], checkpoint.messages[2])}
        )
    elif change == "proposal_duplicate":
        checkpoint = checkpoint.model_copy(
            update={"messages": (*checkpoint.messages, checkpoint.messages[1])}
        )
    elif change == "message_missing":
        checkpoint = checkpoint.model_copy(update={"messages": checkpoint.messages[:-1]})
    elif change == "message_mismatch":
        checkpoint = checkpoint.model_copy(
            update={
                "messages": (
                    *checkpoint.messages[:-1],
                    checkpoint.messages[-1].model_copy(update={"content": "{}"}),
                )
            }
        )
    elif change == "unresolved_mismatch":
        checkpoint = checkpoint.model_copy(update={"unresolved_tools": frozenset({"unrelated"})})
    before = checkpoint.model_dump_json()
    with pytest.raises(DomainError) as failure:
        reconcile_read_only_checkpoint(checkpoint, gateway=gateway)
    assert failure.value.code == "recovery_required"
    assert checkpoint.model_dump_json() == before


async def test_existing_gateway_ledger_only_accepts_exact_read_effect_adjudication(legacy):
    checkpoint, gateway = legacy
    scope_key = (
        checkpoint.request.actor.group_id,
        checkpoint.request.work_id,
        checkpoint.receipts[0].idempotency_key,
    )
    original = checkpoint.ledger[0]
    gateway._ledger[scope_key] = (original.fingerprint, original.receipt)
    derived = reconcile_read_only_checkpoint(checkpoint, gateway=gateway)
    model = ScriptedFakeModel([answer()])
    engine = ScopedToolEngine(model, gateway)
    result = await engine.wait(
        await engine.resume(derived, checkpoint.request.actor, attempt_id="live-recovered")
    )
    assert result.state == RunState.CANDIDATE
    assert gateway.export_ledger(engine._get("live-recovered").scope) == derived.ledger

    gateway._ledger[scope_key] = (original.fingerprint, original.receipt)
    altered = derived.model_copy(
        update={
            "ledger": (
                derived.ledger[0].model_copy(
                    update={
                        "receipt": derived.receipts[0].model_copy(
                            update={"output": {"forged": "result"}}
                        )
                    }
                ),
            )
        }
    )
    with pytest.raises(DomainError) as failure:
        await engine.resume(altered, checkpoint.request.actor, attempt_id="altered")
    assert failure.value.code == "idempotency_conflict"
    assert gateway._ledger[scope_key] == (original.fingerprint, original.receipt)


def test_unrecorded_live_unknown_action_is_never_omitted_by_recovery(legacy):
    checkpoint, gateway = legacy
    live = checkpoint.receipts[0].model_copy(
        update={"call_id": "another-action", "idempotency_key": "another-key"}
    )
    scope_key = (
        checkpoint.request.actor.group_id,
        checkpoint.request.work_id,
        live.idempotency_key,
    )
    gateway._ledger[scope_key] = ("c" * 64, live)
    with pytest.raises(DomainError) as failure:
        reconcile_read_only_checkpoint(checkpoint, gateway=gateway)
    assert failure.value.code == "recovery_required"
    scope = next(iter(gateway._scopes.values()))
    with pytest.raises(DomainError) as failure:
        gateway.restore_ledger(scope, ())
    assert failure.value.code == "recovery_required"
    assert gateway.inspect_effect(scope.group_id, scope.work_id, live.idempotency_key) == live


async def test_existing_unknown_write_cannot_be_adjudicated_by_ledger_restore(legacy):
    checkpoint, gateway = legacy
    derived = reconcile_read_only_checkpoint(checkpoint, gateway=gateway)
    gateway._tools["inspect_item"] = replace(
        gateway._tools["inspect_item"],
        risk_class="local_draft",
        idempotency_mode=IdempotencyMode.RECONCILE_BEFORE_RETRY,
    )
    original = checkpoint.ledger[0]
    scope_key = (
        checkpoint.request.actor.group_id,
        checkpoint.request.work_id,
        original.receipt.idempotency_key,
    )
    gateway._ledger[scope_key] = (original.fingerprint, original.receipt)
    engine = ScopedToolEngine(ScriptedFakeModel([answer()]), gateway)
    with pytest.raises(DomainError) as failure:
        await engine.resume(derived, checkpoint.request.actor, attempt_id="cannot-launder-write")
    assert failure.value.code == "idempotency_conflict"
    assert gateway._ledger[scope_key] == (original.fingerprint, original.receipt)


@pytest.mark.parametrize(
    "code", ["invalid_media_request", "provider_unavailable", "future_read_failure"]
)
async def test_failed_read_continues_with_other_evidence_and_preserves_held_fee(
    execution_request, code
):
    held_fees, calls = {}, []

    async def inspect(arguments, context):
        calls.append(arguments.value)
        if arguments.value == "unavailable":
            held_fees[context.idempotency_key] = "unknown"
            raise DomainError(code, "Synthetic failed inspection")
        return InspectionOutput(observation="Synthetic available result")

    gateway = ToolGateway()
    register(gateway, inspect)
    model = ScriptedFakeModel([proposal("unavailable", "available"), answer()])
    engine = ScopedToolEngine(model, gateway)
    request = execution_request.model_copy(update={"allowed_tools": frozenset({"inspect_item"})})
    result = await engine.wait(await engine.start(request))
    assert result.state == RunState.CANDIDATE and result.checkpoint.safe_to_resume
    assert calls == ["unavailable", "available"]
    assert result.receipts[0].error_code == code
    assert result.receipts[0].effect_state == EffectState.NO_EFFECT
    assert not result.checkpoint.unresolved_tools
    assert held_fees == {f"{request.work_id}:read-0": "unknown"}
    assert code in model.requests[-1].messages[-2].content


async def test_schema_rejection_never_starts_inspection_and_model_can_correct_it(execution_request):
    calls = []

    async def inspect(arguments, context):
        calls.append(arguments.value)
        return InspectionOutput(observation=arguments.value)

    gateway = ToolGateway()
    register(gateway, inspect)
    model = ScriptedFakeModel([proposal(5), proposal("corrected"), answer()])
    engine = ScopedToolEngine(model, gateway)
    request = execution_request.model_copy(update={"allowed_tools": frozenset({"inspect_item"})})
    result = await engine.wait(await engine.start(request))
    assert result.state == RunState.CANDIDATE and calls == ["corrected"]
    assert result.receipts[0].error_code == "invalid_input"
    assert result.receipts[0].effect_state == EffectState.NOT_STARTED
    assert len(result.checkpoint.ledger) == 1


async def test_noncooperative_serial_read_blocks_later_model_resume_and_new_start(
    execution_request,
):
    release = asyncio.Event()
    starts = []

    async def inspect(arguments, context):
        starts.append(arguments.value)
        while not release.is_set():
            try:
                await release.wait()
            except asyncio.CancelledError:
                continue
        return InspectionOutput(observation="Late synthetic result")

    gateway = ToolGateway()
    register(gateway, inspect, timeout=0.01)
    model = ScriptedFakeModel([proposal("first", "second"), answer()])
    engine = ScopedToolEngine(model, gateway)
    request = execution_request.model_copy(update={"allowed_tools": frozenset({"inspect_item"})})
    run = await engine.start(request)
    try:
        result = await asyncio.wait_for(engine.wait(run), 1)
        assert result.state == RunState.PAUSED and result.error_code == "work_busy"
        assert not result.checkpoint.safe_to_resume and starts == ["first"]
        assert len(model.requests) == 1 and not engine.is_quiescent(run)
        with pytest.raises(DomainError) as failure:
            await engine.start(request.model_copy(update={"attempt_id": "overlapping-start"}))
        assert failure.value.code == "work_busy"
        with pytest.raises(DomainError) as failure:
            gateway.restore_ledger(engine._get(run).scope, ())
        assert failure.value.code == "work_busy"
    finally:
        release.set()
        await gateway.quiesce(engine._get(run).scope, timeout_seconds=0.2)


async def test_proven_read_reconciliation_updates_trusted_disposition_without_losing_evidence(
    legacy,
):
    from groupagent.tools.models import (
        ToolFailureDisposition,
        ToolFailureMetadata,
        ToolFailureOrigin,
    )

    legacy, _original_gateway = legacy
    original = legacy.receipts[0].model_copy(
        update={
            "failure": ToolFailureMetadata(
                origin=ToolFailureOrigin.EXECUTION,
                disposition=ToolFailureDisposition.RECONCILE,
            )
        }
    )
    checkpoint = legacy.model_copy(
        update={
            "receipts": (original,),
            "ledger": (legacy.ledger[0].model_copy(update={"receipt": original}),),
            "messages": tuple(
                message.model_copy(update={"content": original.model_dump_json()})
                if message.role == "tool"
                else message
                for message in legacy.messages
            ),
        }
    )
    gateway = ToolGateway()
    register(gateway)
    reconciled = reconcile_read_only_checkpoint(checkpoint, gateway=gateway)
    assert checkpoint.receipts[0].effect_state == EffectState.UNKNOWN
    assert checkpoint.receipts[0].failure.disposition == ToolFailureDisposition.RECONCILE
    assert reconciled.receipts[0].effect_state == EffectState.NO_EFFECT
    assert reconciled.receipts[0].failure == ToolFailureMetadata(
        origin=ToolFailureOrigin.EXECUTION,
        disposition=ToolFailureDisposition.REPAIR,
    )
    assert reconciled.receipts[0].output == original.output and reconciled.usage == checkpoint.usage
    assert reconciled.ledger[0].receipt == reconciled.receipts[0]
    model = ScriptedFakeModel([answer()])
    engine = ScopedToolEngine(model, gateway)
    result = await engine.wait(
        await engine.resume(
            reconciled,
            reconciled.request.actor,
            attempt_id="read-proof-classified",
        )
    )
    assert result.state == RunState.CANDIDATE and len(model.requests) == 1
    assert result.receipts[0].failure.disposition == ToolFailureDisposition.REPAIR
