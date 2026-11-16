"""Legacy compatibility recovery requires complete host and original-call proof."""

from __future__ import annotations

from dataclasses import replace

import pytest

from groupagent.domain import ActorContext, DomainError, Role
from groupagent.execution import (
    EngineCheckpoint,
    ExecutionRequest,
    ModelMessage,
    ModelToolCall,
    RunState,
)
from groupagent.execution.models import UsageTotals
from groupagent.runtime.recovery import (
    RecoveryEvidence,
    checkpoint_digest,
    reconcile_unpublished_compute_checkpoint,
)
from groupagent.tools import EffectState, ToolGateway, ToolReceipt
from groupagent.tools.compute import register_compute_tools
from groupagent.tools.gateway import tool_call_fingerprint
from groupagent.tools.models import ToolLedgerEntry


class NeverRun:
    allowed_programs = frozenset({"python3", "ls", "cat", "wc", "head", "tail", "sort"})

    async def run(self, spec, context):
        raise AssertionError("Reconciliation cannot rerun the original computation")


@pytest.fixture
def legacy_draft(tmp_path):
    gateway = ToolGateway()
    register_compute_tools(gateway, NeverRun())
    arguments = {"code": "print('synthetic probe')", "output_paths": ["declared.txt"]}
    receipt = ToolReceipt(
        call_id="failed-call",
        tool_name="python_compute",
        idempotency_key="synthetic-work:failed-call",
        success=False,
        effect_state=EffectState.UNKNOWN,
        error_code="worker_failed",
    )
    checkpoint = EngineCheckpoint(
        request=ExecutionRequest(
            work_id="synthetic-work",
            attempt_id="synthetic-attempt",
            actor=ActorContext(
                group_id="synthetic-group", member_id="synthetic-owner", role=Role.OWNER
            ),
            objective="Finish the synthetic authorized report",
            workspace=tmp_path.resolve(),
            trace_id="synthetic-trace",
            allowed_tools=frozenset({"python_compute"}),
        ),
        revision=1,
        state=RunState.PAUSED,
        sequence=9,
        safe_to_resume=False,
        error_code="effect_unknown",
        unresolved_tools=frozenset({"python_compute"}),
        receipts=(receipt,),
        ledger=(
            ToolLedgerEntry(
                fingerprint=tool_call_fingerprint("python_compute", arguments), receipt=receipt
            ),
        ),
        messages=(
            ModelMessage(
                role="assistant",
                tool_calls=(
                    ModelToolCall(id=receipt.call_id, name=receipt.tool_name, arguments=arguments),
                ),
            ),
            ModelMessage(
                role="tool", tool_call_id=receipt.call_id, content=receipt.model_dump_json()
            ),
        ),
        usage=UsageTotals(rounds=1000, tokens=900000, tool_calls=100, active_seconds=10000),
        backend_state={"uncertain_model_usage": True},
    )
    evidence = RecoveryEvidence(
        checkpoint_digest(checkpoint),
        True,
        True,
        True,
        True,
        "synthetic-host-proof:immutable-publication-history",
    )
    return checkpoint, gateway, evidence


def test_verified_legacy_worker_failure_becomes_repair_observation_without_replay_or_lost_usage(
    legacy_draft,
):
    checkpoint, gateway, evidence = legacy_draft
    restored = reconcile_unpublished_compute_checkpoint(
        checkpoint, gateway=gateway, evidence=evidence
    )
    assert restored.safe_to_resume and restored.sequence == checkpoint.sequence + 1
    assert restored.usage == checkpoint.usage
    assert restored.receipts[0].effect_state == EffectState.NO_EFFECT
    assert restored.receipts[0].error_code == "worker_failed"
    assert restored.ledger[0].receipt == restored.receipts[0]
    assert ToolReceipt.model_validate_json(restored.messages[-1].content) == restored.receipts[0]
    assert restored.backend_state["uncertain_model_usage"] is True
    assert checkpoint.receipts[0].effect_state == EffectState.UNKNOWN
    assert checkpoint.error_code == "effect_unknown" and not checkpoint.safe_to_resume


@pytest.mark.parametrize(
    "field",
    ["controller_stopped", "workers_absent", "publication_history_clear", "native_delivery_clear"],
)
def test_current_file_absence_cannot_replace_immutable_history_or_stopped_owner_proof(
    legacy_draft, field
):
    checkpoint, gateway, evidence = legacy_draft
    with pytest.raises(DomainError, match="proof is incomplete"):
        reconcile_unpublished_compute_checkpoint(
            checkpoint, gateway=gateway, evidence=replace(evidence, **{field: False})
        )


@pytest.mark.parametrize(
    "change",
    [
        "changed_proposal",
        "missing_observation",
        "missing_ledger",
        "different_checkpoint",
        "published_file",
        "in_flight",
    ],
)
def test_incomplete_or_conflicting_original_effect_evidence_stays_fenced(legacy_draft, change):
    checkpoint, gateway, evidence = legacy_draft
    if change == "changed_proposal":
        call = (
            checkpoint.messages[0]
            .tool_calls[0]
            .model_copy(update={"arguments": {"code": "different"}})
        )
        checkpoint = checkpoint.model_copy(
            update={
                "messages": (
                    checkpoint.messages[0].model_copy(update={"tool_calls": (call,)}),
                    checkpoint.messages[1],
                )
            }
        )
    elif change == "missing_observation":
        checkpoint = checkpoint.model_copy(update={"messages": checkpoint.messages[:1]})
    elif change == "missing_ledger":
        checkpoint = checkpoint.model_copy(update={"ledger": ()})
    elif change == "published_file":
        (checkpoint.request.workspace / "declared.txt").write_text("Synthetic produced output")
    elif change == "in_flight":
        receipt = checkpoint.receipts[0].model_copy(update={"error_code": "tool_in_progress"})
        checkpoint = checkpoint.model_copy(update={"receipts": (receipt,)})
    if change != "different_checkpoint":
        evidence = replace(evidence, checkpoint_digest=checkpoint_digest(checkpoint))
    else:
        evidence = replace(evidence, checkpoint_digest="a" * 64)
    with pytest.raises(DomainError):
        reconcile_unpublished_compute_checkpoint(checkpoint, gateway=gateway, evidence=evidence)


def test_an_external_action_cannot_be_adjudicated_from_a_compute_name_in_output(legacy_draft):
    checkpoint, gateway, evidence = legacy_draft
    gateway._tools.clear()
    with pytest.raises(DomainError):
        reconcile_unpublished_compute_checkpoint(checkpoint, gateway=gateway, evidence=evidence)
