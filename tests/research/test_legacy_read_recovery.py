"""Explicit legacy read reconciliation has no persistence or provider dependency."""

from dataclasses import replace

import pytest

from groupagent.domain import ActorContext, DomainError, Role
from groupagent.execution import (
    EngineCheckpoint,
    ExecutionRequest,
    ModelMessage,
    ModelReply,
    RunState,
    ScopedToolEngine,
    ScriptedFakeModel,
    TokenUsage,
)
from groupagent.execution.models import PendingInput, UsageTotals
from groupagent.research.recovery import reconcile_legacy_research_checkpoint
from groupagent.tools import EffectState, IdempotencyMode, ToolData, ToolDefinition, ToolGateway
from groupagent.tools.models import ToolLedgerEntry, ToolReceipt


async def never_called(arguments, context):
    raise AssertionError("Checkpoint reconciliation must not invoke a handler")


@pytest.fixture
def legacy(tmp_path):
    actor = ActorContext(group_id="synthetic-group", member_id="synthetic-owner", role=Role.OWNER)
    succeeded = ToolReceipt(
        call_id="known-search",
        tool_name="web_search",
        idempotency_key="work:known-search",
        success=True,
        effect_state=EffectState.SUCCEEDED,
        output={"budget_receipt": "synthetic-settled-fee"},
    )
    failed = ToolReceipt(
        call_id="failed-fetch",
        tool_name="web_fetch",
        idempotency_key="work:failed-fetch",
        success=False,
        effect_state=EffectState.UNKNOWN,
        error_code="page_unavailable",
        message="The tool did not complete",
    )
    checkpoint = EngineCheckpoint(
        request=ExecutionRequest(
            work_id="work",
            attempt_id="original-attempt",
            actor=actor,
            objective="Synthetic research",
            workspace=tmp_path.resolve(),
            allowed_tools=frozenset({"web_search", "web_fetch"}),
            trace_id="synthetic-trace",
        ),
        revision=1,
        state=RunState.PAUSED,
        sequence=15,
        pending_inputs=(
            PendingInput(revision=1, input_id="synthetic-input", text="Keep useful sources"),
        ),
        messages=(
            ModelMessage(role="user", content="Synthetic original request"),
            ModelMessage(
                role="tool", tool_call_id=succeeded.call_id, content=succeeded.model_dump_json()
            ),
            ModelMessage(
                role="tool", tool_call_id=failed.call_id, content=failed.model_dump_json()
            ),
        ),
        usage=UsageTotals(rounds=3, tokens=123, tool_calls=2, active_seconds=12.5),
        receipts=(succeeded, failed),
        ledger=tuple(
            ToolLedgerEntry(fingerprint=digit * 64, receipt=receipt)
            for digit, receipt in zip(("a", "b"), (succeeded, failed), strict=True)
        ),
        unresolved_tools=frozenset({"web_fetch"}),
        safe_to_resume=False,
        error_code="effect_unknown",
        backend_state={
            "model_recovery_pending": False,
            "uncertain_model_usage": True,
            "continuation_marker": {"preserve": "synthetic-host-state"},
        },
    )
    definitions = {
        name: ToolDefinition(
            name=name,
            description="Synthetic current read registration",
            input_model=ToolData,
            output_model=ToolData,
            handler=never_called,
            risk_class="read_only",
            idempotency_mode=IdempotencyMode.READ_ONLY,
        )
        for name in ("web_search", "web_fetch")
    }
    return checkpoint, definitions


def test_recovery_derives_a_copy_without_resetting_usage_or_prior_history(legacy):
    original, definitions = legacy
    before = original.model_dump_json()
    derived = reconcile_legacy_research_checkpoint(original, definitions=definitions)

    assert original.model_dump_json() == before
    assert derived.state == RunState.PAUSED and derived.safe_to_resume
    assert derived.error_code is None and not derived.unresolved_tools
    assert derived.sequence == original.sequence + 1
    for field in (
        "request",
        "revision",
        "pending_inputs",
        "usage",
        "backend_state",
        "candidate_text",
    ):
        assert getattr(derived, field) == getattr(original, field)
    assert derived.receipts[0] == original.receipts[0]
    assert derived.ledger[0] == original.ledger[0]
    for previous, current in zip(original.receipts, derived.receipts, strict=True):
        assert current.model_copy(update={"effect_state": previous.effect_state}) == previous
    assert derived.receipts[-1].effect_state == EffectState.NO_EFFECT
    assert derived.ledger[-1].receipt == derived.receipts[-1]
    assert derived.ledger[-1].fingerprint == original.ledger[-1].fingerprint
    assert derived.messages[:-1] == original.messages[:-1]
    assert ToolReceipt.model_validate_json(derived.messages[-1].content) == derived.receipts[-1]


async def test_derived_checkpoint_passes_normal_engine_resume_without_repeating_reads(legacy):
    checkpoint, definitions = legacy
    gateway = ToolGateway()
    for definition in definitions.values():
        gateway.register(definition)
    derived = reconcile_legacy_research_checkpoint(checkpoint, definitions=definitions)
    model = ScriptedFakeModel(
        [
            ModelReply(
                text="Synthetic useful partial result",
                usage=TokenUsage(input_tokens=1, output_tokens=1),
            )
        ]
    )
    engine = ScopedToolEngine(model, gateway)
    outcome = await engine.wait(
        await engine.resume(derived, checkpoint.request.actor, attempt_id="recovered-attempt")
    )
    assert outcome.state == RunState.CANDIDATE, (
        outcome.error_code,
        outcome.checkpoint.backend_state,
    )
    assert outcome.usage.tool_calls == checkpoint.usage.tool_calls
    assert outcome.usage.rounds == checkpoint.usage.rounds + 1
    assert outcome.usage.tokens == checkpoint.usage.tokens + 2
    assert outcome.checkpoint.receipts == derived.receipts


@pytest.mark.parametrize(
    "change", ["unregistered", "write_risk", "reconcile_mode", "name_mismatch"]
)
def test_current_registration_must_prove_the_exact_read_contract(legacy, change):
    checkpoint, definitions = legacy
    definitions = dict(definitions)
    if change == "unregistered":
        definitions.pop("web_fetch")
    else:
        updates = {
            "write_risk": {"risk_class": "local_draft"},
            "reconcile_mode": {"idempotency_mode": IdempotencyMode.RECONCILE_BEFORE_RETRY},
            "name_mismatch": {"name": "another_tool"},
        }
        definitions["web_fetch"] = replace(definitions["web_fetch"], **updates[change])
    with pytest.raises(DomainError) as failure:
        reconcile_legacy_research_checkpoint(checkpoint, definitions=definitions)
    assert failure.value.code == "recovery_required"


@pytest.mark.parametrize(
    "change",
    [
        "model_unknown",
        "write_unknown",
        "ledger_missing",
        "ledger_orphan",
        "ledger_mismatch",
        "message_mismatch",
        "message_missing",
        "unresolved_mismatch",
        "success_unknown",
        "receipt_identity_conflict",
    ],
)
def test_incomplete_or_conflicting_checkpoint_is_never_made_safe(legacy, change):
    checkpoint, definitions = legacy
    failed = checkpoint.receipts[-1]
    if change == "model_unknown":
        checkpoint = checkpoint.model_copy(
            update={"backend_state": {"model_recovery_pending": True}}
        )
    elif change in {"write_unknown", "success_unknown"}:
        failed = failed.model_copy(
            update={"tool_name": "wechat_send"} if change == "write_unknown" else {"success": True}
        )
        checkpoint = checkpoint.model_copy(update={"receipts": (checkpoint.receipts[0], failed)})
    elif change == "ledger_missing":
        checkpoint = checkpoint.model_copy(update={"ledger": checkpoint.ledger[:-1]})
    elif change in {"ledger_orphan", "ledger_mismatch"}:
        changed = failed.model_copy(
            update={"idempotency_key": "unrecorded"}
            if change == "ledger_orphan"
            else {"error_code": "another_error"}
        )
        checkpoint = checkpoint.model_copy(
            update={
                "ledger": (
                    checkpoint.ledger[0],
                    checkpoint.ledger[-1].model_copy(update={"receipt": changed}),
                )
            }
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
        checkpoint = checkpoint.model_copy(update={"unresolved_tools": frozenset({"wechat_send"})})
    elif change == "receipt_identity_conflict":
        checkpoint = checkpoint.model_copy(
            update={
                "receipts": (
                    *checkpoint.receipts,
                    failed.model_copy(
                        update={"success": True, "effect_state": EffectState.SUCCEEDED}
                    ),
                )
            }
        )
    with pytest.raises(DomainError) as failure:
        reconcile_legacy_research_checkpoint(checkpoint, definitions=definitions)
    assert failure.value.code == "recovery_required"
