"""Durable retry plans contain bounded state, not cumulative execution budgets."""

from datetime import UTC, datetime, timedelta

import pytest

from groupagent.domain import ActorContext, DomainError, Role, WorkOrder, WorkState
from groupagent.execution import EngineCheckpoint, ExecutionRequest, ModelMessage, RunState
from groupagent.execution.models import PendingModelSettlement, UsageTotals
from groupagent.runtime.executor import WorkSupervisor
from groupagent.runtime.recovery import (
    RECOVERY_SCOPE_KEY,
    automatic_recovery_pending,
    is_automatic_recovery_reason,
    plan_recovery,
    recovery_due,
    recovery_metadata,
    safe_recovery_checkpoint,
)
from groupagent.tools.models import EffectState, ToolLedgerEntry, ToolReceipt


@pytest.fixture
def recovery_work():
    now = datetime(2026, 11, 16, tzinfo=UTC)
    return WorkOrder(
        id="synthetic-work",
        group_id="synthetic-group",
        requester_id="synthetic-owner",
        request_event_id="synthetic-source",
        goal="Finish the same authorized work",
        created_at=now,
        updated_at=now,
    )


@pytest.fixture
def recovery_checkpoint(recovery_work, tmp_path):
    return EngineCheckpoint(
        request=ExecutionRequest(
            work_id=recovery_work.id,
            attempt_id="synthetic-attempt",
            actor=ActorContext(
                group_id=recovery_work.group_id,
                member_id=recovery_work.requester_id,
                role=Role.OWNER,
            ),
            objective=recovery_work.goal,
            workspace=tmp_path.resolve(),
            trace_id="synthetic-trace",
        ),
        revision=1,
        state=RunState.PAUSED,
        sequence=10,
        messages=(ModelMessage(role="user", content=recovery_work.goal),),
        safe_to_resume=True,
        usage=UsageTotals(rounds=500, tokens=100_000, tool_calls=1000, active_seconds=10_000),
        error_code="execution_stalled",
    )


def test_retry_deadline_survives_replanning_and_backoff_saturates_without_attempt_ceiling(
    recovery_work,
):
    now = recovery_work.created_at
    work = recovery_work
    for index in range(100):
        plan = plan_recovery(work, "execution_stalled", failure_key=f"attempt-{index}", now=now)
        work = work.model_copy(update={"scope": {RECOVERY_SCOPE_KEY: plan}})
        due = datetime.fromisoformat(plan["next_retry_at"])
        assert due - now == timedelta(seconds=min(5 * 2 ** min(index, 6), 300))
        assert not recovery_due(work, now=now)
        assert recovery_due(work, now=due)
        # Restart/polling cannot move the existing deadline forward forever.
        assert (
            plan_recovery(work, "execution_stalled", failure_key=f"attempt-{index}", now=due)
            == plan
        )
        assert len(str(plan)) < 2000
        now = due
    assert plan["consecutive_failures"] == 100 and plan["backoff_step"] == 6
    assert "different method" in plan["strategy_instruction"]
    assert "tight loop" in plan["strategy_instruction"]


@pytest.mark.parametrize(
    "reason",
    [
        "user_request",
        "cancelled",
        "policy_changed",
        "source_unavailable",
        "auth_required",
        "input_required",
        "budget_exhausted",
        "provider_reconciliation_required",
        "effect_unknown",
        "model_context_too_large",
    ],
)
def test_permission_input_billing_and_unknown_effects_never_receive_retry_plan(
    recovery_work, reason
):
    assert not is_automatic_recovery_reason(reason)
    with pytest.raises(DomainError, match="cannot be retried automatically"):
        plan_recovery(recovery_work, reason, failure_key="attempt")


@pytest.mark.parametrize("where", ["receipt", "ledger", "unresolved", "model_pending"])
def test_unknown_effect_or_active_model_cannot_be_replayed_despite_safe_flag(
    recovery_checkpoint, where
):
    receipt = ToolReceipt(
        call_id="native-write",
        tool_name="synthetic_write",
        idempotency_key="same-write",
        success=False,
        effect_state=EffectState.UNKNOWN,
    )
    changes = {
        "receipt": {"receipts": (receipt,)},
        "ledger": {"ledger": (ToolLedgerEntry(fingerprint="a" * 64, receipt=receipt),)},
        "unresolved": {"unresolved_tools": frozenset({"synthetic_write"})},
        "model_pending": {"backend_state": {"model_recovery_pending": True}},
    }[where]
    assert not safe_recovery_checkpoint(recovery_checkpoint.model_copy(update=changes))


def test_unknown_inference_cost_is_preserved_when_quiescent_checkpoint_is_safe(recovery_checkpoint):
    checkpoint = recovery_checkpoint.model_copy(
        update={"backend_state": {"uncertain_model_usage": True, "model_recovery_pending": False}}
    )
    assert safe_recovery_checkpoint(checkpoint)
    assert checkpoint.usage == recovery_checkpoint.usage
    assert checkpoint.backend_state["uncertain_model_usage"] is True


def test_accounting_wait_is_runnable_only_with_typed_quiescent_pending_settlement(
    recovery_checkpoint,
):
    pending = PendingModelSettlement(
        reservation_id="synthetic-held-charge", round_number=1, reserved_tokens=20
    )
    checkpoint = recovery_checkpoint.model_copy(
        update={
            "safe_to_resume": False,
            "error_code": "billing_unavailable",
            "backend_state": {
                "model_recovery_pending": True,
                "model_call_quiescent": True,
                "pending_model_settlement": pending.model_dump(mode="json"),
            },
        }
    )
    assert safe_recovery_checkpoint(checkpoint)
    assert is_automatic_recovery_reason("billing_unavailable")
    for changes in (
        {"model_call_quiescent": False},
        {"pending_model_settlement": {"reservation_id": "synthetic-held-charge"}},
    ):
        assert not safe_recovery_checkpoint(
            checkpoint.model_copy(update={"backend_state": {**checkpoint.backend_state, **changes}})
        )
    receipt = ToolReceipt(
        call_id="external-write",
        tool_name="native_send",
        idempotency_key="never-replay",
        success=False,
        effect_state=EffectState.UNKNOWN,
    )
    assert not safe_recovery_checkpoint(checkpoint.model_copy(update={"receipts": (receipt,)}))


def test_strategy_update_is_idempotent_preserves_usage_and_uses_pending_tool_boundary(
    recovery_work, recovery_checkpoint
):
    plan = plan_recovery(recovery_work, "execution_stalled", failure_key="old-attempt:10")
    work = recovery_work.model_copy(update={"scope": {RECOVERY_SCOPE_KEY: plan}})
    checkpoint = recovery_checkpoint.model_copy(
        update={
            "state": RunState.CANCELLED,
            "backend_state": {"scoped_pending_tools": {"synthetic": "pending"}},
        }
    )
    resumed = WorkSupervisor._recovery_instruction(checkpoint, work)
    assert resumed.state == RunState.PAUSED
    assert resumed.usage == checkpoint.usage and resumed.receipts == checkpoint.receipts
    assert resumed.messages == checkpoint.messages
    assert len(resumed.pending_inputs) == 1
    assert "different method" in resumed.pending_inputs[0].text
    assert WorkSupervisor._recovery_instruction(resumed, work) == resumed


def test_malformed_recovery_state_requires_reconciliation(recovery_work):
    work = recovery_work.model_copy(update={"scope": {RECOVERY_SCOPE_KEY: {"version": 1}}})
    with pytest.raises(DomainError) as failure:
        recovery_metadata(work)
    assert failure.value.code == "reconciliation_required"


def test_new_input_changes_only_retry_window_not_work_budget_or_usage(recovery_work):
    plan = plan_recovery(recovery_work, "execution_stalled", failure_key="attempt-1")
    work = recovery_work.model_copy(
        update={
            "scope": {RECOVERY_SCOPE_KEY: plan},
            "input_versions": ["genuinely-new-source"],
            "budget": {"max_cost_cny": 20},
        }
    )
    next_plan = plan_recovery(work, "execution_stalled", failure_key="attempt-2")
    assert next_plan["backoff_step"] == 0 and next_plan["consecutive_failures"] == 1
    assert work.budget == {"max_cost_cny": 20}


def test_presentation_requires_current_valid_schedule_and_never_converts_explicit_pause(
    recovery_work,
):
    assert not automatic_recovery_pending(recovery_work)
    plan = plan_recovery(recovery_work, "execution_stalled", failure_key="attempt-1")
    queued = recovery_work.model_copy(update={"scope": {RECOVERY_SCOPE_KEY: plan}})
    assert automatic_recovery_pending(queued)
    waiting = queued.model_copy(
        update={"state": WorkState.PAUSED, "pause_reason": "execution_stalled"}
    )
    assert automatic_recovery_pending(waiting)
    assert not automatic_recovery_pending(
        waiting.model_copy(update={"pause_reason": "user_request"})
    )
    assert not automatic_recovery_pending(waiting.model_copy(update={"goal_revision": 2}))
    assert not automatic_recovery_pending(
        waiting.model_copy(update={"input_versions": ["changed"]})
    )
    assert not automatic_recovery_pending(waiting.model_copy(update={"state": WorkState.CANCELLED}))
