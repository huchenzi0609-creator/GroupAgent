"""Bounded pure-inference recovery; no provider, database or sandbox is contacted."""

import asyncio

import pytest

from groupagent.domain import DomainError
from groupagent.execution import (
    ExecutionBudget,
    ModelMessage,
    ModelReply,
    ModelRequest,
    ModelToolCall,
    RunState,
    ScopedToolEngine,
    ScriptedFakeModel,
    TokenUsage,
)
from groupagent.execution.engine import SYSTEM_PROMPT
from groupagent.execution.errors import ModelRejected
from groupagent.execution.ports import estimate_input_tokens


class ModelSequence:
    name = "synthetic-live-recovery-model"

    def __init__(self, outcomes):
        self.outcomes = list(outcomes)
        self.requests = []

    async def complete(self, request):
        self.requests.append(request)
        outcome = self.outcomes.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome


class RecordingBudget:
    def __init__(self):
        self.reserved = {}
        self.settled = {}

    async def reserve(self, request, *, round_number, input_tokens, output_tokens):
        key = str(round_number)
        assert key not in self.reserved
        self.reserved[key] = input_tokens + output_tokens
        return key

    async def settle(self, reservation_id, usage):
        assert reservation_id not in self.settled
        self.settled[reservation_id] = usage


def unavailable():
    return DomainError("provider_unavailable", "Synthetic response lost")


def malformed():
    return DomainError("invalid_model_response", "Synthetic response or usage cannot be parsed")


def final_reply():
    return ModelReply(
        text="Synthetic candidate", usage=TokenUsage(input_tokens=10, output_tokens=5)
    )


def write_reply(prefix=""):
    return ModelReply(
        tool_calls=(
            ModelToolCall(
                id="known-write",
                name=prefix + "workspace_write_text",
                arguments={"path": "once.txt", "text": "Synthetic content"},
            ),
        ),
        finish_reason="tool_calls",
        usage=TokenUsage(input_tokens=20, output_tokens=10),
    )


def retry_request(request, **budget):
    return request.model_copy(
        update={"budget": ExecutionBudget(max_retry_after_seconds=0, **budget)}
    )


@pytest.mark.parametrize("failure", [unavailable, malformed])
async def test_live_recovery_keeps_unknown_reservation_and_does_not_replay_tools(
    execution_request, gateway, failure
):
    budget = RecordingBudget()
    model = ModelSequence([write_reply(), failure(), final_reply()])
    engine = ScopedToolEngine(model, gateway, model_budget=budget)
    result = await engine.wait(await engine.start(retry_request(execution_request)))
    assert result.state == RunState.CANDIDATE and result.usage.rounds == 3
    assert len(result.receipts) == result.usage.tool_calls == 1
    assert result.receipts[0].success and not result.receipts[0].replayed
    assert (execution_request.workspace / "once.txt").read_text() == "Synthetic content"
    assert model.requests[1] == model.requests[2]
    assert budget.settled["2"] is None and set(budget.reserved) == {"1", "2", "3"}
    assert result.usage.tokens == 30 + budget.reserved["2"] + 15
    assert result.checkpoint.safe_to_resume
    assert result.checkpoint.backend_state["uncertain_model_usage"] is True
    assert result.checkpoint.backend_state["model_recovery_pending"] is False


@pytest.mark.parametrize(
    "outcomes",
    [[unavailable()] * 3, [unavailable(), ModelRejected(0)] * 2, [malformed()] * 3],
)
async def test_exhausted_inference_recovery_retains_unknown_fees_and_can_continue(
    execution_request, gateway, outcomes
):
    model = ModelSequence(outcomes)
    budget = RecordingBudget()
    engine = ScopedToolEngine(model, gateway, model_budget=budget)
    result = await engine.wait(await engine.start(retry_request(execution_request)))
    assert result.state == RunState.PAUSED and len(model.requests) == 3
    assert result.usage.rounds == 3 and result.checkpoint.safe_to_resume
    assert result.usage.tokens == sum(budget.reserved.values())
    assert all(usage is None for usage in budget.settled.values())
    restored_model = ModelSequence([final_reply()])
    restored = ScopedToolEngine(restored_model, gateway, model_budget=budget)
    continued = await restored.wait(
        await restored.resume(result.checkpoint, execution_request.actor, attempt_id="funded-retry")
    )
    assert continued.state == RunState.CANDIDATE and continued.usage.rounds == 4
    assert continued.usage.tokens == result.usage.tokens + 15
    assert set(budget.reserved) == {"1", "2", "3", "4"}
    assert all(budget.settled[key] is None for key in ("1", "2", "3"))


@pytest.mark.parametrize("constraint", ["round", "token", "disabled"])
@pytest.mark.parametrize("failure", [unavailable, malformed])
async def test_each_retry_rechecks_work_limits(
    execution_request, gateway, scope, constraint, failure
):
    limits = {}
    expected = failure().code
    if constraint == "round":
        limits["max_rounds"] = 1
        expected = "round_budget_exhausted"
    elif constraint == "token":
        initial = ModelRequest(
            messages=(
                ModelMessage(role="system", content=SYSTEM_PROMPT),
                ModelMessage(role="user", content=execution_request.objective),
            ),
            tools=gateway.describe(scope),
            max_output_tokens=1,
        )
        limits.update(max_tokens=estimate_input_tokens(initial) + 1, max_output_tokens=1)
        expected = "token_budget_exhausted"
    else:
        limits["additional_retry_limit"] = 0
    model = ModelSequence([failure(), final_reply()])
    budget = RecordingBudget()
    engine = ScopedToolEngine(model, gateway, model_budget=budget)
    result = await engine.wait(await engine.start(retry_request(execution_request, **limits)))
    assert result.error_code == expected and len(model.requests) == 1
    assert len(budget.reserved) == 1 and result.usage.rounds == 1
    assert budget.settled == {"1": None} and result.checkpoint.safe_to_resume


@pytest.mark.parametrize("stop", ["cancel", "time"])
@pytest.mark.parametrize("failure", [unavailable, malformed])
async def test_live_recovery_wait_respects_cancel_and_active_deadline(
    execution_request, gateway, stop, failure
):
    model = ModelSequence([failure(), final_reply()])
    engine = None

    async def on_event(event):
        if event.kind == "progress" and event.payload.reason == "model_retry_wait":
            if stop == "cancel":
                await engine.cancel(event.attempt_id, execution_request.actor)
            else:
                run = engine._get(event.attempt_id)
                run.active_since = asyncio.get_running_loop().time() - 1201

    engine = ScopedToolEngine(model, gateway, event_sink=on_event)
    request = retry_request(execution_request, max_active_seconds=1200)
    result = await engine.wait(await engine.start(request))
    assert result.error_code == ("cancelled" if stop == "cancel" else "time_budget_exhausted")
    assert len(model.requests) == 1 and result.checkpoint.safe_to_resume


async def test_timeout_with_late_model_result_is_never_retried_or_dispatched(
    execution_request, gateway
):
    release = asyncio.Event()

    class UncooperativeModel:
        name = "synthetic-late-model"
        calls = 0

        async def complete(self, request):
            self.calls += 1
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                await release.wait()
            return write_reply()

    model = UncooperativeModel()
    engine = ScopedToolEngine(model, gateway)
    request = retry_request(execution_request, model_timeout_seconds=0.01)
    run_id = await engine.start(request)
    try:
        result = await asyncio.wait_for(engine.wait(run_id), 1)
        assert result.error_code == "model_timeout" and not result.checkpoint.safe_to_resume
        assert model.calls == 1 and not engine.is_quiescent(run_id)
    finally:
        release.set()
        await asyncio.gather(*engine._get(run_id).model_tasks)
    assert engine.is_quiescent(run_id)
    assert not (request.workspace / "once.txt").exists()
    assert not (await engine.inspect(run_id)).receipts


@pytest.mark.parametrize("code", ["permission_denied", "scope_violation", "invalid_model_reply"])
async def test_live_model_recovery_does_not_retry_policy_or_reply_errors(
    execution_request, gateway, code
):
    model = ModelSequence([DomainError(code, "Synthetic non-retryable boundary"), final_reply()])
    budget = RecordingBudget()
    engine = ScopedToolEngine(model, gateway, model_budget=budget)
    result = await engine.wait(await engine.start(retry_request(execution_request)))
    assert result.error_code == code and not result.checkpoint.safe_to_resume
    assert len(model.requests) == len(budget.reserved) == 1
    assert budget.settled == {"1": None}


async def test_settlement_failure_never_triggers_a_second_model_request(execution_request, gateway):
    class FailingBudget(RecordingBudget):
        async def settle(self, reservation_id, usage):
            await super().settle(reservation_id, usage)
            raise DomainError("billing_unavailable", "Synthetic settlement failure")

    budget = FailingBudget()
    model = ModelSequence([final_reply()])
    engine = ScopedToolEngine(model, gateway, model_budget=budget)
    result = await engine.wait(await engine.start(retry_request(execution_request)))
    assert result.error_code == "billing_unavailable" and not result.checkpoint.safe_to_resume
    assert len(model.requests) == result.usage.rounds == 1
    assert result.usage.tokens == budget.reserved["1"]
    assert result.checkpoint.backend_state["model_recovery_pending"] is True


async def test_retry_reservation_denial_does_not_clear_the_previous_unknown_boundary(
    execution_request, gateway
):
    class DenySecondReservation(RecordingBudget):
        async def reserve(self, request, *, round_number, input_tokens, output_tokens):
            if self.reserved:
                raise DomainError("budget_exhausted", "Synthetic daily budget exhausted")
            return await super().reserve(
                request,
                round_number=round_number,
                input_tokens=input_tokens,
                output_tokens=output_tokens,
            )

    budget = DenySecondReservation()
    model = ModelSequence([unavailable(), final_reply()])
    engine = ScopedToolEngine(model, gateway, model_budget=budget)
    result = await engine.wait(await engine.start(retry_request(execution_request)))
    assert result.error_code == "budget_exhausted" and result.checkpoint.safe_to_resume
    assert result.checkpoint.backend_state["model_recovery_pending"] is False
    assert len(model.requests) == result.usage.rounds == 1
    assert result.usage.tokens == budget.reserved["1"] and budget.settled["1"] is None


async def test_accounting_returns_and_replays_retained_reply_without_second_inference(
    execution_request, gateway
):
    from groupagent.execution import accounting_recovery_checkpoint

    class RecoverableOfflineLedger:
        def __init__(self):
            self.reserved = []
            self.settled = {}
            self.recovered = []
            self.lose_reply = True

        async def reserve(self, request, *, round_number, input_tokens, output_tokens):
            identifier = f"reservation-{round_number}"
            self.reserved.append(identifier)
            return identifier

        async def settle(self, reservation_id, usage):
            assert reservation_id not in self.settled
            self.settled[reservation_id] = usage
            if self.lose_reply:
                self.lose_reply = False
                raise OSError("Synthetic response lost after the ledger committed")

        async def recover(self, request, reservation_id, usage):
            assert request.actor == execution_request.actor
            assert self.settled[reservation_id] == usage
            self.recovered.append(reservation_id)

    ledger = RecoverableOfflineLedger()
    model = ModelSequence([write_reply()])
    engine = ScopedToolEngine(model, gateway, model_budget=ledger)
    paused = await engine.wait(await engine.start(retry_request(execution_request)))
    assert paused.error_code == "billing_unavailable" and not paused.checkpoint.safe_to_resume
    assert accounting_recovery_checkpoint(paused.checkpoint)
    assert not (execution_request.workspace / "once.txt").exists()
    assert (
        paused.checkpoint.backend_state["pending_model_settlement"]["reply"]["tool_calls"][0]["id"]
        == "known-write"
    )

    resumed_model = ModelSequence([final_reply()])
    resumed = ScopedToolEngine(resumed_model, gateway, model_budget=ledger)
    result = await resumed.wait(
        await resumed.resume(
            paused.checkpoint, execution_request.actor, attempt_id="ledger-returned"
        )
    )
    assert result.state == RunState.CANDIDATE and result.checkpoint.safe_to_resume
    assert result.usage.rounds == 2 and result.usage.tokens == 45
    assert len(model.requests) == len(resumed_model.requests) == 1
    assert ledger.reserved == ["reservation-1", "reservation-2"]
    assert ledger.recovered == ["reservation-1"]
    assert result.usage.tool_calls == 1 and len(result.receipts) == 1
    assert (execution_request.workspace / "once.txt").read_text() == "Synthetic content"
    assert "pending_model_settlement" not in result.checkpoint.backend_state
    assert "recovered_model_reply" not in result.checkpoint.backend_state


async def test_preserved_inflight_checkpoint_reconciles_unknown_charge_after_controller_stops(
    execution_request, gateway
):
    from groupagent.execution import EngineCheckpoint, reconcile_quiesced_inference_checkpoint

    class IdempotentOfflineLedger:
        def __init__(self):
            self.settlements = {}
            self.recoveries = []

        async def reserve(self, request, *, round_number, input_tokens, output_tokens):
            return f"held-{round_number}"

        async def settle(self, reservation_id, usage):
            if reservation_id in self.settlements:
                assert self.settlements[reservation_id] == usage
            self.settlements[reservation_id] = usage

        async def recover(self, request, reservation_id, usage):
            self.recoveries.append((reservation_id, usage))
            await self.settle(reservation_id, usage)

    ledger = IdempotentOfflineLedger()
    model = ScriptedFakeModel([write_reply()], delay_seconds=100)
    initial = ScopedToolEngine(model, gateway, model_budget=ledger)
    run_id = await initial.start(retry_request(execution_request))
    await model.started.wait()
    # Preserve the last durable in-flight boundary, as a killed process would
    # leave it. Join the fixture's old controller before host reconciliation.
    checkpoint = EngineCheckpoint.model_validate_json(
        (await initial.flush_checkpoint(run_id)).model_dump_json()
    )
    await initial.cancel(run_id, execution_request.actor)
    await initial.wait(run_id)
    assert initial.is_quiescent(run_id)
    reconciled = reconcile_quiesced_inference_checkpoint(checkpoint)
    assert reconciled.usage == checkpoint.usage and not reconciled.safe_to_resume
    assert reconciled.backend_state["pending_model_settlement"]["usage"] is None
    resumed_model = ModelSequence([final_reply()])
    resumed = ScopedToolEngine(resumed_model, gateway, model_budget=ledger)
    result = await resumed.wait(
        await resumed.resume(reconciled, execution_request.actor, attempt_id="stopped-inference")
    )
    assert result.state == RunState.CANDIDATE and result.checkpoint.safe_to_resume
    assert ledger.recoveries == [("held-1", None)]
    assert result.usage.tokens == checkpoint.usage.tokens + 15
    assert result.usage.rounds == 2 and len(resumed_model.requests) == 1
    assert not (execution_request.workspace / "once.txt").exists()
    assert result.checkpoint.backend_state["uncertain_model_usage"] is True


async def test_pre_admission_key_is_durable_when_reservation_commit_reply_is_lost(
    execution_request, gateway
):
    from groupagent.execution import EngineCheckpoint, accounting_recovery_checkpoint

    checkpoints = []

    async def save(checkpoint):
        checkpoints.append(EngineCheckpoint.model_validate_json(checkpoint.model_dump_json()))

    class KeyedOfflineLedger:
        def __init__(self):
            self.failed_once = False
            self.recoveries = []

        def recovery_key(self, request, *, round_number):
            return f"engine:{request.work_id}:{request.attempt_id}:{round_number}"

        async def reserve(self, request, *, round_number, input_tokens, output_tokens):
            assert checkpoints[-1].backend_state["inflight_model_reservation"][
                "reservation_id"
            ] == self.recovery_key(request, round_number=round_number)
            if not self.failed_once:
                self.failed_once = True
                raise OSError("Synthetic loss after ledger reservation committed")
            return f"reservation-{round_number}"

        async def recover(self, request, reservation_id, usage):
            assert reservation_id == f"engine:{request.work_id}:attempt-1:1" and usage is None
            self.recoveries.append(reservation_id)

        async def settle(self, reservation_id, usage):
            assert reservation_id == "reservation-2" and usage == final_reply().usage

    ledger = KeyedOfflineLedger()
    model = ModelSequence([])
    initial = ScopedToolEngine(model, gateway, model_budget=ledger, checkpoint_sink=save)
    paused = await initial.wait(await initial.start(retry_request(execution_request)))
    assert paused.error_code == "billing_unavailable" and accounting_recovery_checkpoint(
        paused.checkpoint
    )
    assert model.requests == []
    resumed_model = ModelSequence([final_reply()])
    resumed = ScopedToolEngine(resumed_model, gateway, model_budget=ledger, checkpoint_sink=save)
    result = await resumed.wait(
        await resumed.resume(
            paused.checkpoint, execution_request.actor, attempt_id="keyed-recovery"
        )
    )
    assert result.state == RunState.CANDIDATE and len(resumed_model.requests) == 1
    assert ledger.recoveries == [f"engine:{execution_request.work_id}:attempt-1:1"]
    assert result.usage.rounds == 2 and result.usage.tokens == paused.usage.tokens + 15
