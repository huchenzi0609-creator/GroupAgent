import pytest

from groupagent.domain import DomainError
from groupagent.execution import (
    ExecutionBudget,
    ModelReply,
    RunState,
    ScopedToolEngine,
    ScriptedFakeModel,
    TokenUsage,
)


class RecordingFakeBudget:
    def __init__(self, deny=False):
        self.deny = deny
        self.reservations = []
        self.settlements = []

    async def reserve(self, request, *, round_number, input_tokens, output_tokens):
        if self.deny:
            raise DomainError("budget_exhausted", "Synthetic budget exhausted")
        self.reservations.append((request.work_id, round_number, input_tokens, output_tokens))
        return "synthetic-reservation"

    async def settle(self, reservation_id, usage):
        self.settlements.append((reservation_id, usage))


@pytest.mark.parametrize("deny", [False, True])
async def test_money_reservation_precedes_call_and_uses_actual_usage(
    execution_request, gateway, deny
):
    budget = RecordingFakeBudget(deny)
    usage = TokenUsage(input_tokens=10, output_tokens=5)
    model = ScriptedFakeModel([ModelReply(text="Synthetic candidate", usage=usage)])
    engine = ScopedToolEngine(model, gateway, model_budget=budget)
    result = await engine.wait(await engine.start(execution_request))
    if deny:
        assert result.state == RunState.PAUSED and result.error_code == "budget_exhausted"
        assert not model.requests and result.usage.tokens == 0
        assert not budget.settlements
    else:
        assert result.state == RunState.CANDIDATE and len(budget.reservations) == 1
        assert budget.settlements == [("synthetic-reservation", usage)]


async def test_unknown_provider_usage_does_not_refund_reservation(execution_request, gateway):
    budget = RecordingFakeBudget()
    model = ScriptedFakeModel([], delay_seconds=100)
    engine = ScopedToolEngine(model, gateway, model_budget=budget)
    request = execution_request.model_copy(
        update={"budget": ExecutionBudget(model_timeout_seconds=0.01)}
    )
    result = await engine.wait(await engine.start(request))
    assert result.error_code == "model_timeout"
    assert budget.settlements == [("synthetic-reservation", None)]
    assert not result.checkpoint.safe_to_resume
