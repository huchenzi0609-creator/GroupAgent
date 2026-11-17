from __future__ import annotations

import pytest

from groupagent.domain import DomainError, Role
from groupagent.execution import (
    EngineCheckpoint,
    ExecutionBudget,
    ModelMessage,
    ModelReply,
    RunState,
    ScopedToolEngine,
    ScriptedFakeModel,
    TokenUsage,
)
from groupagent.execution.models import UsageTotals
from groupagent.tools import ToolGateway


class RecordingFakeBudget:
    def __init__(self):
        self.reservations, self.settlements = [], []

    async def reserve(self, request, *, round_number, input_tokens, output_tokens):
        self.reservations.append((request.work_id, round_number, input_tokens, output_tokens))
        return "synthetic-reservation"

    async def settle(self, identifier, usage):
        self.settlements.append((identifier, usage))


def paused_checkpoint(request, *, tokens=144764):
    return EngineCheckpoint(
        request=request,
        revision=request.revision,
        state=RunState.PAUSED,
        sequence=1,
        messages=(ModelMessage(role="user", content="Synthetic retained context " + "x" * 60_000),),
        usage=UsageTotals(rounds=9, tokens=tokens, tool_calls=13),
        safe_to_resume=True,
    )


def answer():
    return ModelReply(
        text="Synthetic resumed candidate", usage=TokenUsage(input_tokens=20, output_tokens=10)
    )


def test_default_has_no_cumulative_token_cap_and_legacy_integer_roundtrips():
    assert ExecutionBudget().max_tokens is None
    assert (
        ExecutionBudget.model_validate_json(ExecutionBudget().model_dump_json()).max_tokens is None
    )
    assert ExecutionBudget.model_validate({"max_tokens": 200000}).max_tokens == 200000


async def test_owner_can_remove_old_cap_and_resume_without_resetting_usage(execution_request):
    legacy = ExecutionBudget(max_tokens=200000, max_output_tokens=6000)
    request = execution_request.model_copy(update={"budget": legacy, "allowed_tools": frozenset()})
    checkpoint = paused_checkpoint(request)
    money = RecordingFakeBudget()
    blocked_model = ScriptedFakeModel([answer()])
    blocked = ScopedToolEngine(blocked_model, ToolGateway(), model_budget=money)
    result = await blocked.wait(
        await blocked.resume(checkpoint, request.actor, attempt_id="old-cap")
    )
    assert result.error_code == "token_budget_exhausted" and not blocked_model.requests
    assert money.reservations == []

    model = ScriptedFakeModel([answer()])
    engine = ScopedToolEngine(model, ToolGateway(), model_budget=money)
    owner = request.actor.model_copy(update={"role": Role.OWNER})
    unlimited = legacy.model_copy(update={"max_tokens": None})
    result = await engine.wait(
        await engine.resume(checkpoint, owner, attempt_id="without-cap", budget=unlimited)
    )
    assert result.state == RunState.CANDIDATE and len(model.requests) == 1
    assert result.usage == checkpoint.usage.model_copy(
        update={"rounds": 10, "tokens": 144794, "active_seconds": result.usage.active_seconds}
    )
    assert result.checkpoint.request.budget.max_tokens is None
    assert model.requests[0].max_output_tokens == 6000
    assert money.reservations[0][2] > 60_000 and money.reservations[0][3] == 6000
    assert money.settlements == [
        ("synthetic-reservation", TokenUsage(input_tokens=20, output_tokens=10))
    ]


async def test_member_cannot_remove_cap_through_direct_engine_resume(execution_request):
    request = execution_request.model_copy(update={"budget": ExecutionBudget(max_tokens=200000)})
    engine = ScopedToolEngine(ScriptedFakeModel([answer()]), ToolGateway())
    with pytest.raises(DomainError) as error:
        await engine.resume(
            paused_checkpoint(request),
            request.actor,
            attempt_id="unauthorized",
            budget=ExecutionBudget(),
        )
    assert error.value.code == "permission_denied"
