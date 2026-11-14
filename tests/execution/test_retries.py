import asyncio

from groupagent.domain import DomainError
from groupagent.execution import ExecutionBudget, ModelReply, RunState, ScopedToolEngine, TokenUsage
from groupagent.execution.errors import ModelRejected, retry_after_seconds


class RejectingFakeModel:
    name = "synthetic-retry-fixture"

    def __init__(self, errors):
        self.errors = list(errors)
        self.calls = 0
        self.started = asyncio.Event()

    async def complete(self, request):
        self.calls += 1
        self.started.set()
        if self.errors:
            raise self.errors.pop(0)
        return ModelReply(
            text="Synthetic candidate", usage=TokenUsage(input_tokens=10, output_tokens=5)
        )


class BudgetFixture:
    def __init__(self):
        self.reserved = []
        self.settled = []

    async def reserve(self, request, *, round_number, input_tokens, output_tokens):
        self.reserved.append((round_number, input_tokens + output_tokens))
        return str(round_number)

    async def settle(self, reservation_id, usage):
        self.settled.append((reservation_id, usage))


async def test_confirmed_rejections_retry_and_each_attempt_remains_budgeted(
    execution_request, gateway
):
    budget = BudgetFixture()
    model = RejectingFakeModel([ModelRejected(0), ModelRejected(0)])
    engine = ScopedToolEngine(model, gateway, model_budget=budget)
    result = await engine.wait(await engine.start(execution_request))
    assert result.state == RunState.CANDIDATE and model.calls == 3
    assert result.usage.rounds == 3
    assert result.usage.tokens == sum(value for _, value in budget.reserved[:2]) + 15
    assert [entry[1] for entry in budget.settled[:2]] == [None, None]
    assert budget.settled[-1][1].total == 15


async def test_retry_limit_and_remaining_round_budget_are_not_reset(execution_request, gateway):
    model = RejectingFakeModel([ModelRejected(0)] * 10)
    engine = ScopedToolEngine(model, gateway)
    result = await engine.wait(await engine.start(execution_request))
    assert (
        result.state == RunState.PAUSED and model.calls == 3 and result.error_code == "rate_limited"
    )
    other = RejectingFakeModel([ModelRejected(0)] * 10)
    engine = ScopedToolEngine(other, gateway)
    request = execution_request.model_copy(
        update={"attempt_id": "limited-rounds", "budget": ExecutionBudget(max_rounds=1)}
    )
    result = await engine.wait(await engine.start(request))
    assert other.calls == 1 and result.error_code == "round_budget_exhausted"


async def test_unknown_outcome_does_not_auto_retry(execution_request, gateway):
    model = RejectingFakeModel(
        [DomainError("provider_unavailable", "Lost response", retryable=True)]
    )
    engine = ScopedToolEngine(model, gateway)
    result = await engine.wait(await engine.start(execution_request))
    assert model.calls == 1 and result.state == RunState.PAUSED
    assert result.usage.tokens > 0 and not result.checkpoint.safe_to_resume


async def test_retry_backoff_is_cancelable_and_capped(execution_request, gateway):
    model = RejectingFakeModel([ModelRejected(300)])
    engine = ScopedToolEngine(model, gateway)
    run_id = await engine.start(execution_request)
    await model.started.wait()
    await engine.cancel(run_id, execution_request.actor)
    result = await asyncio.wait_for(engine.wait(run_id), 0.5)
    assert result.state == RunState.CANCELLED and model.calls == 1
    assert retry_after_seconds("300") == 300
    assert retry_after_seconds("nan") is None and retry_after_seconds("not a date") is None


async def test_checkpoint_flush_uses_unique_ordered_sequences(execution_request, gateway):
    from groupagent.execution import ScriptedFakeModel

    model = ScriptedFakeModel(
        [ModelReply(text="Candidate", usage=TokenUsage(input_tokens=10, output_tokens=5))],
        delay_seconds=0.1,
    )
    checkpoints = []
    events = []

    async def checkpoint_sink(checkpoint):
        await asyncio.sleep(0.001)
        checkpoints.append(checkpoint)

    async def event_sink(event):
        await asyncio.sleep(0.001)
        events.append(event)

    engine = ScopedToolEngine(model, gateway)
    engine.bind_persistence(event_sink, checkpoint_sink)
    run_id = await engine.start(execution_request)
    await model.started.wait()
    await asyncio.gather(*(engine.flush_checkpoint(run_id) for _ in range(3)))
    result = await engine.wait(run_id)
    assert result.state == RunState.CANDIDATE and result.checkpoint.candidate_text == "Candidate"
    assert [event.sequence for event in events] == list(range(1, len(events) + 1))
    assert len({checkpoint.sequence for checkpoint in checkpoints}) == len(checkpoints)
    assert all(
        a.usage.active_seconds <= b.usage.active_seconds
        for a, b in zip(checkpoints, checkpoints[1:], strict=False)
    )
