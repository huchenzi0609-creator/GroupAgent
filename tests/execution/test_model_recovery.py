"""Bounded pure-inference recovery; no provider, database or sandbox is contacted."""

import asyncio
import json
from types import SimpleNamespace

import pytest

from groupagent.domain import DomainError
from groupagent.execution import (
    ExecutionBudget,
    HarnessEngine,
    ModelMessage,
    ModelReply,
    ModelRequest,
    ModelToolCall,
    RunState,
    ScopedToolEngine,
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


async def test_live_recovery_keeps_unknown_reservation_and_does_not_replay_tools(
    execution_request, gateway
):
    budget = RecordingBudget()
    model = ModelSequence([write_reply(), unavailable(), final_reply()])
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


@pytest.mark.parametrize("outcomes", [[unavailable()] * 3, [unavailable(), ModelRejected(0)] * 2])
async def test_exhausted_live_recovery_remains_unsafe_and_cannot_restart(
    execution_request, gateway, outcomes
):
    model = ModelSequence(outcomes)
    budget = RecordingBudget()
    engine = ScopedToolEngine(model, gateway, model_budget=budget)
    result = await engine.wait(await engine.start(retry_request(execution_request)))
    assert result.state == RunState.PAUSED and len(model.requests) == 3
    assert result.usage.rounds == 3 and not result.checkpoint.safe_to_resume
    assert result.usage.tokens == sum(budget.reserved.values())
    assert all(usage is None for usage in budget.settled.values())
    with pytest.raises(DomainError) as failure:
        await engine.resume(result.checkpoint, execution_request.actor, attempt_id="unsafe-restart")
    assert failure.value.code == "recovery_required" and len(model.requests) == 3


@pytest.mark.parametrize("constraint", ["round", "token", "disabled"])
async def test_each_retry_rechecks_work_limits(execution_request, gateway, scope, constraint):
    limits = {}
    expected = "provider_unavailable"
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
    model = ModelSequence([unavailable(), final_reply()])
    budget = RecordingBudget()
    engine = ScopedToolEngine(model, gateway, model_budget=budget)
    result = await engine.wait(await engine.start(retry_request(execution_request, **limits)))
    assert result.error_code == expected and len(model.requests) == 1
    assert len(budget.reserved) == 1 and result.usage.rounds == 1
    assert budget.settled == {"1": None} and not result.checkpoint.safe_to_resume


@pytest.mark.parametrize("stop", ["cancel", "time"])
async def test_live_recovery_wait_respects_cancel_and_active_deadline(
    execution_request, gateway, stop
):
    model = ModelSequence([unavailable(), final_reply()])
    engine = None

    async def on_event(event):
        if event.kind == "progress" and event.payload.reason == "model_retry_wait":
            if stop == "cancel":
                await engine.cancel(event.attempt_id, execution_request.actor)
            else:
                run = engine._get(event.attempt_id)
                run.active_since = asyncio.get_running_loop().time() - 1201

    engine = ScopedToolEngine(model, gateway, event_sink=on_event)
    result = await engine.wait(await engine.start(retry_request(execution_request)))
    assert result.error_code == ("cancelled" if stop == "cancel" else "time_budget_exhausted")
    assert len(model.requests) == 1 and not result.checkpoint.safe_to_resume


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


class ActiveProcess:
    async def wait(self):
        await asyncio.Event().wait()


def harness_fixture(request, gateway, model, budget):
    # Only the host relay is exercised. This runner is never invoked.
    engine = HarnessEngine(model, gateway, object(), model_budget=budget)
    run = engine._create(retry_request(request))
    run.active_since = asyncio.get_running_loop().time()
    engine._bridges[request.attempt_id] = SimpleNamespace(process=ActiveProcess())
    tools = gateway.describe(run.scope)
    payload = {
        "messages": [{"role": "user", "content": request.objective}],
        "tools": [
            {
                **tool,
                "function": {
                    **tool["function"],
                    "name": "mcp__groupagent__" + tool["function"]["name"],
                },
            }
            for tool in tools
        ],
    }
    return engine, run, payload


async def test_host_harness_retries_only_model_and_preserves_previous_tool_receipt(
    execution_request, gateway
):
    model = ModelSequence([write_reply("mcp__groupagent__"), unavailable(), final_reply()])
    budget = RecordingBudget()
    engine, run, payload = harness_fixture(execution_request, gateway, model, budget)
    proposed = await engine._relay_model(run, payload)
    receipt = await engine._relay_tool(
        run,
        {"name": "workspace_write_text", "arguments": proposed.tool_calls[0].arguments},
        list(proposed.tool_calls),
        run.revision,
    )
    payload["messages"].append({"role": "user", "content": str(receipt)})
    reply = await engine._relay_model(run, payload)
    assert reply.text == "Synthetic candidate" and len(run.receipts) == 1
    assert model.requests[1] == model.requests[2]
    assert run.usage.rounds == 3 and run.usage.tokens == 30 + budget.reserved["2"] + 15
    assert budget.settled["2"] is None
    assert run.backend_state["uncertain_model_usage"] is True
    assert run.backend_state["model_recovery_pending"] is False


async def test_host_harness_exhaustion_cannot_clear_unknown_recovery(execution_request, gateway):
    model = ModelSequence([unavailable(), ModelRejected(0), unavailable()])
    budget = RecordingBudget()
    engine, run, payload = harness_fixture(execution_request, gateway, model, budget)
    with pytest.raises(DomainError) as failure:
        await engine._relay_model(run, payload)
    assert failure.value.code == "provider_unavailable"
    assert len(model.requests) == run.usage.rounds == 3 and not run.safe_to_resume
    assert run.usage.tokens == sum(budget.reserved.values())
    assert run.backend_state["model_recovery_pending"] is True
    assert all(value is None for value in budget.settled.values())


class SyntheticHarnessProcess:
    """In-memory SDK protocol fixture; never starts a native worker or Docker."""

    def __init__(self):
        self.commands = asyncio.Queue()
        self.stdout = asyncio.StreamReader()
        self.stdin = self
        self.returncode = None
        self.ended = asyncio.Event()

    def write(self, data):
        self.commands.put_nowait(json.loads(data))

    async def drain(self):
        pass

    def is_closing(self):
        return self.returncode is not None

    async def wait(self):
        await self.ended.wait()
        return self.returncode

    def kill(self):
        self.returncode = 0
        self.ended.set()

    def emit(self, message):
        self.stdout.feed_data(json.dumps(message).encode() + b"\n")


class SyntheticHarnessRunner:
    def __init__(self, process):
        self.process = process

    async def preflight(self):
        return {"harness_sdk": "0.2.2rc2"}

    def _argv(self, *args, **kwargs):
        return ["synthetic-image"]

    def _env(self, home):
        return {}

    def track_container(self, *args):
        pass

    async def remove_container(self, *args):
        self.process.kill()


async def test_host_harness_finished_checkpoint_separates_unknown_fees_from_safe_execution(
    execution_request, gateway, monkeypatch
):
    process = SyntheticHarnessProcess()

    async def create_process(*args, **kwargs):
        return process

    monkeypatch.setattr(asyncio, "create_subprocess_exec", create_process)
    model = ModelSequence([write_reply("mcp__groupagent__"), unavailable(), final_reply()])
    budget = RecordingBudget()
    checkpoints = []

    async def save(checkpoint):
        checkpoints.append(checkpoint)

    engine = HarnessEngine(
        model,
        gateway,
        SyntheticHarnessRunner(process),
        model_budget=budget,
        checkpoint_sink=save,
    )

    async def native_protocol_fixture():
        initial = await process.commands.get()
        tools = [
            {
                **tool,
                "function": {
                    **tool["function"],
                    "name": "mcp__groupagent__" + tool["function"]["name"],
                },
            }
            for tool in initial["tools"]
        ]
        payload = {"messages": [{"role": "user", "content": initial["prompt"]}], "tools": tools}
        process.emit({"type": "request", "id": "first", "method": "model", "payload": payload})
        first = await process.commands.get()
        call = first["result"]["choices"][0]["message"]["tool_calls"][0]
        process.emit(
            {
                "type": "request",
                "id": "effect",
                "method": "tool",
                "payload": {
                    "name": call["function"]["name"].removeprefix("mcp__groupagent__"),
                    "arguments": json.loads(call["function"]["arguments"]),
                },
            }
        )
        effect = await process.commands.get()
        payload["messages"].append({"role": "user", "content": json.dumps(effect["result"])})
        process.emit({"type": "request", "id": "next", "method": "model", "payload": payload})
        recovered = await process.commands.get()
        assert recovered["response_to"] == "next" and "error" not in recovered
        process.emit(
            {
                "type": "finished",
                "finish_reason": "completed",
                "text": recovered["result"]["choices"][0]["message"]["content"],
                "sessions": "synthetic-native-session-checkpoint",
            }
        )

    protocol = asyncio.create_task(native_protocol_fixture())
    try:
        result = await asyncio.wait_for(
            engine.wait(await engine.start(retry_request(execution_request))), 2
        )
        await protocol
    finally:
        protocol.cancel()
        await asyncio.gather(protocol, return_exceptions=True)
    assert result.state == RunState.CANDIDATE and result.checkpoint.safe_to_resume
    assert result.checkpoint.backend_state["sessions"] == "synthetic-native-session-checkpoint"
    assert result.checkpoint.backend_state["uncertain_model_usage"] is True
    assert result.checkpoint.backend_state["model_recovery_pending"] is False
    persisted = checkpoints[-1]
    assert persisted.state == RunState.CANDIDATE and persisted.safe_to_resume
    assert persisted.usage == result.usage and persisted.receipts == result.receipts
    assert persisted.backend_state == result.checkpoint.backend_state
    assert any(
        not item.safe_to_resume and item.backend_state.get("model_recovery_pending") is True
        for item in checkpoints[:-1]
    )
    assert len(result.receipts) == result.usage.tool_calls == 1
    assert result.usage.rounds == 3 and result.usage.tokens == 30 + budget.reserved["2"] + 15
    assert budget.settled["2"] is None and set(budget.reserved) == {"1", "2", "3"}


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
    assert result.error_code == "budget_exhausted" and not result.checkpoint.safe_to_resume
    assert result.checkpoint.backend_state["model_recovery_pending"] is True
    assert len(model.requests) == result.usage.rounds == 1
    assert result.usage.tokens == budget.reserved["1"] and budget.settled["1"] is None
