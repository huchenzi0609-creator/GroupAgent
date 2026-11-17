"""Unlimited cumulative work with bounded per-call resources and loop recovery."""

import asyncio

import pytest
from pydantic import ValidationError

from groupagent.config import ExecutionConfig
from groupagent.domain import Role
from groupagent.execution import (
    ExecutionBudget,
    ModelReply,
    ModelRequest,
    ModelToolCall,
    RunState,
    ScopedToolEngine,
    ScriptedFakeModel,
    TokenUsage,
)
from groupagent.tools import ToolData, ToolDefinition


def proposal(index, *, name="workspace_write_text", arguments=None, prefix=""):
    return ModelReply(
        tool_calls=(
            ModelToolCall(
                id=f"call-{index}",
                name=prefix + name,
                arguments=(
                    arguments
                    if arguments is not None
                    else {"path": f"part-{index}.txt", "text": str(index)}
                ),
            ),
        ),
        finish_reason="tool_calls",
        usage=TokenUsage(input_tokens=20, output_tokens=10),
    )


def final():
    return ModelReply(
        text="Synthetic complete draft", usage=TokenUsage(input_tokens=20, output_tokens=10)
    )


def test_cumulative_limits_default_to_disabled_and_accept_legacy_values():
    for limits, names in (
        (
            ExecutionConfig(),
            ("max_model_rounds", "max_total_tokens", "max_tool_calls", "max_active_seconds"),
        ),
        (ExecutionBudget(), ("max_rounds", "max_tokens", "max_tool_calls", "max_active_seconds")),
    ):
        assert all(getattr(limits, name) is None for name in names)
        assert type(limits).model_validate_json(limits.model_dump_json()) == limits
    config = ExecutionConfig(max_model_rounds=24, max_tool_calls=60, max_active_seconds=1200)
    budget = ExecutionBudget(max_rounds=24, max_tool_calls=60, max_active_seconds=1200)
    assert config.max_model_rounds == budget.max_rounds == 24
    assert config.max_tool_calls == budget.max_tool_calls == 60
    assert config.max_active_seconds == budget.max_active_seconds == 1200


@pytest.mark.parametrize("value", [0, -1, float("nan"), float("inf")])
@pytest.mark.parametrize(
    "model,key",
    [
        (ExecutionConfig, "max_model_rounds"),
        (ExecutionConfig, "max_tool_calls"),
        (ExecutionConfig, "max_active_seconds"),
        (ExecutionBudget, "max_rounds"),
        (ExecutionBudget, "max_tool_calls"),
        (ExecutionBudget, "max_active_seconds"),
        (ExecutionBudget, "model_timeout_seconds"),
    ],
)
def test_disabled_limits_require_none_and_per_call_timeouts_stay_finite(model, key, value):
    with pytest.raises(ValidationError):
        model.model_validate({key: value})


async def test_changing_work_passes_old_round_call_and_active_time_caps(execution_request, gateway):
    model = ScriptedFakeModel([*(proposal(index) for index in range(70)), final()])
    engine = None

    async def age_run(event):
        if event.kind == "run_started":
            engine._get(event.attempt_id).active_since = asyncio.get_running_loop().time() - 1201

    engine = ScopedToolEngine(model, gateway, event_sink=age_run)
    result = await engine.wait(await engine.start(execution_request))
    assert result.state == RunState.CANDIDATE
    assert result.usage.rounds == 71 and result.usage.tool_calls == 70
    assert result.usage.active_seconds > 1200
    detector = result.checkpoint.backend_state["execution_progress_detector"]
    assert len(detector["recent"]) == 16 and detector["repeat_streak"] == 0
    assert all(receipt.success for receipt in result.receipts)


@pytest.mark.parametrize("cycle", [1, 2])
async def test_identical_observations_pause_as_recoverable_anomalies(
    execution_request, gateway, cycle
):
    for index in range(cycle):
        (execution_request.workspace / f"same-{index}.txt").write_text("same")
    replies = [
        proposal(index, name="workspace_read_text", arguments={"path": f"same-{index % cycle}.txt"})
        for index in range(20)
    ]
    model = ScriptedFakeModel(replies)
    engine = ScopedToolEngine(model, gateway)
    result = await engine.wait(await engine.start(execution_request))
    assert result.state == RunState.PAUSED and result.error_code == "activation_yielded"
    assert result.checkpoint.backend_state["activation_yield_reason"] == "execution_stalled"
    assert result.checkpoint.safe_to_resume
    assert result.usage.tool_calls == cycle + 6
    detector = result.checkpoint.backend_state["execution_progress_detector"]
    assert detector["repeat_streak"] == 6 and len(detector["recent"]) <= 16
    assert "same-" not in str(detector)  # Store bounded hashes, never raw arguments.


async def test_repeated_failed_call_pauses_before_spending_more_model_rounds(
    execution_request, gateway
):
    model = ScriptedFakeModel(
        [
            proposal(index, name="workspace_read_text", arguments={"path": "missing.txt"})
            for index in range(20)
        ]
    )
    engine = ScopedToolEngine(model, gateway)
    result = await engine.wait(await engine.start(execution_request))
    assert result.error_code == "activation_yielded" and result.state == RunState.PAUSED
    assert result.checkpoint.backend_state["activation_yield_reason"] == "execution_error_loop"
    assert result.checkpoint.safe_to_resume and result.usage.rounds == 4
    assert all(not receipt.success for receipt in result.receipts)


async def test_same_call_with_changing_observation_is_progress(execution_request, gateway):
    class PollInput(ToolData):
        pass

    class PollOutput(ToolData):
        processed: int

    count = 0

    async def poll(arguments, context):
        nonlocal count
        count += 1
        return PollOutput(processed=count)

    gateway.register(
        ToolDefinition(
            name="progress_poll",
            description="Synthetic progress",
            input_model=PollInput,
            output_model=PollOutput,
            handler=poll,
        )
    )
    request = execution_request.model_copy(update={"allowed_tools": frozenset({"progress_poll"})})
    replies = [proposal(index, name="progress_poll", arguments={}) for index in range(70)]
    engine = ScopedToolEngine(ScriptedFakeModel([*replies, final()]), gateway)
    result = await engine.wait(await engine.start(request))
    assert result.state == RunState.CANDIDATE and count == 70
    assert result.checkpoint.backend_state["execution_progress_detector"]["repeat_streak"] == 0


async def test_outbound_history_compacts_without_rewriting_raw_evidence(execution_request, gateway):
    replies = []
    for index in range(8):
        path = f"page-{index}.txt"
        (execution_request.workspace / path).write_text(f"Source {index}: " + "x" * 3000)
        replies.append(proposal(index, name="workspace_read_text", arguments={"path": path}))
    model = ScriptedFakeModel([*replies, final()])
    engine = ScopedToolEngine(model, gateway)
    budget = ExecutionBudget(max_input_bytes=20_000)
    request = execution_request.model_copy(update={"budget": budget})
    result = await engine.wait(await engine.start(request))
    assert result.state == RunState.CANDIDATE
    assert all(
        len(item.model_dump_json().encode()) <= budget.max_input_bytes for item in model.requests
    )
    assert any(
        "[groupagent-history-compaction:v1]" in item.model_dump_json() for item in model.requests
    )
    raw_request = ModelRequest(messages=result.checkpoint.messages, tools=(), max_output_tokens=1)
    assert len(raw_request.model_dump_json().encode()) <= budget.max_input_bytes
    assert len(result.receipts) == len(result.checkpoint.ledger) == 8
    assert all("x" * 3000 in receipt.output["text"] for receipt in result.receipts)


async def test_loop_detector_survives_checkpoint_resume(execution_request, gateway):
    args = {"path": "same.txt"}
    (execution_request.workspace / "same.txt").write_text("same")
    engine = ScopedToolEngine(
        ScriptedFakeModel(
            [proposal(index, name="workspace_read_text", arguments=args) for index in range(3)]
        ),
        gateway,
    )
    request = execution_request.model_copy(update={"budget": ExecutionBudget(max_rounds=3)})
    first = await engine.wait(await engine.start(request))
    assert first.error_code == "round_budget_exhausted"
    restored = ScopedToolEngine(
        ScriptedFakeModel(
            [proposal(index, name="workspace_read_text", arguments=args) for index in range(3, 20)]
        ),
        gateway,
    )
    owner = request.actor.model_copy(update={"role": Role.OWNER})
    result = await restored.wait(
        await restored.resume(
            first.checkpoint, owner, attempt_id="continued", budget=ExecutionBudget()
        )
    )
    assert result.error_code == "activation_yielded" and result.usage.tool_calls == 7
    assert result.checkpoint.backend_state["activation_yield_reason"] == "execution_stalled"
    assert result.checkpoint.safe_to_resume
