import asyncio
from pathlib import Path

import pytest

from groupagent.domain import ActorContext, DomainError, Role
from groupagent.execution import (
    EngineCheckpoint,
    ExecutionBudget,
    ModelReply,
    ModelToolCall,
    RunState,
    ScopedToolEngine,
    ScriptedFakeModel,
    TokenUsage,
)
from groupagent.tools import EffectState, ToolContext, ToolData, ToolDefinition, ToolGateway
from groupagent.tools.models import ToolFailure


def answer(text: str = "Candidate draft ready.", **overrides) -> ModelReply:
    return ModelReply(text=text, usage=TokenUsage(input_tokens=20, output_tokens=10), **overrides)


def call(name: str = "workspace_write_text", *, call_id: str = "call-1", **arguments) -> ModelReply:
    return ModelReply(
        tool_calls=(ModelToolCall(id=call_id, name=name, arguments=arguments),),
        finish_reason="tool_calls",
        usage=TokenUsage(input_tokens=20, output_tokens=10),
    )


async def test_tool_loop_returns_candidate_and_ordered_typed_events(
    execution_request, gateway
) -> None:
    model = ScriptedFakeModel(
        [
            call(path="draft.txt", text="Synthetic content"),
            call("workspace_read_text", call_id="read", path="draft.txt"),
            answer(),
        ]
    )
    engine = ScopedToolEngine(model, gateway)
    run_id = await engine.start(execution_request)

    async def consume():
        return [event async for event in engine.events(run_id)]

    consumer = asyncio.create_task(consume())
    result = await engine.wait(run_id)
    events = await asyncio.wait_for(consumer, 1)
    assert result.state == RunState.CANDIDATE
    assert result.usage.rounds == 3 and result.usage.tool_calls == 2
    assert [event.sequence for event in events] == list(range(1, len(events) + 1))
    assert events[-1].kind == "run_finished"
    assert events[-1].payload.state == RunState.CANDIDATE
    assert all(event.time.tzinfo is not None for event in events)
    assert all(receipt.success for receipt in result.receipts)
    assert (execution_request.workspace / "draft.txt").read_text() == "Synthetic content"
    assert (await engine.inspect(run_id)).state.value != "completed"
    replay = [event async for event in engine.events(run_id, after_sequence=events[-2].sequence)]
    assert replay == [events[-1]]


@pytest.mark.parametrize(
    "budget,expected",
    [
        (ExecutionBudget(max_tokens=1), "token_budget_exhausted"),
        (ExecutionBudget(max_input_bytes=10), "model_context_too_large"),
    ],
)
async def test_request_budget_is_checked_before_any_model_call(
    execution_request, gateway, budget, expected
):
    model = ScriptedFakeModel([answer()])
    engine = ScopedToolEngine(model, gateway)
    result = await engine.wait(
        await engine.start(execution_request.model_copy(update={"budget": budget}))
    )
    assert result.state == RunState.PAUSED and result.error_code == expected
    assert model.requests == [] and result.candidate_text is None


async def test_tool_batch_budget_is_checked_before_side_effects(execution_request, gateway) -> None:
    reply = call(path="must-not-exist.txt", text="x")
    reply = reply.model_copy(
        update={
            "tool_calls": (
                reply.tool_calls[0],
                reply.tool_calls[0].model_copy(update={"id": "second-call"}),
            )
        }
    )
    model = ScriptedFakeModel([reply])
    engine = ScopedToolEngine(model, gateway)
    execution_request = execution_request.model_copy(
        update={"budget": ExecutionBudget(max_tool_calls=1)}
    )
    result = await engine.wait(await engine.start(execution_request))
    assert result.error_code == "tool_budget_exhausted"
    assert (
        result.receipts == () and not (execution_request.workspace / "must-not-exist.txt").exists()
    )


async def test_round_budget_cannot_be_reported_as_completion(execution_request, gateway) -> None:
    model = ScriptedFakeModel([call(path="partial.txt", text="partial"), answer()])
    engine = ScopedToolEngine(model, gateway)
    execution_request = execution_request.model_copy(
        update={"budget": ExecutionBudget(max_rounds=1)}
    )
    result = await engine.wait(await engine.start(execution_request))
    assert result.state == RunState.PAUSED and result.error_code == "round_budget_exhausted"
    assert len(model.requests) == 1 and result.candidate_text is None


async def test_truncation_and_usage_overrun_never_become_candidates(
    execution_request, gateway
) -> None:
    engine = ScopedToolEngine(
        ScriptedFakeModel([answer("Everything done", finish_reason="length")]), gateway
    )
    result = await engine.wait(await engine.start(execution_request))
    assert result.state == RunState.PAUSED and result.error_code == "model_output_truncated"


async def test_provider_overrun_stops_before_tool_execution(execution_request, gateway) -> None:
    reply = call(path="never.txt", text="x").model_copy(
        update={
            "usage": TokenUsage(input_tokens=20, output_tokens=4097),
        }
    )
    engine = ScopedToolEngine(ScriptedFakeModel([reply]), gateway)
    result = await engine.wait(await engine.start(execution_request))
    assert result.error_code == "token_estimate_exceeded"
    assert result.receipts == () and not (execution_request.workspace / "never.txt").exists()


async def test_forbidden_tool_stops_run_before_claimed_success(execution_request, gateway) -> None:
    model = ScriptedFakeModel([call("bash", command="unsafe"), answer("It succeeded")])
    engine = ScopedToolEngine(model, gateway)
    result = await engine.wait(await engine.start(execution_request))
    assert result.state == RunState.FAILED and result.error_code == "tool_not_registered"
    assert len(model.requests) == 1 and result.candidate_text is None


async def test_required_tool_cannot_be_satisfied_by_model_text(execution_request, gateway) -> None:
    execution_request = execution_request.model_copy(
        update={"required_tools": frozenset({"workspace_write_text"})}
    )
    engine = ScopedToolEngine(ScriptedFakeModel([answer("I generated the document")]), gateway)
    result = await engine.wait(await engine.start(execution_request))
    assert result.state == RunState.FAILED and result.error_code == "validation_required"


async def test_cancel_interrupts_model_and_prevents_new_tools(execution_request, gateway) -> None:
    model = ScriptedFakeModel([call(path="never.txt", text="x")], delay_seconds=100)
    engine = ScopedToolEngine(model, gateway)
    run_id = await engine.start(execution_request)
    await model.started.wait()
    await engine.cancel(run_id, execution_request.actor)
    result = await asyncio.wait_for(engine.wait(run_id), 0.5)
    assert result.state == RunState.CANCELLED and model.cancelled.is_set()
    assert result.receipts == () and not (execution_request.workspace / "never.txt").exists()
    assert not result.checkpoint.safe_to_resume  # billed execution_request is unresolved


class EmptyInput(ToolData):
    pass


class EmptyOutput(ToolData):
    ok: bool = True


async def test_cancel_interrupts_running_tool_and_preserves_uncertainty(execution_request) -> None:
    started, stopped = asyncio.Event(), asyncio.Event()

    async def slow(args: EmptyInput, context: ToolContext) -> EmptyOutput:
        started.set()
        try:
            await asyncio.sleep(100)
            return EmptyOutput()
        finally:
            stopped.set()

    gateway = ToolGateway()
    gateway.register(
        ToolDefinition(
            name="slow_tool",
            description="Synthetic operation",
            input_model=EmptyInput,
            output_model=EmptyOutput,
            handler=slow,
        )
    )
    execution_request = execution_request.model_copy(
        update={"allowed_tools": frozenset({"slow_tool"})}
    )
    engine = ScopedToolEngine(ScriptedFakeModel([call("slow_tool"), answer()]), gateway)
    run_id = await engine.start(execution_request)
    await started.wait()
    await engine.cancel(run_id, execution_request.actor)
    result = await asyncio.wait_for(engine.wait(run_id), 0.5)
    assert result.state == RunState.CANCELLED and stopped.is_set()
    assert result.receipts[-1].effect_state == EffectState.UNKNOWN


async def test_new_input_discards_stale_model_tool_proposal(execution_request, gateway) -> None:
    model = ScriptedFakeModel(
        [
            call(path="stale.txt", text="old objective"),
            answer("Updated candidate"),
        ],
        delay_seconds=0.03,
    )
    engine = ScopedToolEngine(model, gateway)
    run_id = await engine.start(execution_request)
    await model.started.wait()
    revision = await engine.append_input(
        run_id,
        execution_request.actor,
        "Use the updated objective instead",
        1,
        input_id="input-2",
    )
    assert revision == 2
    assert (
        await engine.append_input(
            run_id,
            execution_request.actor,
            "Use the updated objective instead",
            1,
            input_id="input-2",
        )
        == 2
    )
    result = await engine.wait(run_id)
    assert result.state == RunState.CANDIDATE and result.revision == 2
    assert result.candidate_text == "Updated candidate"
    assert not (execution_request.workspace / "stale.txt").exists()
    assert model.requests[-1].messages[-1].content == "Use the updated objective instead"


async def test_unauthorized_control_and_stale_revision_rejected(execution_request, gateway) -> None:
    model = ScriptedFakeModel([answer()], delay_seconds=0.02)
    engine = ScopedToolEngine(model, gateway)
    run_id = await engine.start(execution_request)
    foreign = ActorContext(group_id="other-group", member_id="owner", role=Role.OWNER)
    with pytest.raises(DomainError):
        await engine.cancel(run_id, foreign)
    with pytest.raises(DomainError):
        await engine.append_input(run_id, execution_request.actor, "new", 99)
    await engine.wait(run_id)


async def test_quiesced_model_timeout_retains_usage_and_allows_new_funded_attempt(
    execution_request, gateway
):
    execution_request = execution_request.model_copy(
        update={"budget": ExecutionBudget(model_timeout_seconds=0.01, additional_retry_limit=0)}
    )
    engine = ScopedToolEngine(ScriptedFakeModel([answer()], delay_seconds=100), gateway)
    result = await asyncio.wait_for(engine.wait(await engine.start(execution_request)), 0.5)
    assert result.state == RunState.PAUSED and result.error_code == "model_timeout"
    assert result.usage.tokens > 0 and result.checkpoint.safe_to_resume
    restored = ScopedToolEngine(ScriptedFakeModel([answer()]), gateway)
    resumed = await restored.wait(
        await restored.resume(result.checkpoint, execution_request.actor, attempt_id="retry")
    )
    assert resumed.state == RunState.CANDIDATE
    assert resumed.usage.tokens == result.usage.tokens + 30


async def test_safe_resume_restores_tool_ledger_without_repeating_write(execution_request, gateway):
    execution_request = execution_request.model_copy(
        update={"budget": ExecutionBudget(max_rounds=1)}
    )
    engine = ScopedToolEngine(ScriptedFakeModel([call(path="once.txt", text="once")]), gateway)
    first = await engine.wait(await engine.start(execution_request))
    assert first.checkpoint.safe_to_resume
    checkpoint = EngineCheckpoint.model_validate_json(first.checkpoint.model_dump_json())
    owner = execution_request.actor.model_copy(update={"role": Role.OWNER})
    restored = ScopedToolEngine(
        ScriptedFakeModel(
            [
                call(path="once.txt", text="once"),
                answer(),
            ]
        ),
        gateway,
    )
    run_id = await restored.resume(
        checkpoint,
        owner,
        attempt_id="attempt-2",
        budget=ExecutionBudget(max_rounds=3),
    )
    result = await restored.wait(run_id)
    assert result.state == RunState.CANDIDATE
    assert result.receipts[-1].replayed
    assert (execution_request.workspace / "once.txt").read_text() == "once"


async def test_persistence_failure_before_tool_prevents_effect(execution_request, gateway):
    async def broken_sink(event):
        if event.kind == "tool_requested":
            raise RuntimeError("synthetic persistence outage")

    engine = ScopedToolEngine(
        ScriptedFakeModel([call(path="never.txt", text="x")]),
        gateway,
        event_sink=broken_sink,
    )
    result = await engine.wait(await engine.start(execution_request))
    assert result.state == RunState.FAILED
    assert not (execution_request.workspace / "never.txt").exists()


async def test_required_tool_error_cannot_be_covered_by_model_success_text(
    execution_request, gateway
):
    model = ScriptedFakeModel(
        [
            call("workspace_read_text", path="missing.txt"),
            answer("All verified successfully"),
        ]
    )
    engine = ScopedToolEngine(model, gateway)
    request = execution_request.model_copy(
        update={"required_tools": frozenset({"workspace_read_text"})}
    )
    result = await engine.wait(await engine.start(request))
    assert result.state == RunState.FAILED and result.error_code == "validation_required"


async def test_failed_read_can_be_repaired_within_budget(execution_request, gateway):
    (execution_request.workspace / "known.txt").write_text("synthetic evidence")
    model = ScriptedFakeModel(
        [
            call("workspace_read_text", path="missing.txt"),
            call("workspace_read_text", call_id="repair", path="known.txt"),
            answer(),
        ]
    )
    engine = ScopedToolEngine(model, gateway)
    result = await engine.wait(await engine.start(execution_request))
    assert result.state == RunState.CANDIDATE and result.usage.rounds == 3


@pytest.mark.parametrize("failure", ["no_effect", "not_started"])
async def test_successful_alternative_tool_path_is_not_blocked_by_definite_failure(
    execution_request, gateway, failure
):
    async def unsupported(_args, _context):
        raise ToolFailure("unsupported_feature", "Synthetic unsupported generator", no_effect=True)

    gateway.register(
        ToolDefinition(
            name="document_generate",
            description="Synthetic rejected generator",
            input_model=EmptyInput,
            output_model=EmptyOutput,
            handler=unsupported,
        )
    )
    request = execution_request.model_copy(
        update={
            "allowed_tools": gateway.registered_names,
            "required_tools": frozenset({"workspace_write_text", "workspace_read_text"}),
        }
    )
    bad = call("document_generate", **({"not_in_schema": True} if failure == "not_started" else {}))
    model = ScriptedFakeModel(
        [
            bad,
            call(call_id="alternate-write", path="alternate.md", text="Verified fallback bytes"),
            call("workspace_read_text", call_id="alternate-read", path="alternate.md"),
            answer(),
        ]
    )
    engine = ScopedToolEngine(model, gateway)
    result = await engine.wait(await engine.start(request))
    assert result.state == RunState.CANDIDATE
    assert not result.receipts[0].success
    assert result.receipts[0].effect_state == (
        EffectState.NOT_STARTED if failure == "not_started" else EffectState.NO_EFFECT
    )
    assert result.checkpoint.unresolved_tools == frozenset()
    assert result.receipts[-1].output["text"] == "Verified fallback bytes"


async def test_safe_legacy_checkpoint_drops_only_spurious_no_effect_unresolved_names(
    execution_request, gateway
):
    async def unsupported(_args, _context):
        raise ToolFailure("unsupported_feature", "Synthetic no-effect failure", no_effect=True)

    gateway.register(
        ToolDefinition(
            name="old_generator",
            description="Legacy diagnostic",
            input_model=EmptyInput,
            output_model=EmptyOutput,
            handler=unsupported,
        )
    )
    request = execution_request.model_copy(
        update={"allowed_tools": gateway.registered_names, "budget": ExecutionBudget(max_rounds=1)}
    )
    first = ScopedToolEngine(ScriptedFakeModel([call("old_generator")]), gateway)
    paused = await first.wait(await first.start(request))
    assert paused.state == RunState.PAUSED and paused.checkpoint.safe_to_resume
    legacy = paused.checkpoint.model_copy(update={"unresolved_tools": frozenset({"old_generator"})})
    resumed = ScopedToolEngine(
        ScriptedFakeModel(
            [call(call_id="new-writer", path="new.md", text="Alternative"), answer()]
        ),
        gateway,
    )
    result = await resumed.wait(
        await resumed.resume(
            legacy,
            request.actor.model_copy(update={"role": Role.OWNER}),
            attempt_id="legacy-alternative",
            budget=ExecutionBudget(max_rounds=3),
        )
    )
    assert result.state == RunState.CANDIDATE and result.usage.rounds == 3
    assert not result.receipts[0].success and result.checkpoint.unresolved_tools == frozenset()


async def test_append_at_pretool_persistence_checkpoint_prevents_stale_action(
    execution_request, gateway
):
    engine = None
    updated = False

    async def steer(event):
        nonlocal updated
        if event.kind == "tool_requested" and not updated:
            updated = True
            assert engine is not None
            await engine.append_input(event.attempt_id, execution_request.actor, "Updated goal", 1)

    model = ScriptedFakeModel([call(path="stale.txt", text="stale"), answer("Updated answer")])
    engine = ScopedToolEngine(model, gateway, event_sink=steer)
    result = await engine.wait(await engine.start(execution_request))
    assert result.state == RunState.CANDIDATE and result.revision == 2
    assert not (execution_request.workspace / "stale.txt").exists()
    assert result.receipts[0].error_code == "superseded_by_input"


async def test_default_worker_concurrency_is_bounded(execution_request, gateway, tmp_path: Path):
    model = ScriptedFakeModel([answer()], delay_seconds=100)
    engine = ScopedToolEngine(model, gateway)
    run_id = await engine.start(execution_request)
    other_workspace = tmp_path / "other"
    other_workspace.mkdir()
    other = execution_request.model_copy(
        update={
            "work_id": "other-work",
            "attempt_id": "other-attempt",
            "workspace": other_workspace,
        }
    )
    with pytest.raises(DomainError) as failure:
        await engine.start(other)
    assert failure.value.code == "worker_busy"
    await engine.cancel(run_id, execution_request.actor)
    await engine.wait(run_id)


async def test_time_limit_during_tool_is_unknown_and_never_completed(
    execution_request, monkeypatch
):
    entered, stopped = asyncio.Event(), asyncio.Event()

    async def slow(args: EmptyInput, context: ToolContext) -> EmptyOutput:
        entered.set()
        try:
            await asyncio.Event().wait()
            return EmptyOutput()
        finally:
            stopped.set()

    gateway = ToolGateway()
    gateway.register(
        ToolDefinition(
            name="slow_tool",
            description="Synthetic timed operation",
            input_model=EmptyInput,
            output_model=EmptyOutput,
            handler=slow,
            timeout_seconds=120,
        )
    )
    request = execution_request.model_copy(
        update={
            "allowed_tools": frozenset({"slow_tool"}),
            "budget": ExecutionBudget(max_active_seconds=60),
        }
    )
    engine = ScopedToolEngine(ScriptedFakeModel([call("slow_tool"), answer()]), gateway)
    run_id = await engine.start(request)
    try:
        # Establish that the effectful handler actually started. Advancing only
        # this test loop's clock then expires the work deadline, not setup time.
        await asyncio.wait_for(entered.wait(), 5)
        loop = asyncio.get_running_loop()
        real_time = loop.time
        with monkeypatch.context() as patch:
            patch.setattr(loop, "time", lambda: real_time() + 61)
            result = await asyncio.wait_for(engine.wait(run_id), 2)
        assert result.state == RunState.PAUSED and result.error_code == "effect_unknown"
        assert stopped.is_set() and result.receipts[-1].effect_state == EffectState.UNKNOWN
    finally:
        await engine.cancel(run_id, request.actor)
        await engine.wait(run_id)


async def test_waiting_input_continues_without_losing_budget(execution_request, gateway):
    model = ScriptedFakeModel([answer("Which title?", needs_input=True), answer("Updated draft")])
    engine = ScopedToolEngine(model, gateway)
    run_id = await engine.start(execution_request)
    first = await engine.wait(run_id)
    assert first.state == RunState.WAITING_INPUT
    await engine.append_input(run_id, execution_request.actor, "Use the synthetic title", 1)
    final = await engine.wait(run_id)
    assert final.state == RunState.CANDIDATE and final.revision == 2
    assert final.usage.rounds == 2


async def test_input_arriving_during_terminal_event_is_consumed(execution_request, gateway):
    engine = None

    async def input_on_question(event):
        if event.kind == "needs_input":
            assert engine is not None
            await engine.append_input(
                event.attempt_id, execution_request.actor, "Synthetic input", 1
            )

    model = ScriptedFakeModel([answer("Which title?", needs_input=True), answer("Updated draft")])
    engine = ScopedToolEngine(model, gateway, event_sink=input_on_question)
    final = await engine.wait(await engine.start(execution_request))
    assert final.state == RunState.CANDIDATE and final.revision == 2
    assert final.usage.rounds == 2
