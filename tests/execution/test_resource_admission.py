from __future__ import annotations

import asyncio
from dataclasses import replace

from groupagent.execution import (
    EngineCheckpoint,
    ModelReply,
    ModelToolCall,
    RunState,
    ScopedToolEngine,
    ScriptedFakeModel,
    TokenUsage,
)
from groupagent.execution.models import PendingInput
from groupagent.resources import MemorySnapshot, ResourceGovernor, ResourcePolicy
from groupagent.tools import (
    EffectState,
    ToolContext,
    ToolData,
    ToolDefinition,
    ToolGateway,
    ToolScope,
)

MIB = 1024 * 1024


def resources(available):
    return ResourceGovernor(
        ResourcePolicy(enabled=True, reserve_memory_mb=192, transfer_memory_mb=64),
        sampler=lambda: MemorySnapshot(available[0] * MIB),
    )


class Done(ToolData):
    value: str


def tools(admission, calls, available):
    gateway = ToolGateway()
    for name in ("first", "second", "third"):

        async def handler(arguments, context, name=name):
            calls.append(name)
            if name == "first":
                available[0] = 0
            return Done(value=name)

        gateway.register(ToolDefinition(name, name, ToolData, Done, handler))
    gateway.bind_resource_admission(
        admission, memory_mb=512, tool_names=frozenset({"second", "third"})
    )
    return gateway


def batch():
    return ModelReply(
        tool_calls=tuple(
            ModelToolCall(id=name, name=name, arguments={}) for name in ("first", "second", "third")
        ),
        finish_reason="tool_calls",
        usage=TokenUsage(input_tokens=20, output_tokens=10),
    )


def answer():
    return ModelReply(
        text="Synthetic completed draft", usage=TokenUsage(input_tokens=20, output_tokens=10)
    )


async def test_defer_checkpoint_resumes_exact_unexecuted_batch_without_model_replay(
    execution_request,
):
    available, calls = [2048], []
    admission = resources(available)
    request = execution_request.model_copy(
        update={"allowed_tools": frozenset({"first", "second", "third"})}
    )
    initial_model = ScriptedFakeModel([batch()])
    first = ScopedToolEngine(initial_model, tools(admission, calls, available))
    paused = await first.wait(await first.start(request))
    assert paused.state == RunState.PAUSED and paused.error_code == "resource_deferred"
    assert paused.checkpoint.safe_to_resume
    assert paused.usage.rounds == 1 and paused.usage.tool_calls == 1
    assert calls == ["first"] and len(paused.receipts) == 1
    assert len(paused.checkpoint.ledger) == 1
    saved = EngineCheckpoint.model_validate_json(paused.checkpoint.model_dump_json())
    assert [entry["id"] for entry in saved.backend_state["scoped_pending_tools"]["calls"]] == [
        "second",
        "third",
    ]

    # Another durable attempt while capacity is absent performs no paid model
    # request and does not charge another tool call or replay the completed one.
    waiting_model = ScriptedFakeModel([])
    waiting = ScopedToolEngine(waiting_model, tools(admission, calls, available))
    still_paused = await waiting.wait(
        await waiting.resume(saved, request.actor, attempt_id="attempt-2")
    )
    assert still_paused.error_code == "resource_deferred" and still_paused.checkpoint.safe_to_resume
    assert waiting_model.requests == [] and calls == ["first"]
    assert still_paused.usage.rounds == 1 and still_paused.usage.tool_calls == 1

    available[0] = 2048
    final_model = ScriptedFakeModel([answer()])
    resumed = ScopedToolEngine(final_model, tools(admission, calls, available))
    result = await resumed.wait(
        await resumed.resume(still_paused.checkpoint, request.actor, attempt_id="attempt-3")
    )
    assert result.state == RunState.CANDIDATE
    assert calls == ["first", "second", "third"]
    assert len(initial_model.requests) == len(final_model.requests) == 1
    assert result.usage.rounds == 2 and result.usage.tool_calls == 3
    assert [receipt.call_id for receipt in result.receipts] == ["first", "second", "third"]
    assert "scoped_pending_tools" not in result.checkpoint.backend_state
    history = final_model.requests[0].messages
    assert [message.tool_call_id for message in history if message.role == "tool"] == [
        "first",
        "second",
        "third",
    ]
    assert len([message for message in history if message.tool_calls]) == 1


async def test_new_input_invalidates_deferred_proposals_before_resume(execution_request):
    available, calls = [2048], []
    admission = resources(available)
    request = execution_request.model_copy(
        update={"allowed_tools": frozenset({"first", "second", "third"})}
    )
    initial = ScopedToolEngine(ScriptedFakeModel([batch()]), tools(admission, calls, available))
    paused = await initial.wait(await initial.start(request))
    updated = paused.checkpoint.model_copy(
        update={
            "revision": 2,
            "pending_inputs": (
                PendingInput(revision=2, input_id="update", text="Use the new objective"),
            ),
        }
    )
    available[0] = 2048
    model = ScriptedFakeModel([answer()])
    engine = ScopedToolEngine(model, tools(admission, calls, available))
    result = await engine.wait(await engine.resume(updated, request.actor, attempt_id="updated"))
    assert result.state == RunState.CANDIDATE and calls == ["first"]
    assert any(message.content == "Use the new objective" for message in model.requests[0].messages)
    assert "scoped_pending_tools" not in result.checkpoint.backend_state


async def test_gateway_refusal_precedes_handler_timeout_and_ledger(gateway, scope):
    available = [0]
    admission = resources(available)
    gateway.bind_resource_admission(
        admission, memory_mb=512, tool_names=frozenset({"workspace_write_text"})
    )
    ctx = ToolContext(scope, "write", "write", asyncio.Event())
    arguments = {"path": "draft.txt", "text": "Synthetic content"}
    deferred = await gateway.invoke("workspace_write_text", arguments, ctx)
    assert (
        deferred.error_code == "resource_deferred"
        and deferred.effect_state == EffectState.NOT_STARTED
    )
    assert gateway.export_ledger(scope) == () and not (scope.workspace / "draft.txt").exists()
    assert not admission.busy
    available[0] = 2048
    result = await gateway.invoke("workspace_write_text", arguments, ctx)
    assert result.success and not admission.busy
    available[0] = 0
    replay = await gateway.invoke("workspace_write_text", arguments, replace(ctx, call_id="replay"))
    assert replay.success and replay.replayed


async def test_handler_timeout_retains_capacity_until_handler_really_stops(execution_request):
    admission = resources([2048])
    entered, finish = asyncio.Event(), asyncio.Event()

    async def handler(arguments, context):
        entered.set()
        try:
            await finish.wait()
        except asyncio.CancelledError:
            await finish.wait()  # Deliberately model a cancellation-resistant handler.
        return Done(value="finished")

    gateway = ToolGateway()
    gateway.register(ToolDefinition("slow", "slow", ToolData, Done, handler, timeout_seconds=0.01))
    gateway.bind_resource_admission(admission, memory_mb=512, tool_names=frozenset({"slow"}))
    request = execution_request
    scope = ToolScope(
        request.actor.group_id,
        request.work_id,
        request.actor,
        request.workspace,
        frozenset({"slow"}),
        request.trace_id,
    )
    gateway.bind_scope(scope)
    task = asyncio.create_task(
        gateway.invoke("slow", {}, ToolContext(scope, "slow", "slow", asyncio.Event()))
    )
    await entered.wait()
    result = await task
    assert result.error_code == "tool_timeout" and result.effect_state == EffectState.UNKNOWN
    assert admission.busy and not admission.poisoned
    finish.set()
    for _ in range(10):
        await asyncio.sleep(0)
        if not admission.busy:
            break
    assert not admission.busy and not admission.poisoned
