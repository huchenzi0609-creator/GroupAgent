"""Finite offline public-read concurrency and interrupted-read lifecycle evidence."""

import asyncio
import json
from types import SimpleNamespace

import httpx
import pytest
from pydantic import Field

from groupagent.domain import DomainError
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
from groupagent.research import DeepSeekResearchClient, register_research_tools
from groupagent.resources import MemorySnapshot, ResourceGovernor, ResourcePolicy
from groupagent.tools import (
    EffectState,
    IdempotencyMode,
    PolicyDecision,
    ToolData,
    ToolDefinition,
    ToolGateway,
)


class Arguments(ToolData):
    item: int = Field(ge=0, le=100)


class Observation(ToolData):
    text: str


def register(gateway, name, handler, *, mutable=False, public=True, safe=True, timeout=1):
    gateway.register(
        ToolDefinition(
            name=name,
            description="Synthetic offline operation",
            input_model=Arguments,
            output_model=Observation,
            handler=handler,
            risk_class="local_draft" if mutable else "read_only",
            idempotency_mode=IdempotencyMode.RECONCILE_BEFORE_RETRY
            if mutable
            else IdempotencyMode.READ_ONLY,
            resource_scopes=("group", "work", "public_web") if public else ("group", "work"),
            concurrency_safe=safe,
            timeout_seconds=timeout,
        )
    )


def proposal(*names, arguments=None):
    return ModelReply(
        tool_calls=tuple(
            ModelToolCall(
                id=f"call-{index}",
                name=name,
                arguments=arguments[index] if arguments else {"item": index},
            )
            for index, name in enumerate(names)
        ),
        finish_reason="tool_calls",
        usage=TokenUsage(input_tokens=10, output_tokens=5),
    )


def answer():
    return ModelReply(
        text="Synthetic completed result", usage=TokenUsage(input_tokens=10, output_tokens=5)
    )


def requested(execution_request, names, *, budget=None):
    return execution_request.model_copy(
        update={
            "allowed_tools": frozenset(names),
            **({"budget": budget} if budget else {}),
        }
    )


async def test_two_read_limit_preflight_and_intent_precede_every_handler(execution_request):
    trace, checkpoints, active, maximum = [], [], 0, 0

    class RecordingGateway(ToolGateway):
        async def preflight(self, name, arguments, context):
            trace.append("preflight:" + str(arguments["item"]))
            return await super().preflight(name, arguments, context)

    async def save(checkpoint):
        checkpoints.append(EngineCheckpoint.model_validate_json(checkpoint.model_dump_json()))

    async def observe(arguments, context):
        nonlocal active, maximum
        latest = checkpoints[-1]
        assert latest.safe_to_resume
        assert context.call_id in latest.backend_state["scoped_tool_intents"]
        assert (
            context.idempotency_key == latest.backend_state["scoped_tool_intents"][context.call_id]
        )
        assert len(latest.backend_state["scoped_tool_intents"]) == 2
        trace.append("start:" + str(arguments.item))
        active += 1
        maximum = max(maximum, active)
        await asyncio.sleep(0.01)
        active -= 1
        return Observation(text=str(arguments.item))

    gateway = RecordingGateway()
    register(gateway, "read", observe)
    model = ScriptedFakeModel([proposal(*(["read"] * 4)), answer()])
    engine = ScopedToolEngine(model, gateway, checkpoint_sink=save)
    result = await engine.wait(await engine.start(requested(execution_request, {"read"})))
    assert result.state == RunState.CANDIDATE and maximum == 2 and active == 0
    assert trace[:2] == ["preflight:0", "preflight:1"]
    assert trace[4:6] == ["preflight:2", "preflight:3"]
    assert result.usage.tool_calls == 4 and len(model.requests) == 2
    assert not result.checkpoint.backend_state["scoped_tool_intents"]
    assert [
        message.tool_call_id for message in model.requests[-1].messages if message.role == "tool"
    ] == [f"call-{index}" for index in range(4)]


async def test_completion_order_does_not_change_receipt_or_model_message_order(execution_request):
    both_entered, fast_done = asyncio.Event(), asyncio.Event()
    entered, completed = 0, []

    async def observe(arguments, context):
        nonlocal entered
        entered += 1
        if entered == 2:
            both_entered.set()
        await both_entered.wait()
        if arguments.item == 0:
            await fast_done.wait()
        completed.append(arguments.item)
        fast_done.set()
        return Observation(text=str(arguments.item))

    gateway = ToolGateway()
    register(gateway, "read", observe)
    model = ScriptedFakeModel([proposal("read", "read"), answer()])
    engine = ScopedToolEngine(model, gateway)
    result = await asyncio.wait_for(
        engine.wait(await engine.start(requested(execution_request, {"read"}))), 1
    )
    assert completed == [1, 0]
    assert [receipt.call_id for receipt in result.receipts] == ["call-0", "call-1"]
    assert [
        message.tool_call_id for message in model.requests[-1].messages if message.role == "tool"
    ] == ["call-0", "call-1"]
    assert len(result.checkpoint.ledger) == 2


async def test_mutable_unflagged_and_nonpublic_tools_stay_serial(execution_request):
    active, maximum, order = 0, 0, []

    async def observe(arguments, context):
        nonlocal active, maximum
        active += 1
        maximum = max(maximum, active)
        order.append(arguments.item)
        await asyncio.sleep(0.005)
        active -= 1
        return Observation(text=str(arguments.item))

    gateway = ToolGateway()
    register(gateway, "write", observe, mutable=True)
    register(gateway, "unflagged", observe, safe=False)
    register(gateway, "local_read", observe, public=False)
    for name in ("write", "unflagged", "local_read", "missing"):
        assert not gateway.is_concurrency_safe(name)
    assert gateway.is_read_only("local_read") and not gateway.is_read_only("write")
    names = ("write", "write", "unflagged", "unflagged", "local_read", "local_read")
    model = ScriptedFakeModel([proposal(*names), answer()])
    engine = ScopedToolEngine(model, gateway)
    result = await engine.wait(await engine.start(requested(execution_request, names)))
    assert result.state == RunState.CANDIDATE and maximum == 1
    assert order == list(range(6))


async def test_mutable_barrier_never_overlaps_the_two_read_waves(execution_request):
    active, maximum, writes = 0, 0, []

    async def observe(arguments, context):
        nonlocal active, maximum
        active += 1
        maximum = max(maximum, active)
        await asyncio.sleep(0.005)
        active -= 1
        return Observation(text=str(arguments.item))

    async def write(arguments, context):
        assert active == 0
        writes.append(arguments.item)
        return Observation(text="written")

    gateway = ToolGateway()
    register(gateway, "read", observe)
    register(gateway, "write", write, mutable=True)
    names = ("read", "read", "write", "read", "read")
    engine = ScopedToolEngine(ScriptedFakeModel([proposal(*names), answer()]), gateway)
    result = await engine.wait(await engine.start(requested(execution_request, names)))
    assert result.state == RunState.CANDIDATE and maximum == 2 and writes == [2]


async def test_preflight_schema_failure_keeps_serial_error_diagnostics_and_valid_peer(
    execution_request,
):
    handled, checked = [], []

    class RecordingGateway(ToolGateway):
        async def preflight(self, name, arguments, context):
            checked.append(context.call_id)
            return await super().preflight(name, arguments, context)

    async def observe(arguments, context):
        assert checked == ["call-0", "call-1"]
        handled.append(arguments.item)
        return Observation(text="valid")

    gateway = RecordingGateway()
    register(gateway, "read", observe)
    model = ScriptedFakeModel(
        [proposal("read", "read", arguments=[{"item": -1}, {"item": 1}]), answer()]
    )
    engine = ScopedToolEngine(model, gateway)
    result = await engine.wait(await engine.start(requested(execution_request, {"read"})))
    assert result.state == RunState.CANDIDATE and handled == [1]
    assert result.receipts[0].effect_state == EffectState.NOT_STARTED
    assert result.receipts[0].error_code == "invalid_input" and result.receipts[0].input_issues
    assert result.receipts[1].success and len(result.checkpoint.ledger) == 1


async def test_cancel_waits_for_both_handlers_and_never_starts_the_next_wave(execution_request):
    both_entered, stop = asyncio.Event(), asyncio.Event()
    entered, stopped = [], []

    async def observe(arguments, context):
        entered.append(arguments.item)
        if len(entered) == 2:
            both_entered.set()
        try:
            await stop.wait()
            return Observation(text="unused")
        finally:
            stopped.append(arguments.item)

    gateway = ToolGateway()
    register(gateway, "read", observe)
    model = ScriptedFakeModel([proposal("read", "read", "read")])
    engine = ScopedToolEngine(model, gateway)
    run = await engine.start(requested(execution_request, {"read"}))
    await asyncio.wait_for(both_entered.wait(), 1)
    await engine.cancel(run, execution_request.actor)
    result = await asyncio.wait_for(engine.wait(run), 1)
    assert result.state == RunState.CANCELLED and entered == [0, 1]
    assert sorted(stopped) == [0, 1] and engine.is_quiescent(run)
    assert len(result.receipts) == 2 and all(
        receipt.error_code == "cancelled" for receipt in result.receipts
    )
    assert len(model.requests) == 1


async def test_steering_during_wave_preserves_results_and_skips_stale_following_tools(
    execution_request,
):
    both_entered, release = asyncio.Event(), asyncio.Event()
    entered = []

    async def observe(arguments, context):
        entered.append(arguments.item)
        if len(entered) == 2:
            both_entered.set()
        await release.wait()
        return Observation(text=str(arguments.item))

    gateway = ToolGateway()
    register(gateway, "read", observe)
    model = ScriptedFakeModel([proposal("read", "read", "read"), answer()])
    engine = ScopedToolEngine(model, gateway)
    run = await engine.start(requested(execution_request, {"read"}))
    await asyncio.wait_for(both_entered.wait(), 1)
    await engine.append_input(run, execution_request.actor, "Use the revised goal", 1)
    release.set()
    result = await asyncio.wait_for(engine.wait(run), 1)
    assert result.state == RunState.CANDIDATE and result.revision == 2 and entered == [0, 1]
    assert all(receipt.success for receipt in result.receipts)
    assert any(message.content == "Use the revised goal" for message in model.requests[-1].messages)
    assert [
        message.tool_call_id for message in model.requests[-1].messages if message.role == "tool"
    ] == ["call-0", "call-1", "call-2"]


async def test_checkpoint_outage_before_wave_dispatch_starts_no_handler(execution_request):
    started = []

    async def observe(arguments, context):
        started.append(arguments.item)
        return Observation(text="never")

    async def events(event):
        if event.kind == "tool_requested":
            raise RuntimeError("Synthetic durable intent sink outage")

    gateway = ToolGateway()
    register(gateway, "read", observe)
    engine = ScopedToolEngine(
        ScriptedFakeModel([proposal("read", "read")]), gateway, event_sink=events
    )
    result = await engine.wait(await engine.start(requested(execution_request, {"read"})))
    assert result.state == RunState.FAILED and not started and not result.checkpoint.ledger
    assert set(result.checkpoint.backend_state["scoped_tool_intents"]) == {"call-0", "call-1"}


@pytest.mark.parametrize("mutable", [False, True])
async def test_inflight_checkpoint_distinguishes_pure_reads_from_writes(execution_request, mutable):
    entered, release = asyncio.Event(), asyncio.Event()

    async def observe(arguments, context):
        entered.set()
        await release.wait()
        return Observation(text="finished")

    gateway = ToolGateway()
    register(gateway, "operation", observe, mutable=mutable)
    engine = ScopedToolEngine(ScriptedFakeModel([proposal("operation"), answer()]), gateway)
    run = await engine.start(requested(execution_request, {"operation"}))
    await asyncio.wait_for(entered.wait(), 1)
    checkpoint = await engine.flush_checkpoint(run)
    assert checkpoint.safe_to_resume is (not mutable)
    assert checkpoint.usage.tool_calls == 1
    assert checkpoint.backend_state["scoped_tool_intents"] == {
        "call-0": f"{execution_request.work_id}:call-0"
    }
    assert checkpoint.ledger[0].receipt.error_code == "tool_in_progress"
    release.set()
    assert (await engine.wait(run)).state == RunState.CANDIDATE


@pytest.mark.parametrize("count", [1, 2])
async def test_safe_read_intent_resumes_without_recounting_or_an_extra_model(
    execution_request, count
):
    calls = []

    async def observe(arguments, context):
        calls.append(arguments.item)
        return Observation(text="restored actual evidence")

    async def interrupt(event):
        if event.kind == "tool_requested":
            raise DomainError("synthetic_worker_lost", "Synthetic loss before dispatch")

    request = requested(execution_request, {"read"}, budget=ExecutionBudget(max_tool_calls=count))
    first_gateway = ToolGateway()
    register(first_gateway, "read", observe)
    initial = ScopedToolEngine(
        ScriptedFakeModel([proposal(*(["read"] * count))]), first_gateway, event_sink=interrupt
    )
    paused = await initial.wait(await initial.start(request))
    assert paused.state == RunState.PAUSED and paused.checkpoint.safe_to_resume
    assert paused.usage.tool_calls == count and calls == []
    restored_gateway = ToolGateway()
    register(restored_gateway, "read", observe)
    model = ScriptedFakeModel([answer()])
    restored = ScopedToolEngine(model, restored_gateway)
    result = await restored.wait(
        await restored.resume(paused.checkpoint, request.actor, attempt_id="resume")
    )
    assert result.state == RunState.CANDIDATE and sorted(calls) == list(range(count))
    assert result.usage.tool_calls == count and len(model.requests) == 1
    assert all(receipt.success for receipt in result.receipts)
    assert not result.checkpoint.backend_state["scoped_tool_intents"]


async def test_resumed_read_still_requires_current_policy(execution_request):
    calls = []

    async def observe(arguments, context):
        calls.append(arguments.item)
        return Observation(text="never")

    async def interrupt(event):
        if event.kind == "tool_requested":
            raise DomainError("synthetic_worker_lost", "Synthetic loss")

    request = requested(execution_request, {"read"})
    first_gateway = ToolGateway()
    register(first_gateway, "read", observe)
    initial = ScopedToolEngine(
        ScriptedFakeModel([proposal("read")]), first_gateway, event_sink=interrupt
    )
    paused = await initial.wait(await initial.start(request))

    async def revoked(scope):
        return PolicyDecision(actor=scope.actor, allowed_tools=scope.allowed_tools, active=False)

    gateway = ToolGateway(revoked)
    register(gateway, "read", observe)
    model = ScriptedFakeModel([])
    restored = ScopedToolEngine(model, gateway)
    result = await restored.wait(
        await restored.resume(paused.checkpoint, request.actor, attempt_id="revoked")
    )
    assert result.state == RunState.FAILED and result.error_code == "permission_denied"
    assert calls == [] and model.requests == []


async def test_inflight_read_receipt_cannot_be_restored_as_success(execution_request):
    entered, release = asyncio.Event(), asyncio.Event()
    calls = []

    async def observe(arguments, context):
        calls.append(arguments.item)
        entered.set()
        await release.wait()
        return Observation(text="not checkpointed")

    request = requested(
        execution_request,
        {"read"},
        budget=ExecutionBudget(max_tool_calls=1),
    ).model_copy(update={"required_tools": frozenset({"read"})})
    gateway = ToolGateway()
    register(gateway, "read", observe)
    initial = ScopedToolEngine(ScriptedFakeModel([proposal("read")]), gateway)
    run = await initial.start(request)
    await asyncio.wait_for(entered.wait(), 1)
    checkpoint = await initial.flush_checkpoint(run)
    await initial.cancel(run, request.actor)
    await initial.wait(run)
    assert (
        checkpoint.safe_to_resume and checkpoint.ledger[0].receipt.error_code == "tool_in_progress"
    )

    restarted_gateway = ToolGateway()
    register(restarted_gateway, "read", observe)
    model = ScriptedFakeModel([answer()])
    restarted = ScopedToolEngine(model, restarted_gateway)
    result = await restarted.wait(
        await restarted.resume(
            checkpoint.model_copy(update={"state": RunState.PAUSED}),
            request.actor,
            attempt_id="after-worker-loss",
        )
    )
    assert result.state == RunState.FAILED and result.error_code == "validation_required"
    assert calls == [0] and result.usage.tool_calls == 1
    assert len(result.receipts) == 1 and result.receipts[0].replayed
    assert not result.receipts[0].success and result.receipts[0].output is None


async def test_noncooperative_read_timeout_blocks_following_tools_and_resume_overlap(
    execution_request,
):
    entered, release = asyncio.Event(), asyncio.Event()
    starts = []

    async def observe(arguments, context):
        starts.append(arguments.item)
        if arguments.item == 0:
            entered.set()
            while not release.is_set():
                try:
                    await release.wait()
                except asyncio.CancelledError:
                    continue
        return Observation(text=str(arguments.item))

    gateway = ToolGateway()
    register(gateway, "read", observe, timeout=0.01)
    model = ScriptedFakeModel([proposal("read", "read", "read")])
    engine = ScopedToolEngine(model, gateway)
    request = requested(execution_request, {"read"})
    run = await engine.start(request)
    await asyncio.wait_for(entered.wait(), 1)
    inflight = await engine.flush_checkpoint(run)
    try:
        result = await asyncio.wait_for(engine.wait(run), 1)
        assert result.state == RunState.PAUSED and result.error_code == "work_busy"
        assert not result.checkpoint.safe_to_resume and starts == [0, 1]
        assert len(model.requests) == 1
        restored = ScopedToolEngine(ScriptedFakeModel([]), gateway)
        with pytest.raises(DomainError) as error:
            await restored.resume(
                inflight.model_copy(update={"state": RunState.PAUSED}),
                request.actor,
                attempt_id="overlap",
            )
        assert error.value.code == "work_busy"
    finally:
        release.set()
        await gateway.quiesce(engine._get(run).scope, timeout_seconds=0.2)


async def test_worker_admitted_read_keeps_serial_resource_deferral(execution_request):
    gateway = ToolGateway()

    async def observe(arguments, context):
        return Observation(text="never")

    register(gateway, "read", observe)
    governor = ResourceGovernor(
        ResourcePolicy(enabled=True, reserve_memory_mb=192, transfer_memory_mb=64),
        sampler=lambda: MemorySnapshot(0),
    )
    gateway.bind_resource_admission(governor, memory_mb=512, tool_names=frozenset({"read"}))
    assert not gateway.is_concurrency_safe("read")
    engine = ScopedToolEngine(ScriptedFakeModel([proposal("read", "read")]), gateway)
    result = await engine.wait(await engine.start(requested(execution_request, {"read"})))
    assert result.error_code == "resource_deferred" and result.checkpoint.safe_to_resume
    assert result.usage.tool_calls == 0 and not result.checkpoint.ledger
    assert not result.checkpoint.backend_state["scoped_tool_intents"]


async def test_preflight_does_not_bypass_policy_revoked_before_actual_dispatch(execution_request):
    active, checked, handled = True, 0, []

    async def policy(scope):
        return PolicyDecision(actor=scope.actor, allowed_tools=scope.allowed_tools, active=active)

    class RevokingGateway(ToolGateway):
        async def preflight(self, name, arguments, context):
            nonlocal active, checked
            result = await super().preflight(name, arguments, context)
            checked += 1
            if checked == 2:
                active = False
            return result

    async def observe(arguments, context):
        handled.append(arguments.item)
        return Observation(text="never")

    gateway = RevokingGateway(policy)
    register(gateway, "read", observe)
    model = ScriptedFakeModel([proposal("read", "read")])
    engine = ScopedToolEngine(model, gateway)
    result = await engine.wait(await engine.start(requested(execution_request, {"read"})))
    assert result.state == RunState.FAILED and result.error_code == "permission_denied"
    assert not handled and len(result.receipts) == 2 and not result.checkpoint.ledger
    assert all(receipt.effect_state == EffectState.NOT_STARTED for receipt in result.receipts)


async def test_parallel_research_failure_retains_peer_evidence_and_each_real_fee(execution_request):
    class NoChargeBudget:
        fixture_only = True

        def __init__(self):
            self.states = {}

        def reserve(self, actor, key, operation, price, units, **kwargs):
            self.states[key] = "reserved"
            return SimpleNamespace(id=key, is_new=True)

        def begin(self, actor, key):
            self.states[key] = "inflight"
            return True

        def settle(self, actor, key, units, evidence):
            self.states[key] = "settled"
            return SimpleNamespace(overrun=False)

        def unknown(self, actor, key, reason):
            self.states[key] = "unknown"

    async def public(host):
        return ("93.184.216.34",)

    requests, active, maximum = [], 0, 0

    async def response(request):
        nonlocal active, maximum
        target = json.loads(request.content)["url"]
        requests.append(target)
        active += 1
        maximum = max(maximum, active)
        await asyncio.sleep(0.01)
        active -= 1
        return (
            httpx.Response(503)
            if target.endswith("bad")
            else httpx.Response(
                200,
                json={
                    "title": "Actual synthetic source",
                    "content": "Independent supported evidence",
                },
            )
        )

    budget = NoChargeBudget()
    async with httpx.AsyncClient(transport=httpx.MockTransport(response)) as http:
        client = DeepSeekResearchClient("synthetic-key", client=http, resolver=public)
        gateway = ToolGateway()
        register_research_tools(
            gateway,
            client,
            budget=budget,
            price_ids={"search": "fixture-search", "fetch": "fixture-fetch"},
            additional_retry_limit=0,
        )
        model = ScriptedFakeModel(
            [
                proposal(
                    "web_fetch",
                    "web_fetch",
                    arguments=[
                        {"url": "https://example.com/bad"},
                        {"url": "https://example.com/good"},
                    ],
                ),
                answer(),
            ]
        )
        engine = ScopedToolEngine(model, gateway)
        request = requested(
            execution_request, {"web_fetch"}, budget=ExecutionBudget(max_tool_calls=2)
        )
        result = await engine.wait(await engine.start(request))
    assert result.state == RunState.CANDIDATE and maximum == 2 and len(requests) == 2
    assert not result.receipts[0].success and result.receipts[1].success
    assert result.receipts[0].effect_state == EffectState.NO_EFFECT
    assert result.receipts[1].output["source"]["content"] == "Independent supported evidence"
    assert budget.states == {
        f"{execution_request.work_id}:call-0:web.fetch": "unknown",
        f"{execution_request.work_id}:call-1:web.fetch": "settled",
    }
    assert result.usage.tool_calls == 2 and len(result.checkpoint.ledger) == 2
