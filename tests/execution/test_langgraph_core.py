"""Real LangChain/LangGraph runs with explicitly scripted offline ModelPorts.

No provider or native Harness is contacted. The framework model/tools graph,
typed checkpoint serialization, gateway, filesystem and fault boundaries are real.
"""

import asyncio
import json

import pytest

from groupagent.domain import DomainError
from groupagent.execution import (
    AgentKernel,
    EngineCheckpoint,
    ExecutionBudget,
    LangGraphEngine,
    ModelMessage,
    ModelReply,
    ModelToolCall,
    RunState,
    ScriptedFakeModel,
    TokenUsage,
)
from groupagent.execution.kernel import DurableGraphSaver, KernelHooks, KernelTool
from groupagent.execution.models import PendingInput
from groupagent.tools import EffectState, ToolData, ToolDefinition, ToolGateway
from groupagent.tools.models import ToolFailure


def reply(text="Checked artifact is ready", *calls):
    return ModelReply(
        text=text,
        tool_calls=tuple(calls),
        finish_reason="tool_calls" if calls else "stop",
        usage=TokenUsage(input_tokens=17, output_tokens=11),
    )


def call(name, identifier, **arguments):
    return ModelToolCall(id=identifier, name=name, arguments=arguments)


class Result(ToolData):
    path: str


async def test_real_graph_repairs_missing_declared_output_and_completes_artifact(
    execution_request,
    gateway,
):
    async def no_output(_arguments, _context):
        raise ToolFailure(
            "output_contract_failed",
            "Declared output report.md is missing; process returned without publishing an artifact",
            no_effect=True,
        )

    gateway.register(
        ToolDefinition(
            "draft_generate",
            "Generate a local draft",
            ToolData,
            Result,
            no_output,
        )
    )
    request = execution_request.model_copy(
        update={
            "allowed_tools": gateway.registered_names,
            "required_tools": frozenset({"workspace_write_text", "workspace_read_text"}),
        }
    )
    model = ScriptedFakeModel(
        [
            reply(None, call("draft_generate", "missing-output")),
            reply(
                None,
                call(
                    "workspace_write_text",
                    "repair",
                    path="report.md",
                    text="# Verified report\nSynthetic source evidence.",
                ),
            ),
            reply(None, call("workspace_read_text", "verify", path="report.md")),
            reply(),
        ]
    )
    engine = LangGraphEngine(model, gateway)
    result = await engine.wait(await engine.start(request))
    assert result.state == RunState.CANDIDATE
    assert (request.workspace / "report.md").read_text().startswith("# Verified report")
    assert result.receipts[0].effect_state == EffectState.NO_EFFECT
    observation = json.loads(model.requests[1].messages[-1].content)
    assert observation["error_code"] == "output_contract_failed"
    assert result.receipts[-1].output["text"].startswith("# Verified report")
    assert result.checkpoint.backend_state["engine"] == "langgraph"
    assert result.checkpoint.backend_state["langgraph"]["pending_writes"]


async def test_real_graph_restart_after_model_resumes_pending_tool_without_model_replay(
    execution_request,
    gateway,
):
    model = ScriptedFakeModel(
        [
            reply(
                None,
                call(
                    "workspace_write_text", "only-write", path="once.md", text="One durable write"
                ),
            ),
        ]
    )
    engine = LangGraphEngine(model, gateway, activation_max_rounds=1)
    paused = await engine.wait(await engine.start(execution_request))
    assert paused.state == RunState.PAUSED and paused.error_code == "activation_yielded"
    assert paused.receipts == () and not (execution_request.workspace / "once.md").exists()
    saved = EngineCheckpoint.model_validate_json(paused.checkpoint.model_dump_json())
    resumed_model = ScriptedFakeModel([reply()])
    resumed = LangGraphEngine(resumed_model, gateway)
    result = await resumed.wait(
        await resumed.resume(saved, execution_request.actor, attempt_id="after-model-restart")
    )
    assert result.state == RunState.CANDIDATE
    assert result.usage.rounds == 2 and result.usage.tool_calls == 1
    assert len(model.requests) == len(resumed_model.requests) == 1
    assert (execution_request.workspace / "once.md").read_text() == "One durable write"


async def test_real_graph_restart_after_tool_never_repeats_effect(execution_request):
    effects = []

    async def execute(_arguments, _context):
        effects.append("write")
        return Result(path="artifact.md")

    def gateway():
        registered = ToolGateway()
        registered.register(
            ToolDefinition("write_once", "Create artifact", ToolData, Result, execute)
        )
        return registered

    request = execution_request.model_copy(update={"allowed_tools": frozenset({"write_once"})})
    engine = LangGraphEngine(
        ScriptedFakeModel([reply(None, call("write_once", "once"))]),
        gateway(),
        activation_max_tool_calls=1,
    )
    paused = await engine.wait(await engine.start(request))
    assert paused.error_code == "activation_yielded" and effects == ["write"]
    saved = EngineCheckpoint.model_validate_json(paused.checkpoint.model_dump_json())
    model = ScriptedFakeModel([reply()])
    restored = LangGraphEngine(model, gateway())
    result = await restored.wait(
        await restored.resume(saved, request.actor, attempt_id="after-tool-restart")
    )
    assert result.state == RunState.CANDIDATE and effects == ["write"]
    assert result.usage.tool_calls == 1 and len(model.requests) == 1


async def test_real_graph_restart_consumes_new_input_before_stale_effect(
    execution_request, gateway
):
    engine = LangGraphEngine(
        ScriptedFakeModel(
            [
                reply(
                    None,
                    call("workspace_write_text", "stale", path="stale.md", text="Old objective"),
                ),
            ]
        ),
        gateway,
        activation_max_rounds=1,
    )
    paused = await engine.wait(await engine.start(execution_request))
    saved = paused.checkpoint.model_copy(
        update={
            "revision": 2,
            "pending_inputs": (
                PendingInput(revision=2, input_id="steer", text="Use the updated objective"),
            ),
        }
    )
    model = ScriptedFakeModel([reply("Updated answer")])
    restored = LangGraphEngine(model, gateway)
    result = await restored.wait(
        await restored.resume(saved, execution_request.actor, attempt_id="new-input")
    )
    assert result.state == RunState.CANDIDATE and result.revision == 2
    assert not (execution_request.workspace / "stale.md").exists()
    assert model.requests[0].messages[-1].content == "Use the updated objective"


async def test_real_graph_external_unknown_stops_until_explicit_reconciliation(execution_request):
    effects = []

    async def lost(_arguments, _context):
        effects.append("external-started")
        raise RuntimeError("Synthetic loss after external dispatch")

    gateway = ToolGateway()
    gateway.register(ToolDefinition("external_action", "External action", ToolData, Result, lost))
    request = execution_request.model_copy(update={"allowed_tools": frozenset({"external_action"})})
    model = ScriptedFakeModel([reply(None, call("external_action", "unknown")), reply()])
    engine = LangGraphEngine(model, gateway)
    paused = await engine.wait(await engine.start(request))
    assert paused.error_code == "effect_unknown" and not paused.checkpoint.safe_to_resume
    assert len(model.requests) == 1 and effects == ["external-started"]
    with pytest.raises(DomainError, match="reconciliation"):
        await LangGraphEngine(ScriptedFakeModel([reply()]), gateway).resume(
            paused.checkpoint, request.actor, attempt_id="unsafe-restart"
        )


async def test_real_graph_checkpoint_outage_before_tool_starts_no_effect(
    execution_request, gateway
):
    async def save(checkpoint):
        if checkpoint.backend_state.get("scoped_tool_intents"):
            raise OSError("Synthetic durable storage outage")

    engine = LangGraphEngine(
        ScriptedFakeModel(
            [
                reply(None, call("workspace_write_text", "never", path="never.md", text="Never")),
            ]
        ),
        gateway,
        checkpoint_sink=save,
    )
    result = await engine.wait(await engine.start(execution_request))
    assert result.state == RunState.FAILED
    assert not (execution_request.workspace / "never.md").exists()


async def test_real_graph_keeps_pending_writes_and_prunes_saver_history():
    saved = []

    async def capture(state, _messages):
        saved.append(state)

    model = ScriptedFakeModel([reply(None, call("observe", "observation")), reply()])

    async def observe(_call):
        return "real local observation"

    result = await AgentKernel(model).invoke(
        (ModelMessage(role="user", content="Read and answer"),),
        tools=[
            KernelTool("observe", "Read local data", {"type": "object", "properties": {}}, observe)
        ],
        hooks=KernelHooks(checkpoint=capture),
    )
    assert result.reply.text and saved
    saver = DurableGraphSaver(saved[-1]["thread_id"], state=saved[-1])
    state = saver.get_tuple(saver.config)
    assert "channel_versions" in state.checkpoint and "versions_seen" in state.checkpoint
    assert all("pending_writes" in item for item in saved)
    assert len(saver.storage[saver.thread_id][""]) == 1


async def test_real_graph_long_goal_compacts_and_resumes_across_activation_slices(
    execution_request, gateway, monkeypatch
):
    counts = []
    original = DurableGraphSaver._prune

    def inspect_prune(saver):
        original(saver)
        counts.append(
            (sum(len(items) for items in saver.storage[saver.thread_id].values()), len(saver.blobs))
        )

    monkeypatch.setattr(DurableGraphSaver, "_prune", inspect_prune)
    replies = []
    for index in range(35):
        path = f"evidence-{index}.txt"
        (execution_request.workspace / path).write_text(f"Evidence {index}: " + "x" * 600)
        replies.append(reply(None, call("workspace_read_text", f"read-{index}", path=path)))
    model = ScriptedFakeModel([*replies, reply()])
    request = execution_request.model_copy(update={"budget": ExecutionBudget(max_input_bytes=8000)})
    result = await (engine := LangGraphEngine(model, gateway, activation_max_rounds=9)).wait(
        await engine.start(request)
    )
    slices = 0
    while result.state == RunState.PAUSED:
        assert result.error_code == "activation_yielded" and result.checkpoint.safe_to_resume
        slices += 1
        engine = LangGraphEngine(model, gateway, activation_max_rounds=9)
        result = await engine.wait(
            await engine.resume(
                EngineCheckpoint.model_validate_json(result.checkpoint.model_dump_json()),
                request.actor,
                attempt_id=f"slice-{slices}",
            )
        )
    assert result.state == RunState.CANDIDATE and slices >= 3
    assert len(result.receipts) == 35 and result.usage.rounds == 36
    assert max(checkpoints for checkpoints, _ in counts) <= 2
    assert max(blobs for _, blobs in counts) < 40
    assert len(json.dumps(result.checkpoint.backend_state["langgraph"])) < 70_000
    assert all(len(item.model_dump_json().encode()) <= 8000 for item in model.requests)


def test_retired_harness_cannot_launch_a_native_worker():
    from groupagent.execution import HarnessEngine

    with pytest.raises(DomainError) as error:
        HarnessEngine(object(), object(), object())
    assert error.value.code == "execution_backend_retired"


async def test_real_graph_host_validation_feedback_survives_native_checkpoint(
    execution_request, gateway
):
    initial = LangGraphEngine(ScriptedFakeModel([reply("Unchecked candidate")]), gateway)
    first = await initial.wait(await initial.start(execution_request))
    feedback = "Host validation: report.md was not created. Create and inspect the actual file."
    checkpoint = first.checkpoint.model_copy(
        update={
            "state": RunState.PAUSED,
            "messages": (*first.checkpoint.messages, ModelMessage(role="user", content=feedback)),
        }
    )
    model = ScriptedFakeModel(
        [
            reply(
                None,
                call(
                    "workspace_write_text",
                    "repair-validation",
                    path="report.md",
                    text="Verified artifact",
                ),
            ),
            reply(None, call("workspace_read_text", "read-validation", path="report.md")),
            reply(),
        ]
    )
    engine = LangGraphEngine(model, gateway)
    result = await engine.wait(
        await engine.resume(checkpoint, execution_request.actor, attempt_id="validated")
    )
    assert result.state == RunState.CANDIDATE
    assert model.requests[0].messages[-1].content == feedback
    assert (execution_request.workspace / "report.md").read_text() == "Verified artifact"


async def test_real_graph_reconciled_receipt_updates_native_state_without_repeating_action(
    execution_request,
):
    effects = []

    async def lost(_arguments, _context):
        effects.append("attempt")
        raise RuntimeError("Synthetic unknown action")

    def gateway():
        registered = ToolGateway()
        registered.register(
            ToolDefinition("external_action", "External action", ToolData, Result, lost)
        )
        return registered

    request = execution_request.model_copy(update={"allowed_tools": frozenset({"external_action"})})
    initial = LangGraphEngine(
        ScriptedFakeModel([reply(None, call("external_action", "reconciled"))]), gateway()
    )
    paused = await initial.wait(await initial.start(request))
    reconciled = paused.receipts[0].model_copy(update={"effect_state": EffectState.NO_EFFECT})
    checkpoint = paused.checkpoint.model_copy(
        update={
            "safe_to_resume": True,
            "unresolved_tools": frozenset(),
            "receipts": (reconciled,),
            "ledger": (paused.checkpoint.ledger[0].model_copy(update={"receipt": reconciled}),),
            "messages": tuple(
                message.model_copy(update={"content": reconciled.model_dump_json()})
                if message.role == "tool" and message.tool_call_id == reconciled.call_id
                else message
                for message in paused.checkpoint.messages
            ),
        }
    )
    model = ScriptedFakeModel(
        [reply("Confirmed unpublished failure; no external action was repeated")]
    )
    engine = LangGraphEngine(model, gateway())
    result = await engine.wait(
        await engine.resume(checkpoint, request.actor, attempt_id="reconciled")
    )
    assert result.state == RunState.CANDIDATE and effects == ["attempt"]
    observations = [
        json.loads(message.content)
        for message in model.requests[0].messages
        if message.role == "tool"
    ]
    assert observations[-1]["effect_state"] == "no_effect"


async def test_real_kernel_preserves_modelport_cancellation():
    class CancelledOfflineModel:
        name = "scripted-offline-cancellation"

        async def complete(self, _request):
            raise asyncio.CancelledError()

    with pytest.raises(asyncio.CancelledError):
        await AgentKernel(CancelledOfflineModel()).invoke(
            (ModelMessage(role="user", content="Wait"),)
        )


async def test_unlimited_usage_estimate_is_metering_and_cannot_stop_work(
    execution_request, gateway
):
    model = ScriptedFakeModel(
        [
            reply().model_copy(
                update={"usage": TokenUsage(input_tokens=10_000_000, output_tokens=11)}
            ),
        ]
    )
    engine = LangGraphEngine(model, gateway)
    result = await engine.wait(await engine.start(execution_request))
    assert result.state == RunState.CANDIDATE
    assert result.usage.tokens == 10_000_011


async def test_last_completed_tool_receipt_recovers_before_its_graph_write(
    execution_request, gateway
):
    async def stop_after_receipt(event):
        if event.kind == "tool_finished":
            raise DomainError("synthetic_worker_lost", "Synthetic loss after the last receipt")

    model = ScriptedFakeModel(
        [
            reply(None, call("workspace_write_text", "last-write", path="last.md", text="Once")),
        ]
    )
    initial = LangGraphEngine(model, gateway, event_sink=stop_after_receipt)
    paused = await initial.wait(await initial.start(execution_request))
    assert paused.checkpoint.safe_to_resume and len(paused.receipts) == 1
    assert "scoped_pending_tools" not in paused.checkpoint.backend_state
    resumed_model = ScriptedFakeModel([reply()])
    resumed = LangGraphEngine(resumed_model, gateway)
    result = await asyncio.wait_for(
        resumed.wait(
            await resumed.resume(
                paused.checkpoint, execution_request.actor, attempt_id="after-last-tool-receipt"
            )
        ),
        1,
    )
    assert result.state == RunState.CANDIDATE and result.usage.tool_calls == 1
    assert (execution_request.workspace / "last.md").read_text() == "Once"
    assert len(resumed_model.requests) == 1


@pytest.mark.parametrize("malformed", ["empty", "duplicate"])
async def test_real_graph_repairs_malformed_reply_without_ending_goal(
    execution_request, gateway, malformed
):
    bad = (
        reply("")
        if malformed == "empty"
        else reply(
            None,
            call("workspace_write_text", "duplicate", path="bad.md", text="Bad"),
            call("workspace_write_text", "duplicate", path="bad.md", text="Bad"),
        )
    )
    model = ScriptedFakeModel([bad, reply("Valid final answer")])
    engine = LangGraphEngine(model, gateway)
    result = await engine.wait(await engine.start(execution_request))
    assert result.state == RunState.CANDIDATE and result.candidate_text == "Valid final answer"
    assert len(model.requests) == 2 and result.receipts == ()
    assert not (execution_request.workspace / "bad.md").exists()


@pytest.mark.parametrize("unavailable", ["typo", "outside_grant"])
@pytest.mark.parametrize("peer_first", [False, True])
async def test_unavailable_batch_repairs_before_any_valid_peer_dispatch(
    execution_request,
    gateway,
    unavailable,
    peer_first,
):
    hidden_effects = []

    async def hidden(_arguments, _context):
        hidden_effects.append("forbidden")
        return ToolData()

    gateway.register(
        ToolDefinition(
            "hidden_admin",
            "Known host tool outside this grant",
            ToolData,
            ToolData,
            hidden,
        )
    )
    invalid = call("obsolete_tool" if unavailable == "typo" else "hidden_admin", "unavailable")
    peer = call("workspace_write_text", "must-not-run", path="premature.md", text="Invalid plan")
    request = execution_request.model_copy(
        update={
            "required_tools": frozenset({"workspace_write_text", "workspace_read_text"}),
        }
    )
    model = ScriptedFakeModel(
        [
            reply(None, *((peer, invalid) if peer_first else (invalid, peer))),
            reply(
                None,
                call(
                    "workspace_write_text",
                    "corrected-write",
                    path="actual.md",
                    text="Verified artifact",
                ),
            ),
            reply(None, call("workspace_read_text", "corrected-read", path="actual.md")),
            reply(),
        ]
    )
    engine = LangGraphEngine(model, gateway)
    result = await asyncio.wait_for(engine.wait(await engine.start(request)), 1)
    assert result.state == RunState.CANDIDATE and result.usage.rounds == 4
    assert result.usage.tool_calls == 2 and [receipt.call_id for receipt in result.receipts] == [
        "corrected-write",
        "corrected-read",
    ]
    assert not (request.workspace / "premature.md").exists() and not hidden_effects
    assert (request.workspace / "actual.md").read_text() == "Verified artifact"
    feedback = json.loads(model.requests[1].messages[-1].content)
    assert feedback["type"] == "model_repair" and feedback["code"] == "tool_proposal_rejected"
    assert feedback["batch_executed"] is False and feedback["effect_state"] == "not_started"
    assert feedback["issues"] == [
        {
            "call_id": "unavailable",
            "name": invalid.name,
            "code": "tool_not_registered" if unavailable == "typo" else "tool_not_available",
        }
    ]
    assert set(feedback["available_tools"]) == request.allowed_tools
    assert "hidden_admin" not in result.checkpoint.backend_state["active_tools"]
    assert result.checkpoint.request.allowed_tools == request.allowed_tools


@pytest.mark.parametrize("kind", ["unknown", "empty", "duplicate", "truncated"])
async def test_unchanged_model_repairs_drain_and_resume_without_hot_retry_or_goal_termination(
    execution_request,
    gateway,
    kind,
):
    bad = []
    for index in range(4):
        if kind == "unknown":
            response = reply(None, call("obsolete_tool", f"renamed-{index}"))
        elif kind == "empty":
            response = reply("")
        elif kind == "duplicate":
            response = reply(
                None,
                call("workspace_write_text", f"duplicate-{index}", path="never.md", text="Never"),
                call("workspace_write_text", f"duplicate-{index}", path="never.md", text="Never"),
            )
        else:
            response = reply("Truncated reply").model_copy(update={"finish_reason": "length"})
        bad.append(
            response.model_copy(
                update={"usage": TokenUsage(input_tokens=17 + index, output_tokens=11)}
            )
        )
    model = ScriptedFakeModel(
        [
            *bad,
            reply(
                None,
                call(
                    "workspace_write_text",
                    "repaired-write",
                    path="repaired.md",
                    text="Recovered artifact",
                ),
            ),
            reply(None, call("workspace_read_text", "repaired-read", path="repaired.md")),
            reply(),
        ]
    )
    initial = LangGraphEngine(model, gateway)
    first = await asyncio.wait_for(initial.wait(await initial.start(execution_request)), 1)
    assert first.state == RunState.PAUSED and first.error_code == "activation_yielded"
    assert first.checkpoint.safe_to_resume and first.usage.rounds == 3
    assert first.checkpoint.backend_state["activation_yield_reason"] == "model_repair_stalled"
    assert first.checkpoint.backend_state["model_repair_detector"]["repeat_streak"] == 2
    assert first.receipts == () and len(model.requests) == 3
    # Framework restart retains the detector and replaces prior host repair
    # feedback. A still-unchanged reply yields after one paid request, not128.
    resumed = LangGraphEngine(model, gateway)
    second = await asyncio.wait_for(
        resumed.wait(
            await resumed.resume(
                EngineCheckpoint.model_validate_json(first.checkpoint.model_dump_json()),
                execution_request.actor,
                attempt_id="unchanged-again",
            )
        ),
        1,
    )
    assert second.state == RunState.PAUSED and second.error_code == "activation_yielded"
    assert second.usage.rounds == 4 and len(model.requests) == 4
    assert len(second.checkpoint.backend_state["model_repair_detector"]["recent"]) <= 16
    feedback_messages = [
        message
        for message in second.checkpoint.messages
        if message.role == "user" and '"type":"model_repair"' in (message.content or "")
    ]
    assert len(feedback_messages) == 1 and second.receipts == ()
    corrected = LangGraphEngine(model, gateway)
    final = await asyncio.wait_for(
        corrected.wait(
            await corrected.resume(
                second.checkpoint,
                execution_request.actor,
                attempt_id="corrected-plan",
            )
        ),
        1,
    )
    assert final.state == RunState.CANDIDATE and final.usage.rounds == 7
    assert final.usage.tool_calls == 2 and len(model.requests) == 7
    assert (execution_request.workspace / "repaired.md").read_text() == "Recovered artifact"
    assert not (execution_request.workspace / "never.md").exists()
    assert "model_repair_detector" not in final.checkpoint.backend_state
