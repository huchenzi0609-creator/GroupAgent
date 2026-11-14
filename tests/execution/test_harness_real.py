import asyncio
import os

import pytest

from groupagent.domain import Role
from groupagent.execution import (
    ExecutionBudget,
    HarnessEngine,
    ModelReply,
    ModelToolCall,
    RunState,
    ScriptedFakeModel,
    TokenUsage,
)
from groupagent.sandbox import DockerSandbox, SandboxLimits
from groupagent.tools import ToolData, ToolDefinition, register_request_input_tool
from groupagent.tools.compute import register_compute_tools
from groupagent.tools.models import ToolFailure, ToolInputRequired

IMAGE = os.environ.get("GROUPAGENT_HARNESS_IMAGE", "")
HOST = os.environ.get("GROUPAGENT_DOCKER_HOST")
pytestmark = [
    pytest.mark.sandbox,
    pytest.mark.skipif(not IMAGE, reason="Requires real pinned Harness Docker image"),
]


def runner():
    return DockerSandbox(
        IMAGE, docker_host=HOST, limits=SandboxLimits(memory_mb=1024, temp_mb=256, pids=128)
    )


def write_reply():
    return ModelReply(
        tool_calls=(
            ModelToolCall(
                id="synthetic-write",
                name="mcp__groupagent__workspace_write_text",
                arguments={"path": "harness.txt", "text": "Synthetic Harness output"},
            ),
        ),
        finish_reason="tool_calls",
        usage=TokenUsage(input_tokens=30, output_tokens=20),
    )


def final_reply(text="Synthetic verified candidate"):
    return ModelReply(text=text, usage=TokenUsage(input_tokens=40, output_tokens=10))


async def test_real_sdk_mcp_tool_loop_scoped_gateway_and_log_upload_disabled(
    execution_request, gateway
):
    model = ScriptedFakeModel([write_reply(), final_reply()])
    engine = HarnessEngine(model, gateway, runner(), synthetic_diagnostics=True)
    request = execution_request.model_copy(
        update={"budget": ExecutionBudget(max_active_seconds=45)}
    )
    result = await engine.wait(await engine.start(request))
    assert result.state == RunState.CANDIDATE, (result.error_code, engine.last_diagnostic)
    assert result.candidate_text == "Synthetic verified candidate"
    assert result.receipts[0].success and result.usage.rounds == 2
    assert (request.workspace / "harness.txt").read_text() == "Synthetic Harness output"
    assert result.checkpoint.backend_state["sessions"]
    for model_request in model.requests:
        assert {tool["function"]["name"] for tool in model_request.tools} == {
            "mcp__groupagent__workspace_write_text",
            "mcp__groupagent__workspace_read_text",
        }


async def test_real_sdk_round_pause_and_native_session_resume(execution_request, gateway):
    engine = HarnessEngine(
        ScriptedFakeModel([write_reply()]), gateway, runner(), synthetic_diagnostics=True
    )
    request = execution_request.model_copy(
        update={"budget": ExecutionBudget(max_rounds=1, max_active_seconds=60)}
    )
    paused = await engine.wait(await engine.start(request))
    assert paused.state == RunState.PAUSED and paused.error_code == "round_budget_exhausted", (
        engine.last_diagnostic
    )
    assert paused.checkpoint.safe_to_resume
    restored_model = ScriptedFakeModel([final_reply("Resumed candidate")])
    restored = HarnessEngine(restored_model, gateway, runner(), synthetic_diagnostics=True)
    run_id = await restored.resume(
        paused.checkpoint,
        request.actor.model_copy(update={"role": Role.OWNER}),
        attempt_id="native-resume",
        budget=ExecutionBudget(max_rounds=3, max_active_seconds=120),
    )
    result = await restored.wait(run_id)
    assert result.state == RunState.CANDIDATE, (result.error_code, restored.last_diagnostic)
    assert result.usage.rounds == 2 and len(result.receipts) == 1
    assert "Synthetic Harness output" in restored_model.requests[0].model_dump_json()


async def test_real_sdk_cancel_stops_model_and_container(execution_request, gateway):
    model = ScriptedFakeModel([final_reply()], delay_seconds=100)
    engine = HarnessEngine(model, gateway, runner(), synthetic_diagnostics=True)
    run_id = await engine.start(execution_request)
    await asyncio.wait_for(model.started.wait(), 35)
    await engine.cancel(run_id, execution_request.actor)
    result = await asyncio.wait_for(engine.wait(run_id), 8)
    assert result.state == RunState.CANCELLED and model.cancelled.is_set()


async def test_real_sdk_append_blocks_stale_proposal(execution_request, gateway):
    model = ScriptedFakeModel(
        [write_reply(), final_reply("Interim updated answer"), final_reply("Updated candidate")],
        delay_seconds=0.15,
    )
    engine = HarnessEngine(model, gateway, runner(), synthetic_diagnostics=True)
    run_id = await engine.start(execution_request)
    await asyncio.wait_for(model.started.wait(), 35)
    await engine.append_input(
        run_id, execution_request.actor, "Return text only; do not write files", 1
    )
    result = await engine.wait(run_id)
    assert result.state == RunState.CANDIDATE and result.revision == 2, (
        result.error_code,
        engine.last_diagnostic,
    )
    assert not (execution_request.workspace / "harness.txt").exists()
    assert "Return text only" in model.requests[-1].model_dump_json()
    assert "Return text only" in model.requests[1].model_dump_json()


async def test_sdk_local_argument_failure_cannot_be_covered_by_success_text(
    execution_request, gateway
):
    invalid = write_reply().model_copy(
        update={
            "tool_calls": (
                ModelToolCall(
                    id="invalid-native-args",
                    name="mcp__groupagent__workspace_write_text",
                    arguments={"path": "never.txt"},
                ),
            )
        }
    )
    engine = HarnessEngine(
        ScriptedFakeModel([invalid, final_reply("All done")]),
        gateway,
        runner(),
        synthetic_diagnostics=True,
    )
    request = execution_request.model_copy(
        update={"required_tools": frozenset({"workspace_write_text"})}
    )
    result = await engine.wait(await engine.start(request))
    assert result.state == RunState.FAILED and result.error_code == "validation_required", (
        engine.last_diagnostic
    )
    assert not (execution_request.workspace / "never.txt").exists()


async def test_real_sdk_compute_uses_separate_scoped_worker(execution_request, gateway):
    compute_image = os.environ.get("GROUPAGENT_WORKER_IMAGE")
    if not compute_image:
        pytest.skip("Requires separate real compute worker image")
    register_compute_tools(gateway, DockerSandbox(compute_image, docker_host=HOST))
    reply = ModelReply(
        tool_calls=(
            ModelToolCall(
                id="compute",
                name="mcp__groupagent__python_compute",
                arguments={"code": "print(2+3)"},
            ),
        ),
        finish_reason="tool_calls",
        usage=TokenUsage(input_tokens=20, output_tokens=10),
    )
    model = ScriptedFakeModel([reply, final_reply("The verified result is 5")])
    engine = HarnessEngine(model, gateway, runner(), synthetic_diagnostics=True)
    request = execution_request.model_copy(
        update={
            "allowed_tools": frozenset({"python_compute"}),
            "required_tools": frozenset({"python_compute"}),
        }
    )
    result = await engine.wait(await engine.start(request))
    assert result.state == RunState.CANDIDATE, (result.error_code, engine.last_diagnostic)
    assert result.receipts[0].output["stdout"].strip() == "5"


async def test_real_sdk_host_disconnect_closes_runtime_and_model_wait(execution_request, gateway):
    model = ScriptedFakeModel([final_reply()], delay_seconds=100)
    engine = HarnessEngine(model, gateway, runner(), synthetic_diagnostics=True)
    run_id = await engine.start(execution_request)
    await asyncio.wait_for(model.started.wait(), 35)
    engine._bridges[run_id].process.stdin.close()
    result = await asyncio.wait_for(engine.wait(run_id), 10)
    assert result.state == RunState.PAUSED and result.error_code == "harness_unavailable"
    assert model.cancelled.is_set() and not result.checkpoint.safe_to_resume


async def test_real_sdk_request_input_pauses_without_extra_model_call_and_resumes(
    execution_request, gateway
):
    register_request_input_tool(gateway)
    question = ModelReply(
        tool_calls=(
            ModelToolCall(
                id="premature-write",
                name="mcp__groupagent__workspace_write_text",
                arguments={"path": "before-input.txt", "text": "Must not execute"},
            ),
            ModelToolCall(
                id="missing-period",
                name="mcp__groupagent__request_input",
                arguments={"question": "Which reporting period?", "required_inputs": ["period"]},
            ),
        ),
        finish_reason="tool_calls",
        usage=TokenUsage(input_tokens=30, output_tokens=20),
    )
    model = ScriptedFakeModel([question])
    engine = HarnessEngine(model, gateway, runner(), synthetic_diagnostics=True)
    request = execution_request.model_copy(
        update={
            "allowed_tools": frozenset({"request_input", "workspace_write_text"}),
            "budget": ExecutionBudget(max_active_seconds=60),
        }
    )
    waiting = await engine.wait(await engine.start(request))
    assert waiting.state == RunState.WAITING_INPUT, (waiting.error_code, engine.last_diagnostic)
    assert waiting.checkpoint.input_request.question == "Which reporting period?"
    assert waiting.checkpoint.safe_to_resume and waiting.checkpoint.backend_state["sessions"]
    assert len(model.requests) == 1 and waiting.usage.rounds == 1
    assert not (request.workspace / "before-input.txt").exists()
    resumed_model = ScriptedFakeModel([final_reply("Candidate for the supplied period")])
    resumed = HarnessEngine(resumed_model, gateway, runner(), synthetic_diagnostics=True)
    checkpoint = waiting.checkpoint.model_copy(
        update={
            "backend_state": {
                **waiting.checkpoint.backend_state,
                "resume_instruction": "Accepted input: use Q4 2030.",
            }
        }
    )
    result = await resumed.wait(
        await resumed.resume(checkpoint, request.actor, attempt_id="input-resume")
    )
    assert result.state == RunState.CANDIDATE, (result.error_code, resumed.last_diagnostic)
    assert result.usage.rounds == 2 and result.usage.tool_calls == 1
    assert result.checkpoint.input_request is None
    assert "Q4 2030" in resumed_model.requests[0].model_dump_json()


async def test_real_sdk_trusted_precondition_question_is_not_a_successful_write(
    execution_request, gateway
):
    class Empty(ToolData):
        pass

    async def missing_confirmation(_arguments, _context):
        raise ToolInputRequired(
            "请明确确认真人任务的完成状态。", required_inputs=("真人任务完成确认",)
        )

    gateway.register(
        ToolDefinition(
            name="synthetic_guarded_task",
            description="Explicit synthetic precondition",
            input_model=Empty,
            output_model=Empty,
            handler=missing_confirmation,
            may_request_input=True,
        )
    )
    request = execution_request.model_copy(
        update={
            "allowed_tools": frozenset({"synthetic_guarded_task"}),
            "required_tools": frozenset({"synthetic_guarded_task"}),
            "budget": ExecutionBudget(max_active_seconds=60),
        }
    )
    model = ScriptedFakeModel(
        [
            ModelReply(
                tool_calls=(
                    ModelToolCall(
                        id="confirm-human",
                        name="mcp__groupagent__synthetic_guarded_task",
                        arguments={},
                    ),
                ),
                finish_reason="tool_calls",
                usage=TokenUsage(input_tokens=20, output_tokens=10),
            )
        ]
    )
    engine = HarnessEngine(model, gateway, runner(), synthetic_diagnostics=True)
    result = await engine.wait(await engine.start(request))
    assert result.state == RunState.WAITING_INPUT, (result.error_code, engine.last_diagnostic)
    assert result.checkpoint.safe_to_resume and result.checkpoint.input_request
    assert not result.receipts[0].success and result.receipts[0].effect_state.value == "no_effect"
    assert len(model.requests) == 1 and result.usage.tool_calls == 1


async def test_real_sdk_no_effect_failure_then_alternative_verified_output(
    execution_request, gateway
):
    class Empty(ToolData):
        pass

    async def rejected(_args, _context):
        raise ToolFailure(
            "unsupported_feature", "Synthetic generation path is unsupported", no_effect=True
        )

    gateway.register(
        ToolDefinition(
            name="document_generate",
            description="Explicit synthetic no-effect failure",
            input_model=Empty,
            output_model=Empty,
            handler=rejected,
        )
    )
    rejected_reply = ModelReply(
        tool_calls=(
            ModelToolCall(
                id="unsupported-doc", name="mcp__groupagent__document_generate", arguments={}
            ),
        ),
        finish_reason="tool_calls",
        usage=TokenUsage(input_tokens=20, output_tokens=10),
    )
    read_reply = ModelReply(
        tool_calls=(
            ModelToolCall(
                id="read-alternative",
                name="mcp__groupagent__workspace_read_text",
                arguments={"path": "harness.txt"},
            ),
        ),
        finish_reason="tool_calls",
        usage=TokenUsage(input_tokens=20, output_tokens=10),
    )
    request = execution_request.model_copy(
        update={
            "allowed_tools": gateway.registered_names,
            "required_tools": frozenset({"workspace_write_text", "workspace_read_text"}),
            "budget": ExecutionBudget(max_active_seconds=60),
        }
    )
    model = ScriptedFakeModel(
        [rejected_reply, write_reply(), read_reply, final_reply("Verified alternative candidate")]
    )
    engine = HarnessEngine(model, gateway, runner(), synthetic_diagnostics=True)
    result = await engine.wait(await engine.start(request))
    assert result.state == RunState.CANDIDATE, (result.error_code, engine.last_diagnostic)
    assert not result.receipts[0].success and result.receipts[0].effect_state.value == "no_effect"
    assert result.checkpoint.unresolved_tools == frozenset() and result.usage.tool_calls == 3
    assert result.receipts[-1].output["text"] == "Synthetic Harness output"


async def test_real_sdk_uses_gateway_evidence_for_controlled_process_equivalence(
    execution_request, gateway
):
    from groupagent.sandbox import ProcessResult

    class ExplicitProcessFixture:
        allowed_programs = frozenset({"python3", "ls", "cat", "wc", "head", "tail", "sort"})

        async def run(self, spec, context):
            return ProcessResult(
                image="sha256:" + "a" * 64, exit_code=0, stdout="synthetic", elapsed_seconds=0
            )

    register_compute_tools(gateway, ExplicitProcessFixture())
    request = execution_request.model_copy(
        update={
            "allowed_tools": frozenset({"process_run", "shell_command"}),
            "required_tools": frozenset({"process_run"}),
            "budget": ExecutionBudget(max_active_seconds=45),
        }
    )
    model = ScriptedFakeModel(
        [
            ModelReply(
                tool_calls=(
                    ModelToolCall(
                        id="process",
                        name="mcp__groupagent__shell_command",
                        arguments={"command": "wc"},
                    ),
                ),
                finish_reason="tool_calls",
                usage=TokenUsage(input_tokens=20, output_tokens=10),
            ),
            final_reply(),
        ]
    )
    engine = HarnessEngine(model, gateway, runner())
    result = await engine.wait(await engine.start(request))
    assert result.state == RunState.CANDIDATE
    assert result.receipts[0].tool_name == "shell_command"
    assert result.receipts[0].required_tool_evidence == {"shell_command", "process_run"}
