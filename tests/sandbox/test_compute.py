import asyncio
from pathlib import Path

from groupagent.domain import ActorContext, DomainError, Role
from groupagent.execution import (
    ExecutionBudget,
    ModelReply,
    ModelToolCall,
    RunState,
    ScopedToolEngine,
    ScriptedFakeModel,
    TokenUsage,
)
from groupagent.sandbox import ProcessResult
from groupagent.tools import EffectState, ToolContext, ToolGateway, ToolScope
from groupagent.tools.compute import register_compute_tools


class RecordingFakeSandbox:
    allowed_programs = frozenset({"python3", "ls", "cat", "wc", "head", "tail", "sort"})

    def __init__(self):
        self.calls = []

    async def run(self, spec, context):
        self.calls.append(spec)
        return ProcessResult(
            image="sha256:" + "a" * 64, exit_code=0, stdout="synthetic", elapsed_seconds=0
        )


async def test_registered_compute_tools_cannot_invoke_host_or_unapproved_commands(tmp_path: Path):
    fake = RecordingFakeSandbox()
    gateway = ToolGateway()
    register_compute_tools(gateway, fake)
    actor = ActorContext(group_id="synthetic", member_id="member", role=Role.MEMBER)
    scope = ToolScope(
        "synthetic",
        "work",
        actor,
        tmp_path.resolve(),
        frozenset({"python_compute", "shell_command", "process_run"}),
        "trace",
    )
    gateway.bind_scope(scope)

    async def invoke(name, args, call_id):
        return await gateway.invoke(
            name, args, ToolContext(scope, call_id, call_id, asyncio.Event())
        )

    for i, command in enumerate(
        ["cat x; echo bad", "ls | cat", "curl https://example.com", "HOME=/tmp ls", "ls $(whoami)"]
    ):
        result = await invoke("shell_command", {"command": command}, str(i))
        assert not result.success
    assert not fake.calls
    result = await invoke("python_compute", {"code": "print(6*7)"}, "python")
    assert result.success and fake.calls[0].program == "python3"
    assert fake.calls[0].args[:2] == ("-I", "-c")


async def test_path_errors_are_proven_no_effect_before_runner_and_safe_to_repair(tmp_path):
    fake = RecordingFakeSandbox()
    gateway = ToolGateway()
    register_compute_tools(gateway, fake)
    actor = ActorContext(group_id="synthetic", member_id="member", role=Role.MEMBER)
    scope = ToolScope(
        "synthetic", "work", actor, tmp_path.resolve(), frozenset({"python_compute"}), "trace"
    )
    gateway.bind_scope(scope)
    (tmp_path / "exists.txt").write_text("Synthetic existing input")
    (tmp_path / "nested").mkdir()
    invalid = [
        "/work/workspace/result.txt",
        "absent/result.txt",
        "exists.txt",
        "../outside.txt",
        "nested//result.txt",
    ]
    for index, path in enumerate(invalid):
        receipt = await gateway.invoke(
            "python_compute",
            {"code": "print(1)", "output_paths": [path]},
            ToolContext(scope, str(index), str(index), asyncio.Event()),
        )
        assert (
            receipt.error_code == "invalid_path" and receipt.effect_state == EffectState.NO_EFFECT
        )
        assert "cwd=/work" in receipt.message and "output_paths[0]" in receipt.message
        assert path not in receipt.message
    assert fake.calls == []
    fixed = await gateway.invoke(
        "python_compute",
        {"code": "print(1)", "output_paths": ["result.txt"]},
        ToolContext(scope, "fixed", "fixed", asyncio.Event()),
    )
    assert fixed.success and len(fake.calls) == 1


def compute_call(identifier, path="result.txt"):
    return ModelReply(
        tool_calls=(
            ModelToolCall(
                id=identifier,
                name="python_compute",
                arguments={"code": "print(1)", "output_paths": [path]},
            ),
        ),
        finish_reason="tool_calls",
        usage=TokenUsage(input_tokens=10, output_tokens=10),
    )


async def test_model_repairs_invalid_paths_within_existing_round_and_tool_budgets(tmp_path):
    from groupagent.execution import ExecutionRequest

    actor = ActorContext(group_id="synthetic", member_id="member", role=Role.MEMBER)
    request = ExecutionRequest(
        work_id="repair-work",
        attempt_id="repair-attempt",
        actor=actor,
        objective="Compute a result",
        workspace=tmp_path.resolve(),
        allowed_tools=frozenset({"python_compute"}),
        required_tools=frozenset({"python_compute"}),
        trace_id="trace",
        budget=ExecutionBudget(max_rounds=3, max_tool_calls=2),
    )
    fake = RecordingFakeSandbox()
    gateway = ToolGateway()
    register_compute_tools(gateway, fake)
    model = ScriptedFakeModel(
        [
            compute_call("bad", "missing/result.txt"),
            compute_call("fixed"),
            ModelReply(
                text="Candidate after successful computation",
                usage=TokenUsage(input_tokens=10, output_tokens=5),
            ),
        ]
    )
    engine = ScopedToolEngine(model, gateway)
    result = await engine.wait(await engine.start(request))
    assert (
        result.state == RunState.CANDIDATE
        and result.usage.rounds == 3
        and result.usage.tool_calls == 2
    )
    assert result.receipts[0].effect_state == EffectState.NO_EFFECT and len(fake.calls) == 1
    assert "cwd=/work" in model.requests[1].model_dump_json()


async def test_post_start_invalid_path_remains_unknown_and_is_not_auto_repaired(tmp_path):
    from groupagent.execution import ExecutionRequest

    class UncertainSandbox(RecordingFakeSandbox):
        async def run(self, spec, context):
            self.calls.append(spec)
            raise DomainError("invalid_path", "Synthetic failure after process/export began")

    fake = UncertainSandbox()
    gateway = ToolGateway()
    register_compute_tools(gateway, fake)
    actor = ActorContext(group_id="synthetic", member_id="member", role=Role.MEMBER)
    request = ExecutionRequest(
        work_id="unknown-work",
        attempt_id="unknown-attempt",
        actor=actor,
        objective="Compute",
        workspace=tmp_path.resolve(),
        allowed_tools=frozenset({"python_compute"}),
        trace_id="trace",
    )
    model = ScriptedFakeModel([compute_call("uncertain")])
    engine = ScopedToolEngine(model, gateway)
    result = await engine.wait(await engine.start(request))
    assert result.state == RunState.PAUSED and result.error_code == "effect_unknown"
    assert result.receipts[0].effect_state == EffectState.UNKNOWN and len(fake.calls) == 1
