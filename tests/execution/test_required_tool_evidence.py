"""Host-only process requirement contracts; sandbox/model are explicit fixtures."""

import asyncio
from dataclasses import replace

import pytest

from groupagent.execution import (
    ModelReply,
    ModelToolCall,
    RunState,
    ScopedToolEngine,
    ScriptedFakeModel,
    TokenUsage,
)
from groupagent.sandbox import ProcessResult
from groupagent.tools import (
    EffectState,
    PolicyDecision,
    ToolContext,
    ToolData,
    ToolDefinition,
    ToolGateway,
)
from groupagent.tools.compute import register_compute_tools
from groupagent.tools.models import ToolReceipt, successful_required_tools


class ExplicitProcessFixture:
    allowed_programs = frozenset({"python3", "ls", "cat", "wc", "head", "tail", "sort"})

    def __init__(self):
        self.calls = []

    async def run(self, spec, context):
        self.calls.append(spec)
        return ProcessResult(
            image="sha256:" + "a" * 64, exit_code=0, stdout="synthetic", elapsed_seconds=0
        )


@pytest.mark.parametrize(
    "actual,required,args",
    [
        ("shell_command", "process_run", {"command": "wc"}),
        ("process_run", "shell_command", {"program": "wc"}),
    ],
)
async def test_registered_process_family_fulfills_required_execution(
    execution_request, actual, required, args
):
    runner = ExplicitProcessFixture()
    gateway = ToolGateway()
    register_compute_tools(gateway, runner)
    request = execution_request.model_copy(
        update={
            "allowed_tools": frozenset({actual, required}),
            "required_tools": frozenset({required}),
        }
    )
    model = ScriptedFakeModel(
        [
            ModelReply(
                tool_calls=(ModelToolCall(id="process", name=actual, arguments=args),),
                finish_reason="tool_calls",
                usage=TokenUsage(input_tokens=1, output_tokens=1),
            ),
            ModelReply(
                text="Synthetic candidate", usage=TokenUsage(input_tokens=1, output_tokens=1)
            ),
        ]
    )
    engine = ScopedToolEngine(model, gateway)
    result = await engine.wait(await engine.start(request))
    assert result.state == RunState.CANDIDATE and len(runner.calls) == 1
    assert result.receipts[0].required_tool_evidence == {actual, required}
    assert required not in successful_required_tools(result.receipts, frozenset({actual}))


async def test_replayed_process_evidence_does_not_revive_forbidden_alternative(scope):
    permitted = {"shell_command", "process_run"}

    async def policy(grant):
        return PolicyDecision(actor=grant.actor, allowed_tools=frozenset(permitted))

    runner = ExplicitProcessFixture()
    gateway = ToolGateway(policy)
    register_compute_tools(gateway, runner)
    scope = replace(scope, allowed_tools=frozenset(permitted))
    gateway.bind_scope(scope)
    context = ToolContext(scope, "call", "same-key", asyncio.Event())
    receipt = await gateway.invoke("shell_command", {"command": "wc"}, context)
    assert receipt.required_tool_evidence == permitted
    permitted.remove("process_run")
    replay = await gateway.invoke("shell_command", {"command": "wc"}, context)
    assert replay.success and replay.replayed and len(runner.calls) == 1
    assert "process_run" not in replay.required_tool_evidence
    assert "process_run" not in successful_required_tools((replay,), scope.allowed_tools)


async def test_arbitrary_output_cannot_claim_registered_process_capability(scope):
    class Empty(ToolData):
        pass

    class Claimed(ToolData):
        required_tool_evidence: list[str]

    async def claimed(args, context):
        return Claimed(required_tool_evidence=["process_run", "shell_command"])

    gateway = ToolGateway()
    for name in ("process_run", "shell_command"):
        gateway.register(
            ToolDefinition(
                name=name,
                description="Unprivileged fixture",
                input_model=Empty,
                output_model=Claimed,
                handler=claimed,
            )
        )
    scope = replace(scope, allowed_tools=frozenset({"process_run", "shell_command"}))
    gateway.bind_scope(scope)
    receipt = await gateway.invoke(
        "shell_command", {}, ToolContext(scope, "claim", "claim", asyncio.Event())
    )
    assert receipt.success and not receipt.required_tool_evidence
    assert successful_required_tools((receipt,), scope.allowed_tools) == {"shell_command"}


@pytest.mark.parametrize(
    "effect", [EffectState.NOT_STARTED, EffectState.NO_EFFECT, EffectState.UNKNOWN]
)
def test_failed_receipt_and_claimed_text_never_satisfy_process_requirement(effect):
    receipt = ToolReceipt(
        call_id="x",
        tool_name="shell_command",
        idempotency_key="x",
        success=False,
        effect_state=effect,
        output={"text": "process_run succeeded"},
        required_tool_evidence=frozenset({"process_run"}),
    )
    assert not successful_required_tools((receipt,), frozenset({"process_run", "shell_command"}))
