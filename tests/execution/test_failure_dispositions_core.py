"""Real gateway/compute refusal and graph repair with offline scripted inference.

The rejected executable never reaches the runner. Corrected artifacts use the
real scoped filesystem tools; these tests do not claim real sandbox execution.
"""

import asyncio
import json

import pytest

from groupagent.domain import DomainError
from groupagent.execution import (
    EngineCheckpoint,
    LangGraphEngine,
    ModelReply,
    ModelToolCall,
    RunState,
    ScriptedFakeModel,
    TokenUsage,
)
from groupagent.tools import (
    EffectState,
    PolicyDecision,
    ToolData,
    ToolDefinition,
    ToolGateway,
    register_workspace_tools,
)
from groupagent.tools.compute import register_compute_tools
from groupagent.tools.models import (
    ToolFailure,
    ToolFailureDisposition,
    ToolFailureMetadata,
    ToolFailureOrigin,
)


def proposal(name, identifier, **arguments):
    return ModelReply(
        tool_calls=(ModelToolCall(id=identifier, name=name, arguments=arguments),),
        finish_reason="tool_calls",
        usage=TokenUsage(input_tokens=20, output_tokens=10),
    )


def answer():
    return ModelReply(
        text="Verified local artifact is ready", usage=TokenUsage(input_tokens=20, output_tokens=10)
    )


def correction():
    return [
        proposal(
            "workspace_write_text",
            "corrected-write",
            path="repaired.md",
            text="# Verified artifact\nActual file bytes.",
        ),
        proposal("workspace_read_text", "corrected-read", path="repaired.md"),
        answer(),
    ]


async def test_real_rejected_compute_command_is_observed_then_existing_tools_repair_artifact(
    execution_request, gateway
):
    class MustNotDispatchSandbox:
        allowed_programs = frozenset(
            {"python3", "pdftoppm", "ls", "cat", "wc", "head", "tail", "sort"}
        )
        calls = []

        async def run(self, spec, context):
            self.calls.append(spec)
            raise AssertionError("A rejected executable must not reach the runner")

    runner = MustNotDispatchSandbox()
    register_compute_tools(gateway, runner)
    request = execution_request.model_copy(
        update={
            "allowed_tools": execution_request.allowed_tools | {"process_run", "python_compute"},
            "required_tools": frozenset({"workspace_write_text", "workspace_read_text"}),
        }
    )
    model = ScriptedFakeModel(
        [
            proposal(
                "process_run",
                "denied-pdftoppm",
                program="pdftoppm",
                args=["-png", "input.pdf", "pages"],
            ),
            *correction(),
        ]
    )
    engine = LangGraphEngine(model, gateway)
    result = await asyncio.wait_for(engine.wait(await engine.start(request)), 1)
    assert result.state == RunState.CANDIDATE and len(model.requests) == 4
    assert not runner.calls
    denied = result.receipts[0]
    assert denied.error_code == "permission_denied" and denied.effect_state == EffectState.NO_EFFECT
    assert denied.failure == ToolFailureMetadata(
        origin=ToolFailureOrigin.CAPABILITY, disposition=ToolFailureDisposition.REPAIR
    )
    observation = json.loads(model.requests[1].messages[-1].content)
    assert observation["failure"] == {"origin": "capability", "disposition": "repair"}
    assert observation["error_code"] == "permission_denied" and not observation["success"]
    assert result.receipts[-1].output["text"] == "# Verified artifact\nActual file bytes."
    assert (
        request.workspace / "repaired.md"
    ).read_text() == "# Verified artifact\nActual file bytes."
    assert result.checkpoint.request.allowed_tools == request.allowed_tools


@pytest.mark.parametrize("code", ["permission_denied", "scope_violation", "tool_not_registered"])
async def test_trusted_no_effect_repair_disposition_wins_over_ambiguous_error_name(
    execution_request, gateway, code
):
    async def unavailable(_arguments, _context):
        raise ToolFailure(
            code,
            "This capability cannot accept this proposal",
            no_effect=True,
            origin=ToolFailureOrigin.ARGUMENTS,
            disposition=ToolFailureDisposition.REPAIR,
        )

    gateway.register(
        ToolDefinition(
            "reviewed_capability", "Reviewed host capability", ToolData, ToolData, unavailable
        )
    )
    request = execution_request.model_copy(
        update={
            "allowed_tools": execution_request.allowed_tools | {"reviewed_capability"},
            "required_tools": frozenset({"workspace_write_text", "workspace_read_text"}),
        }
    )
    model = ScriptedFakeModel([proposal("reviewed_capability", "bad-proposal"), *correction()])
    engine = LangGraphEngine(model, gateway)
    result = await engine.wait(await engine.start(request))
    assert result.state == RunState.CANDIDATE and result.receipts[0].error_code == code
    assert result.receipts[0].effect_state == EffectState.NO_EFFECT
    assert (request.workspace / "repaired.md").is_file()


async def test_real_policy_revocation_remains_a_fence_with_the_same_permission_code(
    execution_request,
):
    async def revoked(scope):
        return PolicyDecision(actor=scope.actor, allowed_tools=scope.allowed_tools, active=False)

    gateway = ToolGateway(revoked)
    register_workspace_tools(gateway)
    model = ScriptedFakeModel(
        [
            proposal(
                "workspace_write_text", "must-not-write", path="forbidden.md", text="Forbidden"
            ),
            *correction(),
        ]
    )
    engine = LangGraphEngine(model, gateway)
    result = await engine.wait(await engine.start(execution_request))
    assert result.state == RunState.FAILED and result.error_code == "permission_denied"
    assert len(model.requests) == 1 and not (execution_request.workspace / "forbidden.md").exists()
    assert not (execution_request.workspace / "repaired.md").exists()
    assert result.receipts[0].failure == ToolFailureMetadata(
        origin=ToolFailureOrigin.AUTHORIZATION, disposition=ToolFailureDisposition.FENCE
    )


async def test_unknown_effect_overrides_even_a_misclassified_repair_receipt(execution_request):
    effects = []

    async def uncertain(_arguments, _context):
        effects.append("external-dispatch")
        raise ToolFailure(
            "permission_denied",
            "Outcome lost after dispatch",
            origin=ToolFailureOrigin.CAPABILITY,
            disposition=ToolFailureDisposition.REPAIR,
        )

    class MisclassifiedGateway(ToolGateway):
        async def invoke(self, *args, **kwargs):
            receipt = await super().invoke(*args, **kwargs)
            return receipt.model_copy(
                update={
                    "failure": ToolFailureMetadata(
                        origin=ToolFailureOrigin.CAPABILITY,
                        disposition=ToolFailureDisposition.REPAIR,
                    )
                }
            )

    gateway = MisclassifiedGateway()
    gateway.register(
        ToolDefinition("external_action", "Unknown external action", ToolData, ToolData, uncertain)
    )
    request = execution_request.model_copy(update={"allowed_tools": frozenset({"external_action"})})
    model = ScriptedFakeModel([proposal("external_action", "uncertain"), answer()])
    engine = LangGraphEngine(model, gateway)
    result = await engine.wait(await engine.start(request))
    assert result.state == RunState.PAUSED and result.error_code == "effect_unknown"
    assert (
        result.receipts[0].effect_state == EffectState.UNKNOWN
        and not result.checkpoint.safe_to_resume
    )
    assert effects == ["external-dispatch"] and len(model.requests) == 1


@pytest.mark.parametrize("legacy", [False, True])
async def test_cached_authority_fence_cannot_become_a_model_observation_on_restart(
    execution_request, legacy
):
    calls = []

    async def refused(_arguments, _context):
        calls.append("authority-check")
        raise ToolFailure(
            "permission_denied",
            "Host authority was revoked",
            no_effect=True,
            origin=ToolFailureOrigin.AUTHORIZATION,
            disposition=ToolFailureDisposition.FENCE,
        )

    def registered():
        gateway = ToolGateway()
        gateway.register(
            ToolDefinition("authorized_action", "Reviewed host action", ToolData, ToolData, refused)
        )
        return gateway

    async def lost_after_receipt(event):
        if event.kind == "tool_finished":
            raise DomainError("synthetic_worker_lost", "Stopped after the known refusal receipt")

    request = execution_request.model_copy(
        update={"allowed_tools": frozenset({"authorized_action"})}
    )
    initial = LangGraphEngine(
        ScriptedFakeModel([proposal("authorized_action", "refused")]),
        registered(),
        event_sink=lost_after_receipt,
    )
    paused = await initial.wait(await initial.start(request))
    checkpoint = EngineCheckpoint.model_validate_json(paused.checkpoint.model_dump_json())
    assert checkpoint.safe_to_resume and len(checkpoint.ledger) == 1
    if legacy:
        receipt = checkpoint.receipts[0].model_copy(update={"failure": None})
        checkpoint = checkpoint.model_copy(
            update={
                "receipts": (receipt,),
                "ledger": (checkpoint.ledger[0].model_copy(update={"receipt": receipt}),),
                "messages": tuple(
                    message.model_copy(update={"content": receipt.model_dump_json()})
                    if message.role == "tool"
                    else message
                    for message in checkpoint.messages
                ),
            }
        )
    model = ScriptedFakeModel([answer()])
    resumed = LangGraphEngine(model, registered())
    result = await asyncio.wait_for(
        resumed.wait(
            await resumed.resume(
                checkpoint,
                request.actor,
                attempt_id="cached-refusal",
            )
        ),
        1,
    )
    assert result.state == RunState.FAILED and result.error_code == "permission_denied"
    assert not model.requests and calls == ["authority-check"]
