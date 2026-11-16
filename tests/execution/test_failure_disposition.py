"""Trusted failure boundaries disambiguate repairable proposals from lost authority."""

import asyncio

import pytest

from groupagent.domain import ActorContext, DomainError, Role
from groupagent.sandbox import ProcessResult
from groupagent.tools import ToolContext, ToolData, ToolDefinition, ToolGateway, ToolScope
from groupagent.tools.compute import register_compute_tools
from groupagent.tools.models import (
    EffectState,
    ToolFailure,
    ToolFailureDisposition,
    ToolFailureOrigin,
    ToolReceipt,
    failure_disposition,
)


class RecordingRunner:
    allowed_programs = frozenset({"python3", "ls", "cat", "wc", "head", "tail", "sort"})

    def __init__(self):
        self.calls = []

    async def run(self, spec, context):
        self.calls.append(spec)
        return ProcessResult(
            image="sha256:" + "a" * 64, exit_code=0, stdout="synthetic", elapsed_seconds=0
        )


def context(gateway, tmp_path):
    actor = ActorContext(group_id="synthetic-group", member_id="synthetic-owner", role=Role.OWNER)
    scope = ToolScope(
        actor.group_id,
        "synthetic-work",
        actor,
        tmp_path.resolve(),
        gateway.registered_names,
        "synthetic-trace",
    )
    gateway.bind_scope(scope)
    return ToolContext(scope, "synthetic-call", "synthetic-work:synthetic-call", asyncio.Event())


@pytest.mark.parametrize(
    "name,args",
    [
        ("shell_command", {"command": "pdftoppm -png -r 70 file.pdf prev"}),
        ("process_run", {"program": "pdftoppm", "args": ["file.pdf"]}),
        ("shell_command", {"command": "ls | cat"}),
    ],
)
async def test_unavailable_command_is_verified_unstarted_repair_observation(tmp_path, name, args):
    runner = RecordingRunner()
    gateway = ToolGateway()
    register_compute_tools(gateway, runner)
    receipt = await gateway.invoke(name, args, context(gateway, tmp_path))
    assert (
        receipt.error_code == "permission_denied" and receipt.effect_state == EffectState.NO_EFFECT
    )
    assert receipt.failure.origin == ToolFailureOrigin.CAPABILITY
    assert failure_disposition(receipt) == ToolFailureDisposition.REPAIR
    assert runner.calls == []


@pytest.mark.parametrize(
    "code",
    ["permission_denied", "scope_violation", "stale_lease", "source_unavailable", "policy_changed"],
)
async def test_same_tool_is_fenced_when_current_authority_is_unavailable(tmp_path, code):
    async def revoked(scope):
        raise DomainError(code, "Synthetic current authority rejected this action")

    runner = RecordingRunner()
    gateway = ToolGateway(revoked)
    register_compute_tools(gateway, runner)
    receipt = await gateway.invoke("shell_command", {"command": "ls"}, context(gateway, tmp_path))
    assert receipt.effect_state == EffectState.NOT_STARTED and receipt.error_code == code
    assert receipt.failure.origin == ToolFailureOrigin.AUTHORIZATION
    assert failure_disposition(receipt) == ToolFailureDisposition.FENCE
    assert not runner.calls


async def test_bad_path_proposal_repairs_without_treating_actor_as_revoked(tmp_path):
    runner = RecordingRunner()
    gateway = ToolGateway()
    register_compute_tools(gateway, runner)
    receipt = await gateway.invoke(
        "python_compute",
        {"code": "print(1)", "output_paths": ["/outside"]},
        context(gateway, tmp_path),
    )
    assert receipt.effect_state == EffectState.NO_EFFECT
    assert receipt.failure.origin == ToolFailureOrigin.ARGUMENTS
    assert failure_disposition(receipt) == ToolFailureDisposition.REPAIR
    assert not runner.calls


@pytest.mark.parametrize("code", ["source_unavailable", "stale_lease", "policy_changed"])
async def test_dedicated_authority_failure_cannot_be_overridden_by_handler_repair_hint(
    tmp_path, code
):
    async def bad_hint(arguments, context):
        raise ToolFailure(
            code,
            "Synthetic revoked authority",
            no_effect=True,
            origin=ToolFailureOrigin.EXECUTION,
            disposition=ToolFailureDisposition.REPAIR,
        )

    gateway = ToolGateway()
    gateway.register(
        ToolDefinition("synthetic", "Synthetic authority guard", ToolData, ToolData, bad_hint)
    )
    receipt = await gateway.invoke("synthetic", {}, context(gateway, tmp_path))
    assert failure_disposition(receipt) == ToolFailureDisposition.FENCE
    assert receipt.failure.origin == ToolFailureOrigin.AUTHORIZATION


async def test_unknown_effect_wins_over_any_handler_repair_hint(tmp_path):
    async def unknown(arguments, context):
        raise ToolFailure(
            "permission_denied",
            "Synthetic partial effect",
            origin=ToolFailureOrigin.CAPABILITY,
            disposition=ToolFailureDisposition.REPAIR,
        )

    gateway = ToolGateway()
    gateway.register(
        ToolDefinition("synthetic", "Synthetic uncertain effect", ToolData, ToolData, unknown)
    )
    receipt = await gateway.invoke("synthetic", {}, context(gateway, tmp_path))
    assert receipt.effect_state == EffectState.UNKNOWN
    assert failure_disposition(receipt) == ToolFailureDisposition.RECONCILE


async def test_output_dictionary_cannot_set_trusted_failure_disposition(tmp_path):
    class Output(ToolData):
        failure: dict[str, str]

    async def output(arguments, context):
        return Output(failure={"origin": "authorization", "disposition": "fence"})

    gateway = ToolGateway()
    gateway.register(
        ToolDefinition("synthetic", "Synthetic untrusted output", ToolData, Output, output)
    )
    receipt = await gateway.invoke("synthetic", {}, context(gateway, tmp_path))
    assert receipt.success and receipt.failure is None
    assert failure_disposition(receipt) is None


async def test_typed_unstarted_deferral_is_not_cached_even_with_new_error_code(tmp_path):
    calls = []

    async def capacity(arguments, context):
        calls.append(context.idempotency_key)
        if len(calls) == 1:
            raise ToolFailure(
                "capacity_wait",
                "Synthetic host capacity is unavailable",
                not_started=True,
                origin=ToolFailureOrigin.ADMISSION,
                disposition=ToolFailureDisposition.DEFER,
            )
        return ToolData()

    gateway = ToolGateway()
    gateway.register(
        ToolDefinition("synthetic", "Synthetic typed admission", ToolData, ToolData, capacity)
    )
    tool_context = context(gateway, tmp_path)
    first = await gateway.invoke("synthetic", {}, tool_context)
    assert first.effect_state == EffectState.NOT_STARTED
    assert failure_disposition(first) == ToolFailureDisposition.DEFER
    assert not gateway.export_ledger(tool_context.scope)
    second = await gateway.invoke("synthetic", {}, tool_context)
    assert second.success and len(calls) == 2


def test_legacy_unclassified_permission_receipt_stays_fenced():
    receipt = ToolReceipt(
        call_id="legacy",
        tool_name="synthetic",
        idempotency_key="legacy",
        success=False,
        effect_state=EffectState.NO_EFFECT,
        error_code="permission_denied",
    )
    assert receipt.failure is None
    assert failure_disposition(receipt) == ToolFailureDisposition.FENCE
