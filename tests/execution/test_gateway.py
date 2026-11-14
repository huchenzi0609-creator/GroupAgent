import asyncio
import os
from dataclasses import replace
from pathlib import Path

import pytest
from pydantic import Field

from groupagent.domain import DomainError, Role
from groupagent.tools import (
    EffectState,
    IdempotencyMode,
    PolicyDecision,
    ToolContext,
    ToolData,
    ToolDefinition,
    ToolGateway,
    ToolScope,
    register_workspace_tools,
)


def context(scope: ToolScope, key: str = "logical-write") -> ToolContext:
    return ToolContext(scope=scope, call_id=key, idempotency_key=key, cancelled=asyncio.Event())


async def test_write_replay_read_and_no_overwrite(gateway: ToolGateway, scope: ToolScope) -> None:
    args = {"path": "draft.txt", "text": "Synthetic draft"}
    first = await gateway.invoke("workspace_write_text", args, context(scope))
    second = await gateway.invoke("workspace_write_text", args, context(scope))
    assert first.success and second.success and second.replayed
    result = await gateway.invoke(
        "workspace_read_text", {"path": "draft.txt"}, context(scope, "read")
    )
    assert result.output and result.output["text"] == "Synthetic draft"
    conflict = await gateway.invoke(
        "workspace_write_text", {"path": "draft.txt", "text": "replacement"}, context(scope, "new")
    )
    assert not conflict.success and conflict.error_code == "version_conflict"
    assert (scope.workspace / "draft.txt").read_text() == "Synthetic draft"


async def test_same_idempotency_key_cannot_change_arguments(gateway, scope) -> None:
    await gateway.invoke("workspace_write_text", {"path": "a.txt", "text": "A"}, context(scope))
    conflict = await gateway.invoke(
        "workspace_write_text", {"path": "b.txt", "text": "B"}, context(scope)
    )
    assert conflict.error_code == "idempotency_conflict"
    assert not (scope.workspace / "b.txt").exists()


@pytest.mark.parametrize("path", ["../private.txt", "/etc/passwd", "nested/../../private", "a\\b"])
async def test_path_escape_is_denied(gateway, scope, path: str) -> None:
    receipt = await gateway.invoke(
        "workspace_write_text", {"path": path, "text": "never written"}, context(scope)
    )
    assert receipt.error_code == "scope_violation"
    assert receipt.effect_state == EffectState.NOT_STARTED


async def test_symlink_and_hardlink_reads_are_denied(gateway, scope, tmp_path: Path) -> None:
    outside = tmp_path.parent / f"outside-{tmp_path.name}.txt"
    outside.write_text("synthetic secret")
    (scope.workspace / "link.txt").symlink_to(outside)
    os.link(outside, scope.workspace / "hardlink.txt")
    try:
        for name in ("link.txt", "hardlink.txt"):
            receipt = await gateway.invoke(
                "workspace_read_text", {"path": name}, context(scope, name)
            )
            assert receipt.error_code == "scope_violation"
            assert not receipt.success
    finally:
        outside.unlink()


async def test_cross_group_work_and_model_authority_fields_are_denied(gateway, scope) -> None:
    for forged in (replace(scope, group_id="other"), replace(scope, work_id="other")):
        receipt = await gateway.invoke("workspace_read_text", {"path": "file"}, context(forged))
        assert receipt.error_code == "scope_violation"
    receipt = await gateway.invoke(
        "workspace_write_text",
        {"path": "file", "text": "x", "group_id": "other", "role": "owner"},
        context(scope),
    )
    assert receipt.error_code == "invalid_input"
    assert not (scope.workspace / "file").exists()


async def test_shell_and_non_allowlisted_calls_are_denied(gateway, scope) -> None:
    receipt = await gateway.invoke("bash", {"command": "echo forbidden"}, context(scope))
    assert receipt.error_code == "tool_not_registered"
    restrictive = replace(
        scope, work_id="read-work", allowed_tools=frozenset({"workspace_read_text"})
    )
    directory = scope.workspace.parent / f"read-{scope.workspace.name}"
    directory.mkdir()
    restrictive = replace(restrictive, workspace=directory)
    gateway.bind_scope(restrictive)
    receipt = await gateway.invoke(
        "workspace_write_text", {"path": "x", "text": "x"}, context(restrictive)
    )
    assert receipt.error_code == "permission_denied"


async def test_policy_is_rechecked_after_revocation(scope) -> None:
    active = True

    async def current_policy(grant: ToolScope) -> PolicyDecision:
        return PolicyDecision(
            actor=grant.actor.model_copy(update={"policy_revision": 2}),
            allowed_tools=grant.allowed_tools,
            active=active,
        )

    gateway = ToolGateway(policy_provider=current_policy)
    register_workspace_tools(gateway)
    gateway.bind_scope(scope)
    first = await gateway.invoke(
        "workspace_write_text", {"path": "allowed.txt", "text": "x"}, context(scope)
    )
    assert first.success
    active = False
    second = await gateway.invoke(
        "workspace_write_text", {"path": "denied.txt", "text": "x"}, context(scope, "later")
    )
    assert second.error_code == "permission_denied"
    assert not (scope.workspace / "denied.txt").exists()


def test_workspaces_cannot_overlap_between_works(gateway, scope) -> None:
    with pytest.raises(DomainError, match="overlap"):
        gateway.bind_scope(replace(scope, work_id="another-work"))


class EchoInput(ToolData):
    text: str = Field(max_length=128)


class EchoOutput(ToolData):
    text: str


async def test_quiescence_requires_handler_exit_not_just_cancel_request(scope):
    release, stopped = asyncio.Event(), asyncio.Event()

    async def ignores_cancel(args, _context):
        while not release.is_set():
            try:
                await asyncio.sleep(0.005)
            except asyncio.CancelledError:
                continue
        stopped.set()
        return EchoOutput(text=args.text)

    gateway = ToolGateway()
    gateway.register(
        ToolDefinition(
            name="noncooperative",
            description="Explicit synthetic fault",
            input_model=EchoInput,
            output_model=EchoOutput,
            handler=ignores_cancel,
            timeout_seconds=0.01,
        )
    )
    grant = replace(scope, allowed_tools=frozenset({"noncooperative"}))
    gateway.bind_scope(grant)
    try:
        result = await gateway.invoke("noncooperative", {"text": "synthetic"}, context(grant))
        assert result.effect_state.value == "unknown" and not gateway.is_quiescent(grant)
        with pytest.raises(DomainError) as error:
            await gateway.quiesce(grant, timeout_seconds=0.02)
        assert error.value.code == "work_busy" and not stopped.is_set()
        release.set()
        await asyncio.wait_for(stopped.wait(), 1)
        await gateway.quiesce(grant, timeout_seconds=0.1)
        assert gateway.is_quiescent(grant)
    finally:
        release.set()


async def test_tool_timeout_cancels_handler_and_marks_effect_unknown(scope) -> None:
    stopped = asyncio.Event()

    async def slow(args: EchoInput, ctx: ToolContext) -> EchoOutput:
        try:
            await asyncio.sleep(100)
            return EchoOutput(text=args.text)
        finally:
            stopped.set()

    gateway = ToolGateway()
    gateway.register(
        ToolDefinition(
            name="slow",
            description="Synthetic delayed local operation",
            input_model=EchoInput,
            output_model=EchoOutput,
            handler=slow,
            timeout_seconds=0.01,
        )
    )
    scope = replace(scope, allowed_tools=frozenset({"slow"}))
    gateway.bind_scope(scope)
    receipt = await gateway.invoke("slow", {"text": "x"}, context(scope))
    assert receipt.error_code == "tool_timeout"
    assert receipt.effect_state == EffectState.UNKNOWN
    assert stopped.is_set()


async def test_output_limits_and_output_path_checks(scope) -> None:
    async def oversized(args: EchoInput, ctx: ToolContext) -> EchoOutput:
        return EchoOutput(text="x" * 100)

    gateway = ToolGateway()
    gateway.register(
        ToolDefinition(
            name="oversized",
            description="Synthetic oversized read",
            input_model=EchoInput,
            output_model=EchoOutput,
            handler=oversized,
            max_output_bytes=10,
            idempotency_mode=IdempotencyMode.READ_ONLY,
        )
    )
    scope = replace(scope, allowed_tools=frozenset({"oversized"}))
    gateway.bind_scope(scope)
    receipt = await gateway.invoke("oversized", {"text": "x"}, context(scope))
    assert receipt.error_code == "output_too_large"
    assert receipt.effect_state == EffectState.NO_EFFECT

    class PathOutput(ToolData):
        path: str

    async def forged_path(args: EchoInput, ctx: ToolContext) -> PathOutput:
        return PathOutput(path="../outside.txt")

    paths = ToolGateway()
    paths.register(
        ToolDefinition(
            name="forged_path",
            description="Synthetic invalid output path",
            input_model=EchoInput,
            output_model=PathOutput,
            handler=forged_path,
            output_path_fields=("path",),
            idempotency_mode=IdempotencyMode.READ_ONLY,
        )
    )
    path_scope = replace(scope, allowed_tools=frozenset({"forged_path"}))
    paths.bind_scope(path_scope)
    rejected = await paths.invoke("forged_path", {"text": "x"}, context(path_scope))
    assert rejected.error_code == "scope_violation" and not rejected.success


async def test_observer_cannot_call_tools(gateway, scope) -> None:
    observer = scope.actor.model_copy(update={"role": Role.OBSERVER})
    isolated = ToolGateway()
    register_workspace_tools(isolated)
    scope = replace(scope, actor=observer)
    isolated.bind_scope(scope)
    receipt = await isolated.invoke("workspace_read_text", {"path": "x"}, context(scope))
    assert receipt.error_code == "permission_denied"
