from __future__ import annotations

import asyncio
import os
from dataclasses import replace
from pathlib import Path
from uuid import uuid4

import pytest

from groupagent.domain import ActorContext, DomainError, Role
from groupagent.resources import (
    MemorySnapshot,
    ResourceDeferred,
    ResourceGovernor,
    ResourcePolicy,
    linux_memory_snapshot,
)
from groupagent.sandbox import DockerSandbox, ProcessResult, ProcessSpec, SandboxLimits
from groupagent.tools import ToolContext, ToolData, ToolDefinition, ToolGateway, ToolScope

MIB = 1024 * 1024


def governor(*, available: int = 2048) -> ResourceGovernor:
    return ResourceGovernor(
        ResourcePolicy(enabled=True, reserve_memory_mb=192, transfer_memory_mb=64),
        sampler=lambda: MemorySnapshot(available * MIB),
    )


def context(tmp_path: Path, key: str = "one") -> ToolContext:
    actor = ActorContext(group_id="synthetic-group", member_id="synthetic-member", role=Role.MEMBER)
    return ToolContext(
        ToolScope(
            actor.group_id, "synthetic-work", actor, tmp_path.resolve(), frozenset(), "trace"
        ),
        key,
        key,
        asyncio.Event(),
    )


def counters(directory: Path, *, maximum: str, current: str) -> None:
    directory.mkdir(parents=True, exist_ok=True)
    (directory / "memory.max").write_text(maximum)
    (directory / "memory.current").write_text(current)


def test_host_available_and_tightest_cgroup_ancestor_both_apply(tmp_path):
    meminfo, membership, mount = tmp_path / "meminfo", tmp_path / "cgroup", tmp_path / "mount"
    meminfo.write_text("MemTotal: 2097152 kB\nMemAvailable: 819200 kB\n")
    membership.write_text("0::/pot/worker\n")
    counters(mount, maximum="max", current=str(1000 * MIB))
    counters(mount / "pot", maximum=str(1024 * MIB), current=str(624 * MIB))
    counters(mount / "pot" / "worker", maximum=str(768 * MIB), current=str(16 * MIB))
    sample = linux_memory_snapshot(meminfo=meminfo, process_cgroup=membership, cgroup_root=mount)
    assert sample.host_available_bytes == 800 * MIB
    assert sample.cgroup_headroom_bytes == 400 * MIB
    assert sample.available_bytes == 400 * MIB
    admission = ResourceGovernor(
        ResourcePolicy(enabled=True, reserve_memory_mb=128, transfer_memory_mb=64),
        sampler=lambda: sample,
    )
    assert not admission.ready(256)  # 256 + 128 + 64 exceeds the parent headroom.
    (mount / "pot" / "memory.current").write_text(str(500 * MIB))
    sample = linux_memory_snapshot(meminfo=meminfo, process_cgroup=membership, cgroup_root=mount)
    assert admission.ready(256)
    # No separate subtraction of a co-resident application's current RSS.
    assert sample.available_bytes == 524 * MIB


def test_v1_and_unreadable_memory_information(tmp_path):
    meminfo, membership, mount = tmp_path / "meminfo", tmp_path / "cgroup", tmp_path / "mount"
    meminfo.write_text("MemAvailable: 1048576 kB\n")
    membership.write_text("5:memory:/pot\n")
    memory = mount / "memory" / "pot"
    memory.mkdir(parents=True)
    (memory / "memory.limit_in_bytes").write_text(str(768 * MIB))
    (memory / "memory.usage_in_bytes").write_text(str(256 * MIB))
    sample = linux_memory_snapshot(meminfo=meminfo, process_cgroup=membership, cgroup_root=mount)
    assert sample.available_bytes == 512 * MIB
    (memory / "memory.usage_in_bytes").write_text("malformed")
    assert not linux_memory_snapshot(
        meminfo=meminfo, process_cgroup=membership, cgroup_root=mount
    ).readable
    meminfo.unlink()
    assert not linux_memory_snapshot(
        meminfo=meminfo, process_cgroup=membership, cgroup_root=mount
    ).readable


def test_disabled_default_and_full_cap_admission():
    disabled = ResourceGovernor(sampler=lambda: pytest.fail("Disabled policy sampled the host"))
    assert disabled.ready(1024)
    first, second = disabled.reserve(1024), disabled.reserve(1024)
    first.release()
    second.release()
    assert not disabled.busy and not disabled.poisoned
    admission = governor(available=768)
    assert admission.ready(512)
    reservation = admission.reserve(512)
    assert admission.busy and not admission.ready(128)
    with pytest.raises(ResourceDeferred):
        admission.reserve(128)
    reservation.release()
    assert admission.ready(512)
    assert not governor(available=767).ready(512)
    unknown = ResourceGovernor(ResourcePolicy(enabled=True), sampler=lambda: MemorySnapshot(None))
    assert not unknown.ready(128)


class FakeSandbox(DockerSandbox):
    """Exercise real admission/tracking/removal with no daemon or subprocess."""

    def __init__(self, admission, counts, *, entered=None, finish=None, fail_remove=False):
        super().__init__(
            "sha256:" + "a" * 64,
            docker_path="synthetic-unused-docker",
            limits=SandboxLimits(memory_mb=512),
            resource_governor=admission,
        )
        self.counts, self.entered, self.finish = counts, entered, finish
        self.fail_remove = fail_remove

    async def preflight(self):
        return {}

    async def _control(self, home, *args, **kwargs):
        return (0, b"still-present\n") if self.fail_remove and args[0] == "ps" else (0, b"")

    async def _run(self, spec, ctx):
        name = "groupagent-" + uuid4().hex
        self.track_container(name, ctx.scope)
        self.counts["active"] += 1
        self.counts["peak"] = max(self.counts["peak"], self.counts["active"])
        self.counts["order"].append(ctx.call_id)
        try:
            if self.entered:
                self.entered.set()
            if self.finish:
                await self.finish.wait()
            else:
                await asyncio.sleep(0)
            return ProcessResult(image=self.image, exit_code=0, elapsed_seconds=0)
        finally:
            await self.remove_container(ctx.workspace, name)
            self.counts["active"] -= 1


async def test_independent_sandboxes_share_capacity_and_idle_closes_it(tmp_path):
    admission, counts = governor(), {"active": 0, "peak": 0, "order": []}
    entered, finish = asyncio.Event(), asyncio.Event()
    first = FakeSandbox(admission, counts, entered=entered, finish=finish)
    second = FakeSandbox(admission, counts)
    ctx = context(tmp_path)
    running = asyncio.create_task(first.run(ProcessSpec(program="python3"), ctx))
    await entered.wait()
    with pytest.raises(ResourceDeferred):
        await second.run(ProcessSpec(program="python3"), replace(ctx, call_id="two"))
    assert counts["peak"] == 1 and admission.busy
    finish.set()
    assert (await running).succeeded
    assert not admission.busy and not DockerSandbox.scope_active(ctx.group_id, ctx.work_id)
    assert (await second.run(ProcessSpec(program="python3"), ctx)).succeeded
    assert not admission.busy and not admission.poisoned


async def test_inherited_reservation_still_serializes_actual_children(tmp_path):
    admission, counts = governor(), {"active": 0, "peak": 0, "order": []}
    first, second = FakeSandbox(admission, counts), FakeSandbox(admission, counts)
    reservation = admission.reserve(512)
    ctx = context(tmp_path)
    try:
        with reservation.activate():
            results = await asyncio.gather(
                first.run(ProcessSpec(program="python3"), ctx),
                second.run(ProcessSpec(program="python3"), replace(ctx, call_id="two")),
            )
        assert all(result.succeeded for result in results)
        assert counts == {"active": 0, "peak": 1, "order": ["one", "two"]}
        assert admission.busy  # The outer handler still owns its reservation.
    finally:
        reservation.release()
    assert not admission.busy and not admission.poisoned


async def test_gateway_reservation_reaches_concurrent_nested_sandboxes(tmp_path):
    admission, counts = governor(), {"active": 0, "peak": 0, "order": []}
    first, second = FakeSandbox(admission, counts), FakeSandbox(admission, counts)
    gateway = ToolGateway()

    async def handler(arguments, ctx):
        assert admission.current(512) is not None
        await asyncio.gather(
            first.run(ProcessSpec(program="python3"), ctx),
            second.run(ProcessSpec(program="python3"), replace(ctx, call_id="two")),
        )
        return ToolData()

    gateway.register(ToolDefinition("nested", "nested", ToolData, ToolData, handler))
    gateway.bind_resource_admission(admission, memory_mb=512, tool_names=frozenset({"nested"}))
    ctx = context(tmp_path)
    ctx = replace(ctx, scope=replace(ctx.scope, allowed_tools=frozenset({"nested"})))
    gateway.bind_scope(ctx.scope)
    result = await gateway.invoke("nested", {}, ctx)
    assert result.success and counts["peak"] == 1 and counts["order"] == ["one", "two"]
    assert not admission.busy and not admission.poisoned


async def test_cancelling_child_waiter_never_releases_another_child(tmp_path):
    admission, counts = governor(), {"active": 0, "peak": 0, "order": []}
    entered, finish = asyncio.Event(), asyncio.Event()
    first = FakeSandbox(admission, counts, entered=entered, finish=finish)
    second = FakeSandbox(admission, counts)
    ctx, waiting = context(tmp_path), context(tmp_path, "two")
    reservation = admission.reserve(512)
    try:
        with reservation.activate():
            running = asyncio.create_task(first.run(ProcessSpec(program="python3"), ctx))
            await entered.wait()
            waiter = asyncio.create_task(second.run(ProcessSpec(program="python3"), waiting))
            await asyncio.sleep(0)
            waiting.cancelled.set()
            with pytest.raises(DomainError) as error:
                await waiter
            assert error.value.code == "cancelled"
            assert counts["active"] == 1 and admission.busy and not admission.poisoned
            finish.set()
            await running
    finally:
        finish.set()
        reservation.release()
    assert not admission.busy and not admission.poisoned and counts["order"] == ["one"]


async def test_unconfirmed_removal_poisons_capacity(tmp_path):
    admission, counts = governor(), {"active": 0, "peak": 0, "order": []}
    sandbox = FakeSandbox(admission, counts, fail_remove=True)
    ctx = context(tmp_path)
    try:
        with pytest.raises(DomainError) as error:
            await sandbox.run(ProcessSpec(program="python3"), ctx)
        assert error.value.code == "work_busy"
        assert admission.poisoned and not admission.ready(128)
        assert DockerSandbox.scope_active(ctx.group_id, ctx.work_id)
        with pytest.raises(ResourceDeferred):
            await FakeSandbox(admission, counts).run(ProcessSpec(program="python3"), ctx)
    finally:
        for name, (_, group, work) in tuple(DockerSandbox._managed.items()):
            if (group, work) == (ctx.group_id, ctx.work_id):
                DockerSandbox._managed.pop(name)
                DockerSandbox._container_reservations.pop(name, None)


async def test_cancel_during_subprocess_creation_confirms_absence(tmp_path, monkeypatch):
    admission = governor()
    sandbox = DockerSandbox(
        "sha256:" + "a" * 64,
        docker_path="synthetic-unused-docker",
        limits=SandboxLimits(memory_mb=512),
        resource_governor=admission,
    )
    creating, removed = asyncio.Event(), []

    async def preflight():
        return {}

    async def launch(*args, **kwargs):
        creating.set()
        await asyncio.Event().wait()

    async def control(home, *args, **kwargs):
        removed.append(args[0])
        return 0, b""

    monkeypatch.setattr(sandbox, "preflight", preflight)
    monkeypatch.setattr(sandbox, "_control", control)
    monkeypatch.setattr(asyncio, "create_subprocess_exec", launch)
    ctx = context(tmp_path)
    task = asyncio.create_task(sandbox.run(ProcessSpec(program="python3"), ctx))
    await creating.wait()
    assert admission.busy and DockerSandbox.scope_active(ctx.group_id, ctx.work_id)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert removed == ["rm", "ps"]
    assert not admission.busy and not admission.poisoned
    assert not DockerSandbox.scope_active(ctx.group_id, ctx.work_id)


def test_direct_launch_cannot_bypass_enabled_admission():
    sandbox = DockerSandbox(
        "sha256:" + "a" * 64, docker_path="unused", resource_governor=governor()
    )
    with pytest.raises(ResourceDeferred):
        sandbox._argv("groupagent-" + "b" * 32)


class InventorySandbox(DockerSandbox):
    def __init__(self, admission, containers, *, failure=None):
        super().__init__("sha256:" + "a" * 64, docker_path="unused", resource_governor=admission)
        self.containers, self.failure = dict(containers), failure
        self.removed = []
        self.inventories = 0

    async def _control(self, home, *args, **kwargs):
        if args[0] == "rm":
            name = args[-1]
            self.removed.append(name)
            if self.failure != "remove":
                self.containers.pop(name, None)
            return 0, b""
        if "label=org.groupagent.managed=1" in args:
            self.inventories += 1
            if self.failure == "inventory" or (
                self.failure == "final_inventory" and self.inventories > 1
            ):
                return 1, b""
            return 0, "".join(
                name + "\t" + (group or "") + "\n" for name, group in self.containers.items()
            ).encode()
        names = [name for name in self.containers if "name=^/" + name + "$" in args]
        return 0, "\n".join(names).encode()


def test_scope_argv_labels_work_and_audio_group_without_raw_identity(tmp_path):
    sandbox = DockerSandbox("sha256:" + "a" * 64, docker_path="unused")
    ctx = context(tmp_path)
    for work_id in (ctx.work_id, "input:synthetic-audio-event"):
        scope = replace(ctx.scope, work_id=work_id)
        argv = sandbox._argv("groupagent-" + "a" * 32, scope=scope)
        assert "org.groupagent.managed=1" in argv
        assert "org.groupagent.group=" + sandbox.group_label(ctx.group_id) in argv
        assert "org.groupagent.scope=" + sandbox.scope_label(ctx.group_id, work_id) in argv
        assert all(ctx.group_id not in argument and work_id not in argument for argument in argv)


async def test_restart_barrier_removes_untracked_work_and_audio_but_preserves_other_group():
    admission = governor()
    group, foreign = (
        DockerSandbox.group_label("own-group"),
        DockerSandbox.group_label("other-group"),
    )
    work, audio, other = ("groupagent-" + letter * 32 for letter in "abc")
    sandbox = InventorySandbox(admission, {work: group, audio: group, other: foreign})
    # These daemon containers deliberately have no process-local _managed entry,
    # as after a crash/restart. The audio scope need not have any WorkOrder.
    assert all(name not in DockerSandbox._managed for name in (work, audio, other))
    assert await sandbox.reconcile_group("own-group") == 2
    assert set(sandbox.removed) == {work, audio} and sandbox.containers == {other: foreign}
    assert sandbox.inventories == 2 and not admission.busy and not admission.poisoned
    assert admission.ready(512)


@pytest.mark.parametrize("failure", ["inventory", "remove", "final_inventory"])
async def test_restart_barrier_failure_poisons_fresh_governor(failure):
    admission = governor()
    name = "groupagent-" + "a" * 32
    sandbox = InventorySandbox(
        admission, {name: DockerSandbox.group_label("own-group")}, failure=failure
    )
    with pytest.raises(DomainError) as error:
        await sandbox.reconcile_group("own-group")
    assert error.value.code == "resource_reconciliation_required" and error.value.retryable
    assert admission.poisoned and not admission.ready(512)


async def test_restart_barrier_retains_legacy_unlabelled_managed_workers():
    admission = governor()
    legacy = "groupagent-" + "a" * 32
    own = "groupagent-" + "b" * 32
    sandbox = InventorySandbox(
        admission, {legacy: None, own: DockerSandbox.group_label("own-group")}
    )
    with pytest.raises(DomainError) as error:
        await sandbox.reconcile_group("own-group")
    assert error.value.code == "resource_reconciliation_required" and admission.poisoned
    assert sandbox.removed == [] and set(sandbox.containers) == {legacy, own}


async def test_restart_barrier_refuses_to_overlap_an_admitted_handler():
    admission = governor()
    reservation = admission.reserve(512)
    sandbox = InventorySandbox(admission, {})
    try:
        with pytest.raises(DomainError) as error:
            await sandbox.reconcile_group("own-group")
        assert error.value.code == "resource_reconciliation_required"
        assert sandbox.inventories == 0 and admission.poisoned
    finally:
        reservation.release()


@pytest.mark.sandbox
@pytest.mark.skipif(
    not os.environ.get("GROUPAGENT_WORKER_IMAGE"), reason="Requires an existing pinned local worker"
)
async def test_real_restart_barrier_removes_idle_audio_orphan(tmp_path):
    image = os.environ["GROUPAGENT_WORKER_IMAGE"]
    endpoint = os.environ.get("GROUPAGENT_DOCKER_HOST")
    group, name = "synthetic-restart-" + uuid4().hex, "groupagent-" + uuid4().hex
    actor = ActorContext(group_id=group, member_id="synthetic-member", role=Role.MEMBER)
    scope = ToolScope(
        group, "input:synthetic-orphan", actor, tmp_path.resolve(), frozenset(), "trace"
    )
    launcher = DockerSandbox(
        image,
        docker_host=endpoint,
        limits=SandboxLimits(memory_mb=128, workspace_mb=16, temp_mb=16),
    )
    # Detached stdin remains open, leaving only the trusted stdlib supervisor
    # waiting for a request. No parser, provider, browser or compute job starts.
    argv = launcher._argv(name, scope=scope)
    command = argv[argv.index("run") :]
    command.insert(-1, "--detach")
    admission = governor()
    restarted = DockerSandbox(image, docker_host=endpoint, resource_governor=admission)
    try:
        code, _ = await launcher._control(tmp_path, *command)
        assert code == 0 and name not in DockerSandbox._managed
        code, raw = await launcher._control(
            tmp_path, "ps", "--all", "--filter", "name=^/" + name + "$", "--format", "{{.Names}}"
        )
        assert code == 0 and raw.decode().strip() == name
        assert await restarted.reconcile_group(group) == 1
        code, raw = await launcher._control(
            tmp_path, "ps", "--all", "--filter", "name=^/" + name + "$", "--format", "{{.Names}}"
        )
        assert code == 0 and not raw.strip()
        assert not admission.poisoned and admission.ready(512)
    finally:
        await launcher.remove_container(tmp_path, name)
