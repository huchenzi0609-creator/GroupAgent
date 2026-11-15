"""Real scoped PostgreSQL + filesystem export tests; no Docker or providers."""

from __future__ import annotations

import asyncio
import hashlib
import os
from dataclasses import replace

import pytest

from groupagent.artifacts import ArtifactScope
from groupagent.memory import MemoryService
from groupagent.runtime.artifact_tools import register_export_tool
from groupagent.tools import EffectState, ToolContext, ToolGateway

from .test_toolkit_contract import create_document, prepare
from .test_toolkit_contract import toolkit_setup as toolkit_setup

pytestmark = pytest.mark.postgres


async def bridge(setup, *, max_bytes=30 * 1024 * 1024, max_workspace_bytes=512 * 1024 * 1024):
    run = await prepare(setup)
    generated = await create_document(run)
    gateway = ToolGateway()
    register_export_tool(
        gateway,
        setup.env.store,
        setup.env.registry,
        setup.env.artifacts,
        run.work,
        run.lease,
        setup.env.actor,
        max_bytes=max_bytes,
        max_workspace_bytes=max_workspace_bytes,
    )
    scope = replace(run.scope, allowed_tools=frozenset({"artifact_export"}))
    gateway.bind_scope(scope)
    run.export_gateway, run.export_scope = gateway, scope
    return run, generated.output


async def export(run, source, path="source.md", *, key="export"):
    return await run.export_gateway.invoke(
        "artifact_export",
        {"file_id": source["file_id"], "version_id": source["version_id"], "path": path},
        ToolContext(run.export_scope, key, key, asyncio.Event()),
    )


async def test_export_is_an_atomic_independent_copy_with_same_sha_replay(toolkit_setup):
    setup = toolkit_setup
    run, original = await bridge(setup)
    artifact_scope = ArtifactScope(setup.env.actor.group_id, run.work.id, setup.env.actor.member_id)
    before = setup.env.artifacts.get(artifact_scope, original["file_id"], original["version_id"])
    bytes_before = setup.env.artifacts.read(
        artifact_scope, original["file_id"], original["version_id"]
    )
    result = await export(run, original)
    assert result.success and result.output["state"] == "exported"
    target = run.workspace / result.output["path"]
    inode = target.stat().st_ino
    assert target.read_bytes() == bytes_before
    assert target.stat().st_nlink == 1 and target.stat().st_mode & 0o077 == 0
    assert result.output["sha256"] == hashlib.sha256(bytes_before).hexdigest()
    assert "file_id" not in result.output and "version_id" not in result.output
    assert str(setup.env.root) not in str(result.output)
    repeated = await export(run, original, key="new-request-same-bytes")
    assert repeated.success and repeated.output["state"] == "already_present"
    assert target.stat().st_ino == inode
    target.write_bytes(b"changed working copy")
    assert (
        setup.env.artifacts.read(artifact_scope, original["file_id"], original["version_id"])
        == bytes_before
    )
    assert (
        setup.env.artifacts.get(artifact_scope, original["file_id"], original["version_id"])
        == before
    )
    conflict = await export(run, original, key="conflicting-target")
    assert not conflict.success and conflict.error_code == "path_conflict"
    assert conflict.effect_state == EffectState.NO_EFFECT
    assert target.read_bytes() == b"changed working copy"


@pytest.mark.parametrize(
    "path",
    ["../escape.md", "/tmp/escape.md", "missing/source.md", "bad\\path.md", "source/./file.md"],
)
async def test_invalid_export_paths_never_write(toolkit_setup, path):
    run, original = await bridge(toolkit_setup)
    result = await export(run, original, path)
    assert not result.success and result.effect_state in {
        EffectState.NOT_STARTED,
        EffectState.NO_EFFECT,
    }
    assert not (run.workspace.parent / "escape.md").exists()
    assert not list(run.workspace.glob(".artifact-export-*"))


async def test_symlink_and_hardlink_targets_are_not_followed(toolkit_setup):
    setup = toolkit_setup
    run, original = await bridge(setup)
    outside = setup.env.root / "outside-sentinel"
    outside.write_bytes(b"untouched synthetic sentinel")
    (run.workspace / "link.md").symlink_to(outside)
    failed = await export(run, original, "link.md")
    assert not failed.success and outside.read_bytes() == b"untouched synthetic sentinel"
    (run.workspace / "link.md").unlink()
    os.link(outside, run.workspace / "hard.md")
    failed = await export(run, original, "hard.md", key="hardlink")
    assert not failed.success and outside.read_bytes() == b"untouched synthetic sentinel"


async def test_only_current_goal_admitted_version_can_be_exported(toolkit_setup):
    setup = toolkit_setup
    old, old_output = await bridge(setup)
    current, current_output = await bridge(setup)
    foreign = await export(current, old_output)
    assert not foreign.success and foreign.error_code == "scope_violation"
    mixed = await export(
        current,
        {"file_id": old_output["file_id"], "version_id": current_output["version_id"]},
        key="mismatched-pair",
    )
    assert not mixed.success and mixed.error_code == "scope_violation"
    with setup.env.store.transaction(setup.env.actor) as tx:
        tx.conn.execute(
            "DELETE FROM ga_work_outputs WHERE group_id=%s AND work_id=%s AND version_id=%s",
            (setup.env.actor.group_id, current.work.id, current_output["version_id"]),
        )
    absent = await export(current, current_output, key="not-admitted")
    assert not absent.success and absent.error_code == "scope_violation"
    assert not (current.workspace / "source.md").exists()


async def test_withdrawal_and_policy_change_are_rechecked_before_export(toolkit_setup):
    setup = toolkit_setup
    run, original = await bridge(setup)
    MemoryService(setup.env.store).forget(setup.env.actor, event_ids=[run.event.id])
    unavailable = await export(run, original)
    assert not unavailable.success and unavailable.error_code in {
        "stale_lease",
        "source_unavailable",
    }
    assert not (run.workspace / "source.md").exists()
    run, original = await bridge(setup)
    with setup.env.store.transaction(setup.env.actor) as tx:
        tx.conn.execute(
            "UPDATE ga_groups SET policy_revision=policy_revision+1 WHERE id=%s",
            (setup.env.actor.group_id,),
        )
    changed = await export(run, original)
    assert not changed.success and changed.error_code == "policy_changed"
    assert not (run.workspace / "source.md").exists()


async def test_file_and_workspace_limits_precede_publication(toolkit_setup):
    run, original = await bridge(toolkit_setup, max_bytes=1)
    too_large = await export(run, original)
    assert not too_large.success and too_large.error_code == "quota_exceeded"
    run, original = await bridge(toolkit_setup, max_workspace_bytes=1)
    full = await export(run, original)
    assert not full.success and full.error_code == "quota_exceeded"
    assert not (run.workspace / "source.md").exists()


async def test_source_change_after_publication_is_not_called_no_effect(toolkit_setup, monkeypatch):
    setup = toolkit_setup
    run, original = await bridge(setup)
    called = 0
    read = setup.env.artifacts.read

    def mutate_after_read(*args, **kwargs):
        nonlocal called
        value = read(*args, **kwargs)
        called += 1
        return value

    monkeypatch.setattr(setup.env.artifacts, "read", mutate_after_read)
    # The post-publication lease check is exercised by changing the durable lease
    # immediately after the atomic link, while preserving the actual exported bytes.
    link = os.link

    def link_then_revoke(*args, **kwargs):
        result = link(*args, **kwargs)
        with setup.env.store.transaction(setup.env.actor) as tx:
            tx.conn.execute(
                "UPDATE ga_work_orders SET fencing_token=fencing_token+1 WHERE group_id=%s AND id=%s",
                (setup.env.actor.group_id, run.work.id),
            )
        return result

    monkeypatch.setattr("groupagent.runtime.artifact_tools.os.link", link_then_revoke)
    result = await export(run, original)
    assert not result.success and result.effect_state == EffectState.UNKNOWN
    assert (run.workspace / "source.md").exists() and called == 1
