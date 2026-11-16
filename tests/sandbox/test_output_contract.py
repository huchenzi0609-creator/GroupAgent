"""Worker draft failures must be repairable observations with a proved boundary."""

from __future__ import annotations

import asyncio
import base64
import hashlib

import pytest
from pydantic import BaseModel

from groupagent.domain import ActorContext, Role
from groupagent.resources import ResourceDeferred
from groupagent.sandbox import DockerSandbox, ProcessResult, ProcessSpec
from groupagent.sandbox.models import DraftPublicationUnknown, UnpublishedDraftError
from groupagent.sandbox.supervisor import collect_outputs
from groupagent.tools import (
    EffectState,
    ToolContext,
    ToolData,
    ToolDefinition,
    ToolGateway,
    ToolScope,
)
from groupagent.tools.compute import register_compute_tools
from groupagent.tools.models import ToolFailure


@pytest.fixture
def output_context(tmp_path):
    actor = ActorContext(group_id="synthetic-group", member_id="synthetic-member", role=Role.MEMBER)
    return ToolContext(
        ToolScope(
            actor.group_id,
            "synthetic-work",
            actor,
            tmp_path.resolve(),
            frozenset({"python_compute"}),
            "synthetic-trace",
        ),
        "synthetic-call",
        "synthetic-work:synthetic-call",
        asyncio.Event(),
    )


def worker_entry(name, data):
    return {
        "path": name,
        "data": base64.b64encode(data).decode(),
        "sha256": hashlib.sha256(data).hexdigest(),
        "size_bytes": len(data),
    }


def worker_result(entries):
    return {
        "exit_code": 0,
        "stdout": "synthetic output",
        "stderr": "",
        "timed_out": False,
        "cancelled": False,
        "output_limited": False,
        "outputs": entries,
    }


def test_missing_declared_output_rejects_whole_manifest_without_exception_text(tmp_path):
    (tmp_path / "first.txt").write_text("Synthetic valid draft")
    outputs, issues = collect_outputs(
        tmp_path,
        ["first.txt", "other-name.txt"],
        {"max_file_bytes": 100, "max_artifact_bytes": 1000},
    )
    assert outputs == []
    assert issues == [{"output_index": 1, "kind": "missing"}]
    assert "other-name" not in str(issues) and str(tmp_path) not in str(issues)


@pytest.mark.parametrize("kind", ["symlink", "directory", "oversized", "total_size"])
def test_generic_output_contract_rejects_unsafe_drafts(tmp_path, kind):
    target = tmp_path / "draft"
    if kind == "symlink":
        target.symlink_to("/etc/passwd")
    elif kind == "directory":
        target.mkdir()
    else:
        target.write_bytes(b"synthetic draft")
    outputs, issues = collect_outputs(
        tmp_path,
        ["draft"],
        {
            "max_file_bytes": 1 if kind == "oversized" else 100,
            "max_artifact_bytes": 1 if kind == "total_size" else 100,
        },
    )
    assert outputs == [] and len(issues) == 1
    assert issues[0]["kind"] == ("total_size_exceeded" if kind == "total_size" else "invalid_file")


def test_all_hashes_are_validated_before_any_workspace_publication(output_context):
    sandbox = DockerSandbox("sha256:" + "a" * 64)
    entries = [worker_entry("first.txt", b"first"), worker_entry("second.txt", b"second")]
    entries[1]["sha256"] = "b" * 64
    with pytest.raises(ValueError, match="hash differs"):
        sandbox._publish_outputs(
            ProcessSpec(program="python3", output_paths=("first.txt", "second.txt")),
            output_context,
            worker_result(entries),
        )
    assert list(output_context.workspace.iterdir()) == []


def test_publication_failure_rolls_back_only_this_call_and_preserves_existing_file(output_context):
    sandbox = DockerSandbox("sha256:" + "a" * 64)
    (output_context.workspace / "second.txt").write_text("Synthetic existing artifact")
    with pytest.raises(UnpublishedDraftError) as failed:
        sandbox._publish_outputs(
            ProcessSpec(program="python3", output_paths=("first.txt", "second.txt")),
            output_context,
            worker_result(
                [worker_entry("first.txt", b"new"), worker_entry("second.txt", b"other")]
            ),
        )
    assert failed.value.code == "output_publication_failed"
    assert not (output_context.workspace / "first.txt").exists()
    assert (output_context.workspace / "second.txt").read_text() == "Synthetic existing artifact"


def test_unverified_publication_rollback_remains_unknown(output_context, monkeypatch):
    sandbox = DockerSandbox("sha256:" + "a" * 64)
    (output_context.workspace / "second.txt").write_text("Synthetic existing artifact")

    def refuse_unlink(*args, **kwargs):
        raise OSError("Synthetic failed disk rollback")

    monkeypatch.setattr("groupagent.sandbox.docker.os.unlink", refuse_unlink)
    with pytest.raises(DraftPublicationUnknown) as failed:
        sandbox._publish_outputs(
            ProcessSpec(program="python3", output_paths=("first.txt", "second.txt")),
            output_context,
            worker_result(
                [worker_entry("first.txt", b"new"), worker_entry("second.txt", b"other")]
            ),
        )
    assert failed.value.code == "publication_unknown"
    assert (output_context.workspace / "first.txt").exists()


class ResultRunner:
    allowed_programs = frozenset({"python3", "ls", "cat", "wc", "head", "tail", "sort"})

    def __init__(self, result):
        self.result, self.calls = result, 0

    async def run(self, spec, context):
        self.calls += 1
        if isinstance(self.result, Exception):
            raise self.result
        return self.result


@pytest.mark.parametrize(
    "failure", ["output_contract_failed", "worker_failed", "timeout", "process"]
)
async def test_compute_returns_sanitized_unpublished_observation_and_caches_exact_attempt(
    output_context, failure
):
    runner = ResultRunner(
        ProcessResult(
            image="sha256:" + "a" * 64,
            exit_code=0 if failure != "process" else 1,
            stdout="Synthetic output OPENAI_API_KEY=sk-synthetic-secret-key-value",
            stderr="Synthetic script diagnostic",
            error_code=failure if failure in {"output_contract_failed", "worker_failed"} else None,
            output_issues=({"output_index": 0, "kind": "missing"},)
            if failure == "output_contract_failed"
            else (),
            timed_out=failure == "timeout",
            elapsed_seconds=0.1,
        )
    )
    gateway = ToolGateway()
    register_compute_tools(gateway, runner)
    gateway.bind_scope(output_context.scope)
    arguments = {"code": "print('synthetic')", "output_paths": ["result.txt"]}
    receipt = await gateway.invoke("python_compute", arguments, output_context)
    assert not receipt.success and receipt.effect_state == EffectState.NO_EFFECT
    assert receipt.output["publication_state"] == "unpublished"
    assert receipt.output["stderr"] == "Synthetic script diagnostic"
    assert "sk-synthetic" not in receipt.output["stdout"]
    if failure == "output_contract_failed":
        assert receipt.error_code == "output_contract_failed"
        assert receipt.output["output_issues"] == [{"output_index": 0, "kind": "missing"}]
    replay = await gateway.invoke("python_compute", arguments, output_context)
    assert replay.replayed and runner.calls == 1
    assert list(output_context.workspace.iterdir()) == []


async def test_uncertain_publication_never_becomes_no_effect(output_context):
    gateway = ToolGateway()
    register_compute_tools(
        gateway,
        ResultRunner(DraftPublicationUnknown("publication_unknown", "Synthetic failed rollback")),
    )
    gateway.bind_scope(output_context.scope)
    receipt = await gateway.invoke("python_compute", {"code": "print(1)"}, output_context)
    assert (
        receipt.error_code == "publication_unknown" and receipt.effect_state == EffectState.UNKNOWN
    )


async def test_unstarted_runner_admission_yields_and_can_resume_exact_call(output_context):
    runner = ResultRunner(ResourceDeferred())
    gateway = ToolGateway()
    register_compute_tools(gateway, runner)
    gateway.bind_scope(output_context.scope)
    arguments = {"code": "print('synthetic')"}
    denied = await gateway.invoke("python_compute", arguments, output_context)
    assert (
        denied.error_code == "resource_deferred" and denied.effect_state == EffectState.NOT_STARTED
    )
    assert gateway.export_ledger(output_context.scope) == ()
    runner.result = ProcessResult(
        image="sha256:" + "a" * 64, exit_code=0, stdout="Synthetic resumed", elapsed_seconds=0
    )
    resumed = await gateway.invoke("python_compute", arguments, output_context)
    assert resumed.success and runner.calls == 2


async def test_malformed_failure_diagnostics_preserve_verified_no_effect(output_context):
    class Input(ToolData):
        pass

    class Output(ToolData):
        count: int

    class BadDiagnostic(BaseModel):
        secret: str

    async def fail(arguments, context):
        raise ToolFailure(
            "process_failed",
            "Synthetic controlled failure",
            no_effect=True,
            output=BadDiagnostic(secret="DO_NOT_EXPOSE"),
        )

    gateway = ToolGateway()
    gateway.register(
        ToolDefinition(
            name="python_compute",
            description="Synthetic tool",
            input_model=Input,
            output_model=Output,
            handler=fail,
        )
    )
    gateway.bind_scope(output_context.scope)
    receipt = await gateway.invoke("python_compute", {}, output_context)
    assert receipt.error_code == "invalid_output" and receipt.effect_state == EffectState.NO_EFFECT
    assert "DO_NOT_EXPOSE" not in receipt.model_dump_json()
