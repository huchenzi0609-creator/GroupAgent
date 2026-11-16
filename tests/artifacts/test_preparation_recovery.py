"""Actual DOCX edit bytes in one graph; optional Docker, scripted inference only."""

from __future__ import annotations

import asyncio
import io
import json
import os

import pytest
from docx import Document

from groupagent.artifacts import ArtifactScope
from groupagent.domain import DomainError
from groupagent.execution import (
    ExecutionRequest,
    LangGraphEngine,
    ModelReply,
    ModelToolCall,
    RunState,
    TokenUsage,
)
from groupagent.media import IsolatedMediaWorker
from groupagent.sandbox import DockerSandbox, SandboxLimits
from groupagent.tools import EffectState
from groupagent.tools.models import ToolFailureDisposition, ToolFailureOrigin

from .test_managed_images import runtime
from .test_native_locators import SyntheticNativeParserPort, budget_report


@pytest.mark.parametrize("isolated", [False, pytest.param(True, marks=pytest.mark.sandbox)])
async def test_mismatched_edit_is_observed_and_corrected_in_same_graph_with_immutable_versions(
    tmp_path, isolated
):
    if isolated:
        image = os.environ.get("GROUPAGENT_WORKER_IMAGE")
        if not image:
            pytest.skip("Requires immutable Docker worker; scripted inference, no provider calls")
        worker = IsolatedMediaWorker(
            DockerSandbox(
                image,
                docker_host=os.environ.get("GROUPAGENT_DOCKER_HOST"),
                limits=SandboxLimits(memory_mb=512, workspace_mb=128, temp_mb=128, pids=128),
            )
        )
    else:
        worker = SyntheticNativeParserPort(tmp_path)
    store, service, gateway, context = runtime(tmp_path, worker)
    try:
        scope = ArtifactScope(context.group_id, context.work_id, context.actor.member_id)
        original = await service.generate(context, "budget.docx", budget_report())
        before = store.read(scope, original.file_id, original.version_id)

        class ReadAndCorrectFixtureModel:
            name = "scripted-native-document-repair/offline"

            def __init__(self):
                self.requests = []
                self.revised = None

            async def complete(self, request):
                self.requests.append(request)
                step = len(self.requests)
                arguments = {"file_id": original.file_id, "version_id": original.version_id}
                if step in {1, 3}:
                    name = "document_edit"
                    arguments = {
                        "file_id": original.file_id,
                        "expected_version_id": original.version_id,
                        "operation": "text",
                        "target": "paragraph:0",
                        "expected_text": "Wrong expected text",
                        "replacement": "预算150元",
                    }
                    if step == 3:
                        read = json.loads(request.messages[-1].content)
                        unit = next(
                            unit
                            for unit in read["output"]["data"]["units"]
                            if "预算100元" in unit["text"]
                        )
                        assert unit["editable"]
                        arguments.update(target=unit["locator"], expected_text="预算100元")
                elif step in {2, 4}:
                    previous = json.loads(request.messages[-1].content)
                    name = "document_read"
                    if step == 2:
                        assert not previous["success"]
                        assert previous["error_code"] == "edit_target_mismatch"
                        assert previous["failure"] == {
                            "origin": "precondition",
                            "disposition": "repair",
                        }
                        assert store.get(scope, original.file_id).version_id == original.version_id
                    else:
                        assert previous["success"]
                        self.revised = previous["output"]["version_id"]
                        arguments["version_id"] = self.revised
                else:
                    read = json.loads(request.messages[-1].content)
                    assert any(
                        "预算150元" in unit["text"] for unit in read["output"]["data"]["units"]
                    )
                    return ModelReply(
                        text="Actual revised document is ready",
                        usage=TokenUsage(input_tokens=5, output_tokens=5),
                    )
                return ModelReply(
                    tool_calls=(
                        ModelToolCall(id=f"repair-{step}", name=name, arguments=arguments),
                    ),
                    finish_reason="tool_calls",
                    usage=TokenUsage(input_tokens=5, output_tokens=5),
                )

        model = ReadAndCorrectFixtureModel()
        engine = LangGraphEngine(model, gateway)
        result = await asyncio.wait_for(
            engine.wait(
                await engine.start(
                    ExecutionRequest(
                        work_id=context.work_id,
                        attempt_id="synthetic-repair-attempt",
                        actor=context.actor,
                        objective="Correct the document using its exact current text and verify the result",
                        workspace=context.workspace,
                        allowed_tools=gateway.registered_names,
                        required_tools=frozenset({"document_edit", "document_read"}),
                        trace_id=context.scope.trace_id,
                    )
                )
            ),
            timeout=90 if isolated else 5,
        )
        assert result.state == RunState.CANDIDATE and len(model.requests) == 5
        failed = result.receipts[0]
        assert not failed.success and failed.effect_state == EffectState.NO_EFFECT
        assert failed.failure.origin == ToolFailureOrigin.PRECONDITION
        assert failed.failure.disposition == ToolFailureDisposition.REPAIR
        assert "document_read/search" in failed.message
        assert model.revised != original.version_id
        after = store.read(scope, original.file_id, model.revised)
        assert store.get(scope, original.file_id).version_id == model.revised
        assert store.read(scope, original.file_id, original.version_id) == before
        assert "预算100元" in "\n".join(p.text for p in Document(io.BytesIO(before)).paragraphs)
        assert "预算150元" in "\n".join(p.text for p in Document(io.BytesIO(after)).paragraphs)
    finally:
        store.close()


async def test_same_error_after_revision_publication_remains_unknown(tmp_path, monkeypatch):
    store, service, gateway, context = runtime(tmp_path, SyntheticNativeParserPort(tmp_path))
    try:
        scope = ArtifactScope(context.group_id, context.work_id, context.actor.member_id)
        original = await service.generate(context, "budget.docx", budget_report())
        before = store.read(scope, original.file_id, original.version_id)
        read = await service.read(context, original.file_id, original.version_id)
        target = next(unit.locator for unit in read.units if "预算100元" in unit.text)
        publish = store.revise

        def publish_then_lose_receipt(*args, **kwargs):
            publish(*args, **kwargs)
            raise DomainError("edit_target_mismatch", "Synthetic lost publication receipt")

        monkeypatch.setattr(store, "revise", publish_then_lose_receipt)
        failed = await gateway.invoke(
            "document_edit",
            {
                "file_id": original.file_id,
                "expected_version_id": original.version_id,
                "operation": "text",
                "target": target,
                "expected_text": "预算100元",
                "replacement": "预算150元",
            },
            context,
        )
        assert failed.error_code == "edit_target_mismatch"
        assert failed.effect_state == EffectState.UNKNOWN
        assert failed.failure.disposition == ToolFailureDisposition.RECONCILE
        revised = store.get(scope, original.file_id)
        assert revised.version_id != original.version_id
        assert store.read(scope, original.file_id, original.version_id) == before
        assert "预算150元" in "\n".join(
            p.text for p in Document(io.BytesIO(store.read(scope, revised.file_id))).paragraphs
        )
    finally:
        store.close()
