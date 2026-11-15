from __future__ import annotations

import asyncio
from dataclasses import replace

import pytest
from pydantic import ValidationError

from groupagent.artifacts import (
    ArtifactScope,
    ArtifactStore,
    IsolatedDocumentService,
    Report,
    Section,
)
from groupagent.artifacts.tools import DocumentCreateInput, register_document_tools
from groupagent.domain import ActorContext, DomainError, Role
from groupagent.tools import EffectState, ToolContext, ToolGateway, ToolScope
from groupagent.tools.models import ToolFailure


class SyntheticDocumentWorker:
    """No network/process. Actual format/render evidence lives in the Docker matrix."""

    def __init__(self, failure=None):
        self.calls = []
        self.failure = failure

    async def run(self, context, operation, inputs, **kwargs):
        self.calls.append(operation)
        if self.failure:
            raise self.failure
        assert operation == "document_generate"
        return {"format": "md"}, (b"# Synthetic report\n\nResult 87.\n",)


@pytest.fixture
def document_tools(tmp_path):
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    store = ArtifactStore(tmp_path / "store")
    worker = SyntheticDocumentWorker()
    service = IsolatedDocumentService(store, worker)
    gateway = ToolGateway()
    register_document_tools(gateway, service)
    scope = ToolScope(
        "synthetic-group",
        "synthetic-work",
        ActorContext(group_id="synthetic-group", member_id="author", role=Role.OWNER),
        workspace.resolve(),
        gateway.registered_names,
        "synthetic-trace",
    )
    gateway.bind_scope(scope)
    context = ToolContext(scope, "document-create", "document-create", asyncio.Event())
    yield store, worker, service, gateway, context
    store.close()


def report(**changes):
    return {
        "name": "report.md",
        "kind": "report",
        "title": "Synthetic report",
        "sections": [{"title": "Findings", "paragraphs": ["Result 87."]}],
        **changes,
    }


@pytest.mark.parametrize(
    "values",
    [
        report(name="报告标题"),
        report(name="../report.md"),
        report(name="/work/report.docx"),
        report(name="report.pptx"),
        report(
            name="report.docx",
            kind="deck",
            sections=[],
            slides=[{"title": "Slide", "bullets": ["Result"]}],
        ),
        report(name="report.mp4"),
        report(sections=[]),
    ],
)
async def test_invalid_create_arguments_never_start_worker_and_explain_fields(
    document_tools, values
):
    store, worker, _, gateway, context = document_tools
    result = await gateway.invoke("document_generate", values, context)
    assert not result.success and result.effect_state == EffectState.NOT_STARTED
    assert result.input_issues and any(issue.hint for issue in result.input_issues)
    assert not worker.calls and not store.list(
        ArtifactScope(context.group_id, context.work_id, context.actor.member_id)
    )
    assert "name" in str(result.input_issues) or "report" in str(result.input_issues)


def test_schema_accepts_only_consistent_report_and_deck_names():
    for extension in ("md", "docx", "pdf"):
        assert DocumentCreateInput.model_validate(report(name="报告." + extension)).kind == "report"
    deck = {
        "name": "课程.pptx",
        "kind": "deck",
        "title": "课程",
        "slides": [{"title": "Results", "bullets": ["87"]}],
    }
    assert DocumentCreateInput.model_validate(deck).kind == "deck"
    with pytest.raises(ValidationError):
        DocumentCreateInput.model_validate(
            report(
                sections=[
                    {
                        "title": "Findings",
                        "paragraphs": [],
                        "tables": [{"headers": ["a", "b"], "rows": [["only one"]]}],
                    }
                ]
            )
        )


async def test_invalid_name_can_be_repaired_without_unknown_pause(document_tools):
    store, worker, _, gateway, context = document_tools
    invalid = await gateway.invoke("document_generate", report(name="report"), context)
    assert invalid.effect_state == EffectState.NOT_STARTED
    fixed = await gateway.invoke("document_generate", report(), context)
    assert fixed.success and fixed.effect_state == EffectState.SUCCEEDED
    assert fixed.output["name"] == "report.md" and len(worker.calls) == 1
    assert (
        len(store.list(ArtifactScope(context.group_id, context.work_id, context.actor.member_id)))
        == 1
    )


async def test_isolated_parser_validation_failure_is_known_no_effect(document_tools):
    store, worker, _, gateway, context = document_tools
    worker.failure = DomainError("invalid_chart", "untrusted source description must not leak")
    result = await gateway.invoke("document_generate", report(), context)
    assert not result.success and result.effect_state == EffectState.NO_EFFECT
    assert result.error_code == "invalid_chart" and "chart.categories/series" in result.message
    assert "untrusted source" not in result.message
    assert (
        store.list(ArtifactScope(context.group_id, context.work_id, context.actor.member_id)) == ()
    )


async def test_unknown_worker_failure_is_not_blanket_marked_no_effect(document_tools):
    _, worker, _, gateway, context = document_tools
    worker.failure = DomainError("unknown_worker_side_effect", "synthetic unknown outcome")
    result = await gateway.invoke("document_generate", report(), context)
    assert result.effect_state == EffectState.UNKNOWN


async def test_failure_after_publication_keeps_unknown_effect(document_tools, monkeypatch):
    store, _, _, gateway, context = document_tools
    create = store.create

    def publish_then_fail(*args, **kwargs):
        create(*args, **kwargs)
        raise DomainError("unsupported_format", "synthetic persistence failure after publication")

    monkeypatch.setattr(store, "create", publish_then_fail)
    result = await gateway.invoke("document_generate", report(), context)
    assert result.effect_state == EffectState.UNKNOWN
    assert (
        len(store.list(ArtifactScope(context.group_id, context.work_id, context.actor.member_id)))
        == 1
    )


async def test_read_and_import_preflight_failures_have_safe_diagnostics(document_tools):
    _, worker, service, gateway, context = document_tools
    result = await gateway.invoke(
        "document_read", {"file_id": "0" * 32, "version_id": "1" * 32}, context
    )
    assert (
        result.error_code == "artifact_not_found" and result.effect_state == EffectState.NO_EFFECT
    )
    assert "file_id/version_id" in result.message and not worker.calls
    with pytest.raises(ToolFailure) as error:
        await service.import_document(context, "private-host/path.docx", b"unparsed")
    assert error.value.no_effect and error.value.code == "invalid_name"
    assert "private-host" not in str(error.value) and not worker.calls
    with pytest.raises(ToolFailure) as error:
        await service.generate(
            context, "wrong.pptx", Report("Report", (Section("Section", ("body",)),))
        )
    assert error.value.no_effect and not worker.calls


async def test_missing_edit_target_is_rejected_before_handler(document_tools):
    _, worker, _, gateway, context = document_tools
    context = replace(context, call_id="edit", idempotency_key="edit")
    result = await gateway.invoke(
        "document_edit",
        {"file_id": "0" * 32, "expected_version_id": "1" * 32, "operation": "text"},
        context,
    )
    assert result.effect_state == EffectState.NOT_STARTED
    assert result.input_issues and "target" in result.input_issues[0].hint
    assert not worker.calls


async def test_markdown_block_failure_has_correctable_specific_no_effect_hint(document_tools):
    _, worker, _, gateway, context = document_tools
    worker.failure = DomainError("unsupported_markdown_block", "Synthetic fenced JSON rejected")
    result = await gateway.invoke(
        "document_generate",
        report(sections=[{"title": "Example", "paragraphs": ['```json\n{"status":"ok"}\n```']}]),
        context,
    )
    assert not result.success and result.effect_state == EffectState.NO_EFFECT
    assert "workspace_write_text" in result.message and "artifact_from_workspace" in result.message


async def test_md_text_edit_rejects_before_worker_and_template_edit_is_supported(document_tools):
    store, worker, service, gateway, context = document_tools
    from groupagent.artifacts import ArtifactScope

    version = store.create(
        ArtifactScope(context.group_id, context.work_id, context.actor.member_id),
        "budget.md",
        b"# Budget\n\n## Source\n\nSynthetic data\n",
    )
    result = await gateway.invoke(
        "document_edit",
        {
            "file_id": version.file_id,
            "expected_version_id": version.version_id,
            "operation": "text",
            "target": "## Source",
            "expected_text": "## Source",
            "replacement": "Total 1000\n\n## Source",
        },
        context,
    )
    assert not result.success and result.effect_state == EffectState.NO_EFFECT
    assert result.error_code == "unsupported_text_target_format" and not worker.calls
    assert "template" in result.message and "section" in result.message
