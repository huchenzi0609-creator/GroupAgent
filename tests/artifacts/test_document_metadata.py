"""Actual synthetic Office bytes and read protocol; no isolation/provider claim."""

from __future__ import annotations

import asyncio
import io
import json
import zipfile
from dataclasses import asdict
from xml.etree import ElementTree

import pytest
from pptx import Presentation

from groupagent.artifacts import (
    ArtifactError,
    ArtifactScope,
    ArtifactStore,
    Deck,
    IsolatedDocumentService,
    Slide,
)
from groupagent.artifacts.formats import build_pptx
from groupagent.artifacts.models import DocumentRead
from groupagent.artifacts.tools import DocumentDataOutput, register_document_tools
from groupagent.artifacts.worker_operations import document_operation
from groupagent.domain import ActorContext, DomainError, Role
from groupagent.tools import ToolContext, ToolGateway, ToolScope


def pptx_bytes(title="Actual file theme"):
    return build_pptx(Deck(title, (Slide("Visible heading", ("Only the supplied visible body",)),)))


def worker_read(raw, format="pptx", **parameters):
    return document_operation(
        "document_read",
        {
            "parameters": {
                "format": format,
                "metadata": {"title": "Untrusted host input"},
                **parameters,
            },
            "outputs": (),
        },
        lambda request, index=0: raw,
        lambda name: None,
    )


def test_actual_metadata_title_is_distinct_from_visible_units_and_changes_with_actual_bytes():
    raw = pptx_bytes()
    original = worker_read(raw)
    assert original["metadata"] == {"title": "Actual file theme"}
    assert all("Actual file theme" not in unit["text"] for unit in original["units"])
    deck = Presentation(io.BytesIO(raw))
    deck.core_properties.title = "Changed file theme"
    buffer = io.BytesIO()
    deck.save(buffer)
    changed = worker_read(buffer.getvalue())
    assert changed["metadata"] == {"title": "Changed file theme"}
    assert changed["units"] == original["units"]
    assert (
        worker_read(buffer.getvalue(), start_unit=1, max_units=1)["metadata"] == changed["metadata"]
    )
    deck.core_properties.title = ""
    empty = io.BytesIO()
    deck.save(empty)
    assert worker_read(empty.getvalue())["metadata"] == {"title": ""}  # No filename/input fallback.


def test_markdown_and_old_read_result_have_independent_empty_metadata_defaults():
    assert worker_read(b"# Synthetic MD\n", "md")["metadata"] == {}
    one, two = DocumentRead("f", "v", "md", ()), DocumentRead("f", "v", "md", ())
    one.metadata["title"] = "local caller mutation"
    assert two.metadata == {}
    assert DocumentDataOutput.from_data(asdict(two)).data["metadata"] == {}


def test_actual_pptx_metadata_credentials_are_redacted_and_oversized_title_is_rejected():
    secret = "sk-" + "synthetic_only_" * 3
    result = worker_read(pptx_bytes("Theme " + secret))
    assert secret not in json.dumps(result)
    assert result["metadata"]["title"] == "Theme [凭据已隐藏]"
    raw = pptx_bytes()
    output = io.BytesIO()
    with (
        zipfile.ZipFile(io.BytesIO(raw)) as source,
        zipfile.ZipFile(output, "w", zipfile.ZIP_DEFLATED) as target,
    ):
        for name in source.namelist():
            data = source.read(name)
            if name == "docProps/core.xml":
                core = ElementTree.fromstring(data)
                core.find("{http://purl.org/dc/elements/1.1/}title").text = "X" * 1001
                data = ElementTree.tostring(core)
            target.writestr(name, data)
    with pytest.raises(ArtifactError) as caught:
        worker_read(output.getvalue())
    assert caught.value.code == "document_limit" and "X" * 50 not in str(caught.value)


class NativeReadWorkerFixture:
    """Execute the real worker operation on synthetic bytes in-process only."""

    async def run(self, context, operation, inputs, *, parameters, outputs=()):
        assert operation == "document_read"
        return worker_read(inputs[0], **parameters), ()


@pytest.fixture
def read_runtime(tmp_path):
    with ArtifactStore(tmp_path / "store") as store:
        actor = ActorContext(group_id="g", member_id="m", role=Role.MEMBER)
        service = IsolatedDocumentService(store, NativeReadWorkerFixture())
        gateway = ToolGateway()
        register_document_tools(gateway, service)
        scope = ToolScope("g", "w", actor, tmp_path, gateway.registered_names, "read-trace")
        gateway.bind_scope(scope)
        yield store, service, gateway, ToolContext(scope, "read", "read", asyncio.Event())


async def test_isolated_read_and_model_tool_preserve_actual_worker_metadata_roundtrip(read_runtime):
    store, service, gateway, context = read_runtime
    scope = ArtifactScope("g", "w", "m")
    version = store.create(
        scope,
        "Misleading filename.pptx",
        pptx_bytes(),
        provenance={"metadata": {"title": "Forged host title"}},
    )
    direct = await service.read(context, version.file_id, version.version_id)
    assert direct.metadata == {"title": "Actual file theme"}
    receipt = await gateway.invoke(
        "document_read", {"file_id": version.file_id, "version_id": version.version_id}, context
    )
    assert receipt.success
    wire = DocumentDataOutput.model_validate_json(json.dumps(receipt.output))
    assert wire.data["metadata"] == direct.metadata and wire.untrusted_content
    assert all("Actual file theme" not in unit["text"] for unit in wire.data["units"])


async def test_runtime_host_does_not_parse_office_and_legacy_protocol_defaults_empty(read_runtime):
    store, service, _, context = read_runtime
    version = store.create(ArtifactScope("g", "w", "m"), "synthetic.pptx", b"not an Office package")

    class ProtocolOnlyFixture:
        result = {"units": [], "metadata": {"title": "Worker supplied result"}}

        async def run(self, *args, **kwargs):
            return self.result, ()

    worker = ProtocolOnlyFixture()
    service.worker = worker
    assert (await service.read(context, version.file_id)).metadata == {
        "title": "Worker supplied result"
    }
    worker.result = {"units": []}
    assert (await service.read(context, version.file_id)).metadata == {}
    worker.result = {"units": [], "metadata": {"title": 42}}
    with pytest.raises(DomainError, match="invalid metadata"):
        await service.read(context, version.file_id)
