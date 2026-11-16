"""Actual synthetic Office bytes and read protocol; no isolation/provider claim."""

from __future__ import annotations

import asyncio
import io
import json
import zipfile
from dataclasses import asdict
from xml.etree import ElementTree

import pytest
from PIL import Image, ImageDraw
from pptx import Presentation

from groupagent.artifacts import (
    ArtifactError,
    ArtifactScope,
    ArtifactStore,
    Deck,
    ImageData,
    IsolatedDocumentService,
    Report,
    Section,
    Slide,
    TableData,
)
from groupagent.artifacts.formats import build_docx, build_pptx
from groupagent.artifacts.models import DocumentRead
from groupagent.artifacts.tools import DocumentDataOutput, register_document_tools
from groupagent.artifacts.worker_operations import document_operation
from groupagent.domain import ActorContext, DomainError, Role
from groupagent.tools import ToolContext, ToolGateway, ToolScope


def pptx_bytes(title="Actual file theme"):
    return build_pptx(Deck(title, (Slide("Visible heading", ("Only the supplied visible body",)),)))


def pptx_table_bytes():
    return build_pptx(
        Deck(
            "Actual file theme",
            (Slide("Measurements", (), TableData(("Name", "Value"), (("North", "7"),))),),
        )
    )


def worker_read(raw, format="pptx", **parameters):
    return document_operation(
        "document_read",
        {
            "parameters": {
                "format": format,
                "metadata": {"title": "Untrusted host input", "native_table_count": "999"},
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
    assert original["metadata"] == {"title": "Actual file theme", "native_table_count": "0"}
    assert all("Actual file theme" not in unit["text"] for unit in original["units"])
    deck = Presentation(io.BytesIO(raw))
    deck.core_properties.title = "Changed file theme"
    buffer = io.BytesIO()
    deck.save(buffer)
    changed = worker_read(buffer.getvalue())
    assert changed["metadata"] == {"title": "Changed file theme", "native_table_count": "0"}
    assert changed["units"] == original["units"]
    assert (
        worker_read(buffer.getvalue(), start_unit=1, max_units=1)["metadata"] == changed["metadata"]
    )
    deck.core_properties.title = ""
    empty = io.BytesIO()
    deck.save(empty)
    assert worker_read(empty.getvalue())["metadata"] == {"title": "", "native_table_count": "0"}


def test_actual_native_table_count_is_independent_of_aggregate_text_edit_locator_support():
    raw = pptx_table_bytes()
    result = worker_read(raw)
    assert result["metadata"] == {"title": "Actual file theme", "native_table_count": "1"}
    aggregate = next(
        unit for unit in result["units"] if "shape:groupagent.table" in unit["locator"]
    )
    assert aggregate["editable"] is False and "North" in aggregate["text"]
    deck = Presentation(io.BytesIO(raw))
    table = next(shape for shape in deck.slides[0].shapes if shape.has_table)
    table._element.getparent().remove(table._element)
    changed = io.BytesIO()
    deck.save(changed)
    assert worker_read(changed.getvalue())["metadata"] == {
        "title": "Actual file theme",
        "native_table_count": "0",
    }


@pytest.mark.parametrize("screenshot", [False, True])
def test_text_grid_or_screenshot_is_not_counted_as_a_native_table(screenshot):
    if screenshot:
        image = Image.new("RGB", (320, 120), "white")
        draw = ImageDraw.Draw(image)
        draw.rectangle((5, 5, 315, 115), outline="black")
        draw.line((5, 60, 315, 60), fill="black")
        draw.text((10, 10), "Name | Value", fill="black")
        draw.text((10, 70), "North | 7", fill="black")
        pixels = io.BytesIO()
        image.save(pixels, format="PNG")
        slide = Slide(
            "Looks like a table", (), image=ImageData(pixels.getvalue(), "Table screenshot")
        )
    else:
        slide = Slide(
            "Looks like a table", ("+-------+-------+", "| Name | Value |", "| North | 7 |")
        )
    raw = build_pptx(Deck("Image or text only", (slide,)))
    assert worker_read(raw)["metadata"]["native_table_count"] == "0"


def test_docx_native_table_count_comes_from_actual_body_tables():
    raw = build_docx(
        Report("Report", (Section("Data", (), (TableData(("Name", "Value"), (("North", "7"),)),)),))
    )
    assert worker_read(raw, "docx")["metadata"] == {"native_table_count": "1"}


@pytest.mark.parametrize("duplicate", [False, True])
def test_word_counts_embedded_pixels_and_distinguishes_duplicate_images(duplicate):
    def picture(color):
        output = io.BytesIO()
        Image.new("RGB", (160, 100), color).save(output, format="PNG")
        return ImageData(output.getvalue(), "Synthetic test picture")

    first = picture("red")
    second = first if duplicate else picture("blue")
    raw = build_docx(
        Report(
            "Two images",
            (Section("Pictures", ("Synthetic images only.",), images=(first, second)),),
        )
    )
    metadata = worker_read(raw, "docx")["metadata"]
    assert metadata["embedded_image_count"] == "2"
    assert metadata["distinct_embedded_image_count"] == ("1" if duplicate else "2")


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
        pptx_table_bytes(),
        provenance={"metadata": {"title": "Forged host title"}},
    )
    direct = await service.read(context, version.file_id, version.version_id)
    assert direct.metadata == {"title": "Actual file theme", "native_table_count": "1"}
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


@pytest.mark.parametrize("bad_count", [42, "-1", "1.0", "01", " 1 ", "NaN", "1\n2"])
async def test_count_protocol_requires_a_canonical_nonnegative_integer_string(
    read_runtime, bad_count
):
    store, service, _, context = read_runtime
    version = store.create(ArtifactScope("g", "w", "m"), "synthetic.pptx", b"protocol fixture")

    class InvalidCountFixture:
        async def run(self, *args, **kwargs):
            return {
                "units": [],
                "metadata": {"title": "Title", "native_table_count": bad_count},
            }, ()

    service.worker = InvalidCountFixture()
    with pytest.raises(DomainError, match="invalid metadata"):
        await service.read(context, version.file_id)
