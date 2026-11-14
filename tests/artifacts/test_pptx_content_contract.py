"""Actual native PPTX bytes; no renderer, process, database or provider calls."""

from __future__ import annotations

import asyncio
import copy
import io

import pytest
from pptx import Presentation

from groupagent.artifacts import (
    ArtifactScope,
    ArtifactStore,
    Deck,
    IsolatedDocumentService,
    Slide,
    TableData,
)
from groupagent.artifacts.formats import DECK_MARKER, build_pptx, edit_deck_slide
from groupagent.artifacts.tools import TableInput, register_document_tools
from groupagent.artifacts.worker_operations import unpack_document
from groupagent.domain import ActorContext, Role
from groupagent.tools import ToolContext, ToolGateway, ToolScope


def visible_text(slide):
    return [
        paragraph.text
        for shape in slide.shapes
        if shape.has_text_frame
        for paragraph in shape.text_frame.paragraphs
        if paragraph.text
    ]


def test_native_pptx_uses_only_supplied_visible_slide_text_without_default_footer():
    deck = Deck(
        "Metadata theme",
        (
            Slide("Purpose", ("Prepare a verified result",)),
            Slide("People", ("Reader: collect facts", "Reviewer: check values")),
            Slide("Next action", ("Collect facts",)),
        ),
    )
    raw = build_pptx(deck)
    parsed = Presentation(io.BytesIO(raw))
    assert len(parsed.slides) == len(deck.slides) == 3
    assert parsed.core_properties.title == deck.title
    assert parsed.core_properties.keywords == DECK_MARKER
    for actual, requested in zip(parsed.slides, deck.slides, strict=True):
        assert visible_text(actual) == [requested.title, *requested.bullets]
        assert not any(shape.name == "groupagent.footer" for shape in actual.shapes)
    # The native title/body locator contract still supports normal editing.
    revised = Presentation(
        io.BytesIO(edit_deck_slide(raw, 1, title="People revised", bullets=None))
    )
    assert revised.slides[0]._element.xml == parsed.slides[0]._element.xml
    assert visible_text(revised.slides[1]) == ["People revised", *deck.slides[1].bullets]


@pytest.mark.parametrize("horizontal", [False, True])
def test_native_table_preserves_declared_pair_rows_and_explicit_horizontal_layout(horizontal):
    declared = TableInput(
        headers=("North", "South") if horizontal else ("Name", "Value"),
        rows=(("7", "9"),) if horizontal else (("North", "7"), ("South", "9")),
    )
    deck = Deck(
        "Metadata only", (Slide("Measurements", (), TableData(declared.headers, declared.rows)),)
    )
    parsed = Presentation(io.BytesIO(build_pptx(deck)))
    tables = [shape.table for shape in parsed.slides[0].shapes if shape.has_table]
    assert len(tables) == 1
    assert [[cell.text for cell in row.cells] for row in tables[0].rows] == [
        list(declared.headers),
        *map(list, declared.rows),
    ]
    assert visible_text(parsed.slides[0]) == ["Measurements"]


@pytest.fixture
def model_table_bytes(tmp_path):
    class NativePptxFixtureWorker:
        """Run the real byte builder in-process, never Docker or external tools."""

        async def run(self, context, operation, inputs, *, parameters, outputs):
            assert operation == "document_generate" and parameters["format"] == "pptx"
            deck = unpack_document(parameters["document"], lambda index: inputs[index])
            return {"format": "pptx"}, (build_pptx(deck),)

    store = ArtifactStore(tmp_path / "artifacts")
    service = IsolatedDocumentService(store, NativePptxFixtureWorker())
    gateway = ToolGateway()
    register_document_tools(gateway, service)
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    actor = ActorContext(group_id="synthetic-group", member_id="member", role=Role.MEMBER)
    scope = ToolScope(
        actor.group_id,
        "synthetic-work",
        actor,
        workspace,
        gateway.registered_names,
        "synthetic-trace",
    )
    gateway.bind_scope(scope)

    async def generate(table):
        original = copy.deepcopy(table)
        result = await gateway.invoke(
            "document_generate",
            {
                "name": "measurements.pptx",
                "kind": "deck",
                "title": "Synthetic measurements",
                "slides": [{"title": "Measurements", "bullets": [], "table": table}],
            },
            ToolContext(scope, "create", "create", asyncio.Event()),
        )
        assert result.success, result
        assert table == original
        raw = store.read(
            ArtifactScope(actor.group_id, scope.work_id, actor.member_id),
            result.output["file_id"],
            result.output["version_id"],
        )
        deck = Presentation(io.BytesIO(raw))
        tables = [shape.table for shape in deck.slides[0].shapes if shape.has_table]
        assert len(tables) == 1
        return [[cell.text for cell in row.cells] for row in tables[0].rows]

    yield generate
    store.close()


@pytest.mark.parametrize("layout", [None, "records", "columns"])
async def test_model_wide_scalar_args_have_records_default_and_explicit_columns_optout(
    model_table_bytes, layout
):
    table = {"headers": ["Alpha", "Beta", "Gamma"], "rows": [["12", "24", "36"]]}
    if layout is not None:
        table["layout"] = layout
    actual = await model_table_bytes(table)
    assert actual == (
        [["Alpha", "Beta", "Gamma"], ["12", "24", "36"]]
        if layout == "columns"
        else [["项目", "数值"], ["Alpha", "12"], ["Beta", "24"], ["Gamma", "36"]]
    )


async def test_records_normalization_keeps_original_numeric_strings_precision_and_order(
    model_table_bytes,
):
    actual = await model_table_bytes(
        {"headers": ["First", "Second", "Third"], "rows": [["001.2300", " -0.0400 ", "+6.02e+23"]]}
    )
    assert actual == [
        ["项目", "数值"],
        ["First", "001.2300"],
        ["Second", " -0.0400 "],
        ["Third", "+6.02e+23"],
    ]


@pytest.mark.parametrize(
    "headers,rows",
    [
        (["Name", "Value"], [["North", "7"]]),
        (["North", "South"], [["7", "pending"]]),
        (["North", "South"], [["", "9"]]),
        (["", "South"], [["7", "9"]]),
        (["2024", "2025"], [["7", "9"]]),
        (["North", "South"], [["7", "9"], ["8", "10"]]),
        (["North", "South"], []),
        (["Only"], [["7"]]),
    ],
)
async def test_records_do_not_guess_existing_mixed_empty_or_numeric_header_tables(
    model_table_bytes, headers, rows
):
    assert await model_table_bytes({"headers": headers, "rows": rows}) == [headers, *rows]
