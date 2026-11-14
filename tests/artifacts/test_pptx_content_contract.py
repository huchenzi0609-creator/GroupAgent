"""Actual native PPTX bytes; no renderer, process, database or provider calls."""

from __future__ import annotations

import io

import pytest
from pptx import Presentation

from groupagent.artifacts import Deck, Slide, TableData
from groupagent.artifacts.formats import DECK_MARKER, build_pptx, edit_deck_slide
from groupagent.artifacts.tools import TableInput


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
