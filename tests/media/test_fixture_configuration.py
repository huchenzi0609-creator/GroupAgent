from __future__ import annotations

import io

import pytest
from PIL import Image, ImageFont

from .conftest import png_fixture


def test_missing_explicit_font_does_not_silently_use_a_fallback(tmp_path, monkeypatch):
    monkeypatch.setenv("GROUPAGENT_TEST_CJK_FONT", str(tmp_path / "not-present.ttc"))
    with pytest.raises(pytest.fail.Exception, match="font is not available"):
        png_fixture("Synthetic ASCII fixture")


def test_explicit_collection_face_is_used(tmp_path, monkeypatch):
    path = tmp_path / "fixture-font.ttc"
    path.write_bytes(b"Synthetic path only; font loader is explicitly stubbed")
    actual_font = ImageFont.load_default(size=54)
    selected = []

    def fixture_loader(name, size, *, index):
        selected.append((name, size, index))
        return actual_font

    monkeypatch.setenv("GROUPAGENT_TEST_CJK_FONT", str(path))
    monkeypatch.setenv("GROUPAGENT_TEST_CJK_FONT_INDEX", "2")
    monkeypatch.setattr(ImageFont, "truetype", fixture_loader)
    result = png_fixture("Synthetic ASCII fixture")
    assert selected == [(str(path), 54, 2)]
    with Image.open(io.BytesIO(result)) as image:
        assert image.size == (1600, 800)


@pytest.mark.parametrize("index", ["-1", "16", "not-an-index"])
def test_invalid_collection_index_fails_explicitly(tmp_path, monkeypatch, index):
    path = tmp_path / "fixture-font.ttc"
    path.write_bytes(b"No font parse is allowed before index validation")
    monkeypatch.setenv("GROUPAGENT_TEST_CJK_FONT", str(path))
    monkeypatch.setenv("GROUPAGENT_TEST_CJK_FONT_INDEX", index)
    with pytest.raises(pytest.fail.Exception, match="font index"):
        png_fixture("Synthetic ASCII fixture")
