"""Read-only Word acceptance checks against actual OpenXML bytes, no provider calls."""

from __future__ import annotations

import hashlib
import importlib.util
import io
from pathlib import Path

import pytest
from docx import Document
from PIL import Image

SCRIPT = Path(__file__).resolve().parents[1] / "scripts/live_durable_core_acceptance.py"
SPEC = importlib.util.spec_from_file_location("live_durable_core_acceptance_check", SCRIPT)
runner = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(runner)


@pytest.mark.parametrize("placement", ["below", "above", "missing"])
def test_real_word_image_source_mapping_requires_corresponding_below_caption(placement):
    document = Document()
    sources = []
    for index, color in enumerate(("red", "blue")):
        png = io.BytesIO()
        Image.new("RGB", (10 + index, 10), color).save(png, format="PNG")
        content = png.getvalue()
        url = f"https://example.org/image-{index}"
        if placement == "above":
            document.add_paragraph("Source: " + url)
        document.add_picture(io.BytesIO(content))
        document.add_paragraph("Caption" + (" Source: " + url if placement == "below" else ""))
        sources.append(
            {
                "media_metadata": {"preview_sha256": hashlib.sha256(content).hexdigest()},
                "public_image": {"page_url": url},
            }
        )
    result = io.BytesIO()
    document.save(result)
    checked = runner.inspect_word_sources(result.getvalue(), sources)
    assert checked["word_image_count"] == 2
    assert checked["word_sources_below_images"] is (placement == "below")
    assert checked["source_link_count"] == (0 if placement == "missing" else 2)
    # Namespace URLs exist in every DOCX; they are not user-visible citations.
    assert all(url.startswith("https://example.org/") for url in checked["visible_source_urls"])


def test_correct_url_with_wrong_embedded_source_bytes_is_rejected():
    document = Document()
    png = io.BytesIO()
    Image.new("RGB", (10, 10), "green").save(png, format="PNG")
    sources = []
    for index in range(2):
        url = f"https://example.org/image-{index}"
        document.add_picture(io.BytesIO(png.getvalue()))
        document.add_paragraph("Source: " + url)
        sources.append(
            {
                "media_metadata": {"preview_sha256": "0" * 64},
                "public_image": {"page_url": url},
            }
        )
    result = io.BytesIO()
    document.save(result)
    checked = runner.inspect_word_sources(result.getvalue(), sources)
    assert checked["source_link_count"] == 2
    assert not checked["word_sources_below_images"]
