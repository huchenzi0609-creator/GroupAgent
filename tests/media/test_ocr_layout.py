from __future__ import annotations

import io
from pathlib import Path
from types import SimpleNamespace

import pytest
from PIL import Image

from groupagent.domain import DomainError
from groupagent.media import validation

HEADER = b"level\tpage_num\tblock_num\tpar_num\tline_num\tword_num\tleft\ttop\twidth\theight\tconf\ttext\n"


def raster():
    result = io.BytesIO()
    Image.new("RGB", (640, 620), "white").save(result, "PNG")
    return result.getvalue()


def tsv(text):
    return HEADER + b"".join(
        f"5\t1\t{index}\t1\t1\t1\t40\t{40 * index}\t90\t30\t94.5\t{word}\n".encode()
        for index, word in enumerate(text.split(), start=1)
    )


@pytest.mark.parametrize(
    "responses,expected,attempts",
    [
        ([tsv("Already Read")], "Already\nRead", (6,)),
        ([HEADER, tsv("Start Review Done")], "Start\nReview\nDone", (6, 11)),
        ([HEADER, HEADER, tsv("Dark ink")], "Dark\nink", (6, 11, 11)),
        ([HEADER, HEADER, HEADER], "", (6, 11, 11)),
    ],
)
def test_fallback_only_for_empty_text_is_bounded_and_records_real_layout(
    monkeypatch, responses, expected, attempts
):
    calls = []
    clocks = iter((100.0, 100.0, 100.25, 100.5))
    monkeypatch.setattr(validation.shutil, "which", lambda name: "/fixture/tesseract")
    monkeypatch.setattr(validation, "time", SimpleNamespace(monotonic=lambda: next(clocks)))

    def run(arguments, *, timeout):
        mode = int(arguments[arguments.index("--psm") + 1])
        calls.append((mode, timeout, Path(arguments[1])))
        assert calls[-1][2].is_file()
        assert arguments[-1] == "tsv" and arguments[4] == "eng+chi_sim"
        assert calls[-1][2].name == ("dark-ink.png" if len(calls) == 3 else "page.png")
        return responses[len(calls) - 1]

    monkeypatch.setattr(validation, "_run", run)
    result = validation.ocr_image(raster(), page_number=8)
    assert result.text == expected and result.page_number == 8
    assert result.method == "tesseract"
    assert result.segmentation_mode == attempts[-1]
    assert result.attempted_segmentation_modes == attempts
    assert result.preprocessing == ("dark_ink_threshold_32" if len(calls) == 3 else "none")
    assert tuple(call[0] for call in calls) == attempts
    assert [call[1] for call in calls] == [60.0, 59.75, 59.5][: len(calls)]
    assert not any(call[2].exists() for call in calls)
    assert result.width_pixels == 640 and result.height_pixels == 620
    if expected:
        assert result.confidence == pytest.approx(0.945)
        assert all(word.confidence == 0.945 for word in result.words)
        assert result.words[0].box == (40, 40, 130, 70)
    else:
        assert result.confidence is None and not result.words


def test_timeout_budget_is_shared_across_segmentation_attempts(monkeypatch):
    clocks = iter((10.0, 10.0, 70.0))
    calls = []
    monkeypatch.setattr(validation.shutil, "which", lambda name: "/fixture/tesseract")
    monkeypatch.setattr(validation, "time", SimpleNamespace(monotonic=lambda: next(clocks)))

    def run(arguments, *, timeout):
        calls.append(arguments)
        return HEADER

    monkeypatch.setattr(validation, "_run", run)
    with pytest.raises(DomainError) as error:
        validation.ocr_image(raster())
    assert error.value.code == "media_timeout" and len(calls) == 1


def test_dark_ink_retry_changes_only_the_ocr_view_and_keeps_source_coordinates(monkeypatch):
    image = Image.new("RGB", (640, 620), "white")
    image.putpixel((11, 12), (0, 0, 0))
    image.putpixel((30, 20), (31, 55, 94))
    output = io.BytesIO()
    image.save(output, "PNG")
    original = output.getvalue()
    calls = []
    monkeypatch.setattr(validation.shutil, "which", lambda name: "/fixture/tesseract")

    def run(arguments, *, timeout):
        calls.append(arguments)
        if len(calls) < 3:
            return HEADER
        with Image.open(arguments[1]) as view:
            assert view.size == image.size
            assert view.getpixel((11, 12)) == 0
            assert view.getpixel((30, 20)) == 255
        return tsv("Readable")

    monkeypatch.setattr(validation, "_run", run)
    result = validation.ocr_image(original)
    assert output.getvalue() == original
    assert result.preprocessing == "dark_ink_threshold_32"
    assert result.words[0].box == (40, 40, 130, 70)


@pytest.mark.parametrize("output", [b"unexpected\nresponse", HEADER + b"malformed\n"])
def test_malformed_ocr_output_does_not_trigger_layout_retries(monkeypatch, output):
    calls = []
    monkeypatch.setattr(validation.shutil, "which", lambda name: "/fixture/tesseract")

    def run(arguments, *, timeout):
        calls.append(arguments)
        return output

    monkeypatch.setattr(validation, "_run", run)
    with pytest.raises(DomainError) as error:
        validation.ocr_image(raster())
    assert error.value.code == "invalid_ocr_response" and len(calls) == 1


def test_ocr_process_failure_does_not_become_an_empty_success(monkeypatch):
    calls = []
    monkeypatch.setattr(validation.shutil, "which", lambda name: "/fixture/tesseract")

    def run(arguments, *, timeout):
        calls.append(arguments)
        raise DomainError("media_processing_failed", "Synthetic process failure")

    monkeypatch.setattr(validation, "_run", run)
    with pytest.raises(DomainError) as error:
        validation.ocr_image(raster())
    assert error.value.code == "media_processing_failed" and len(calls) == 1
