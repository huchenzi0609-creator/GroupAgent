from __future__ import annotations

import io

from reportlab.lib.utils import ImageReader
from reportlab.pdfgen import canvas

from .conftest import png_fixture, wav_fixture


async def test_real_isolated_image_and_english_chinese_ocr(isolated_worker, media_context):
    image = png_fixture("SYNTHETIC REPORT\nDeadline 2026-11-20\nScore 87\n课程报告 样本人数24人")
    info, outputs = await isolated_worker.run(
        media_context, "image_validate", (image,), outputs=("view.png",)
    )
    assert info["width"] == 1600 and info["height"] == 800 and outputs[0].startswith(b"\x89PNG")
    result, _ = await isolated_worker.run(
        media_context, "ocr_image", (image,), parameters={"languages": "eng+chi_sim"}
    )
    text = result["pages"][0]["text"]
    assert "87" in text and "2026" in text and "24" in text
    assert "课程" in text.replace(" ", "")


async def test_real_scanned_pdf_ocr_keeps_page_numbers(isolated_worker, media_context):
    output = io.BytesIO()
    document = canvas.Canvas(output, pagesize=(800, 400))
    document.drawImage(
        ImageReader(io.BytesIO(png_fixture("PAGE ONE\nResult 87", size=(1200, 600)))),
        0,
        0,
        width=800,
        height=400,
    )
    document.showPage()
    document.drawImage(
        ImageReader(io.BytesIO(png_fixture("PAGE TWO\nSample 24", size=(1200, 600)))),
        0,
        0,
        width=800,
        height=400,
    )
    document.save()
    result, _ = await isolated_worker.run(
        media_context, "ocr_pdf", (output.getvalue(),), parameters={"languages": "eng"}
    )
    assert [page["page_number"] for page in result["pages"]] == [1, 2]
    assert "87" in result["pages"][0]["text"] and "24" in result["pages"][1]["text"]
    assert all(page["method"] == "tesseract" for page in result["pages"])


async def test_real_audio_normalization_and_chunking(isolated_worker, media_context):
    result, outputs = await isolated_worker.run(
        media_context,
        "audio_validate",
        (wav_fixture(rate=24000, channels=2),),
        outputs=("normalized.wav", *(f"chunk-{i}.wav" for i in range(6))),
    )
    assert result["channels"] == 2 and result["chunk_indices"] == [1]
    assert outputs[0].startswith(b"RIFF") and outputs[1].startswith(b"RIFF")
