from __future__ import annotations

import io

from PIL import Image, ImageDraw, ImageFont
from reportlab.lib.utils import ImageReader
from reportlab.pdfgen import canvas

from .conftest import png_fixture, wav_fixture


async def test_real_sparse_flow_diagram_ocr_preserves_actual_method(isolated_worker, media_context):
    image = Image.new("RGB", (640, 620), "white")
    draw = ImageDraw.Draw(image)
    font = ImageFont.load_default(size=34)
    for index, label in enumerate(("Start", "Review", "Done")):
        top = 40 + index * 190
        draw.rectangle((150, top, 490, top + 120), outline="black", width=4)
        draw.text((320, top + 60), label, fill="black", font=font, anchor="mm")
        if index < 2:
            draw.line((320, top + 125, 320, top + 175), fill="black", width=4)
            draw.polygon(((310, top + 165), (320, top + 180), (330, top + 165)), fill="black")
    output = io.BytesIO()
    image.save(output, "PNG")
    result, _ = await isolated_worker.run(
        media_context, "ocr_image", (output.getvalue(),), parameters={"languages": "eng"}
    )
    page = result["pages"][0]
    assert all(label in page["text"] for label in ("Start", "Review", "Done"))
    assert page["method"] == "tesseract" and page["segmentation_mode"] in {6, 3, 11}
    assert page["attempted_segmentation_modes"] in ([6], [6, 3], [6, 3, 11])
    assert page["preprocessing"] == (
        "dark_ink_threshold_32" if len(page["attempted_segmentation_modes"]) == 3 else "none"
    )
    assert page["confidence"] is not None and 0 <= page["confidence"] <= 1


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
