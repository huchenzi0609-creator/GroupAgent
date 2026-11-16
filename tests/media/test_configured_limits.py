"""Configured limits reach real decoders; no Docker, provider or database calls."""

from __future__ import annotations

import io
import struct
import zlib
from pathlib import Path

import pytest
from PIL import Image

from groupagent.artifacts import ArtifactStore, IsolatedDocumentService, Report, Section
from groupagent.artifacts.documents import build_docx
from groupagent.domain import DomainError
from groupagent.media.service import MediaService
from groupagent.media.validation import inspect_image
from groupagent.media.worker import execute
from groupagent.tools.models import ToolFailure

from .test_providers import route


def png() -> bytes:
    output = io.BytesIO()
    Image.new("RGB", (8, 5), "white").save(output, "PNG")
    return output.getvalue()


class InlineWorker:
    """Execute the actual worker dispatcher inline, without claiming isolation."""

    def __init__(self):
        self.calls = []

    async def run(self, context, operation, inputs, *, parameters=None, outputs=()):
        self.calls.append((operation, parameters))
        names = tuple(f"input-{index}.bin" for index in range(len(inputs)))
        for name, content in zip(names, inputs, strict=True):
            Path(name).write_bytes(content)
        result = execute(
            {
                "operation": operation,
                "inputs": names,
                "outputs": outputs,
                "parameters": parameters or {},
            }
        )
        return result, tuple(Path(name).read_bytes() for name in outputs)


@pytest.fixture
def inline_worker(tmp_path, monkeypatch):
    root = tmp_path / "inline-worker"
    root.mkdir()
    monkeypatch.chdir(root)
    return InlineWorker()


def test_image_byte_limit_rejects_before_opening_the_decoder(monkeypatch):
    content = png()
    monkeypatch.setattr(Image, "open", lambda *args, **kwargs: pytest.fail("Decoder was opened"))
    with pytest.raises(DomainError) as error:
        inspect_image(content, max_bytes=len(content) - 1)
    assert error.value.code == "image_size_limit"


def test_twelve_mp_limit_rejects_twenty_mp_header_before_any_pixel_load(monkeypatch):
    original = png()
    # Advertise a larger real PNG header without allocating a 20 MP raster.
    header = struct.pack(">II", 5000, 4000) + original[24:29]
    checksum = zlib.crc32(b"IHDR" + header).to_bytes(4, "big")
    content = original[:16] + header + checksum + original[33:]
    monkeypatch.setattr(Image.Image, "load", lambda *args: pytest.fail("Pixels were decoded"))
    with pytest.raises(DomainError) as error:
        inspect_image(content, max_pixels=12_000_000)
    assert error.value.code == "image_pixel_limit"


def test_exact_image_byte_and_pixel_limits_still_decode_complete_pixels():
    content = png()
    info, preview = inspect_image(content, max_bytes=len(content), max_pixels=40)
    assert (info.width, info.height) == (8, 5)
    with Image.open(io.BytesIO(preview)) as decoded:
        decoded.load()
        assert decoded.size == (8, 5)


@pytest.mark.parametrize("operation", ["image_validate", "ocr_image"])
@pytest.mark.parametrize(
    "limits,code",
    [({"max_bytes": 1}, "image_size_limit"), ({"max_pixels": 39}, "image_pixel_limit")],
)
def test_actual_worker_dispatcher_enforces_configured_image_limits(
    inline_worker, operation, limits, code
):
    Path("image.bin").write_bytes(png())
    with pytest.raises(DomainError) as error:
        execute(
            {
                "operation": operation,
                "inputs": ["image.bin"],
                "outputs": ["preview.png"],
                "parameters": limits,
            }
        )
    assert error.value.code == code
    assert not Path("preview.png").exists()


async def test_oversized_ingest_does_not_start_worker_or_publish(
    tmp_path, media_context, media_source, inline_worker
):
    with ArtifactStore(tmp_path / "store") as store:
        service = MediaService(store, inline_worker, max_image_bytes=8 * 1024**2)
        with pytest.raises(DomainError) as error:
            await service.ingest_image(media_context, media_source, b"x" * (8 * 1024**2 + 1))
        assert error.value.code == "image_size_limit"
        assert not inline_worker.calls and not store.list(service.scope(media_context))


async def test_ingest_passes_caps_to_real_decoder_and_only_publishes_accepted_pixels(
    tmp_path, media_context, media_source, inline_worker
):
    content = png()
    with ArtifactStore(tmp_path / "store") as store:
        accepted = MediaService(
            store, inline_worker, max_image_bytes=len(content), max_image_pixels=40
        )
        reference = await accepted.ingest_image(media_context, media_source, content)
        assert (reference.width, reference.height) == (8, 5)
        assert inline_worker.calls[-1] == (
            "image_validate",
            {"max_bytes": len(content), "max_pixels": 40},
        )
        rejected = MediaService(store, inline_worker, max_image_pixels=39)
        with pytest.raises(DomainError) as error:
            await rejected.ingest_image(media_context, media_source, content)
        assert error.value.code == "image_pixel_limit"
        assert len(store.list(accepted.scope(media_context))) == 1


@pytest.mark.parametrize("operation", ["vision", "local_ocr", "cloud_ocr"])
@pytest.mark.parametrize("limit", ["bytes", "pixels"])
async def test_existing_images_are_rechecked_before_vision_or_ocr_payment(
    tmp_path, media_context, media_source, inline_worker, operation, limit
):
    content = png()
    with ArtifactStore(tmp_path / "store") as store:
        reference = await MediaService(store, inline_worker).ingest_image(
            media_context, media_source, content
        )
        service = MediaService(
            store,
            inline_worker,
            **(
                {"max_image_bytes": len(content) - 1}
                if limit == "bytes"
                else {"max_image_pixels": 39}
            ),
        )
        inline_worker.calls.clear()
        with pytest.raises(DomainError) as error:
            if operation == "vision":
                await service.analyze_images(media_context, (reference,), "Read", route("vision"))
            else:
                await service.ocr(
                    media_context,
                    reference,
                    route=route("ocr") if operation == "cloud_ocr" else None,
                )
        assert error.value.code == ("image_size_limit" if limit == "bytes" else "image_pixel_limit")
        if limit == "bytes":
            assert not inline_worker.calls


async def test_vision_image_count_cannot_expand_via_existing_artifact_references(
    tmp_path, media_context, media_source, inline_worker
):
    with ArtifactStore(tmp_path / "store") as store:
        service = MediaService(store, inline_worker, max_images=4)
        reference = await service.ingest_image(media_context, media_source, png())
        inline_worker.calls.clear()
        with pytest.raises(DomainError) as error:
            await service.analyze_images(media_context, (reference,) * 5, "Read", route("vision"))
        assert error.value.code == "invalid_media_request" and not inline_worker.calls


async def test_office_import_exceeding_16_mib_does_not_start_worker_or_publish(
    tmp_path, media_context, inline_worker
):
    with ArtifactStore(tmp_path / "store") as store:
        service = IsolatedDocumentService(store, inline_worker, max_import_bytes=16 * 1024**2)
        with pytest.raises(ToolFailure) as error:
            await service.import_document(
                media_context, "synthetic.docx", b"x" * (16 * 1024**2 + 1)
            )
        assert error.value.code == "file_too_large" and error.value.no_effect
        assert not inline_worker.calls and not store.list(MediaService.scope(media_context))


async def test_markdown_limit_is_independent_and_reaches_actual_worker_reader(
    tmp_path, media_context, inline_worker
):
    content = b"# Synthetic\n\nBody.\n"
    with ArtifactStore(tmp_path / "store") as store:
        service = IsolatedDocumentService(
            store, inline_worker, max_import_bytes=1, max_markdown_bytes=len(content)
        )
        version = await service.import_document(media_context, "synthetic.md", content)
        assert version.size_bytes == len(content)
        assert inline_worker.calls[-1] == (
            "document_inspect",
            {"format": "md", "max_import_bytes": len(content)},
        )
        with pytest.raises(ToolFailure) as error:
            await service.import_document(media_context, "too-large.md", content + b"x")
        assert error.value.no_effect and len(inline_worker.calls) == 1


def test_document_worker_rejects_input_before_the_format_parser(inline_worker, monkeypatch):
    from groupagent.artifacts import worker_operations

    monkeypatch.setattr(
        worker_operations, "extract", lambda *args: pytest.fail("Parser was called")
    )
    Path("document.bin").write_bytes(b"x" * 17)
    with pytest.raises(DomainError) as error:
        execute(
            {
                "operation": "document_inspect",
                "inputs": ["document.bin"],
                "outputs": [],
                "parameters": {"format": "docx", "max_import_bytes": 16},
            }
        )
    assert error.value.code == "media_size_limit"


def test_standard_service_limits_remain_unchanged():
    media = MediaService(None, None)
    documents = IsolatedDocumentService(None, None)
    assert (media.max_image_bytes, media.max_image_pixels, media.max_images) == (
        15 * 1024**2,
        40_000_000,
        10,
    )
    assert (documents.max_import_bytes, documents.max_markdown_bytes) == (30 * 1024**2, 2 * 1024**2)
    assert documents.max_output_bytes == 25 * 1024**2


async def test_trusted_generated_adoption_uses_output_cap_without_widening_inputs(
    tmp_path, media_context, inline_worker
):
    content = build_docx(Report("Synthetic", (Section("Results", ("Body.",)),)))
    with ArtifactStore(tmp_path / "store") as store:
        service = IsolatedDocumentService(
            store,
            inline_worker,
            max_import_bytes=len(content) - 1,
            max_output_bytes=len(content),
        )
        with pytest.raises(ToolFailure) as error:
            await service.import_document(media_context, "input.docx", content)
        assert error.value.no_effect and not inline_worker.calls
        version = await service.import_document(
            media_context, "output.docx", content, generated_output=True
        )
        assert version.size_bytes == len(content)
        assert len(store.list(MediaService.scope(media_context))) == 1


async def test_generated_adoption_and_native_generation_reject_oversized_outputs(
    tmp_path, media_context, inline_worker
):
    document = Report("Synthetic", (Section("Results", ("Body.",)),))
    content = build_docx(document)
    with ArtifactStore(tmp_path / "store") as store:
        service = IsolatedDocumentService(store, inline_worker, max_output_bytes=1)
        with pytest.raises(ToolFailure) as error:
            await service.import_document(
                media_context, "output.docx", content, generated_output=True
            )
        assert error.value.no_effect and not inline_worker.calls
        with pytest.raises(ToolFailure) as error:
            await service.generate(media_context, "native.docx", document)
        assert error.value.code == "file_too_large" and error.value.no_effect
        assert not store.list(MediaService.scope(media_context))


@pytest.mark.parametrize(
    "service,limits",
    [
        (MediaService, {"max_image_bytes": 0}),
        (MediaService, {"max_image_pixels": True}),
        (MediaService, {"max_images": 11}),
        (IsolatedDocumentService, {"max_import_bytes": 0}),
        (IsolatedDocumentService, {"max_markdown_bytes": 2 * 1024**2 + 1}),
    ],
)
def test_invalid_service_limits_cannot_silently_widen_or_disable_caps(service, limits):
    with pytest.raises(ValueError):
        service(None, None, **limits)
