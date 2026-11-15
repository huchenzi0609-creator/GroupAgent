"""Real synthetic image bytes with inline decoding and fixture HTTP, no live API."""

import base64
import hashlib
import io
import json

import httpx
import pytest
from PIL import Image, PngImagePlugin

from groupagent.artifacts import ArtifactStore
from groupagent.domain import DomainError
from groupagent.media import MediaHTTPProvider, MediaService
from groupagent.media.models import VisionResult

from .test_providers import route
from .test_service_contract import (
    InlineSyntheticWorker,
    NoChargeFixtureBudget,
    vision_response,
)


def source_png():
    image = Image.new("RGBA", (48, 32), (40, 80, 120, 160))
    metadata = PngImagePlugin.PngInfo()
    metadata.add_text("synthetic-source", "Original metadata is not a provider view")
    output = io.BytesIO()
    image.save(output, "PNG", pnginfo=metadata, compress_level=0)
    return output.getvalue()


async def test_normalized_view_is_bound_to_exact_original_and_actual_http_pixels(
    tmp_path, media_context, media_source
):
    original = source_png()
    worker, budget, captured = InlineSyntheticWorker(), NoChargeFixtureBudget(), []

    def respond(request):
        captured.append(request)
        return vision_response(request)

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as http:
        with ArtifactStore(tmp_path / "store") as store:
            service = MediaService(
                store,
                worker,
                providers={"deepseek": MediaHTTPProvider("deepseek", "fixture-key", client=http)},
                budget=budget,
            )
            reference = await service.ingest_image(media_context, media_source, original)
            result = await service.analyze_images(
                media_context, (reference,), "Describe the image", route("vision")
            )
            assert len(captured) == 1 and len(result.image_bindings) == 1
            binding = result.image_bindings[0]
            body = json.loads(captured[0].content)
            image_url = next(
                item["image_url"]["url"]
                for item in body["messages"][0]["content"]
                if item["type"] == "image_url"
            )
            view = base64.b64decode(image_url.split(",", 1)[1])
            assert view != original and binding.source_sha256 != binding.normalized_view_sha256
            assert binding.source_sha256 == reference.sha256 == hashlib.sha256(original).hexdigest()
            assert binding.normalized_view_sha256 == hashlib.sha256(view).hexdigest()
            assert result.image_sha256 == (binding.normalized_view_sha256,)
            assert (binding.file_id, binding.version_id, binding.source) == (
                reference.file_id,
                reference.version_id,
                media_source,
            )
            assert binding.image_index == 0 and (binding.width, binding.height) == (48, 32)
            assert binding.alpha_composited_on_white and not binding.static_first_frame
            assert result.static_views == (False,)
            with Image.open(io.BytesIO(view)) as image:
                assert image.mode == "RGB"
                assert (
                    binding.normalized_pixel_sha256 == hashlib.sha256(image.tobytes()).hexdigest()
                )
            assert (
                store.read(service.scope(media_context), reference.file_id, reference.version_id)
                == original
            )
            assert set(budget.states.values()) == {"settled"}


@pytest.mark.parametrize(
    "corruption",
    ["reference", "source_hash", "view_hash", "view_bytes", "source_length", "missing_view"],
)
async def test_inconsistent_image_evidence_is_rejected_before_any_paid_request(
    tmp_path, media_context, media_source, corruption
):
    class FaultyWorker(InlineSyntheticWorker):
        corrupt = False

        async def run(self, *args, **kwargs):
            result, outputs = await super().run(*args, **kwargs)
            if self.corrupt:
                if corruption == "source_hash":
                    result["source_sha256"] = "0" * 64
                elif corruption == "view_hash":
                    result["preview_sha256"] = "0" * 64
                elif corruption == "view_bytes":
                    outputs = (outputs[0] + b"different bytes",)
                elif corruption == "source_length":
                    result["byte_length"] += 1
                elif corruption == "missing_view":
                    outputs = ()
            return result, outputs

    worker, budget, captured = FaultyWorker(), NoChargeFixtureBudget(), []

    def respond(request):
        captured.append(request)
        return vision_response(request)

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as http:
        with ArtifactStore(tmp_path / "store") as store:
            service = MediaService(
                store,
                worker,
                providers={"deepseek": MediaHTTPProvider("deepseek", "fixture-key", client=http)},
                budget=budget,
            )
            reference = await service.ingest_image(media_context, media_source, source_png())
            if corruption == "reference":
                reference = reference.model_copy(update={"sha256": "0" * 64})
            worker.corrupt = True
            with pytest.raises(DomainError) as failed:
                await service.analyze_images(media_context, (reference,), "Read", route("vision"))
            expected = (
                "media_reference_mismatch"
                if corruption == "reference"
                else "worker_integrity_failed"
            )
            assert failed.value.code == expected
            assert not captured and not budget.keys and not budget.states


def test_old_vision_result_remains_readable_without_implying_an_original_hash():
    result = VisionResult.model_validate(
        {
            "text": "Legacy view observation",
            "sources": [],
            "image_sha256": ["a" * 64],
            "static_views": [False],
            "provider": "deepseek",
            "model": "fixture",
            "request_id": "legacy",
        }
    )
    assert result.image_bindings == () and result.image_sha256 == ("a" * 64,)
