from __future__ import annotations

import base64
import io
import json

import httpx
import pytest
from PIL import Image

from groupagent.domain import DomainError
from groupagent.media import CapabilityRegistry, MediaHTTPProvider, ProviderRoute

from .conftest import png_fixture, wav_fixture


def route(capability, model=None, provider="deepseek"):
    return ProviderRoute(
        route_id="fixture-" + capability,
        provider=provider,
        capability=capability,
        model=model
        or {"vision": "deepseek-flash", "asr": "tools/asr", "tts": "tools/tts", "ocr": "tools/ocr"}[
            capability
        ],
        price_id="fixture-price",
    )


@pytest.mark.parametrize(
    "provider,capability,model",
    [
        ("deepseek", "image_generation", "deepseek-flash"),
        ("deepseek", "asr", "deepseek-flash"),
        ("deepseek", "vision", "unknown-model"),
        ("openai", "image_generation", "gpt-image-1"),
    ],
)
def test_unknown_or_wrong_capability_is_not_inferred(provider, capability, model):
    with pytest.raises(DomainError) as error:
        CapabilityRegistry().resolve(route(capability, model, provider))
    assert error.value.code == "unsupported_model_capability"


async def test_vision_sends_actual_ordered_pixels_and_association():
    captured = []

    def response(request):
        captured.append(request)
        return httpx.Response(
            200,
            json={
                "id": "vision-fixture",
                "choices": [{"finish_reason": "stop", "message": {"content": "The score is 87."}}],
                "usage": {"prompt_tokens": 400, "completion_tokens": 20},
            },
        )

    pixels = png_fixture()
    async with httpx.AsyncClient(transport=httpx.MockTransport(response)) as client:
        provider = MediaHTTPProvider("deepseek", "synthetic-key", client=client)
        result = await provider.vision(
            route("vision"),
            "Read the score",
            (pixels,),
            ("message=one; author=alice; static view",),
        )
    body = json.loads(captured[0].content)
    assert body["messages"][0]["role"] == "system"
    assert "Do not identify" in body["messages"][0]["content"]
    user = next(item for item in body["messages"] if item["role"] == "user")
    image = user["content"][2]["image_url"]["url"]
    assert base64.b64decode(image.split(",", 1)[1]) == pixels
    assert "author=alice" in user["content"][1]["text"]
    assert result.text == "The score is 87." and result.usage["input_tokens"] == 400
    assert "synthetic-key" not in result.model_dump_json()


async def test_asr_uses_separate_endpoint_and_keeps_confidence_unknown():
    captured = []

    def response(request):
        captured.append(request)
        return httpx.Response(
            200,
            json={
                "request_id": "asr-fixture",
                "text": "请李明明天提交24份报告",
                "duration_ms": 400,
            },
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(response)) as client:
        result = await MediaHTTPProvider("deepseek", "synthetic-key", client=client).transcribe(
            route("asr"), wav_fixture(), 0.4
        )
    assert captured[0].url.path == "/v1/tools/asr"
    assert b'name="file"' in captured[0].content and b"RIFF" in captured[0].content
    assert b'name="model"' not in captured[0].content
    assert result.confidence is None and result.usage == {"audio_seconds": 1.0}


async def test_tts_is_separate_pcm_output_with_preset_voice():
    captured = []
    pcm = wav_fixture(rate=24000)[44:]

    def response(request):
        captured.append(request)
        return httpx.Response(
            200, content=pcm, headers={"Content-Type": "audio/pcm", "x-request-id": "tts-fixture"}
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(response)) as client:
        result = await MediaHTTPProvider("deepseek", "synthetic-key", client=client).synthesize(
            route("tts"), "你好"
        )
    assert captured[0].url.path == "/v1/tools/tts"
    assert json.loads(captured[0].content) == {
        "text": "你好",
        "voice_id": "stella",
        "format": "pcm",
        "filter_markdown": True,
    }
    assert result.content == pcm and result.usage == {"characters": 4.0}


async def test_image_generation_uses_independent_provider_and_real_inline_image():
    image = Image.new("RGB", (1024, 1024), "blue")
    output = io.BytesIO()
    image.save(output, "PNG")
    captured = []

    def response(request):
        captured.append(request)
        return httpx.Response(
            200,
            json={
                "created": 123,
                "data": [{"b64_json": base64.b64encode(output.getvalue()).decode()}],
            },
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(response)) as client:
        result = await MediaHTTPProvider(
            "openai", "synthetic-image-key", client=client
        ).generate_image(
            route("image_generation", "gpt-image-2.5-sunburst-2026-09-08", "openai"),
            "A classroom illustration",
        )
    assert (
        captured[0].url.host == "api.openai.com"
        and captured[0].url.path == "/v1/images/generations"
    )
    assert result.content == output.getvalue() and result.usage == {"images": 1.0}


async def test_ocr_checks_every_page_and_200_error_objects():
    payload = {
        "id": "ocr-fixture",
        "page_count": 2,
        "pages": [
            {"index": 0, "text": "Page one", "finish_reason": "stop"},
            {"index": 1, "text": "Page two", "finish_reason": "stop"},
        ],
    }
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: httpx.Response(200, json=payload))
    ) as client:
        provider = MediaHTTPProvider("deepseek", "synthetic-key", client=client)
        result = await provider.ocr(route("ocr"), png_fixture(), "image.png")
        assert [page.page_number for page in result.pages] == [1, 2]
        payload["pages"][1]["finish_reason"] = "length"
        with pytest.raises(DomainError):
            await provider.ocr(route("ocr"), png_fixture(), "image.png")
        payload.clear()
        payload["error"] = {"code": "gateway_timeout_error"}
        with pytest.raises(DomainError):
            await provider.ocr(route("ocr"), png_fixture(), "image.png")


@pytest.mark.parametrize("status", [302, 429, 503])
async def test_provider_failures_do_not_retry_or_follow_redirects(status):
    captured = []

    def response(request):
        captured.append(request)
        return httpx.Response(status, headers={"Location": "https://untrusted.invalid/key"})

    async with httpx.AsyncClient(transport=httpx.MockTransport(response)) as client:
        with pytest.raises(DomainError):
            await MediaHTTPProvider("deepseek", "synthetic-secret", client=client).vision(
                route("vision"), "Read", (png_fixture(),), ("Source",)
            )
    assert len(captured) == 1 and captured[0].url.host == "api.deepseek.com"


async def test_truncated_and_non_audio_tts_output_is_not_accepted():
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda request: httpx.Response(
                200, content=b"error" * 1000, headers={"Content-Type": "text/html"}
            )
        )
    ) as client:
        with pytest.raises(DomainError) as error:
            await MediaHTTPProvider("deepseek", "synthetic-key", client=client).synthesize(
                route("tts"), "Read this"
            )
        assert error.value.code == "invalid_provider_audio"


async def test_wrong_provider_key_is_not_forwarded():
    calls = []
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: calls.append(request))
    ) as client:
        provider = MediaHTTPProvider("deepseek", "synthetic-deepseek-key", client=client)
        with pytest.raises(DomainError) as error:
            await provider.generate_image(
                route("image_generation", "gpt-image-2.5-sunburst-2026-09-08", "openai"),
                "Draw a diagram",
            )
        assert error.value.code == "provider_route_mismatch"
    assert not calls
