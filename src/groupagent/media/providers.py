"""Documented, separately registered HTTP capabilities; no modality inference.

Official schemas verified 2026-11-14; see README.md. Calls are made only by the
budgeted MediaService. This module does not discover keys from the environment.
"""

from __future__ import annotations

import base64
import binascii
import json
from dataclasses import dataclass
from typing import Any

import httpx
from pydantic import SecretStr

from groupagent.domain import DomainError

from .models import Capability, OCRPage, ProviderResponse, ProviderRoute


@dataclass(frozen=True)
class CapabilitySpec:
    provider: str
    capability: Capability
    model: str
    endpoint: str
    documentation: str


_REVIEWED = (
    CapabilitySpec(
        "deepseek",
        Capability.VISION,
        "deepseek-flash",
        "https://api.deepseek.com/chat/completions",
        "https://api-docs.deepseek.com/guides/vision/",
    ),
    CapabilitySpec(
        "deepseek",
        Capability.ASR,
        "tools/asr",
        "https://api.deepseek.com/v1/tools/asr",
        "https://api-docs.deepseek.com/guides/asr/",
    ),
    CapabilitySpec(
        "deepseek",
        Capability.TTS,
        "tools/tts",
        "https://api.deepseek.com/v1/tools/tts",
        "https://api-docs.deepseek.com/guides/tts/",
    ),
    CapabilitySpec(
        "deepseek",
        Capability.OCR,
        "tools/ocr",
        "https://api.deepseek.com/v1/tools/ocr",
        "https://api-docs.deepseek.com/guides/ocr/",
    ),
    CapabilitySpec(
        "openai",
        Capability.IMAGE_GENERATION,
        "gpt-image-2.5-sunburst-2026-09-08",
        "https://api.openai.com/v1/images/generations",
        "https://developers.openai.com/api/reference/resources/images/methods/generate",
    ),
    CapabilitySpec(
        "openai",
        Capability.IMAGE_GENERATION,
        "gpt-image-2.5-flare-2026-09-08",
        "https://api.openai.com/v1/images/generations",
        "https://developers.openai.com/api/reference/resources/images/methods/generate",
    ),
)


class CapabilityRegistry:
    def __init__(self) -> None:
        self._specs = {(spec.provider, spec.capability, spec.model): spec for spec in _REVIEWED}

    def resolve(self, route: ProviderRoute) -> CapabilitySpec:
        result = self._specs.get((route.provider, route.capability, route.model))
        if result is None:
            raise DomainError(
                "unsupported_model_capability", "Provider/model capability has not been reviewed"
            )
        return result

    def describe(self, *, include_extensions: bool = False) -> tuple[CapabilitySpec, ...]:
        return tuple(
            spec
            for spec in self._specs.values()
            if include_extensions or spec.capability != Capability.IMAGE_GENERATION
        )


class MediaHTTPProvider:
    def __init__(
        self,
        provider: str,
        api_key: str,
        *,
        client: httpx.AsyncClient | None = None,
        registry: CapabilityRegistry | None = None,
        timeout_seconds: float = 180,
    ):
        if (
            provider not in {"deepseek", "openai"}
            or not api_key
            or any(ord(c) < 33 or ord(c) > 126 for c in api_key)
        ):
            raise DomainError(
                "provider_not_configured", "A reviewed provider and explicit key are required"
            )
        self.provider = provider
        self._secret = SecretStr(api_key)
        self._owned = client is None
        self.client = client or httpx.AsyncClient(trust_env=False, follow_redirects=False)
        self.fixture_transport = client is not None and isinstance(
            client._transport, httpx.MockTransport
        )
        self.registry = registry or CapabilityRegistry()
        self.timeout_seconds = min(180, max(1, timeout_seconds))

    async def aclose(self) -> None:
        if self._owned:
            await self.client.aclose()

    async def _request(
        self,
        spec: CapabilitySpec,
        *,
        body: dict[str, Any] | None = None,
        files: dict[str, Any] | None = None,
        data: dict[str, str] | None = None,
        binary: bool = False,
    ) -> tuple[Any, str]:
        if spec.provider != self.provider:
            raise DomainError("provider_route_mismatch", "Route belongs to another provider")
        if body is not None and len(json.dumps(body).encode()) > 48 * 1024 * 1024:
            raise DomainError(
                "provider_input_limit", "Media request exceeds the provider body limit"
            )
        limit = 36 * 1024 * 1024
        try:
            async with self.client.stream(
                "POST",
                spec.endpoint,
                json=body,
                files=files,
                data=data,
                headers={
                    "Authorization": "Bearer " + self._secret.get_secret_value(),
                    "Accept-Encoding": "identity",
                },
                timeout=self.timeout_seconds,
                follow_redirects=False,
            ) as response:
                if 300 <= response.status_code < 400:
                    raise DomainError(
                        "provider_redirect_rejected", "Media provider redirects are not followed"
                    )
                if response.status_code >= 400:
                    code = (
                        "provider_rate_limited"
                        if response.status_code == 429
                        else "provider_request_failed"
                    )
                    raise DomainError(
                        code,
                        f"Media provider returned HTTP {response.status_code}",
                        retryable=False,
                    )
                declared = response.headers.get("content-length")
                if declared is not None and (not declared.isdecimal() or int(declared) > limit):
                    raise DomainError(
                        "provider_output_limit", "Provider response exceeds its byte limit"
                    )
                result = bytearray()
                async for chunk in response.aiter_bytes(chunk_size=65536):
                    result.extend(chunk)
                    if len(result) > limit:
                        raise DomainError(
                            "provider_output_limit", "Provider response exceeds its byte limit"
                        )
                request_id = response.headers.get("x-request-id", "")
                if binary:
                    mime = response.headers.get("content-type", "").split(";", 1)[0].lower()
                    if mime not in {
                        "audio/pcm",
                        "audio/l16",
                        "application/octet-stream",
                        "binary/octet-stream",
                    }:
                        raise DomainError(
                            "invalid_provider_audio", "PCM response has an unexpected content type"
                        )
                    return bytes(result), request_id
                try:
                    payload = json.loads(result)
                    if not isinstance(payload, dict) or "error" in payload:
                        raise ValueError("error object")
                    return payload, request_id
                except (ValueError, UnicodeError):
                    raise DomainError(
                        "invalid_provider_response",
                        "Media provider returned an incomplete or error result",
                    ) from None
        except httpx.HTTPError:
            raise DomainError(
                "provider_outcome_unknown",
                "Media request response was lost; budget remains reserved",
                effect_state="unknown",
            ) from None

    async def vision(
        self, route: ProviderRoute, prompt: str, images: tuple[bytes, ...], labels: tuple[str, ...]
    ) -> ProviderResponse:
        spec = self.registry.resolve(route)
        if (
            route.capability != Capability.VISION
            or not images
            or len(images) > 10
            or len(labels) != len(images)
        ):
            raise DomainError(
                "invalid_media_request", "Vision needs one to ten ordered, labeled images"
            )
        content: list[dict[str, Any]] = [{"type": "text", "text": prompt}]
        for label, image in zip(labels, images, strict=True):
            if not image.startswith(b"\x89PNG\r\n\x1a\n"):
                raise DomainError(
                    "unvalidated_pixels", "Vision input must be worker-validated static PNG pixels"
                )
            content.extend(
                (
                    {"type": "text", "text": label},
                    {
                        "type": "image_url",
                        "image_url": {
                            "url": "data:image/png;base64," + base64.b64encode(image).decode()
                        },
                    },
                )
            )
        payload, request_id = await self._request(
            spec,
            body={
                "model": route.model,
                "messages": [{"role": "user", "content": content}],
                "thinking": {"type": "disabled"},
                "max_tokens": 2048,
            },
        )
        try:
            choice = payload["choices"][0]
            if choice["finish_reason"] != "stop" or not isinstance(
                choice["message"]["content"], str
            ):
                raise ValueError("incomplete")
            usage = payload["usage"]
            if any(
                type(usage.get(key)) is not int or usage[key] < 0
                for key in ("prompt_tokens", "completion_tokens")
            ):
                raise ValueError("usage missing")
            return ProviderResponse(
                request_id=str(payload.get("id") or request_id),
                text=choice["message"]["content"],
                usage={
                    "input_tokens": float(usage["prompt_tokens"]),
                    "output_tokens": float(usage["completion_tokens"]),
                },
            )
        except (KeyError, TypeError, ValueError, IndexError):
            raise DomainError(
                "invalid_provider_response", "Vision response is incomplete"
            ) from None

    async def transcribe(
        self, route: ProviderRoute, content: bytes, duration_seconds: float
    ) -> ProviderResponse:
        spec = self.registry.resolve(route)
        if (
            route.capability != Capability.ASR
            or len(content) > 8 * 1024 * 1024
            or not 0 < duration_seconds <= 120
        ):
            raise DomainError(
                "provider_input_limit", "DeepSeek ASR chunks are at most 8 MiB and 120 seconds"
            )
        if not content.startswith(b"RIFF") or content[8:12] != b"WAVE":
            raise DomainError("unvalidated_audio", "ASR receives worker-normalized PCM WAV only")
        payload, request_id = await self._request(
            spec, files={"file": ("speech.wav", content, "audio/wav")}
        )
        try:
            text, millis = payload["text"], payload["duration_ms"]
            if (
                not isinstance(text, str)
                or type(millis) not in {int, float}
                or not 0 < millis <= 120_000
            ):
                raise ValueError("invalid transcript")
            if abs(millis / 1000 - duration_seconds) > max(1, duration_seconds * 0.1):
                raise ValueError("duration mismatch")
            return ProviderResponse(
                request_id=str(payload.get("request_id") or request_id),
                text=text,
                duration_seconds=millis / 1000,
                confidence=None,
                usage={"audio_seconds": math_ceil_seconds(millis / 1000)},
            )
        except (KeyError, ValueError, TypeError):
            raise DomainError(
                "invalid_provider_response",
                "ASR response is incomplete or has inconsistent duration",
            ) from None

    async def synthesize(
        self, route: ProviderRoute, text: str, *, voice: str = "stella"
    ) -> ProviderResponse:
        spec = self.registry.resolve(route)
        billed = sum(2 if "\u4e00" <= char <= "\u9fff" else 1 for char in text)
        if (
            route.capability != Capability.TTS
            or voice not in {"stella", "tide"}
            or not text.strip()
            or billed > 800
        ):
            raise DomainError(
                "invalid_media_request", "Short TTS needs text and an allowed preset voice"
            )
        # PCM has a known format. The worker wraps and verifies complete received samples.
        content, request_id = await self._request(
            spec,
            body={"text": text, "voice_id": voice, "format": "pcm", "filter_markdown": True},
            binary=True,
        )
        if len(content) < 4800 or len(content) % 2 or len(content) > 24_000 * 2 * 60:
            raise DomainError(
                "invalid_provider_audio", "Speech output is incomplete or exceeds sixty seconds"
            )
        return ProviderResponse(
            request_id=request_id or "http-complete",
            content=content,
            content_type="audio/pcm;rate=24000;channels=1;bits=16",
            duration_seconds=len(content) / 48000,
            usage={"characters": float(billed)},
        )

    async def generate_image(self, route: ProviderRoute, prompt: str) -> ProviderResponse:
        spec = self.registry.resolve(route)
        if (
            route.capability != Capability.IMAGE_GENERATION
            or not prompt.strip()
            or len(prompt) > 8000
        ):
            raise DomainError(
                "invalid_media_request", "An image generation route and bounded prompt are required"
            )
        payload, request_id = await self._request(
            spec,
            body={
                "model": route.model,
                "prompt": prompt,
                "n": 1,
                "size": "1024x1024",
                "output_format": "png",
                "quality": "medium",
            },
        )
        try:
            data = payload["data"]
            if len(data) != 1 or not isinstance(data[0].get("b64_json"), str):
                raise ValueError("no inline image")
            content = base64.b64decode(data[0]["b64_json"], validate=True)
            return ProviderResponse(
                request_id=request_id or str(payload.get("created", "")),
                content=content,
                content_type="image/png",
                usage={"images": 1.0},
            )
        except (KeyError, ValueError, TypeError, binascii.Error):
            raise DomainError(
                "invalid_provider_image", "Image generation did not return one inline image"
            ) from None

    async def ocr(self, route: ProviderRoute, content: bytes, filename: str) -> ProviderResponse:
        spec = self.registry.resolve(route)
        if route.capability != Capability.OCR or len(content) > 30 * 1024 * 1024:
            raise DomainError("provider_input_limit", "OCR input exceeds the document limit")
        payload, request_id = await self._request(
            spec,
            files={"file": (filename, content, "application/octet-stream")},
            data={"purpose": "text_recognition"},
        )
        try:
            pages = payload["pages"]
            if payload["page_count"] != len(pages) or not 1 <= len(pages) <= 200:
                raise ValueError("page mismatch")
            output = []
            for index, page in enumerate(pages):
                if (
                    page["index"] != index
                    or page["finish_reason"] != "stop"
                    or not isinstance(page["text"], str)
                ):
                    raise ValueError("incomplete page")
                output.append(
                    OCRPage(page_number=index + 1, text=page["text"], method="deepseek_ocr")
                )
            return ProviderResponse(
                request_id=str(payload.get("id") or request_id),
                pages=tuple(output),
                usage={"pages": float(len(pages))},
            )
        except (KeyError, TypeError, ValueError):
            raise DomainError(
                "invalid_provider_response", "OCR did not return every complete page"
            ) from None


def math_ceil_seconds(value: float) -> float:
    import math

    return float(math.ceil(value))
