from __future__ import annotations

import io
import json
import wave
from dataclasses import replace
from types import SimpleNamespace

import httpx
import pytest

from groupagent.artifacts import ArtifactStore
from groupagent.domain import DomainError
from groupagent.media import MediaHTTPProvider, MediaService, register_media_tools
from groupagent.media.validation import inspect_audio, inspect_image, split_pcm_wav
from groupagent.tools import ToolGateway

from .conftest import png_fixture, wav_fixture
from .test_providers import route


class NoChargeFixtureBudget:
    """Only used with explicit MockTransport, never a production budget substitute."""

    fixture_only = True

    def __init__(self):
        self.keys = {}
        self.states = {}
        self.actual_units = {}

    def reserve(self, actor, key, operation, price_id, estimates, **kwargs):
        fresh = key not in self.keys
        identifier = self.keys.setdefault(key, "fixture-reservation-" + str(len(self.keys)))
        self.states.setdefault(identifier, "reserved")
        return SimpleNamespace(id=identifier, is_new=fresh)

    def begin(self, actor, identifier):
        if self.states[identifier] != "reserved":
            return False
        self.states[identifier] = "inflight"
        return True

    def settle(self, actor, identifier, units, evidence):
        self.states[identifier] = "settled"
        self.actual_units[identifier] = units
        assert evidence
        return SimpleNamespace(overrun=False)

    def unknown(self, actor, identifier, reason):
        if self.states[identifier] != "settled":
            self.states[identifier] = "unknown"


class InlineSyntheticWorker:
    """Unit-fixture decoding only. Separate Docker tests establish actual isolation."""

    def __init__(self):
        self.calls = []

    async def run(self, context, operation, inputs, *, parameters=None, outputs=(), **kwargs):
        self.calls.append(operation)
        if operation == "image_validate":
            info, pixels = inspect_image(inputs[0])
            return info.model_dump(mode="json"), (pixels,)
        if operation == "audio_validate":
            info, normalized = inspect_audio(inputs[0])
            chunks = split_pcm_wav(normalized)
            return {
                **info.model_dump(mode="json"),
                "chunk_indices": list(range(1, len(chunks) + 1)),
            }, (normalized, *chunks, *(b"" for _ in range(6 - len(chunks))))
        if operation == "tts_wrap":
            output = io.BytesIO()
            with wave.open(output, "wb") as stream:
                stream.setnchannels(1)
                stream.setsampwidth(2)
                stream.setframerate(24000)
                stream.writeframes(inputs[0])
            info, content = inspect_audio(output.getvalue())
            return info.model_dump(mode="json"), (content,)
        raise AssertionError("Fixture worker operation not defined")


def vision_response(request):
    return httpx.Response(
        200,
        json={
            "id": "fixture-vision",
            "choices": [{"finish_reason": "stop", "message": {"content": "Score 87"}}],
            "usage": {"prompt_tokens": 400, "completion_tokens": 20},
        },
    )


async def test_media_scope_source_quote_order_and_actual_pixel_http(
    tmp_path, media_context, media_source
):
    budget, worker, captured = NoChargeFixtureBudget(), InlineSyntheticWorker(), []

    def response(request):
        captured.append(request)
        return vision_response(request)

    async with httpx.AsyncClient(transport=httpx.MockTransport(response)) as http:
        with ArtifactStore(tmp_path / "store") as store:
            service = MediaService(
                store,
                worker,
                providers={"deepseek": MediaHTTPProvider("deepseek", "fixture-key", client=http)},
                budget=budget,
            )
            first = await service.ingest_image(media_context, media_source, png_fixture())
            second_source = media_source.model_copy(
                update={
                    "source_message_id": "quoted-parent",
                    "member_id": "other-member",
                    "quote_message_id": "quoted-original",
                    "sequence": 1,
                }
            )
            second = await service.ingest_image(
                media_context, second_source, png_fixture("OTHER SOURCE\nValue 24")
            )
            result = await service.analyze_images(
                media_context, (second, first), "Compare", route("vision")
            )
            assert result.sources == (second_source, media_source) and len(result.image_sha256) == 2
            assert result.budget_receipts and set(budget.states.values()) == {"settled"}
            body = json.loads(captured[0].content)
            assert (
                len(
                    [
                        entry
                        for entry in body["messages"][0]["content"]
                        if entry["type"] == "image_url"
                    ]
                )
                == 2
            )
            assert "quoted-original" in body["messages"][0]["content"][1]["text"]
            with pytest.raises(DomainError) as error:
                await service.analyze_images(media_context, (first,), "Again", route("vision"))
            assert error.value.code == "provider_reconciliation_required" and len(captured) == 1


async def test_video_and_unavailable_sources_never_start_worker(
    tmp_path, media_context, media_source
):
    worker = InlineSyntheticWorker()
    with ArtifactStore(tmp_path / "store") as store:
        service = MediaService(store, worker)
        for source, code in (
            (
                media_source.model_copy(update={"declared_kind": "unsupported_video"}),
                "unsupported_video",
            ),
            (media_source.model_copy(update={"available": False}), "media_unavailable"),
            (media_source.model_copy(update={"group_id": "other"}), "scope_violation"),
        ):
            with pytest.raises(DomainError) as error:
                await service.ingest_image(media_context, source, png_fixture())
            assert error.value.code == code
    assert worker.calls == []


async def test_missing_budget_never_calls_provider(tmp_path, media_context, media_source):
    captured = []
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: captured.append(request))
    ) as http:
        with ArtifactStore(tmp_path / "store") as store:
            service = MediaService(
                store,
                InlineSyntheticWorker(),
                providers={"deepseek": MediaHTTPProvider("deepseek", "fixture-key", client=http)},
            )
            reference = await service.ingest_image(media_context, media_source, png_fixture())
            with pytest.raises(DomainError) as error:
                await service.analyze_images(media_context, (reference,), "Read", route("vision"))
            assert error.value.code == "provider_or_budget_unconfigured"
    assert not captured


async def test_provider_timeout_or_429_keeps_reservation_unknown(
    tmp_path, media_context, media_source
):
    budget = NoChargeFixtureBudget()
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: httpx.Response(429))
    ) as http:
        with ArtifactStore(tmp_path / "store") as store:
            service = MediaService(
                store,
                InlineSyntheticWorker(),
                providers={"deepseek": MediaHTTPProvider("deepseek", "fixture-key", client=http)},
                budget=budget,
            )
            reference = await service.ingest_image(media_context, media_source, png_fixture())
            with pytest.raises(DomainError):
                await service.analyze_images(media_context, (reference,), "Read", route("vision"))
    assert set(budget.states.values()) == {"unknown"}


async def test_speech_artifact_is_real_wav_and_explicit_request_is_required(
    tmp_path, media_context, media_source
):
    budget, captured = NoChargeFixtureBudget(), []

    def response(request):
        captured.append(request)
        return httpx.Response(
            200,
            content=wav_fixture(rate=24000)[44:],
            headers={"Content-Type": "audio/pcm", "x-request-id": "fixture-tts"},
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(response)) as http:
        with ArtifactStore(tmp_path / "store") as store:
            service = MediaService(
                store,
                InlineSyntheticWorker(),
                providers={"deepseek": MediaHTTPProvider("deepseek", "fixture-key", client=http)},
                budget=budget,
            )
            source = media_source.model_copy(
                update={"declared_kind": "text", "origin_kind": "generated"}
            )
            with pytest.raises(DomainError) as error:
                await service.synthesize(media_context, "Read this", route("tts"), source)
            assert error.value.code == "voice_not_requested" and not captured and not budget.keys
            result = await service.synthesize(
                media_context, "Read this", route("tts"), source, explicitly_requested=True
            )
            raw = store.read(service.scope(media_context), result.file_id, result.version_id)
            assert (
                raw.startswith(b"RIFF")
                and result.duration_seconds == 0.4
                and result.budget_receipts
            )
            assert store.validations(service.scope(media_context), result.file_id)[-1].passed


async def test_asr_marks_critical_fields_and_keeps_original_author(
    tmp_path, media_context, media_source
):
    budget = NoChargeFixtureBudget()

    def response(request):
        return httpx.Response(
            200,
            json={
                "request_id": "fixture-asr",
                "text": "请李明明晚八点完成24份报告",
                "duration_ms": 400,
            },
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(response)) as http:
        with ArtifactStore(tmp_path / "store") as store:
            service = MediaService(
                store,
                InlineSyntheticWorker(),
                providers={"deepseek": MediaHTTPProvider("deepseek", "fixture-key", client=http)},
                budget=budget,
            )
            source = media_source.model_copy(
                update={"declared_kind": "audio", "member_id": "original-audio-author"}
            )
            reference = await service.ingest_audio(media_context, source, wav_fixture())
            result = await service.transcribe(
                media_context, reference, route("asr"), critical_terms=("李明",)
            )
            assert result.source.member_id == "original-audio-author"
            assert (
                result.needs_clarification and result.confidence is None and result.budget_receipts
            )
            assert {item.field for item in result.uncertainty} == {"name", "number", "date_or_time"}


async def test_tool_cannot_self_authorize_speech(tmp_path, media_context, media_source):
    with ArtifactStore(tmp_path / "store") as store:
        service = MediaService(store, InlineSyntheticWorker())
        gateway = ToolGateway()
        register_media_tools(gateway, service, {"tts": route("tts")})
        scope = replace(media_context.scope, allowed_tools=frozenset({"speech_synthesize"}))
        gateway.bind_scope(scope)
        context = replace(media_context, scope=scope)
        result = await gateway.invoke(
            "speech_synthesize",
            {"text": "hello", "route_id": "tts", "explicitly_requested": True},
            context,
        )
        assert not result.success and result.error_code == "invalid_input"
        result = await gateway.invoke(
            "speech_synthesize",
            {"text": "hello", "route_id": "tts"},
            replace(context, idempotency_key="another"),
        )
        assert not result.success and result.error_code == "voice_not_requested"


async def test_fixture_budget_cannot_authorize_live_transport(
    tmp_path, media_context, media_source
):
    provider = MediaHTTPProvider("deepseek", "synthetic-never-used-key")
    try:
        with ArtifactStore(tmp_path / "store") as store:
            service = MediaService(
                store,
                InlineSyntheticWorker(),
                providers={"deepseek": provider},
                budget=NoChargeFixtureBudget(),
            )
            reference = await service.ingest_image(media_context, media_source, png_fixture())
            with pytest.raises(DomainError) as error:
                await service.analyze_images(media_context, (reference,), "Read", route("vision"))
            assert error.value.code == "fixture_budget_forbidden"
    finally:
        await provider.aclose()
