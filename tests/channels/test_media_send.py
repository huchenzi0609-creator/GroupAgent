from __future__ import annotations

import asyncio
import io
import json
import wave

import httpx
import pytest
from PIL import Image

from groupagent.channels import AgentWechatAdapter
from groupagent.domain import DeliveryState, DomainError

from .conftest import ACCOUNT, GROUP, ROOM, TOKEN


def image_bytes():
    output = io.BytesIO()
    Image.new("RGB", (32, 32), "blue").save(output, "PNG")
    return output.getvalue()


def audio_bytes():
    output = io.BytesIO()
    with wave.open(output, "wb") as stream:
        stream.setnchannels(1)
        stream.setsampwidth(2)
        stream.setframerate(16000)
        stream.writeframes(b"\x10\x00" * 16000)
    return output.getvalue()


def capabilities(*, voice=True, image_limit=26214383, voice_limit=26214383):
    return {
        "nativeRuntime": True,
        "mediaUploadMaxBytes": 536870912,
        "mediaSendMaxBytes": {"file": 536870912, "image": image_limit, "voice": voice_limit},
        "mediaKinds": ["image", "file", "voice", "video"] if voice else ["image", "file", "video"],
        "quotedTextSend": True,
    }


@pytest.mark.parametrize(
    "kind,name,content,duration",
    [("image", "picture.png", image_bytes(), None), ("voice", "speech.wav", audio_bytes(), 1.0)],
)
async def test_real_byte_staging_and_typed_native_media_send(
    adapter, daemon, kind, name, content, duration
):
    daemon.upload_name, daemon.upload_content = name, content
    receipt = await adapter.send_media(
        GROUP, name, content, kind=kind, idempotency_key="media-intent", duration_seconds=duration
    )
    assert (
        receipt.job_id == "job-1" and receipt.wire_kind == kind and receipt.requested_kind == kind
    )
    assert receipt.delivery_state == DeliveryState.PENDING and receipt.recipient_read is None
    request = next(request for request in daemon.requests if request.url.path.endswith("/sends"))
    body = json.loads(request.content)
    assert body["kind"] == kind and body["threadId"] == ROOM and body["sourceId"] == "source-1"
    assert body["durationMs"] == (1000 if kind == "voice" else None)
    repeated = await adapter.send_media(
        GROUP,
        name,
        content,
        kind=kind,
        idempotency_key="media-intent",
        duration_seconds=1 if kind == "voice" else None,
    )
    assert repeated.job_id == receipt.job_id and daemon.upload_calls == daemon.send_calls == 1


async def test_voice_file_fallback_is_explicit_and_not_a_voice_bubble(adapter, daemon):
    content = audio_bytes()
    daemon.upload_name, daemon.upload_content = "speech.wav", content
    daemon.failure = lambda request: (
        httpx.Response(200, json=capabilities(voice=False))
        if request.url.path.endswith("/capabilities")
        else None
    )
    with pytest.raises(DomainError) as error:
        await adapter.send_media(
            GROUP,
            "speech.wav",
            content,
            kind="voice",
            idempotency_key="voice-intent",
            duration_seconds=1,
        )
    assert error.value.code == "native_voice_unavailable" and daemon.upload_calls == 0
    receipt = await adapter.send_media(
        GROUP,
        "speech.wav",
        content,
        kind="voice",
        idempotency_key="voice-intent",
        duration_seconds=1,
        allow_file_fallback=True,
    )
    assert receipt.requested_kind == "voice" and receipt.wire_kind == "file"
    assert receipt.fallback_reason == "native_voice_unavailable"
    assert json.loads(daemon.requests[-1].content)["kind"] == "file"


async def test_long_voice_requires_explicit_file_fallback(adapter, daemon):
    content = audio_bytes()
    daemon.upload_name, daemon.upload_content = "speech.wav", content
    with pytest.raises(DomainError) as error:
        await adapter.send_media(
            GROUP, "speech.wav", content, kind="voice", idempotency_key="long", duration_seconds=61
        )
    assert error.value.code == "native_voice_duration_limit" and daemon.upload_calls == 0
    receipt = await adapter.send_media(
        GROUP,
        "speech.wav",
        content,
        kind="voice",
        idempotency_key="long",
        duration_seconds=61,
        allow_file_fallback=True,
    )
    assert receipt.wire_kind == "file" and receipt.fallback_reason == "native_voice_duration_limit"


async def test_capability_byte_limit_refuses_staging(adapter, daemon):
    daemon.failure = lambda request: (
        httpx.Response(200, json=capabilities(image_limit=20))
        if request.url.path.endswith("/capabilities")
        else None
    )
    with pytest.raises(DomainError) as error:
        await adapter.send_media(
            GROUP, "picture.png", image_bytes(), kind="image", idempotency_key="image"
        )
    assert error.value.code == "attachment_too_large" and daemon.upload_calls == 0


@pytest.mark.parametrize(
    "kind,name,content,duration,code",
    [
        ("image", "picture.png", b"not an image", None, "media_format_mismatch"),
        ("voice", "speech.wav", audio_bytes(), None, "media_metadata_required"),
        ("voice", "speech.wav", audio_bytes(), float("nan"), "media_metadata_required"),
        ("voice", "speech.wav", audio_bytes(), True, "media_metadata_required"),
        ("image", "video.mp4", b"video", None, "unsupported_video"),
        ("image", "picture.png", b"\0\0\0\x18ftypisom", None, "unsupported_video"),
    ],
)
async def test_invalid_media_never_reaches_the_channel(
    adapter, daemon, kind, name, content, duration, code
):
    with pytest.raises(DomainError) as error:
        await adapter.send_media(
            GROUP, name, content, kind=kind, idempotency_key="intent", duration_seconds=duration
        )
    assert error.value.code == code and not daemon.requests


async def test_changed_media_same_key_conflicts_and_restart_looks_up(adapter, daemon, config):
    content = image_bytes()
    daemon.upload_name, daemon.upload_content = "picture.png", content
    first = await adapter.send_media(
        GROUP, "picture.png", content, kind="image", idempotency_key="media-key"
    )
    with pytest.raises(DomainError) as error:
        await adapter.send_media(
            GROUP, "picture.png", content + b"different", kind="image", idempotency_key="media-key"
        )
    assert error.value.code == "idempotency_conflict"
    async with httpx.AsyncClient(transport=httpx.MockTransport(daemon)) as client:
        restarted = AgentWechatAdapter(config, TOKEN, client=client)
        found = await restarted.lookup_delivery(GROUP, "media-key")
        assert (
            found.job_id == first.job_id and found.wire_kind is None
        )  # No fabricated restart mode.
        with pytest.raises(DomainError) as error:
            await restarted.send_media(
                GROUP, "picture.png", content, kind="image", idempotency_key="media-key"
            )
        assert error.value.code == "delivery_requires_reconciliation"
    assert daemon.upload_calls == daemon.send_calls == 1


async def test_lost_media_send_response_reconciles_with_original_key(adapter, daemon):
    content = image_bytes()
    daemon.upload_name, daemon.upload_content = "picture.png", content

    def lost(request):
        if request.url.path.endswith("/sends"):
            key = request.headers["Idempotency-Key"]
            daemon.sent[key] = {
                "id": "accepted-job",
                "accountId": ACCOUNT,
                "threadId": ROOM,
                "kind": "send",
                "state": "accepted",
                "serverMessageId": "18446744073709551001",
                "reason": None,
            }
            raise httpx.ReadTimeout("Synthetic lost response", request=request)
        return None

    daemon.failure = lost
    with pytest.raises(DomainError) as error:
        await adapter.send_media(
            GROUP, "picture.png", content, kind="image", idempotency_key="lost-key"
        )
    assert error.value.code == "delivery_unknown" and error.value.effect_state == "unknown"
    daemon.failure = None
    receipt = await adapter.lookup_delivery(GROUP, "lost-key")
    assert receipt.accepted and receipt.wire_kind == "image" and receipt.recipient_read is None
    assert daemon.upload_calls == 1


async def test_concurrent_same_media_key_only_stages_once(adapter, daemon):
    content = image_bytes()
    daemon.upload_name, daemon.upload_content = "picture.png", content
    results = await asyncio.gather(
        *(
            adapter.send_media(
                GROUP, "picture.png", content, kind="image", idempotency_key="concurrent"
            )
            for _ in range(2)
        )
    )
    assert results[0].job_id == results[1].job_id
    assert daemon.upload_calls == daemon.send_calls == 1


async def test_foreign_group_and_legacy_file_route_remain_restricted(adapter, daemon):
    with pytest.raises(DomainError) as error:
        await adapter.send_media(
            "other-group", "picture.png", image_bytes(), kind="image", idempotency_key="foreign"
        )
    assert error.value.code == "scope_denied" and not daemon.requests
    with pytest.raises(DomainError) as error:
        await adapter.send_file(GROUP, "speech.wav", audio_bytes(), "old-file-route")
    assert error.value.code == "unsupported_attachment" and not daemon.requests
