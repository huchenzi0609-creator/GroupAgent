from __future__ import annotations

import asyncio
import io
import math
import os
import struct
import wave
from pathlib import Path

import pytest
from PIL import Image, ImageDraw, ImageFont

from groupagent.domain import ActorContext, Role
from groupagent.media import IsolatedMediaWorker, MediaSource
from groupagent.sandbox import DockerSandbox, SandboxLimits
from groupagent.tools import ToolContext, ToolScope


def png_fixture(
    text: str = "SYNTHETIC REPORT\nDeadline 2026-11-20\nScore 87", *, size=(1600, 800)
) -> bytes:
    raster = Image.new("RGB", size, "white")
    configured = os.environ.get("GROUPAGENT_TEST_CJK_FONT") or os.environ.get("GROUPAGENT_PDF_FONT")
    candidates = ([Path(configured)] if configured else []) + [
        Path("/System/Library/Fonts/Supplemental/Arial Unicode.ttf"),
        Path("/usr/share/fonts/truetype/wqy/wqy-zenhei.ttc"),
        Path("/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc"),
    ]
    font_path = next((path for path in candidates if path.is_file()), None)
    if configured and font_path is None:
        pytest.fail("The configured synthetic CJK fixture font is not available")
    if font_path is None and any("\u4e00" <= character <= "\u9fff" for character in text):
        pytest.skip("A CJK font is required to create an actual Chinese OCR fixture")
    font = (
        ImageFont.truetype(str(font_path), 54)
        if font_path is not None
        else ImageFont.load_default(size=54)
    )
    ImageDraw.Draw(raster).multiline_text((70, 70), text, fill="black", font=font, spacing=30)
    output = io.BytesIO()
    raster.save(output, "PNG")
    return output.getvalue()


def wav_fixture(*, rate=16000, seconds=0.4, channels=1, frequency=440) -> bytes:
    output = io.BytesIO()
    with wave.open(output, "wb") as stream:
        stream.setnchannels(channels)
        stream.setsampwidth(2)
        stream.setframerate(rate)
        data = b"".join(
            struct.pack("<h", int(10000 * math.sin(2 * math.pi * frequency * index / rate)))
            * channels
            for index in range(int(rate * seconds))
        )
        stream.writeframes(data)
    return output.getvalue()


@pytest.fixture
def media_context(tmp_path):
    scope = ToolScope(
        "media-group",
        "media-work",
        ActorContext(group_id="media-group", member_id="media-member", role=Role.OWNER),
        tmp_path.resolve(),
        frozenset(),
        "synthetic-media-trace",
    )
    return ToolContext(scope, "media-call", "media-key", asyncio.Event())


@pytest.fixture
def media_source():
    return MediaSource(
        group_id="media-group",
        work_id="media-work",
        source_event_id="synthetic-event",
        source_message_id="synthetic-message",
        member_id="media-member",
        attachment_id="synthetic-attachment",
        declared_kind="image",
    )


@pytest.fixture
def isolated_worker():
    image = os.environ.get("GROUPAGENT_WORKER_IMAGE")
    if not image:
        pytest.skip("Requires the reviewed immutable Docker worker image")
    return IsolatedMediaWorker(
        DockerSandbox(
            image,
            docker_host=os.environ.get("GROUPAGENT_DOCKER_HOST"),
            limits=SandboxLimits(memory_mb=1024, workspace_mb=256, temp_mb=256, pids=128),
        )
    )
