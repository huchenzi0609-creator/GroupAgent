from __future__ import annotations

import io

import pytest
from PIL import Image

from groupagent.domain import DomainError
from groupagent.media.service import critical_uncertainties
from groupagent.media.validation import inspect_audio, inspect_image, reject_video

from .conftest import wav_fixture


@pytest.mark.parametrize("case", range(30))
def test_thirty_synthetic_image_decodes(case):
    formats = ("PNG", "JPEG", "WEBP", "GIF")
    format_name = formats[case % 4]
    raster = Image.new("RGB", (100 + case * 5, 70 + case * 3), (case * 7, 80, 170))
    output = io.BytesIO()
    raster.save(output, format_name)
    info, view = inspect_image(output.getvalue())
    assert info.width == raster.width and info.height == raster.height
    assert info.static_first_frame == (format_name == "GIF")
    with Image.open(io.BytesIO(view)) as actual:
        assert actual.format == "PNG" and actual.size == raster.size


def test_gif_first_frame_is_preserved_and_marked_static():
    images = [Image.new("RGB", (80, 80), color) for color in ("red", "green", "blue")]
    output = io.BytesIO()
    images[0].save(output, "GIF", save_all=True, append_images=images[1:], duration=100, loop=0)
    info, view = inspect_image(output.getvalue())
    assert info.static_first_frame and info.frame_count == 3
    with Image.open(io.BytesIO(view)) as image:
        assert image.getpixel((30, 30)) == (255, 0, 0)


@pytest.mark.parametrize("case", range(20))
def test_twenty_synthetic_pcm_audio_decodes(case):
    # Tones test real container/sample validation, not speech recognition accuracy.
    rate = (8000, 16000, 24000, 48000)[case % 4]
    data = wav_fixture(rate=rate, channels=1 + case % 2, frequency=200 + case * 20)
    info, result = inspect_audio(data)
    assert info.sample_rate == rate and info.channels == 1 + case % 2
    assert 0.39 <= info.duration_seconds <= 0.41 and result == data


def test_corrupt_images_and_truncated_audio_fail():
    with pytest.raises(DomainError):
        inspect_image(b"not an image")
    with pytest.raises(DomainError) as error:
        inspect_audio(wav_fixture()[:-40])
    assert error.value.code == "truncated_audio"


@pytest.mark.parametrize(
    "content",
    [
        b"\x00\x00\x00\x20ftypisom" + b"x" * 30,
        b"RIFFxxxxAVI " + b"x" * 30,
        b"\x1aE\xdf\xa3" + b"x" * 20,
        b"OggS" + b"theora",
    ],
)
def test_video_signature_rejected_before_decoder(content):
    with pytest.raises(DomainError) as error:
        reject_video(content)
    assert error.value.code == "unsupported_video"


def test_low_or_unknown_confidence_critical_fields_require_clarification():
    text = "请张三明晚八点完成24份报告"
    unknown = critical_uncertainties(text, None, ("张三",))
    assert {entry.field for entry in unknown} == {"name", "number", "date_or_time"}
    assert all(entry.reason == "confidence_unavailable" for entry in unknown)
    assert critical_uncertainties(text, 0.5, ("张三",))
    assert not critical_uncertainties(text, 0.95, ("张三",))
    assert not critical_uncertainties("今天的天气如何", 0.95, ())
