"""Real bounded decoders. Run inside the isolated media worker for untrusted bytes."""

from __future__ import annotations

import csv
import hashlib
import io
import json
import math
import re
import shutil
import subprocess
import tempfile
import time
import warnings
import wave
from pathlib import Path
from typing import Literal

from PIL import Image, ImageOps, UnidentifiedImageError

from groupagent.domain import DomainError

from .models import AudioInfo, ImageInfo, OCRPage, OCRResult, OCRWord

IMAGE_BYTES = 15 * 1024 * 1024
IMAGE_PIXELS = 40_000_000
AUDIO_BYTES = 25 * 1024 * 1024
AUDIO_SECONDS = 600
_VIDEO_BRANDS = {b"isom", b"mp41", b"mp42", b"avc1", b"qt  ", b"M4V ", b"3gp4", b"3gp5"}


def reject_video(content: bytes) -> None:
    if (
        content.startswith(b"\x1aE\xdf\xa3")
        or content[:3] == b"FLV"
        or (content.startswith(b"RIFF") and content[8:12] == b"AVI ")
        or (content[4:8] == b"ftyp" and content[8:12] in _VIDEO_BRANDS)
        or (content.startswith(b"OggS") and b"theora" in content[:65536])
    ):
        raise DomainError("unsupported_video", "Video bytes are not processed")


def inspect_image(
    content: bytes, *, max_bytes: int = IMAGE_BYTES, max_pixels: int = IMAGE_PIXELS
) -> tuple[ImageInfo, bytes]:
    if (
        type(max_bytes) is not int
        or not 0 < max_bytes <= IMAGE_BYTES
        or type(max_pixels) is not int
        or not 0 < max_pixels <= IMAGE_PIXELS
    ):
        raise ValueError("Image limits must be within 15 MiB and 40 megapixels")
    reject_video(content)
    if not content or len(content) > max_bytes:
        raise DomainError("image_size_limit", "Image exceeds its configured byte limit")
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("error", Image.DecompressionBombWarning)
            with Image.open(io.BytesIO(content)) as original:
                format_name = (original.format or "").lower()
                if format_name not in {"jpeg", "png", "webp", "gif"}:
                    raise DomainError(
                        "unsupported_image", "Supported images are JPEG, PNG, WebP and GIF"
                    )
                if original.width * original.height > max_pixels:
                    raise DomainError(
                        "image_pixel_limit", "Image exceeds its configured pixel limit"
                    )
                frames = int(getattr(original, "n_frames", 1))
                original.verify()
            with Image.open(io.BytesIO(content)) as original:
                original.seek(0)
                first = ImageOps.exif_transpose(original)
                alpha_composited = "A" in first.getbands() or "transparency" in first.info
                if alpha_composited:
                    first = Image.alpha_composite(
                        Image.new("RGBA", first.size, "white"), first.convert("RGBA")
                    )
                first = first.convert("RGB")
                first.load()
                width, height = first.size
                pixels = hashlib.sha256(first.tobytes()).hexdigest()
                preview = io.BytesIO()
                first.save(preview, format="PNG", optimize=True)
                data = preview.getvalue()
        if len(data) > 25 * 1024 * 1024:
            raise DomainError("image_output_limit", "Decoded static view exceeds the output limit")
        return ImageInfo.model_validate(
            {
                "format": format_name,
                "mime_type": "image/" + ("jpeg" if format_name == "jpeg" else format_name),
                "width": width,
                "height": height,
                "byte_length": len(content),
                "source_sha256": hashlib.sha256(content).hexdigest(),
                "pixel_sha256": pixels,
                "static_first_frame": format_name == "gif",
                "frame_count": frames,
                "preview_sha256": hashlib.sha256(data).hexdigest(),
                "alpha_composited_on_white": alpha_composited,
            }
        ), data
    except DomainError:
        raise
    except (
        OSError,
        ValueError,
        UnidentifiedImageError,
        Image.DecompressionBombError,
        Image.DecompressionBombWarning,
    ):
        raise DomainError("invalid_image", "Image cannot be fully decoded") from None


def _run(arguments: list[str], *, timeout: float = 60) -> bytes:
    try:
        result = subprocess.run(
            arguments, stdin=subprocess.DEVNULL, capture_output=True, timeout=timeout, check=False
        )
    except subprocess.TimeoutExpired:
        raise DomainError("media_timeout", "Media operation timed out") from None
    except OSError:
        raise DomainError("media_dependency_missing", "Media executable is unavailable") from None
    if result.returncode:
        raise DomainError("media_processing_failed", "Media tool could not process the input")
    if len(result.stdout) > 4 * 1024 * 1024:
        raise DomainError("media_output_limit", "Media metadata exceeds its limit")
    return result.stdout


def _wave_info(content: bytes) -> tuple[int, int, int, float]:
    try:
        with wave.open(io.BytesIO(content), "rb") as stream:
            rate, channels, width, frames = (
                stream.getframerate(),
                stream.getnchannels(),
                stream.getsampwidth(),
                stream.getnframes(),
            )
            if (
                stream.getcomptype() != "NONE"
                or width != 2
                or channels not in {1, 2}
                or not 8000 <= rate <= 96000
            ):
                raise DomainError("unsupported_audio", "Expected mono/stereo 16-bit PCM WAV")
            if frames <= 0 or frames / rate > AUDIO_SECONDS:
                raise DomainError("audio_duration_limit", "Audio must be at most ten minutes")
            data = stream.readframes(frames)
            if len(data) != frames * channels * width:
                raise DomainError("truncated_audio", "WAV frame data is incomplete")
            # RIFF size and frame count must describe the actual complete body.
            if int.from_bytes(content[4:8], "little") + 8 > len(content):
                raise DomainError("truncated_audio", "WAV container is incomplete")
            return rate, channels, frames, frames / rate
    except DomainError:
        raise
    except (wave.Error, EOFError, OSError, ZeroDivisionError):
        raise DomainError("invalid_audio", "Audio cannot be decoded as PCM WAV") from None


def inspect_audio(content: bytes, *, normalize: bool = False) -> tuple[AudioInfo, bytes]:
    reject_video(content)
    if not content or len(content) > AUDIO_BYTES:
        raise DomainError("audio_size_limit", "Audio must be nonempty and at most 25 MiB")
    is_wave = content.startswith(b"RIFF") and content[8:12] == b"WAVE"
    if is_wave:
        rate, channels, _, duration = _wave_info(content)
        normalized = content
        codec, format_name = "pcm_s16le", "wav"
        if not normalize or (rate == 16000 and channels == 1):
            return AudioInfo(
                format="wav",
                codec=codec,
                sample_rate=rate,
                channels=channels,
                duration_seconds=duration,
                byte_length=len(content),
                sha256=hashlib.sha256(content).hexdigest(),
                normalized_sha256=hashlib.sha256(content).hexdigest(),
            ), content
    elif not (
        content.startswith((b"OggS", b"fLaC", b"ID3"))
        or content[:2] in {b"\xff\xfb", b"\xff\xf3", b"\xff\xf2", b"\xff\xf1", b"\xff\xf9"}
        or (content[4:8] == b"ftyp" and content[8:12] in {b"M4A ", b"M4B "})
    ):
        raise DomainError("unsupported_audio", "Unknown or unsupported audio container")
    ffprobe, ffmpeg = shutil.which("ffprobe"), shutil.which("ffmpeg")
    if not ffprobe or not ffmpeg:
        raise DomainError(
            "media_dependency_missing", "Audio validation requires FFmpeg and FFprobe"
        )
    with tempfile.TemporaryDirectory(prefix="ga-audio-") as directory:
        source, target = Path(directory) / "source.audio", Path(directory) / "normalized.wav"
        source.write_bytes(content)
        try:
            metadata = json.loads(
                _run(
                    [
                        ffprobe,
                        "-v",
                        "error",
                        "-show_streams",
                        "-show_format",
                        "-of",
                        "json",
                        str(source),
                    ]
                )
            )
            streams = metadata["streams"]
            if any(stream.get("codec_type") == "video" for stream in streams):
                raise DomainError(
                    "unsupported_video", "Video streams are never decoded or transcribed"
                )
            if len(streams) != 1 or streams[0].get("codec_type") != "audio":
                raise DomainError("unsupported_audio", "Exactly one audio stream is required")
            duration = float(metadata["format"]["duration"])
            rate, channels = int(streams[0]["sample_rate"]), int(streams[0]["channels"])
            codec = str(streams[0]["codec_name"])
            format_name = (
                "ogg"
                if content.startswith(b"OggS")
                else "flac"
                if content.startswith(b"fLaC")
                else "m4a"
                if content[4:8] == b"ftyp"
                else "mp3"
            )
            if (
                not math.isfinite(duration)
                or not 0 < duration <= AUDIO_SECONDS
                or channels not in {1, 2}
            ):
                raise DomainError(
                    "audio_duration_limit", "Audio is too long or has unsupported channels"
                )
        except DomainError:
            raise
        except (ValueError, KeyError, TypeError):
            raise DomainError("invalid_audio", "Audio metadata is incomplete") from None
        # Decode the entire audio to catch truncated/corrupt packets; never map a video stream.
        _run(
            [
                ffmpeg,
                "-nostdin",
                "-v",
                "error",
                "-xerror",
                "-i",
                str(source),
                "-map",
                "0:a:0",
                "-vn",
                "-sn",
                "-dn",
                "-ac",
                "1",
                "-ar",
                "16000",
                "-c:a",
                "pcm_s16le",
                "-f",
                "wav",
                str(target),
            ]
        )
        normalized = target.read_bytes()
        _, _, _, decoded_duration = _wave_info(normalized)
        if abs(decoded_duration - duration) > max(1.0, duration * 0.02):
            raise DomainError(
                "truncated_audio", "Decoded audio duration disagrees with the container"
            )
        return AudioInfo(
            format=format_name,
            codec=codec,
            sample_rate=rate,
            channels=channels,
            duration_seconds=decoded_duration,
            byte_length=len(content),
            sha256=hashlib.sha256(content).hexdigest(),
            normalized_sha256=hashlib.sha256(normalized).hexdigest(),
        ), normalized


def split_pcm_wav(content: bytes, *, seconds: int = 110) -> tuple[bytes, ...]:
    _wave_info(content)
    chunks = []
    with wave.open(io.BytesIO(content), "rb") as source:
        while frames := source.readframes(source.getframerate() * seconds):
            result = io.BytesIO()
            with wave.open(result, "wb") as target:
                target.setnchannels(source.getnchannels())
                target.setsampwidth(2)
                target.setframerate(source.getframerate())
                target.writeframes(frames)
            chunks.append(result.getvalue())
    return tuple(chunks)


def _tesseract_text(output: bytes) -> tuple[str, tuple[OCRWord, ...]]:
    words = []
    lines: dict[tuple[str, ...], list[str]] = {}
    try:
        reader = csv.DictReader(io.StringIO(output.decode("utf-8")), delimiter="\t")
        required = {"text", "conf", "left", "top", "width", "height"}
        required.update(("page_num", "block_num", "par_num", "line_num"))
        if not required <= set(reader.fieldnames or ()):
            raise ValueError("Missing TSV columns")
        for row in reader:
            text = (row.get("text") or "").strip()
            confidence = float(row["conf"])
            if not math.isfinite(confidence):
                raise ValueError("Invalid confidence")
            if not text or confidence < 0:
                continue
            x, y, width, height = (int(row[key]) for key in ("left", "top", "width", "height"))
            words.append(
                OCRWord(
                    text=text,
                    confidence=min(1, confidence / 100),
                    box=(x, y, x + width, y + height),
                )
            )
            key = tuple(row[field] for field in ("page_num", "block_num", "par_num", "line_num"))
            lines.setdefault(key, []).append(text)
    except (KeyError, TypeError, ValueError, UnicodeError):
        raise DomainError("invalid_ocr_response", "Tesseract output is malformed") from None
    text = "\n".join(" ".join(line) for line in lines.values())
    return text, tuple(words)


def ocr_image(
    content: bytes,
    *,
    languages: str = "eng+chi_sim",
    page_number: int = 1,
    max_bytes: int = IMAGE_BYTES,
    max_pixels: int = IMAGE_PIXELS,
) -> OCRPage:
    if not re.fullmatch(r"(?:eng|chi_sim)(?:\+(?:eng|chi_sim))?", languages):
        raise DomainError(
            "unsupported_ocr_language",
            "Only configured English and simplified Chinese OCR are enabled",
        )
    info, raster = inspect_image(content, max_bytes=max_bytes, max_pixels=max_pixels)
    executable = shutil.which("tesseract")
    if not executable:
        raise DomainError("media_dependency_missing", "Tesseract is not installed in the worker")
    attempts: list[Literal[3, 4, 6, 11]] = []
    modes: tuple[Literal[3, 4, 6, 11], ...] = (6, 3, 11)
    preprocessing: Literal["none", "dark_ink_threshold_32"] = "none"
    first_symbols: (
        tuple[
            str,
            tuple[OCRWord, ...],
            Literal[3, 4, 6, 11],
            Literal["none", "dark_ink_threshold_32"],
        ]
        | None
    ) = None
    with tempfile.TemporaryDirectory(prefix="ga-ocr-") as directory:
        source = Path(directory) / "page.png"
        source.write_bytes(raster)
        deadline = time.monotonic() + 60
        # PSM 6: uniform block; 3: automatic layout; 11: sparse text. Empty or
        # punctuation-only results permit another attempt: arrows can be read
        # as braces without any body text. Keep the first symbol result if no
        # attempt finds letters/numbers, and never replace recognized words
        # with a preferred transcript or manufacture confidence. A dark-ink view can
        # separate black glyphs from colored diagram borders. It is reported,
        # has the same dimensions/coordinates, and is not a new source artifact.
        # https://tesseract-ocr.github.io/tessdoc/ImproveQuality.html#page-segmentation-method
        for index, mode in enumerate(modes):
            if index == 2:
                source = Path(directory) / "dark-ink.png"
                with Image.open(io.BytesIO(raster)) as image:
                    ImageOps.grayscale(image).point([0] * 32 + [255] * 224).save(source, "PNG")
                preprocessing = "dark_ink_threshold_32"
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise DomainError("media_timeout", "Media operation timed out")
            output = _run(
                [executable, str(source), "stdout", "-l", languages, "--psm", str(mode), "tsv"],
                timeout=remaining,
            )
            attempts.append(mode)
            text, words = _tesseract_text(output)
            if any(character.isalnum() for character in text):
                break
            if text and first_symbols is None:
                first_symbols = text, words, mode, preprocessing
        else:
            if first_symbols is not None:
                text, words, mode, preprocessing = first_symbols
    return OCRPage(
        page_number=page_number,
        text=text,
        method="tesseract",
        words=words,
        confidence=sum(word.confidence for word in words) / len(words) if words else None,
        segmentation_mode=mode,
        attempted_segmentation_modes=tuple(attempts),
        preprocessing=preprocessing,
        width_pixels=info.width,
        height_pixels=info.height,
    )


def ocr_pdf(
    content: bytes,
    *,
    languages: str = "eng+chi_sim",
    page_indices: tuple[int, ...] | None = None,
    force_ocr: bool = False,
) -> OCRResult:
    from groupagent.artifacts.formats import open_pdf

    reader = open_pdf(content)
    indices = page_indices if page_indices is not None else tuple(range(len(reader.pages)))
    if any(type(index) is not int or not 0 <= index < len(reader.pages) for index in indices):
        raise DomainError("invalid_page_selection", "OCR page is outside the document")
    executable = shutil.which("pdftoppm")
    pages = []
    with tempfile.TemporaryDirectory(prefix="ga-pdf-ocr-") as directory:
        source = Path(directory) / "source.pdf"
        source.write_bytes(content)
        for index in indices:
            page = reader.pages[index]
            text = (page.extract_text() or "").strip()
            if not force_ocr and text and (len(re.sub(r"\s+", "", text)) >= 40 or not page.images):
                pages.append(
                    OCRPage(
                        page_number=index + 1,
                        text=text,
                        method="embedded_text",
                        width_points=float(page.mediabox.width),
                        height_points=float(page.mediabox.height),
                    )
                )
                continue
            if not executable:
                raise DomainError("media_dependency_missing", "PDF OCR requires Poppler")
            prefix = Path(directory) / f"page-{index + 1}"
            _run(
                [
                    executable,
                    "-f",
                    str(index + 1),
                    "-l",
                    str(index + 1),
                    "-singlefile",
                    "-scale-to",
                    "2200",
                    "-png",
                    str(source),
                    str(prefix),
                ]
            )
            recognized = ocr_image(
                prefix.with_suffix(".png").read_bytes(),
                languages=languages,
                page_number=index + 1,
            )
            width, height = float(page.mediabox.width), float(page.mediabox.height)
            assert recognized.width_pixels and recognized.height_pixels
            coordinates_supported = tuple(page.mediabox.lower_left) == (0, 0) and tuple(
                page.cropbox
            ) == tuple(page.mediabox)
            converted = tuple(
                word.model_copy(
                    update={
                        "pdf_box": _pdf_box(
                            word.box,
                            width,
                            height,
                            recognized.width_pixels,
                            recognized.height_pixels,
                            page.rotation % 360,
                        )
                        if coordinates_supported
                        else None
                    }
                )
                for word in recognized.words
            )
            pages.append(
                recognized.model_copy(
                    update={"words": converted, "width_points": width, "height_points": height}
                )
            )
    return OCRResult(
        pages=tuple(pages),
        source_sha256=hashlib.sha256(content).hexdigest(),
        engine="pypdf+tesseract",
    )


def _pdf_box(
    box: tuple[int, int, int, int],
    width: float,
    height: float,
    pixel_width: int,
    pixel_height: int,
    rotation: int,
) -> tuple[float, float, float, float]:
    def point(x: float, y: float) -> tuple[float, float]:
        px, py = x / pixel_width, y / pixel_height
        if rotation == 90:
            return py * width, px * height
        if rotation == 180:
            return (1 - px) * width, py * height
        if rotation == 270:
            return (1 - py) * width, (1 - px) * height
        return px * width, (1 - py) * height

    a, b = point(box[0], box[1]), point(box[2], box[3])
    return min(a[0], b[0]), min(a[1], b[1]), max(a[0], b[0]), max(a[1], b[1])
