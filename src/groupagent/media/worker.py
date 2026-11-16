"""Credential-free media/document worker entry point, executed inside SandboxRunner."""

from __future__ import annotations

import io
import json
import os
import sys
import wave
from array import array
from pathlib import Path
from typing import Any

from groupagent.artifacts.models import ArtifactError
from groupagent.domain import DomainError

from .validation import inspect_audio, inspect_image, ocr_image, ocr_pdf, split_pcm_wav


def local_path(value: str) -> Path:
    if (
        not isinstance(value, str)
        or not value
        or "/" in value
        or "\\" in value
        or value in {".", ".."}
        or "\x00" in value
    ):
        raise DomainError("scope_violation", "Worker filenames must be simple relative names")
    path = Path.cwd() / value
    if path.is_symlink():
        raise DomainError("scope_violation", "Worker symlinks are not accepted")
    return path


def _input(request: dict[str, Any], index: int = 0) -> bytes:
    path = local_path(request["inputs"][index])
    max_bytes = request.get("parameters", {}).get("max_import_bytes", 30 * 1024 * 1024)
    if type(max_bytes) is not int or not 0 < max_bytes <= 30 * 1024 * 1024:
        raise DomainError("invalid_media_request", "Worker input byte limit is invalid")
    if not path.is_file() or path.stat().st_size > max_bytes:
        raise DomainError("media_size_limit", "Worker input exceeds its configured byte limit")
    return path.read_bytes()


def execute(request: dict[str, Any]) -> dict[str, Any]:
    operation, outputs, params = (
        request["operation"],
        request["outputs"],
        request.get("parameters", {}),
    )
    if operation == "image_validate":
        metadata, preview = inspect_image(
            _input(request),
            max_bytes=params.get("max_bytes", 15 * 1024 * 1024),
            max_pixels=params.get("max_pixels", 40_000_000),
        )
        local_path(outputs[0]).write_bytes(preview)
        return metadata.model_dump(mode="json")
    if operation == "audio_validate":
        audio_metadata, normalized = inspect_audio(_input(request), normalize=True)
        local_path(outputs[0]).write_bytes(normalized)
        chunks = split_pcm_wav(normalized)
        if len(chunks) > len(outputs) - 1:
            raise DomainError(
                "audio_duration_limit", "Audio needs more chunks than the bounded request allows"
            )
        names = []
        for name, chunk in zip(outputs[1:], chunks, strict=False):
            local_path(name).write_bytes(chunk)
            names.append(name)
        return {**audio_metadata.model_dump(mode="json"), "chunks": names}
    if operation == "tts_wrap":
        pcm = _input(request)
        if not pcm or len(pcm) % 2 or len(pcm) > 24_000 * 2 * 60:
            raise DomainError("invalid_audio", "TTS must return complete short PCM16 samples")
        samples = array("h", pcm)
        if len(pcm) < 4800 or max(abs(sample) for sample in samples) < 10:
            raise DomainError("inaudible_tts", "Speech output is silent or too short to be usable")
        output = io.BytesIO()
        with wave.open(output, "wb") as audio:
            audio.setnchannels(1)
            audio.setsampwidth(2)
            audio.setframerate(24000)
            audio.writeframes(pcm)
        speech_metadata, _ = inspect_audio(output.getvalue())
        local_path(outputs[0]).write_bytes(output.getvalue())
        return speech_metadata.model_dump(mode="json")
    if operation == "ocr_image":
        result = ocr_image(
            _input(request),
            languages=params.get("languages", "eng+chi_sim"),
            max_bytes=params.get("max_bytes", 15 * 1024 * 1024),
            max_pixels=params.get("max_pixels", 40_000_000),
        )
        return {"pages": [result.model_dump(mode="json")]}
    if operation == "ocr_pdf":
        return ocr_pdf(
            _input(request),
            languages=params.get("languages", "eng+chi_sim"),
            force_ocr=params.get("force_ocr", False),
        ).model_dump(mode="json")
    if operation == "chart_render":
        from groupagent.artifacts.visuals import chart_png
        from groupagent.artifacts.worker_operations import _chart

        content = chart_png(_chart(params["chart"]))
        metadata, _ = inspect_image(content)
        local_path(outputs[0]).write_bytes(content)
        return metadata.model_dump(mode="json")
    if operation.startswith("document_"):
        from groupagent.artifacts.worker_operations import document_operation

        return document_operation(operation, request, _input, local_path)
    if operation == "browser_snapshot":
        from groupagent.research.browser_worker import browser_snapshot

        return browser_snapshot(_input(request), params, local_path(outputs[0]))
    raise DomainError("unsupported_worker_operation", "Worker operation is not registered")


def main() -> None:
    if len(sys.argv) != 3:
        raise SystemExit(2)
    result_path = local_path(sys.argv[2])
    request: dict[str, Any] = {}
    try:
        path = local_path(sys.argv[1])
        if path.stat().st_size > 2 * 1024 * 1024:
            raise DomainError("request_too_large", "Worker request exceeds its limit")
        request = json.loads(path.read_bytes())
        if set(request) - {"operation", "inputs", "outputs", "parameters"}:
            raise DomainError("invalid_worker_request", "Worker request contains unknown fields")
        for name in request["outputs"]:
            target = local_path(name)
            with target.open("xb"):
                pass
        result = {"ok": True, "result": execute(request)}
    except (DomainError, ArtifactError) as error:
        result = {"ok": False, "error_code": error.code}
    except Exception:
        result = {"ok": False, "error_code": "invalid_worker_input"}
    encoded = json.dumps(result, ensure_ascii=False, allow_nan=False).encode()
    if len(encoded) > 4 * 1024 * 1024:
        encoded = b'{"ok":false,"error_code":"worker_output_limit"}'
    result_path.write_bytes(encoded)
    os.chmod(result_path, 0o600)


if __name__ == "__main__":
    main()
