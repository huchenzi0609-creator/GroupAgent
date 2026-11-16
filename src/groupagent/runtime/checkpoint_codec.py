"""Lossless checkpoint storage encoding with a small, non-authoritative status header.

Compression removes a fixed JSON storage threshold; it does not bound the decoded
Python execution state. Receipts, ledger and raw history are preserved exactly.
"""

from __future__ import annotations

import base64
import binascii
import gzip
import hashlib
import json
import re
import zlib
from collections import deque
from typing import Any

from pydantic import BaseModel

from groupagent.execution.models import EngineCheckpoint, EngineEvent
from groupagent.tools.models import ToolReceipt

from .text import visible_text

COMPRESSION_THRESHOLD_BYTES = 1024 * 1024
_CHUNK_BYTES = 64 * 1024
_CODEC_KEY = "checkpoint_codec"
_EVENT_CODEC_KEY = "runtime_event_codec"
_CODEC_FIELDS = frozenset({"version", "encoding", "decoded_bytes", "sha256", "payload"})
_JSONB_UNSUPPORTED = re.compile(rb"\\u(?:0000|[dD][89aAbBcCdDeEfF][0-9a-fA-F]{2})")


class CheckpointCodecError(ValueError):
    """A stored checkpoint cannot be verified; never recover from its header alone."""


def serialize_runtime_model(model: BaseModel) -> bytes:
    """Preserve JSON strings even when Pydantic's UTF-8 writer rejects a surrogate."""
    try:
        return model.model_dump_json().encode("utf-8")
    except (ValueError, UnicodeError):
        return json.dumps(
            model.model_dump(mode="json"), ensure_ascii=True, separators=(",", ":"), allow_nan=False
        ).encode("ascii")


def _jsonb_projection(value: Any) -> Any:
    """A non-authoritative display projection, never the original event/evidence.

    PostgreSQL JSONB cannot represent U+0000, although JSON and model/tool text
    can. Keep the original bytes in the verified envelope instead of dropping
    or changing them. Only the SQL-readable status projection escapes that code
    point into visible text.
    """
    if isinstance(value, str):
        return visible_text(value)
    if isinstance(value, dict):
        return {_jsonb_projection(key): _jsonb_projection(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [_jsonb_projection(item) for item in value]
    return value


def _status_header(checkpoint: EngineCheckpoint) -> dict[str, Any]:
    failed_count = 0
    failed: deque[ToolReceipt] = deque(maxlen=8)
    for receipt in checkpoint.receipts:
        if not receipt.success:
            failed_count += 1
            failed.append(receipt)
    return _jsonb_projection(
        {
            "state": checkpoint.state.value,
            "error_code": checkpoint.error_code[:100] if checkpoint.error_code else None,
            "usage": checkpoint.usage.model_dump(mode="json"),
            "sequence": checkpoint.sequence,
            "safe_to_resume": checkpoint.safe_to_resume,
            # This is a bounded display projection, not ToolReceipt/auth input. The
            # actual receipt objects, including every output, stay in the payload.
            "receipts": [
                {
                    "call_id": receipt.call_id[:256],
                    "tool_name": receipt.tool_name[:64],
                    "success": False,
                    "effect_state": receipt.effect_state.value,
                    "error_code": receipt.error_code[:100] if receipt.error_code else None,
                    "message": receipt.message[:512] if receipt.message else None,
                }
                for receipt in failed
            ],
            "receipt_count": len(checkpoint.receipts),
            "failed_receipt_count": failed_count,
        }
    )


def encode_checkpoint(checkpoint: EngineCheckpoint) -> dict[str, Any]:
    """Losslessly encode large or JSONB-incompatible text; retain compatible legacy bodies."""
    raw = serialize_runtime_model(checkpoint)
    if len(raw) <= COMPRESSION_THRESHOLD_BYTES and not _JSONB_UNSUPPORTED.search(raw):
        return checkpoint.model_dump(mode="json")
    packed = gzip.compress(raw, compresslevel=1, mtime=0)
    return {
        **_status_header(checkpoint),
        _CODEC_KEY: {
            "version": 1,
            "encoding": "gzip+base64",
            "decoded_bytes": len(raw),
            "sha256": hashlib.sha256(raw).hexdigest(),
            "payload": base64.b64encode(packed).decode("ascii"),
        },
    }


def _event_status_header(event: EngineEvent) -> dict[str, Any]:
    body = event.model_dump(mode="json")
    payload = dict(body["payload"])
    # Raw model/user/tool content is retained in the verified payload, while
    # existing SQL fact/proof readers keep their host status/receipt metadata.
    payload.pop("candidate_text", None)
    payload.pop("text", None)
    if isinstance(payload.get("receipt"), dict):
        payload["receipt"] = {
            key: value for key, value in payload["receipt"].items() if key != "output"
        }
        for name in ("required_tool_evidence", "loaded_tools"):
            # Frozenset JSON order changes with a restarted interpreter's hash
            # seed. Only this display projection is canonicalized; the verified
            # original event payload remains byte-for-byte intact.
            payload["receipt"][name] = sorted(payload["receipt"].get(name, ()))
    body["payload"] = payload
    return _jsonb_projection(body)


def encode_event(event: EngineEvent) -> dict[str, Any]:
    """Preserve arbitrary tool text without handing invalid Unicode to JSONB."""
    raw = serialize_runtime_model(event)
    if not _JSONB_UNSUPPORTED.search(raw):
        return event.model_dump(mode="json")
    return {
        **_event_status_header(event),
        _EVENT_CODEC_KEY: {
            "version": 1,
            "encoding": "gzip+base64",
            "decoded_bytes": len(raw),
            "sha256": hashlib.sha256(raw).hexdigest(),
            "payload": base64.b64encode(gzip.compress(raw, compresslevel=1, mtime=0)).decode(
                "ascii"
            ),
        },
    }


def _decode_gzip(packed: bytes, expected_bytes: int) -> bytearray:
    """Inflate one stream incrementally, never accepting more than expected+1 bytes."""
    decoder = zlib.decompressobj(wbits=31)
    raw = bytearray()
    try:
        for position in range(0, len(packed), _CHUNK_BYTES):
            pending = packed[position : position + _CHUNK_BYTES]
            while True:
                limit = min(_CHUNK_BYTES, expected_bytes - len(raw) + 1)
                block = decoder.decompress(pending, max_length=limit)
                raw.extend(block)
                if len(raw) > expected_bytes:
                    raise CheckpointCodecError("Checkpoint decoded length exceeds its declaration")
                if decoder.unused_data:
                    raise CheckpointCodecError("Checkpoint contains trailing compressed data")
                pending = decoder.unconsumed_tail
                if pending:
                    continue
                if len(block) == limit and not decoder.eof:
                    # A final DEFLATE match may still have buffered output even
                    # when all input bytes from this chunk have been consumed.
                    continue
                break
        if not decoder.eof or len(raw) != expected_bytes:
            raise CheckpointCodecError("Checkpoint compressed stream is incomplete or mis-sized")
    except zlib.error:
        raise CheckpointCodecError("Checkpoint compressed stream is invalid") from None
    return raw


def _header_matches(value: Any, expected: Any) -> bool:
    """Compare JSON types as well as values; Python otherwise equates False and 0."""
    if isinstance(expected, dict):
        return (
            isinstance(value, dict)
            and value.keys() == expected.keys()
            and all(_header_matches(value[key], item) for key, item in expected.items())
        )
    if isinstance(expected, list):
        return (
            isinstance(value, list)
            and len(value) == len(expected)
            and all(
                _header_matches(actual, item) for actual, item in zip(value, expected, strict=True)
            )
        )
    if isinstance(expected, float):
        # JSONB may normalize a numeric representation; bool is never a number
        # in the checkpoint header contract.
        return type(value) in {int, float} and value == expected
    return type(value) is type(expected) and value == expected


def _decode_payload(codec: Any) -> bytearray:
    if not isinstance(codec, dict) or set(codec) != _CODEC_FIELDS:
        raise CheckpointCodecError("Checkpoint codec descriptor is invalid")
    if type(codec["version"]) is not int or codec["version"] != 1:
        raise CheckpointCodecError("Checkpoint codec version is unsupported")
    if codec["encoding"] != "gzip+base64":
        raise CheckpointCodecError("Checkpoint encoding is unsupported")
    expected_bytes = codec["decoded_bytes"]
    digest = codec["sha256"]
    payload = codec["payload"]
    if type(expected_bytes) is not int or expected_bytes < 1:
        raise CheckpointCodecError("Checkpoint decoded length is invalid")
    if (
        not isinstance(digest, str)
        or len(digest) != 64
        or any(character not in "0123456789abcdef" for character in digest)
        or not isinstance(payload, str)
    ):
        raise CheckpointCodecError("Checkpoint payload metadata is invalid")
    try:
        packed = base64.b64decode(payload, validate=True)
    except (ValueError, binascii.Error):
        raise CheckpointCodecError("Checkpoint base64 payload is invalid") from None
    raw = _decode_gzip(packed, expected_bytes)
    if hashlib.sha256(raw).hexdigest() != digest:
        raise CheckpointCodecError("Checkpoint payload hash does not match")
    return raw


def decode_checkpoint(body: dict[str, Any]) -> EngineCheckpoint:
    """Read legacy bodies or verify a lossless envelope before using any authority."""
    if not isinstance(body, dict):
        raise CheckpointCodecError("Checkpoint body must be an object")
    if _CODEC_KEY not in body:
        try:
            return EngineCheckpoint.model_validate(body)
        except (TypeError, ValueError):
            raise CheckpointCodecError("Legacy checkpoint payload is invalid") from None
    raw = _decode_payload(body[_CODEC_KEY])
    try:
        checkpoint = EngineCheckpoint.model_validate_json(raw)
    except (TypeError, ValueError):
        try:
            # JSON permits escaped unpaired surrogates, while the native UTF-8
            # parser refuses them. Python's JSON reader retains those original
            # code points; model validation still verifies the entire contract.
            checkpoint = EngineCheckpoint.model_validate(json.loads(raw))
        except (TypeError, ValueError):
            raise CheckpointCodecError("Decoded checkpoint payload is invalid") from None
    expected_header = _status_header(checkpoint)
    header = {key: value for key, value in body.items() if key != _CODEC_KEY}
    if not _header_matches(header, expected_header):
        raise CheckpointCodecError("Checkpoint status header does not match its payload")
    return checkpoint


def decode_event(body: dict[str, Any]) -> EngineEvent:
    if not isinstance(body, dict):
        raise CheckpointCodecError("Event body must be an object")
    if _EVENT_CODEC_KEY not in body:
        try:
            return EngineEvent.model_validate(body)
        except (TypeError, ValueError):
            raise CheckpointCodecError("Legacy event payload is invalid") from None
    raw = _decode_payload(body[_EVENT_CODEC_KEY])
    try:
        event = EngineEvent.model_validate_json(raw)
    except (TypeError, ValueError):
        try:
            event = EngineEvent.model_validate(json.loads(raw))
        except (TypeError, ValueError):
            raise CheckpointCodecError("Decoded event payload is invalid") from None
    header = {key: value for key, value in body.items() if key != _EVENT_CODEC_KEY}
    if not _header_matches(header, _event_status_header(event)):
        raise CheckpointCodecError("Event status header does not match its payload")
    return event
