"""Lossless checkpoint storage encoding with a small, non-authoritative status header.

Compression removes a fixed JSON storage threshold; it does not bound the decoded
Python execution state. Receipts, ledger and raw history are preserved exactly.
"""

from __future__ import annotations

import base64
import binascii
import gzip
import hashlib
import zlib
from collections import deque
from typing import Any

from groupagent.execution.models import EngineCheckpoint
from groupagent.tools.models import ToolReceipt

COMPRESSION_THRESHOLD_BYTES = 1024 * 1024
_CHUNK_BYTES = 64 * 1024
_CODEC_KEY = "checkpoint_codec"
_CODEC_FIELDS = frozenset({"version", "encoding", "decoded_bytes", "sha256", "payload"})


class CheckpointCodecError(ValueError):
    """A stored checkpoint cannot be verified; never recover from its header alone."""


def _status_header(checkpoint: EngineCheckpoint) -> dict[str, Any]:
    failed_count = 0
    failed: deque[ToolReceipt] = deque(maxlen=8)
    for receipt in checkpoint.receipts:
        if not receipt.success:
            failed_count += 1
            failed.append(receipt)
    return {
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


def encode_checkpoint(checkpoint: EngineCheckpoint) -> dict[str, Any]:
    """Keep small legacy bodies; losslessly compress checkpoints larger than 1 MiB."""
    raw = checkpoint.model_dump_json().encode("utf-8")
    if len(raw) <= COMPRESSION_THRESHOLD_BYTES:
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


def decode_checkpoint(body: dict[str, Any]) -> EngineCheckpoint:
    """Read legacy bodies or verify a versioned envelope before model validation.

    The length bounds decompression by the declared payload size rather than a
    new cumulative work limit. It is not a replacement for worker memory limits.
    Header fields can support status displays; recovery uses only verified raw
    payload fields and rejects any disagreement with that header.
    """
    if not isinstance(body, dict):
        raise CheckpointCodecError("Checkpoint body must be an object")
    if _CODEC_KEY not in body:
        try:
            return EngineCheckpoint.model_validate(body)
        except (TypeError, ValueError):
            raise CheckpointCodecError("Legacy checkpoint payload is invalid") from None
    codec = body[_CODEC_KEY]
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
    try:
        checkpoint = EngineCheckpoint.model_validate_json(raw)
    except (TypeError, ValueError):
        raise CheckpointCodecError("Decoded checkpoint payload is invalid") from None
    expected_header = _status_header(checkpoint)
    header = {key: value for key, value in body.items() if key != _CODEC_KEY}
    if not _header_matches(header, expected_header):
        raise CheckpointCodecError("Checkpoint status header does not match its payload")
    return checkpoint
