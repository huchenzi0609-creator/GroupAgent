"""Scope-safe staging around the shared sandbox. No host parsing fallback."""

from __future__ import annotations

import hashlib
import json
import os
import stat
from typing import Any, Protocol
from uuid import uuid4

from groupagent.domain import DomainError
from groupagent.sandbox import ProcessResult, ProcessSpec
from groupagent.tools import ToolContext
from groupagent.tools.paths import open_parent


class SandboxPort(Protocol):
    async def run(self, spec: ProcessSpec, context: ToolContext) -> ProcessResult: ...


class IsolatedMediaWorker:
    def __init__(self, sandbox: SandboxPort):
        self.sandbox = sandbox

    @staticmethod
    def _write(context: ToolContext, name: str, content: bytes) -> None:
        with open_parent(context.workspace, name) as (parent, leaf):
            fd = os.open(
                leaf, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600, dir_fd=parent
            )
            with os.fdopen(fd, "wb") as stream:
                stream.write(content)

    @staticmethod
    def _read(context: ToolContext, name: str, *, limit: int) -> bytes:
        with open_parent(context.workspace, name) as (parent, leaf):
            fd = os.open(leaf, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=parent)
            with os.fdopen(fd, "rb") as stream:
                info = os.fstat(stream.fileno())
                if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_size > limit:
                    raise DomainError(
                        "invalid_worker_output", "Worker output is not a bounded private file"
                    )
                content = stream.read(limit + 1)
        if len(content) > limit:
            raise DomainError("worker_output_limit", "Worker output exceeds its byte limit")
        return content

    async def run(
        self,
        context: ToolContext,
        operation: str,
        inputs: tuple[bytes, ...],
        *,
        parameters: dict[str, Any] | None = None,
        outputs: tuple[str, ...] = (),
        timeout_seconds: float = 180,
    ) -> tuple[dict[str, Any], tuple[bytes, ...]]:
        if context.group_id != context.actor.group_id or context.cancelled.is_set():
            raise DomainError("scope_violation", "Media operation has no active scoped context")
        prefix = "media-" + uuid4().hex
        input_names = tuple(f"{prefix}-input-{index}.bin" for index in range(len(inputs)))
        output_names = tuple(f"{prefix}-{name}" for name in outputs)
        request_name, result_name = prefix + "-request.json", prefix + "-result.json"
        all_names = (request_name, result_name, *input_names, *output_names)
        if len(set(all_names)) != len(all_names):
            raise DomainError("invalid_input", "Media output names must be unique")
        try:
            for name, content in zip(input_names, inputs, strict=True):
                self._write(context, name, content)
            request = {
                "operation": operation,
                "inputs": input_names,
                "outputs": output_names,
                "parameters": parameters or {},
            }
            self._write(
                context,
                request_name,
                json.dumps(request, ensure_ascii=False, allow_nan=False).encode(),
            )
            spec = ProcessSpec(
                program="python3",
                args=("-I", "-m", "groupagent.media.worker", request_name, result_name),
                input_paths=(request_name, *input_names),
                output_paths=(result_name, *output_names),
                timeout_seconds=timeout_seconds,
                max_output_bytes=64000,
            )
            result = await self.sandbox.run(spec, context)
            if not result.succeeded:
                code = (
                    "cancelled"
                    if result.cancelled
                    else "media_timeout"
                    if result.timed_out
                    else "media_worker_failed"
                )
                raise DomainError(code, "Isolated media processing did not complete")
            manifest = {entry.path: entry for entry in result.outputs}
            values = []
            for name in (result_name, *output_names):
                content = self._read(context, name, limit=32 * 1024 * 1024)
                entry = manifest.get(name)
                if (
                    entry is None
                    or entry.sha256 != hashlib.sha256(content).hexdigest()
                    or entry.size_bytes != len(content)
                ):
                    raise DomainError(
                        "worker_integrity_failed", "Worker output differs from its verified export"
                    )
                values.append(content)
            payload = json.loads(values[0])
            if payload.get("ok") is not True:
                code = payload.get("error_code", "media_worker_failed")
                raise DomainError(
                    code if isinstance(code, str) else "media_worker_failed",
                    "Isolated media worker rejected the input",
                )
            data = payload.get("result")
            if not isinstance(data, dict):
                raise DomainError("invalid_worker_output", "Worker result must be a JSON object")
            if operation == "audio_validate":
                data["chunk_indices"] = [
                    output_names.index(name) for name in data.get("chunks", ())
                ]
            return data, tuple(values[1:])
        finally:
            for name in all_names:
                try:
                    with open_parent(context.workspace, name) as (parent, leaf):
                        try:
                            os.unlink(leaf, dir_fd=parent)
                        except FileNotFoundError:
                            pass
                except DomainError:
                    # Cleanup cannot replace the primary worker/policy error.
                    # Any path that became unsafe is deliberately not traversed.
                    continue
