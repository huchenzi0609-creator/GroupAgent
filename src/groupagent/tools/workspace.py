"""Small reviewed file operations; arbitrary host execution is intentionally absent."""

from __future__ import annotations

import hashlib
import os
import stat
import uuid

from groupagent.domain import DomainError

from .gateway import ToolGateway
from .models import (
    IdempotencyMode,
    ToolContext,
    ToolDefinition,
    WorkspaceReadInput,
    WorkspaceReadOutput,
    WorkspaceWriteInput,
    WorkspaceWriteOutput,
)
from .paths import open_parent

MAX_TEXT_BYTES = 128_000


async def read_text(args: WorkspaceReadInput, context: ToolContext) -> WorkspaceReadOutput:
    with open_parent(context.workspace, args.path) as (parent, name):
        descriptor = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=parent)
        with os.fdopen(descriptor, "rb") as source:
            info = os.fstat(source.fileno())
            if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
                raise DomainError(
                    "scope_violation", "Only private regular workspace files are read"
                )
            data = source.read(MAX_TEXT_BYTES + 1)
            if len(data) > MAX_TEXT_BYTES:
                raise DomainError("input_too_large", "The file exceeds the text tool limit")
    try:
        text = data.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise DomainError("invalid_input", "This tool only reads UTF-8 text files") from exc
    return WorkspaceReadOutput(path=args.path, text=text)


async def write_text(args: WorkspaceWriteInput, context: ToolContext) -> WorkspaceWriteOutput:
    data = args.text.encode("utf-8")
    if len(data) > MAX_TEXT_BYTES:
        raise DomainError("input_too_large", "The text exceeds the file size limit")
    with open_parent(context.workspace, args.path) as (parent, name):
        # Publish an independent draft using an atomic no-overwrite link. Existing files,
        # including symlinks/hardlinks, are never modified by this tool.
        temporary = f".groupagent-{uuid.uuid4().hex}.tmp"
        descriptor = os.open(
            temporary,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
            mode=0o600,
            dir_fd=parent,
        )
        try:
            with os.fdopen(descriptor, "wb") as target:
                target.write(data)
                target.flush()
                os.fsync(target.fileno())
            try:
                os.link(
                    temporary,
                    name,
                    src_dir_fd=parent,
                    dst_dir_fd=parent,
                    follow_symlinks=False,
                )
            except FileExistsError as exc:
                raise DomainError("version_conflict", "The draft already exists") from exc
        finally:
            os.unlink(temporary, dir_fd=parent)
    return WorkspaceWriteOutput(
        path=args.path, bytes_written=len(data), sha256=hashlib.sha256(data).hexdigest()
    )


def register_workspace_tools(gateway: ToolGateway) -> None:
    gateway.register(
        ToolDefinition(
            name="workspace_read_text",
            description="Read a UTF-8 text file in this work's private workspace (128 KB maximum).",
            input_model=WorkspaceReadInput,
            output_model=WorkspaceReadOutput,
            handler=read_text,
            risk_class="read_only",
            idempotency_mode=IdempotencyMode.READ_ONLY,
            input_path_fields=("path",),
            max_output_bytes=800_000,
        )
    )
    gateway.register(
        ToolDefinition(
            name="workspace_write_text",
            description="Create an independent UTF-8 draft. Existing files cannot be overwritten.",
            input_model=WorkspaceWriteInput,
            output_model=WorkspaceWriteOutput,
            handler=write_text,
            input_path_fields=("path",),
            output_path_fields=("path",),
            max_artifact_bytes=MAX_TEXT_BYTES,
            idempotency_mode=IdempotencyMode.RECONCILE_BEFORE_RETRY,
        )
    )
