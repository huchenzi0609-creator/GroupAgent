"""Trusted bridge from authorized immutable artifacts to a work's file capability.

Only relative workspace paths are returned. Source artifacts are never mutated;
complex parsing/editing remains inside the separate compute sandbox.
"""

from __future__ import annotations

import asyncio
import fcntl
import hashlib
import os
import stat
from typing import ClassVar, Literal
from uuid import uuid4

from pydantic import Field

from groupagent.artifacts import ArtifactScope, ArtifactStore, ArtifactVersion
from groupagent.domain import ActorContext, DomainError, ExecutionLease, WorkOrder
from groupagent.memory.common import memory_lock
from groupagent.persistence import PostgresStore
from groupagent.tools import IdempotencyMode, ToolContext, ToolData, ToolDefinition, ToolGateway
from groupagent.tools.models import ToolFailure
from groupagent.tools.paths import open_parent

from .artifacts import ArtifactRegistry

_ALLOWED_FORMATS = {
    "md",
    "docx",
    "pdf",
    "pptx",
    "png",
    "jpeg",
    "jpg",
    "webp",
    "gif",
    "wav",
    "ogg",
    "mp3",
    "flac",
    "m4a",
    "aac",
}


class ExportInput(ToolData):
    validation_hints: ClassVar[dict[str, str]] = {
        "*": 'Use the matching file_id/version_id from this work input manifest or a prior artifact tool. path must be a relative basename such as "source.docx"; existing parent folders only, never /work/workspace or an ArtifactStore path.'
    }
    file_id: str = Field(pattern=r"^[a-f0-9]{32}$")
    version_id: str = Field(pattern=r"^[a-f0-9]{32}$")
    path: str = Field(
        min_length=1,
        max_length=512,
        description="Relative path inside this work workspace. Use a new top-level filename, for example source.docx. Existing files are replayed only if their bytes match.",
    )


class ExportOutput(ToolData):
    # Source IDs are deliberately not top-level file_id/version_id: exporting is
    # not a newly produced artifact and RecordingGateway must not publish it.
    source_file_id: str
    source_version_id: str
    path: str
    format: str
    sha256: str
    size_bytes: int
    state: Literal["exported", "already_present"]
    source_unchanged: Literal[True] = True


def _existing(parent: int, leaf: str, *, max_bytes: int) -> tuple[str, int] | None:
    try:
        handle = os.open(leaf, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=parent)
    except FileNotFoundError:
        return None
    with os.fdopen(handle, "rb") as stream:
        info = os.fstat(stream.fileno())
        if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_size > max_bytes:
            raise DomainError(
                "invalid_path", "Export target must be a bounded private regular file"
            )
        digest = hashlib.sha256()
        length = 0
        for chunk in iter(lambda: stream.read(65536), b""):
            length += len(chunk)
            if length > max_bytes:
                raise DomainError("quota_exceeded", "Export target exceeds its byte limit")
            digest.update(chunk)
        return digest.hexdigest(), length


def _workspace_size(directory: int, *, maximum: int) -> int:
    """Bounded anchored traversal; never follows workspace links."""
    seen = 0
    total = 0

    def visit(fd: int) -> None:
        nonlocal seen, total
        for name in os.listdir(fd):
            seen += 1
            if seen > 4096:
                raise DomainError("quota_exceeded", "Workspace file count exceeds export bounds")
            info = os.stat(name, dir_fd=fd, follow_symlinks=False)
            if stat.S_ISDIR(info.st_mode):
                child = os.open(name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=fd)
                try:
                    visit(child)
                finally:
                    os.close(child)
            elif stat.S_ISREG(info.st_mode) and info.st_nlink == 1:
                total += info.st_size
                if total > maximum:
                    raise DomainError("quota_exceeded", "Workspace exceeds its byte limit")
            else:
                raise DomainError("invalid_path", "Workspace contains an unsupported file or link")

    visit(directory)
    return total


def register_export_tool(
    gateway: ToolGateway,
    store: PostgresStore,
    registry: ArtifactRegistry,
    artifacts: ArtifactStore,
    work: WorkOrder,
    lease: ExecutionLease,
    actor: ActorContext,
    *,
    max_bytes: int = 30 * 1024 * 1024,
    max_workspace_bytes: int = 512 * 1024 * 1024,
) -> None:
    """Register an export capability for exactly one currently leased work."""
    if not 0 < max_bytes <= 30 * 1024 * 1024 or not 0 < max_workspace_bytes <= 512 * 1024 * 1024:
        raise ValueError("Export limits must fit the product byte limits")

    def authorized(context: ToolContext, arguments: ExportInput) -> ArtifactVersion:
        if context.cancelled.is_set():
            raise DomainError("cancelled", "Export was cancelled")
        if (context.group_id, context.work_id, context.actor.member_id) != (
            actor.group_id,
            work.id,
            actor.member_id,
        ) or (lease.group_id, lease.work_id) != (work.group_id, work.id):
            raise DomainError("scope_violation", "Export does not match its bound work and actor")
        with store.transaction(context.actor, trace_id=context.call_id) as tx:
            memory_lock(tx)
            tx.assert_lease(lease)
            current = tx.get_work(work.id)
            row = registry._row(tx, arguments.version_id)
            registry.public_sources_in_tx(tx, row)
            if row["work_id"] != work.id or row["file_id"] != arguments.file_id:
                raise DomainError(
                    "scope_violation",
                    "Import the authorized version into this work before exporting",
                )
            if (
                tx.conn.execute(
                    "SELECT 1 FROM ga_work_outputs WHERE group_id=%s AND work_id=%s AND goal_revision=%s AND version_id=%s",
                    (context.group_id, work.id, lease.goal_revision, arguments.version_id),
                ).fetchone()
                is None
            ):
                raise DomainError(
                    "scope_violation", "Version was not admitted to this goal revision"
                )
            expected_raw = current.scope.get("source_fingerprints")
            expected = (
                {key: str(value) for key, value in expected_raw.items()}
                if isinstance(expected_raw, dict)
                else None
            )
            retained = current.scope.get("retained_fact_source_ids", [])
            sources = tuple(
                sorted(
                    set(row["source_event_ids"]) | {current.request_event_id} | set(expected or {})
                )
            )
            registry.source_fingerprints(
                tx,
                sources,
                expected=expected,
                retained_sources=tuple(value for value in retained if isinstance(value, str))
                if isinstance(retained, list)
                else (),
            )
            version = ArtifactVersion(**row["body"])
            if version.format not in _ALLOWED_FORMATS or version.size_bytes > max_bytes:
                raise DomainError(
                    "quota_exceeded", "Export format or source size is outside this capability"
                )
            return version

    def transfer(arguments: ExportInput, context: ToolContext) -> ExportOutput:
        # Every failure before the temporary write is proven not to have exported
        # bytes. Later unexpected failures preserve uncertainty; no blanket rule.
        try:
            if (
                any(part in {"", ".", ".."} for part in arguments.path.split("/"))
                or len(arguments.path.encode()) > 512
            ):
                raise DomainError("invalid_path", "Export needs a bounded relative path")
            version = authorized(context, arguments)
            content = artifacts.read(
                ArtifactScope(context.group_id, work.id, context.actor.member_id),
                arguments.file_id,
                arguments.version_id,
            )
            if (
                len(content) != version.size_bytes
                or hashlib.sha256(content).hexdigest() != version.sha256
            ):
                raise DomainError(
                    "validation_failed", "Artifact bytes differ from the authorized version"
                )
            if len(content) > max_bytes:
                raise DomainError("quota_exceeded", "Artifact exceeds the export byte limit")
        except DomainError as error:
            raise ToolFailure(
                error.code,
                "Artifact export did not start. Check matching scoped file_id/version_id, current lease/policy and byte limits; import another work's version first.",
                no_effect=True,
            ) from None
        except (ValueError, UnicodeError):
            raise ToolFailure(
                "invalid_path",
                "Field path must be a bounded relative workspace filename.",
                no_effect=True,
            ) from None

        state: Literal["exported", "already_present"] = "exported"
        temporary: str | None = None
        mutated = False
        linked = False
        try:
            with open_parent(context.workspace, arguments.path) as (parent, leaf):
                root_fd = os.open(context.workspace, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
                lock = None
                try:
                    lock = os.open(
                        ".artifact-export.lock",
                        os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW,
                        0o600,
                        dir_fd=root_fd,
                    )
                    if not stat.S_ISREG(os.fstat(lock).st_mode) or os.fstat(lock).st_nlink != 1:
                        raise DomainError("invalid_path", "Workspace export lock is invalid")
                    fcntl.flock(lock, fcntl.LOCK_EX)
                    present = _existing(parent, leaf, max_bytes=max_bytes)
                    if present is not None:
                        if present != (version.sha256, version.size_bytes):
                            raise DomainError(
                                "path_conflict", "Export target already contains different bytes"
                            )
                        state = "already_present"
                    else:
                        used = _workspace_size(root_fd, maximum=max_workspace_bytes)
                        if used + len(content) > max_workspace_bytes:
                            raise DomainError(
                                "quota_exceeded", "Export would exceed the workspace byte limit"
                            )
                        authorized(context, arguments)
                        temporary = ".artifact-export-" + uuid4().hex
                        fd = os.open(
                            temporary,
                            os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                            0o600,
                            dir_fd=parent,
                        )
                        mutated = True
                        try:
                            with os.fdopen(fd, "wb") as stream:
                                stream.write(content)
                                stream.flush()
                                os.fsync(stream.fileno())
                            # link is atomic and refuses overwrite. A concurrent same-
                            # content target is acceptable; any other target is refused.
                            try:
                                os.link(
                                    temporary,
                                    leaf,
                                    src_dir_fd=parent,
                                    dst_dir_fd=parent,
                                    follow_symlinks=False,
                                )
                                linked = True
                            except FileExistsError:
                                if _existing(parent, leaf, max_bytes=max_bytes) != (
                                    version.sha256,
                                    version.size_bytes,
                                ):
                                    raise DomainError(
                                        "path_conflict", "Export target changed during publication"
                                    ) from None
                                state = "already_present"
                            os.unlink(temporary, dir_fd=parent)
                            temporary = None
                            os.fsync(parent)
                        finally:
                            if temporary is not None:
                                try:
                                    os.unlink(temporary, dir_fd=parent)
                                    temporary = None
                                except FileNotFoundError:
                                    temporary = None
                    # Source withdrawal or policy/lease loss cannot be reported as
                    # successful export. If publication already happened, retain
                    # UNKNOWN and let the runtime's quiesced cleanup remove bytes.
                    verified = authorized(context, arguments)
                    if verified.sha256 != version.sha256:
                        raise DomainError("validation_failed", "Authorized artifact changed")
                finally:
                    if lock is not None:
                        fcntl.flock(lock, fcntl.LOCK_UN)
                        os.close(lock)
                    os.close(root_fd)
        except (DomainError, OSError) as error:
            if not mutated:
                code = error.code if isinstance(error, DomainError) else "invalid_path"
                raise ToolFailure(
                    code,
                    "Export target was not changed. Use a new relative top-level filename (for example source.docx), an existing safe parent folder, and an active work lease; different existing bytes are never overwritten.",
                    no_effect=True,
                ) from None
            if (
                not linked
                and temporary is None
                and isinstance(error, DomainError)
                and error.code == "path_conflict"
            ):
                raise ToolFailure(
                    error.code,
                    "Concurrent target has different bytes; this export did not overwrite it. Choose a new relative filename.",
                    no_effect=True,
                ) from None
            raise
        return ExportOutput(
            source_file_id=version.file_id,
            source_version_id=version.version_id,
            path=arguments.path,
            format=version.format,
            sha256=version.sha256,
            size_bytes=version.size_bytes,
            state=state,
        )

    async def export(arguments: ExportInput, context: ToolContext) -> ExportOutput:
        return await asyncio.to_thread(transfer, arguments, context)

    gateway.register(
        ToolDefinition(
            name="artifact_export",
            description='Copy one authorized immutable version from this work into its workspace for isolated Python/Shell processing. Example: {"file_id":"<input file ID>","version_id":"<input version ID>","path":"source.docx"}. Then use python_compute input_paths=["source.docx"], save to a different declared output filename, and adopt it with artifact_from_workspace. No ArtifactStore host path is exposed; different existing bytes are never overwritten.',
            input_model=ExportInput,
            output_model=ExportOutput,
            handler=export,
            resource_scopes=("group", "work", "workspace", "artifacts"),
            risk_class="local_draft",
            idempotency_mode=IdempotencyMode.NATIVE_KEY,
            timeout_seconds=60,
            max_input_bytes=4096,
            max_output_bytes=4096,
            max_artifact_bytes=max_bytes,
        )
    )
