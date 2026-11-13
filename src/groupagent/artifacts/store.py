"""Immutable file versions with scoped access and an atomic current pointer.

This is a POSIX local-filesystem adapter, not a general OS sandbox. All paths below
the configured root are traversed using anchored directory descriptors and
O_NOFOLLOW. Metadata and payloads are never opened through caller-supplied paths.
"""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
import re
import stat
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

from .models import ArtifactError, ArtifactScope, ArtifactVersion, ValidationRecord

FORMATS = {".md": "md", ".docx": "docx", ".pdf": "pdf", ".pptx": "pptx"}
VIDEO_SUFFIXES = {".mp4", ".mov", ".avi", ".webm", ".mkv", ".m4v"}
MAX_FILE_BYTES = 30 * 1024 * 1024
_REF = re.compile(r"^[a-f0-9]{32}$")
_DIRECTORY = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW


def document_format(name: str) -> str:
    if (
        not isinstance(name, str)
        or not name.strip()
        or len(name.encode("utf-8")) > 240
        or name in {".", ".."}
        or any(c in name for c in ("/", "\\", "\x00"))
        or any(ord(c) < 32 for c in name)
    ):
        raise ArtifactError("invalid_name", "A filename without path components is required")
    suffix = Path(name).suffix.lower()
    if suffix in VIDEO_SUFFIXES:
        raise ArtifactError("unsupported_video", "Video processing is outside this worker")
    if suffix not in FORMATS:
        raise ArtifactError("unsupported_format", "Supported formats are md, docx, pdf and pptx")
    return FORMATS[suffix]


def _reference(value: str) -> str:
    if not isinstance(value, str) or not _REF.fullmatch(value):
        raise ArtifactError("invalid_reference", "An internal artifact identifier is required")
    return value


class ArtifactStore:
    def __init__(self, root: Path | str, *, max_file_bytes: int = MAX_FILE_BYTES):
        if max_file_bytes <= 0 or max_file_bytes > MAX_FILE_BYTES:
            raise ArtifactError("invalid_limit", "File limit must be within 30 MiB")
        path = Path(root).absolute()
        if path.is_symlink():
            raise ArtifactError("unsafe_path", "Artifact root must not be a symlink")
        path.mkdir(mode=0o700, parents=True, exist_ok=True)
        try:
            self._root_fd = os.open(path, _DIRECTORY)
        except OSError as exc:
            raise ArtifactError("unsafe_path", "Artifact root is not a safe directory") from exc
        self.root = path
        self.max_file_bytes = max_file_bytes
        self._closed = False

    def __enter__(self) -> ArtifactStore:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def close(self) -> None:
        if not self._closed:
            os.close(self._root_fd)
            self._closed = True

    def _bytes(self, content: bytes) -> bytes:
        if not isinstance(content, bytes) or not content:
            raise ArtifactError("invalid_content", "Artifact content must be nonempty bytes")
        if len(content) > self.max_file_bytes:
            raise ArtifactError("file_too_large", "Artifact exceeds the configured byte limit")
        return content

    @contextmanager
    def _directory(self, parent: int, name: str, *, create: bool = False) -> Iterator[int]:
        if self._closed:
            raise ArtifactError("store_closed", "Artifact store is closed")
        if create:
            try:
                os.mkdir(name, mode=0o700, dir_fd=parent)
                os.fsync(parent)
            except FileExistsError:
                pass
        try:
            fd = os.open(name, _DIRECTORY, dir_fd=parent)
        except FileNotFoundError as exc:
            raise ArtifactError("artifact_not_found", "Artifact reference does not exist") from exc
        except OSError as exc:
            raise ArtifactError("unsafe_path", "Artifact path is not a safe directory") from exc
        try:
            yield fd
        finally:
            os.close(fd)

    @contextmanager
    def _file(self, file_id: str, *, create: bool = False) -> Iterator[int]:
        with self._directory(self._root_fd, _reference(file_id), create=create) as fd:
            yield fd

    @contextmanager
    def _locked(self, fd: int, *, create: bool = False) -> Iterator[None]:
        try:
            lock = os.open(
                "lock",
                os.O_RDWR | os.O_NOFOLLOW | (os.O_CREAT if create else 0),
                mode=0o600,
                dir_fd=fd,
            )
        except OSError as exc:
            raise ArtifactError("unsafe_path", "Artifact lock is unsafe") from exc
        try:
            if not stat.S_ISREG(os.fstat(lock).st_mode):
                raise ArtifactError("unsafe_path", "Artifact lock must be a regular file")
            fcntl.flock(lock, fcntl.LOCK_EX)
            yield
        finally:
            os.close(lock)

    def _read(self, fd: int, name: str, *, limit: int = 1024 * 1024) -> bytes:
        try:
            handle = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=fd)
        except FileNotFoundError as exc:
            raise ArtifactError("artifact_not_found", "Artifact data is missing") from exc
        except OSError as exc:
            raise ArtifactError("unsafe_path", "Artifact data path is unsafe") from exc
        try:
            info = os.fstat(handle)
            if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
                raise ArtifactError(
                    "unsafe_path", "Artifact data must be a single-link regular file"
                )
            if info.st_size > limit:
                raise ArtifactError("file_too_large", "Stored data exceeds the configured limit")
            with os.fdopen(handle, "rb", closefd=False) as stream:
                data = stream.read(limit + 1)
            if len(data) > limit:
                raise ArtifactError("file_too_large", "Stored data exceeds the configured limit")
            return data
        finally:
            os.close(handle)

    def _json(self, fd: int, name: str) -> dict[str, Any]:
        try:
            result = json.loads(self._read(fd, name))
            if not isinstance(result, dict):
                raise ValueError("not an object")
            return result
        except (ValueError, UnicodeError) as exc:
            raise ArtifactError("corrupt_metadata", "Artifact metadata cannot be decoded") from exc

    def _write(self, fd: int, name: str, data: bytes) -> None:
        try:
            handle = os.open(
                name, os.O_CREAT | os.O_EXCL | os.O_WRONLY | os.O_NOFOLLOW, 0o600, dir_fd=fd
            )
        except OSError as exc:
            raise ArtifactError("unsafe_path", "Cannot create immutable artifact data") from exc
        try:
            with os.fdopen(handle, "wb", closefd=False) as stream:
                stream.write(data)
                stream.flush()
            os.fsync(handle)
        finally:
            os.close(handle)

    def _write_json(self, fd: int, name: str, value: dict[str, Any]) -> None:
        self._write(fd, name, json.dumps(value, ensure_ascii=False, sort_keys=True).encode("utf-8"))

    def _scoped(self, fd: int, scope: ArtifactScope) -> dict[str, Any]:
        if not isinstance(scope, ArtifactScope):
            raise ArtifactError("invalid_scope", "ArtifactScope is required")
        record = self._json(fd, "file.json")
        if record.get("group_id") != scope.group_id or record.get("work_id") != scope.work_id:
            raise ArtifactError("scope_denied", "Artifact belongs to another group or work")
        return record

    def _version(self, fd: int, record: dict[str, Any], version_id: str | None) -> ArtifactVersion:
        if version_id is None:
            version_id = self._json(fd, "current.json").get("version_id")
        _reference(version_id)  # type: ignore[arg-type]
        with self._directory(fd, "versions") as versions:
            with self._directory(versions, version_id) as version:  # type: ignore[arg-type]
                metadata = self._json(version, "version.json")
        try:
            result = ArtifactVersion(**metadata)
        except (ValueError, TypeError) as exc:
            raise ArtifactError("corrupt_metadata", "Artifact version metadata is invalid") from exc
        if (
            any(
                getattr(result, key) != record.get(key)
                for key in ("file_id", "group_id", "work_id", "name", "format")
            )
            or result.version_id != version_id
        ):
            raise ArtifactError("corrupt_metadata", "Artifact version identity does not match")
        return result

    def _publish(
        self,
        fd: int,
        record: dict[str, Any],
        scope: ArtifactScope,
        content: bytes,
        parent_version_id: str | None,
    ) -> ArtifactVersion:
        version_id = uuid4().hex
        version = ArtifactVersion(
            **{k: record[k] for k in ("file_id", "group_id", "work_id", "name", "format")},
            version_id=version_id,
            sha256=hashlib.sha256(content).hexdigest(),
            size_bytes=len(content),
            parent_version_id=parent_version_id,
            created_by=scope.actor_id,
            created_at=datetime.now(UTC).isoformat(),
        )
        with self._directory(fd, "versions", create=True) as versions:
            staging_name = f".pending-{version_id}"
            with self._directory(versions, staging_name, create=True) as staging:
                self._write(staging, "content", content)
                self._write_json(staging, "version.json", version.to_dict())
                os.fsync(staging)
            os.rename(staging_name, version_id, src_dir_fd=versions, dst_dir_fd=versions)
            os.fsync(versions)
        pointer_name = f".current-{uuid4().hex}.json"
        self._write_json(fd, pointer_name, {"version_id": version_id})
        os.replace(pointer_name, "current.json", src_dir_fd=fd, dst_dir_fd=fd)
        os.fsync(fd)
        return version

    def create(self, scope: ArtifactScope, name: str, content: bytes) -> ArtifactVersion:
        if not isinstance(scope, ArtifactScope):
            raise ArtifactError("invalid_scope", "ArtifactScope is required")
        format_name = document_format(name)
        self._bytes(content)
        file_id = uuid4().hex
        record = {
            "file_id": file_id,
            "group_id": scope.group_id,
            "work_id": scope.work_id,
            "name": name,
            "format": format_name,
        }
        with self._file(file_id, create=True) as fd, self._locked(fd, create=True):
            self._write_json(fd, "file.json", record)
            return self._publish(fd, record, scope, content, None)

    def revise(
        self,
        scope: ArtifactScope,
        file_id: str,
        content: bytes,
        *,
        expected_version_id: str,
    ) -> ArtifactVersion:
        _reference(expected_version_id)
        with self._file(file_id) as fd:
            record = self._scoped(fd, scope)
            with self._locked(fd):
                current = self._version(fd, record, None)
                if current.version_id != expected_version_id:
                    raise ArtifactError("version_conflict", "Current artifact version has changed")
                self._bytes(content)
                return self._publish(fd, record, scope, content, expected_version_id)

    def get(
        self,
        scope: ArtifactScope,
        file_id: str,
        version_id: str | None = None,
    ) -> ArtifactVersion:
        with self._file(file_id) as fd:
            return self._version(fd, self._scoped(fd, scope), version_id)

    def read(self, scope: ArtifactScope, file_id: str, version_id: str | None = None) -> bytes:
        with self._file(file_id) as fd:
            version = self._version(fd, self._scoped(fd, scope), version_id)
            with self._directory(fd, "versions") as versions:
                with self._directory(versions, version.version_id) as version_fd:
                    content = self._read(version_fd, "content", limit=self.max_file_bytes)
        if (
            len(content) != version.size_bytes
            or hashlib.sha256(content).hexdigest() != version.sha256
        ):
            raise ArtifactError("integrity_mismatch", "Artifact content no longer matches its hash")
        return content

    def list(self, scope: ArtifactScope, *, name_contains: str = "") -> tuple[ArtifactVersion, ...]:
        result = []
        for file_id in os.listdir(self._root_fd):
            if not _REF.fullmatch(file_id):
                continue
            try:
                version = self.get(scope, file_id)
            except ArtifactError as exc:
                if exc.code in {"scope_denied", "artifact_not_found"}:
                    continue
                raise
            if name_contains.casefold() in version.name.casefold():
                result.append(version)
        return tuple(sorted(result, key=lambda v: (v.created_at, v.file_id)))

    def record_validation(
        self,
        scope: ArtifactScope,
        file_id: str,
        version_id: str,
        validation: ValidationRecord,
        *,
        previews: tuple[bytes, ...] = (),
    ) -> ValidationRecord:
        _reference(validation.validation_id)
        with self._file(file_id) as fd, self._locked(fd):
            version = self._version(fd, self._scoped(fd, scope), version_id)
            if (
                validation.file_id,
                validation.version_id,
                validation.sha256,
                validation.format,
            ) != (
                file_id,
                version_id,
                version.sha256,
                version.format,
            ):
                raise ArtifactError("validation_mismatch", "Validation describes another artifact")
            if previews:
                hashes = tuple(hashlib.sha256(preview).hexdigest() for preview in previews)
                if (
                    hashes != validation.preview_sha256
                    or sum(map(len, previews)) > 100 * 1024 * 1024
                ):
                    raise ArtifactError(
                        "validation_mismatch", "Preview bytes do not match validation"
                    )
                with self._directory(fd, "previews", create=True) as preview_root:
                    with self._directory(
                        preview_root, validation.validation_id, create=True
                    ) as pages:
                        for index, preview in enumerate(previews):
                            self._write(pages, f"page-{index + 1}.png", preview)
                        os.fsync(pages)
            with self._directory(fd, "validations", create=True) as validations:
                self._write_json(
                    validations, f"{validation.validation_id}.json", validation.to_dict()
                )
                os.fsync(validations)
        return validation

    def read_preview(
        self,
        scope: ArtifactScope,
        file_id: str,
        version_id: str,
        validation_id: str,
        *,
        page_index: int = 0,
    ) -> bytes:
        _reference(validation_id)
        records = self.validations(scope, file_id, version_id)
        record = next((r for r in records if r.validation_id == validation_id), None)
        if record is None or not 0 <= page_index < len(record.preview_sha256):
            raise ArtifactError("preview_not_found", "Validation preview does not exist")
        # A visual-review attestation refers to its parent render's immutable previews.
        preview_id = record.parent_validation_id or validation_id
        with self._file(file_id) as fd:
            self._scoped(fd, scope)
            with self._directory(fd, "previews") as root:
                with self._directory(root, _reference(preview_id)) as pages:
                    content = self._read(
                        pages, f"page-{page_index + 1}.png", limit=10 * 1024 * 1024
                    )
        if hashlib.sha256(content).hexdigest() != record.preview_sha256[page_index]:
            raise ArtifactError("integrity_mismatch", "Preview bytes no longer match their hash")
        return content

    def validations(
        self,
        scope: ArtifactScope,
        file_id: str,
        version_id: str | None = None,
    ) -> tuple[ValidationRecord, ...]:
        with self._file(file_id) as fd:
            version = self._version(fd, self._scoped(fd, scope), version_id)
            try:
                with self._directory(fd, "validations") as validations:
                    records = [
                        ValidationRecord.from_dict(self._json(validations, name))
                        for name in os.listdir(validations)
                        if name.endswith(".json") and _REF.fullmatch(name[:-5])
                    ]
            except ArtifactError as exc:
                if exc.code == "artifact_not_found":
                    return ()
                raise
        return tuple(
            sorted(
                (
                    r
                    for r in records
                    if r.version_id == version.version_id and r.sha256 == version.sha256
                ),
                key=lambda r: (r.created_at, r.validation_id),
            )
        )
