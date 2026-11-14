from __future__ import annotations

import hashlib
import json
import os
import re
from datetime import UTC, datetime
from pathlib import Path

from groupagent.domain import ActorContext, DomainError, Role

RESTORE_LOCK_KEY = 7342938221337


def require_ops(actor: ActorContext) -> None:
    if actor.role not in (Role.MAINTAINER, Role.OWNER):
        raise DomainError(
            "permission_denied", "A trusted maintenance or owner identity is required"
        )


def clock(now: datetime | None = None) -> datetime:
    now = now or datetime.now(UTC)
    if now.utcoffset() is None:
        raise DomainError("invalid_input", "Operation clock must include a timezone")
    return now.astimezone(UTC)


def canonical(value) -> bytes:
    return json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False
    ).encode("utf-8")


def digest(value) -> str:
    return hashlib.sha256(canonical(value)).hexdigest()


def file_hash(path: Path, *, max_bytes: int = 2 * 1024**3) -> tuple[str, int]:
    if path.is_symlink() or not path.is_file():
        raise DomainError("unsafe_path", "Expected a regular managed file")
    total = 0
    checksum = hashlib.sha256()
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
    with os.fdopen(descriptor, "rb") as source:
        while block := source.read(1024 * 1024):
            total += len(block)
            if total > max_bytes:
                raise DomainError(
                    "quota_exceeded", "Operation file exceeds its configured size bound"
                )
            checksum.update(block)
    return checksum.hexdigest(), total


def private_root(path: Path) -> Path:
    if path.is_symlink():
        raise DomainError("unsafe_path", "Managed storage root cannot be a symlink")
    path = path.resolve()
    path.mkdir(mode=0o700, parents=True, exist_ok=True)
    if not path.is_dir() or path.stat().st_mode & 0o077:
        raise DomainError("unsafe_path", "Managed storage root requires private 0700 permissions")
    return path


def managed_directory(root: Path, identifier: str, *, create: bool = False) -> Path:
    if not re.fullmatch(r"[a-f0-9]{32}", identifier):
        raise DomainError("invalid_input", "Invalid managed backup identifier")
    folder = root / identifier
    if folder.is_symlink():
        raise DomainError("unsafe_path", "Managed backup directory cannot be a symlink")
    if create:
        folder.mkdir(mode=0o700)
    if not folder.is_dir() or folder.resolve().parent != root:
        raise DomainError("not_found", "Managed backup directory was not found")
    return folder


def atomic_json(path: Path, value) -> None:
    temporary = path.with_name(path.name + ".pending")
    descriptor = os.open(
        temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0), 0o600
    )
    try:
        with os.fdopen(descriptor, "wb") as target:
            target.write(canonical(value))
            target.flush()
            os.fsync(target.fileno())
        os.replace(temporary, path)
        directory = os.open(path.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        if temporary.exists():
            temporary.unlink()
