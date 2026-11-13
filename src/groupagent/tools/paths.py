"""Path capabilities for vetted file tools, not an operating-system sandbox.

These checks reject model-requested escapes. An untrusted Python handler could
ignore them, so only reviewed implementations belong in this process.
"""

from __future__ import annotations

import os
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path, PurePosixPath

from groupagent.domain import DomainError


def scoped_path(workspace: Path, value: str) -> Path:
    if not value or "\\" in value or "\x00" in value:
        raise DomainError("scope_violation", "A relative workspace path is required")
    relative = PurePosixPath(value)
    if relative.is_absolute() or any(part in {"..", "."} for part in value.split("/")):
        raise DomainError("scope_violation", "Paths must remain inside this work's workspace")
    root = workspace.resolve(strict=True)
    candidate = root.joinpath(*relative.parts)
    cursor = root
    for part in relative.parts:
        cursor /= part
        if cursor.is_symlink():
            raise DomainError("scope_violation", "Symlinks are not workspace capabilities")
    if not candidate.resolve(strict=False).is_relative_to(root) or candidate == root:
        raise DomainError("scope_violation", "Paths must remain inside this work's workspace")
    return candidate


@contextmanager
def open_parent(workspace: Path, value: str) -> Iterator[tuple[int, str]]:
    """Open each directory without following symlinks (POSIX only, otherwise deny)."""
    scoped_path(workspace, value)
    if not hasattr(os, "O_NOFOLLOW") or os.open not in os.supports_dir_fd:
        raise DomainError("dependency_unavailable", "Secure file operations need POSIX openat")
    parts = PurePosixPath(value).parts
    descriptors = []
    try:
        descriptor = os.open(workspace, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        descriptors.append(descriptor)
        for part in parts[:-1]:
            descriptor = os.open(
                part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=descriptor
            )
            descriptors.append(descriptor)
        yield descriptor, parts[-1]
    except OSError as exc:
        raise DomainError("invalid_path", "Workspace path could not be opened safely") from exc
    finally:
        for descriptor in reversed(descriptors):
            os.close(descriptor)
