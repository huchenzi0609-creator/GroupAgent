"""Small control-plane helpers; never serialize credentials into worker state."""

from __future__ import annotations

import json
import re
from pathlib import Path

from pydantic import SecretStr

from groupagent.domain import DomainError


def load_secret_file(path: Path, allowed: frozenset[str]) -> dict[str, SecretStr]:
    if path.is_symlink() or path.stat().st_mode & 0o077:
        raise DomainError("unsafe_secret_file", "Credential file must be private and not a symlink")
    if path.stat().st_size > 64 * 1024:
        raise DomainError("invalid_secret_file", "Credential file exceeds its limit")
    result = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        if "=" not in stripped:
            raise DomainError("invalid_secret_file", "Use literal NAME=value entries")
        key, value = stripped.split("=", 1)
        key, value = key.strip(), value.strip()
        if key not in allowed:
            continue
        if key in result or "\x00" in value:
            raise DomainError("invalid_secret_file", "Credential fields must be unique literals")
        if value.startswith('"'):
            value = json.loads(value)
        elif value.startswith("'") and value.endswith("'"):
            value = value[1:-1]
        if not isinstance(value, str) or any(c in value for c in "\r\n\x00"):
            raise DomainError("invalid_secret_file", "Credential must be a single-line string")
        if value:
            result[key] = SecretStr(value)
    return result


_CREDENTIALS = re.compile(
    r"-----BEGIN [^-]*PRIVATE KEY-----.*?-----END [^-]*PRIVATE KEY-----"
    r"|(?<![A-Za-z0-9_-])(?:sk|ghp|github_pat|xoxb|xoxp)[-_][A-Za-z0-9_-]{16,}"
    r"(?![A-Za-z0-9_-])"
    r"|(?<=://)[^\s/:@]+:[^\s/@]+@",
    re.DOTALL,
)


def redact_credentials(text: str, known_secrets: tuple[SecretStr, ...] = ()) -> str:
    for value in known_secrets:
        secret = value.get_secret_value()
        if secret:
            text = text.replace(secret, "[凭据已隐藏]")
    return _CREDENTIALS.sub("[凭据已隐藏]", text)
