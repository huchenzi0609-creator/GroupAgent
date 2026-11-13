"""Screen public target URLs without ever connecting to the target HTTP origin.

DNS checks reject currently private answers. They do not pin the provider's DNS
resolution, prove a target stays public, or replace the provider's fetch policy.
Only the fixed DeepSeek provider endpoints receive local HTTP requests.
"""

from __future__ import annotations

import asyncio
import ipaddress
import re
import socket
from collections.abc import Awaitable, Callable
from urllib.parse import SplitResult, urlsplit, urlunsplit

from groupagent.domain import DomainError

Resolver = Callable[[str], Awaitable[tuple[str, ...]]]
_PRIVATE_SUFFIXES = (
    ".localhost",
    ".local",
    ".localdomain",
    ".internal",
    ".intranet",
    ".lan",
    ".home",
    ".home.arpa",
    ".corp",
    ".invalid",
    ".test",
    ".example",
    ".alt",
    ".onion",
)
_QUERY_URL = re.compile(r"\b[a-zA-Z][a-zA-Z0-9+.-]*://[^\s<>\"']+")
_SITE_OPERATOR = re.compile(r"(?<!\w)-?site:([^\s()]+)", re.IGNORECASE)


async def system_resolver(host: str) -> tuple[str, ...]:
    rows = await asyncio.get_running_loop().getaddrinfo(host, None, type=socket.SOCK_STREAM)
    return tuple(sorted({row[4][0] for row in rows}))


def is_public_address(value: str) -> bool:
    try:
        address = ipaddress.ip_address(value)
    except ValueError:
        return False
    if not address.is_global or any(
        (
            address.is_private,
            address.is_loopback,
            address.is_link_local,
            address.is_multicast,
            address.is_reserved,
            address.is_unspecified,
        )
    ):
        return False
    if isinstance(address, ipaddress.IPv6Address):
        if address.ipv4_mapped or address.sixtofour or address.teredo:
            return False
        if address in ipaddress.ip_network("64:ff9b::/96") or address in ipaddress.ip_network(
            "64:ff9b:1::/48"
        ):
            return False
    return True


def parse_public_url(url: str) -> tuple[str, str, bool]:
    """Return canonical URL, host, is_literal_ip; fail before any provider request."""
    try:
        url.encode("utf-8")
    except UnicodeError:
        raise DomainError("url_not_public", "The target URL is not valid Unicode") from None
    if (
        not url
        or len(url) > 8192
        or "\\" in url
        or any(
            character.isspace() or ord(character) < 32 or ord(character) == 127 for character in url
        )
        or re.search(r"%(?:00|0a|0d|5c)", url, re.IGNORECASE)
    ):
        raise DomainError("url_not_public", "A valid public HTTP or HTTPS URL is required")
    try:
        parsed = urlsplit(url)
        host = parsed.hostname
        port = parsed.port
    except ValueError:
        raise DomainError("url_not_public", "The target URL is invalid") from None
    if (
        parsed.scheme not in {"http", "https"}
        or not host
        or parsed.username is not None
        or parsed.password is not None
        or "%" in host
        or port not in {None, 80 if parsed.scheme == "http" else 443}
    ):
        raise DomainError("url_not_public", "Only public standard-port HTTP(S) URLs are allowed")
    host = host.rstrip(".").lower()
    try:
        address = ipaddress.ip_address(host)
    except ValueError:
        address = None
    if address is not None:
        if not is_public_address(host):
            raise DomainError(
                "url_not_public", "Private or special-purpose addresses are not allowed"
            )
        authority = f"[{host}]" if address.version == 6 else host
    else:
        try:
            host = host.encode("idna").decode("ascii")
        except UnicodeError:
            raise DomainError("url_not_public", "The target hostname is invalid") from None
        labels = host.split(".")
        if (
            host == "localhost"
            or host.endswith(_PRIVATE_SUFFIXES)
            or len(host) > 253
            or len(labels) < 2
            or not re.fullmatch(r"(?:[a-z]{2,63}|xn--[a-z0-9-]{2,59})", labels[-1])
            or any(
                not re.fullmatch(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?", part) for part in labels
            )
        ):
            raise DomainError("url_not_public", "A public fully-qualified hostname is required")
        authority = host
    normalized = urlunsplit(
        SplitResult(
            scheme=parsed.scheme,
            netloc=authority,
            path=parsed.path or "/",
            query=parsed.query,
            fragment="",
        )
    )
    return normalized, host, address is not None


class PublicURLPolicy:
    def __init__(self, resolver: Resolver = system_resolver, *, timeout_seconds: float = 3) -> None:
        if timeout_seconds <= 0:
            raise ValueError("DNS timeout must be positive")
        self._resolver = resolver
        self._timeout_seconds = timeout_seconds

    async def validate(self, url: str) -> str:
        normalized, host, literal = parse_public_url(url)
        if literal:
            return normalized
        try:
            async with asyncio.timeout(self._timeout_seconds):
                addresses = await self._resolver(host)
        except (TimeoutError, OSError):
            raise DomainError(
                "url_resolution_failed", "The public target could not be resolved"
            ) from None
        if not addresses or any(not is_public_address(address) for address in addresses):
            raise DomainError("url_not_public", "The hostname resolves to a non-public address")
        return normalized

    async def validate_query(self, query: str) -> None:
        # Search has no direct target fetch, but explicit URL/site operands still pass
        # the same admission policy before being disclosed to the provider.
        candidates = [match.group(0) for match in _QUERY_URL.finditer(query)]
        for match in _SITE_OPERATOR.finditer(query):
            value = match.group(1)
            candidates.append(value if "://" in value else f"https://{value}")
        for candidate in dict.fromkeys(candidates):
            await self.validate(candidate)
