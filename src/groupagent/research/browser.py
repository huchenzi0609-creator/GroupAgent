"""Public-page broker plus an offline browser worker. No cookies or broad egress."""

from __future__ import annotations

import asyncio
import hashlib
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from urllib.parse import urljoin, urlsplit

import httpx

from groupagent.domain import DomainError
from groupagent.media.isolation import IsolatedMediaWorker
from groupagent.tools import ToolContext, ToolData

from .urls import Resolver, is_public_address, parse_public_url, system_resolver


@dataclass(frozen=True)
class FetchedHTML:
    requested_url: str
    final_url: str
    content: bytes
    sha256: str
    status_code: int


class PublicPageBroker:
    """Only trusted approved hostnames can be fetched. Every connection pins public DNS."""

    def __init__(
        self,
        allowed_hosts: frozenset[str],
        *,
        resolver: Resolver = system_resolver,
        client: httpx.AsyncClient | None = None,
        max_bytes: int = 4 * 1024 * 1024,
    ):
        self.allowed_hosts = frozenset(host.lower() for host in allowed_hosts)
        self.resolver = resolver
        self.client = client or httpx.AsyncClient(
            trust_env=False, follow_redirects=False, timeout=20
        )
        self._owned = client is None
        self.max_bytes = min(4 * 1024 * 1024, max(1, max_bytes))

    async def aclose(self) -> None:
        if self._owned:
            await self.client.aclose()

    @asynccontextmanager
    async def _get(self, target: httpx.URL, host: str) -> AsyncIterator[httpx.Response]:
        # A borrowed client may have default API headers/cookies. Construct a new
        # request instead of client.build_request/stream, and disable its auth hook.
        request = httpx.Request(
            "GET",
            target,
            headers={
                "Host": host,
                "User-Agent": "GroupAgentResearch/0.1",
                "Accept": "text/html,application/xhtml+xml",
                "Accept-Encoding": "identity",
            },
            extensions={
                "sni_hostname": host,
                "timeout": {"connect": 20, "read": 20, "write": 20, "pool": 20},
            },
        )
        response = await self.client.send(request, auth=None, follow_redirects=False, stream=True)
        try:
            yield response
        finally:
            await response.aclose()

    async def fetch(self, url: str) -> FetchedHTML:
        requested = url
        for _ in range(4):
            canonical, host, literal = parse_public_url(url)
            if host not in self.allowed_hosts:
                raise DomainError(
                    "browser_host_not_authorized",
                    "Browser target is not in this work's approved source hosts",
                )
            if urlsplit(canonical).path.lower().endswith((".mp4", ".mov", ".avi", ".webm", ".mkv")):
                raise DomainError("unsupported_video", "Video targets are never fetched")
            try:
                addresses = (host,) if literal else await asyncio.wait_for(self.resolver(host), 3)
            except (OSError, TimeoutError):
                raise DomainError(
                    "url_resolution_failed", "Public source could not be resolved"
                ) from None
            if not addresses or any(not is_public_address(address) for address in addresses):
                raise DomainError(
                    "url_not_public", "Source resolved to a private or special address"
                )
            # Connect to a checked IP rather than resolving the hostname a second time.
            target = httpx.URL(canonical).copy_with(host=addresses[0])
            try:
                async with self._get(target, host) as response:
                    if response.status_code in {301, 302, 303, 307, 308}:
                        location = response.headers.get("location")
                        if not location:
                            raise DomainError(
                                "browser_redirect_invalid", "Source redirect has no target"
                            )
                        url = urljoin(canonical, location)
                        continue
                    if response.status_code in {401, 403, 429}:
                        raise DomainError(
                            "browser_access_blocked",
                            "Source requires access approval or has rate-limited this request",
                        )
                    if response.status_code != 200:
                        raise DomainError(
                            "browser_fetch_failed", "Source page did not return success"
                        )
                    mime = response.headers.get("content-type", "").split(";", 1)[0].strip().lower()
                    if mime not in {"text/html", "application/xhtml+xml"}:
                        raise DomainError(
                            "browser_non_html", "Browser broker accepts HTML pages only"
                        )
                    length = response.headers.get("content-length")
                    if length and (not length.isdecimal() or int(length) > self.max_bytes):
                        raise DomainError(
                            "browser_page_limit", "Source page exceeds the bounded HTML limit"
                        )
                    content = bytearray()
                    async for chunk in response.aiter_bytes(chunk_size=65536):
                        content.extend(chunk)
                        if len(content) > self.max_bytes:
                            raise DomainError(
                                "browser_page_limit", "Source page exceeds the bounded HTML limit"
                            )
                    text = bytes(content).lower()
                    if any(
                        marker in text
                        for marker in (
                            b"g-recaptcha",
                            b"hcaptcha",
                            b"cf-chl-",
                            b"verify you are human",
                            b"captcha challenge",
                        )
                    ):
                        raise DomainError(
                            "browser_captcha",
                            "A CAPTCHA or access challenge must be handled by the user",
                        )
                    return FetchedHTML(
                        requested,
                        canonical,
                        bytes(content),
                        hashlib.sha256(content).hexdigest(),
                        response.status_code,
                    )
            except httpx.HTTPError:
                raise DomainError(
                    "browser_fetch_failed", "Public source request did not complete"
                ) from None
        raise DomainError("browser_redirect_limit", "Source exceeded the redirect limit")


class BrowserObservation(ToolData):
    requested_url: str
    final_url: str
    title: str
    text: str
    retrieved_at: datetime
    html_sha256: str
    screenshot_sha256: str
    truncated: bool
    untrusted_content: bool = True
    external_resources_blocked: bool = True
    screenshot_file_id: str | None = None
    screenshot_version_id: str | None = None
    javascript_enabled: bool = False
    active_content_removed: int = 0


class ControlledBrowser:
    def __init__(self, broker: PublicPageBroker, worker: IsolatedMediaWorker):
        self.broker, self.worker = broker, worker

    async def open(self, context: ToolContext, url: str) -> tuple[BrowserObservation, bytes]:
        if context.cancelled.is_set():
            raise DomainError("cancelled", "Browser work was cancelled")
        page = await self.broker.fetch(url)
        result, outputs = await self.worker.run(
            context,
            "browser_snapshot",
            (page.content,),
            parameters={"url": page.final_url},
            outputs=("screenshot.png",),
            timeout_seconds=90,
        )
        if result.get("access_blocked"):
            raise DomainError("browser_captcha", "The rendered page presents an access challenge")
        return BrowserObservation(
            requested_url=page.requested_url,
            final_url=page.final_url,
            title=result["title"],
            text=result["text"],
            retrieved_at=datetime.now(UTC),
            html_sha256=page.sha256,
            screenshot_sha256=hashlib.sha256(outputs[0]).hexdigest(),
            truncated=result["truncated"],
            active_content_removed=result.get("active_content_removed", 0),
        ), outputs[0]
