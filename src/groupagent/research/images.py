"""Discover existing public raster images and import scoped, worker-validated bytes.

No image generation, authenticated downloads, JavaScript, remote subresources or
general-purpose HTTP tool is exposed. Candidate IDs only authorize an exact URL
observed on one bounded page in the same work and actor scope.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import re
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from html.parser import HTMLParser
from typing import Literal
from urllib.parse import urljoin, urlsplit
from uuid import uuid4

import httpx
from pydantic import AwareDatetime, Field, TypeAdapter

from groupagent.artifacts import ArtifactScope
from groupagent.domain import DomainError
from groupagent.media import MediaRef, MediaService, MediaSource
from groupagent.tools import (
    EffectState,
    IdempotencyMode,
    ToolContext,
    ToolData,
    ToolDefinition,
    ToolGateway,
)
from groupagent.tools.models import ToolLedgerEntry

from .browser import FetchedHTML, PublicPageBroker
from .urls import Resolver, parse_public_url, system_resolver

# These errors arise after the image/page request pipeline has started. A
# limit, scope, receipt-validation or resource-admission error is not a request.
# Positive lists also preserve old failed observations without letting a chain
# of recovery errors consume the next operation's budget.
_PUBLIC_HTTP_FAILURES = frozenset(
    {
        "url_not_public",
        "url_resolution_failed",
        "public_image_timeout",
    }
)
_PAGE_ATTEMPT_FAILURES = _PUBLIC_HTTP_FAILURES | frozenset(
    {
        "browser_redirect_invalid",
        "browser_redirect_limit",
        "browser_access_blocked",
        "browser_fetch_failed",
        "browser_non_html",
        "browser_encoding_unsupported",
        "browser_page_limit",
        "browser_captcha",
        "public_image_page_encoding",
    }
)
_IMPORT_ATTEMPT_FAILURES = _PUBLIC_HTTP_FAILURES | frozenset(
    {
        "public_image_redirect_invalid",
        "public_image_redirect_limit",
        "public_image_access_blocked",
        "public_image_fetch_failed",
        "public_image_content_type",
        "public_image_encoding_unsupported",
        "public_image_size_limit",
        "image_size_limit",
        "image_pixel_limit",
        "image_output_limit",
        "invalid_image",
        "unsupported_image",
        "unsupported_video",
        "worker_integrity_failed",
        "media_worker_failed",
        "media_timeout",
        "invalid_worker_output",
        "worker_output_limit",
    }
)


class ImageCandidatesInput(ToolData):
    page_url: str = Field(min_length=1, max_length=8192)
    limit: int = Field(default=10, ge=1, le=20)


class ImageImportInput(ToolData):
    candidate_id: str = Field(pattern=r"^[a-f0-9]{32}$")


class PublicImageCandidate(ToolData):
    candidate_id: str = Field(pattern=r"^[a-f0-9]{32}$")
    image_url: str = Field(min_length=1, max_length=8192)
    page_url: str = Field(min_length=1, max_length=8192)
    page_final_url: str = Field(min_length=1, max_length=8192)
    page_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    retrieved_at: AwareDatetime
    discovery: Literal["img_src", "img_srcset", "open_graph", "twitter", "image_link"]
    alt_text: str = ""
    claimed_width: int | None = Field(default=None, gt=0, le=40_000_000)
    claimed_height: int | None = Field(default=None, gt=0, le=40_000_000)
    dimensions_verified: Literal[False] = False
    untrusted_content: Literal[True] = True


class ImageCandidatesOutput(ToolData):
    scope_fingerprint: str = Field(pattern=r"^[a-f0-9]{64}$")
    requested_url: str
    final_url: str
    title: str
    retrieved_at: AwareDatetime
    citation: str
    html_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    html_bytes_received: int = Field(ge=0, le=4 * 1024 * 1024)
    candidates: tuple[PublicImageCandidate, ...] = Field(max_length=20)
    filtered_candidate_count: int = Field(ge=0)
    truncated: bool
    untrusted_content: Literal[True] = True


class PublicImageRef(MediaRef):
    page_citation: str = Field(min_length=1, max_length=8500)
    image_citation: str = Field(min_length=1, max_length=8500)


def _access_citation(url: str, retrieved_at: datetime, *, image: bool = False) -> str:
    stamp = retrieved_at.astimezone(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")
    label = "图片访问时间" if image else "页面访问时间"
    return f"{url}（{label} UTC：{stamp}）"


@dataclass
class _Discovered:
    url: str
    discovery: Literal["img_src", "img_srcset", "open_graph", "twitter", "image_link"]
    alt_text: str = ""
    width: int | None = None
    height: int | None = None


def _dimension(value: str | None) -> int | None:
    if value and re.fullmatch(r"[0-9]{1,8}", value.strip()):
        number = int(value)
        return number if 0 < number <= 40_000_000 else None
    return None


def _srcset(value: str) -> list[tuple[str, int | None]]:
    """Bounded URL/width extraction; URLs containing commas are not split blindly."""
    candidates: list[tuple[str, int | None]] = []
    position = 0
    while position < len(value) and len(candidates) < 40:
        while position < len(value) and (value[position].isspace() or value[position] == ","):
            position += 1
        start = position
        while position < len(value) and not value[position].isspace():
            position += 1
        url = value[start:position]
        if url.endswith(","):
            candidates.append((url.rstrip(","), None))
            continue
        start = position
        while position < len(value) and value[position] != ",":
            position += 1
        descriptor = value[start:position].strip()
        width = _dimension(descriptor[:-1]) if descriptor.endswith("w") else None
        if url and (
            not descriptor or re.fullmatch(r"(?:[0-9]+w|[0-9]+(?:\.[0-9]+)?x)", descriptor)
        ):
            candidates.append((url, width))
    return candidates


class _ImagesHTML(HTMLParser):
    """Extract inert URLs/metadata only; scripts and HTML instructions are data."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.candidates: list[_Discovered] = []
        self.title = ""
        self.in_title = False
        self.truncated = False
        self.last_meta: _Discovered | None = None

    def add(self, value: _Discovered) -> None:
        if not value.url or len(value.url) > 8192:
            return
        if len(self.candidates) >= 128:
            self.truncated = True
            return
        value.alt_text = value.alt_text[:500]
        self.candidates.append(value)

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        values = {name: value or "" for name, value in attrs}
        if tag == "title":
            self.in_title = True
        elif tag == "img":
            width, height = _dimension(values.get("width")), _dimension(values.get("height"))
            alt = values.get("alt") or values.get("title", "")
            if values.get("src"):
                self.add(_Discovered(values["src"], "img_src", alt, width, height))
            # These lazy attributes are inert hints, not requests to execute JS.
            if values.get("data-src"):
                self.add(_Discovered(values["data-src"], "img_src", alt, width, height))
            for url, candidate_width in _srcset(values.get("srcset", "")):
                self.add(_Discovered(url, "img_srcset", alt, candidate_width))
        elif tag == "source" and values.get("type", "").startswith("image/"):
            for url, width in _srcset(values.get("srcset", "")):
                self.add(_Discovered(url, "img_srcset", width=width))
        elif tag == "meta":
            key = (values.get("property") or values.get("name", "")).lower()
            content = values.get("content", "")
            if key in {
                "og:image",
                "og:image:url",
                "og:image:secure_url",
                "twitter:image",
                "twitter:image:src",
            }:
                self.last_meta = _Discovered(
                    content, "twitter" if key.startswith("twitter:") else "open_graph"
                )
                self.add(self.last_meta)
            elif key in {"og:image:width", "og:image:height"} and self.last_meta is not None:
                if key.endswith(":width"):
                    self.last_meta.width = _dimension(content)
                else:
                    self.last_meta.height = _dimension(content)
        elif tag == "link" and "image_src" in values.get("rel", "").lower().split():
            self.add(_Discovered(values.get("href", ""), "image_link"))
        elif tag == "a":
            href = values.get("href", "")
            try:
                is_image = (
                    urlsplit(href).path.lower().endswith((".png", ".jpg", ".jpeg", ".gif", ".webp"))
                )
            except ValueError:
                return
            if is_image:
                self.add(_Discovered(href, "image_link", values.get("title", "")))

    def handle_endtag(self, tag: str) -> None:
        if tag == "title":
            self.in_title = False

    def handle_data(self, data: str) -> None:
        if self.in_title:
            self.title = (self.title + data)[:500]


def _parse_page(page: FetchedHTML) -> _ImagesHTML:
    encoding = "utf-8"
    declared = re.search(
        rb"charset\s*=\s*[\"']?([a-zA-Z0-9_-]+)", page.content[:8192], re.IGNORECASE
    )
    if declared:
        encoding = declared[1].decode().lower()
    if encoding not in {
        "utf-8",
        "utf8",
        "gbk",
        "gb2312",
        "gb18030",
        "big5",
        "windows-1252",
        "iso-8859-1",
    }:
        raise DomainError("public_image_page_encoding", "Source page encoding is unsupported")
    try:
        text = page.content.decode(encoding)
    except UnicodeError:
        raise DomainError("public_image_page_encoding", "Source page cannot be decoded") from None
    parser = _ImagesHTML()
    parser.feed(text)
    parser.close()
    return parser


class PublicImageService:
    """Small per-execution registry; imports cannot supply arbitrary resource URLs."""

    def __init__(
        self,
        media: MediaService,
        *,
        resolver: Resolver = system_resolver,
        client: httpx.AsyncClient | None = None,
        max_image_bytes: int | None = None,
        max_html_bytes: int = 1024 * 1024,
        timeout_seconds: float = 25,
    ) -> None:
        self.media, self.resolver, self.client = media, resolver, client
        requested_image_bytes = (
            media.max_image_bytes if max_image_bytes is None else max_image_bytes
        )
        if (
            type(requested_image_bytes) is not int
            or not 0 < requested_image_bytes <= 15 * 1024 * 1024
            or type(max_html_bytes) is not int
            or not 0 < max_html_bytes <= 4 * 1024 * 1024
            or not 0 < timeout_seconds <= 60
        ):
            raise ValueError("Public image limits exceed the reviewed bounded profile")
        self.max_image_bytes = min(requested_image_bytes, media.max_image_bytes)
        self.max_images = media.max_images
        self.max_import_attempts = min(12, self.max_images * 3)
        self.max_html_bytes, self.timeout_seconds = max_html_bytes, timeout_seconds
        self._candidates: dict[str, tuple[tuple[str, str, str], PublicImageCandidate]] = {}
        self._pages: dict[tuple[str, str, str, str], ImageCandidatesOutput] = {}
        self._imported: dict[str, MediaRef] = {}
        self._import_attempts_started = 0
        self._retained_image_hashes: set[str] = set()
        self._pages_started = 0
        self._import_lock = asyncio.Lock()

    def _scope(self, context: ToolContext) -> tuple[str, str, str]:
        scope = self.media.scope(context)
        return scope.group_id, scope.work_id, scope.actor_id

    @staticmethod
    def _scope_fingerprint(scope: tuple[str, str, str]) -> str:
        return hashlib.sha256(json.dumps(scope, ensure_ascii=False).encode()).hexdigest()

    async def _fetch(self, url: str, *, image: bool) -> FetchedHTML:
        # Owned HTTP clients are closed after every operation; no per-work socket
        # pools accumulate on the 2 GB single-worker runtime.
        broker = PublicPageBroker(
            None, resolver=self.resolver, client=self.client, max_bytes=self.max_html_bytes
        )
        try:
            async with asyncio.timeout(self.timeout_seconds):
                return await broker._fetch_resource(
                    url,
                    kind="image" if image else "html",
                    max_bytes=self.max_image_bytes if image else self.max_html_bytes,
                )
        except TimeoutError:
            raise DomainError("public_image_timeout", "Public image request timed out") from None
        finally:
            await broker.aclose()

    async def candidates(
        self, context: ToolContext, page_url: str, *, limit: int = 10
    ) -> ImageCandidatesOutput:
        scope = self._scope(context)
        if not 1 <= limit <= 20:
            raise DomainError("invalid_input", "Image candidate limit must be from one to twenty")
        try:
            canonical, _, _ = parse_public_url(page_url)
        except DomainError:
            raise DomainError(
                "public_image_page_url_invalid", "Provide a public standard-port source page URL"
            ) from None
        cache_key = (*scope, canonical)
        if cache_key in self._pages:
            cached = self._pages[cache_key]
            return cached.model_copy(
                update={
                    "candidates": cached.candidates[:limit],
                    "truncated": cached.truncated or len(cached.candidates) > limit,
                }
            )
        if self._pages_started >= 6 or len(self._candidates) >= 100:
            raise DomainError(
                "public_image_discovery_limit",
                "This execution has reached its public image discovery limit",
            )
        self._pages_started += 1
        page = await self._fetch(canonical, image=False)
        self._scope(context)
        parser = _parse_page(page)
        retrieved = page.retrieved_at or datetime.now(UTC)
        discovered = sorted(
            parser.candidates,
            key=lambda item: (item.width or 0) * (item.height or 1),
            reverse=True,
        )
        output: list[PublicImageCandidate] = []
        seen: set[str] = set()
        filtered = 0
        for item in discovered:
            try:
                url, _, _ = parse_public_url(urljoin(page.final_url, item.url))
            except (DomainError, ValueError):
                filtered += 1
                continue
            if url in seen:
                continue
            seen.add(url)
            if len(output) >= 20 or len(self._candidates) >= 100:
                parser.truncated = True
                continue
            candidate = PublicImageCandidate(
                candidate_id=uuid4().hex,
                image_url=url,
                page_url=canonical,
                page_final_url=page.final_url,
                page_sha256=page.sha256,
                retrieved_at=retrieved,
                discovery=item.discovery,
                alt_text=item.alt_text,
                claimed_width=item.width,
                claimed_height=item.height,
            )
            self._candidates[candidate.candidate_id] = (scope, candidate)
            output.append(candidate)
        result = ImageCandidatesOutput(
            scope_fingerprint=self._scope_fingerprint(scope),
            requested_url=canonical,
            final_url=page.final_url,
            title=parser.title.strip(),
            retrieved_at=retrieved,
            citation=_access_citation(page.final_url, retrieved),
            html_sha256=page.sha256,
            html_bytes_received=len(page.content),
            candidates=tuple(output),
            filtered_candidate_count=filtered,
            truncated=parser.truncated,
        )
        self._pages[cache_key] = result
        return result.model_copy(
            update={
                "candidates": result.candidates[:limit],
                "truncated": result.truncated or len(result.candidates) > limit,
            }
        )

    def candidate(self, context: ToolContext, candidate_id: str) -> PublicImageCandidate:
        scope = self._scope(context)
        entry = self._candidates.get(candidate_id)
        if entry is None or entry[0] != scope:
            raise DomainError(
                "public_image_candidate_unknown",
                "Discover an image candidate in this work before importing it",
            )
        return entry[1]

    def _public_reference(self, context: ToolContext, reference: MediaRef) -> PublicImageRef:
        """Build citation strings only from the immutable stored image provenance."""
        version = self.media.store.get(
            self.media.scope(context), reference.file_id, reference.version_id
        )
        provenance = version.provenance.get("public_image", {})
        try:
            if (
                provenance.get("sha256") != reference.sha256
                or version.sha256 != reference.sha256
                or provenance.get("final_url") != reference.source.source_url
            ):
                raise ValueError("Stored image provenance differs")
            page_url = provenance["page_final_url"]
            image_url = provenance["final_url"]
            if any(parse_public_url(url)[0] != url for url in (page_url, image_url)):
                raise ValueError("Stored source URL is not canonical")
            timestamp = TypeAdapter(AwareDatetime)
            page_time = timestamp.validate_python(provenance["page_retrieved_at"])
            image_time = timestamp.validate_python(provenance["retrieved_at"])
        except (DomainError, ValueError, KeyError, TypeError, AttributeError):
            raise DomainError(
                "public_image_evidence_invalid", "Stored image citation provenance is invalid"
            ) from None
        return PublicImageRef(
            **{name: getattr(reference, name) for name in MediaRef.model_fields},
            page_citation=_access_citation(page_url, page_time),
            image_citation=_access_citation(image_url, image_time, image=True),
        )

    def restore_candidates(
        self, context: ToolContext, entries: tuple[ToolLedgerEntry, ...]
    ) -> None:
        """Restore only a host gateway's scoped ledger, never model/workspace JSON.

        The registrar obtains this through export_ledger(context.scope), whose
        bound grant checks supply the group/work/actor association. The engine's
        existing trusted checkpoint restores that ledger before tool execution.
        """
        scope = self._scope(context)
        pages: set[tuple[str, str]] = set()
        failed_pages: set[str] = set()
        failed_imports: set[str] = set()
        imported: dict[str, PublicImageRef] = {}
        restored_candidates = dict(self._candidates)
        restored_imported = dict(self._imported)
        for entry in entries:
            receipt = entry.receipt
            if receipt.tool_name not in {"web_image_candidates", "web_image_import"}:
                continue
            if receipt.error_code in {"resource_deferred", "tool_in_progress"}:
                continue
            if not receipt.success:
                if (
                    receipt.tool_name == "web_image_candidates"
                    and receipt.error_code in _PAGE_ATTEMPT_FAILURES
                ):
                    failed_pages.add(receipt.idempotency_key)
                elif (
                    receipt.tool_name == "web_image_import"
                    and receipt.error_code in _IMPORT_ATTEMPT_FAILURES
                ):
                    failed_imports.add(receipt.idempotency_key)
                continue
            if receipt.effect_state not in {EffectState.SUCCEEDED, EffectState.NO_EFFECT}:
                raise DomainError(
                    "public_image_evidence_invalid", "Image receipt has no confirmed result"
                )
            if receipt.tool_name == "web_image_import":
                try:
                    reference = PublicImageRef.model_validate(receipt.output)
                except ValueError:
                    raise DomainError(
                        "public_image_evidence_invalid", "Image receipt result is invalid"
                    ) from None
                if (
                    reference.source.group_id,
                    reference.source.work_id,
                    reference.source.member_id,
                ) != scope or reference.source.origin_kind != "research":
                    raise DomainError(
                        "public_image_evidence_invalid", "Image receipt belongs to another scope"
                    )
                imported[reference.version_id] = reference
                continue
            try:
                result = ImageCandidatesOutput.model_validate(receipt.output)
                if result.scope_fingerprint != self._scope_fingerprint(scope):
                    raise ValueError("Candidate receipt belongs to another scope")
                for url in (result.requested_url, result.final_url):
                    if parse_public_url(url)[0] != url:
                        raise ValueError("Noncanonical page URL")
                for candidate in result.candidates:
                    if (
                        candidate.page_url != result.requested_url
                        or candidate.page_final_url != result.final_url
                        or candidate.page_sha256 != result.html_sha256
                        or candidate.retrieved_at != result.retrieved_at
                        or parse_public_url(candidate.image_url)[0] != candidate.image_url
                    ):
                        raise ValueError("Candidate differs from its page receipt")
                    existing = restored_candidates.get(candidate.candidate_id)
                    if existing is not None and existing != (scope, candidate):
                        raise ValueError("Candidate identity conflict")
                    if existing is None and len(restored_candidates) >= 100:
                        raise ValueError("Too many restored candidates")
                    restored_candidates[candidate.candidate_id] = (scope, candidate)
            except (DomainError, ValueError):
                raise DomainError(
                    "public_image_evidence_invalid", "Candidate receipt provenance is invalid"
                ) from None
            pages.add((result.requested_url, result.html_sha256))
        artifact_scope = ArtifactScope(*scope)
        for reference in imported.values():
            actual = self.media.load_ref(context, reference.file_id, reference.version_id)
            if self._public_reference(context, actual) != reference:
                raise DomainError(
                    "public_image_evidence_invalid",
                    "Imported image receipt differs from its stored reference",
                )
            version = self.media.store.get(artifact_scope, reference.file_id, reference.version_id)
            provenance = version.provenance.get("public_image", {})
            for candidate_id, (candidate_scope, candidate) in restored_candidates.items():
                if (
                    candidate_scope == scope
                    and candidate.image_url == provenance.get("requested_url")
                    and candidate.page_sha256 == provenance.get("page_sha256")
                ):
                    restored_imported[candidate_id] = actual
        self._candidates = restored_candidates
        self._imported = restored_imported
        self._retained_image_hashes.update(reference.sha256 for reference in imported.values())
        # Budget exhaustion stops new requests; it does not invalidate previously
        # successful immutable evidence or block reuse after a configuration
        # change. Repeated/legacy failed limit receipts remain untouched.
        self._pages_started = max(self._pages_started, min(6, len(pages) + len(failed_pages)))
        self._import_attempts_started = max(
            self._import_attempts_started,
            min(self.max_import_attempts, len(imported) + len(failed_imports)),
        )

    async def import_image(
        self, context: ToolContext, candidate_id: str, source: MediaSource
    ) -> PublicImageRef:
        async with self._import_lock:
            return await self._import_image(context, candidate_id, source)

    async def _import_image(
        self, context: ToolContext, candidate_id: str, source: MediaSource
    ) -> PublicImageRef:
        candidate = self.candidate(context, candidate_id)
        if (source.group_id, source.work_id, source.member_id) != self._scope(context):
            raise DomainError(
                "scope_violation", "Research image source belongs to another work or actor"
            )
        if not source.available:
            raise DomainError("media_unavailable", "Research image request source is unavailable")
        if source.declared_kind == "unsupported_video":
            raise DomainError(
                "public_image_source_invalid", "Video sources never begin image downloads"
            )
        if source.declared_kind not in {"image", "file"}:
            raise DomainError(
                "public_image_source_invalid", "Research image source must describe an image"
            )
        # Apply the existing revocation/source guard before HTTP or cached return,
        # in addition to the normal ingest checks after asynchronous download.
        await self.media._check(context, (source,))
        if candidate_id in self._imported:
            reference = self._imported[candidate_id]
            await self.media._content(context, reference)
            return self._public_reference(context, reference)
        if len(self._retained_image_hashes) >= self.max_images:
            raise DomainError(
                "public_image_import_limit",
                "This execution has reached its configured retained-image limit; use the imported images",
            )
        if self._import_attempts_started >= self.max_import_attempts:
            raise DomainError(
                "public_image_attempt_limit",
                "This execution has exhausted its public image download attempts; use the imported images or report completed results",
            )
        if (
            urlsplit(candidate.image_url)
            .path.lower()
            .endswith((".mp4", ".mov", ".avi", ".webm", ".mkv"))
        ):
            raise DomainError(
                "public_image_candidate_invalid", "Video targets are not image candidates"
            )
        self._import_attempts_started += 1
        image = await self._fetch(candidate.image_url, image=True)
        self._scope(context)
        source = source.model_copy(
            update={
                "declared_kind": "image",
                "origin_kind": "research",
                "source_url": image.final_url,
            }
        )
        reference = await self.media.ingest_image(
            context,
            source,
            image.content,
            research_provenance={
                "requested_url": candidate.image_url,
                "final_url": image.final_url,
                "page_url": candidate.page_url,
                "page_final_url": candidate.page_final_url,
                "page_sha256": candidate.page_sha256,
                "page_retrieved_at": candidate.retrieved_at.isoformat(),
                "retrieved_at": (image.retrieved_at or datetime.now(UTC)).isoformat(),
                "content_type": image.content_type,
                "sha256": image.sha256,
                "discovery": candidate.discovery,
            },
        )
        self._imported[candidate_id] = reference
        self._retained_image_hashes.add(reference.sha256)
        return self._public_reference(context, reference)


def register_public_image_tools(
    gateway: ToolGateway,
    service: PublicImageService,
    *,
    source_factory: Callable[[ToolContext, str], Awaitable[MediaSource]],
) -> None:
    async def candidates(
        arguments: ImageCandidatesInput, context: ToolContext
    ) -> ImageCandidatesOutput:
        service.restore_candidates(context, gateway.export_ledger(context.scope))
        return await service.candidates(context, arguments.page_url, limit=arguments.limit)

    async def import_image(arguments: ImageImportInput, context: ToolContext) -> PublicImageRef:
        service.restore_candidates(context, gateway.export_ledger(context.scope))
        candidate = service.candidate(context, arguments.candidate_id)
        source = await source_factory(context, candidate.image_url)
        return await service.import_image(context, arguments.candidate_id, source)

    gateway.register(
        ToolDefinition(
            name="web_image_candidates",
            description=(
                "Find existing public image URLs in a page returned by web_search or supplied by the user. "
                "Reads bounded HTML img/srcset, image links and social image metadata without scripts or logins. "
                "URLs, alt text and claimed dimensions are untrusted; import a candidate to verify real pixels and dimensions. "
                "No images are generated and no provider image-generation capability is required."
            ),
            input_model=ImageCandidatesInput,
            output_model=ImageCandidatesOutput,
            handler=candidates,
            risk_class="read_only",
            resource_scopes=("group", "work", "public_web"),
            timeout_seconds=service.timeout_seconds + 3,
            max_input_bytes=16384,
            max_output_bytes=600000,
            cost_meter="no_provider_charge",
            idempotency_mode=IdempotencyMode.READ_ONLY,
        )
    )
    gateway.register(
        ToolDefinition(
            name="web_image_import",
            description=(
                "Download one existing public raster image using a candidate_id from web_image_candidates in this work. "
                "Only JPEG/PNG/WebP/GIF under configured byte/pixel limits are accepted after full isolated-worker decoding. "
                "Returns a real scoped file_id/version_id and verified dimensions for artifact_deliver, image_analyze or "
                "document_generate section images. GIF analysis uses its static first frame. Source URLs are preserved; "
                "copy page_citation/image_citation for their actual UTC access times. "
                "discovery alone does not establish a reuse license."
            ),
            input_model=ImageImportInput,
            output_model=PublicImageRef,
            handler=import_image,
            risk_class="read_only",
            resource_scopes=("group", "work", "public_web", "artifacts"),
            timeout_seconds=service.timeout_seconds + 185,
            max_input_bytes=1024,
            max_output_bytes=40000,
            max_artifact_bytes=service.max_image_bytes,
            cost_meter="no_provider_charge",
            idempotency_mode=IdempotencyMode.READ_ONLY,
        )
    )
