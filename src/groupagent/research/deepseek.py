"""Fixed-origin Search/Fetch client; target pages are fetched by the provider.

Primary API references, accessed 2026-11-13:
https://api-docs.deepseek.com/guides/search/
https://api-docs.deepseek.com/api/search/
https://api-docs.deepseek.com/api/fetch/

No credential discovery, implicit retry, local target HTTP request or citation
generation occurs here. Provider text remains untrusted observation data.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
from datetime import UTC, datetime
from typing import Any, Literal, TypeVar

import httpx
from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, SecretStr, ValidationError

from groupagent.domain import DomainError

from .models import FetchInput, FetchOutput, ResearchSource, SearchInput, SearchOutput
from .urls import PublicURLPolicy, Resolver, system_resolver

SEARCH_ENDPOINT = "https://api.deepseek.com/v1/tools/search"
FETCH_ENDPOINT = "https://api.deepseek.com/v1/tools/fetch"
OutputT = TypeVar("OutputT", SearchOutput, FetchOutput)


class _SearchResult(BaseModel):
    model_config = ConfigDict(extra="ignore")
    title: str
    url: str
    snippet: str
    summary: str
    content: str
    date: AwareDatetime | None = None


class _SearchResponse(BaseModel):
    results: tuple[_SearchResult, ...] = Field(max_length=50)


class _FetchResponse(BaseModel):
    title: str
    content: str


def _truncate(value: str, limit: int) -> tuple[str, bool]:
    encoded = value.encode("utf-8")
    if len(encoded) <= limit:
        return value, False
    return encoded[:limit].decode("utf-8", errors="ignore"), True


class DeepSeekResearchClient:
    def __init__(
        self,
        api_key: str | SecretStr,
        *,
        client: httpx.AsyncClient | None = None,
        resolver: Resolver = system_resolver,
        timeout_seconds: float = 20,
        max_response_bytes: int = 2_000_000,
        max_output_bytes: int = 256_000,
        max_search_content_bytes: int = 16_000,
        max_search_text_bytes: int = 16_000,
        max_fetch_content_bytes: int = 100_000,
    ) -> None:
        if (
            min(
                timeout_seconds,
                max_response_bytes,
                max_output_bytes,
                max_search_content_bytes,
                max_search_text_bytes,
                max_fetch_content_bytes,
            )
            <= 0
        ):
            raise ValueError("Research limits must be positive")
        self._api_key = api_key if isinstance(api_key, SecretStr) else SecretStr(api_key)
        if not self._api_key.get_secret_value().strip():
            raise ValueError("An explicit research provider credential is required")
        self._owns_client = client is None
        self._client = client or httpx.AsyncClient(
            timeout=timeout_seconds,
            follow_redirects=False,
            trust_env=False,
        )
        self.fixture_transport = client is not None and isinstance(
            client._transport, httpx.MockTransport
        )
        self.url_policy = PublicURLPolicy(resolver, timeout_seconds=min(3, timeout_seconds))
        self.timeout_seconds = timeout_seconds
        self.max_output_bytes = max_output_bytes
        self._max_response_bytes = max_response_bytes
        self._search_content_bytes = max_search_content_bytes
        self._search_text_bytes = max_search_text_bytes
        self._fetch_content_bytes = max_fetch_content_bytes

    async def aclose(self) -> None:
        if self._owns_client:
            await self._client.aclose()

    async def _post(self, operation: Literal["search", "fetch"], body: dict[str, Any]) -> Any:
        endpoint = SEARCH_ENDPOINT if operation == "search" else FETCH_ENDPOINT
        try:
            async with asyncio.timeout(self.timeout_seconds):
                async with self._client.stream(
                    "POST",
                    endpoint,
                    json=body,
                    headers={"Authorization": f"Bearer {self._api_key.get_secret_value()}"},
                    follow_redirects=False,
                    timeout=self.timeout_seconds,
                ) as response:
                    status = response.status_code
                    errors = {
                        400: ("invalid_research_request", False),
                        401: ("auth_required", False),
                        402: ("provider_budget_exhausted", False),
                        403: ("fetch_forbidden", False),
                        404: ("page_unavailable", False),
                        408: ("research_timeout", True),
                        413: ("research_request_too_large", False),
                        429: ("rate_limited", True),
                        504: ("research_timeout", True),
                    }
                    if status in errors:
                        code, retryable = errors[status]
                        raise DomainError(
                            code,
                            "The research provider did not return a result",
                            retryable=retryable,
                        )
                    if 300 <= status < 400:
                        raise DomainError(
                            "provider_redirect_rejected", "Provider redirect was refused"
                        )
                    if status != 200:
                        raise DomainError(
                            "provider_unavailable",
                            "The research provider is unavailable",
                            retryable=status >= 500,
                        )
                    data = bytearray()
                    async for chunk in response.aiter_bytes():
                        data.extend(chunk)
                        if len(data) > self._max_response_bytes:
                            raise DomainError(
                                "research_response_too_large",
                                "The research response exceeded its limit",
                            )
        except (TimeoutError, httpx.TimeoutException):
            raise DomainError(
                "research_timeout", "The research request exceeded its deadline", retryable=True
            ) from None
        except httpx.HTTPError:
            raise DomainError(
                "provider_unavailable", "The research request failed", retryable=True
            ) from None
        try:
            return json.loads(data)
        except (TypeError, ValueError):
            raise DomainError(
                "invalid_research_response", "The research provider returned invalid JSON"
            ) from None

    def _bounded(self, output: OutputT) -> OutputT:
        if len(output.model_dump_json().encode("utf-8")) > self.max_output_bytes:
            raise DomainError(
                "research_output_too_large", "The normalized research result exceeds its limit"
            )
        return output

    def _source(
        self,
        *,
        url: str,
        title: str,
        content: str,
        operation: Literal["search", "fetch"],
        retrieved_at: datetime,
        snippet: str = "",
        summary: str = "",
        published_at: datetime | None = None,
        text_budget_bytes: int | None = None,
        include_content: bool = True,
    ) -> ResearchSource:
        limits = {
            "title": 2048,
            "snippet": 2048,
            "summary": 8192,
            "content": self._search_content_bytes
            if operation == "search"
            else self._fetch_content_bytes,
        }
        if not include_content:
            limits["content"] = 0
        original = {"title": title, "snippet": snippet, "summary": summary, "content": content}
        values: dict[str, str] = {}
        truncated = []
        for field, value in original.items():
            # Retain provider prefixes, never synthesize a summary. Search text
            # shares this budget in title/snippet/summary/content order; URL and
            # provenance remain intact and subject to the final output bound.
            limit = limits[field]
            if text_budget_bytes is not None:
                limit = min(limit, text_budget_bytes)
            try:
                values[field], clipped = _truncate(value, limit)
            except UnicodeError:
                raise DomainError(
                    "invalid_research_response", "The provider returned invalid Unicode text"
                ) from None
            if text_budget_bytes is not None:
                text_budget_bytes -= len(values[field].encode("utf-8"))
            if clipped:
                truncated.append(field)
        returned_content = values["content"].encode("utf-8")
        return ResearchSource(
            source_id=hashlib.sha256(url.encode("utf-8")).hexdigest(),
            url=url,
            operation=operation,
            evidence_kind="search_result" if operation == "search" else "provider_fetched_page",
            retrieved_at=retrieved_at,
            published_at=published_at,
            content_sha256=hashlib.sha256(returned_content).hexdigest(),
            content_bytes_received=len(content.encode("utf-8")),
            content_bytes_returned=len(returned_content),
            truncated_fields=tuple(truncated),
            title=values["title"],
            snippet=values["snippet"],
            summary=values["summary"],
            content=values["content"],
        )

    async def search(self, arguments: SearchInput) -> SearchOutput:
        try:
            async with asyncio.timeout(self.timeout_seconds):
                return await self._search(arguments)
        except TimeoutError:
            raise DomainError(
                "research_timeout", "The search exceeded its deadline", retryable=True
            ) from None

    async def _search(self, arguments: SearchInput) -> SearchOutput:
        await self.url_policy.validate_query(arguments.query)
        raw = await self._post("search", arguments.model_dump())
        retrieved = datetime.now(UTC)
        try:
            response = _SearchResponse.model_validate(raw)
        except ValidationError:
            raise DomainError(
                "invalid_research_response", "The search response did not match its schema"
            ) from None
        if len(response.results) > arguments.count:
            raise DomainError(
                "invalid_research_response", "The search response exceeded the requested count"
            )
        admitted = await asyncio.gather(
            *(self.url_policy.validate(result.url) for result in response.results),
            return_exceptions=True,
        )
        returned_count = sum(not isinstance(url, BaseException) for url in admitted)
        source_text_bytes = self._search_text_bytes // max(1, returned_count)
        sources = []
        filtered = 0
        for result, url in zip(response.results, admitted, strict=True):
            if isinstance(url, BaseException):
                filtered += 1
                continue
            sources.append(
                self._source(
                    url=url,
                    title=result.title,
                    snippet=result.snippet,
                    summary=result.summary,
                    content=result.content,
                    operation="search",
                    retrieved_at=retrieved,
                    published_at=result.date,
                    text_budget_bytes=source_text_bytes,
                    include_content=arguments.need_content,
                )
            )
        return self._bounded(
            SearchOutput(
                query=arguments.query,
                retrieved_at=retrieved,
                results=tuple(sources),
                provider_result_count=len(response.results),
                filtered_result_count=filtered,
            )
        )

    async def fetch(self, arguments: FetchInput) -> FetchOutput:
        try:
            async with asyncio.timeout(self.timeout_seconds):
                return await self._fetch(arguments)
        except TimeoutError:
            raise DomainError(
                "research_timeout", "The fetch exceeded its deadline", retryable=True
            ) from None

    async def _fetch(self, arguments: FetchInput) -> FetchOutput:
        url = await self.url_policy.validate(arguments.url)
        raw = await self._post("fetch", {"url": url})
        retrieved = datetime.now(UTC)
        try:
            response = _FetchResponse.model_validate(raw)
        except ValidationError:
            raise DomainError(
                "invalid_research_response", "The fetch response did not match its schema"
            ) from None
        if not response.content.strip():
            raise DomainError("page_unavailable", "The provider did not return usable page content")
        return self._bounded(
            FetchOutput(
                requested_url=url,
                source=self._source(
                    url=url,
                    title=response.title,
                    content=response.content,
                    operation="fetch",
                    retrieved_at=retrieved,
                ),
            )
        )
