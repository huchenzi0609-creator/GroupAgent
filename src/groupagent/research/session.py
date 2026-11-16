"""One work-scoped public-read cache, singleflight, and local evidence reader."""

from __future__ import annotations

import asyncio
import hashlib
import json
from collections import OrderedDict
from collections.abc import Awaitable, Callable, Iterable
from typing import TypeVar, cast

from groupagent.domain import DomainError
from groupagent.tools import ToolContext
from groupagent.tools.models import ToolLedgerEntry, ToolReceipt

from .deepseek import DeepSeekResearchClient
from .evidence import EvidenceStore, project_text, scope_key, utf8_prefix
from .models import (
    FetchInput,
    FetchOutput,
    ResearchSource,
    SearchInput,
    SearchOutput,
    SourceReadInput,
    SourceReadOutput,
)
from .urls import parse_public_url

ReadOutput = SearchOutput | FetchOutput
OutputT = TypeVar("OutputT", SearchOutput, FetchOutput)


class ResearchSession:
    """Successful observations are reused; unknown/failed attempts never become evidence.

    Authority and monetary admission stay with the gateway/loader. Every gateway
    invocation authorizes before entering this class, including cache hits.
    Cache keys describe actual provider arguments, never a model call ID.
    """

    def __init__(
        self,
        client: DeepSeekResearchClient,
        evidence_store: EvidenceStore | None = None,
        *,
        question: str = "",
        max_cache_entries: int = 64,
    ) -> None:
        if max_cache_entries <= 0:
            raise ValueError("Research cache capacity must be positive")
        self.client = client
        self.evidence_store = evidence_store or EvidenceStore()
        self.question = question
        self.max_cache_entries = max_cache_entries
        self._cache: OrderedDict[tuple[str, str], ReadOutput] = OrderedDict()
        self._sources: dict[tuple[str, str], ResearchSource] = {}
        self._inflight: dict[tuple[str, str], asyncio.Task[ReadOutput]] = {}

    @staticmethod
    def request_key(operation: str, arguments: SearchInput | FetchInput) -> str:
        body = arguments.model_dump(mode="json", exclude={"refresh"})
        if operation == "fetch":
            body["url"] = parse_public_url(str(body["url"]))[0]
        return hashlib.sha256(
            json.dumps(
                {"version": 1, "provider": "deepseek", "operation": operation, "input": body},
                sort_keys=True,
                ensure_ascii=False,
                separators=(",", ":"),
            ).encode()
        ).hexdigest()

    @staticmethod
    def _result_sources(output: ReadOutput) -> tuple[ResearchSource, ...]:
        return output.results if isinstance(output, SearchOutput) else (output.source,)

    def _remember(self, context: ToolContext, key: str, output: ReadOutput) -> None:
        scoped = scope_key(context)
        for source in self._result_sources(output):
            source_key = (scoped, source.source_id)
            if self._sources.get(source_key) != source:
                self.evidence_store.source_content(context, source)
                self._sources[source_key] = source
        self._cache[(scoped, key)] = output
        self._cache.move_to_end((scoped, key))
        while len(self._cache) > self.max_cache_entries:
            self._cache.popitem(last=False)

    def restore(
        self,
        receipts: Iterable[ToolReceipt | ToolLedgerEntry],
        context: ToolContext,
        *,
        request_key: str | None = None,
    ) -> None:
        """Rebuild from successful host receipts, validating manifests before reuse.

        Legacy receipts without manifests remain valid historical observations but
        cannot masquerade as locally readable complete evidence.
        """
        for item in receipts:
            receipt = item.receipt if isinstance(item, ToolLedgerEntry) else item
            if not receipt.success or receipt.tool_name not in {"web_search", "web_fetch"}:
                continue
            model = SearchOutput if receipt.tool_name == "web_search" else FetchOutput
            output = model.model_validate(receipt.output or {})
            if request_key is not None and output.request_key != request_key:
                continue
            if not output.request_key or any(
                source.evidence_ref is None for source in self._result_sources(output)
            ):
                continue
            cache_key = (scope_key(context), output.request_key)
            if self._cache.get(cache_key) == output:
                continue
            self._remember(context, output.request_key, output)

    def _project_source(
        self,
        source: ResearchSource,
        query: str,
        text_budget: int,
        *,
        include_content: bool = True,
    ) -> ResearchSource:
        # Reserve body space before metadata. Empty metadata shares flow to the
        # body, rather than a long snippet silently erasing requested evidence.
        remaining = text_budget
        values: dict[str, str] = {}
        clipped = []
        for name, fraction in (("title", 8), ("snippet", 8), ("summary", 4)):
            value = getattr(source, name)
            values[name] = utf8_prefix(value, text_budget // fraction)
            remaining -= len(values[name].encode())
            if values[name] != value:
                clipped.append(name)
        content = project_text(source.content, query, remaining) if include_content else ""
        values["content"] = content
        if content != source.content:
            clipped.append("content")
        return source.model_copy(
            update={
                **values,
                "content_sha256": hashlib.sha256(content.encode()).hexdigest(),
                "content_bytes_returned": len(content.encode()),
                "truncated_fields": tuple(dict.fromkeys((*source.truncated_fields, *clipped))),
            }
        )

    async def _read_once(
        self,
        context: ToolContext,
        key: str,
        loader: Callable[[], Awaitable[OutputT]],
        projector: Callable[[OutputT], OutputT],
        *,
        refresh: bool = False,
    ) -> OutputT:
        cache_key = (scope_key(context), key)
        if context.cancelled.is_set():
            raise DomainError("cancelled", "Research work was cancelled")
        if context.deadline_monotonic is not None and (
            context.deadline_monotonic <= asyncio.get_running_loop().time()
        ):
            raise DomainError("time_budget_exhausted", "Research work exceeded its deadline")
        if refresh:
            self._cache.pop(cache_key, None)
        cached = self._cache.get(cache_key)
        if cached is not None:
            # Detect deleted or altered files before promising a continuation.
            for source in self._result_sources(cached):
                self.evidence_store.source_content(context, source)
            self._cache.move_to_end(cache_key)
            return cached  # type: ignore[return-value]
        task = self._inflight.get(cache_key)
        if task is None:

            async def load() -> OutputT:
                received = await loader()
                saved: ReadOutput
                if isinstance(received, SearchOutput):
                    saved = received.model_copy(
                        update={
                            "results": tuple(
                                self.evidence_store.save(context, s) for s in received.results
                            ),
                            "request_key": key,
                        }
                    )
                else:
                    saved = received.model_copy(
                        update={
                            "source": self.evidence_store.save(context, received.source),
                            "request_key": key,
                        }
                    )
                projected = projector(cast(OutputT, saved))
                self.client._bounded(projected)
                self._remember(context, key, projected)
                return projected

            task = asyncio.create_task(load())
            self._inflight[cache_key] = task
        try:
            return await task  # type: ignore[return-value]
        finally:
            if task.done() and self._inflight.get(cache_key) is task:
                self._inflight.pop(cache_key, None)

    async def search(
        self,
        arguments: SearchInput,
        context: ToolContext,
        loader: Callable[[], Awaitable[SearchOutput]],
    ) -> SearchOutput:
        key = self.request_key("search", arguments)

        def project(output: SearchOutput) -> SearchOutput:
            per_source = self.client._search_text_bytes // max(1, len(output.results))
            return output.model_copy(
                update={
                    "results": tuple(
                        self._project_source(
                            source,
                            self.question or arguments.query,
                            per_source,
                            include_content=arguments.need_content,
                        )
                        for source in output.results
                    ),
                }
            )

        return await self._read_once(context, key, loader, project, refresh=arguments.refresh)

    async def fetch(
        self,
        arguments: FetchInput,
        context: ToolContext,
        loader: Callable[[], Awaitable[FetchOutput]],
    ) -> FetchOutput:
        key = self.request_key("fetch", arguments)
        return await self._read_once(
            context,
            key,
            loader,
            lambda output: output.model_copy(
                update={
                    "source": self._project_source(
                        output.source,
                        self.question,
                        self.client._fetch_content_bytes,
                    ),
                }
            ),
            refresh=arguments.refresh,
        )

    def read(self, arguments: SourceReadInput, context: ToolContext) -> SourceReadOutput:
        source = self._sources.get((scope_key(context), arguments.source_id))
        if source is None:
            raise DomainError("research_source_unknown", "Read a source discovered in this work")
        content = self.evidence_store.source_content(context, source)
        raw = content.encode()
        if arguments.offset > len(raw):
            raise DomainError("invalid_input", "Source offset is beyond the saved evidence")
        next_offset = None
        if arguments.query:
            excerpt = project_text(content, arguments.query, arguments.limit)
        else:
            start = arguments.offset
            # A caller may choose a byte inside a multibyte code point. Advance
            # to its next UTF-8 boundary instead of returning invalid Unicode.
            while start < len(raw) and raw[start] & 0xC0 == 0x80:
                start += 1
            excerpt = raw[start : start + arguments.limit].decode("utf-8", errors="ignore")
            if start < len(raw) and not excerpt:
                raise DomainError("invalid_input", "Increase limit to fit a UTF-8 code point")
            end = start + len(excerpt.encode())
            next_offset = end if end < len(raw) else None
        clipped = excerpt != content
        projected = source.model_copy(
            update={
                "content": excerpt,
                "content_sha256": hashlib.sha256(excerpt.encode()).hexdigest(),
                "content_bytes_returned": len(excerpt.encode()),
                "truncated_fields": tuple(
                    dict.fromkeys(
                        (
                            *(field for field in source.truncated_fields if field != "content"),
                            *(("content",) if clipped else ()),
                        )
                    )
                ),
            }
        )
        return SourceReadOutput(source=projected, next_offset=next_offset)
