"""Research observations carry provenance, not permission or proof of a conclusion."""

from __future__ import annotations

from typing import Literal

from pydantic import AwareDatetime, Field, field_validator

from groupagent.tools import ToolData


class SearchInput(ToolData):
    query: str = Field(min_length=1, max_length=128)
    count: int = Field(default=5, ge=1, le=10)
    need_content: bool = False
    refresh: bool = Field(
        default=False,
        description="Request a fresh external observation instead of reusing this work's saved evidence",
    )

    @field_validator("query")
    @classmethod
    def nonblank_query(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("A search query cannot be blank")
        try:
            value.encode("utf-8")
        except UnicodeError:
            raise ValueError("A search query must be valid Unicode text") from None
        return value.strip()


class FetchInput(ToolData):
    url: str = Field(min_length=1, max_length=8192)
    refresh: bool = Field(
        default=False,
        description="Request a fresh external observation instead of reusing this work's saved evidence",
    )


class EvidenceRef(ToolData):
    """Host-created immutable local evidence reference, never a filesystem capability."""

    version: Literal[1] = 1
    source_id: str = Field(pattern=r"^[a-f0-9]{64}$")
    scope_key: str = Field(pattern=r"^[a-f0-9]{64}$")
    content_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    content_bytes: int = Field(ge=0, le=2_000_000)


class ResearchSource(ToolData):
    source_id: str
    url: str
    title: str
    retrieved_at: AwareDatetime
    citation: str = Field(
        default="",
        description="Client-built URL and actual retrieval time in UTC; copy this citation, optionally adding local time",
    )
    provider: Literal["deepseek", "public_http"] = "deepseek"
    operation: Literal["search", "fetch"]
    evidence_kind: Literal[
        "search_result", "provider_fetched_page", "public_image_page", "public_image"
    ]
    untrusted_content: Literal[True] = True
    published_at: AwareDatetime | None = None
    snippet: str = ""
    summary: str = ""
    content: str = ""
    content_sha256: str = Field(
        description=(
            "SHA-256 of returned, possibly truncated, text; direct public image/page evidence hashes fetched source bytes"
        )
    )
    content_bytes_received: int = Field(ge=0)
    content_bytes_returned: int = Field(ge=0)
    truncated_fields: tuple[str, ...] = ()
    # DeepSeek fetch has no final URL. Direct public HTTP records checked redirects.
    final_url: str | None = None
    evidence_ref: EvidenceRef | None = None


class SearchOutput(ToolData):
    query: str
    retrieved_at: AwareDatetime
    results: tuple[ResearchSource, ...]
    provider_result_count: int = Field(ge=0)
    filtered_result_count: int = Field(ge=0)
    untrusted_content: Literal[True] = True
    budget_receipt: str | None = None
    request_key: str | None = Field(default=None, pattern=r"^[a-f0-9]{64}$")


class FetchOutput(ToolData):
    requested_url: str
    source: ResearchSource
    untrusted_content: Literal[True] = True
    budget_receipt: str | None = None
    request_key: str | None = Field(default=None, pattern=r"^[a-f0-9]{64}$")


class SourceReadInput(ToolData):
    source_id: str = Field(pattern=r"^[a-f0-9]{64}$")
    query: str = Field(default="", max_length=512)
    offset: int = Field(default=0, ge=0, description="UTF-8 byte offset in the saved source")
    limit: int = Field(default=12_000, ge=1, le=32_000, description="Maximum returned UTF-8 bytes")


class SourceReadOutput(ToolData):
    source: ResearchSource
    next_offset: int | None = Field(default=None, ge=0)
    untrusted_content: Literal[True] = True
