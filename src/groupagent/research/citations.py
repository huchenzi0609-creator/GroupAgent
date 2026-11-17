"""Canonical citations and pure access-time checks over actual tool sources.

The caller supplies only successful, current-work research receipts. Page text,
publication dates, preformatted citation fields and the current clock cannot
provide or replace retrieval provenance. No provider or resolver is called.
"""

from __future__ import annotations

import re
from collections.abc import Iterable, Mapping
from datetime import UTC, datetime, timedelta
from typing import Any, Literal
from urllib.parse import urlsplit, urlunsplit

from pydantic import AwareDatetime, BaseModel, ConfigDict

from groupagent.domain import DomainError

from .models import ResearchSource

_URL = re.compile(
    r"https?://[^\s<>\]\"'，。；：！？、（）【】《》〈〉“”‘’「」『』〔〕［］｛｝]+",
    re.I,
)
_ACCESS = re.compile(r"访问|检索|查阅|读取|access(?:ed)?|retriev(?:ed|al)|fetch(?:ed)?", re.I)
_STAMP = re.compile(
    r"(?<!\d)(\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}(?::\d{2}(?:\.\d{1,6})?)?"
    r"(?:Z|[+-]\d{2}:?\d{2})?)(?![\d:+-])",
    re.I,
)


class CitationProvenance(BaseModel):
    """URL/time from verified artifact lineage, without invented body metrics.

    Construct only after the host validates the immutable public-image lineage.
    This is a citation binding, not evidence of reading the source page's body.
    Plain mappings still require the complete ResearchSource receipt contract.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)
    url: str
    retrieved_at: AwareDatetime
    operation: Literal["search", "fetch"] = "fetch"


def _source(
    value: ResearchSource | CitationProvenance | Mapping[str, Any],
) -> ResearchSource | CitationProvenance:
    try:
        # Revalidate instances too: model_copy/model_construct must not turn a
        # naive time into a local-clock assumption. Never expose body inputs in
        # a Pydantic exception or serialization warning.
        supplied = (
            value.model_dump(mode="python", warnings=False)
            if isinstance(value, ResearchSource | CitationProvenance)
            else value
        )
        source = (
            CitationProvenance.model_validate(supplied)
            if isinstance(value, CitationProvenance)
            else ResearchSource.model_validate(supplied)
        )
        parsed = urlsplit(source.url)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            raise ValueError("Invalid source URL")
        return source
    except (TypeError, ValueError):
        raise DomainError(
            "validation_failed",
            "Research source provenance is invalid or lacks an aware retrieval time",
        ) from None


def _url(value: str) -> str:
    parsed = urlsplit(value.rstrip(".,;，。；"))
    return urlunsplit((parsed.scheme.lower(), parsed.netloc.lower(), parsed.path, parsed.query, ""))


def _citation_urls(text: str) -> list[tuple[str, int, int]]:
    """Keep URL parentheses; remove only an identified Markdown link wrapper."""
    urls = []
    position = 0
    while match := _URL.search(text, position):
        value, end = match[0], match.end()
        position = end
        if text[: match.start()].rstrip().endswith("]("):
            depth = 1
            for index, char in enumerate(value):
                depth += (char == "(") - (char == ")")
                if depth == 0:
                    value, end = value[:index], match.start() + index
                    # Resume after this wrapper, not after the original token:
                    # an immediately following URL must still be checked.
                    position = end + 1
                    break
        urls.append((value, match.start(), end))
    return urls


def source_citation(value: ResearchSource | CitationProvenance | Mapping[str, Any]) -> str:
    """Render a second-precision UTC citation from unchanged aware retrieved_at."""
    source = _source(value)
    stamp = source.retrieved_at.astimezone(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")
    operation = "检索结果观察时间" if source.operation == "search" else "页面访问时间"
    return f"{source.url}（{operation} UTC：{stamp}）"


def _matches_time(record: str, match: re.Match[str], stamp: datetime, *, require_utc: bool) -> bool:
    token = match[1]
    prefix, suffix = record[: match.start()], record[match.end() :]
    if not _ACCESS.search(prefix):
        return False
    try:
        candidate = datetime.fromisoformat(token.upper().replace("Z", "+00:00"))
    except ValueError:
        return False
    if candidate.tzinfo is None:
        if not (
            re.search(r"(?<![A-Za-z0-9_])UTC\s*[：:（）()\s]*$", prefix, re.I)
            or re.match(r"\s*UTC\b", suffix, re.I)
        ):
            return False
        candidate = candidate.replace(tzinfo=UTC)
    # Mandatory canonical audit records disclose UTC. Optional displayed times
    # may use a truthful explicit local offset without adding another UTC copy.
    if require_utc and candidate.utcoffset() != timedelta(0):
        return False
    expected = stamp.astimezone(candidate.tzinfo)
    precision = re.sub(r"(?:Z|[+-]\d{2}:?\d{2})$", "", token, flags=re.I)
    if "." not in precision:
        expected = expected.replace(microsecond=0)
        if precision.count(":") == 1:
            expected = expected.replace(second=0)
    return candidate == expected


def _matches_utc_time(record: str, stamp: datetime) -> bool:
    return any(
        _matches_time(record, match, stamp, require_utc=True) for match in _STAMP.finditer(record)
    )


def validate_research_citations(
    text: str,
    sources: Iterable[ResearchSource | CitationProvenance | Mapping[str, Any]],
    *,
    require_access_time: bool = False,
    require_citation: bool = True,
) -> None:
    """Require actual provenance for every URL; optionally bind its UTC time.

    Access records use the source URL followed by an access/retrieval label and
    an ISO time with Z/+00:00, or an adjacent explicit UTC label. Each URL is
    checked separately on its own citation line, up to the next URL. Minute/second
    precision is compared at that precision; explicit fractional seconds must
    match exactly. Extra local-time displays are allowed alongside canonical UTC.
    With require_citation=False, a brief sourced answer may omit visible links;
    all supplied provenance is still validated. Any displayed retrieval timestamp
    alongside a link must be truthful even when displaying times is optional.
    The mandatory UTC audit contract remains opt-in.
    """
    if not isinstance(text, str):
        raise DomainError("validation_failed", "Research citation text must be a string")
    try:
        source_values = iter(sources)
    except TypeError:
        raise DomainError(
            "validation_failed", "Research source provenance must be a collection"
        ) from None
    observed: dict[str, list[datetime]] = {}
    for value in source_values:
        source = _source(value)
        observed.setdefault(_url(source.url), []).append(source.retrieved_at)
    matches = _citation_urls(text)
    if not matches:
        if require_citation or require_access_time:
            raise DomainError(
                "validation_failed", "Research delivery needs an actual source citation"
            )
        return
    cited = {_url(match[0]) for match in matches}
    if not cited.issubset(observed):
        raise DomainError("validation_failed", "A cited URL has no successful research provenance")
    verified: set[str] = set()
    for index, match in enumerate(matches):
        url = _url(match[0])
        end = matches[index + 1][1] if index + 1 < len(matches) else len(text)
        record = text[match[2] : end].split("\n", 1)[0][:1000]
        if not require_access_time:
            for time in _STAMP.finditer(record):
                # A nearby publication date is not a runtime access claim.
                clause = re.split(r"[，,;；。！？!?]", record[: time.start()])[-1]
                if _ACCESS.search(clause) and not any(
                    _matches_time(record, time, stamp, require_utc=False) for stamp in observed[url]
                ):
                    raise DomainError(
                        "validation_failed",
                        "A displayed retrieval time does not match actual research provenance",
                    )
            continue
        try:
            if any(_matches_utc_time(record, stamp) for stamp in observed[url]):
                verified.add(url)
        except ValueError:
            continue
    if require_access_time and verified != cited:
        raise DomainError(
            "validation_failed",
            "Each cited URL needs its own actual retrieval time in UTC; copy source.citation, optionally adding local time",
        )
