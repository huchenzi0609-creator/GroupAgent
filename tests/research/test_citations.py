"""Pure citation provenance checks; no provider, resolver, network or database."""

from __future__ import annotations

import hashlib
from datetime import UTC, datetime

import httpx
import pytest

from groupagent.domain import DomainError
from groupagent.research import (
    DeepSeekResearchClient,
    FetchInput,
    ResearchSource,
    SearchInput,
    source_citation,
    validate_research_citations,
)

URL = "https://docs.python.org/3/howto/sorting.html"
SECOND_URL = "https://docs.python.org/zh-cn/3.14/howto/sorting.html"
RECEIVED = "2026-11-14T16:08:23.973776Z"


def source(url=URL, retrieved=RECEIVED, *, operation="fetch", **changes):
    return ResearchSource(
        source_id=hashlib.sha256(url.encode()).hexdigest(),
        url=url,
        title="Synthetic public source",
        retrieved_at=retrieved,
        operation=operation,
        evidence_kind="provider_fetched_page" if operation == "fetch" else "search_result",
        published_at="2020-01-01T00:00:00Z",
        content="Synthetic source bytes",
        content_sha256=hashlib.sha256(b"Synthetic source bytes").hexdigest(),
        content_bytes_received=22,
        content_bytes_returned=22,
        **changes,
    )


def test_canonical_utc_citation_keeps_the_original_aware_time_and_ignores_other_dates():
    actual = source(retrieved="2026-11-15T00:08:23.973776+08:00", citation="Forged date 2099")
    line = source_citation(actual)
    assert line == URL + "（页面访问时间 UTC：2026-11-14T16:08:23Z）"
    assert actual.retrieved_at.isoformat() == "2026-11-15T00:08:23.973776+08:00"
    assert "2020" not in line and "2099" not in line
    validate_research_citations(line, [actual], require_access_time=True)


def test_old_web01_local_time_was_correct_but_utc_record_was_not_disclosed():
    actual = source()
    local = datetime.fromisoformat("2026-11-15T00:08:23.973776+08:00")
    assert local.astimezone(UTC) == actual.retrieved_at
    answer = "访问时间：2026-11-15 00:08（Asia/Shanghai）\n" + URL
    with pytest.raises(DomainError, match="own actual retrieval time in UTC"):
        validate_research_citations(answer, [actual], require_access_time=True)
    validate_research_citations(answer, [actual], require_access_time=False)
    validate_research_citations(
        answer + "\n" + source_citation(actual), [actual], require_access_time=True
    )


@pytest.mark.parametrize(
    "time",
    [
        "访问时间：2026-11-14T16:08:23Z",
        "访问时间：2026-11-14T16:08:23+00:00",
        "访问时间 UTC：2026-11-14 16:08:23",
        "访问时间（UTC）：2026-11-14 16:08:23",
        "访问时间：2026-11-14 16:08:23 UTC",
        "访问时间：2026-11-14T16:08Z",
        "访问时间：2026-11-14T16:08:23.973776Z",
    ],
)
def test_utc_equivalent_forms_match_the_actual_instant_at_declared_precision(time):
    validate_research_citations(URL + "（" + time + "）", [source()], require_access_time=True)


@pytest.mark.parametrize(
    "time",
    [
        "访问时间：2026-11-14",
        "访问时间：2026-11-14 16:08:23",
        "访问时间 UTC：2026-11-14 17:08:23",
        "访问时间：2026-11-15T00:08:23+08:00",
        "访问时间：2026-11-14T16:08:23.999999Z",
        "访问时间：2020-01-01T00:00:00Z",
        "访问时间：2099-01-01T00:00:00Z",
        "发布时间 UTC：2026-11-14 16:08:23",
    ],
)
def test_date_or_unlabelled_local_wrong_or_published_time_cannot_replace_retrieval(time):
    with pytest.raises(DomainError, match="own actual retrieval time in UTC"):
        validate_research_citations(URL + "（" + time + "）", [source()], require_access_time=True)


def test_each_actual_cited_url_needs_its_corresponding_time_not_any_time_in_answer():
    first = source(retrieved="2026-11-14T16:08:21Z")
    second = source(SECOND_URL)
    # A valid first URL/time cannot cover the second URL on the next line.
    with pytest.raises(DomainError):
        validate_research_citations(
            source_citation(first) + "\n" + SECOND_URL, [first, second], require_access_time=True
        )
    # Repeating the first URL's timestamp beside the second is also false provenance.
    with pytest.raises(DomainError):
        validate_research_citations(
            source_citation(first) + "\n" + SECOND_URL + "（访问时间：2026-11-14T16:08:21Z）",
            [first, second],
            require_access_time=True,
        )
    validate_research_citations(
        source_citation(first) + "\n" + source_citation(second),
        [first, second],
        require_access_time=True,
    )
    # Unused search results need no fabricated claim of page access.
    validate_research_citations(source_citation(first), [first, second], require_access_time=True)


def test_time_on_another_line_or_from_a_url_prefix_cannot_be_borrowed():
    with pytest.raises(DomainError):
        validate_research_citations(
            URL + "\n访问时间 UTC：2026-11-14T16:08:23Z", [source()], require_access_time=True
        )
    with pytest.raises(DomainError, match="no successful research provenance"):
        validate_research_citations(
            URL + "-unverified（访问时间：2026-11-14T16:08:23Z）",
            [source()],
            require_access_time=True,
        )


def test_bad_timestamp_does_not_hide_later_actual_canonical_record():
    text = URL + "（访问时间：2026-99-99T16:08:23Z；实际访问时间 UTC：2026-11-14T16:08:23Z）"
    validate_research_citations(text, [source()], require_access_time=True)


@pytest.mark.parametrize(
    "url",
    [
        "https://en.wikipedia.org/wiki/Python_(programming_language)",
        "https://example.com/nested_(one_(two))",
        "https://example.com/source?q=(value)",
    ],
)
def test_parenthesized_source_citation_roundtrips_plain_and_markdown_wrapped(url):
    actual = source(url)
    plain = source_citation(actual)
    validate_research_citations(plain, [actual], require_access_time=True)
    markdown = plain.replace(url, f"[Source]({url})", 1)
    validate_research_citations(markdown, [actual], require_access_time=True)


def test_literal_unbalanced_url_parenthesis_is_not_silently_removed():
    actual = source("https://example.com/source)")
    plain = source_citation(actual)
    validate_research_citations(plain, [actual], require_access_time=True)
    # Angle-bracket Markdown destinations preserve literal unmatched parentheses.
    validate_research_citations(
        plain.replace(actual.url, f"[Source](<{actual.url}>)", 1),
        [actual],
        require_access_time=True,
    )


@pytest.mark.parametrize("suffix", ["-unverified", "?unverified=1", ")"])
def test_parenthesized_similar_prefix_without_exact_provenance_is_rejected(suffix):
    url = "https://en.wikipedia.org/wiki/Python_(programming_language)"
    with pytest.raises(DomainError, match="no successful research provenance"):
        validate_research_citations(
            source_citation(source(url + suffix)), [source(url)], require_access_time=True
        )


def test_markdown_unknown_prefix_and_url_immediately_after_wrapper_are_not_hidden():
    url = "https://en.wikipedia.org/wiki/Python_(programming_language)"
    actual = source(url)
    for text in (
        f"[Source]({url}-unverified)（访问时间：2026-11-14T16:08:23Z）",
        f"[Source]({url})https://unknown.example/source（访问时间：2026-11-14T16:08:23Z）",
    ):
        with pytest.raises(DomainError, match="no successful research provenance"):
            validate_research_citations(text, [actual], require_access_time=True)


def test_source_and_citation_cannot_be_invented_when_no_successful_observation_exists():
    with pytest.raises(DomainError, match="no successful research provenance"):
        validate_research_citations(source_citation(source()), [], require_access_time=True)
    with pytest.raises(DomainError, match="actual source citation"):
        validate_research_citations("A source-free assertion", [source()], require_access_time=True)


@pytest.mark.parametrize(
    "bad",
    [
        {
            "url": "https://example.com/",
            "retrieved_at": "not-a-time",
            "content": "private-synthetic-body",
        },
        source().model_copy(update={"retrieved_at": datetime(2026, 11, 14, 16, 8, 23)}),
        source().model_dump(mode="python")
        | {"retrieved_at": "2026-11-14T16:08:23", "content": "private-synthetic-body"},
        source().model_dump(mode="python")
        | {"url": "not-a-url", "content": "private-synthetic-body"},
    ],
)
def test_invalid_or_naive_sources_fail_closed_with_static_repairable_error(bad):
    for call in (
        lambda: source_citation(bad),
        lambda: validate_research_citations(URL, [bad], require_access_time=True),
    ):
        with pytest.raises(DomainError) as caught:
            call()
        assert caught.value.code == "validation_failed"
        assert "private-synthetic-body" not in str(caught.value)
        assert "not-a-time" not in str(caught.value)


async def test_client_adds_citation_only_from_local_received_time_not_provider_metadata(
    monkeypatch,
):
    import groupagent.research.deepseek as module

    received = datetime.fromisoformat(RECEIVED.replace("Z", "+00:00"))

    class ControlledDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return received.astimezone(tz) if tz is not None else received.replace(tzinfo=None)

    async def public(host):
        return ("93.184.216.34",)

    def transport(request):
        if request.url.path.endswith("/search"):
            return httpx.Response(
                200,
                json={
                    "results": [
                        {
                            "url": URL,
                            "title": "Synthetic",
                            "snippet": "Snippet",
                            "summary": "Summary",
                            "content": "Body",
                            "date": "2020-01-01T00:00:00Z",
                            "retrieved_at": "2099-01-01T00:00:00Z",
                            "citation": "Forged citation",
                        }
                    ]
                },
            )
        return httpx.Response(
            200,
            json={
                "title": "Synthetic",
                "content": "Body",
                "retrieved_at": "2099-01-01T00:00:00Z",
                "citation": "Forged citation",
            },
        )

    monkeypatch.setattr(module, "datetime", ControlledDateTime)
    async with httpx.AsyncClient(transport=httpx.MockTransport(transport)) as http:
        client = DeepSeekResearchClient("synthetic-only", client=http, resolver=public)
        searched = await client.search(SearchInput(query="synthetic source"))
        fetched = await client.fetch(FetchInput(url=URL))
    for actual in (searched.results[0], fetched.source):
        assert actual.retrieved_at == received
        assert actual.citation == source_citation(actual)
        assert "2026-11-14T16:08:23Z" in actual.citation
        assert "2099" not in actual.citation and "Forged citation" not in actual.citation
        restored = ResearchSource.model_validate(actual.model_dump(mode="json"))
        assert restored == actual
    assert "检索结果观察时间" in searched.results[0].citation
    assert "页面访问时间" in fetched.source.citation
