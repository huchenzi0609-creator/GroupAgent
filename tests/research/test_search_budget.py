"""Synthetic HTTP evidence for the search collection's UTF-8 text budget."""

import hashlib
import json

import httpx
import pytest

from groupagent.domain import DomainError
from groupagent.research import DeepSeekResearchClient, SearchInput

TEXT_FIELDS = ("title", "snippet", "summary", "content")


async def public_resolver(host: str) -> tuple[str, ...]:
    return ("93.184.216.34",)


def source(index=0, **fields):
    return {
        "url": f"https://example.com/source/{index}",
        "title": "标题",
        "snippet": "片段",
        "summary": "摘要",
        "content": "正文",
        "date": "2026-11-13T08:00:00Z",
    } | fields


async def search(results, *, need_content=True, count=10, **limits):
    # Serialize explicitly so invalid-surrogate cases exercise provider JSON,
    # rather than failing in the synthetic HTTP response constructor.
    body = json.dumps({"results": results}).encode()
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda _: httpx.Response(200, content=body))
    ) as http:
        client = DeepSeekResearchClient(
            "synthetic-research-key", client=http, resolver=public_resolver, **limits
        )
        return await client.search(
            SearchInput(query="synthetic collection", count=count, need_content=need_content)
        )


def text_bytes(item):
    return sum(len(getattr(item, field).encode()) for field in TEXT_FIELDS)


async def test_five_large_results_share_all_four_text_fields_without_losing_sources():
    originals = [
        source(
            index,
            title="标题" * 400,
            snippet="片段🧪" * 1000,
            summary="摘要" * 4000,
            content="正文" * 5000,
        )
        for index in range(5)
    ]
    output = await search(originals, max_search_text_bytes=16_000)

    assert len(output.results) == output.provider_result_count == 5
    assert output.filtered_result_count == 0
    assert sum(map(text_bytes, output.results)) <= 16_000
    for original, item in zip(originals, output.results, strict=True):
        assert text_bytes(item) <= 3200
        assert item.url == original["url"]
        assert item.source_id == hashlib.sha256(item.url.encode()).hexdigest()
        assert item.provider == "deepseek" and item.operation == "search"
        assert item.evidence_kind == "search_result" and item.untrusted_content
        assert item.retrieved_at == output.retrieved_at
        assert item.published_at.isoformat() == "2026-11-13T08:00:00+00:00"
        assert item.final_url is None
        assert item.truncated_fields == TEXT_FIELDS
        for field in TEXT_FIELDS:
            assert original[field].startswith(getattr(item, field))
        assert item.content_bytes_received == len(original["content"].encode())
        assert item.content_bytes_returned == len(item.content.encode())
        assert item.content_sha256 == hashlib.sha256(item.content.encode()).hexdigest()


@pytest.mark.parametrize("result_count", [1, 5, 10])
async def test_actual_returned_count_shares_budget_and_hashes_exact_unicode_prefix(result_count):
    original = "证据🧪" * 5000
    output = await search(
        [source(index, content=original) for index in range(result_count)],
        # The requested count stays ten even when the provider returns fewer.
        count=10,
        max_search_text_bytes=16_007,
        max_search_content_bytes=100_000,
    )

    assert len(output.results) == result_count
    assert sum(map(text_bytes, output.results)) <= 16_007
    for item in output.results:
        assert text_bytes(item) <= 16_007 // result_count
        assert text_bytes(item) >= 16_007 // result_count - 3
        assert item.content and original.startswith(item.content)
        assert item.content.encode().decode() == item.content
        assert item.truncated_fields == ("content",)
        assert item.content_bytes_received == 50_000
        assert item.content_bytes_returned == len(item.content.encode())
        assert item.content_sha256 == hashlib.sha256(item.content.encode()).hexdigest()


async def test_short_single_result_is_unchanged_when_text_fits():
    original = source()
    output = await search([original], max_search_text_bytes=100)
    item = output.results[0]
    assert {field: getattr(item, field) for field in TEXT_FIELDS} == {
        field: original[field] for field in TEXT_FIELDS
    }
    assert item.truncated_fields == ()
    assert item.content_bytes_received == item.content_bytes_returned == 6
    assert item.content_sha256 == hashlib.sha256("正文".encode()).hexdigest()


async def test_existing_per_content_limit_remains_with_a_larger_collection_budget():
    output = await search(
        [source(content="正文" * 100)],
        max_search_text_bytes=16_000,
        max_search_content_bytes=10,
    )
    item = output.results[0]
    assert item.content == "正文正"
    assert item.content_bytes_returned == 9
    assert item.content_bytes_received == 600
    assert item.truncated_fields == ("content",)


@pytest.mark.parametrize("provider_content", ["", "不应返回的正文" * 1000])
async def test_need_content_false_keeps_no_body_and_reports_actual_discard(provider_content):
    output = await search(
        [source(content=provider_content)], need_content=False, max_search_text_bytes=100
    )
    item = output.results[0]
    assert item.title == "标题" and item.snippet == "片段" and item.summary == "摘要"
    assert item.content == "" and item.content_bytes_returned == 0
    assert item.content_bytes_received == len(provider_content.encode())
    assert item.content_sha256 == hashlib.sha256(b"").hexdigest()
    assert item.truncated_fields == (("content",) if provider_content else ())


async def test_need_content_false_still_bounds_all_search_metadata_text():
    output = await search(
        [
            source(index, title="名", snippet="段", summary="概要🧪" * 1000, content="")
            for index in range(5)
        ],
        need_content=False,
        max_search_text_bytes=600,
    )
    assert len(output.results) == 5
    assert sum(map(text_bytes, output.results)) <= 600
    assert all(item.summary and item.truncated_fields == ("summary",) for item in output.results)


async def test_filtered_sources_are_counted_but_do_not_consume_returned_text_budget():
    output = await search(
        [source(0, content="文" * 1000), source(1, url="http://127.0.0.1/private")],
        max_search_text_bytes=601,
    )
    assert output.provider_result_count == 2 and output.filtered_result_count == 1
    assert len(output.results) == 1
    assert 590 <= text_bytes(output.results[0]) <= 601
    assert "127.0.0.1" not in output.model_dump_json()


async def test_budget_smaller_than_result_count_retains_all_provenance():
    output = await search([source(index) for index in range(5)], max_search_text_bytes=1)
    assert len(output.results) == 5 and output.filtered_result_count == 0
    assert sum(map(text_bytes, output.results)) == 0
    assert all(item.url and item.truncated_fields == TEXT_FIELDS for item in output.results)


async def test_collection_budget_does_not_bypass_serialized_output_hard_limit():
    with pytest.raises(DomainError, match="normalized research result") as failure:
        await search([source()], max_search_text_bytes=1, max_output_bytes=100)
    assert failure.value.code == "research_output_too_large"


@pytest.mark.parametrize("need_content", [False, True])
async def test_truncation_does_not_hide_invalid_unicode(need_content):
    with pytest.raises(DomainError) as failure:
        await search(
            [source(content="\ud800")],
            need_content=need_content,
            max_search_text_bytes=1,
        )
    assert failure.value.code == "invalid_research_response"


@pytest.mark.parametrize("limit", [0, -1])
def test_search_text_budget_must_be_positive(limit):
    with pytest.raises(ValueError, match="limits must be positive"):
        DeepSeekResearchClient("synthetic-research-key", max_search_text_bytes=limit)
