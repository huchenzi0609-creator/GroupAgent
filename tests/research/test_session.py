"""Offline research evidence/cost/restart contracts; no live HTTP or database."""

import asyncio
import hashlib
import json
from types import SimpleNamespace

import httpx
import pytest

from groupagent.domain import ActorContext, DomainError, Role
from groupagent.research import (
    DeepSeekResearchClient,
    EvidenceStore,
    ResearchSession,
    ResearchSource,
    project_research_sources,
    register_research_tools,
)
from groupagent.research.evidence import project_text
from groupagent.tools import PolicyDecision, ToolContext, ToolGateway, ToolScope
from groupagent.tools.models import ToolLedgerEntry


class NoChargeBudget:
    fixture_only = True

    def __init__(self):
        self.calls = []
        self.states = {}

    def reserve(self, actor, key, operation, price_id, units, **kwargs):
        self.calls.append(key)
        self.states[key] = "reserved"
        return SimpleNamespace(id=key, is_new=True)

    def begin(self, actor, key):
        self.states[key] = "inflight"
        return True

    def settle(self, actor, key, units, evidence):
        self.states[key] = "settled"
        return SimpleNamespace(overrun=False)

    def unknown(self, actor, key, reason):
        self.states[key] = "unknown"


async def public(host):
    return ("93.184.216.34",)


def bind(gateway, workspace, *, group="group", work="work", member="member"):
    workspace.mkdir(exist_ok=True, parents=True)
    scope = ToolScope(
        group,
        work,
        ActorContext(group_id=group, member_id=member, role=Role.OWNER),
        workspace.resolve(),
        frozenset({"web_search", "web_fetch", "web_read"}),
        "trace",
    )
    gateway.bind_scope(scope)
    return scope


async def invoke(gateway, scope, name, arguments, key):
    return await gateway.invoke(name, arguments, ToolContext(scope, key, key, asyncio.Event()))


def register(gateway, http, budget=None, **options):
    client = DeepSeekResearchClient(
        "synthetic-key",
        client=http,
        resolver=public,
        max_search_content_bytes=8000,
        max_search_text_bytes=16000,
        max_fetch_content_bytes=12000,
    )
    session = ResearchSession(
        client,
        question=options.pop("question", ""),
        max_cache_entries=options.pop("max_cache_entries", 64),
    )
    register_research_tools(
        gateway,
        client,
        session=session,
        budget=budget,
        price_ids={"search": "fixture-search", "fetch": "fixture-fetch"} if budget else None,
        additional_retry_limit=0,
        **options,
    )
    return session


async def test_same_request_singleflights_and_preserves_time_and_actual_fee(tmp_path):
    budget, requests = NoChargeBudget(), []

    async def response(request):
        requests.append(request)
        await asyncio.sleep(0.01)
        return httpx.Response(200, json={"title": "Source", "content": "Saved evidence"})

    async with httpx.AsyncClient(transport=httpx.MockTransport(response)) as http:
        gateway = ToolGateway()
        register(gateway, http, budget)
        scope = bind(gateway, tmp_path)
        first, second = await asyncio.gather(
            *(
                invoke(gateway, scope, "web_fetch", {"url": "https://example.com/source"}, key)
                for key in ("first", "second")
            )
        )
        third = await invoke(
            gateway,
            scope,
            "web_fetch",
            {"url": "https://EXAMPLE.com/source#section"},
            "third",
        )
    assert first.success and second.success and third.success
    assert len(requests) == len(budget.calls) == 1
    assert first.output == second.output == third.output
    assert first.output["budget_receipt"] == "first:web.fetch"
    assert set(budget.states.values()) == {"settled"}


async def test_receipt_rehydration_reads_tail_and_reuses_request_after_restart(tmp_path):
    content = "无关前言。" * 4000 + "\n页尾的实际答案：缓存必须保留证据。"
    requests = []

    def response(request):
        requests.append(request)
        return httpx.Response(200, json={"title": "Source", "content": content})

    async with httpx.AsyncClient(transport=httpx.MockTransport(response)) as http:
        gateway = ToolGateway()
        register(gateway, http)
        scope = bind(gateway, tmp_path)
        first = await invoke(gateway, scope, "web_fetch", {"url": "https://example.com/a"}, "first")
        assert first.success
        assert len(first.output["source"]["content"].encode()) <= 12000
        entries = tuple(
            ToolLedgerEntry.model_validate_json(
                json.dumps(
                    item.model_dump(mode="json"),
                    sort_keys=True,
                )
            )
            for item in gateway.export_ledger(scope)
        )
        restarted = ToolGateway()
        register(restarted, http)
        restarted.bind_scope(scope)
        restarted.restore_ledger(scope, entries)
        second = await invoke(
            restarted, scope, "web_fetch", {"url": "https://example.com/a"}, "new"
        )
        read = await invoke(
            restarted,
            scope,
            "web_read",
            {
                "source_id": first.output["source"]["source_id"],
                "query": "页尾实际答案缓存保留证据",
                "limit": 1800,
            },
            "read",
        )
    assert second.success and second.output == first.output and len(requests) == 1
    assert read.success and "缓存必须保留证据" in read.output["source"]["content"]
    assert read.output["source"]["citation"] == first.output["source"]["citation"]
    assert read.output["source"]["evidence_kind"] == "provider_fetched_page"


async def test_cache_hit_freshly_authorizes_and_never_crosses_scope(tmp_path):
    active = True

    async def policy(scope):
        return PolicyDecision(actor=scope.actor, allowed_tools=scope.allowed_tools, active=active)

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda _: httpx.Response(
                200,
                json={"title": "Source", "content": "Scoped evidence"},
            )
        )
    ) as http:
        gateway = ToolGateway(policy)
        session = register(gateway, http)
        scope = bind(gateway, tmp_path / "one")
        first = await invoke(gateway, scope, "web_fetch", {"url": "https://example.com/a"}, "first")
        active = False
        denied = await invoke(
            gateway, scope, "web_fetch", {"url": "https://example.com/a"}, "second"
        )
        assert denied.error_code == "permission_denied"
        active = True
        other = bind(gateway, tmp_path / "two", work="other-work")
        read = await invoke(
            gateway,
            other,
            "web_read",
            {
                "source_id": first.output["source"]["source_id"],
            },
            "read",
        )
        assert read.error_code == "research_source_unknown"
        with pytest.raises(DomainError, match="another work"):
            session.evidence_store.source_content(
                ToolContext(other, "other", "other", asyncio.Event()),
                ResearchSource.model_validate(first.output["source"]),
            )


async def test_unknown_failure_is_not_cached_or_refunded(tmp_path):
    budget, requests = NoChargeBudget(), []

    def response(request):
        requests.append(request)
        if len(requests) == 1:
            return httpx.Response(503)
        return httpx.Response(200, json={"title": "Source", "content": "Recovered evidence"})

    async with httpx.AsyncClient(transport=httpx.MockTransport(response)) as http:
        gateway = ToolGateway()
        register(gateway, http, budget)
        scope = bind(gateway, tmp_path)
        failed, succeeded, cached = [
            await invoke(
                gateway,
                scope,
                "web_fetch",
                {"url": "https://example.com/a"},
                key,
            )
            for key in ("failed", "succeeded", "cached")
        ]
    assert not failed.success and succeeded.success and cached.success
    assert len(requests) == len(budget.calls) == 2
    assert budget.states == {"failed:web.fetch": "unknown", "succeeded:web.fetch": "settled"}
    assert cached.output["budget_receipt"] == "succeeded:web.fetch"


async def test_explicit_refresh_reads_again_and_later_hits_retain_the_new_fee(tmp_path):
    budget, requests = NoChargeBudget(), []

    def response(request):
        requests.append(request)
        assert "refresh" not in json.loads(request.content)
        return httpx.Response(200, json={"title": "Source", "content": f"Evidence {len(requests)}"})

    async with httpx.AsyncClient(transport=httpx.MockTransport(response)) as http:
        gateway = ToolGateway()
        register(gateway, http, budget)
        scope = bind(gateway, tmp_path)
        original = await invoke(
            gateway, scope, "web_fetch", {"url": "https://example.com/a"}, "old"
        )
        refreshed = await invoke(
            gateway,
            scope,
            "web_fetch",
            {
                "url": "https://example.com/a",
                "refresh": True,
            },
            "fresh",
        )
        reused = await invoke(
            gateway, scope, "web_fetch", {"url": "https://example.com/a"}, "reused"
        )
    assert original.success and refreshed.success and reused.success
    assert len(requests) == len(budget.calls) == 2
    assert refreshed.output == reused.output and "Evidence 2" in reused.output["source"]["content"]
    assert reused.output["budget_receipt"] == "fresh:web.fetch"
    assert original.output["source"]["source_id"] != refreshed.output["source"]["source_id"]


async def test_memory_cache_eviction_recovers_from_receipts_without_an_external_read(tmp_path):
    requests = []

    def response(request):
        requests.append(json.loads(request.content)["url"])
        return httpx.Response(200, json={"title": "Source", "content": "Retained evidence"})

    async with httpx.AsyncClient(transport=httpx.MockTransport(response)) as http:
        gateway = ToolGateway()
        register(gateway, http, max_cache_entries=1)
        scope = bind(gateway, tmp_path)
        results = [
            await invoke(
                gateway,
                scope,
                "web_fetch",
                {
                    "url": f"https://example.com/{url}",
                },
                key,
            )
            for url, key in (("a", "a"), ("b", "b"), ("a", "a-again"))
        ]
    assert all(item.success for item in results) and len(requests) == 2
    assert results[0].output == results[2].output


async def test_one_read_failure_does_not_discard_an_independent_success(tmp_path):
    requests = []

    async def response(request):
        target = json.loads(request.content)["url"]
        requests.append(target)
        await asyncio.sleep(0.005)
        return (
            httpx.Response(503)
            if target.endswith("bad")
            else httpx.Response(
                200,
                json={"title": "Good", "content": "Independent evidence"},
            )
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(response)) as http:
        gateway = ToolGateway()
        register(gateway, http)
        scope = bind(gateway, tmp_path)
        bad, good = await asyncio.gather(
            *(
                invoke(gateway, scope, "web_fetch", {"url": f"https://example.com/{name}"}, name)
                for name in ("bad", "good")
            )
        )
        cached = await invoke(
            gateway, scope, "web_fetch", {"url": "https://example.com/good"}, "cached"
        )
    assert not bad.success and good.success and cached.success
    assert len(requests) == 2 and cached.output == good.output


async def test_search_field_budget_keeps_body_and_actual_options_choose_cache_key(tmp_path):
    requests = []

    def response(request):
        body = json.loads(request.content)
        requests.append(body)
        return httpx.Response(
            200,
            json={
                "results": [
                    dict(
                        title="题目" * 400,
                        url=f"https://example.com/{index}",
                        snippet="片段" * 1000,
                        summary="概要" * 1000,
                        content="其他前言。" * 3000 + "正确搜索质量证据位于页尾。",
                    )
                    for index in range(body["count"])
                ]
            },
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(response)) as http:
        gateway = ToolGateway()
        register(gateway, http)
        scope = bind(gateway, tmp_path)
        first = await invoke(
            gateway,
            scope,
            "web_search",
            {
                "query": "正确搜索质量证据",
                "count": 10,
                "need_content": True,
            },
            "first",
        )
        repeated = await invoke(
            gateway,
            scope,
            "web_search",
            {
                "query": "正确搜索质量证据",
                "count": 10,
                "need_content": True,
            },
            "repeated",
        )
        changed = await invoke(
            gateway,
            scope,
            "web_search",
            {
                "query": "正确搜索质量证据",
                "count": 5,
                "need_content": True,
            },
            "changed",
        )
    assert first.success and repeated.success and changed.success
    assert len(requests) == 2 and first.output == repeated.output
    assert all("正确搜索质量证据" in source["content"] for source in first.output["results"])
    assert (
        sum(
            len(
                "".join(
                    source[field]
                    for field in (
                        "title",
                        "snippet",
                        "summary",
                        "content",
                    )
                ).encode()
            )
            for source in first.output["results"]
        )
        <= 16000
    )


async def test_tampered_saved_body_and_symlink_cannot_be_used_as_evidence(tmp_path):
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda _: httpx.Response(
                200,
                json={"title": "Source", "content": "Original evidence"},
            )
        )
    ) as http:
        gateway = ToolGateway()
        register(gateway, http)
        scope = bind(gateway, tmp_path)
        first = await invoke(gateway, scope, "web_fetch", {"url": "https://example.com/a"}, "first")
        reference = first.output["source"]["evidence_ref"]
        blob = (
            tmp_path
            / ".research-evidence"
            / reference["scope_key"]
            / (reference["source_id"] + ".txt")
        )
        blob.write_text("Altered evidence")
        tampered = await invoke(
            gateway, scope, "web_fetch", {"url": "https://example.com/a"}, "tampered"
        )
        assert tampered.error_code == "research_evidence_invalid"
        blob.unlink()
        outside = tmp_path / "outside.txt"
        outside.write_text("Original evidence")
        blob.symlink_to(outside)
        linked = await invoke(
            gateway,
            scope,
            "web_read",
            {
                "source_id": reference["source_id"],
            },
            "symlink",
        )
        assert not linked.success and linked.error_code == "scope_violation"


def test_chinese_projector_ranks_relevant_source_and_windows_before_unrelated_prefix():
    sources = [
        {
            "source_id": "a",
            "url": "https://example.com/a",
            "operation": "fetch",
            "content": "无关前言。" * 5000,
        },
        {
            "source_id": "b",
            "url": "https://example.com/b",
            "operation": "fetch",
            "content": "无关前言。" * 5000 + "搜索质量依靠保留相关正文。",
        },
    ]
    result = project_research_sources(sources, "如何改善搜索质量并保留相关正文", max_bytes=2400)
    assert result[0]["source_id"] == "b"
    assert "搜索质量依靠保留相关正文" in result[0]["received_content_excerpt"]
    assert sum(len(item["received_content_excerpt"].encode()) for item in result) <= 2400
    assert all(item["omitted_received_content"] for item in result)
    assert project_text("完整短正文", "正文", 100) == "完整短正文"


async def test_continuation_preserves_utf8_and_measures_actual_returned_content(tmp_path):
    content = "汉字🧪" * 3000
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda _: httpx.Response(
                200,
                json={"title": "Source", "content": content},
            )
        )
    ) as http:
        gateway = ToolGateway()
        register(gateway, http)
        scope = bind(gateway, tmp_path)
        first = await invoke(gateway, scope, "web_fetch", {"url": "https://example.com/a"}, "first")
        read = await invoke(
            gateway,
            scope,
            "web_read",
            {
                "source_id": first.output["source"]["source_id"],
                "offset": 12001,
                "limit": 100,
            },
            "read",
        )
    assert read.success
    source = read.output["source"]
    assert "\ufffd" not in source["content"]
    assert source["content_bytes_received"] == len(content.encode())
    assert source["content_bytes_returned"] == len(source["content"].encode()) <= 100
    assert source["content_sha256"] == hashlib.sha256(source["content"].encode()).hexdigest()
    assert read.output["next_offset"] > 12001


def test_evidence_bounds_are_explicit():
    with pytest.raises(ValueError):
        EvidenceStore(max_source_bytes=2_000_001)


async def test_store_quota_preserves_earlier_evidence_and_wire_guard_saves_nothing(tmp_path):
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda _: httpx.Response(
                200,
                json={"title": "Source", "content": "12345678"},
            )
        )
    ) as http:
        client = DeepSeekResearchClient("synthetic-key", client=http, resolver=public)
        store = EvidenceStore(max_source_bytes=10, max_scope_bytes=12)
        session = ResearchSession(client, store)
        gateway = ToolGateway()
        register_research_tools(gateway, client, session=session, additional_retry_limit=0)
        scope = bind(gateway, tmp_path / "quota")
        first = await invoke(gateway, scope, "web_fetch", {"url": "https://example.com/a"}, "first")
        refused = await invoke(
            gateway, scope, "web_fetch", {"url": "https://example.com/b"}, "refused"
        )
        continued = await invoke(
            gateway,
            scope,
            "web_read",
            {
                "source_id": first.output["source"]["source_id"],
            },
            "continued",
        )
        assert refused.error_code == "research_evidence_limit"
        assert continued.success and continued.output["source"]["content"] == "12345678"
        assert len(list((tmp_path / "quota" / ".research-evidence").rglob("*.txt"))) == 1

        guarded_client = DeepSeekResearchClient(
            "synthetic-key",
            client=http,
            resolver=public,
            max_response_bytes=10,
        )
        guarded = ToolGateway()
        register_research_tools(guarded, guarded_client, additional_retry_limit=0)
        guarded_scope = bind(guarded, tmp_path / "guarded")
        oversized = await invoke(
            guarded,
            guarded_scope,
            "web_fetch",
            {
                "url": "https://example.com/a",
            },
            "oversized",
        )
        assert oversized.error_code == "research_response_too_large"
        assert not (tmp_path / "guarded" / ".research-evidence").exists()
