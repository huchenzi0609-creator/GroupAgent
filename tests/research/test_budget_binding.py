from __future__ import annotations

import asyncio
from types import SimpleNamespace

import httpx
import pytest

from groupagent.domain import ActorContext, DomainError, Role
from groupagent.research import DeepSeekResearchClient, register_research_tools
from groupagent.tools import ToolContext, ToolGateway, ToolScope


class NoChargeResearchFixtureBudget:
    fixture_only = True

    def __init__(self):
        self.state = None
        self.operation = None
        self.usage = None

    def reserve(self, actor, key, operation, price_id, estimated_units, **kwargs):
        self.state, self.operation = "reserved", operation
        return SimpleNamespace(id="fixture-reservation", is_new=True)

    def begin(self, actor, identifier):
        self.state = "inflight"
        return True

    def settle(self, actor, identifier, actual_units, evidence_ref):
        self.state, self.usage = "settled", actual_units
        return SimpleNamespace(overrun=False)

    def unknown(self, actor, identifier, reason):
        self.state = "unknown"


async def public(host):
    return ("93.184.216.34",)


@pytest.mark.parametrize("status", [200, 429])
async def test_search_reserves_before_http_and_settles_or_keeps_unknown(tmp_path, status):
    budget = NoChargeResearchFixtureBudget()
    calls = []

    def response(request):
        assert budget.state == "inflight"
        calls.append(request)
        return httpx.Response(status, json={"results": []})

    async with httpx.AsyncClient(transport=httpx.MockTransport(response)) as http:
        client = DeepSeekResearchClient("synthetic-key", client=http, resolver=public)
        gateway = ToolGateway()
        register_research_tools(
            gateway,
            client,
            budget=budget,
            price_ids={"search": "search-price", "fetch": "fetch-price"},
        )
        scope = ToolScope(
            "g",
            "w",
            ActorContext(group_id="g", member_id="m", role=Role.OWNER),
            tmp_path.resolve(),
            frozenset({"web_search"}),
            "trace",
        )
        gateway.bind_scope(scope)
        result = await gateway.invoke(
            "web_search",
            {"query": "synthetic public topic"},
            ToolContext(scope, "call", "key", asyncio.Event()),
        )
    assert len(calls) == 1 and budget.operation == "web.search"
    assert result.success == (status == 200)
    assert budget.state == ("settled" if status == 200 else "unknown")
    if status == 200:
        assert (
            budget.usage["calls"] == 1 and result.output["budget_receipt"] == "fixture-reservation"
        )


async def test_real_transport_cannot_register_unpriced_research():
    client = DeepSeekResearchClient("synthetic-never-used-key")
    try:
        with pytest.raises(DomainError) as error:
            register_research_tools(ToolGateway(), client)
        assert error.value.code == "budget_unconfigured"
    finally:
        await client.aclose()
