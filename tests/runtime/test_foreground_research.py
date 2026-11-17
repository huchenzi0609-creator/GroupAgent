"""Real PostgreSQL ledger with an explicit offline public-provider fixture."""

import json
from types import SimpleNamespace

import httpx
import pytest

from groupagent.billing import BudgetLedger
from groupagent.billing.prices import deepseek_20261114
from groupagent.domain import DomainError, Role
from groupagent.execution.models import ModelToolCall
from groupagent.research.deepseek import DeepSeekResearchClient
from groupagent.runtime.foreground_research import ForegroundResearch

from .conftest import source

pytestmark = pytest.mark.postgres


async def test_inline_search_is_authorized_and_metered_by_inbound_source(runtime_env):
    env = runtime_env
    event = source(env, "查一条合成公开资料")
    ledger = BudgetLedger(env.store)
    ledger.configure(env.actor, currency="CNY", daily_limit=None, total_limit=None)
    for price in deepseek_20261114():
        ledger.register_price(env.actor, price)
    calls = []

    def provider(request):
        calls.append(request)
        return httpx.Response(200, json={"results": []})

    async with httpx.AsyncClient(transport=httpx.MockTransport(provider)) as http:
        research = ForegroundResearch(DeepSeekResearchClient("synthetic", client=http), ledger)
        assert not research(env.actor.model_copy(update={"role": Role.OBSERVER}), event, {})
        assert not research(
            env.actor, event, {"_recipient_evidence": SimpleNamespace(recipient="other")}
        )
        search, fetch = research(env.actor, event, {})
        arguments = {"query": "synthetic public research topic"}
        result = json.loads(
            await search.handler(ModelToolCall(id="one", name=search.name, arguments=arguments))
        )
        assert result["status"] == "observed" and result["evidence"]["query"] == arguments["query"]
        assert "budget_receipt" not in result["evidence"]
        with pytest.raises(DomainError) as duplicate:
            await search.handler(ModelToolCall(id="two", name=search.name, arguments=arguments))
        assert duplicate.value.code == "provider_reconciliation_required"
        with pytest.raises(DomainError):
            await fetch.handler(
                ModelToolCall(
                    id="three", name=fetch.name, arguments={"url": "http://127.0.0.1/test"}
                )
            )

    assert len(calls) == 1
    with env.store.transaction(env.actor) as tx:
        receipts = tx.conn.execute(
            "SELECT source_event_id,work_id,operation,state FROM ga_budget_reservations "
            "WHERE group_id=%s",
            (tx.group_id,),
        ).fetchall()
    assert receipts == [
        {
            "source_event_id": event.id,
            "work_id": None,
            "operation": "web.search",
            "state": "settled",
        }
    ]
