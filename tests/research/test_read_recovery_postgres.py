"""Real isolated PostgreSQL fee holds with an entirely synthetic HTTP provider."""

import asyncio
import os
from datetime import UTC, datetime
from decimal import Decimal
from uuid import uuid4

import httpx
import pytest

from groupagent.billing import BudgetLedger, PriceSnapshot
from groupagent.domain import ActorContext, EventKind, GroupEvent, Role
from groupagent.persistence import PostgresStore
from groupagent.research import DeepSeekResearchClient, register_research_tools
from groupagent.tools import EffectState, ToolContext, ToolGateway, ToolScope

pytestmark = pytest.mark.postgres


async def public(host):
    return ("93.184.216.34",)


@pytest.mark.parametrize("call_cost", [Decimal("0.005"), Decimal(11)])
async def test_retry_keeps_unknown_fees_and_cannot_exceed_twenty_yuan_cap(tmp_path, call_cost):
    dsn = os.environ.get("GROUPAGENT_TEST_DSN")
    if not dsn:
        pytest.skip("Requires isolated PostgreSQL")
    store = PostgresStore(dsn)
    store.migrate()
    group = "research-recovery-" + uuid4().hex
    store.register_group(group)
    actor = ActorContext(group_id=group, member_id="synthetic-owner", role=Role.OWNER)
    budget = BudgetLedger(store, allow_test_prices=True)
    budget.configure(actor, currency="CNY", daily_limit=Decimal(50), total_limit=Decimal(20))
    for operation in ("search", "fetch"):
        budget.register_price(
            actor,
            PriceSnapshot(
                id=operation,
                provider="synthetic",
                operation="web." + operation,
                currency="CNY",
                rates={"calls": call_cost},
                source_url="fixture://not-a-commercial-price",
                captured_at=datetime.now(UTC),
                test_only=True,
            ),
        )
    stamp = datetime.now(UTC)
    identifier = uuid4().hex
    request = GroupEvent(
        id=identifier,
        source_key="research-recovery:" + identifier,
        group_id=actor.group_id,
        actor_id=actor.member_id,
        kind=EventKind.TEXT,
        text="Synthetic public research request",
        occurred_at=stamp,
        received_at=stamp,
    )
    with store.transaction(actor) as tx:
        tx.ingest_event(request, identifier, "fixture", 0)
        work = tx.create_work("Synthetic research fee recovery", request.id)

    calls = []

    def response(request):
        calls.append(request)
        return httpx.Response(503) if len(calls) == 1 else httpx.Response(200, json={"results": []})

    async with httpx.AsyncClient(transport=httpx.MockTransport(response)) as http:
        gateway = ToolGateway()
        register_research_tools(
            gateway,
            DeepSeekResearchClient("synthetic-key", client=http, resolver=public),
            budget=budget,
            price_ids={"search": "search", "fetch": "fetch"},
            retry_backoff_seconds=0,
        )
        scope = ToolScope(
            actor.group_id, work.id, actor, tmp_path.resolve(), frozenset({"web_search"}), "fixture"
        )
        gateway.bind_scope(scope)
        receipt = await gateway.invoke(
            "web_search",
            {"query": "synthetic public evidence"},
            ToolContext(scope, "call", "key", asyncio.Event()),
        )

    assert receipt.effect_state == EffectState.NO_EFFECT
    with store.connect() as conn:
        rows = conn.execute(
            "SELECT key,state,reserved_amount,actual_amount FROM ga_budget_reservations "
            "WHERE group_id=%s ORDER BY created_at,id",
            (actor.group_id,),
        ).fetchall()
        policy = conn.execute(
            "SELECT daily_limit,total_limit FROM ga_budget_policy WHERE group_id=%s",
            (actor.group_id,),
        ).fetchone()
    assert policy["daily_limit"] == Decimal(50) and policy["total_limit"] == Decimal(20)
    assert rows[0]["key"] == "key:web.search" and rows[0]["state"] == "unknown"
    assert rows[0]["reserved_amount"] == call_cost and rows[0]["actual_amount"] is None
    held_and_settled = sum(
        row["actual_amount"] if row["state"] == "settled" else row["reserved_amount"]
        for row in rows
    )
    assert held_and_settled <= Decimal(20)
    if call_cost == Decimal(11):
        assert receipt.error_code == "budget_exhausted" and len(calls) == len(rows) == 1
    else:
        assert receipt.success and len(calls) == len(rows) == 2
        assert rows[1]["key"] == "key:web.search:retry:1" and rows[1]["state"] == "settled"
        assert rows[1]["actual_amount"] == call_cost
    restarted = BudgetLedger(store, allow_test_prices=True)
    report = restarted.report(actor)
    assert any(row["state"] == "unknown" and row["calls"] == 1 for row in report["states"])
