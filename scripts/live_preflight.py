"""Explicitly authorized, single-ledger local provider check; never sends to WeChat."""

from __future__ import annotations

import asyncio
import json
from decimal import Decimal
from pathlib import Path

from groupagent.billing import BudgetLedger
from groupagent.billing.metering import MeteredModel
from groupagent.billing.prices import deepseek_20261114
from groupagent.domain import ActorContext, Role
from groupagent.execution.deepseek import DeepSeekHTTPModel
from groupagent.execution.models import ModelMessage, ModelRequest
from groupagent.persistence import PostgresStore
from groupagent.security import load_secret_file

ROOT = Path(__file__).resolve().parents[1]
GROUP = "local-acceptance-20261114"
OWNER = "synthetic-acceptance-owner"


def setup():
    store = PostgresStore((ROOT / "runtime/postgres/dsn.txt").read_text().strip())
    store.migrate()
    store.register_group(GROUP)
    actor = ActorContext(group_id=GROUP, member_id=OWNER, role=Role.OWNER)
    ledger = BudgetLedger(store)
    with store.connect() as conn:
        existing = conn.execute(
            "SELECT currency,daily_limit,total_limit FROM ga_budget_policy WHERE group_id=%s",
            (GROUP,),
        ).fetchone()
    if existing is None:
        ledger.configure(actor, currency="CNY", daily_limit=Decimal(50), total_limit=Decimal(50))
    elif (existing["currency"], existing["daily_limit"], existing["total_limit"]) != (
        "CNY",
        Decimal(50),
        Decimal(50),
    ):
        raise RuntimeError("Existing acceptance budget differs; do not reset it")
    for price in deepseek_20261114():
        ledger.register_price(actor, price)
    secrets = load_secret_file(ROOT / ".env.local", frozenset({"DEEPSEEK_API_KEY"}))
    return store, ledger, actor, secrets["DEEPSEEK_API_KEY"]


async def main():
    _, ledger, actor, key = setup()
    model = DeepSeekHTTPModel(key)
    try:
        metered = MeteredModel(model, ledger, actor, price_id="deepseek-20261114-text")
        reply = await metered.complete(
            ModelRequest(
                messages=(
                    ModelMessage(role="user", content="这是合成开发测试，请只回复：连接成功。"),
                ),
                tools=(),
                max_output_tokens=32,
            )
        )
        result = {
            "provider": "deepseek",
            "model": model.model,
            "live_channel": False,
            "response_valid": "连接成功" in (reply.text or ""),
            "usage": reply.usage.model_dump(),
            "budget": ledger.report(actor),
        }
        destination = ROOT / "runtime/live"
        destination.mkdir(parents=True, exist_ok=True)
        (destination / "preflight.json").write_text(
            json.dumps(result, ensure_ascii=False, indent=2, default=str)
        )
        print(json.dumps(result, ensure_ascii=False, default=str))
    finally:
        await model.aclose()


if __name__ == "__main__":
    asyncio.run(main())
