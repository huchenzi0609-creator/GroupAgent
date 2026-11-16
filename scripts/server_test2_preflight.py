"""Explicit test_2 preflight: no group sends; one bounded model call if requested."""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
from decimal import Decimal
from pathlib import Path

from groupagent.billing import BudgetLedger
from groupagent.billing.metering import MeteredModel
from groupagent.billing.prices import deepseek_20261114
from groupagent.channels import AgentWechatAdapter
from groupagent.channels.models import ChannelCursor
from groupagent.config import AppConfig
from groupagent.domain import ActorContext, Role
from groupagent.execution.deepseek import DeepSeekHTTPModel
from groupagent.execution.models import ModelMessage, ModelRequest
from groupagent.persistence import PostgresStore
from groupagent.runtime.app import channel_config
from groupagent.security import load_secret_file


async def run(args):
    if args.output.exists():
        raise RuntimeError("Use a new preflight output path; preserve earlier evidence")
    config = AppConfig.model_validate_json(args.config.read_text())
    allocation = json.loads(args.allocation.read_text())
    assert allocation["source_paid_entry_frozen"] is True
    assert Decimal(allocation["source_upper_cny"]) + Decimal(
        allocation["allocated_cap_cny"]
    ) <= Decimal(allocation["parent_limit_cny"])
    assert (
        hashlib.sha256(config.group.internal_id.encode()).hexdigest()
        == allocation["target_group_sha256"]
    )
    assert config.spend.session_total_limit == Decimal(allocation["allocated_cap_cny"])
    secrets = load_secret_file(
        args.secrets, frozenset({"DEEPSEEK_API_KEY", "GROUPAGENT_CHANNEL_TOKEN"})
    )
    store = PostgresStore(args.dsn.read_text().strip())
    store.migrate()
    store.register_group(config.group.internal_id, config.group.policy_revision)
    owner = ActorContext(
        group_id=config.group.internal_id,
        member_id=config.group.owner_ids[0],
        role=Role.OWNER,
        policy_revision=config.group.policy_revision,
    )
    technical = owner.model_copy(
        update={"member_id": "service:channel-intake", "role": Role.MAINTAINER}
    )
    ledger = BudgetLedger(store)
    with store.transaction(owner) as tx:
        prior = tx.conn.execute(
            "SELECT currency,daily_limit,total_limit FROM ga_budget_policy WHERE group_id=%s",
            (owner.group_id,),
        ).fetchone()
    if prior is None:
        ledger.configure(
            owner,
            currency="CNY",
            daily_limit=config.spend.group_daily_limit,
            total_limit=config.spend.session_total_limit,
            time_zone=config.group.time_zone,
        )
    else:
        assert (prior["currency"], prior["daily_limit"], prior["total_limit"]) == (
            "CNY",
            config.spend.group_daily_limit,
            config.spend.session_total_limit,
        )
    for price in deepseek_20261114():
        ledger.register_price(owner, price)
    channel = AgentWechatAdapter(
        channel_config(config), secrets["GROUPAGENT_CHANNEL_TOKEN"].get_secret_value()
    )
    model = DeepSeekHTTPModel(secrets["DEEPSEEK_API_KEY"], model=config.model_routes.text_agent)
    try:
        capability = await channel.capabilities()
        assert capability.backend == "native"
        members = await channel.members(limit=100)
        page = await channel._request("GET", channel._path("groups"), params={"limit": 1})
        value = str(page["asOfEventCursor"])
        assert value.isdigit()
        cursor = ChannelCursor(
            account_id=config.runtime.channel_account_id,
            group_id=config.group.internal_id,
            external_group_id=config.group.external_group_id,
            source_epoch=config.runtime.source_epoch,
            daemon_server_id=capability.daemon_server_id,
            value=value,
        )
        binding = [
            config.runtime.channel_account_id,
            config.group.internal_id,
            config.group.external_group_id,
        ]
        stream = "agentwechat:" + hashlib.sha256(json.dumps(binding).encode()).hexdigest()
        with store.transaction(technical) as tx:
            saved = tx.get_cursor(stream)
            if saved is None:
                tx.advance_cursor(
                    stream,
                    cursor.serialize(),
                    expected_cursor_revision=0,
                    reason="authorized_test2_initial_tail_no_history_dispatch",
                )
            tx._audit(
                "test2_channel_preflight",
                "test2-native",
                member_count=len(members.members),
                native_backend=True,
                initial_tail_seeded=saved is None,
            )
        result = {
            "native_channel": True,
            "member_count": len(members.members),
            "room_names_present": sum(bool(m.room_display_name) for m in members.members),
            "native_voice_reported": capability.native_voice_reported,
            "initial_tail_seeded": saved is None,
            "group_messages_sent": 0,
            "model_checked": False,
            "upstream_item_purge_supported": False,
        }
        if args.check_model:
            reply = await MeteredModel(
                model, ledger, owner, price_id=config.spend.pricing_snapshot_id
            ).complete(
                ModelRequest(
                    messages=(
                        ModelMessage(
                            role="user", content="这是授权服务器验收，请只回复：连接成功。"
                        ),
                    ),
                    tools=(),
                    max_output_tokens=32,
                )
            )
            result["model_checked"] = "连接成功" in (reply.text or "")
            assert result["model_checked"]
        result["budget"] = ledger.report(owner)
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(result, ensure_ascii=False, indent=2, default=str) + "\n")
        args.output.chmod(0o600)
        print(json.dumps({k: v for k, v in result.items() if k != "budget"}, ensure_ascii=False))
    finally:
        await channel.aclose()
        await model.aclose()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("config", "dsn", "secrets", "allocation", "output"):
        parser.add_argument("--" + name, type=Path, required=True)
    parser.add_argument("--check-model", action="store_true")
    asyncio.run(run(parser.parse_args()))


if __name__ == "__main__":
    main()
