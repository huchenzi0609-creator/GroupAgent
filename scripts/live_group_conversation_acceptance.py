"""Authorized real DeepSeek foreground checks on a synthetic local group.

No WeChat/native sends or background execution. Work admission is real PG, not
evidence that the delegated research has executed. Costs stay in the ledger;
there is no added monetary/token/goal budget.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import re
import time
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

from groupagent.config import AppConfig
from groupagent.domain import EventKind, GroupEvent
from groupagent.persistence import PostgresStore
from groupagent.runtime.app import build_app

ROOT = Path(__file__).resolve().parents[1]


def save(path, report):
    path.write_text(json.dumps(report, ensure_ascii=False, default=str, indent=2) + "\n")
    path.chmod(0o600)


async def run(args):
    destination = ROOT / "runtime/live" / args.run_id
    destination.mkdir(parents=True, mode=0o700, exist_ok=False)
    group = "local-turn-contract-" + args.run_id
    config = AppConfig.model_validate(
        {
            "deployment_mode": "test",
            "group": {
                "enabled": True,
                "internal_id": group,
                "external_group_id": "synthetic-only@chatroom",
                "owner_ids": ["synthetic-owner"],
                "bot_member_id": "synthetic-owner",
                "shared_account": True,
            },
            "resources": {"profile": "standard"},
            "spend": {
                "group_daily_limit": None,
                "session_total_limit": None,
                "production_requires_explicit_limit": False,
                "pricing_snapshot_id": "deepseek-20261114-text",
            },
            "model_routes": {"text_agent": "deepseek-flash"},
            "runtime": {
                "channel": "local",
                "engine": "langgraph",
                "worker_image": "sha256:" + "0" * 64,
            },
        }
    )
    store = PostgresStore(args.dsn_file.read_text().strip())
    app = build_app(config, store, destination / "app", args.secrets)
    report = {
        "run_id": args.run_id,
        "provider": "actual DeepSeek",
        "passed": False,
        "scope": "real foreground/model/PG/local spool; research admission only, no worker or WeChat",
        "cases": [],
    }
    output = destination / "report.json"
    save(output, report)

    async def turn(name, text, *, peer=False, quote=None):
        stamp = datetime.now(UTC)
        event = GroupEvent(
            id=uuid4().hex,
            source_key=uuid4().hex,
            group_id=group,
            actor_id="synthetic-peer" if peer else "synthetic-owner",
            kind=EventKind.QUOTE if quote else EventKind.TEXT,
            text=text,
            reply_to=quote,
            occurred_at=stamp,
            received_at=stamp,
            channel_account_id="local-account",
            trusted_actor_origin="agentwechat_authenticated_message_metadata",
        )
        actor = app.coordinator.actor(event)
        with store.transaction(actor) as tx:
            tx.ingest_event(event, "synthetic:" + event.id, "1", 0)
        before = time.monotonic()
        result = await app.coordinator.process(event)
        await app.dispatcher.tick()
        with store.transaction(app.owner) as tx:
            works = tx.list_work()
            rows = tx.conn.execute(
                "SELECT p.rendered_text FROM ga_delivery_parts p "
                "JOIN ga_deliveries d ON d.group_id=p.group_id AND d.id=p.intent_id "
                "WHERE d.group_id=%s AND d.request_event_id=%s ORDER BY p.position",
                (group, event.id),
            ).fetchall()
        entry = {
            "case": name,
            "seconds": time.monotonic() - before,
            "kind": result.decision.kind,
            "reply": result.decision.response,
            "wire_text": [row["rendered_text"] for row in rows],
            "work_count": len(works),
            "work_id": result.work_id,
        }
        report["cases"].append(entry)
        save(output, report)
        print(
            json.dumps(
                {
                    "case": name,
                    "kind": result.decision.kind,
                    "work_count": len(works),
                    "seconds": entry["seconds"],
                }
            ),
            flush=True,
        )
        return event, entry

    try:
        if args.case == "stable_concept":
            _, concept = await turn("stable_concept", "Pot，RAG是什么？")
            assert concept["reply"] and concept["work_count"] == 0
            report["passed"] = True
            return
        if args.case == "implicit_install":
            _, installed = await turn(
                "implicit_install_lookup",
                "Pot，告诉我如何下载并安装WorkBuddy。",
            )
            assert installed["work_id"] and installed["work_count"] == 1
            assert installed["reply"] and installed["wire_text"], (
                "A directly addressed action must acknowledge real admission, not escape silently"
            )
            report["passed"] = True
            return
        peer, ambient = await turn("peer_source", "我先试一下群里的功能。", peer=True)
        assert ambient["kind"] in {"observe", "ignore"} and not ambient["wire_text"]
        _, quiet = await turn(
            "peer_quote_narrative", "我用框架重写了Pot的内核，刚才在测验。", quote=peer.id
        )
        assert quiet["kind"] in {"observe", "ignore"} and not quiet["wire_text"]
        _, chat = await turn("brief_plain_chat", "Pot，请用一句话说说什么是人。")
        assert chat["reply"] and chat["work_count"] == 0
        assert not re.search(r"\*\*|^#{1,6} |```|\|---", "\n".join(chat["wire_text"]), re.M)
        assert len([part for part in re.split(r"[。！？!?]", chat["reply"]) if part.strip()]) <= 3
        _, action = await turn(
            "lookup_first_turn",
            "Pot，去网上核实Python官方下载入口，查完告诉我，不需要先问我要链接。",
        )
        assert action["work_id"] and action["work_count"] == 1
        _, progress = await turn("followup_same_work", "Pot，你现在在做什么？请简短说明。")
        assert progress["reply"] and progress["work_count"] == 1
        assert not any("需要我把" in (case["reply"] or "") for case in report["cases"])
        report["passed"] = True
    finally:
        report["finished_at"] = datetime.now(UTC).isoformat()
        save(output, report)
        await app.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--dsn-file", type=Path, required=True)
    parser.add_argument("--secrets", type=Path, default=ROOT / ".env.local")
    parser.add_argument(
        "--case", choices=("dialogue", "implicit_install", "stable_concept"), default="dialogue"
    )
    asyncio.run(run(parser.parse_args()))
