"""Opt-in integrated live DeepSeek/worker checks, local spool only, shared 50 CNY cap."""

from __future__ import annotations

import asyncio
import json
import time
from datetime import UTC, datetime
from uuid import uuid4

from live_preflight import ROOT, setup

from groupagent.config import AppConfig
from groupagent.coordinator import CoordinationResult, Decision
from groupagent.domain import EventKind, GroupEvent
from groupagent.runtime.app import build_app

CASES = [
    (
        "compute",
        "请帮我用 Python 实际计算 1 到 100 的平方和，给出计算结果和简短核对过程。",
        "338350",
    ),
    (
        "markdown",
        "请新建一项独立工作：制作一份名为校园节水活动方案的 Markdown 文件，包含目标、三项行动、两周时间安排、三位成员的分工建议；用虚构姓名，不作真实分配，写约300字并交付文件。",
        ".md",
    ),
    (
        "docx",
        "请新建独立工作：制作并交付一份 Word 文档，标题为大学小组调研提纲，包含研究问题、访谈提纲、数据整理方法三个部分，正文不少于250字，使用中文。",
        ".docx",
    ),
]


async def run(args):
    store, ledger, actor, _key = setup()
    destination = ROOT / "runtime/live" / args.run_id
    if destination.exists():
        raise RuntimeError("Use a new run ID; never overwrite acceptance evidence")
    destination.mkdir(parents=True, mode=0o700)
    # Earlier media fixtures predate the common service; their synthetic source
    # markers are not conversation requests. Do not change any actual chat data.
    with store.transaction(actor) as tx:
        old = tx.conn.execute(
            "SELECT e.id FROM ga_events e WHERE group_id=%s AND "
            "(source_key LIKE 'live-media:%%' OR source_key LIKE 'synthetic:media-%%') AND "
            "NOT EXISTS(SELECT 1 FROM ga_decisions d WHERE d.group_id=e.group_id AND d.event_id=e.id)",
            (actor.group_id,),
        ).fetchall()
        for row in old:
            result = CoordinationResult(
                event_id=row["id"],
                decision=Decision(
                    kind="observe", reason_code="acceptance_fixture_not_user_request"
                ),
            )
            tx.record_decision(
                row["id"], {"kind": "observe", "result": result.model_dump(mode="json")}
            )
    config = AppConfig.model_validate(
        {
            "deployment_mode": "test",
            "group": {
                "enabled": True,
                "internal_id": actor.group_id,
                "external_group_id": "synthetic-local@chatroom",
                "owner_ids": [actor.member_id],
            },
            "spend": {
                "group_daily_limit": 50,
                "session_total_limit": 50,
                "pricing_snapshot_id": "deepseek-20261114-text",
            },
            "model_routes": {
                "text_agent": "deepseek-flash",
                "vision": "deepseek-flash",
                "speech_to_text": "tools/asr",
                "text_to_speech": "tools/tts",
            },
            "runtime": {
                "channel": "local",
                "channel_account_id": "local:" + args.run_id,
                "engine": "harness",
                "worker_image": args.worker_image,
                "harness_image": args.harness_image,
                "docker_host": args.docker_host,
                "local_member_names": {actor.member_id: "合成测试同学"},
            },
        }
    )
    app = build_app(config, store, destination / "app", ROOT / ".env.local")
    records = []
    try:
        await app.start()
        for identifier, prompt, expected in CASES[: args.cases]:
            stamp = datetime.now(UTC)
            request = GroupEvent(
                id=uuid4().hex,
                source_key="live-app:" + args.run_id + ":" + identifier,
                group_id=actor.group_id,
                actor_id=actor.member_id,
                kind=EventKind.TEXT,
                text=prompt,
                occurred_at=stamp,
                received_at=stamp,
            )
            app.channel.append_incoming(request)
            started = time.monotonic()
            status = "TIMEOUT"
            while time.monotonic() - started < args.timeout:
                await app.step()
                await asyncio.sleep(0.2)
                with store.transaction(actor) as tx:
                    decided = tx.get_decision(request.id)
                    work = tx.conn.execute(
                        "SELECT * FROM ga_work_orders WHERE group_id=%s AND request_event_id=%s",
                        (actor.group_id, request.id),
                    ).fetchone()
                    if work and work["state"] in {
                        "completed",
                        "failed",
                        "paused",
                        "cancelled",
                        "waiting_input",
                    }:
                        status = work["state"]
                        break
                    if decided and decided["result"]["decision"]["kind"] in {
                        "reply",
                        "clarify",
                        "observe",
                        "ignore",
                    }:
                        status = "no_work:" + decided["result"]["decision"]["kind"]
                        break
            with store.transaction(actor) as tx:
                artifacts = tx.conn.execute(
                    "SELECT name,format,sha256,validation FROM ga_artifact_catalog WHERE group_id=%s AND work_id=%s AND shared",
                    (actor.group_id, work["id"] if work else "none"),
                ).fetchall()
                intents = tx.conn.execute(
                    "SELECT payload_ref,state FROM ga_deliveries WHERE group_id=%s AND work_id=%s",
                    (actor.group_id, work["id"] if work else "none"),
                ).fetchall()
            met = (
                any(expected in r["name"] for r in artifacts)
                if expected.startswith(".")
                else any(expected in r["payload_ref"] and r["state"] == "accepted" for r in intents)
            )
            record = {
                "id": identifier,
                "status": "PASS" if status == "completed" and met else "FAIL",
                "work_state": status,
                "work_id": work["id"] if work else None,
                "seconds": round(time.monotonic() - started, 2),
                "artifacts": artifacts,
                "local_delivery_count": len(intents),
                "channel": "local_spool",
                "live_wechat": False,
            }
            records.append(record)
            (destination / "results.json").write_text(
                json.dumps(
                    {"cases": records, "budget": ledger.report(actor)},
                    ensure_ascii=False,
                    indent=2,
                    default=str,
                )
            )
            print(json.dumps(record, ensure_ascii=False, default=str), flush=True)
    finally:
        await app.close()
        print(json.dumps({"budget": ledger.report(actor)}, default=str), flush=True)


if __name__ == "__main__":
    raise SystemExit(
        "This historical Harness acceptance entry point is retired. Use scripts/live_durable_core_acceptance.py; it retains accounting without goal budgets."
    )
