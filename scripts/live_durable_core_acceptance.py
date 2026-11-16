"""Authorized DeepSeek + real worker acceptance, synthetic local channel only.

No WeChat connection. Limits are explicitly disabled; costs remain in the
durable ledger. The Mac host uses a declared synthetic memory sample to exercise
the server_2gb configuration; this does not measure the production game/server.
"""

from __future__ import annotations

import argparse
import asyncio
import io
import json
import time
import zipfile
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

from pptx import Presentation
from pypdf import PdfReader

from groupagent.artifacts import ArtifactScope
from groupagent.billing import BudgetLedger
from groupagent.config import AppConfig
from groupagent.domain import EventKind, GroupEvent
from groupagent.persistence import PostgresStore
from groupagent.resources import MemorySnapshot
from groupagent.runtime.app import build_app

ROOT = Path(__file__).resolve().parents[1]
CASES = {
    "name": "Pot，请告诉我你叫什么名字，用一句话回答。",
    "compute": "Pot，请用Python实际计算1到100的平方和，告诉我结果并简短核对。",
    "illustrated_pdf": (
        "Pot，请制作并交付一份PDF，文件名为核心验收.pdf，标题为核心验收。"
        "解释1到100平方和338350，包含两张用程序绘制的简单示意图，每图附简短说明。"
        "只需两页以内，不需要搜索或插画服务。请直接完成并发文件。"
    ),
    "memes_word": (
        "Pot，请找2张奶蛙表情包，放在一份Word文档里，文件名为奶蛙验收.docx。"
        "每张图下面附对应来源链接。请直接完成并交付文档。"
    ),
    "pptx": (
        "Pot，请制作并交付恰好2页的PPT，文件名为小组验收.pptx。"
        "第一页标题为目标，写三项小组协作建议；第二页标题为安排，写一周计划。"
        "不需要搜索，也不要登记真实分工。"
    ),
}


def save(path, value):
    temporary = path.with_suffix(".pending")
    temporary.write_text(json.dumps(value, ensure_ascii=False, indent=2, default=str) + "\n")
    temporary.chmod(0o600)
    temporary.replace(path)


async def run(args):
    destination = ROOT / "runtime/live" / args.run_id
    destination.mkdir(parents=True, mode=0o700, exist_ok=False)
    group = "durable-local-" + args.run_id
    config = AppConfig.model_validate(
        {
            "deployment_mode": "test",
            "group": {
                "enabled": True,
                "internal_id": group,
                "external_group_id": "synthetic-durable-local@chatroom",
                "owner_ids": ["synthetic-owner"],
            },
            "resources": {"profile": "server_2gb"},
            "spend": {
                "group_daily_limit": None,
                "session_total_limit": None,
                "production_requires_explicit_limit": False,
                "pricing_snapshot_id": "deepseek-20261114-text",
            },
            "model_routes": {
                "text_agent": "deepseek-flash",
                "vision": "deepseek-flash",
                "speech_to_text": "tools/asr",
            },
            "runtime": {
                "channel": "local",
                "engine": "langgraph",
                "worker_image": args.worker_image,
                "docker_host": args.docker_host,
                "local_member_names": {"synthetic-owner": "合成测试同学"},
            },
        }
    )
    store = PostgresStore(args.dsn_file.read_text().strip())
    app = build_app(config, store, destination / "app", args.secrets)
    assert app.worker_governor
    app.worker_governor._sampler = lambda: MemorySnapshot(1300 * 1024**2)
    report = {
        "run_id": args.run_id,
        "framework": "langchain1.5.0/langgraph1.2.22",
        "worker_image": args.worker_image,
        "live_provider": "DeepSeek",
        "channel": "synthetic LocalChannel",
        "resource_sample": "synthetic 1300MiB available; actual worker limit 512MiB",
        "started_at": datetime.now(UTC).isoformat(),
        "expected_cases": args.case or list(CASES),
        "cases": [],
    }
    output = destination / "report.json"
    save(output, report)
    task = asyncio.create_task(app.run())
    try:
        while app._application_lease is None:
            if task.done():
                task.result()
            await asyncio.sleep(0.05)
        for name in args.case or list(CASES):
            now = datetime.now(UTC)
            event = GroupEvent(
                id=uuid4().hex,
                source_key=f"durable-acceptance:{args.run_id}:{name}",
                group_id=group,
                actor_id="synthetic-owner",
                kind=EventKind.TEXT,
                text=CASES[name],
                occurred_at=now,
                received_at=now,
            )
            app.channel.append_incoming(event)
            began = time.monotonic()
            entry = {"case": name, "event_id": event.id, "passed": False}
            report["cases"].append(entry)
            save(output, report)
            while time.monotonic() - began < args.timeout:
                if task.done():
                    task.result()
                    raise RuntimeError("Application stopped during acceptance")
                with store.transaction(app.owner) as tx:
                    rows = tx.conn.execute(
                        "SELECT id,state FROM ga_work_orders WHERE group_id=%s AND request_event_id=%s",
                        (group, event.id),
                    ).fetchall()
                    replies = tx.conn.execute(
                        "SELECT state,payload_ref,created_at FROM ga_deliveries "
                        "WHERE group_id=%s AND request_event_id=%s ORDER BY created_at",
                        (group, event.id),
                    ).fetchall()
                    decision_row = tx.conn.execute(
                        "SELECT body FROM ga_decisions WHERE group_id=%s AND event_id=%s",
                        (group, event.id),
                    ).fetchone()
                    decided = decision_row["body"] if decision_row else None
                if replies:
                    entry.setdefault("first_response_seconds", time.monotonic() - began)
                if name == "name":
                    if replies and all(row["state"] == "accepted" for row in replies):
                        texts = [row["payload_ref"] for row in replies]
                        entry["passed"] = (
                            not rows
                            and len(texts) == 1
                            and "Pot" in texts[0]
                            and "稍后" not in texts[0]
                            and len(texts[0]) < 160
                        )
                        entry["responses"] = texts
                        break
                elif rows and all(row["state"] == "completed" for row in rows):
                    entry["passed"] = True
                    break
                elif any(row["state"] in {"failed", "cancelled", "waiting_input"} for row in rows):
                    break
                elif decided and not decided.get("agent_pending_response") and not rows:
                    entry["failure"] = "execution_request_answered_without_execution"
                    entry["responses"] = [row["payload_ref"] for row in replies]
                    break
                await asyncio.sleep(0.2)
            entry["seconds"] = time.monotonic() - began
            entry["works"] = rows
            if rows:
                checkpoint = app.state.load_checkpoint(app.owner, rows[0]["id"])
                if checkpoint:
                    entry["tool_receipts"] = [
                        r.model_dump(mode="json") for r in checkpoint.receipts
                    ]
                    entry["usage"] = checkpoint.usage.model_dump()
                    if name == "compute":
                        entry["passed"] &= any(
                            r.success
                            and r.tool_name == "python_compute"
                            and "338350" in str(r.output)
                            for r in checkpoint.receipts
                        )
                with store.transaction(app.owner) as tx:
                    outputs = tx.conn.execute(
                        "SELECT c.body,c.validation FROM ga_work_outputs o JOIN ga_artifact_catalog c "
                        "ON c.group_id=o.group_id AND c.version_id=o.version_id "
                        "WHERE o.group_id=%s AND o.work_id=%s AND o.purpose='result'",
                        (group, rows[0]["id"]),
                    ).fetchall()
                formats = []
                for row in outputs:
                    version = row["body"]
                    formats.append(version["format"])
                    if not entry["passed"]:
                        continue  # Retain incomplete drafts as evidence, never claim validation.
                    scope = ArtifactScope(group, rows[0]["id"], version["created_by"])
                    content = app.dispatcher.artifacts.artifacts.read(
                        scope, version["file_id"], version["version_id"]
                    )
                    assert row["validation"] and row["validation"].get("passed")
                    if name == "illustrated_pdf" and version["format"] == "pdf":
                        pdf = PdfReader(io.BytesIO(content))
                        entry["page_count"] = len(pdf.pages)
                        entry["passed"] &= len(pdf.pages) <= 2
                        entry["pdf_image_count"] = sum(len(p.images) for p in pdf.pages)
                        entry["passed"] &= entry["pdf_image_count"] >= 2
                        entry["passed"] &= "338350" in "".join(
                            p.extract_text() or "" for p in pdf.pages
                        )
                    elif name == "memes_word" and version["format"] == "docx":
                        with zipfile.ZipFile(io.BytesIO(content)) as archive:
                            entry["word_image_count"] = sum(
                                n.startswith("word/media/") for n in archive.namelist()
                            )
                            xml = archive.read("word/document.xml").decode()
                            entry["source_link_count"] = xml.count("https://") + xml.count(
                                "http://"
                            )
                        entry["passed"] &= (
                            entry["word_image_count"] >= 2 and entry["source_link_count"] >= 2
                        )
                    elif name == "pptx" and version["format"] == "pptx":
                        entry["slide_count"] = len(Presentation(io.BytesIO(content)).slides)
                        entry["passed"] &= entry["slide_count"] == 2
                entry["formats"] = formats
                expected = {"illustrated_pdf": "pdf", "memes_word": "docx", "pptx": "pptx"}.get(
                    name
                )
                if expected:
                    entry["passed"] &= expected in formats
            save(output, report)
            print(
                json.dumps(
                    {k: v for k, v in entry.items() if k not in {"tool_receipts"}},
                    ensure_ascii=False,
                ),
                flush=True,
            )
            if not entry["passed"]:
                break
    finally:
        app.stop.set()
        await asyncio.wait_for(task, timeout=30)
        report["accounting"] = BudgetLedger(store).report(app.owner)
        report["finished_at"] = datetime.now(UTC).isoformat()
        report["passed"] = len(report["cases"]) == len(report["expected_cases"]) and all(
            c["passed"] for c in report["cases"]
        )
        save(output, report)
    if not report["passed"]:
        raise SystemExit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--dsn-file", type=Path, required=True)
    parser.add_argument("--worker-image", required=True)
    parser.add_argument("--docker-host", required=True)
    parser.add_argument("--secrets", type=Path, default=ROOT / ".env.local")
    parser.add_argument("--case", choices=list(CASES), action="append")
    parser.add_argument("--timeout", type=int, default=300)
    asyncio.run(run(parser.parse_args()))
