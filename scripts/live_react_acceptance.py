"""Run a small, synthetic holdout against the real model, PG, worker and LocalChannel."""

from __future__ import annotations

import argparse
import asyncio
import json
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

from groupagent.config import AppConfig
from groupagent.domain import EventKind, GroupEvent, WorkState
from groupagent.persistence import PostgresStore
from groupagent.resources import MemorySnapshot
from groupagent.runtime.app import build_app

ROOT = Path(__file__).resolve().parents[1]


def write_report(path: Path, report: dict) -> None:
    data = path.with_suffix(".pending")
    data.write_text(json.dumps(report, ensure_ascii=False, indent=2, default=str) + "\n")
    data.chmod(0o600)
    data.replace(path)


async def run(args) -> None:
    destination = ROOT / "runtime" / "live" / args.run_id
    if not args.resume:
        destination.mkdir(parents=True, exist_ok=False, mode=0o700)
    group = f"local-react-holdout-{args.run_id}"
    config = AppConfig.model_validate(
        {
            "deployment_mode": "test",
            "group": {
                "enabled": True,
                "internal_id": group,
                "external_group_id": "synthetic-local-only@chatroom",
                "owner_ids": ["synthetic-owner"],
            },
            "conversation": {"proactive_mode": "shadow"},
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
                "worker_image": args.worker_image,
                "docker_host": args.docker_host,
            },
        }
    )
    store = PostgresStore(args.dsn_file.read_text().strip())
    app = build_app(config, store, destination / "app", args.secrets)
    report: dict = (
        json.loads((destination / "report.json").read_text())
        if args.resume
        else {
            "scope": "Synthetic inputs, real DeepSeek/PG/LocalChannel, actual isolated compute; no WeChat",
            "resource_sample": "Synthetic 1300 MiB host headroom; actual worker cgroup limit",
            "cases": [],
            "passed": False,
            "worker_image": args.worker_image,
        }
    )
    assert not report["passed"] and report["worker_image"] == args.worker_image
    write_report(destination / "report.json", report)
    assert app.worker_governor is not None
    app.worker_governor._sampler = lambda: MemorySnapshot(1300 * 1024**2)
    factory = app.coordinator.provider_factory
    foreground_calls: list[dict] = list(report.get("foreground_calls", ()))

    def instrument(actor, event):
        provider = factory(actor, event)
        original = provider.kernel.model.complete

        async def complete(request):
            began = time.monotonic()
            response = await original(request)
            foreground_calls.append(
                {
                    "event_id": event.id,
                    "seconds": time.monotonic() - began,
                    "calls": [tool.name for tool in response.tool_calls],
                }
            )
            return response

        provider.kernel.model.complete = complete
        return provider

    app.coordinator.provider_factory = instrument

    def ingest(text, *, sender="synthetic-owner", reply_to=None):
        now = datetime.now(UTC)
        identifier = uuid4().hex
        event = GroupEvent(
            id=identifier,
            source_key="held-out:" + identifier,
            group_id=group,
            actor_id=sender,
            kind=EventKind.QUOTE if reply_to else EventKind.TEXT,
            text=text,
            reply_to=reply_to,
            occurred_at=now,
            received_at=now,
            channel_account_id=config.runtime.channel_account_id,
            trusted_actor_origin="agentwechat_authenticated_message_metadata",
        )
        with store.transaction(app.coordinator.actor(event)) as tx:
            tx.ingest_event(event, "held-out:" + identifier, "1", 0)
        return event

    def add(name, **data):
        report["cases"].append({"case": name, **data})
        write_report(destination / "report.json", report)
        print(json.dumps(report["cases"][-1], ensure_ascii=False, default=str), flush=True)

    async def turn(name, event):
        before = len(foreground_calls)
        began = time.monotonic()
        result = await app.coordinator.process(event)
        await app.dispatcher.tick()
        with store.transaction(app.owner) as tx:
            replies = tx.conn.execute(
                "SELECT state,payload_ref FROM ga_deliveries WHERE group_id=%s AND request_event_id=%s",
                (group, event.id),
            ).fetchall()
        add(
            name,
            seconds=round(time.monotonic() - began, 3),
            model_calls=[item["calls"] for item in foreground_calls[before:]],
            kind=result.decision.kind,
            work=bool(result.work_id),
            replies=replies,
        )
        return result, replies, len(foreground_calls) - before

    async def settle(identifier):
        deadline = time.monotonic() + 160
        while time.monotonic() < deadline:
            await app.execute_tick()
            await app.dispatcher.tick()
            with store.transaction(app.owner) as tx:
                work_state = tx.get_work(identifier).state
            if work_state in {
                WorkState.COMPLETED,
                WorkState.FAILED,
                WorkState.CANCELLED,
                WorkState.WAITING_INPUT,
            }:
                return work_state
            await asyncio.sleep(0.2)
        return work_state

    try:
        await app.start()
        if not args.resume:
            _, replies, calls = await turn(
                "ordinary", ingest("Pot，用一句话解释为什么水珠容易从荷叶上滚落。")
            )
            assert len(replies) == 1 and replies[0]["state"] == "accepted" and calls == 1
            peer = ingest("小姚，你的展柜钥匙在我这里。", sender="synthetic-peer")
            _, replies, _ = await turn(
                "to_peer",
                ingest("我只是问你一会儿哪里取钥匙，Pot之前也参加过。", reply_to=peer.id),
            )
            assert not replies

            example, replies, _ = await turn(
                "code_only",
                ingest("Pot，给我一小段 Python 示例，用来数「竹叶」出现的次数。不用运行。"),
            )
            assert not example.work_id and len(replies) == 1 and replies[0]["state"] == "accepted"
            with store.transaction(app.owner) as tx:
                assert not tx.list_work()

            source = ingest("周班展台的借用登记今晚截止，我已经提交了，别再催了。")
            with store.transaction(app.owner) as tx:
                task = tx.create_team_task(
                    "核对周班展台的登记回执",
                    due_at=datetime.now(UTC) + timedelta(minutes=40),
                    source_event_id=source.id,
                )
                tx.update_team_task(
                    task.id,
                    expected_revision=task.revision,
                    progress="已提交，馆方已接受，团队已知情。当前无须再提醒。",
                    source_event_id=source.id,
                )
            assert app.observer is not None
            assert await app.observer.tick() in {"silent", "shadow"}
            with store.transaction(app.owner) as tx:
                result = tx.conn.execute(
                    "SELECT state,reason_code,proposal_sha256,delivery_id FROM ga_react_observations "
                    "WHERE group_id=%s AND resource_id=%s",
                    (group, task.id),
                ).fetchone()
            add("observer_shadow", receipt=result)
            assert result["state"] == "silent" and result["delivery_id"] is None

            compound = ingest(
                "Pot，请实际运行 Python 求 4、6、8 的立方和，并把实跑结果发来。"
                "另外从这条消息起十分钟后，在本群原样提醒「调暗展柜灯」。"
            )
            started, replies, _ = await turn("mixed_work_and_reminder", compound)
            assert replies and started.work_id
        with store.transaction(app.owner) as tx:
            works = tx.list_work()
            assert len(works) == 1 and "python_compute" in works[0].scope["required_tools"]
            identifier = works[0].id
            compound = tx.get_event(works[0].request_event_id)
            if args.resume and works[0].pause_reason == "unsupported_model_capability":
                # A previous fixture version omitted the reviewed media routes.
                # This terminal preparation failure took no tool action.
                tx.transition_work(
                    identifier,
                    WorkState.QUEUED,
                    expected_revision=works[0].revision,
                    reason="held_out_test_routes_corrected",
                )
            tx.conn.execute(
                "INSERT INTO ga_conversation_policy(group_id,subject_id,quiet) VALUES(%s,%s,true) "
                "ON CONFLICT (group_id,subject_id) DO UPDATE SET quiet=true",
                (group, app.owner.member_id),
            )
        scheduler = app.coordinator.scheduler
        rules = scheduler.list(app.owner)
        assert len(rules) == 1
        rule = rules[0]
        assert rule.execution_kind.value == "explicit_reminder" and rule.timing.kind == "at"
        base = timedelta(minutes=10)
        assert abs((rule.timing.at - compound.occurred_at - base).total_seconds()) < 2
        scheduler.wake(app.owner, rule.timing.at + timedelta(seconds=1))
        occurred = scheduler.occurrences(app.owner, rule.id)
        assert len(occurred) == 1 and occurred[0].work_id is None and occurred[0].delivery_id
        scheduler.wake(app.owner, rule.timing.at + timedelta(seconds=2))
        await app.dispatcher.tick()
        with store.transaction(app.owner) as tx:
            notice = tx.get_delivery(occurred[0].delivery_id)
        assert notice.state.value == "accepted" and notice.payload_ref == "text:调暗展柜灯"
        assert len(scheduler.occurrences(app.owner, rule.id)) == 1
        add("simulated_due_with_personal_quiet", actual_state=notice.state.value, one_delivery=True)

        work_state = await settle(identifier)
        checkpoint = app.state.load_checkpoint(app.owner, identifier)
        proof = (
            [
                {"name": receipt.tool_name, "output": receipt.output}
                for receipt in checkpoint.receipts
                if receipt.success and receipt.tool_name == "python_compute"
            ]
            if checkpoint
            else []
        )
        add("actual_isolated_compute", state=work_state.value, tool_receipts=proof)
        assert work_state == WorkState.COMPLETED and proof and "792" in str(proof)

        if args.silent_goal:
            known_rules = {item.id for item in scheduler.list(app.owner)}
            await turn(
                "conditional_future_goal",
                ingest(
                    "Pot，另约七分钟后检查本群的分工有没有已经逾期的任务。到时候再看，"
                    "若没有逾期的，就静默结束，完全不发通知，只有发现逾期才告诉大家。"
                ),
            )
            future = [item for item in scheduler.list(app.owner) if item.id not in known_rules]
            assert len(future) == 1 and future[0].scope.get("allow_silent_result") is True
            assert future[0].next_due_at is not None
            scheduler.wake(app.owner, future[0].next_due_at + timedelta(seconds=1))
            acting = scheduler.occurrences(app.owner, future[0].id)[0]
            assert acting.work_id and acting.delivery_id is None
            state = await settle(acting.work_id)
            with store.transaction(app.owner) as tx:
                quiet = tx.conn.execute(
                    "SELECT passed FROM ga_work_validations WHERE group_id=%s AND work_id=%s "
                    "AND check_key='silent_result'",
                    (group, acting.work_id),
                ).fetchone()
                sends = [
                    intent for intent in tx.list_delivery() if intent.work_id == acting.work_id
                ]
            add(
                "conditional_future_silence", state=state.value, quiet=quiet, group_sends=len(sends)
            )
            assert state == WorkState.COMPLETED and quiet and quiet["passed"] and not sends
        report["passed"] = True
    finally:
        report["foreground_calls"] = foreground_calls
        write_report(destination / "report.json", report)
        await app.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--dsn-file", required=True, type=Path)
    parser.add_argument("--worker-image", required=True)
    parser.add_argument("--docker-host", required=True)
    parser.add_argument("--secrets", type=Path, default=ROOT / ".env.local")
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--silent-goal", action="store_true")
    asyncio.run(run(parser.parse_args()))
