"""Opt-in local Spec:92 measurements; imports never bootstrap keys or providers.

existing: read-only historical DB-clock support, not commit/monotonic SLO proof.
probe: root-operated real-model short answers/optional admissions through app.run.
cancel: deterministic provider + real local intake/DB/gateway, no keys or billing.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import math
import re
import time
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

from live_runtime_matrix import ROOT, canonical_hash, save, source_hash

from groupagent.config import AppConfig
from groupagent.domain import ActorContext, DomainError, EventKind, GroupEvent, Role
from groupagent.persistence import PostgresStore

LIMITS = {"short_answer": 20.0, "long_admission": 5.0, "cancel_fence": 2.0}
QUESTIONS = (
    "请用一句话解释JSON是什么。",
    "请用一句话说明列表与元组的区别。",
    "请用一句话解释哈希的用途。",
    "请用一句话解释数据库事务。",
    "请用一句话说明HTTP状态码404。",
    "请用一句话解释单元测试。",
    "请用一句话解释UTF-8。",
    "请用一句话解释版本控制。",
    "请用一句话解释中位数。",
    "请用一句话说明平均数和中位数的区别。",
    "请用一句话解释幂等。",
    "请用一句话解释缓存。",
    "请用一句话说明CSV是什么。",
    "请用一句话解释队列。",
    "请用一句话解释时间戳。",
    "请用一句话解释时区。",
    "请用一句话解释只读权限。",
    "请用一句话解释文件校验和。",
    "请用一句话解释并发。",
    "请用一句话解释乐观锁。",
    "请用一句话解释数据库索引。",
    "请用一句话解释重试退避。",
    "请用一句话解释日志。",
    "请用一句话解释数据备份。",
    "请用一句话解释数据恢复。",
    "请用一句话解释引用来源。",
    "请用一句话解释结构化数据。",
    "请用一句话解释输入验证。",
    "请用一句话解释超时。",
    "请用一句话解释软件回归测试。",
)


def p95(values):
    values = sorted(values)
    if not values:
        return None
    if any(
        not isinstance(value, (int, float))
        or isinstance(value, bool)
        or not math.isfinite(value)
        or value < 0
        for value in values
    ):
        raise ValueError("Latency samples must be finite nonnegative measured values")
    return values[math.ceil(0.95 * len(values)) - 1]


def summarize(records):
    result = {}
    for kind, limit in LIMITS.items():
        rows = [row for row in records if row["kind"] == kind]
        measured = [
            row["latency_upper_seconds"]
            for row in rows
            if row.get("latency_upper_seconds") is not None
        ]
        valid = all(row.get("status") == "PASS" for row in rows)
        percentile = p95(measured) if len(measured) == len(rows) else None
        result[kind] = {
            "samples": len(rows),
            "failed_or_missing": sum(row.get("status") != "PASS" for row in rows),
            "p95_conservative_upper_seconds": percentile,
            "limit_seconds": limit,
            "status": "NOT_RUN"
            if len(rows) < 30
            else "FAIL"
            if not valid or percentile is None or percentile > limit
            else "PASS",
        }
    return result


def create_run(run_id, config, mode):
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,79}", run_id) or ".." in run_id:
        raise ValueError("Invalid independent run ID")
    root = ROOT / "runtime/live" / run_id
    if root.resolve() != root.absolute():
        raise ValueError("Run path cannot be redirected")
    root.mkdir(parents=True, exist_ok=False, mode=0o700)
    manifest = {
        "schema_version": "groupagent.performance-acceptance.v1",
        "run_id": run_id,
        "mode": mode,
        "source_sha256": source_hash(),
        "config_sha256": canonical_hash(config),
        "script_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "started_at": datetime.now(UTC).isoformat(),
        "ended_at": None,
        "thresholds": LIMITS,
        "percentile_definition": "nearest rank: ascending[ceil(0.95*n)-1]; failures are retained and block PASS",
        "clock": {
            name: getattr(time.get_clock_info("monotonic"), name)
            for name in ("implementation", "resolution", "monotonic", "adjustable")
        },
        "receive_definition": "commit bracket: last DB query snapshot observing absence to first DB query observing the committed event",
        "outbox_definition": "commit bracket: last DB query snapshot observing no bound intent to first DB query observing it",
        "latency_upper_definition": "outbox/fence observation end minus durable-receive lower bound",
        "platform_transport": "NOT_APPLICABLE: LocalChannel only; no WeChat ingress or mobile-delivery latency",
        "executor_lane": "held to prevent expensive work execution; not a heavy-worker throughput benchmark",
        "cancel_provider": "deterministic fixture; never reported as live model latency",
    }
    save(root / "config.json", config)
    save(root / "manifest.json", manifest)
    return root, manifest


def observe(store, actor, event_id):
    with store.connect() as conn:
        conn.execute("SET TRANSACTION READ ONLY")
        started = time.monotonic_ns()
        row = conn.execute(
            "SELECT e.id,e.created_at AS event_db_at,d.id AS delivery_id,d.created_at AS outbox_db_at,c.body AS decision "
            "FROM ga_events e LEFT JOIN ga_decisions c ON c.group_id=e.group_id AND c.event_id=e.id "
            "LEFT JOIN LATERAL (SELECT id,created_at FROM ga_deliveries WHERE group_id=e.group_id AND request_event_id=e.id ORDER BY created_at,id LIMIT 1) d ON true "
            "WHERE e.group_id=%s AND e.id=%s",
            (actor.group_id, event_id),
        ).fetchone()
        return started, time.monotonic_ns(), dict(row) if row else None


async def await_outbox(app, actor, event, *, submitted_ns, timeout, poll_seconds):
    received_lower = submitted_ns
    received_upper = None
    outbox_lower = submitted_ns
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        started, ended, row = await asyncio.to_thread(observe, app.store, actor, event.id)
        if row is None:
            received_lower = started
        elif received_upper is None:
            received_upper = ended
        if row and row.get("delivery_id"):
            return {
                "event_id": event.id,
                "delivery_id": row["delivery_id"],
                "received_lower_ns": received_lower,
                "received_upper_ns": received_upper,
                "outbox_lower_ns": outbox_lower,
                "outbox_upper_ns": ended,
                "latency_lower_seconds": max(0, (outbox_lower - received_upper) / 1e9),
                "latency_upper_seconds": (ended - received_lower) / 1e9,
                "db_recorded_seconds": (row["outbox_db_at"] - row["event_db_at"]).total_seconds(),
                "event_db_at": row["event_db_at"].isoformat(),
                "outbox_db_at": row["outbox_db_at"].isoformat(),
                "decision": row["decision"],
            }
        outbox_lower = started
        await asyncio.sleep(poll_seconds)
    return {
        "event_id": event.id,
        "status": "TIMEOUT",
        "received_lower_ns": received_lower,
        "received_upper_ns": received_upper,
        "latency_upper_seconds": None,
        "censored_after_seconds": timeout,
    }


async def submit(app, actor, root, label, text, *, reply_to=None, timeout=60, poll_seconds=0.02):
    stamp = datetime.now(UTC)
    event = GroupEvent(
        id=uuid4().hex,
        source_key="performance:" + actor.group_id + ":" + root.name + ":" + label,
        group_id=actor.group_id,
        actor_id=actor.member_id,
        kind=EventKind.QUOTE if reply_to else EventKind.TEXT,
        text=text,
        occurred_at=stamp,
        received_at=stamp,
        reply_to=reply_to,
        channel_account_id=app.config.runtime.channel_account_id,
        source_epoch=app.config.runtime.source_epoch,
    )
    submitted = time.monotonic_ns()
    app.channel.append_incoming(event)
    return event, await await_outbox(
        app, actor, event, submitted_ns=submitted, timeout=timeout, poll_seconds=poll_seconds
    )


class HeldExecutor:
    async def tick(self):
        pass

    async def close(self):
        pass


class NoProviderSummary:
    async def summarize(self, *args, **kwargs):
        raise DomainError(
            "performance_no_summary_provider",
            "Deterministic cancellation probe does not call a model",
        )


def deterministic_app(store, config, root):
    from groupagent.artifacts import ArtifactStore
    from groupagent.channels.local import LocalChannel
    from groupagent.conversation.service import ConversationCoordinator
    from groupagent.coordinator import Decision
    from groupagent.memory import MemberService, MemoryService
    from groupagent.runtime.app import RuntimeApp, channel_config
    from groupagent.runtime.artifacts import ArtifactRegistry
    from groupagent.runtime.dispatch import Dispatcher
    from groupagent.scheduling import Scheduler

    class Provider:
        async def decide(self, event, context):
            if event.text.startswith("性能取消样本准备"):
                return Decision(
                    kind="start_work",
                    goal="可取消的空工作",
                    reason_code="deterministic_performance_fixture",
                    directness="direct",
                    confidence=1,
                )
            raise AssertionError("Cancel controls must use the actual deterministic control path")

    coordinator = ConversationCoordinator(
        config,
        store,
        lambda actor, event: Provider(),
        memory=MemoryService(store, NoProviderSummary()),
        members=MemberService(store),
        scheduler=Scheduler(store),
    )

    app = RuntimeApp(
        coordinator,
        LocalChannel(root / "app" / "channel", channel_config(config)),
        HeldExecutor(),
        None,
    )
    artifacts = ArtifactStore(root / "app" / "artifacts")
    app.dispatcher = Dispatcher(
        app.state, app.technical, app.resolve_actor, app.channel, ArtifactRegistry(store, artifacts)
    )
    app._performance_artifacts = artifacts
    return app


def ensure_idle(store, actor):
    with store.connect() as conn:
        conn.execute("SET TRANSACTION READ ONLY")
        active = conn.execute(
            "SELECT id FROM ga_work_orders WHERE group_id=%s AND state IN ('queued','running','validating','delivering','waiting_external') LIMIT 1",
            (actor.group_id,),
        ).fetchone()
        pending = conn.execute(
            "SELECT e.id FROM ga_events e LEFT JOIN ga_decisions d ON d.group_id=e.group_id AND d.event_id=e.id LEFT JOIN ga_event_retry r ON r.group_id=e.group_id AND r.event_id=e.id WHERE e.group_id=%s AND e.memory_state='active' AND d.event_id IS NULL AND (r.event_id IS NULL OR NOT r.blocked) LIMIT 1",
            (actor.group_id,),
        ).fetchone()
        delivery = conn.execute(
            "SELECT id FROM ga_deliveries WHERE group_id=%s AND state IN ('pending','dispatching','unknown') LIMIT 1",
            (actor.group_id,),
        ).fetchone()
        scheduled = conn.execute(
            "SELECT id FROM ga_schedules WHERE group_id=%s AND state='active' UNION ALL SELECT id FROM ga_trigger_rules WHERE group_id=%s AND state='active' LIMIT 1",
            (actor.group_id, actor.group_id),
        ).fetchone()
    if active or pending or delivery or scheduled:
        raise RuntimeError(
            "Another run has unresolved work/events/delivery/rules; do not take them over or alter their state"
        )


async def cancel_sample(app, actor, root, index, timeout, poll_seconds):
    from groupagent.domain import WorkState
    from groupagent.tools import (
        PolicyDecision,
        ToolContext,
        ToolData,
        ToolDefinition,
        ToolGateway,
        ToolScope,
    )

    event, seed = await submit(
        app,
        actor,
        root,
        f"cancel-seed-{index}",
        "性能取消样本准备：请受理一个可取消的空工作。",
        timeout=timeout,
        poll_seconds=poll_seconds,
    )
    work_id = (seed.get("decision") or {}).get("result", {}).get("work_id")
    if not work_id:
        return {
            "kind": "cancel_fence",
            "index": index,
            "status": "FAIL",
            "reason": "seed_not_admitted",
            "seed": seed,
            "latency_upper_seconds": None,
        }
    with app.store.transaction(actor) as tx:
        lease = tx.claim_work(work_id, "performance-fixture:" + root.name, ttl_seconds=300)
    calls = []

    class Empty(ToolData):
        pass

    async def handler(value, context):
        calls.append(context.call_id)
        return Empty()

    async def policy(scope):
        with app.store.transaction(actor) as tx:
            tx.assert_lease(lease)
        return PolicyDecision(actor=actor, allowed_tools=frozenset({"fence_probe"}))

    gateway = ToolGateway(policy)
    gateway.register(
        ToolDefinition("fence_probe", "Non-effectful performance gate probe", Empty, Empty, handler)
    )
    workspace = root / "fence" / str(index)
    workspace.mkdir(parents=True)
    scope = ToolScope(
        actor.group_id, work_id, actor, workspace.resolve(), frozenset({"fence_probe"}), event.id
    )
    gateway.bind_scope(scope)
    before = await gateway.invoke(
        "fence_probe", {}, ToolContext(scope, "before", "before", asyncio.Event())
    )
    cancel, record = await submit(
        app,
        actor,
        root,
        f"cancel-{index}",
        "停止这个任务",
        reply_to=event.id,
        timeout=timeout,
        poll_seconds=poll_seconds,
    )
    receipt = await gateway.invoke(
        "fence_probe", {}, ToolContext(scope, "after", "after", asyncio.Event())
    )
    denied = time.monotonic_ns()
    with app.store.transaction(actor) as tx:
        work = tx.get_work(work_id)
    success = (
        before.success
        and not receipt.success
        and receipt.error_code == "stale_lease"
        and len(calls) == 1
        and work.state == WorkState.CANCELLED
    )
    record.update(
        kind="cancel_fence",
        index=index,
        status="PASS" if success else "FAIL",
        work_id=work_id,
        source_event_id=cancel.id,
        deterministic_provider=True,
        fence_receipt=receipt.model_dump(mode="json"),
        handler_calls_before_after=[1, len(calls)],
        fence_observed_ns=denied,
        latency_upper_seconds=(denied - record["received_lower_ns"]) / 1e9,
    )
    return record


async def sample_app(
    app, actor, root, *, mode, short_count, long_count, cancel_count, timeout, poll_seconds
):
    records = []
    daemon = asyncio.create_task(app.run())
    try:
        while app._application_lease is None:
            if daemon.done():
                daemon.result()
            await asyncio.sleep(0.01)
        for kind, count in (("short_answer", short_count), ("long_admission", long_count)):
            for index in range(count):
                if daemon.done():
                    daemon.result()
                prompt = (
                    QUESTIONS[index % len(QUESTIONS)]
                    if kind == "short_answer"
                    else f"请新建独立工作，制作一份合成调研报告{index + 1}，整理资料并交付Markdown。"
                )
                event, record = await submit(
                    app,
                    actor,
                    root,
                    f"{kind}-{index}",
                    prompt,
                    timeout=timeout,
                    poll_seconds=poll_seconds,
                )
                decision = (record.get("decision") or {}).get("result", {}).get("decision", {})
                work_id = (record.get("decision") or {}).get("result", {}).get("work_id")
                with app.store.connect() as conn:
                    conn.execute("SET TRANSACTION READ ONLY")
                    proof = conn.execute(
                        "SELECT b.id,b.state,b.actual_amount,b.evidence_ref FROM ga_budget_reservations b JOIN ga_price_snapshots p ON p.group_id=b.group_id AND p.id=b.price_id WHERE b.group_id=%s AND b.source_event_id=%s AND b.state='settled' AND NOT COALESCE((p.body->>'test_only')::boolean,false)",
                        (actor.group_id, event.id),
                    ).fetchall()
                success = (
                    (
                        decision.get("kind") == "reply"
                        and decision.get("action") is None
                        and not work_id
                    )
                    if kind == "short_answer"
                    else (decision.get("kind") == "start_work" and bool(work_id))
                )
                success = (
                    success
                    and bool(proof)
                    and decision.get("reason_code")
                    not in {
                        "budget_exhausted",
                        "provider_unavailable",
                        "permission_denied",
                        "not_found",
                        "read_only_scope",
                        "provider_output_invalid",
                    }
                )
                record.update(
                    kind=kind,
                    index=index,
                    status="PASS" if success else record.get("status", "FAIL"),
                    provider="real_app_shared_ledger",
                    work_id=work_id,
                    provider_receipts=[dict(row) for row in proof],
                )
                records.append(record)
                save(root / "samples.json", {"records": records})
                if work_id:
                    _, cleanup = await submit(
                        app,
                        actor,
                        root,
                        f"cleanup-{kind}-{index}",
                        "停止这个任务",
                        reply_to=event.id,
                        timeout=timeout,
                        poll_seconds=poll_seconds,
                    )
                    record["human_cancel_cleanup"] = cleanup
                    if (cleanup.get("decision") or {}).get("result", {}).get("decision", {}).get(
                        "control"
                    ) != "cancel":
                        record["status"] = "FAIL"
                        save(root / "samples.json", {"records": records})
                        raise RuntimeError(
                            "Human cancellation cleanup did not commit; do not execute expensive work"
                        )
        for index in range(cancel_count):
            if daemon.done():
                daemon.result()
            records.append(await cancel_sample(app, actor, root, index, timeout, poll_seconds))
            save(root / "samples.json", {"records": records})
    finally:
        # Let the real local dispatcher finish its already-created intents;
        # this does not call a model and is outside the measured outbox interval.
        drain_until = time.monotonic() + 5
        while not daemon.done() and time.monotonic() < drain_until:
            with app.store.connect() as conn:
                conn.execute("SET TRANSACTION READ ONLY")
                pending = conn.execute(
                    "SELECT 1 FROM ga_deliveries WHERE group_id=%s AND state IN ('pending','dispatching') LIMIT 1",
                    (actor.group_id,),
                ).fetchone()
            if pending is None:
                break
            await asyncio.sleep(0.05)
        app.stop.set()
        try:
            await asyncio.wait_for(daemon, 15)
        except TimeoutError:
            daemon.cancel()
            await asyncio.gather(daemon, return_exceptions=True)
        save(root / "samples.json", {"records": records})
        if hasattr(app, "_performance_artifacts"):
            app._performance_artifacts.close()
    return records


def historical(store, matrix_roots):
    reports = []
    for root in matrix_roots:
        metadata = json.loads((root / "manifest.json").read_text())
        config = json.loads((root / "config.json").read_text())
        dataset = json.loads((root / "dataset.json").read_text())
        if canonical_hash(dataset["cases"]) != metadata.get("dataset_sha256"):
            raise ValueError("Historical run dataset checksum changed")
        if metadata.get("config_sha256") and metadata["config_sha256"] != canonical_hash(config):
            raise ValueError("Historical run configuration changed")
        saved = json.loads((root / "results.json").read_text())
        saved_records = {record["id"]: record for record in saved["cases"]}
        cases = [case for case in dataset["cases"] if not case["category"].startswith("schedule")]
        records = []
        with store.connect() as conn:
            conn.execute("SET TRANSACTION READ ONLY")
            for case in cases:
                row = conn.execute(
                    "SELECT e.id,e.created_at AS received_at,d.created_at AS outbox_at,c.body FROM ga_events e JOIN ga_decisions c ON c.group_id=e.group_id AND c.event_id=e.id LEFT JOIN LATERAL (SELECT created_at FROM ga_deliveries WHERE group_id=e.group_id AND request_event_id=e.id ORDER BY created_at,id LIMIT 1)d ON true WHERE e.group_id=%s AND e.source_key=%s AND EXISTS(SELECT 1 FROM ga_budget_reservations b JOIN ga_price_snapshots p ON p.group_id=b.group_id AND p.id=b.price_id WHERE b.group_id=e.group_id AND b.source_event_id=e.id AND b.state='settled' AND b.actual_amount>0 AND NOT COALESCE((p.body->>'test_only')::boolean,false))",
                    (metadata["paid_budget_group"], "matrix:" + root.name + ":" + case["id"]),
                ).fetchone()
                if not row:
                    continue
                decision = row["body"].get("result", {}).get("decision", {})
                records.append(
                    {
                        "case_id": case["id"],
                        "event_id": row["id"],
                        "actual_kind": decision.get("kind"),
                        "source_sha256": saved_records.get(case["id"], {}).get(
                            "original_execution_source_sha256"
                        )
                        or saved_records.get(case["id"], {}).get("execution_source_sha256")
                        or metadata["source_sha256"],
                        "status": "PASS"
                        if decision.get("kind") == "start_work" and row["outbox_at"]
                        else "FAIL",
                        "db_recorded_seconds": (
                            row["outbox_at"] - row["received_at"]
                        ).total_seconds()
                        if row["outbox_at"]
                        else None,
                    }
                )
        values = [
            row["db_recorded_seconds"] for row in records if row["db_recorded_seconds"] is not None
        ]
        versions = sorted({row["source_sha256"] for row in records})
        reports.append(
            {
                "run_id": root.name,
                "source_sha256": metadata["source_sha256"],
                "config_sha256": canonical_hash(config),
                "records": records,
                "samples": len(records),
                "db_clock_p95_seconds": p95(values) if len(versions) <= 1 else None,
                "source_versions": versions,
                "by_source": [
                    {
                        "source_sha256": version,
                        "samples": sum(row["source_sha256"] == version for row in records),
                        "db_clock_p95_seconds": p95(
                            [
                                row["db_recorded_seconds"]
                                for row in records
                                if row["source_sha256"] == version
                                and row["db_recorded_seconds"] is not None
                            ]
                        ),
                    }
                    for version in versions
                ],
                "missing_to_30": max(0, 30 - len(records)),
                "qualification": "SUPPORTING_ONLY",
                "limitations": [
                    "No historical commit/monotonic brackets were recorded.",
                    "Matrix used app.step(batched=False); do not call this the daemon batching SLO.",
                ],
            }
        )
    return {
        "runs": reports,
        "pooled_single_build_slo": "NOT_RUN: distinct source/config strata are not silently pooled",
    }


async def _main_body(args, raw, root, manifest):
    if args.mode == "existing":
        result = historical(PostgresStore(args.dsn), args.matrix_run)
    else:
        if raw.get("runtime", {}).get("channel") != "local":
            raise ValueError("Performance probes are local channel only")
        config = AppConfig.model_validate(raw)
        if args.mode == "probe":
            from live_preflight import setup

            from groupagent.runtime.app import build_app

            store, ledger, actor, _key = setup()
            if actor.group_id != config.group.internal_id:
                raise ValueError("Use the existing shared budget group")
            ensure_idle(store, actor)
            app = build_app(config, store, root / "app", ROOT / ".env.local")
            app.supervisor = HeldExecutor()
            manifest["shared_budget_before"] = ledger.report(actor)
        else:
            store = PostgresStore(args.dsn)
            actor = ActorContext(
                group_id=config.group.internal_id,
                member_id=config.group.owner_ids[0],
                role=Role.OWNER,
                policy_revision=config.group.policy_revision,
            )
            ensure_idle(store, actor)
            app = deterministic_app(store, config, root)
        save(root / "manifest.json", manifest)
        records = await sample_app(
            app,
            actor,
            root,
            mode=args.mode,
            short_count=args.short_count if args.mode == "probe" else 0,
            long_count=args.long_count if args.mode == "probe" else 0,
            cancel_count=args.samples if args.mode == "cancel" else 0,
            timeout=args.timeout,
            poll_seconds=args.poll_ms / 1000,
        )
        result = {"measurements": summarize(records), "records": records}
        if args.mode == "probe":
            result["shared_budget_after"] = ledger.report(actor)
    manifest.update(
        state="completed",
        ended_at=datetime.now(UTC).isoformat(),
        source_sha256_at_end=source_hash(),
    )
    manifest["source_changed_during_run"] = (
        manifest["source_sha256"] != manifest["source_sha256_at_end"]
    )
    save(root / "manifest.json", manifest)
    save(root / "result.json", {**result, "manifest": str(root / "manifest.json")})
    print(
        json.dumps(
            {"run": str(root), **{key: value for key, value in result.items() if key != "records"}},
            ensure_ascii=False,
            default=str,
        )
    )


async def main(args):
    raw = json.loads(args.config_from.read_text()) if args.config_from else {}
    if args.mode != "existing":
        if raw.get("runtime", {}).get("channel") != "local":
            raise ValueError("Performance probes are local only")
        raw["runtime"]["channel_account_id"] = "performance:" + args.run_id
        raw = AppConfig.model_validate(raw).model_dump(mode="json")
    root, manifest = create_run(args.run_id, raw, args.mode)
    manifest["sample_plan"] = {
        "short_count": args.short_count if args.mode == "probe" else 0,
        "long_count": args.long_count if args.mode == "probe" else 0,
        "cancel_count": args.samples if args.mode == "cancel" else 0,
        "timeout_seconds": args.timeout,
        "observation_poll_ms": args.poll_ms,
        "short_questions_sha256": canonical_hash(QUESTIONS),
    }
    save(root / "manifest.json", manifest)
    try:
        await _main_body(args, raw, root, manifest)
    except BaseException as error:
        manifest.update(
            ended_at=datetime.now(UTC).isoformat(),
            state="aborted",
            error_type=type(error).__name__,
            error_code=getattr(error, "code", None),
            source_sha256_at_end=source_hash(),
        )
        save(root / "manifest.json", manifest)
        saved = (
            json.loads((root / "samples.json").read_text())
            if (root / "samples.json").exists()
            else {"records": []}
        )
        save(
            root / "result.json",
            {
                **saved,
                "measurements": summarize(saved["records"]),
                "status": "ABORTED",
                "manifest": str(root / "manifest.json"),
            },
        )
        raise


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=["existing", "probe", "cancel"], required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--config-from", type=Path)
    parser.add_argument("--dsn")
    parser.add_argument("--matrix-run", type=Path, action="append", default=[])
    parser.add_argument("--short-count", type=int, default=30)
    parser.add_argument("--long-count", type=int, default=0)
    parser.add_argument("--samples", type=int, default=30)
    parser.add_argument("--timeout", type=float, default=60)
    parser.add_argument("--poll-ms", type=float, default=20)
    args = parser.parse_args()
    if args.mode in {"existing", "cancel"} and not args.dsn:
        parser.error("--dsn is required without reading credentials")
    if args.mode in {"probe", "cancel"} and not args.config_from:
        parser.error("--config-from is required")
    if args.mode == "existing" and not args.matrix_run:
        parser.error("--matrix-run is required")
    if (
        any(not 0 <= value <= 200 for value in (args.short_count, args.long_count, args.samples))
        or args.timeout < 20
        or not 1 <= args.poll_ms <= 1000
    ):
        parser.error("Invalid bounded sampling settings")
    asyncio.run(main(args))
