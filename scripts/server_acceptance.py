"""Root-operated server acceptance; imports and `plan` perform no external calls.

worker: real Linux/Docker, real host admission, synthetic files, no model calls.
        Run before enabling the daemon's heavy lane; governors are per process.
state: already-migrated EMPTY synthetic PG database and an EMPTY restore target;
       exercises storage/service APIs, not model semantics or a daemon restart.
live: read-only observation of a running test_2 daemon; sends/calls no providers.
      Mobile viewing, human audio meaning and actual restart need operator proof.

Every invocation writes a new private phase directory. Failures contain stable
codes, never exception text, DSNs, credentials, chat bodies or member identifiers.
No phase migrates schema, changes existing services, or creates budget policy.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import io
import json
import math
import os
import platform
import re
import struct
import time
import wave
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any
from uuid import uuid4

ROOT = Path(__file__).resolve().parents[1]
SCHEMA = "groupagent.server-acceptance.v1"
PLAN = (
    ("W01", "worker", "md_docx_pdf_pptx_generate_validate", "no_provider"),
    ("W02", "worker", "chart_image_decode_local_ocr", "no_provider"),
    ("W03", "worker", "pcm_audio_full_decode_normalize", "no_provider_not_asr_semantics"),
    ("W04", "worker", "offline_browser_and_python_compute", "no_provider"),
    ("D01", "state", "queue_cancel_task_memory_schedule_reconnect_restore", "empty_synthetic_db"),
    ("L01", "live", "member_request_compute_markdown_native_receipts", "existing_allocated_budget"),
    ("L02", "live", "member_audio_asr", "existing_allocated_budget_and_human_audio_review"),
    ("L03", "live", "echo_manual_account_restart_no_resend", "native_receipts_and_operator_proof"),
)


class CheckFailed(Exception):
    def __init__(self, code: str):
        self.code = code


def require(condition: Any, code: str) -> None:
    if not condition:
        raise CheckFailed(code)


def canonical_hash(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(
            value, sort_keys=True, ensure_ascii=False, separators=(",", ":"), default=str
        ).encode()
    ).hexdigest()


def file_hash(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def source_hash() -> str:
    return canonical_hash(
        {
            str(path.relative_to(ROOT)): file_hash(path)
            for path in sorted((ROOT / "src").rglob("*.py"))
        }
    )


def private_file(path: Path, *, maximum: int = 2 * 1024**2) -> bytes:
    require(path.absolute() == path.resolve() and path.is_file(), "private_file_invalid")
    info = path.stat()
    require(
        info.st_uid == os.getuid() and info.st_nlink == 1 and not info.st_mode & 0o077,
        "private_file_permissions",
    )
    require(info.st_size <= maximum, "private_file_too_large")
    return path.read_bytes()


def read_object(path: Path) -> dict[str, Any]:
    value = json.loads(private_file(path))
    require(isinstance(value, dict), "metadata_not_object")
    return value


def identifier(value: str) -> str:
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}", value):
        raise argparse.ArgumentTypeError("Use 1-64 letters, numbers, underscores or hyphens")
    return value


class Report:
    def __init__(self, args):
        parent = args.output.absolute()
        require(parent == parent.resolve(), "output_path_redirected")
        parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.root = parent / args.run_id / args.command
        require(self.root.parent.absolute() == self.root.parent.resolve(), "run_path_redirected")
        self.root.mkdir(parents=True, exist_ok=False, mode=0o700)
        self.root.chmod(0o700)
        self.body: dict[str, Any] = {
            "schema_version": SCHEMA,
            "run_id": args.run_id,
            "phase": args.command,
            "source_sha256": source_hash(),
            "runner_sha256": file_hash(Path(__file__)),
            "started_at": datetime.now(UTC).isoformat(),
            "machine": {"system": platform.system(), "architecture": platform.machine()},
            "provider_calls_by_runner": 0,
            "cases": [],
        }
        self.save()

    def save(self):
        target = self.root / "report.json"
        temporary = self.root / (uuid4().hex + ".pending")
        fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            json.dump(self.body, stream, ensure_ascii=False, indent=2, default=str)
        temporary.replace(target)

    async def case(self, case_id, operation):
        started = time.monotonic()
        try:
            evidence = await operation()
            row = {"id": case_id, "status": "PASS", "code": "verified", "evidence": evidence}
        except Exception as error:
            code = getattr(error, "code", "operation_failed")
            code = code if re.fullmatch(r"[a-z0-9_]{1,80}", str(code)) else "operation_failed"
            status = (
                "NEEDS_REVIEW"
                if code
                in {
                    "human_receipt_not_attested",
                    "human_content_review_missing",
                    "restart_operator_proof_missing",
                }
                else "NOT_RUN"
                if code in {"request_event_binding_missing", "restart_baseline_missing"}
                else "FAIL"
            )
            row = {"id": case_id, "status": status, "code": code}
        row["seconds"] = round(time.monotonic() - started, 3)
        self.body["cases"].append(row)
        self.save()
        print(json.dumps(row, ensure_ascii=False), flush=True)

    def finish(self) -> int:
        self.body["ended_at"] = datetime.now(UTC).isoformat()
        rows = self.body["cases"]
        self.body["status"] = (
            "PASS"
            if rows and all(r["status"] == "PASS" for r in rows)
            else "FAIL"
            if any(r["status"] == "FAIL" for r in rows)
            else "NEEDS_REVIEW"
        )
        self.save()
        return 0 if self.body["status"] == "PASS" else 2


async def worker_phase(args, report: Report):
    from groupagent.artifacts import Deck, Section, Slide
    from groupagent.artifacts import Report as DocumentReport
    from groupagent.artifacts.worker_operations import pack_document
    from groupagent.config import AppConfig
    from groupagent.domain import ActorContext, Role
    from groupagent.media import IsolatedMediaWorker
    from groupagent.resources import ResourceGovernor, ResourcePolicy, linux_memory_snapshot
    from groupagent.runtime.resource_profile import worker_limits
    from groupagent.sandbox import DockerSandbox, ProcessSpec
    from groupagent.tools import ToolContext, ToolScope

    require(platform.system() == "Linux", "linux_host_required")
    config = AppConfig.model_validate(
        {
            "group": {
                "internal_id": "synthetic",
                "external_group_id": "synthetic",
                "owner_ids": ["owner"],
            },
            "resources": {"profile": "server_2gb"},
        }
    )
    governor = ResourceGovernor(
        ResourcePolicy(enabled=True, reserve_memory_mb=128, transfer_memory_mb=128)
    )
    measurements: list[dict[str, Any]] = []

    class MeasuredSandbox(DockerSandbox):
        async def run(self, spec, context):
            started = time.monotonic()
            while not governor.ready(self.limits.memory_mb):
                require(not governor.poisoned, "worker_cleanup_unconfirmed")
                require(
                    time.monotonic() - started < args.capacity_timeout, "resource_capacity_timeout"
                )
                await asyncio.sleep(5)
            result = await super().run(spec, context)
            measurements.append(
                {"seconds": round(result.elapsed_seconds, 3), **result.resource_usage}
            )
            return result

    sandbox = MeasuredSandbox(
        args.worker_image,
        docker_host=args.docker_host,
        limits=worker_limits(config),
        resource_governor=governor,
    )
    image = await sandbox.preflight()
    expected_arch = {"x86_64": "amd64", "aarch64": "arm64"}.get(platform.machine())
    require(
        expected_arch is not None and image["architecture"] == expected_arch,
        "worker_architecture_mismatch",
    )
    workspace = report.root / "workspace"
    workspace.mkdir(mode=0o700)
    actor = ActorContext(group_id="synthetic", member_id="owner", role=Role.OWNER)
    context = ToolContext(
        ToolScope(
            "synthetic", "server-acceptance", actor, workspace.resolve(), frozenset(), "synthetic"
        ),
        "server-acceptance",
        "server-acceptance",
        asyncio.Event(),
    )
    worker = IsolatedMediaWorker(sandbox)
    report.body.update(
        {
            "worker_image_sha256": canonical_hash(image["image_id"]),
            "worker_limits": sandbox.limits.model_dump(),
            "admission_sampler": "actual_linux_memavailable_and_cgroup",
            "host_memory_readable": linux_memory_snapshot().readable,
        }
    )
    report.save()

    async def documents():
        files = []
        for kind in ("md", "docx", "pdf", "pptx"):
            document = (
                Deck("Synthetic acceptance", (Slide("Result", ("Synthetic score 87",)),))
                if kind == "pptx"
                else DocumentReport(
                    "Synthetic acceptance", (Section("Result", ("Synthetic score 87",)),)
                )
            )
            packed, inputs = pack_document(document)
            _, generated = await worker.run(
                context,
                "document_generate",
                inputs,
                parameters={"format": kind, "document": packed},
                outputs=("result." + kind,),
            )
            checked, previews = await worker.run(
                context,
                "document_validate",
                generated,
                parameters={
                    "format": kind,
                    "render": kind != "md",
                    "expected_text": ["Synthetic score 87"],
                },
                outputs=("preview.png",) if kind != "md" else (),
            )
            require(
                checked["passed"] and (kind == "md" or previews[0].startswith(b"\x89PNG")),
                "document_validation_failed",
            )
            files.append(
                {
                    "format": kind,
                    "sha256": hashlib.sha256(generated[0]).hexdigest(),
                    "bytes": len(generated[0]),
                }
            )
            (report.root / ("synthetic." + kind)).write_bytes(generated[0])
        return {"artifacts": files, "model_semantics": "not_tested"}

    async def image_ocr():
        chart = {
            "title": "Synthetic score 87",
            "kind": "bar",
            "categories": ["SCORE"],
            "series": [{"name": "RESULT", "values": [87]}],
        }
        _, charts = await worker.run(
            context, "chart_render", (), parameters={"chart": chart}, outputs=("chart.png",)
        )
        metadata, _ = await worker.run(
            context,
            "image_validate",
            charts,
            parameters={"max_bytes": 8 * 1024**2, "max_pixels": 12_000_000},
            outputs=("view.png",),
        )
        ocr, _ = await worker.run(
            context,
            "ocr_image",
            charts,
            parameters={"languages": "eng", "max_bytes": 8 * 1024**2, "max_pixels": 12_000_000},
        )
        text = "\n".join(page["text"] for page in ocr["pages"])
        require(
            "87" in text and metadata["width"] * metadata["height"] <= 12_000_000,
            "ocr_fixture_mismatch",
        )
        (report.root / "synthetic-chart.png").write_bytes(charts[0])
        return {
            "chart_sha256": hashlib.sha256(charts[0]).hexdigest(),
            "ocr_text_sha256": canonical_hash(text),
            "decoded_pixels": metadata["width"] * metadata["height"],
        }

    async def audio():
        output = io.BytesIO()
        with wave.open(output, "wb") as stream:
            stream.setnchannels(1)
            stream.setsampwidth(2)
            stream.setframerate(16000)
            stream.writeframes(
                b"".join(
                    struct.pack("<h", int(8000 * math.sin(2 * math.pi * 440 * i / 16000)))
                    for i in range(8000)
                )
            )
        metadata, files = await worker.run(
            context,
            "audio_validate",
            (output.getvalue(),),
            outputs=("normalized.wav", *(f"chunk-{i}.wav" for i in range(6))),
        )
        require(
            0.49 <= metadata["duration_seconds"] <= 0.51 and files[0].startswith(b"RIFF"),
            "audio_fixture_mismatch",
        )
        return {
            "normalized_sha256": hashlib.sha256(files[0]).hexdigest(),
            "duration_seconds": metadata["duration_seconds"],
            "asr_semantics": "NOT_RUN",
        }

    async def browser_compute():
        metadata, files = await worker.run(
            context,
            "browser_snapshot",
            (b"<html><title>Synthetic acceptance</title><body>Synthetic score 87</body></html>",),
            outputs=("screenshot.png",),
        )
        require(
            "Synthetic score 87" in metadata["text"] and files[0].startswith(b"\x89PNG"),
            "browser_fixture_mismatch",
        )
        result = await sandbox.run(
            ProcessSpec(
                program="python3", args=("-I", "-c", "print(sum(i*i for i in range(1,101)))")
            ),
            context,
        )
        require(result.succeeded and result.stdout.strip() == "338350", "compute_fixture_mismatch")
        return {
            "screenshot_sha256": hashlib.sha256(files[0]).hexdigest(),
            "compute_stdout_sha256": canonical_hash(result.stdout),
        }

    for case_id, operation in (
        ("W01", documents),
        ("W02", image_ocr),
        ("W03", audio),
        ("W04", browser_compute),
    ):
        await report.case(case_id, operation)
    report.body["worker_measurements"] = measurements
    report.body["worker_cleanup_confirmed"] = not governor.busy and not governor.poisoned
    require(report.body["worker_cleanup_confirmed"], "worker_cleanup_unconfirmed")


async def state_phase(args, report: Report):
    from groupagent.domain import ActorContext, DomainError, EventKind, GroupEvent, Role, WorkState
    from groupagent.memory import MemoryService
    from groupagent.ops import BackupManager, RestorationService
    from groupagent.persistence import PostgresStore
    from groupagent.runtime.state import RuntimeState
    from groupagent.scheduling import Scheduler
    from groupagent.scheduling.models import AtTiming

    dsn = private_file(args.dsn_file, maximum=32768).decode().strip()
    target_dsn = private_file(args.restore_dsn_file, maximum=32768).decode().strip()
    store, target = PostgresStore(dsn), PostgresStore(target_dsn)
    with store.connect() as conn:
        identity = (conn.info.host, conn.info.port, conn.info.dbname)
        require(
            conn.execute("SELECT count(*) AS n FROM ga_groups").fetchone()["n"] == 0,
            "synthetic_database_not_empty",
        )
    with target.connect() as conn:
        require(
            identity != (conn.info.host, conn.info.port, conn.info.dbname),
            "restore_target_is_source",
        )
        require(
            conn.execute(
                "SELECT count(*) AS n FROM pg_tables WHERE schemaname NOT IN ('pg_catalog','information_schema')"
            ).fetchone()["n"]
            == 0,
            "restore_target_not_empty",
        )

    async def lifecycle():
        group = "server-acceptance-" + args.run_id
        store.register_group(group)
        actor = ActorContext(group_id=group, member_id="synthetic-owner", role=Role.OWNER)
        now, event_id = datetime.now(UTC), uuid4().hex
        event = GroupEvent(
            id=event_id,
            source_key="synthetic:" + args.run_id,
            group_id=group,
            actor_id=actor.member_id,
            kind=EventKind.TEXT,
            text="Synthetic score 87 and synthetic duty",
            occurred_at=now,
            received_at=now,
        )
        with store.transaction(actor) as tx:
            tx.ingest_event(event, "synthetic-acceptance", "1", 0)
            first = tx.create_work("Synthetic first work", event.id)
            second_event = event.model_copy(
                update={"id": uuid4().hex, "source_key": event.source_key + ":second"}
            )
            tx.ingest_event(second_event, "synthetic-acceptance", "2", 1)
            second = tx.create_work("Synthetic queued work", second_event.id)
            task = tx.create_team_task("Synthetic human duty", source_event_id=event.id)
        memory = MemoryService(store)
        fact = memory.record_fact(
            actor, "synthetic_score", 87, source_event_ids=[event.id], explicit=True
        )
        require(len(RuntimeState(store).queued(actor)) == 2, "durable_queue_mismatch")
        with store.transaction(actor) as tx:
            lease = tx.claim_work(first.id, "synthetic-acceptance-worker", ttl_seconds=30)
            current = tx.get_work(first.id)
            tx.transition_work(
                first.id,
                WorkState.CANCELLED,
                expected_revision=current.revision,
                lease=lease,
                reason="synthetic_cancel",
            )
            tx.transition_work(
                second.id,
                WorkState.CANCELLED,
                expected_revision=second.revision,
                reason="synthetic_cancel",
            )
        require(not RuntimeState(store).queued(actor), "cancelled_queue_not_empty")
        scheduler = Scheduler(store)
        due_at = datetime.now(UTC) + timedelta(seconds=1)
        schedule = scheduler.create(
            actor,
            "Synthetic once",
            "Synthetic score reminder",
            AtTiming(at=due_at),
            request_key="synthetic:" + args.run_id,
        )
        await asyncio.sleep(1.1)
        occurrences = scheduler.due(actor)
        require(len(occurrences) == 1 and not scheduler.due(actor), "schedule_not_exactly_once")
        reopened = PostgresStore(dsn)
        with reopened.transaction(actor) as tx:
            require(tx.get_work(first.id).state == WorkState.CANCELLED, "work_reconnect_mismatch")
            require(
                tx.get_team_task(task.id).owner_id == actor.member_id, "task_reconnect_mismatch"
            )
        require(
            MemoryService(reopened).get_fact(actor, fact.id).value == 87,
            "memory_reconnect_mismatch",
        )
        require(
            Scheduler(reopened).get(actor, schedule.id).id == schedule.id,
            "schedule_reconnect_mismatch",
        )
        manager = BackupManager(reopened, report.root / "backups", args.pg_bin)
        backup = manager.create(actor, build_ref=report.body["source_sha256"])
        manifest = manager.verify(actor, backup.id)
        restored = manager.restore(actor, backup.id, target_dsn)
        require(
            restored.quarantined and restored.verified_tables == len(manifest.tables),
            "restore_verification_failed",
        )
        try:
            with target.transaction(actor):
                pass
        except DomainError as error:
            require(error.code == "restore_pending", "restore_quarantine_mismatch")
        else:
            raise CheckFailed("restore_quarantine_missing")
        with RestorationService(target).maintenance(actor) as tx:
            require(tx.get_team_task(task.id).owner_id == actor.member_id, "restored_task_mismatch")
            require(tx.get_event(event.id).text == event.text, "restored_source_mismatch")
        return {
            "queue_cancel": True,
            "task_owner": True,
            "fact_memory": True,
            "schedule_exactly_once": True,
            "new_store_object_reconnect": True,
            "daemon_restart": "NOT_RUN",
            "backup_sha256": manifest.archive_sha256,
            "verified_tables": restored.verified_tables,
            "restore_quarantined": True,
            "paid_policy_created": False,
        }

    await report.case("D01", lifecycle)


def allocation_check(allocation: dict[str, Any], group_id: str, cap: Decimal) -> dict[str, Any]:
    parent = Decimal(str(allocation["parent_limit_cny"]))
    used = Decimal(str(allocation["source_upper_cny"]))
    allocated = Decimal(str(allocation["allocated_cap_cny"]))
    require(
        all(value.is_finite() for value in (parent, used, allocated)), "allocation_amount_invalid"
    )
    require(
        parent > 0 and used >= 0 and allocated > 0 and used + allocated <= parent,
        "allocation_exceeds_parent_budget",
    )
    require(
        allocated == cap and allocation.get("source_paid_entry_frozen") is True,
        "allocation_not_frozen_or_cap_mismatch",
    )
    require(
        allocation.get("target_group_sha256")
        == hashlib.sha256(group_id.encode("utf-8")).hexdigest(),
        "allocation_group_mismatch",
    )
    return {
        "parent_limit_cny": str(parent),
        "source_upper_cny": str(used),
        "allocated_cap_cny": str(allocated),
        "manifest_sha256": canonical_hash(allocation),
    }


async def live_phase(args, report: Report):
    from groupagent.billing import BudgetLedger
    from groupagent.config import load_config
    from groupagent.domain import ActorContext, Role
    from groupagent.persistence import PostgresStore

    private_file(args.config)
    config = load_config(args.config)
    require(
        config.runtime.channel == "agentwechat" and config.runtime.allow_live_send,
        "live_channel_binding_required",
    )
    require(
        config.resources.enabled and config.runtime.engine == "langgraph", "server_profile_required"
    )
    proof = read_object(args.live_evidence)
    require(proof.get("test_group") == "test_2", "test_group_attestation_missing")
    store = PostgresStore(private_file(args.dsn_file, maximum=32768).decode().strip())
    actor = ActorContext(
        group_id=config.group.internal_id,
        member_id=config.group.owner_ids[0],
        role=Role.OWNER,
        policy_revision=config.group.policy_revision,
    )
    with store.transaction(actor) as tx:
        policy = tx.conn.execute(
            "SELECT currency,daily_limit,total_limit FROM ga_budget_policy WHERE group_id=%s",
            (actor.group_id,),
        ).fetchone()
    require(
        policy
        and policy["currency"] == "CNY"
        and (
            config.spend.monetary_limits_disabled
            and policy["daily_limit"] is None
            and policy["total_limit"] is None
            or not config.spend.monetary_limits_disabled
            and policy["total_limit"] is not None
        ),
        "existing_budget_required",
    )
    if config.spend.monetary_limits_disabled:
        report.body["budget_authorization"] = "owner_configured_unlimited"
    elif args.budget_allocation_file:
        report.body["budget_allocation"] = allocation_check(
            read_object(args.budget_allocation_file), actor.group_id, policy["total_limit"]
        )
    else:
        report.body["budget_authorization"] = "root_responsibility_not_verified_by_runner"
    budget = BudgetLedger(store).report(actor)
    accounting = budget.get("accounting") or {}
    report.body["budget_snapshot"] = {
        key: accounting[key]
        for key in ("gross_total", "gross_settled_estimate", "held", "total", "daily")
        if key in accounting
    }
    report.body["group_binding_sha256"] = canonical_hash(config.group.external_group_id)
    report.body["operator_evidence_sha256"] = canonical_hash(proof)
    receipts: dict[str, list[str]] = {}

    async def observe_request(case_id):
        record = proof.get(case_id, {})
        event_id = record.get("event_id")
        require(
            isinstance(event_id, str) and 0 < len(event_id) <= 256, "request_event_binding_missing"
        )
        with store.transaction(actor) as tx:
            event = tx.get_event(event_id)
            source_id = event_id
            if case_id == "L02":
                rows = tx.conn.execute(
                    "SELECT body FROM ga_media_inputs WHERE group_id=%s AND event_id=%s AND available",
                    (actor.group_id, event_id),
                ).fetchall()
                completed = [row["body"] for row in rows if row["body"].get("derived_event_id")]
                require(len(completed) == 1, "audio_transcript_not_complete")
                source_id = completed[0]["derived_event_id"]
                require(
                    completed[0].get("transcript", {}).get("budget_receipts"),
                    "audio_paid_receipt_missing",
                )
            decision = tx.get_decision(source_id)
            require(decision is not None, "request_not_processed")
            works = tx.conn.execute(
                "SELECT id,state FROM ga_work_orders WHERE group_id=%s AND request_event_id=%s",
                (actor.group_id, source_id),
            ).fetchall()
            deliveries = tx.conn.execute(
                "SELECT id,state,payload_ref FROM ga_deliveries WHERE group_id=%s AND request_event_id=%s",
                (actor.group_id, source_id),
            ).fetchall()
            parts = tx.conn.execute(
                "SELECT p.platform_message_id,p.payload_hash,p.state FROM ga_delivery_parts p JOIN ga_deliveries d ON d.group_id=p.group_id AND d.id=p.intent_id WHERE d.group_id=%s AND d.request_event_id=%s ORDER BY p.intent_id,p.position",
                (actor.group_id, source_id),
            ).fetchall()
            require(
                deliveries
                and parts
                and all(row["state"] == "accepted" and row["platform_message_id"] for row in parts)
                and all(row["state"] == "accepted" for row in deliveries),
                "native_delivery_not_accepted",
            )
            if case_id == "L01":
                require(len(works) == 1 and works[0]["state"] == "completed", "work_not_completed")
                artifacts = tx.conn.execute(
                    "SELECT format,sha256 FROM ga_artifact_catalog WHERE group_id=%s AND work_id=%s AND shared",
                    (actor.group_id, works[0]["id"]),
                ).fetchall()
                require(
                    any(row["format"] == "md" for row in artifacts), "markdown_artifact_missing"
                )
                trace = tx.conn.execute(
                    "SELECT body FROM ga_runtime_events WHERE group_id=%s AND work_id=%s AND kind='tool_finished'",
                    (actor.group_id, works[0]["id"]),
                ).fetchall()
                calls = [row["body"].get("payload", {}).get("receipt", {}) for row in trace]
                require(
                    any(
                        call.get("tool_name") == "python_compute"
                        and call.get("success")
                        and "338350" in str(call.get("output", {}).get("stdout", ""))
                        for call in calls
                    ),
                    "actual_compute_receipt_missing",
                )
        receipts[case_id] = sorted(canonical_hash(row["platform_message_id"]) for row in parts)
        require(record.get("human_received") is True, "human_receipt_not_attested")
        require(
            record.get(
                "mobile_file_opened" if case_id == "L01" else "human_audio_semantics_verified"
            )
            is True,
            "human_content_review_missing",
        )
        return {
            "event_sha256": canonical_hash(event.id),
            "native_receipt_sha256": receipts[case_id],
            "human_attestation": True,
            "body_exported": False,
        }

    await report.case("L01", lambda: observe_request("L01"))
    await report.case("L02", lambda: observe_request("L02"))
    with store.transaction(actor) as tx:
        report.body["runtime_start_count"] = tx.conn.execute(
            "SELECT count(*) AS n FROM ga_audit_events WHERE group_id=%s AND action='runtime_started'",
            (actor.group_id,),
        ).fetchone()["n"]
    report.body["request_receipts"] = receipts
    report.save()

    async def echo_restart():
        record = proof.get("L03", {})
        with store.transaction(actor) as tx:
            echo = tx.get_event(record.get("echo_event_id", "missing"))
            manual = tx.get_event(record.get("manual_event_id", "missing"))
            echo_decision, manual_decision = tx.get_decision(echo.id), tx.get_decision(manual.id)
            matched = tx.conn.execute(
                "SELECT count(*) AS n FROM ga_delivery_parts WHERE group_id=%s AND state='accepted' AND platform_message_id=%s",
                (actor.group_id, echo.platform_message_id),
            ).fetchone()["n"]
            require(
                matched and echo.actor_id == manual.actor_id,
                "echo_receipt_or_shared_actor_mismatch",
            )
            echo_kind = echo_decision["result"]["decision"]["kind"] if echo_decision else None
            manual_kind = manual_decision["result"]["decision"]["kind"] if manual_decision else None
            require(
                echo_kind in {"observe", "ignore"} and manual_kind in {"reply", "start_work"},
                "echo_or_manual_routing_mismatch",
            )
        starts = report.body["runtime_start_count"]
        require(args.before_report is not None, "restart_baseline_missing")
        before = read_object(args.before_report)
        require(
            before.get("group_binding_sha256") == report.body["group_binding_sha256"],
            "restart_group_changed",
        )
        require(starts > before.get("runtime_start_count", starts), "daemon_restart_not_observed")
        require(receipts and receipts == before.get("request_receipts"), "restart_receipts_changed")
        require(record.get("operator_restart_observed") is True, "restart_operator_proof_missing")
        return {
            "echo_observed": True,
            "manual_same_account_processed": True,
            "restart_no_resend": True,
            "before_report_sha256": canonical_hash(before),
        }

    await report.case("L03", echo_restart)


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    sub = result.add_subparsers(dest="command", required=True)
    for command in ("plan", "worker", "state", "live"):
        selected = sub.add_parser(command)
        selected.add_argument("--run-id", required=True, type=identifier)
        selected.add_argument("--output", type=Path, default=ROOT / "runtime/server-acceptance")
        if command == "worker":
            selected.add_argument("--worker-image", required=True)
            selected.add_argument("--docker-host")
            selected.add_argument(
                "--capacity-timeout", type=int, choices=range(5, 601), default=120
            )
        if command in {"state", "live"}:
            selected.add_argument("--dsn-file", required=True, type=Path)
        if command == "state":
            selected.add_argument("--restore-dsn-file", required=True, type=Path)
            selected.add_argument("--pg-bin", required=True, type=Path)
        if command == "live":
            selected.add_argument("--config", required=True, type=Path)
            selected.add_argument("--live-evidence", required=True, type=Path)
            selected.add_argument("--budget-allocation-file", type=Path)
            selected.add_argument("--before-report", type=Path)
    return result


async def run(args) -> int:
    report = Report(args)
    try:
        if args.command == "plan":
            report.body["matrix"] = [
                {"id": row[0], "phase": row[1], "coverage": row[2], "condition": row[3]}
                for row in PLAN
            ]
            report.body["cases"] = [
                {"id": "PLAN", "status": "PASS", "code": "plan_only_no_external_calls"}
            ]
        else:
            await {"worker": worker_phase, "state": state_phase, "live": live_phase}[args.command](
                args, report
            )
    except Exception as error:
        code = getattr(error, "code", "phase_failed")
        report.body["cases"].append(
            {
                "id": "PHASE",
                "status": "FAIL",
                "code": code if re.fullmatch(r"[a-z0-9_]{1,80}", str(code)) else "phase_failed",
            }
        )
    return report.finish()


def main(argv=None) -> int:
    args = parser().parse_args(argv)
    try:
        return asyncio.run(run(args))
    except Exception as error:
        code = getattr(error, "code", "runner_initialization_failed")
        print(
            json.dumps(
                {
                    "status": "FAIL",
                    "code": code
                    if re.fullmatch(r"[a-z0-9_]{1,80}", str(code))
                    else "runner_initialization_failed",
                }
            )
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
