"""Operator-only representative execution after real deterministic admission.

Three independent synthetic requests, separate from the frozen 30-task matrix.
No held executor, provider replacement, expected-answer prompt, or budget reset.
Imports and the default dry inspection never bootstrap credentials or Docker.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import re
import time
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

from live_preflight import GROUP, OWNER, ROOT, setup
from live_runtime_matrix import canonical_hash, save, source_hash
from local_performance_acceptance import ensure_idle
from runtime_acceptance_checks import decode_artifact

from groupagent.config import AppConfig
from groupagent.conversation.actions import OutputRequirement
from groupagent.conversation.intent import new_work_intent
from groupagent.domain import EventKind, GroupEvent
from groupagent.runtime.app import build_app
from groupagent.tools import ToolContext, ToolScope

# Only these verbatim user requests enter LocalChannel / the application.
INPUTS = (
    {"id": "FAST-MD", "text": "请制作一份介绍JSON数据结构的Markdown，标题为“快速受理闭环”。"},
    {"id": "FAST-PPTX", "text": "请制作3页介绍JSON数据结构的PPT，标题为“快速受理分页验证”。"},
    {"id": "FAST-COMPUTE", "text": "请计算17*23。"},
)
TERMINAL = {"completed", "failed", "paused", "cancelled", "waiting_input"}


def input_manifest():
    return {
        "kind": "verbatim_synthetic_user_requests_no_checker_gold",
        "cases": list(INPUTS),
        "inputs_sha256": canonical_hash(INPUTS),
        "separate_from_runtime_matrix_30": True,
    }


def configuration(args):
    return AppConfig.model_validate(
        {
            "deployment_mode": "test",
            "group": {
                "enabled": True,
                "internal_id": GROUP,
                "external_group_id": "synthetic-local@chatroom",
                "owner_ids": [OWNER],
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
                "channel_account_id": "fast-execution:" + args.run_id,
                "engine": "harness",
                "worker_image": args.worker_image,
                "harness_image": args.harness_image,
                "docker_host": args.docker_host,
                "local_member_names": {OWNER: "合成测试同学"},
            },
        }
    )


def create_run(args, config):
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,79}", args.run_id) or ".." in args.run_id:
        raise ValueError("Invalid independent run ID")
    root = ROOT / "runtime/live" / args.run_id
    if root.resolve() != root.absolute():
        raise ValueError("Run path cannot be redirected")
    root.mkdir(parents=True, exist_ok=False, mode=0o700)
    files = {
        str(p.relative_to(ROOT)): hashlib.sha256(p.read_bytes()).hexdigest()
        for p in sorted((ROOT / "src").rglob("*.py"))
    }
    manifest = {
        "kind": "real_fast_admission_representative_execution",
        "run_id": args.run_id,
        "started_at": datetime.now(UTC).isoformat(),
        "source_sha256": source_hash(),
        "source_files": files,
        "config_sha256": canonical_hash(config.model_dump(mode="json")),
        "worker_image": args.worker_image,
        "harness_image": args.harness_image,
        "runner_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "decoder_sha256": hashlib.sha256(
            Path(decode_artifact.__code__.co_filename).read_bytes()
        ).hexdigest(),
        "inputs_sha256": input_manifest()["inputs_sha256"],
        "paid_budget_group": GROUP,
        "total_cny_limit": 50,
        "channel": "local_only",
        "live_wechat": False,
        "executor_lane": "normal build_app Harness supervisor, not held or replaced",
        "denominator": 3,
        "separate_from_runtime_matrix_30": True,
        "costs_are_conservative_ledger_estimates_not_provider_cash_debits": True,
    }
    save(root / "manifest.json", manifest)
    save(root / "config.json", config.model_dump(mode="json"))
    save(root / "input-manifest.json", input_manifest())
    return root, manifest


def admission_snapshot(store, actor, event):
    """Read actual DB state before the normal executor is started."""
    with store.transaction(actor) as tx:
        source = tx.get_event(event.id)
        decision = tx.get_decision(event.id)
        works = tx.conn.execute(
            "SELECT id,goal,request_event_id,requester_id,scope,required_checks,state "
            "FROM ga_work_orders WHERE group_id=%s AND request_event_id=%s",
            (actor.group_id, event.id),
        ).fetchall()
        acks = tx.conn.execute(
            "SELECT id,request_event_id,payload_ref FROM ga_deliveries "
            "WHERE group_id=%s AND request_event_id=%s AND work_id IS NULL",
            (actor.group_id, event.id),
        ).fetchall()
        actual_fingerprint = tx.event_fingerprint(event.id)
    work = dict(works[0]) if len(works) == 1 else {}
    scope = work.get("scope", {})
    candidate = (decision or {}).get("result", {})
    intent = new_work_intent(event.text)
    declared_outputs = list(intent.outputs) if intent else []
    checks = {
        "one_work": len(works) == 1,
        "one_ack": len(acks) == 1 and candidate.get("delivery_id") == acks[0]["id"],
        "actual_source": source.id == event.id
        and source.source_key == event.source_key
        and source.text == event.text
        and source.actor_id == actor.member_id
        and source.group_id == actor.group_id
        and source.content_state == "active",
        "source_fingerprint": scope.get("source_fingerprints", {}).get(event.id)
        == actual_fingerprint,
        "original_goal": work.get("goal") == event.text,
        "original_requester": work.get("requester_id") == actor.member_id,
        "original_source_id": work.get("request_event_id") == event.id,
        "scope": scope.get("allowed_group_ids") == [actor.group_id]
        and scope.get("delivery_target") == "origin_group",
        "deterministic_admission": scope.get("admission_path") == "deterministic_current_request",
        "candidate": candidate.get("work_id") == work.get("id")
        and candidate.get("decision", {}).get("kind") == "start_work"
        and candidate["decision"].get("reason_code") == "explicit_new_work_admission",
        "complete_output_constraints": bool(intent)
        and output_constraints_equal(scope.get("outputs"), declared_outputs),
    }
    return {
        "valid": all(checks.values()),
        "checks": checks,
        "source": source.model_dump(mode="json"),
        "source_fingerprint": actual_fingerprint,
        "work_id": work.get("id"),
        "ack_id": acks[0]["id"] if len(acks) == 1 else None,
        "actual_goal": work.get("goal"),
        "declared_outputs": scope.get("outputs"),
        "required_checks": work.get("required_checks"),
        "admission_path": scope.get("admission_path"),
        "decision": candidate.get("decision"),
    }


def output_constraints_equal(actual, expected):
    """Compare schema semantics, retaining format, exact pages and literal titles."""
    try:
        return [
            OutputRequirement.model_validate(item).model_dump(mode="json") for item in actual
        ] == [OutputRequirement.model_validate(item).model_dump(mode="json") for item in expected]
    except (ValueError, TypeError):
        return False


def work_state(store, actor, work_id):
    with store.transaction(actor) as tx:
        work = tx.get_work(work_id)
    return work


def harness_binding(backend):
    sessions = backend.get("sessions")
    encoded = sessions.encode() if isinstance(sessions, str) else b""
    return {
        **{key: backend.get(key) for key in ("version", "group_id", "work_id")},
        "session_checkpoint_sha256": hashlib.sha256(encoded).hexdigest() if encoded else None,
        "session_checkpoint_encoded_bytes": len(encoded),
    }


async def collect(app, actor, event, admission, root, case_id):
    work_id = admission.get("work_id")
    if not work_id:
        return {"scope": {"group_id": actor.group_id, "event_id": event.id}, "artifacts": []}
    work = work_state(app.store, actor, work_id)
    with app.store.transaction(actor) as tx:
        catalog = tx.conn.execute(
            "SELECT version_id,validation FROM ga_artifact_catalog "
            "WHERE group_id=%s AND work_id=%s AND shared ORDER BY created_at,version_id",
            (actor.group_id, work_id),
        ).fetchall()
        deliveries = tx.conn.execute(
            "SELECT * FROM ga_deliveries WHERE group_id=%s AND (work_id=%s OR id=%s) ORDER BY created_at,id",
            (actor.group_id, work_id, admission.get("ack_id")),
        ).fetchall()
        parts = tx.conn.execute(
            "SELECT p.* FROM ga_delivery_parts p JOIN ga_deliveries d "
            "ON d.group_id=p.group_id AND d.id=p.intent_id "
            "WHERE d.group_id=%s AND (d.work_id=%s OR d.id=%s) ORDER BY d.created_at,p.position",
            (actor.group_id, work_id, admission.get("ack_id")),
        ).fetchall()
        billing = tx.conn.execute(
            "SELECT id,group_id,work_id,operation,state,actual_units,actual_amount,evidence_ref "
            "FROM ga_budget_reservations WHERE group_id=%s AND work_id=%s ORDER BY created_at,id",
            (actor.group_id, work_id),
        ).fetchall()
    artifacts = []
    workspace = root / "verification" / case_id
    workspace.mkdir(parents=True, exist_ok=True, mode=0o700)
    context = ToolContext(
        ToolScope(actor.group_id, "verify:" + work_id, actor, workspace, frozenset(), case_id),
        "acceptance:" + case_id,
        "acceptance:" + root.name + ":" + case_id,
        asyncio.Event(),
    )
    for row in catalog:
        version, content = app.inputs.registry.read(
            actor, row["version_id"], require_validated=True
        )
        if version.work_id != work_id or version.group_id != actor.group_id:
            raise RuntimeError("Collected artifact belongs to another work")
        if (
            hashlib.sha256(content).hexdigest() != version.sha256
            or len(content) != version.size_bytes
        ):
            raise RuntimeError("Collected immutable bytes differ")
        target = root / "outputs" / case_id / (version.version_id + "." + version.format)
        target.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        with target.open("xb") as output:
            output.write(content)
        try:
            decoded = await decode_artifact(app.inputs.worker, context, version.format, content)
        except Exception as error:
            decoded = {
                "decode_error": {"type": type(error).__name__, "code": getattr(error, "code", None)}
            }
        artifacts.append(
            {
                **asdict(version),
                "validation": row["validation"],
                "decoded": decoded,
                "content_path": str(target),
            }
        )
    checkpoint = app.state.load_checkpoint(actor, work_id)
    receipts = (
        [
            {
                "group_id": actor.group_id,
                "work_id": work_id,
                "receipt": receipt.model_dump(mode="json"),
            }
            for receipt in checkpoint.receipts
        ]
        if checkpoint
        else []
    )
    local = []
    for part in parts:
        receipt = await app.channel.lookup_delivery(actor.group_id, part["idempotency_key"])
        path = (
            root
            / "app/channel/outgoing"
            / (hashlib.sha256(part["idempotency_key"].encode()).hexdigest() + ".json")
        )
        envelope = json.loads(path.read_text()) if path.is_file() and not path.is_symlink() else {}
        payload = path.with_suffix(".payload")
        local.append(
            {
                "intent_id": part["intent_id"],
                "part": dict(part),
                "receipt": receipt.model_dump(mode="json") if receipt else None,
                "spool_record": envelope,
                "spool_record_sha256": hashlib.sha256(path.read_bytes()).hexdigest()
                if envelope
                else None,
                "delivered_bytes_sha256": hashlib.sha256(payload.read_bytes()).hexdigest()
                if payload.is_file()
                else None,
            }
        )
    return {
        "scope": {"group_id": actor.group_id, "work_id": work_id, "event_id": event.id},
        "work_state": work.state.value,
        "work_updated_at": work.updated_at.isoformat(),
        "artifacts": artifacts,
        "tool_receipts": receipts,
        "deliveries": deliveries,
        "local_receipts": local,
        "billing": billing,
        "harness": harness_binding(checkpoint.backend_state) if checkpoint else {},
        "checkpoint_state": checkpoint.state.value if checkpoint else None,
        "checkpoint_usage": checkpoint.usage.model_dump(mode="json") if checkpoint else None,
    }


def assess(case_id, admission, evidence):
    """Private checker values are never passed to build_app, channel, or Harness."""
    scope = evidence.get("scope", {})
    local, deliveries = evidence.get("local_receipts", []), evidence.get("deliveries", [])
    harness = evidence.get("harness", {})
    accepted = {str(item["id"]) for item in deliveries if item.get("state") == "accepted"}
    checks = {
        "admission": admission.get("valid") is True
        and bool(admission.get("checks"))
        and all(admission["checks"].values())
        and admission.get("work_id") == scope.get("work_id")
        and admission.get("source", {}).get("id") == scope.get("event_id")
        and admission.get("source", {}).get("group_id") == scope.get("group_id"),
        "completed": evidence.get("work_state") == "completed",
        "harness_execution": harness.get("version") == "0.2.2rc2"
        and bool(re.fullmatch(r"[a-f0-9]{64}", str(harness.get("session_checkpoint_sha256", ""))))
        and harness.get("session_checkpoint_encoded_bytes", 0) > 0
        and harness.get("group_id") == scope.get("group_id")
        and harness.get("work_id") == scope.get("work_id"),
        "metered_execution": any(
            row.get("group_id") == scope.get("group_id")
            and row.get("work_id") == scope.get("work_id")
            and row.get("operation") == "text"
            and row.get("state") == "settled"
            and bool(row.get("actual_units"))
            and row.get("evidence_ref")
            for row in evidence.get("billing", [])
        ),
        "local_accepted": bool(local)
        and bool(deliveries)
        and len(accepted) == len(deliveries)
        and all(
            item.get("part", {}).get("state") == "accepted"
            and (item.get("receipt") or {}).get("state") == "accepted"
            and item.get("spool_record", {}).get("transport") == "local-only"
            and item["receipt"].get("group_id") == scope.get("group_id")
            and item["receipt"].get("idempotency_key") == item["part"].get("idempotency_key")
            and item["receipt"].get("platform_message_id")
            == item["part"].get("platform_message_id")
            and bool(item["receipt"].get("platform_message_id"))
            and item.get("intent_id") in accepted
            and item.get("spool_record", {}).get("receipt") == item["receipt"]
            for item in local
        ),
        "ack_accepted": admission.get("ack_id") in accepted
        and any(item.get("intent_id") == admission.get("ack_id") for item in local),
    }
    if case_id == "FAST-COMPUTE":
        success = [
            item["receipt"]
            for item in evidence.get("tool_receipts", [])
            if item.get("group_id") == scope.get("group_id")
            and item.get("work_id") == scope.get("work_id")
            and item.get("receipt", {}).get("success") is True
            and item["receipt"].get("tool_name") == "python_compute"
        ]
        checks["actual_calculation"] = any(
            item.get("output", {}).get("exit_code") == 0
            and re.search(r"(?<![\d.])391(?![\d.])", item["output"].get("stdout", ""))
            for item in success
        )
        checks["delivered_calculation"] = any(
            item.get("work_id") == scope.get("work_id")
            and item.get("state") == "accepted"
            and re.search(r"(?<![\d.])391(?![\d.])", item.get("payload_ref", ""))
            for item in deliveries
        )
    else:
        fmt, title = (
            ("md", "快速受理闭环") if case_id == "FAST-MD" else ("pptx", "快速受理分页验证")
        )
        selected = [a for a in evidence.get("artifacts", []) if a.get("format") == fmt]
        checks["actual_validated_file"] = bool(selected) and all(
            a.get("group_id") == scope.get("group_id")
            and a.get("work_id") == scope.get("work_id")
            and a.get("sha256") == a.get("decoded", {}).get("source_sha256")
            and a.get("validation", {}).get("passed") is True
            and a["validation"].get("sha256") == a.get("sha256")
            and title in a.get("decoded", {}).get("text", "")
            and "JSON" in a.get("decoded", {}).get("text", "")
            and (fmt != "pptx" or a["decoded"].get("unit_count") == 3)
            for a in selected
        )
        checks["actual_file_delivered"] = bool(selected) and all(
            any(
                item.get("payload_ref") == "artifact:" + a["version_id"]
                and str(item.get("id")) in accepted
                and any(
                    x.get("intent_id") == str(item["id"])
                    and x.get("delivered_bytes_sha256") == a.get("sha256")
                    for x in local
                )
                for item in deliveries
            )
            for a in selected
        )
    return {"status": "PASS" if all(checks.values()) else "FAIL", "checks": checks}


async def run(args):
    if any(new_work_intent(case["text"]) is None for case in INPUTS):
        raise RuntimeError(
            "A verbatim representative request no longer qualifies; do not rewrite it"
        )
    if not args.execute:
        print(
            json.dumps(
                {"mode": "dry_inspection_no_credentials", **input_manifest()}, ensure_ascii=False
            )
        )
        return
    config = configuration(args)
    root, manifest = create_run(args, config)
    records, app, fatal = [], None, None
    try:
        store, ledger, actor, _key = setup()
        if actor.group_id != GROUP or actor.member_id != OWNER:
            raise RuntimeError("Acceptance ledger identity differs; never reset or substitute it")
        save(root / "budget-before.json", ledger.report(actor))
        ensure_idle(store, actor)
        app = build_app(config, store, root / "app", ROOT / ".env.local")
        from groupagent.runtime.executor import WorkSupervisor
        from groupagent.sandbox import DockerSandbox

        if (
            not isinstance(app.supervisor, WorkSupervisor)
            or not isinstance(app.inputs.worker.sandbox, DockerSandbox)
            or app.inputs.worker.sandbox.image != args.worker_image
        ):
            raise RuntimeError("Representative execution requires the real supervisor and Docker")
        save(
            root / "execution-components.json",
            {
                "app": type(app).__module__ + "." + type(app).__qualname__,
                "supervisor": type(app.supervisor).__module__
                + "."
                + type(app.supervisor).__qualname__,
                "factory": app.supervisor.factory.__qualname__,
                "worker_image": app.inputs.worker.sandbox.image,
                "harness_image": config.runtime.harness_image,
                "engine": config.runtime.engine,
            },
        )
        await app.start()
        for case in INPUTS:
            record = {"id": case["id"], "status": "FAIL", "input_text": case["text"]}
            try:
                ensure_idle(store, actor)
                if source_hash() != manifest["source_sha256"]:
                    raise RuntimeError("Product source changed during this independent run")
                stamp = datetime.now(UTC)
                event = GroupEvent(
                    id=uuid4().hex,
                    source_key="fast-execution:" + args.run_id + ":" + case["id"],
                    group_id=actor.group_id,
                    actor_id=actor.member_id,
                    kind=EventKind.TEXT,
                    text=case["text"],
                    occurred_at=stamp,
                    received_at=stamp,
                )
                save(root / "inputs" / (case["id"] + ".json"), event.model_dump(mode="json"))
                app.channel.append_incoming(event)
                await app.poll_intake()
                await (
                    app.process_pending()
                )  # Real default batching; parser bypasses only clear new work.
                admission = admission_snapshot(store, actor, event)
                save(root / "admission" / (case["id"] + ".json"), admission)
                record["admission"] = admission
                if not admission["valid"]:
                    raise RuntimeError("Actual DB admission proof failed")
                deadline = time.monotonic() + args.timeout
                while time.monotonic() < deadline:
                    await app.execute_tick()  # Normal real Harness lane, never held or replaced.
                    await app.dispatcher.tick()
                    current = work_state(store, actor, admission["work_id"])
                    if current.state.value in TERMINAL:
                        break
                    await asyncio.sleep(0.2)
                evidence = await collect(app, actor, event, admission, root, case["id"])
                evidence = json.loads(json.dumps(evidence, default=str))
                save(root / "evidence" / (case["id"] + ".json"), evidence)
                record.update(assess(case["id"], admission, evidence))
                record["work_state"] = evidence["work_state"]
                record["budget_after"] = ledger.report(actor)
            except Exception as error:
                record["error"] = {
                    "type": type(error).__name__,
                    "code": getattr(error, "code", None),
                }
            records.append(record)
            save(
                root / "results.json",
                {"cases": records, "denominator": 3, "budget": ledger.report(actor)},
            )
            print(
                json.dumps(
                    {
                        "id": case["id"],
                        "status": record["status"],
                        "work_state": record.get("work_state"),
                        "error": record.get("error"),
                    }
                ),
                flush=True,
            )
    except Exception as error:
        fatal = {"type": type(error).__name__, "code": getattr(error, "code", None)}
    finally:
        if app is not None:
            try:
                await app.close()
            except Exception as error:
                fatal = {
                    "type": type(error).__name__,
                    "code": getattr(error, "code", None),
                    "phase": "close",
                }
        manifest.update(ended_at=datetime.now(UTC).isoformat(), source_sha256_at_end=source_hash())
        save(root / "completion-manifest.json", manifest)
        budget = ledger.report(actor) if "ledger" in locals() else None
        save(root / "budget-after.json", {"report": budget, "not_run": budget is None})
        recorded = {row["id"] for row in records}
        records.extend(
            {"id": case["id"], "status": "NOT_RUN"} for case in INPUTS if case["id"] not in recorded
        )
        unchanged = manifest["source_sha256_at_end"] == manifest["source_sha256"]
        final_result = {
            "cases": records,
            "denominator": 3,
            "source_unchanged": unchanged,
            "status": "PASS"
            if unchanged and not fatal and all(row["status"] == "PASS" for row in records)
            else "FAIL",
            "fatal_error": fatal,
            "budget": budget,
            "separate_from_runtime_matrix_30": True,
        }
        save(root / "results.json", final_result)
        print(
            json.dumps(
                {
                    "status": final_result["status"],
                    "denominator": 3,
                    "results_path": str(root / "results.json"),
                }
            ),
            flush=True,
        )
    return final_result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--worker-image", required=True)
    parser.add_argument("--harness-image", required=True)
    parser.add_argument("--docker-host", required=True)
    parser.add_argument("--timeout", type=int, default=360)
    parser.add_argument("--execute", action="store_true", help="Root-operated real execution only")
    result = asyncio.run(run(parser.parse_args()))
    if result is not None and result["status"] != "PASS":
        raise SystemExit(1)
