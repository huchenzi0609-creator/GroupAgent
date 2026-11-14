"""Evaluate frozen synthetic interaction labels; never apply candidate business writes."""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import re
import time
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

from live_preflight import ROOT, setup

from groupagent.billing.metering import MeteredModel
from groupagent.conversation.provider import ModelDecisionProvider
from groupagent.coordinator import CoordinationResult, Decision
from groupagent.domain import DomainError, EventKind, GroupEvent
from groupagent.execution.deepseek import DeepSeekHTTPModel

PROVIDER_MODEL = "deepseek-flash"
PROVIDER_BASE_URL = "https://api.deepseek.com"


def _canonical_hash(value):
    return hashlib.sha256(
        json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def _source_hash():
    digest = hashlib.sha256()
    for path in sorted((ROOT / "src").rglob("*.py")):
        digest.update(
            str(path.relative_to(ROOT)).encode()
            + b"\0"
            + hashlib.sha256(path.read_bytes()).digest()
        )
    return digest.hexdigest()


def _save(path: Path, value):
    if path.is_symlink() or path.parent.resolve() != path.parent.absolute():
        raise RuntimeError("Evaluation metadata cannot traverse symlinks")
    temporary = path.with_name(path.name + "." + uuid4().hex + ".pending")
    fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "w") as output:
        json.dump(value, output, ensure_ascii=False, indent=2)
        output.flush()
        os.fsync(output.fileno())
    temporary.replace(path)


def prepare_run(args, dataset):
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,79}", args.run_id) or ".." in args.run_id:
        raise RuntimeError("Evaluation run ID must be a bounded identifier")
    if type(args.limit) is not int or args.limit < 0 or args.concurrency not in (1, 2, 3):
        raise RuntimeError("Invalid evaluation bounds")
    if _canonical_hash(dataset["cases"]) != dataset.get("cases_sha256"):
        raise RuntimeError("Frozen interaction dataset checksum changed")
    destination = ROOT / "runtime/live" / args.run_id
    if destination.resolve() != destination.absolute():
        raise RuntimeError("Evaluation directory must be canonical")
    # Refuse before credentials/bootstrap: a prior partially completed run is
    # evidence, never an append destination for another evaluation.
    try:
        destination.mkdir(parents=True, exist_ok=False, mode=0o700)
    except FileExistsError:
        raise RuntimeError(
            "Existing evaluation run cannot be reused; choose a new run ID"
        ) from None
    manifest = {
        "schema_version": "groupagent.interaction-evaluation-manifest.v1",
        "run_id": args.run_id,
        "dataset_sha256": dataset["cases_sha256"],
        "dataset_snapshot_sha256": _canonical_hash(dataset),
        "source_sha256": _source_hash(),
        "runner_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "provider_config": {
            "provider": "deepseek",
            "model": PROVIDER_MODEL,
            "base_url": PROVIDER_BASE_URL,
            "pricing_snapshot_id": "deepseek-20261114-text",
            "max_schema_retries": 2,
            "max_output_tokens": 2500,
        },
        "selected_case_ids": [
            case["id"]
            for case in (dataset["cases"][: args.limit] if args.limit else dataset["cases"])
        ],
        "concurrency": args.concurrency,
        "started_at": datetime.now(UTC).isoformat(),
        "ended_at": None,
        "state": "prepared",
    }
    _save(destination / "dataset.json", dataset)
    _save(destination / "manifest.json", manifest)
    fd = os.open(destination / "cases.jsonl", os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    os.close(fd)
    return destination, manifest


async def run(args):
    dataset = json.loads((ROOT / "tests/fixtures/interaction_cases.json").read_text())
    destination, manifest = prepare_run(args, dataset)
    try:
        result = await _run_cases(args, destination, dataset, manifest)
    except BaseException as error:
        manifest.update(
            state="aborted",
            ended_at=datetime.now(UTC).isoformat(),
            error_type=type(error).__name__,
            source_sha256_at_end=_source_hash(),
        )
        manifest["source_changed_during_run"] = (
            manifest["source_sha256"] != manifest["source_sha256_at_end"]
        )
        _save(destination / "manifest.json", manifest)
        raise
    manifest.update(
        state="completed",
        ended_at=datetime.now(UTC).isoformat(),
        source_sha256_at_end=_source_hash(),
    )
    manifest["source_changed_during_run"] = (
        manifest["source_sha256"] != manifest["source_sha256_at_end"]
    )
    _save(destination / "manifest.json", manifest)
    result.update(
        manifest_path=str(destination / "manifest.json"),
        manifest_sha256=hashlib.sha256((destination / "manifest.json").read_bytes()).hexdigest(),
        source_sha256=manifest["source_sha256"],
        source_changed_during_run=manifest["source_changed_during_run"],
    )
    _save(destination / "result.json", result)
    print(json.dumps(result, ensure_ascii=False), flush=True)
    return result


def equivalent(actual, expected):
    if isinstance(expected, dict):
        return isinstance(actual, dict) and all(
            key in actual and equivalent(actual[key], value) for key, value in expected.items()
        )
    if isinstance(expected, list):
        return isinstance(actual, list) and actual == expected
    if isinstance(expected, str) and isinstance(actual, str) and "T" in expected:
        try:
            return datetime.fromisoformat(actual.replace("Z", "+00:00")) == datetime.fromisoformat(
                expected.replace("Z", "+00:00")
            )
        except ValueError:
            pass
    return actual == expected


def score(case, decision):
    errors = []
    action = decision.action or {}
    if decision.kind not in case["expected_kind"]:
        errors.append("decision_kind")
    if action.get("op") != case["expected_action_op"]:
        errors.append("action_kind")
    effects = {decision.kind, action.get("op")}
    forbidden = bool(effects.intersection(case["forbidden_writes"]))
    if forbidden:
        errors.append("forbidden_write_candidate")
    if "expected_control" in case and decision.control != case["expected_control"]:
        errors.append("control")
    if "expected_target_id" in case:
        actual = (
            decision.target_work_id
            or action.get("task_id")
            or action.get("schedule_id")
            or action.get("fact_id")
        )
        if actual != case["expected_target_id"]:
            errors.append("target_identity")
    if "expected_assignee_id" in case:
        actual = (
            case["context"]["requester_member_id"]
            if action.get("op") == "claim_task"
            else action.get("member_id")
        )
        if actual != case["expected_assignee_id"]:
            errors.append("assignee_identity")
    if "expected_args" in case and not equivalent(action, case["expected_args"]):
        errors.append("action_arguments")
    return errors, forbidden


async def _run_cases(args, destination, dataset, manifest):
    store, ledger, payer, key = setup()
    cases = dataset["cases"][: args.limit] if args.limit else dataset["cases"]
    results = []
    client = DeepSeekHTTPModel(key, model=PROVIDER_MODEL, base_url=PROVIDER_BASE_URL)
    manifest["provider_config"]["actual_model"] = client.model
    manifest["provider_config"]["actual_client_name"] = client.name
    manifest["paid_budget_group"] = payer.group_id
    semaphore = asyncio.Semaphore(args.concurrency)

    async def one(case):
        async with semaphore:
            started = time.monotonic()
            stamp = datetime.now(UTC)
            billing_event = GroupEvent(
                id="eval:" + args.run_id + ":" + case["id"],
                source_key="eval:" + args.run_id + ":" + case["id"],
                group_id=payer.group_id,
                actor_id=payer.member_id,
                kind=EventKind.SYSTEM,
                text="Synthetic semantic evaluation case " + case["id"],
                occurred_at=stamp,
                received_at=stamp,
                trusted_actor_origin="local_acceptance_harness",
            )
            with store.transaction(payer) as tx:
                ingested = tx.ingest_event(
                    billing_event, "eval:" + args.run_id + ":" + case["id"], "1", 0
                )
                billing_event = ingested.event
                result = CoordinationResult(
                    event_id=billing_event.id,
                    decision=Decision(
                        kind="observe", reason_code="acceptance_fixture_not_user_request"
                    ),
                )
                tx.record_decision(
                    billing_event.id, {"kind": "observe", "result": result.model_dump(mode="json")}
                )
            actor_id = case["context"]["requester_member_id"]
            event = GroupEvent(
                id=case["context"]["model_event"]["id"],
                source_key="synthetic:" + case["id"],
                group_id=case["context"]["group_id"],
                actor_id=actor_id,
                kind=EventKind(case["event_kind"]),
                text=case["text"],
                occurred_at=datetime.fromisoformat(case["occurred_at"]),
                received_at=stamp,
            )
            provider = ModelDecisionProvider(
                MeteredModel(
                    client,
                    ledger,
                    payer,
                    price_id="deepseek-20261114-text",
                    source_event_id=billing_event.id,
                ),
                max_retries=2,
            )
            record = {
                "id": case["id"],
                "category": case["category"],
                "labels_sha256": dataset["cases_sha256"],
                "side_effects_applied": False,
            }
            try:
                decision = await provider.decide(event, case["context"])
                errors, forbidden = score(case, decision)
                record.update(
                    status="PASS" if not errors else "FAIL",
                    errors=errors,
                    forbidden_write_candidate=forbidden,
                    decision=decision.model_dump(mode="json"),
                )
            except DomainError as error:
                record.update(
                    status="ERROR",
                    error_code=error.code,
                    errors=[error.code],
                    forbidden_write_candidate=False,
                )
            record["diagnostics"] = getattr(provider, "diagnostics", [])
            record["seconds"] = round(time.monotonic() - started, 3)
            results.append(record)
            with (destination / "cases.jsonl").open("a", encoding="utf-8") as output:
                output.write(json.dumps(record, ensure_ascii=False) + "\n")
            if record["status"] != "PASS" or len(results) % 20 == 0:
                print(
                    json.dumps(
                        {
                            "completed": len(results),
                            "id": record["id"],
                            "status": record["status"],
                            "errors": record["errors"],
                        },
                        ensure_ascii=False,
                    ),
                    flush=True,
                )

    try:
        await asyncio.gather(*(one(case) for case in cases))
        totals = Counter(record["status"] for record in results)
        by_category = {
            category: dict(
                Counter(record["status"] for record in results if record["category"] == category)
            )
            for category in sorted({record["category"] for record in results})
        }
        ordered = sorted(record["seconds"] for record in results)
        result = {
            "run_id": args.run_id,
            "dataset_hash": dataset["cases_sha256"],
            "count": len(results),
            "statuses": dict(totals),
            "pass_rate": totals["PASS"] / len(results),
            "forbidden_write_candidates": sum(
                record["forbidden_write_candidate"] for record in results
            ),
            "business_writes_applied": 0,
            "latency_p95_seconds": ordered[min(len(ordered) - 1, int(len(ordered) * 0.95))],
            "categories": by_category,
            "budget": ledger.report(payer),
            "limitations": [
                "Synthetic cases pre-labeled by an independent agent, not human adjudication",
                "This tests candidate understanding; transactional authorization and side effects require separate integration tests",
                "Repeated prompt tuning on these cases is regression evidence, not an unseen-task generalization estimate",
            ],
        }
        return result
    finally:
        await client.aclose()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--concurrency", type=int, default=2, choices=[1, 2, 3])
    asyncio.run(run(parser.parse_args()))
