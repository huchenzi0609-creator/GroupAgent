"""Frozen 30-task acceptance through the actual app, with only a local channel.

Every paid request, including fixture speech and verification transcription,
uses live_preflight's existing cumulative ledger. Expectations stay out of the
agent's context. Failed cases and their actual output are retained unchanged.
"""

from __future__ import annotations

import asyncio
import fcntl
import hashlib
import json
import os
import re
import shutil
import time
import traceback
from dataclasses import asdict
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import uuid4

from live_preflight import ROOT, setup
from pydantic import ValidationError

from groupagent.artifacts import ArtifactScope
from groupagent.config import AppConfig
from groupagent.coordinator import CoordinationResult, Decision
from groupagent.domain import EventKind, GroupEvent, WorkState
from groupagent.media import MediaService
from groupagent.media.acceptance import make_image_cases
from groupagent.media.models import MediaSource
from groupagent.runtime.app import build_app
from groupagent.tools import ToolContext, ToolScope


def save(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    if path.is_symlink() or path.parent.resolve() != path.parent.absolute():
        raise RuntimeError("Acceptance evidence cannot traverse symlinks")
    temporary = path.with_name(path.name + "." + uuid4().hex + ".pending")
    fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "w") as output:
        json.dump(value, output, ensure_ascii=False, indent=2, default=str)
        output.flush()
        os.fsync(output.fileno())
    temporary.replace(path)


def canonical_hash(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(
            value, sort_keys=True, ensure_ascii=False, separators=(",", ":"), default=str
        ).encode()
    ).hexdigest()


def source_hash() -> str:
    digest = hashlib.sha256()
    for path in sorted((ROOT / "src").rglob("*.py")):
        digest.update(
            str(path.relative_to(ROOT)).encode()
            + b"\0"
            + hashlib.sha256(path.read_bytes()).digest()
        )
    return digest.hexdigest()


def read_json(path: Path) -> dict:
    if path.is_symlink() or path.resolve() != path.absolute() or path.stat().st_size > 64 * 1024**2:
        raise RuntimeError("Invalid acceptance metadata path")
    value = json.loads(path.read_text())
    if not isinstance(value, dict):
        raise RuntimeError("Acceptance metadata must be an object")
    return value


def select_cases(dataset: dict, case_ids: str | None = None) -> list[dict]:
    """A focused smoke run includes declared input dependencies, never new gold."""
    cases = dataset["cases"]
    if not case_ids:
        return list(cases)
    wanted = {value.strip() for value in case_ids.split(",") if value.strip()}
    known = {case["id"]: case for case in cases}
    if not wanted or not wanted <= known.keys():
        raise RuntimeError("Requested acceptance case IDs are empty or unknown")
    while True:
        dependencies = {
            item["source_case"]
            for identity in wanted
            for item in known[identity].get("inputs", [])
            if item.get("source_case")
        }
        if not dependencies <= known.keys():
            raise RuntimeError("An acceptance input dependency is unknown")
        if dependencies <= wanted:
            break
        wanted.update(dependencies)
    selected = [case for case in cases if case["id"] in wanted]
    prior: set[str] = set()
    for case in selected:
        if any(
            item.get("source_case") and item["source_case"] not in prior
            for item in case.get("inputs", [])
        ):
            raise RuntimeError("Acceptance input dependencies must precede their consumers")
        prior.add(case["id"])
    return selected


def validate_resume(root: Path, dataset: dict, args) -> tuple[dict, dict, dict]:
    manifest = read_json(root / "manifest.json")
    original = read_json(root / "dataset.json")
    config = read_json(root / "config.json")
    results = (
        read_json(root / "results.json") if (root / "results.json").exists() else {"cases": []}
    )
    digest = canonical_hash(dataset["cases"])
    if (
        digest != dataset.get("cases_sha256")
        or digest != manifest.get("dataset_sha256")
        or original != dataset
    ):
        raise RuntimeError("Resume dataset/gold differs from the frozen run")
    runtime = config.get("runtime", {})
    if any(
        manifest.get(key) != getattr(args, key) or runtime.get(key) != getattr(args, key)
        for key in ("worker_image", "harness_image")
    ):
        raise RuntimeError("Resume worker/harness images differ from the original run")
    if (
        runtime.get("channel") != "local"
        or manifest.get("channel") != "local_only"
        or runtime.get("channel_account_id") != "matrix:" + args.run_id
    ):
        raise RuntimeError("Resume requires the original local-only channel identity")
    if runtime.get("docker_host") != args.docker_host:
        raise RuntimeError("Resume Docker configuration differs from the original run")
    if manifest.get("config_sha256") and manifest["config_sha256"] != canonical_hash(config):
        raise RuntimeError("Resume configuration checksum changed")
    ids = [row.get("id") for row in results.get("cases", [])]
    if len(ids) != len(set(ids)) or not set(ids) <= {case["id"] for case in dataset["cases"]}:
        raise RuntimeError("Resume results contain duplicated or foreign cases")
    selected = manifest.get("selected_case_ids", [case["id"] for case in dataset["cases"]])
    if (
        not isinstance(selected, list)
        or len(selected) != len(set(selected))
        or not set(selected) <= {case["id"] for case in dataset["cases"]}
    ):
        raise RuntimeError("Resume case selection is invalid")
    if getattr(args, "case_ids", None) and selected != [
        case["id"] for case in select_cases(dataset, args.case_ids)
    ]:
        raise RuntimeError("Resume cannot change the original case selection")
    if not set(ids) <= set(selected):
        raise RuntimeError("Resume results contain cases outside their declared selection")
    return manifest, config, results


class MatrixRun:
    def __init__(self, args):
        self.args = args
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,79}", args.run_id) or ".." in args.run_id:
            raise RuntimeError("Run ID must be a bounded identifier")
        self.dataset = read_json(ROOT / "tests/fixtures/runtime_acceptance_cases.json")
        if canonical_hash(self.dataset["cases"]) != self.dataset["cases_sha256"]:
            raise RuntimeError("Frozen case checksum mismatch")
        selection = select_cases(self.dataset, getattr(args, "case_ids", None))
        self.root = ROOT / "runtime/live" / args.run_id
        self.resuming = bool(getattr(args, "resume", False))
        self.recheck_only = bool(getattr(args, "recheck_only", False))
        if self.recheck_only and not self.resuming:
            raise RuntimeError("--recheck-only requires --resume")
        if not self.resuming and self.root.exists():
            raise RuntimeError("Use --resume for an existing run; prior evidence is immutable")
        if self.resuming and not self.root.is_dir():
            raise RuntimeError("Resume run directory was not found")
        self.root.mkdir(parents=True, exist_ok=self.resuming, mode=0o700)
        if self.root.resolve() != self.root.absolute():
            raise RuntimeError("Run directory must be canonical")
        self._lock = os.open(
            self.root / "runner.lock", os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600
        )
        try:
            fcntl.flock(self._lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            os.close(self._lock)
            raise RuntimeError("Another acceptance runner owns this run") from None
        original_manifest, saved_config, prior = (
            validate_resume(self.root, self.dataset, args)
            if self.resuming
            else ({}, {}, {"cases": []})
        )
        if self.resuming:
            selected_ids = original_manifest.get(
                "selected_case_ids", [case["id"] for case in self.dataset["cases"]]
            )
            selection = [case for case in self.dataset["cases"] if case["id"] in selected_ids]
        self.selected_cases = selection
        self.records = {row["id"]: row for row in prior["cases"]}
        self.events: dict[str, GroupEvent] = {}
        self.rules: dict[str, dict] = {}
        self.artifacts: dict[str, list[dict]] = {}
        self.current_source = source_hash()
        self.original_manifest = original_manifest
        self.revision = 0
        self.evidence_root = self.root
        if self.resuming:
            existing = [
                int(path.name) for path in (self.root / "revisions").glob("[0-9]*") if path.is_dir()
            ]
            self.revision = max(existing, default=0) + 1
            archive = self.root / "revisions" / f"{self.revision:04d}"
            archive.mkdir(parents=True, mode=0o700)
            for name in ("results.json", "manifest.json", "config.json", "dataset.json"):
                if (self.root / name).exists():
                    shutil.copy2(self.root / name, archive / name)
            if (self.root / "evidence").exists():
                shutil.copytree(self.root / "evidence", archive / "evidence", symlinks=False)
            self.evidence_root = self.root / "rechecks" / f"{self.revision:04d}"
            self.evidence_root.mkdir(parents=True, mode=0o700)
        # Only the root/operator invokes this constructor in a real run. Unit
        # tests inject setup/build_app; resume validation above never reads secrets.
        self.store, self.ledger, self.actor, _key = setup()
        if self.resuming:
            if (
                saved_config.get("group", {}).get("internal_id") != self.actor.group_id
                or original_manifest.get("paid_budget_group") != self.actor.group_id
            ):
                raise RuntimeError("Resume budget/group identity changed")
            if self.actor.member_id not in saved_config["group"]["owner_ids"]:
                raise RuntimeError("Resume requester identity changed")
            config = AppConfig.model_validate(saved_config)
            if canonical_hash(config.model_dump(mode="json")) != canonical_hash(saved_config):
                raise RuntimeError(
                    "Resume configuration schema/defaults changed; use a new run for new configuration"
                )
        else:
            config = AppConfig.model_validate(
                {
                    "deployment_mode": "test",
                    "group": {
                        "enabled": True,
                        "internal_id": self.actor.group_id,
                        "external_group_id": "synthetic-local@chatroom",
                        "owner_ids": [self.actor.member_id],
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
                        "channel_account_id": "matrix:" + args.run_id,
                        "engine": "harness",
                        "worker_image": args.worker_image,
                        "harness_image": args.harness_image,
                        "docker_host": args.docker_host,
                        "local_member_names": {self.actor.member_id: "合成测试同学"},
                    },
                }
            )
        self.app = build_app(config, self.store, self.root / "app", ROOT / ".env.local")
        self.inputs = self.app.inputs
        assert self.inputs is not None
        if self.resuming:
            with self.store.transaction(self.actor) as tx:
                topics = tx.conn.execute(
                    "SELECT id FROM ga_memory_topics WHERE group_id=%s AND title=%s AND created_by=%s",
                    (self.actor.group_id, "独立本地验收：" + args.run_id, self.actor.member_id),
                ).fetchall()
            if len(topics) != 1:
                raise RuntimeError("Original acceptance topic is missing or ambiguous")
            self.topic = type("TopicIdentity", (), {"id": topics[0]["id"]})()
            self._hydrate()
        else:
            self.topic = self.app.coordinator.memory.create_topic(
                self.actor, "独立本地验收：" + args.run_id
            )
            save(self.root / "dataset.json", self.dataset)
            save(self.root / "config.json", config.model_dump(mode="json"))
            self.original_manifest = {
                "source_sha256": self.current_source,
                "dataset_sha256": self.dataset["cases_sha256"],
                "config_sha256": canonical_hash(config.model_dump(mode="json")),
                "worker_image": args.worker_image,
                "harness_image": args.harness_image,
                "channel": "local_only",
                "paid_budget_group": self.actor.group_id,
                "total_cny_limit": 50,
                "selected_case_ids": [case["id"] for case in selection],
                "stop_on_failure": bool(getattr(args, "stop_on_failure", False)),
                "started_at": datetime.now(UTC),
            }
            save(self.root / "manifest.json", self.original_manifest)
        self.verification_manifest = {
            "revision": self.revision,
            "source_sha256": self.current_source,
            "original_source_sha256": self.original_manifest["source_sha256"],
            "mixed_product_sources": self.current_source != self.original_manifest["source_sha256"],
            "dataset_sha256": self.dataset["cases_sha256"],
            "config_sha256": canonical_hash(
                saved_config if self.resuming else config.model_dump(mode="json")
            ),
            "worker_image": args.worker_image,
            "harness_image": args.harness_image,
            "checker_sha256": hashlib.sha256(
                (ROOT / "scripts/runtime_acceptance_checks.py").read_bytes()
            ).hexdigest(),
            "runner_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
            "resumed_at": datetime.now(UTC),
            "completed_rechecks_allow_paid_calls": False,
            "recheck_only": self.recheck_only,
        }
        save(self.evidence_root / "verification-manifest.json", self.verification_manifest)

    def _evidence(self, case_id):
        path = self.records.get(case_id, {}).get("evidence_path")
        candidate = Path(path) if path else self.root / "evidence" / (case_id + ".json")
        if not candidate.is_relative_to(self.root) or candidate.resolve() != candidate.absolute():
            raise RuntimeError("Evidence reference escaped the run directory")
        return read_json(candidate) if candidate.is_file() else {}

    def _hydrate(self):
        for case in self.dataset["cases"]:
            identity = case["id"]
            evidence = self._evidence(identity)
            state_path = self.root / "progress" / (identity + ".json")
            state = read_json(state_path) if state_path.exists() else {}
            with self.store.transaction(self.actor) as tx:
                row = tx.conn.execute(
                    "SELECT id FROM ga_events WHERE group_id=%s AND source_key=%s",
                    (self.actor.group_id, "matrix:" + self.args.run_id + ":" + identity),
                ).fetchone()
                event_id = (
                    (row or {}).get("id")
                    or state.get("event_id")
                    or evidence.get("scope", {}).get("event_id")
                )
                if event_id:
                    event = tx.get_event(event_id)
                    if (
                        case["category"] != "schedule_fire"
                        and event.source_key != "matrix:" + self.args.run_id + ":" + identity
                    ):
                        raise RuntimeError("Stored case event has another run identity")
                    self.events[identity] = event
            self.artifacts[identity] = evidence.get("artifacts", [])
            rule = evidence.get("scheduling", {}).get("rule")
            if isinstance(rule, dict):
                self.rules[identity] = rule

    def _write_results(self):
        save(
            self.root / "results.json",
            {
                "cases": [
                    self.records[case["id"]]
                    for case in self.dataset["cases"]
                    if case["id"] in self.records
                ],
                "budget": self.ledger.report(self.actor),
                "verification_revision": self.revision,
                "verification_manifest": str(self.evidence_root / "verification-manifest.json"),
            },
        )

    def _progress(self, case, **values):
        path = self.root / "progress" / (case["id"] + ".json")
        existing = read_json(path) if path.exists() else {}
        save(
            path,
            {
                **existing,
                **values,
                "case_id": case["id"],
                "source_sha256": self.current_source,
                "updated_at": datetime.now(UTC),
            },
        )

    def context(self, case_id: str, *, work_id: str | None = None) -> ToolContext:
        workspace = self.root / "verification" / case_id
        workspace.mkdir(parents=True, exist_ok=True, mode=0o700)
        return ToolContext(
            ToolScope(
                self.actor.group_id,
                work_id or "verify:" + case_id,
                self.actor,
                workspace.resolve(),
                frozenset(),
                case_id,
            ),
            "acceptance:" + case_id,
            "acceptance:" + self.args.run_id + ":" + case_id,
            asyncio.Event(),
        )

    async def speech(self, label: str, *, text: str | None = None, content: bytes | None = None):
        """Audited test-only media work; never claims an application task succeeded."""
        now = datetime.now(UTC)
        event = GroupEvent(
            id=uuid4().hex,
            source_key="matrix-media:" + self.args.run_id + ":" + label,
            group_id=self.actor.group_id,
            actor_id=self.actor.member_id,
            kind=EventKind.SYSTEM,
            text="Synthetic acceptance media preparation or verification",
            occurred_at=now,
            received_at=now,
        )
        with self.store.transaction(self.actor) as tx:
            tx.ingest_event(event, event.id, "1", 0)
            observed = CoordinationResult(
                event_id=event.id,
                decision=Decision(
                    kind="observe", reason_code="acceptance_fixture_not_user_request"
                ),
            )
            tx.record_decision(
                event.id, {"kind": "observe", "result": observed.model_dump(mode="json")}
            )
            work = tx.create_work("Synthetic media test: " + label, event.id)
            lease = tx.claim_work(work.id, "acceptance-media", ttl_seconds=300)
        context = self.context(label, work_id=work.id)
        service = MediaService(
            self.inputs.artifacts,
            self.inputs.worker,
            providers=self.inputs.providers,
            budget=self.ledger,
        )
        source = MediaSource(
            group_id=self.actor.group_id,
            work_id=work.id,
            source_event_id=event.id,
            source_message_id=event.id,
            member_id=self.actor.member_id,
            attachment_id=label,
            declared_kind="audio",
            origin_kind="synthetic_fixture",
        )
        try:
            if text is not None:
                reference = await service.synthesize(
                    context, text, self.inputs.routes["tts"], source, explicitly_requested=True
                )
                data = self.inputs.artifacts.read(
                    ArtifactScope(self.actor.group_id, work.id, self.actor.member_id),
                    reference.file_id,
                    reference.version_id,
                )
                return data, reference.model_dump(mode="json")
            assert content is not None
            reference = await service.ingest_audio(context, source, content)
            transcript = await service.transcribe(context, reference, self.inputs.routes["asr"])
            return transcript.text, transcript.model_dump(mode="json")
        finally:
            with self.store.transaction(self.actor) as tx:
                current = tx.get_work(work.id)
                if current.state == WorkState.RUNNING:
                    tx.transition_work(
                        work.id,
                        WorkState.CANCELLED,
                        expected_revision=current.revision,
                        reason="acceptance_probe_finished",
                        lease=lease,
                    )

    def counts(self) -> dict[str, int]:
        with self.store.transaction(self.actor) as tx:
            return {
                key: tx.conn.execute(
                    "SELECT count(*) AS n FROM " + table + " WHERE group_id=%s", (tx.group_id,)
                ).fetchone()["n"]
                for key, table in (
                    ("human_tasks", "ga_team_tasks"),
                    ("schedules", "ga_schedules"),
                    ("works", "ga_work_orders"),
                    ("occurrences", "ga_schedule_occurrences"),
                    ("deliveries", "ga_deliveries"),
                )
            }

    def list_rules(self):
        return [
            rule.model_dump(mode="json") for rule in self.app.coordinator.scheduler.list(self.actor)
        ]

    async def prepare(self, case: dict) -> tuple[GroupEvent, list[dict]]:
        now = datetime.now(UTC)
        identifier = uuid4().hex
        kind = EventKind.TEXT
        attachments = []
        reply_to = None
        media_evidence = []
        for item in case.get("inputs", []):
            if item.get("fixture") == "groupagent.media.acceptance.make_image_cases":
                fixture = next(
                    value for value in make_image_cases() if value.case_id == item["case_id"]
                )
                content = fixture.pixels
                handle = self.app.channel.add_attachment(
                    identifier, item["case_id"] + ".png", content, kind="image"
                )
                attachments.append(handle.as_attachment())
                kind = EventKind.IMAGE
                media_evidence.append(
                    {
                        "event_id": identifier,
                        "attachment_id": handle.asset_id,
                        "source_sha256": hashlib.sha256(content).hexdigest(),
                        "bytes": len(content),
                    }
                )
            elif item.get("fixture") == "parent_budgeted_tts":
                content, preparation = await self.speech(case["id"] + "-fixture", text=item["text"])
                # A WAV attachment is a file message, not a native voice utterance:
                # the caption explicitly asks for transcription, not instructions
                # contained in the recording.
                handle = self.app.channel.add_attachment(
                    identifier, "synthetic-speech.wav", content, kind="file"
                )
                attachments.append(handle.as_attachment())
                kind = EventKind.FILE
                media_evidence.append(
                    {
                        "event_id": identifier,
                        "attachment_id": handle.asset_id,
                        "source_sha256": hashlib.sha256(content).hexdigest(),
                        "bytes": len(content),
                        "preparation": preparation,
                    }
                )
            elif item.get("source_case"):
                parent = item["source_case"]
                if self.records.get(parent, {}).get("execution_state") not in {
                    "completed",
                    "reply",
                }:
                    raise RuntimeError(
                        "A required earlier case did not produce a usable result: " + parent
                    )
                reply_to = self.events[parent].id
                kind = EventKind.QUOTE
        event = GroupEvent(
            id=identifier,
            source_key="matrix:" + self.args.run_id + ":" + case["id"],
            group_id=self.actor.group_id,
            actor_id=self.actor.member_id,
            kind=kind,
            text=case["prompt"],
            occurred_at=now,
            received_at=datetime.now(UTC),
            reply_to=reply_to,
            attachments=attachments,
            channel_account_id=self.app.config.runtime.channel_account_id,
            source_epoch=self.app.config.runtime.source_epoch,
            platform_message_id=identifier,
        )
        self.events[case["id"]] = event
        self.app.channel.append_incoming(event)
        await self.app.poll_intake()
        # Keep the live app's conversation evidence separate from billing and
        # modality fixtures while retaining one cumulative group budget.
        self.app.coordinator.memory.link_event(
            self.actor, event.id, self.topic.id, evidence_event_ids=[event.id]
        )
        return event, media_evidence

    async def wait_result(self, event: GroupEvent, *, work_id: str | None = None):
        started = time.monotonic()
        result = None
        work = None
        while time.monotonic() - started < self.args.timeout:
            await self.app.step()
            await asyncio.sleep(0.15)
            with self.store.transaction(self.actor) as tx:
                result = tx.get_decision(event.id)
                selected = work_id or (result or {}).get("result", {}).get("work_id")
                work = tx.get_work(selected) if selected else None
                if work and work.state in {
                    WorkState.COMPLETED,
                    WorkState.FAILED,
                    WorkState.CANCELLED,
                    WorkState.PAUSED,
                    WorkState.WAITING_INPUT,
                }:
                    return work.state.value, result, work
                if result and not work:
                    deliveries = tx.conn.execute(
                        "SELECT state FROM ga_deliveries WHERE group_id=%s AND request_event_id=%s",
                        (tx.group_id, event.id),
                    ).fetchall()
                    if not deliveries or all(
                        row["state"] in {"accepted", "rejected"} for row in deliveries
                    ):
                        return result["result"]["decision"]["kind"], result, work
        if work:
            with self.store.transaction(self.actor) as tx:
                current = tx.get_work(work.id)
                if current.state not in {
                    WorkState.COMPLETED,
                    WorkState.FAILED,
                    WorkState.CANCELLED,
                }:
                    tx.transition_work(
                        current.id,
                        WorkState.CANCELLED,
                        expected_revision=current.revision,
                        reason="acceptance_timeout",
                    )
        return "timeout", result, work

    async def collect(
        self,
        case,
        event,
        work,
        media_evidence,
        before,
        rule_before,
        *,
        scheduling=None,
        allow_paid_verification=True,
        previous_evidence=None,
    ):
        from runtime_acceptance_checks import decode_artifact

        with self.store.transaction(self.actor) as tx:
            deliveries = tx.conn.execute(
                "SELECT * FROM ga_deliveries WHERE group_id=%s AND (request_event_id=%s OR work_id=%s) ORDER BY created_at,id",
                (tx.group_id, event.id, work.id if work else None),
            ).fetchall()
            parts = tx.conn.execute(
                "SELECT p.* FROM ga_delivery_parts p JOIN ga_deliveries d ON d.group_id=p.group_id AND d.id=p.intent_id WHERE d.group_id=%s AND (d.request_event_id=%s OR d.work_id=%s) ORDER BY d.created_at,p.position",
                (tx.group_id, event.id, work.id if work else None),
            ).fetchall()
            catalog = tx.conn.execute(
                "SELECT * FROM ga_artifact_catalog WHERE group_id=%s AND work_id=%s AND shared ORDER BY created_at",
                (tx.group_id, work.id if work else None),
            ).fetchall()
        checkpoint = self.app.state.load_checkpoint(self.actor, work.id) if work else None
        calls = (
            {call.id: call.arguments for msg in checkpoint.messages for call in msg.tool_calls}
            if checkpoint
            else {}
        )
        receipts = (
            [
                {
                    "group_id": self.actor.group_id,
                    "work_id": work.id,
                    "receipt": receipt.model_dump(mode="json"),
                    "arguments": calls.get(receipt.call_id),
                }
                for receipt in checkpoint.receipts
            ]
            if checkpoint and work
            else []
        )
        artifacts = []
        for row in catalog:
            version, content = self.inputs.registry.read(
                self.actor, row["version_id"], require_validated=True
            )
            if Path(version.name).name != version.name:
                raise RuntimeError("Artifact filename escaped its output directory")
            target = self.evidence_root / "outputs" / case["id"] / version.name
            target.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
            target.write_bytes(content)
            decoded = await decode_artifact(
                self.inputs.worker, self.context(case["id"]), version.format, content
            )
            if version.format in {"wav", "ogg", "mp3"}:
                if allow_paid_verification:
                    transcript, proof = await self.speech(
                        case["id"] + "-verify-" + version.version_id, content=content
                    )
                    decoded.setdefault("audio", {}).update(
                        asr_transcript=transcript,
                        asr_receipt=proof,
                        asr_source_sha256=hashlib.sha256(content).hexdigest(),
                        asr_evidence=proof,
                    )
                else:
                    for old in (previous_evidence or {}).get("artifacts", []):
                        prior_audio = (old.get("decoded") or {}).get("audio") or {}
                        if (
                            old.get("version_id") == version.version_id
                            and prior_audio.get("asr_source_sha256") == version.sha256
                        ):
                            decoded.setdefault("audio", {}).update(
                                {
                                    key: prior_audio[key]
                                    for key in (
                                        "asr_transcript",
                                        "asr_receipt",
                                        "asr_source_sha256",
                                        "asr_evidence",
                                    )
                                    if key in prior_audio
                                }
                            )
            artifacts.append(
                {
                    **asdict(version),
                    "sha256": hashlib.sha256(content).hexdigest(),
                    "validation": row["validation"],
                    "decoded": decoded,
                    "content_path": str(target),
                    "provenance": version.provenance,
                }
            )
        self.artifacts[case["id"]] = artifacts
        rules = self.list_rules()
        new_rules = [rule for rule in rules if rule["id"] not in {r["id"] for r in rule_before}]
        referenced = next(
            (
                self.rules[item["source_case"]]
                for item in case.get("inputs", [])
                if item.get("reference") == "actual_rule"
            ),
            None,
        )
        selected = (
            new_rules[0]
            if len(new_rules) == 1
            else next((r for r in rules if referenced and r["id"] == referenced["id"]), None)
        )
        if selected:
            self.rules[case["id"]] = selected
        after = self.counts()
        evidence = {
            "scope": {
                "group_id": self.actor.group_id,
                "work_id": work.id if work else None,
                "event_id": event.id,
                "requester_id": self.actor.member_id,
                "occurred_at": event.occurred_at.isoformat(),
            },
            "answer_text": "\n".join(
                row["payload_ref"][5:]
                for row in deliveries
                if row["state"] == "accepted" and row["payload_ref"].startswith("text:")
            ),
            "artifacts": artifacts,
            "tool_receipts": receipts,
            "input_media": media_evidence,
            "delivery": {
                "parts": parts,
                "intents": deliveries,
                "work_completed_at": work.updated_at.isoformat()
                if work and work.state == WorkState.COMPLETED
                else None,
            },
            "before_after": {
                **{key + "_before": value for key, value in before.items()},
                **{key + "_after": value for key, value in after.items()},
            },
            "scheduling": {
                "rules_before": rule_before,
                "rules_after": rules,
                "rule": selected,
                "reference_rule": referenced,
                "reference_case_id": next(
                    (
                        item["source_case"]
                        for item in case.get("inputs", [])
                        if item.get("reference") == "actual_rule"
                    ),
                    None,
                ),
                **(scheduling or {}),
            },
        }
        from groupagent.sandbox import DockerSandbox

        sandbox = self.inputs.worker.sandbox
        if (
            isinstance(sandbox, DockerSandbox)
            and sandbox.image == self.original_manifest["worker_image"]
        ):
            evidence.update(
                execution_backend="docker",
                execution_backend_proof={
                    "runtime_type": type(sandbox).__module__ + "." + type(sandbox).__qualname__,
                    "worker_image": sandbox.image,
                    "manifest_path": str(self.root / "manifest.json"),
                },
            )
        if previous_evidence:
            for historical in ("before_after", "scheduling"):
                if historical in previous_evidence:
                    evidence[historical] = previous_evidence[historical]
        for item in case.get("inputs", []):
            if item.get("reference") == "delivered_artifact":
                originals = self.artifacts[item["source_case"]]
                source = originals[0]
                _, original_bytes = self.inputs.registry.read(
                    self.actor, source["version_id"], require_validated=True
                )
                evidence["comparison"] = {
                    "source_artifact": source,
                    "source_sha256_after": hashlib.sha256(original_bytes).hexdigest(),
                    "new_artifact": artifacts[0] if artifacts else None,
                }
                if artifacts:
                    chain = set()
                    pending = [artifacts[0]["version_id"]]
                    with self.store.transaction(self.actor) as tx:
                        while pending and len(chain) < 100:
                            identifier = pending.pop()
                            if identifier in chain:
                                continue
                            row = tx.conn.execute(
                                "SELECT body FROM ga_artifact_catalog WHERE group_id=%s AND version_id=%s",
                                (tx.group_id, identifier),
                            ).fetchone()
                            if row is None:
                                continue
                            chain.add(identifier)
                            body = row["body"]
                            parents = [
                                body.get("parent_version_id"),
                                body.get("provenance", {}).get("input_version_id"),
                            ]
                            parents += body.get("provenance", {}).get("input_versions", [])
                            pending.extend(value for value in parents if isinstance(value, str))
                    evidence["comparison"]["provenance_version_ids"] = sorted(chain)
        # Source binding is evidenced by the actual imported file and successful
        # media receipt, never by the mere fact that the prompt had an attachment.
        if work and media_evidence:
            with self.store.transaction(self.actor) as tx:
                imported = tx.conn.execute(
                    "SELECT body FROM ga_artifact_catalog WHERE group_id=%s AND work_id=%s",
                    (tx.group_id, work.id),
                ).fetchall()
            for original in media_evidence:
                source_files = [
                    row["body"]
                    for row in imported
                    if row["body"].get("provenance", {}).get("source_event_id") == event.id
                    and row["body"]["sha256"] == original["source_sha256"]
                ]
                used = [
                    r["receipt"]
                    for r in receipts
                    if r["receipt"]["success"]
                    and r["receipt"]["tool_name"]
                    in {"image_analyze", "media_ocr", "provider_ocr", "audio_transcribe"}
                ]
                used = [
                    receipt
                    for receipt in used
                    if any(
                        source.get("source_event_id") == event.id
                        and source.get("attachment_id") == original["attachment_id"]
                        for source in (
                            [receipt.get("output", {}).get("source", {})]
                            + receipt.get("output", {}).get("sources", [])
                        )
                        if isinstance(source, dict)
                    )
                ]
                original.update(binding_verified=bool(source_files and used))
                if source_files and used:
                    original[
                        "pixel_input_sha256"
                        if case["category"] == "image_analysis"
                        else "audio_input_sha256"
                    ] = source_files[0]["sha256"]
                    proofs = [
                        proof
                        for receipt in used
                        for proof in receipt.get("output", {}).get("budget_receipts", [])
                        if isinstance(proof, str)
                    ]
                    original["provider_receipt"] = ",".join(proofs) or next(
                        (
                            str(receipt["output"]["request_id"])
                            for receipt in used
                            if receipt.get("output", {}).get("request_id")
                        ),
                        "",
                    )
                    if case["category"] == "asr":
                        original["transcript"] = "\n".join(
                            str(receipt.get("output", {}).get("text", "")) for receipt in used
                        )
        return evidence

    def _review_evidence(self, case, evidence):
        from runtime_png_ocr import attach_observations

        evidence = attach_observations(self.root, case["id"], evidence)
        path = self.root / "reviews" / (case["id"] + ".json")
        if not path.exists():
            return evidence
        review = read_json(path)
        annotations = review.get("reviews", review)
        if not isinstance(annotations, dict):
            raise RuntimeError("Review evidence must be a mapping")
        evidence = dict(evidence)
        evidence["reviews"] = {
            **evidence.get("reviews", {}),
            **{key: value for key, value in annotations.items() if key in case["checks"]},
        }
        evidence["review_sources"] = [
            {
                "path": str(path),
                "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                "type": review.get("reviewer_type", "independent_attestation_not_human_by_default"),
            }
        ]
        return evidence

    async def resume_case(self, case):
        """Return True for any already-started case; never execute it again."""
        from runtime_acceptance_checks import evaluate_case

        identity = case["id"]
        old = self.records.get(identity)
        prior = self._evidence(identity)
        event = self.events.get(identity)
        progress_path = self.root / "progress" / (identity + ".json")
        progress = read_json(progress_path) if progress_path.exists() else {}
        with self.store.transaction(self.actor) as tx:
            fixture = tx.conn.execute(
                "SELECT 1 FROM ga_events WHERE group_id=%s AND source_key=%s",
                (tx.group_id, "matrix-media:" + self.args.run_id + ":" + identity + "-fixture"),
            ).fetchone()
            decision = tx.get_decision(event.id) if event else None
            work_id = (
                (old or {}).get("work_id")
                or prior.get("scope", {}).get("work_id")
                or progress.get("work_id")
                or (decision or {}).get("result", {}).get("work_id")
            )
            work = tx.get_work(work_id) if work_id else None
        if not any((old, prior, event, progress, fixture)):
            return False
        if old and old.get("execution_state") in {
            "failed",
            "paused",
            "cancelled",
            "waiting_input",
            "timeout",
        }:
            return True
        if work is None or work.state != WorkState.COMPLETED:
            if old is None:
                self.records[identity] = {
                    "id": identity,
                    "category": case["category"],
                    "execution_state": work.state.value if work else "started_unresolved",
                    "work_id": work.id if work else None,
                    "evaluation": {
                        "status": "NOT_RUN",
                        "reason": "Existing started case was retained without executing or paying again",
                    },
                }
                self._write_results()
            return True
        if event is None:
            raise RuntimeError("Completed case has no canonical original event")
        if case["category"] != "schedule_fire" and work.request_event_id != event.id:
            raise RuntimeError("Completed work belongs to a different canonical request")
        if prior.get("scope", {}).get("work_id") not in {None, work.id}:
            raise RuntimeError("Prior evidence refers to a different work")
        before = {
            key.removesuffix("_before"): value
            for key, value in prior.get("before_after", {}).items()
            if key.endswith("_before")
        } or progress.get("before", {})
        evidence = await self.collect(
            case,
            event,
            work,
            prior.get("input_media", progress.get("input_media", [])),
            before,
            prior.get("scheduling", {}).get("rules_before", progress.get("rules_before", [])),
            scheduling=prior.get("scheduling"),
            allow_paid_verification=False,
            previous_evidence=prior,
        )
        if not prior.get("before_after"):
            evidence["before_after"] = {
                **{key + "_before": value for key, value in before.items()},
                **{key + "_after": value for key, value in progress.get("after", {}).items()},
            }
        evidence = self._review_evidence(case, evidence)
        path = self.evidence_root / "evidence" / (identity + ".json")
        save(path, evidence)
        result = evaluate_case(case, json.loads(json.dumps(evidence, default=str)))
        self.records[identity] = {
            **(old or {}),
            "id": identity,
            "category": case["category"],
            "execution_state": "completed",
            "work_id": work.id,
            "decision": decision,
            "evaluation": result,
            "evidence_path": str(path),
            "verification_revision": self.revision,
            "paid_calls_during_recheck": 0,
            "original_execution_source_sha256": (old or {}).get(
                "execution_source_sha256", self.original_manifest["source_sha256"]
            ),
        }
        self._write_results()
        print(
            json.dumps(
                {"id": identity, "mode": "completed_recheck", "evaluation": result},
                ensure_ascii=False,
            ),
            flush=True,
        )
        return True

    def _assert_no_implicit_retries(self):
        # app.step runs the normal group dispatcher/supervisor. Do not let it
        # silently pick up an old failed-attempt's leftover pending execution.
        with self.store.transaction(self.actor) as tx:
            active = tx.conn.execute(
                "SELECT id,state FROM ga_work_orders WHERE group_id=%s AND state IN ('queued','running','validating','delivering')",
                (tx.group_id,),
            ).fetchall()
            pending = tx.conn.execute(
                "SELECT e.id FROM ga_events e LEFT JOIN ga_decisions d ON d.group_id=e.group_id AND d.event_id=e.id LEFT JOIN ga_event_retry r ON r.group_id=e.group_id AND r.event_id=e.id WHERE e.group_id=%s AND e.memory_state='active' AND d.event_id IS NULL AND (r.event_id IS NULL OR NOT r.blocked) LIMIT 1",
                (tx.group_id,),
            ).fetchone()
        if active or pending:
            raise RuntimeError(
                "Unresolved prior work/events require review before continuing unstarted cases; use --resume --recheck-only"
            )

    async def case(self, case):
        from runtime_acceptance_checks import evaluate_case

        started = time.monotonic()
        before = self.counts()
        rule_before = self.list_rules()
        self._progress(case, phase="started", before=before, rules_before=rule_before)
        if case["category"] == "schedule_fire":
            parent = case["inputs"][0]["source_case"]
            rule = self.rules[parent]
            event = self.events[parent]
            self.events[case["id"]] = event
            due = datetime.fromisoformat(rule["next_due_at"]) + timedelta(seconds=1)
            self.app.coordinator.scheduler.due(self.actor, now=due)
            with self.store.transaction(self.actor) as tx:
                occurrence = tx.conn.execute(
                    "SELECT * FROM ga_schedule_occurrences WHERE group_id=%s AND schedule_id=%s AND work_id IS NOT NULL ORDER BY scheduled_at DESC LIMIT 1",
                    (tx.group_id, rule["id"]),
                ).fetchone()
            if not occurrence:
                raise RuntimeError("No actual scheduled occurrence was admitted")
            self._progress(
                case, phase="occurrence_admitted", event_id=event.id, work_id=occurrence["work_id"]
            )
            duplicate_before = self.counts()
            self.app.coordinator.scheduler.due(self.actor, now=due)
            duplicate_after = self.counts()
            state, decision, work = await self.wait_result(event, work_id=occurrence["work_id"])
            self._progress(
                case,
                phase="execution_finished",
                event_id=event.id,
                work_id=work.id if work else None,
                execution_state=state,
                after=self.counts(),
            )
            with self.store.transaction(self.actor) as tx:
                occurrences = tx.conn.execute(
                    "SELECT * FROM ga_schedule_occurrences WHERE group_id=%s AND schedule_id=%s",
                    (tx.group_id, rule["id"]),
                ).fetchall()
            evidence = await self.collect(
                case,
                event,
                work,
                [],
                before,
                rule_before,
                scheduling={
                    "occurrences": occurrences,
                    "works": [work.model_dump(mode="json")] if work else [],
                    "duplicate_counts_before": duplicate_before,
                    "duplicate_counts_after": duplicate_after,
                },
            )
        else:
            event, media_evidence = await self.prepare(case)
            self._progress(case, phase="received", event_id=event.id, input_media=media_evidence)
            state, decision, work = await self.wait_result(event)
            self._progress(
                case,
                phase="execution_finished",
                event_id=event.id,
                work_id=work.id if work else None,
                execution_state=state,
                after=self.counts(),
            )
            # Replay the already-persisted canonical event; it must not create
            # another rule or work and does not make another model call.
            if case["category"] == "schedule_create":
                replay_before = self.counts()
                replay = await self.app.coordinator.process(event)
                replay_after = self.counts()
                duplicate = {
                    "duplicate_counts_before": {
                        **replay_before,
                        "rules": replay_before["schedules"],
                    },
                    "duplicate_counts_after": {**replay_after, "rules": replay_after["schedules"]},
                    "duplicate": replay.duplicate,
                }
            else:
                duplicate = {}
            evidence = await self.collect(
                case, event, work, media_evidence, before, rule_before, scheduling=duplicate
            )
        evidence = json.loads(json.dumps(evidence, ensure_ascii=False, default=str))
        evidence = self._review_evidence(case, evidence)
        save(self.evidence_root / "evidence" / (case["id"] + ".json"), evidence)
        result = evaluate_case(case, evidence)
        record = {
            "id": case["id"],
            "category": case["category"],
            "execution_state": state,
            "seconds": round(time.monotonic() - started, 2),
            "evaluation": result,
            "decision": decision,
            "work_id": work.id if work else None,
            "evidence_path": str(self.evidence_root / "evidence" / (case["id"] + ".json")),
            "execution_source_sha256": self.current_source,
        }
        self.records[case["id"]] = record
        self._progress(case, phase="recorded", event_id=event.id, work_id=work.id if work else None)
        self._write_results()
        print(
            json.dumps(
                {
                    "id": case["id"],
                    "execution_state": state,
                    "evaluation": result,
                    "seconds": record["seconds"],
                },
                ensure_ascii=False,
                default=str,
            ),
            flush=True,
        )

    async def run(self):
        started = False
        try:
            for case in getattr(self, "selected_cases", self.dataset["cases"])[
                : self.args.limit or None
            ]:
                try:
                    if self.resuming and await self.resume_case(case):
                        continue
                    if self.recheck_only:
                        continue
                    if self.resuming:
                        self._assert_no_implicit_retries()
                    if not started:
                        await self.app.start()
                        started = True
                    await self.case(case)
                    if (
                        getattr(self.args, "stop_on_failure", False)
                        and self.records[case["id"]]["evaluation"]["status"] == "FAIL"
                    ):
                        # Retain the failed case and leave later cases NOT_RUN;
                        # do not spend more once a new repair is already needed.
                        break
                except Exception as error:
                    record = {
                        "id": case["id"],
                        "category": case["category"],
                        "execution_state": "error",
                        "evaluation": {
                            "status": "FAIL",
                            "error_type": type(error).__name__,
                            "error_code": getattr(error, "code", None),
                        },
                        "trace_frames": [
                            {"file": frame.filename, "line": frame.lineno, "function": frame.name}
                            for frame in traceback.extract_tb(error.__traceback__)
                        ],
                        "validation_errors": error.errors(
                            include_input=False, include_context=False, include_url=False
                        )
                        if isinstance(error, ValidationError)
                        else None,
                    }
                    self.records[case["id"]] = {**self.records.get(case["id"], {}), **record}
                    self._write_results()
                    print(json.dumps(record, ensure_ascii=False), flush=True)
                    # Harness/programming failures are different from a checked
                    # task FAIL: stop before paying for more uninspectable cases.
                    raise
        finally:
            await self.app.close()
            os.close(self._lock)
            print(json.dumps({"budget": self.ledger.report(self.actor)}, default=str), flush=True)


if __name__ == "__main__":
    raise SystemExit(
        "This historical Harness matrix is retained for evidence readers only. Use scripts/live_durable_core_acceptance.py for the standard runtime."
    )
