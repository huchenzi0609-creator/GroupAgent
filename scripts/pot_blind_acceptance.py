"""Run frozen independent Pot cases through real DeepSeek, PostgreSQL and LocalChannel."""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import io
import json
import math
import platform
import re
import subprocess
import sys
import time
import traceback
import unicodedata
from datetime import UTC, datetime, timedelta
from datetime import time as WallTime
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit
from uuid import uuid4

from markdown_it import MarkdownIt
from pot_release_gate import (
    BLIND_LOCK,
    bindings,
    blind_status,
    coverage,
    junit_status,
    observe_worker_source,
    postgres_identity,
    print_summary,
    read_json,
    sha256,
    write_json,
)
from runtime_acceptance_checks import decode_artifact

from groupagent.config import AppConfig
from groupagent.domain import DomainError, EventKind, GroupEvent, TeamTaskState, WorkState
from groupagent.persistence import PostgresStore
from groupagent.resources import MemorySnapshot
from groupagent.runtime.app import build_app
from groupagent.runtime.dispatch import Dispatcher
from groupagent.tools import ToolContext, ToolScope

OWNER = "synthetic-owner"
GUEST = "synthetic-guest"
TERMINAL = {WorkState.COMPLETED, WorkState.CANCELLED, WorkState.FAILED, WorkState.WAITING_INPUT}
FILLER = re.compile(
    r"(?:请)?(?:稍等(?:一下|片刻)?|正在处理(?:中)?|还在处理|我会尽快(?:处理|回复)?|请耐心等待)"
)
MARKUP = re.compile(r"(?m)^\s*(?:[>#]|\d+[.)、]|[-*]\s+)|\*\*|\x60\x60\x60")


def dt(value):
    return datetime.fromisoformat(value)


def pattern_all(text, patterns):
    return all(re.search(pattern, text, re.I) for pattern in patterns)


def spool_text(receipt):
    return receipt["payload"].get("text", "")


def only_text(turn):
    return [item for item in turn["receipts"] if item["payload"]["kind"] == "text"]


def response(turn):
    return "\n".join(spool_text(receipt).removeprefix("【Pot】") for receipt in only_text(turn))


def useful_text(turn, patterns=(), *, compact_whitespace=False):
    sent = only_text(turn)
    text = response(turn)
    bare = text.strip("。！!，,、 ：:；;")
    return (
        len(sent) == 1
        and bool(re.search(r"\w", bare))
        and not FILLER.fullmatch(bare)
        and pattern_all(re.sub(r"\s+", "", text) if compact_whitespace else text, patterns)
    )


def clock_display(text, expected, *, seconds_required):
    """Check a requested local clock display in colon or Chinese forms."""
    value = WallTime.fromisoformat(expected)
    text = unicodedata.normalize("NFKC", text)
    clocks = re.findall(r"(?<!\d)(\d{1,2}):(\d{2})(?::(\d{2}))?(?![\d:])", text)
    clocks += re.findall(r"(?<!\d)(\d{1,2})[点时](\d{1,2})分(?:(\d{1,2})秒)?", text)
    for hour, minute, seconds in clocks:
        if (int(hour), int(minute)) == (value.hour, value.minute) and (
            int(seconds) == value.second if seconds else not seconds_required
        ):
            return True
    return False


def schedule_native_receipts(facts, spool):
    """Join accepted occurrence delivery records to independent channel receipts."""
    native_ids = {
        delivery["platform_message_id"]
        for rule in facts
        for occurrence in rule["occurrences"]
        for delivery in occurrence["deliveries"]
        if delivery["state"] == "accepted" and delivery["platform_message_id"]
    }
    return [
        item
        for item in spool
        if item["receipt"]["state"] == "accepted"
        and item["receipt"]["platform_message_id"] in native_ids
    ]


def one_badge(turn):
    return useful_text(turn) and all(
        spool_text(row).startswith("【Pot】") and spool_text(row).count("【Pot】") == 1
        for row in only_text(turn)
    )


def accepted_by(turn, seconds=10):
    return bool(
        turn["first_receipt_seconds"] is not None and 0 <= turn["first_receipt_seconds"] <= seconds
    )


def actual_url(value, domains):
    try:
        host = urlsplit(value).hostname or ""
    except ValueError:
        return False
    return any(host == domain or host.endswith("." + domain) for domain in domains)


def official_source(url, spec):
    if not actual_url(url, spec["official_domains"]):
        return False
    patterns = spec.get("official_source_patterns", ())
    return not patterns or any(re.search(pattern, url, re.I) for pattern in patterns)


def page_url_key(value):
    if not isinstance(value, str):
        return None
    try:
        parsed = urlsplit(value.strip())
        if (
            parsed.scheme not in {"http", "https"}
            or not parsed.hostname
            or parsed.username
            or parsed.password
        ):
            return None
    except ValueError:
        return None
    return urlunsplit(
        (
            parsed.scheme,
            parsed.netloc.lower(),
            parsed.path.rstrip("/") or "/",
            parsed.query,
            "",
        )
    )


def markdown_urls(text):
    """Read link destinations separately from native Markdown delimiters."""
    urls = set()
    for token in MarkdownIt("commonmark").parse(text):
        for child in token.children or ():
            if child.type == "link_open" and (href := child.attrGet("href")):
                urls.add(href)
            elif child.type == "text":
                urls.update(
                    url.rstrip(".,，。；;：:")
                    for url in re.findall(r"https?://[^\s<>\[\]()（）]+", child.content)
                )
    return urls


def fetch_source_urls(output):
    source = output.get("source", output)
    return {
        key
        for value in (source.get("url"), source.get("final_url"))
        if (key := page_url_key(value)) is not None
    }


class Scenario:
    def __init__(self, args, store, destination, identity, spec, row):
        self.args, self.store, self.root = args, store, destination
        self.identity, self.spec, self.row = identity, spec, row
        self.group = f"local-pot-blind-{uuid4().hex}"
        self.app = None
        self.clock = None
        self.model_calls = []
        self.after_model_reply = None
        self.when = []
        self.config = AppConfig.model_validate(
            {
                "deployment_mode": "test",
                "group": {
                    "enabled": True,
                    "internal_id": self.group,
                    "external_group_id": "synthetic-only@chatroom",
                    "owner_ids": [OWNER],
                    "bot_member_id": OWNER,
                    "shared_account": True,
                    "time_zone": "Asia/Shanghai",
                },
                "conversation": {"proactive_mode": "shadow"},
                "features": {"speech_reply_on_request": False},
                "resources": {"profile": "server_2gb"},
                "spend": {
                    "group_daily_limit": None,
                    "session_total_limit": None,
                    "production_requires_explicit_limit": False,
                    "pricing_snapshot_id": "deepseek-20261114-text",
                },
                "model_routes": {
                    "text_agent": args.model,
                    "vision": args.vision_model,
                    "speech_to_text": args.asr_model,
                },
                "runtime": {
                    "channel": "local",
                    "channel_account_id": "local-blind-" + uuid4().hex,
                    "local_member_names": spec.get(
                        "room_names", {OWNER: "晓岑", GUEST: "青簌", "synthetic-peer": "豆时"}
                    ),
                    "worker_image": args.worker_image,
                    "docker_host": args.docker_host,
                },
            }
        )

    async def open(self):
        self.app = build_app(self.config, self.store, self.root / "app", self.args.secrets)
        if self.app.worker_governor is not None:
            # Deterministic memory admission test surface on the local machine.
            self.headroom = 1300
            self.app.worker_governor._sampler = lambda: MemorySnapshot(self.headroom * 1024**2)
        self.app.coordinator.clock = lambda: self.clock or datetime.now(UTC)
        original_factory = self.app.coordinator.provider_factory

        def traced(actor, event):
            provider = original_factory(actor, event)
            complete = provider.kernel.model.complete

            async def recorded(request):
                began = time.monotonic()
                record = {"source_event_id": event.id, "status": "started"}
                self.model_calls.append(record)
                try:
                    reply = await complete(request)
                    if self.after_model_reply is not None:
                        self.after_model_reply(event, reply)
                    record["tool_observations"] = [
                        json.loads(item.content)
                        for item in request.messages
                        if item.role == "tool" and item.content and item.content.startswith("{")
                    ]
                    record.update(
                        status="returned",
                        response=reply.model_dump(mode="json"),
                    )
                    # Store only synthetic request observations, without HTTP headers.
                    if len(request.messages) > 1 and request.messages[1].content:
                        try:
                            record["input_context"] = json.loads(request.messages[1].content)
                        except (TypeError, ValueError):
                            pass
                    return reply
                except Exception as error:
                    record.update(status="error", code=getattr(error, "code", type(error).__name__))
                    raise
                finally:
                    record["seconds"] = time.monotonic() - began

            provider.kernel.model.complete = recorded
            return provider

        self.app.coordinator.provider_factory = traced
        await self.app.start()

    async def close(self):
        if self.app:
            await self.app.close()

    def check(self, key, condition, *evidence):
        self.row["checks"][key] = {
            "status": "passed" if condition else "failed",
            "evidence": list(evidence),
        }

    def block(self, key, reason):
        self.row["checks"][key] = {"status": "blocked", "reason": reason, "evidence": []}

    def work_state(self):
        with self.store.transaction(self.app.owner) as tx:
            return tx.list_work()

    def tasks(self):
        with self.store.transaction(self.app.owner) as tx:
            return tx.list_team_tasks()

    def rules(self):
        return self.app.coordinator.scheduler.list(self.app.owner)

    def spool(self):
        return [
            read_json(path)
            for path in sorted((self.root / "app" / "channel" / "outgoing").glob("*.json"))
        ]

    def ledger(self, source_ids):
        if not source_ids:
            return []
        with self.store.transaction(self.app.owner) as tx:
            return tx.conn.execute(
                "SELECT id,state,source_event_id,operation,reserved_amount,actual_amount,reason,evidence_ref "
                "FROM ga_budget_reservations WHERE group_id=%s AND source_event_id=ANY(%s) "
                "ORDER BY id",
                (self.group, list(source_ids)),
            ).fetchall()

    def pending_admission(self, source_event_id):
        with self.store.transaction(self.app.owner) as tx:
            return (
                tx.conn.execute(
                    "SELECT 1 FROM ga_deliveries d "
                    "JOIN ga_decisions c ON c.group_id=d.group_id AND c.event_id=d.request_event_id "
                    "JOIN ga_work_orders w ON w.group_id=d.group_id AND w.request_event_id=c.event_id "
                    "AND w.id=c.body->'work_admission'->>'work_id' AND w.requester_id=d.created_by "
                    "WHERE d.group_id=%s AND d.request_event_id=%s AND d.idempotency_key=%s "
                    "AND d.id=c.body->'work_admission'->>'delivery_id' AND d.state='pending' LIMIT 1",
                    (self.group, source_event_id, f"early_ack:{source_event_id}:1"),
                ).fetchone()
                is not None
            )

    async def process_with_admission_receipts(self, source_event_id):
        foreground = asyncio.create_task(self.app.process_pending(batched=False))
        try:
            while not foreground.done():
                if self.pending_admission(source_event_id):
                    await self.app.dispatcher.tick()
                await asyncio.wait((foreground,), timeout=0.05)
            await foreground
        finally:
            if not foreground.done():
                foreground.cancel()
                await asyncio.gather(foreground, return_exceptions=True)
        await self.app.dispatcher.tick()

    def foreground_status(self, source_event_id):
        """Observe this source, independently of which event the last tick chose."""
        with self.store.transaction(self.app.owner) as tx:
            row = tx.conn.execute(
                "SELECT d.event_id IS NOT NULL AND d.body->>'agent_pending_response' "
                "IS DISTINCT FROM 'true' AS decided, r.blocked, r.attempts, r.code, r.next_attempt_at "
                "FROM ga_events e LEFT JOIN ga_decisions d ON d.group_id=e.group_id AND d.event_id=e.id "
                "LEFT JOIN ga_event_retry r ON r.group_id=e.group_id AND r.event_id=e.id "
                "WHERE e.group_id=%s AND e.id=%s",
                (self.group, source_event_id),
            ).fetchone()
        if row is None:
            return {"state": "source_not_persisted"}
        if row["decided"]:
            return {"state": "decided"}
        retry = (
            {
                "attempts": row["attempts"],
                "code": row["code"],
                "next_attempt_at": row["next_attempt_at"],
            }
            if row["code"]
            else None
        )
        return {"state": "blocked" if row["blocked"] else "pending", "retry": retry}

    async def process_event_until_observed(self, source_event_id, *, dispatch):
        """Drain normal foreground ticks until the requested source settles."""
        retries = {}
        observation = self.foreground_status(source_event_id)
        window = asyncio.timeout(self.args.timeout)
        try:
            async with window:
                while observation["state"] == "pending":
                    if dispatch:
                        await self.process_with_admission_receipts(source_event_id)
                    else:
                        await self.app.process_pending(batched=False)
                    observation = self.foreground_status(source_event_id)
                    retry = observation.get("retry")
                    if retry:
                        retries[retry["code"]] = retry
                    if observation["state"] == "pending":
                        await asyncio.sleep(0.05)
        except TimeoutError:
            if not window.expired():
                raise
            observation = self.foreground_status(source_event_id)
            retry = observation.get("retry")
            if retry:
                retries[retry["code"]] = retry
            if observation["state"] == "pending":
                observation["state"] = "observation_timeout"
        if dispatch:
            await self.app.dispatcher.tick()
        return {**observation, "observed_retries": list(retries.values())}

    async def event(
        self,
        key,
        *,
        actor=OWNER,
        quote=None,
        event_time=None,
        platform_id=None,
        text=None,
        attachment=None,
        process=True,
        dispatch=True,
        latency=False,
    ):
        stamp = datetime.now(UTC)
        event_id = uuid4().hex
        attachment_proof = None
        attachments = ()
        kind = EventKind.QUOTE if quote else EventKind.TEXT
        if attachment is not None:
            name, content, handle_kind = attachment
            handle = self.app.channel.add_attachment(event_id, name, content, kind=handle_kind)
            attachments = (handle.as_attachment(),)
            kind = {"image": EventKind.IMAGE, "audio": EventKind.AUDIO}.get(
                handle_kind, EventKind.FILE
            )
            attachment_proof = {
                "asset_id": handle.asset_id,
                "sha256": handle.sha256,
                "size": len(content),
            }
        record = GroupEvent(
            id=event_id,
            source_key="blind:" + event_id,
            group_id=self.group,
            actor_id=actor,
            kind=kind,
            text=self.spec[key] if text is None else text,
            occurred_at=event_time or stamp,
            received_at=stamp,
            reply_to=quote,
            channel_account_id=self.config.runtime.channel_account_id,
            source_epoch=self.config.runtime.source_epoch,
            platform_message_id=platform_id or event_id,
            mentioned_member_ids=(),
            trusted_actor_origin="agentwechat_authenticated_message_metadata",
            attachments=attachments,
        )
        self.app.channel.append_incoming(record)
        await self.app.poll_intake()
        foreground = {"state": "fixture_ingest_only"}
        if process:
            foreground = await self.process_event_until_observed(record.id, dispatch=dispatch)
        elif dispatch:
            await self.app.dispatcher.tick()
        with self.store.transaction(self.app.owner) as tx:
            event = tx.conn.execute(
                "SELECT 1 FROM ga_events WHERE group_id=%s AND id=%s", (self.group, record.id)
            ).fetchone()
            decision = tx.get_decision(record.id) if event else None
            parts = tx.conn.execute(
                "SELECT p.platform_message_id,p.state,p.intent_id,d.idempotency_key,d.accepted_at "
                "FROM ga_delivery_parts p JOIN ga_deliveries d ON d.id=p.intent_id "
                "AND d.group_id=p.group_id WHERE d.group_id=%s AND d.request_event_id=%s "
                "ORDER BY d.created_at,p.position",
                (self.group, record.id),
            ).fetchall()
        platform_ids = {row["platform_message_id"] for row in parts if row["state"] == "accepted"}
        receipts = [
            row for row in self.spool() if row["receipt"]["platform_message_id"] in platform_ids
        ]
        first_acceptance = min(
            (
                dt(row["receipt"]["observed_at"])
                for row in receipts
                if row["receipt"]["state"] == "accepted"
            ),
            default=None,
        )
        delay = (
            (first_acceptance - stamp).total_seconds()
            if first_acceptance and event_time is None
            else None
        )
        evidence = {
            "turn": key,
            "event_id": record.id,
            "occurred_at": record.occurred_at.isoformat(),
            "persisted": bool(event),
            "foreground": foreground,
            "decision": decision,
            "work_id": (decision or {}).get("result", {}).get("work_id"),
            "parts": parts,
            "receipts": receipts,
            "first_accepted_at": first_acceptance.isoformat() if first_acceptance else None,
            "first_receipt_seconds": delay,
            "latency_required": latency,
            "model_rounds": sum(call["source_event_id"] == record.id for call in self.model_calls),
            "attachment": attachment_proof,
        }
        self.row["turns"].append(evidence)
        if latency and delay is not None:
            self.when.append({"turn": key, "first_accepted_seconds": delay})
        return evidence

    async def settle(self, work_id, *, allow_delivery_hold=False):
        if not work_id:
            return None
        end = time.monotonic() + self.args.timeout
        work = None
        while time.monotonic() < end:
            await self.app.execute_tick()
            await self.app.dispatcher.tick()
            with self.store.transaction(self.app.owner) as tx:
                work = tx.get_work(work_id)
            if work and (
                work.state in TERMINAL or allow_delivery_hold and work.state == WorkState.DELIVERING
            ):
                break
            await asyncio.sleep(0.15)
        return self.proof(work) if work else None

    def proof(self, work):
        snapshot = self.app.state.load_checkpoint(self.app.owner, work.id)
        with self.store.transaction(self.app.owner) as tx:
            attempts = tx.conn.execute(
                "SELECT id,attempt_no,state,goal_revision FROM ga_execution_attempts "
                "WHERE group_id=%s AND work_id=%s ORDER BY attempt_no",
                (self.group, work.id),
            ).fetchall()
            validation = tx.conn.execute(
                "SELECT check_key,passed,evidence_ref FROM ga_work_validations "
                "WHERE group_id=%s AND work_id=%s AND goal_revision=%s ORDER BY check_key",
                (self.group, work.id, work.goal_revision),
            ).fetchall()
            parts = tx.conn.execute(
                "SELECT p.platform_message_id,p.state,p.intent_id "
                "FROM ga_delivery_parts p JOIN ga_deliveries d ON d.id=p.intent_id AND d.group_id=p.group_id "
                "WHERE d.group_id=%s AND d.work_id=%s ORDER BY d.created_at,p.position",
                (self.group, work.id),
            ).fetchall()
            artifacts = tx.conn.execute(
                "SELECT version_id,validation FROM ga_artifact_catalog "
                "WHERE group_id=%s AND work_id=%s AND shared",
                (self.group, work.id),
            ).fetchall()
        platform_ids = {row["platform_message_id"] for row in parts if row["state"] == "accepted"}
        output = {
            "work": work.model_dump(mode="json"),
            "attempts": attempts,
            "validation": validation,
            "spool": [
                item
                for item in self.spool()
                if item["receipt"]["platform_message_id"] in platform_ids
            ],
            "receipts": [item.model_dump(mode="json") for item in snapshot.receipts]
            if snapshot
            else [],
            "artifacts": artifacts,
        }
        self.row["work_proofs"].append(output)
        return output

    async def artifacts(self, proof):
        results = []
        for metadata in proof["artifacts"]:
            version, blob = self.app.inputs.registry.read(
                self.app.owner, metadata["version_id"], require_validated=True
            )
            workspace = self.root / "inspect"
            workspace.mkdir(mode=0o700, exist_ok=True)
            context = ToolContext(
                ToolScope(
                    self.group,
                    proof["work"]["id"],
                    self.app.owner,
                    workspace,
                    frozenset(),
                    "blind-inspection",
                ),
                uuid4().hex,
                "blind-inspection",
                asyncio.Event(),
            )
            decoded = await decode_artifact(self.app.inputs.worker, context, version.format, blob)
            accepted = next(
                (
                    item["payload"]["sha256"]
                    for item in proof["spool"]
                    if item["payload"].get("sha256") == version.sha256
                ),
                None,
            )
            results.append(
                {
                    "version_id": version.version_id,
                    "format": version.format,
                    "sha256": hashlib.sha256(blob).hexdigest(),
                    "accepted_sha256": accepted,
                    "decoded": decoded,
                    "provenance": version.provenance,
                    "validation": metadata["validation"],
                }
            )
        return results

    def record_read_only_fixture(self, event, rationale):
        with self.store.transaction(self.app.owner) as tx:
            tx.record_decision(event["event_id"], {"kind": "observe", "reason_code": rationale})

    async def fault_dispatch(self):
        async def lose_receipt(*_):
            raise RuntimeError("blind_injected_after_submit")

        faulted = Dispatcher(
            self.app.state,
            self.app.technical,
            self.app.resolve_actor,
            self.app.channel,
            self.app.coordinator.artifacts,
            after_submit=lose_receipt,
        )
        try:
            await faulted.tick()
        except RuntimeError as error:
            if str(error) != "blind_injected_after_submit":
                raise
            return True
        return False

    def accepted_native_quote(self, turn):
        return only_text(turn)[0]["receipt"]["platform_message_id"]

    def snapshot(self, rules=None):
        return [rule.model_dump(mode="json") for rule in (self.rules() if rules is None else rules)]

    def occurrence_proof(self, rule):
        return [
            item.model_dump(mode="json")
            for item in self.app.coordinator.scheduler.facts(self.app.owner, schedule_id=rule.id)
        ]

    async def wake(self, instant):
        self.clock = instant
        result = self.app.coordinator.scheduler.wake(self.app.owner, instant)
        await self.app.dispatcher.tick()
        return result


async def chat01(s):
    first = await s.event("name", latency=True)
    second = await s.event("followup", latency=True)
    s.check(
        "useful_single_replies",
        useful_text(first, s.spec["name_patterns"])
        and useful_text(second, s.spec["followup_patterns"], compact_whitespace=True),
        first,
        second,
    )
    s.check("single_badge", one_badge(first) and one_badge(second), first, second)
    s.check(
        "no_work_or_filler",
        not s.work_state() and useful_text(first) and useful_text(second),
        {"work_count": len(s.work_state())},
        first,
        second,
    )
    s.check(
        "latency",
        accepted_by(first) and accepted_by(second) and useful_text(first) and useful_text(second),
        first,
        second,
    )


async def chat02(s):
    discussion = await s.event("discussion", actor=GUEST)
    peer = await s.event("peer_question", actor="synthetic-peer", quote=discussion["event_id"])
    addressed = await s.event("addressed", latency=True)
    continuation = await s.event("continuation")
    quote = (
        await s.event("quote_question", quote=s.accepted_native_quote(addressed))
        if only_text(addressed)
        else {}
    )
    chatter = await s.event("unrelated", actor="synthetic-peer", quote=discussion["event_id"])
    s.check(
        "human_conversation_silent_with_context",
        all(row["persisted"] and not row["receipts"] for row in (discussion, peer, chatter)),
        discussion,
        peer,
        chatter,
    )
    s.check(
        "natural_and_quoted_continuations",
        all(
            row and useful_text(row, s.spec["patterns"]) for row in (addressed, continuation, quote)
        ),
        addressed,
        continuation,
        quote or {"quote": "no original native receipt"},
    )
    s.check(
        "no_unrelated_effect",
        not s.work_state() and not s.rules() and not s.tasks(),
        {"works": len(s.work_state()), "rules": len(s.rules()), "tasks": len(s.tasks())},
    )


async def chat03(s):
    first = await s.event("first", dispatch=False)
    fault = await s.fault_dispatch()
    after_submit = s.spool()
    await s.close()
    await s.open()
    await s.app.dispatcher.tick()
    after_restart = s.spool()
    if len(after_restart) != 1:
        raise AssertionError("expected exactly one reconciled wire submission")
    wire = after_restart[0]
    native_id = wire["receipt"]["platform_message_id"]
    echo = await s.event("human_copy", text=spool_text(wire), platform_id=native_id)
    repeat = await s.event("human_copy", text=spool_text(wire), platform_id=native_id)
    human = await s.event("human_copy")
    s.check(
        "human_text_reaches_model",
        human["model_rounds"] >= 1 and useful_text(human, [s.spec["patterns"][1]]),
        human,
    )
    s.check(
        "verified_echo_skips_model",
        all(not row["receipts"] and not row["model_rounds"] for row in (echo, repeat)),
        echo,
        repeat,
    )
    s.check(
        "uncertain_send_reconciled_once",
        fault and after_submit == after_restart,
        first,
        {"injected_after_submit": fault, "spool": after_restart},
    )
    receipt = await s.app.channel.lookup_delivery(s.group, wire["receipt"]["idempotency_key"])
    s.check(
        "exact_delivery_receipt",
        receipt is not None
        and receipt.state == "accepted"
        and receipt.platform_message_id == native_id
        and s.spec["patterns"][0] in spool_text(wire),
        {"wire": wire, "lookup": receipt.model_dump(mode="json") if receipt else None},
    )


def categorized_group_summary(turn, title_patterns):
    member_assignment = r"分工|(?:组员|成员)(?:认领|分配|分派|任务|职责)"
    return useful_text(turn, [*title_patterns, member_assignment, "提醒", "我|Pot"])


async def group02(s):
    claim = await s.event("claim")
    initial = s.tasks()
    progress = await s.event("progress")
    updated = s.tasks()
    tentative = await s.event("tentative", actor=GUEST)
    after = s.tasks()
    summary = await s.event("list", latency=True)
    same = bool(len(initial) == len(updated) == 1 and initial[0].id == updated[0].id)
    s.check(
        "claim_and_progress_one_owner_record",
        same
        and updated[0].owner_id == OWNER
        and pattern_all(updated[0].title, s.spec["title_patterns"])
        and pattern_all(updated[0].progress, s.spec["progress_patterns"])
        and claim["event_id"] in updated[0].source_event_ids
        and progress["event_id"] in updated[0].source_event_ids
        and updated[0].revision == initial[0].revision + 1
        and progress["event_id"] in updated[0].field_sources.get("progress", []),
        claim,
        progress,
        {"tasks": [task.model_dump(mode="json") for task in updated]},
    )
    s.check(
        "tentative_comment_no_write",
        updated == after and not tentative["receipts"],
        tentative,
        {"task_ids": [task.id for task in after]},
    )
    s.check(
        "lists_separate_types",
        categorized_group_summary(summary, s.spec["title_patterns"])
        and not s.work_state()
        and not s.rules(),
        summary,
    )


async def work01(s):
    example = await s.event("example", latency=True)
    count = s.work_state()
    s.check(
        "example_without_execution",
        not count and useful_text(example, ["sum|for|偶数"]),
        example,
        {"work_count": len(count)},
    )
    executed = await s.event("execute", latency=True)
    output = await s.settle(executed["work_id"])
    if not output:
        return
    successful = [
        receipt
        for receipt in output["receipts"]
        if receipt["tool_name"] == "python_compute" and receipt["success"]
    ]
    gold = s.spec["expected_number"]
    correct = any(
        gold in json.dumps(receipt["output"], ensure_ascii=False) for receipt in successful
    )
    s.check("actual_python_stdout", correct, output)
    s.check(
        "work_and_delivery_receipts",
        correct
        and output["work"]["state"] == "completed"
        and len(output["attempts"]) > 0
        and any(item["passed"] for item in output["validation"])
        and any(gold in spool_text(row) for row in output["spool"]),
        executed,
        output,
    )


async def time01(s):
    anchor = dt(s.spec["anchor"])
    due = dt(s.spec["due"])
    s.clock = anchor - timedelta(seconds=3)
    await s.event("lead", event_time=s.clock)
    s.clock = anchor + timedelta(seconds=1)
    request = await s.event("request", event_time=anchor)
    rules = s.rules()
    exact = len(rules) == 1 and rules[0].timing.kind == "at" and rules[0].timing.at == due
    s.check(
        "one_due_from_event_anchor_and_shanghai_display",
        exact
        and rules[0].time_zone == "Asia/Shanghai"
        and clock_display(response(request), s.spec["due"][11:19], seconds_required=True)
        and pattern_all(response(request), [r"Asia/Shanghai|北京时间|上海时间|\+08:00"]),
        request,
        {"rules": s.snapshot(rules)},
    )
    if not exact:
        return
    rule = rules[0]
    original_rule_ids = {item.id for item in rules}
    before = await s.event("status_before", event_time=s.clock)
    rule_ids_after_pre_due_query = {item.id for item in s.rules()}
    before_fact = s.occurrence_proof(rule)
    model_count = len(s.model_calls)
    first_wake = await s.wake(due + timedelta(seconds=1))
    await s.wake(due + timedelta(seconds=2))
    delivered_without_model = len(s.model_calls) == model_count
    facts = s.occurrence_proof(rule)
    reminder = schedule_native_receipts(facts, s.spool())
    rule_ids_before_post_due_query = {item.id for item in s.rules()}
    after = await s.event("status_after", event_time=s.clock)
    rule_ids_after_post_due_query = {item.id for item in s.rules()}
    unchanged_rules = (
        original_rule_ids
        == rule_ids_after_pre_due_query
        == rule_ids_before_post_due_query
        == rule_ids_after_post_due_query
    )
    rule_id_evidence = {
        "original": sorted(original_rule_ids),
        "after_pre_due_query": sorted(rule_ids_after_pre_due_query),
        "before_post_due_query": sorted(rule_ids_before_post_due_query),
        "after_post_due_query": sorted(rule_ids_after_post_due_query),
    }
    before_reads = foreground_observations(s, before)
    after_reads = foreground_observations(s, after)
    s.check(
        "status_before_and_after",
        unchanged_rules
        and bool(before_fact and not before_fact[0]["occurrences"])
        and useful_text(before, ["尚未|还没|未发送|没发|未投递|未到"])
        and clock_display(
            response(before),
            s.spec["initial_current_time"],
            seconds_required=s.spec.get("seconds_required", False),
        )
        and clock_display(response(before), s.spec["due"][11:19], seconds_required=False)
        and useful_text(after, ["(?:已|成功).{0,15}(?:发送|发出|投递|送达)|回执"])
        and not pattern_all(response(after), ["尚未|还没|未发送|没发|未投递"])
        and all(
            any(
                read.get("kind") == "read_group_context"
                and read.get("resource") == "schedules"
                and read.get("status") == "observed"
                for read in reads
            )
            for reads in (before_reads, after_reads)
        ),
        before,
        after,
        {
            "before_facts": before_fact,
            "after_facts": facts,
            "before_reads": before_reads,
            "after_reads": after_reads,
            "rule_ids": rule_id_evidence,
        },
    )
    body = spool_text(reminder[0]).removeprefix("【Pot】") if len(reminder) == 1 else ""
    correct_body = (
        body == s.spec["payload"] and rule.execution_kind.value == "explicit_reminder"
        if s.spec.get("verbatim")
        else pattern_all(body, s.spec["payload_patterns"])
    )
    occurrences = facts[0]["occurrences"] if facts else []
    s.check(
        "one_accepted_reminder",
        unchanged_rules
        and delivered_without_model
        and len(first_wake) == len(occurrences) == len(reminder) == 1
        and first_wake[0].work_id is None
        and dt(occurrences[0]["scheduled_at"]) == due
        and len(occurrences[0]["deliveries"]) == 1
        and correct_body,
        {
            "schedule_facts": facts,
            "local_receipts": reminder,
            "model_calls_at_due": not delivered_without_model,
            "rule_ids": rule_id_evidence,
        },
    )


async def time02(s):
    anchor = dt(s.spec["anchor"])
    s.clock = anchor + timedelta(seconds=1)
    midnight = await s.event("midnight", event_time=anchor)
    half_at = anchor + timedelta(seconds=4)
    s.clock = half_at + timedelta(seconds=1)
    half = await s.event("half", event_time=half_at)
    before = s.rules()
    half_due = half_at + timedelta(minutes=s.spec["half_minutes"])
    half_rule = next((r for r in before if r.next_due_at == half_due), None)
    s.clock += timedelta(seconds=2)
    shift = await s.event("shift", event_time=s.clock)
    shifted = s.rules()
    s.check(
        "natural_offsets_and_cross_midnight",
        any(rule.next_due_at == dt(s.spec["midnight_due"]) for rule in before)
        and s.spec["midnight_due"][11:19] in response(midnight)
        and half_rule is not None
        and any(
            r.id == half_rule.id
            and r.next_due_at == half_due + timedelta(minutes=s.spec["shift_minutes"])
            for r in shifted
        ),
        midnight,
        half,
        shift,
        {"before": s.snapshot(before), "after": s.snapshot(shifted)},
    )
    s.clock = anchor + timedelta(minutes=13)
    late = await s.event("late", event_time=anchor - timedelta(seconds=15))
    s.check(
        "late_event_not_reanchored",
        {r.id: r for r in shifted} == {r.id: r for r in s.rules()}
        and useful_text(late, ["已经|过了|错过|来不及|未安排|无法登记"]),
        late,
        {"rules": s.snapshot()},
    )
    before_ambiguity = {r.id: r for r in s.rules()}
    ambiguous = await s.event("ambiguous", event_time=s.clock)
    discussion = await s.event("discussion", actor=GUEST, event_time=s.clock)
    s.check(
        "ambiguity_and_discussion_no_rule",
        before_ambiguity == {r.id: r for r in s.rules()}
        and useful_text(ambiguous, ["九点|9点|09:00|9:00", "三点|3点|15"])
        and not any(read.get("committed") for read in foreground_observations(s, ambiguous))
        and not discussion["receipts"],
        ambiguous,
        discussion,
        {"rules": s.snapshot()},
    )


async def work04(s):
    if not s.app.worker_governor:
        return
    s.headroom = s.spec["load_mb"][0]
    work = await s.event("work", latency=True)
    await s.app.execute_tick()
    await s.app.dispatcher.tick()
    quick = await s.event("quick", latency=True)
    s.headroom = s.spec["load_mb"][1]
    output = await s.settle(work["work_id"])
    s.check(
        "healthy_chat_latency_during_work",
        useful_text(quick, s.spec["quick_patterns"]) and accepted_by(quick),
        quick,
    )
    s.check(
        "one_useful_admission",
        bool(work["work_id"])
        and accepted_by(work)
        and useful_text(work, ["PDF|青岸|标本"])
        and not any(FILLER.search(spool_text(row)) for row in (output or {}).get("spool", [])),
        work,
        output or {"execution": "missing"},
    )
    if output:
        s.check(
            "fault_recovery_preserves_version",
            output["work"]["id"] == work["work_id"]
            and output["work"]["goal_revision"] == 1
            and output["work"]["state"] == "completed"
            and len(s.work_state()) == 1,
            output,
        )
        file_receipts = [item for item in output["spool"] if item["payload"]["kind"] == "file"]
        s.check(
            "valid_result_accepted",
            output["work"]["state"] == "completed"
            and len(file_receipts) == 1
            and any(item["passed"] for item in output["validation"])
            and bool(output["artifacts"]),
            output,
        )


def short_condensation_reply(turn, topic_patterns):
    text = re.sub(r"冰凉|低温|温度(?:较|偏|很|相对)?低", "冷", response(turn))
    return useful_text(turn) and pattern_all(text, topic_patterns)


async def chat04(s):
    topic = await s.event("topic", actor=GUEST)
    short = await s.event("short", latency=True)
    s.check(
        "brief_plain_text",
        not topic["receipts"]
        and not topic["work_id"]
        and short_condensation_reply(short, s.spec["short_patterns"])
        and len(response(short)) <= 100
        and MARKUP.search(response(short)) is None
        and "？" not in response(short),
        topic,
        short,
    )
    long = await s.event("long", latency=True)
    work = await s.settle(long["work_id"])
    if not work:
        return
    files = await s.artifacts(work)
    markdown = next((artifact for artifact in files if artifact["format"] == "md"), None)
    if not markdown:
        return
    body = markdown["decoded"].get("text", "")
    urls = {page_url_key(url) for url in markdown_urls(body)}
    fetched_pages = {
        url
        for receipt in successful_tools(work, {"web_fetch", "browser_open"})
        for url in fetch_source_urls(receipt["output"])
    }
    verified_official = {
        url for url in urls & fetched_pages if url and official_source(url, s.spec)
    }
    s.check(
        "long_complete_cited_artifact",
        all(marker in body for marker in s.spec["long_markers"])
        and len(re.findall(r"[\u3400-\u9fff]", body)) >= s.spec["long_min_han"]
        and bool(verified_official),
        work,
        markdown,
        {"official_pages_shared_by_output_and_fetch_receipt": sorted(verified_official)},
    )
    s.check("accepted_bytes", markdown["accepted_sha256"] == markdown["sha256"], markdown)


async def chat05(s):
    request = await s.event("work", latency=True)
    # Use a pending legacy progress notice to test suppression across policy changes.
    started = None
    progress_id = None
    body = "例行后台进度提示"
    if request["work_id"]:
        with s.store.transaction(s.app.owner) as tx:
            started = tx.get_work(request["work_id"])
            progress_id = tx._insert_delivery(
                None,
                "text:" + body,
                hashlib.sha256(body.encode()).hexdigest(),
                "blind-progress:" + started.id,
            ).id
            tx.conn.execute(
                "INSERT INTO ga_progress_notices(group_id,work_id,notice_key,intent_id) VALUES(%s,%s,%s,%s)",
                (s.group, started.id, "recovery:blind", progress_id),
            )
    before = s.spool()
    quiet = await s.event("quiet")
    discussion = await s.event("discussion", actor=GUEST)
    after = s.spool()
    with s.store.transaction(s.app.owner) as tx:
        quiet_policy = tx.conn.execute(
            "SELECT quiet,quiet_since_sequence FROM ga_conversation_policy "
            "WHERE group_id=%s AND subject_id='*'",
            (s.group,),
        ).fetchone()
        quiet_notice = tx.get_delivery(progress_id) if progress_id else None
    s.check(
        "immediate_silence",
        bool(quiet_policy and quiet_policy["quiet"] and quiet_policy["quiet_since_sequence"])
        and not quiet["receipts"]
        and not discussion["receipts"]
        and before == after,
        quiet,
        discussion,
        {"native_count": len(after), "group_policy": quiet_policy},
    )
    quiet_phase = await s.settle(request["work_id"], allow_delivery_hold=True)
    result = quiet_phase
    lift = None
    ready_to_publish = bool(quiet_phase and quiet_phase["work"]["state"] == "delivering")
    if ready_to_publish or quiet_notice and quiet_notice.state.value != "rejected":
        lift = await s.event("lift_quiet")
        if ready_to_publish:
            result = await s.settle(request["work_id"])
    if progress_id:
        with s.store.transaction(s.app.owner) as tx:
            progress = tx.get_delivery(progress_id).model_dump(mode="json")
            accepted_parts = tx.conn.execute(
                "SELECT platform_message_id FROM ga_delivery_parts "
                "WHERE group_id=%s AND intent_id=%s AND state='accepted'",
                (s.group, progress_id),
            ).fetchall()
        s.check(
            "pending_notice_retired",
            progress["state"] == "rejected"
            and not progress["platform_message_id"]
            and not accepted_parts
            and not any(spool_text(item).removeprefix("【Pot】") == body for item in s.spool()),
            {
                "quiet_notice": quiet_notice.model_dump(mode="json") if quiet_notice else None,
                "final_notice": progress,
                "accepted_parts": accepted_parts,
            },
            lift or {"lift": "not required"},
        )
    artifact_rows = (
        await s.artifacts(result) if result and result["work"]["state"] == "completed" else []
    )
    work_file_receipts = [
        item for item in (result or {}).get("spool", []) if item["payload"]["kind"] == "file"
    ]
    s.check(
        "authorized_work_retained",
        bool(
            started
            and quiet_phase
            and result
            and quiet_phase["work"]["state"] in {"delivering", "completed"}
            and any(item["passed"] for item in quiet_phase["validation"])
            and result["work"]["state"] == "completed"
            and result["work"]["id"] == quiet_phase["work"]["id"] == started.id
            and result["work"]["goal_revision"]
            == quiet_phase["work"]["goal_revision"]
            == started.goal_revision
            and len(s.work_state()) == 1
            and len(work_file_receipts) == 1
            and len(artifact_rows) == 1
            and artifact_rows[0]["format"] == "pdf"
            and artifact_rows[0]["accepted_sha256"] == artifact_rows[0]["sha256"]
            and artifact_rows[0]["decoded"].get("unit_count") == 1
            and all(
                marker in artifact_rows[0]["decoded"].get("text", "")
                for marker in s.spec["expected_pdf_text"]
            )
        ),
        request,
        quiet_phase or {"quiet_phase": "no verified result or delivery hold"},
        lift or {"lift": "no held delivery"},
        result or {"completion": "missing"},
        {"actual_artifacts": artifact_rows, "native_file_receipts": work_file_receipts},
    )


async def group01(s):
    members = s.app.coordinator.members
    missed = await s.event(
        "missing_roster_identity",
        actor=s.spec["missing_id"],
        text="例行合成群成员观察",
        process=False,
    )
    s.record_read_only_fixture(missed, "blind_roster_seed")
    initial = {
        identity: members.get(s.app.owner, identity).model_dump(mode="json")
        for identity in [*s.spec["room_names"], s.spec["missing_id"]]
    }
    answer = await s.event("initial", latency=True)
    s.check(
        "initial_room_roster",
        useful_text(answer, [s.spec["room_names"][OWNER]]) and not s.work_state(),
        answer,
        {"members": initial},
    )
    # Verify the same display name belongs to two stable, distinct members.
    names = [record.get("display_name") or record.get("name") for record in initial.values()]
    s.check(
        "stable_missing_and_duplicate_names",
        names.count(s.spec["room_names"][GUEST]) == 2
        and initial[GUEST]["member_id"] != initial["synthetic-twin"]["member_id"]
        and initial[s.spec["missing_id"]]["member_id"] == s.spec["missing_id"],
        {"members": initial},
    )
    manual = await s.event("manual")
    after_manual = {
        identity: members.get(s.app.owner, identity).model_dump(mode="json") for identity in initial
    }
    changed = {identity for identity in initial if initial[identity] != after_manual[identity]}
    s.check(
        "single_manual_update",
        changed == {s.spec["missing_id"]}
        and s.spec["manual_name"] in str(after_manual[s.spec["missing_id"]]),
        manual,
        {"changed": sorted(changed), "members": after_manual},
    )
    refresh_calls = []

    async def refresh(group_id, *, timeout_seconds):
        refresh_calls.append({"group": group_id, "timeout": timeout_seconds})
        s.app.channel.members_fixture = tuple(
            value.model_copy(update={"room_display_name": s.spec["renamed"]})
            if value.member_id == OWNER
            else value
            for value in s.app.channel.members_fixture
        )

    # The fixed roster transport stands in for the local group roster changing.
    s.app.channel.refresh_members = refresh
    await s.app.roster.sync_pending()
    idle = len(refresh_calls)
    updated = await s.event("refresh")
    after_refresh = {
        identity: members.get(s.app.owner, identity).model_dump(mode="json") for identity in initial
    }
    manual_fields = ("confirmed_name", "confirmed_by")
    s.check(
        "on_demand_refresh_only",
        idle == 0
        and len(refresh_calls) == 1
        and useful_text(updated, [re.escape(s.spec["renamed"])])
        and after_refresh[OWNER]["room_name"] == s.spec["renamed"]
        and all(
            after_refresh[identity].get(field) == after_manual[identity].get(field)
            for identity in initial
            for field in manual_fields
        )
        and not any(item.get("committed") for item in foreground_observations(s, updated))
        and not ((updated.get("decision") or {}).get("result", {}).get("action_result")),
        updated,
        {
            "synthetic_roster_transport_calls": refresh_calls,
            "members_before_refresh": after_manual,
            "members_after_refresh": after_refresh,
        },
    )


async def group03(s):
    claim = await s.event("claim")
    retired = await s.event("sensitive", process=False)
    s.app.coordinator.memory.forget(s.app.owner, event_ids=[retired["event_id"]])
    foreign_group = "foreign-blind-" + uuid4().hex
    s.store.register_group(foreign_group)
    foreign_actor = s.app.owner.model_copy(update={"group_id": foreign_group})
    foreign_id = uuid4().hex
    now = datetime.now(UTC)
    foreign_event = GroupEvent(
        id=foreign_id,
        source_key="foreign:" + foreign_id,
        group_id=foreign_group,
        actor_id=OWNER,
        kind=EventKind.TEXT,
        text=s.spec["foreign"],
        occurred_at=now,
        received_at=now,
    )
    with s.store.transaction(foreign_actor) as tx:
        tx.ingest_event(foreign_event, "blind-foreign", "1", 0)
        tx.create_team_task(s.spec["shared_title"], source_event_id=foreign_id)
    for index in range(s.spec["filler_count"]):
        filler = await s.event(
            "filler", actor=GUEST, text=s.spec["filler"].format(index), process=False
        )
        s.record_read_only_fixture(filler, "blind_compaction_filler")
    summary = await s.app.coordinator.memory.compact(s.app.owner, force=True, token_budget=8000)
    checkpoint = s.app.coordinator.memory.checkpoint(s.app.owner)
    # Leave irrelevant filler in the actual history. It need not consume real
    # inference calls just to fill a compaction window.
    await s.close()
    await s.open()
    recalled = await s.event("recall", latency=True)
    tasks = s.tasks()
    owner_name = s.app.coordinator.members.get(s.app.owner, OWNER).display_name
    recall_patterns = [
        s.spec["wanted"],
        re.escape(owner_name) + "|你(?:是|来|负责|接手|认领|担任|已)|(?:是|由|给|负责人[：:是]?)你",
    ]
    s.check(
        "long_history_and_restart_recall",
        summary is not None
        and checkpoint["through_sequence"] > 0
        and useful_text(recalled, recall_patterns)
        and any(
            task.owner_id == OWNER and claim["event_id"] in task.source_event_ids for task in tasks
        ),
        recalled,
        {"checkpoint": checkpoint, "compacted": summary is not None},
    )
    s.check(
        "structured_facts_provenance",
        any(
            task.owner_id == OWNER
            and claim["event_id"] in task.source_event_ids
            and s.spec["shared_title"] in task.title
            for task in tasks
        ),
        claim,
        {"tasks": [task.model_dump(mode="json") for task in tasks]},
    )
    observed = [item for item in s.model_calls if item["source_event_id"] == recalled["event_id"]]
    combined = response(recalled) + json.dumps(observed, ensure_ascii=False)
    s.check(
        "withdrawn_and_foreign_absent",
        bool(observed)
        and not any(value in combined for value in s.spec["excluded"])
        and len(s.work_state()) == 0,
        recalled,
        {"provider_rounds": len(observed)},
    )


def successful_tools(proof, names):
    return [item for item in proof["receipts"] if item["success"] and item["tool_name"] in names]


def foreground_tools(s, event):
    return [
        call
        for item in s.model_calls
        if item["source_event_id"] == event["event_id"]
        for call in (item.get("response") or {}).get("tool_calls", [])
    ]


def work_facts_observed(s, turn):
    facts = []
    for observation in foreground_observations(s, turn):
        kind, status = observation.get("kind"), observation.get("status")
        if kind == "read_work_facts" and status == "matched":
            facts.extend(observation.get("facts", []))
        elif (
            kind == "control_work"
            and status == "observed"
            and observation.get("code") == "work_control_state_observed"
            and observation.get("committed") is False
            and not observation.get("replayed")
            and not observation.get("work_effects")
        ):
            snapshot = observation.get("facts")
            if not isinstance(snapshot, dict) or snapshot.get("status") != "matched":
                continue
            observed_facts = snapshot.get("facts")
            if isinstance(observed_facts, list) and len(observed_facts) == 1:
                facts.extend(observed_facts)
    return facts


def work_fact_has(facts, work_id, work_rows):
    expected = {
        item["id"]: item.get("goal_revision")
        for work in work_rows
        if (item := (work if isinstance(work, dict) else work.model_dump(mode="json")))
    }
    return work_id in expected and any(
        isinstance(observed, dict)
        and observed.get("id") == work_id
        and observed.get("goal_revision") == expected[work_id]
        for observed in facts
    )


def quoted_work_context(observations, source_event_id, work_id):
    """Use host-projected facts only when the current native quote binds the work."""
    facts = []
    for observation in observations:
        if observation["source_event_id"] != source_event_id:
            continue
        context = observation.get("input_context", {}).get("context", {})
        routing = context.get("trusted_routing") or {}
        if (
            routing.get("source") != "bot_reply"
            or routing.get("recipient") != "agent"
            or context.get("quoted_work_id") != work_id
        ):
            continue
        facts.extend((context.get("work_facts_overview") or {}).get("facts", []))
        facts.extend(context.get("works") or [])
    return facts


def queued_progress_subjects(turn, facts, work):
    """Prove a queued reply references the observed work's subject or actual output."""
    accepted_text = response(turn)
    if not useful_text(turn) or not re.search(
        r"排队|待执行|等待开始|尚未开始|还没开始", accepted_text
    ):
        return set()
    if work is None:
        return set()

    def terms(text):
        return set(
            re.findall(r"(?<![A-Za-z0-9])(?:[a-z][a-z0-9-]{2,})(?![A-Za-z0-9])", text.lower())
        )

    common_words = {
        "pdf",
        "word",
        "markdown",
        "docx",
        "pptx",
        "python",
        "http",
        "https",
        "www",
        "com",
        "org",
        "html",
        "json",
        "the",
        "and",
        "for",
    }
    planned_formats = {
        item.get("format") if isinstance(item, dict) else item
        for item in (getattr(work, "scope", None) or {}).get("outputs", [])
    }
    seen = set()
    unproduced = set()
    for fact in facts:
        if (
            fact.get("state") == "queued"
            and work_fact_has([fact], work.id, [work])
            and isinstance(fact.get("goal"), str)
        ):
            seen.update(terms(fact["goal"]) & terms(work.goal) & terms(accepted_text))
            actual_outputs = fact.get("outputs")
            if (
                "pdf" in planned_formats
                and isinstance(actual_outputs, dict)
                and actual_outputs.get("candidate_count")
                == actual_outputs.get("validated_count")
                == 0
                and re.search(
                    r"(?:尚未|没有|还没|未).{0,20}(?:生成|成稿|交付|产出).{0,12}PDF",
                    accepted_text,
                    re.I,
                )
            ):
                unproduced.add("PDF")
    return (seen - common_words) | unproduced


async def work02(s):
    empty = await s.event("empty", latency=True)
    empty_read = foreground_observations(s, empty)
    empty_overviews = [
        overview
        for observation in s.model_calls
        if observation["source_event_id"] == empty["event_id"]
        and isinstance(
            overview := observation.get("input_context", {})
            .get("context", {})
            .get("work_facts_overview"),
            dict,
        )
        and overview.get("complete") is True
        and overview.get("facts") == []
    ]
    s.check(
        "empty_state_honest",
        useful_text(empty, ["没有|暂无|空|没有在|无|当前没"])
        and not s.work_state()
        and (
            bool(empty_overviews)
            or any(
                item.get("kind") == "read_work_facts" and item.get("status") == "empty"
                for item in empty_read
            )
        ),
        empty,
        {"work_fact_observations": empty_read, "complete_empty_host_overviews": empty_overviews},
    )
    first = await s.event("one", latency=True)
    second = await s.event("two", latency=True)
    initial = s.work_state()
    await s.event("chatter", actor=GUEST)
    progress = await s.event("progress", latency=True)
    after_progress = s.work_state()
    read_only_progress = [(work.id, work.revision) for work in initial] == [
        (work.id, work.revision) for work in after_progress
    ]
    progress_facts = work_facts_observed(s, progress)
    progress_work = next((work for work in initial if work.id == second["work_id"]), None)
    progress_subjects = queued_progress_subjects(progress, progress_facts, progress_work)
    output = await s.settle(first["work_id"])
    old = None
    if output and output["spool"]:
        old = await s.event("old", quote=output["spool"][-1]["receipt"]["platform_message_id"])
    old_observed_facts = work_facts_observed(s, old) if old else []
    old_quote_facts = (
        quoted_work_context(s.model_calls, old["event_id"], first["work_id"]) if old else []
    )
    fresh = await s.event("new", latency=True)
    same_work = bool(
        first["work_id"] and second["work_id"] and first["work_id"] != second["work_id"]
    )
    s.check(
        "same_version_for_progress_and_quote",
        same_work
        and read_only_progress
        and (useful_text(progress, s.spec["progress_patterns"]) or bool(progress_subjects))
        and old is not None
        and useful_text(old, s.spec["old_patterns"])
        and output is not None
        and output["work"]["state"] == "completed"
        and work_fact_has(progress_facts, second["work_id"], initial)
        and work_fact_has(
            [*old_observed_facts, *old_quote_facts], first["work_id"], [output["work"]]
        ),
        first,
        second,
        progress,
        old or {"quote": "no original receipt"},
        output or {"execution": "missing"},
        {
            "quoted_host_facts": old_quote_facts,
            "read_work_facts": old_observed_facts,
            "grounded_progress_subjects": sorted(progress_subjects),
        },
    )
    s.check(
        "no_write_from_lookup",
        read_only_progress
        and not any(turn["work_id"] for turn in (empty, fresh))
        and progress["work_id"] in (None, second["work_id"]),
        empty,
        progress,
        fresh,
    )
    s.check(
        "fresh_topic_without_old_work",
        useful_text(fresh, s.spec["fresh_patterns"])
        and not any(word in response(fresh) for word in s.spec["forbidden_old_in_new"]),
        fresh,
    )


def work_map(s):
    return {work.id: work.model_dump(mode="json") for work in s.work_state()}


async def work03(s):
    seeds = []
    states = ("paused", "running", "completed", "paused")
    for title, state in zip(s.spec["seed_titles"], states, strict=True):
        event = await s.event("seed", text=title, process=False)
        with s.store.transaction(s.app.owner) as tx:
            work = tx.create_work(title, event["event_id"])
            tx.conn.execute(
                "UPDATE ga_work_orders SET state=%s WHERE group_id=%s AND id=%s",
                (state, s.group, work.id),
            )
        s.record_read_only_fixture(event, "blind_work_state_seed")
        seeds.append(work.id)
    before = work_map(s)
    request = await s.event("request")
    after = work_map(s)
    modified = {identity for identity in before if before[identity] != after[identity]}
    s.check(
        "exact_authorized_cancel_set",
        modified == {seeds[0], seeds[3]}
        and all(after[k]["state"] == "cancelled" for k in modified),
        request,
        {"before": before, "after": after},
    )

    async def seed_paused(title):
        event = await s.event("seed", text=title, process=False)
        s.record_read_only_fixture(event, "blind_paused_race_source")
        with s.store.transaction(s.app.owner) as tx:
            item = tx.create_work(title, event["event_id"])
            return tx.transition_work(
                item.id, WorkState.PAUSED, expected_revision=item.revision, reason="human_paused"
            )

    victim = await seed_paused(s.spec["seed_titles"][0])
    other = await seed_paused(s.spec["seed_titles"][3])
    injected = []

    def resume_after_model_selection(event, model_reply):
        if injected or not any(call.name == "control_work" for call in model_reply.tool_calls):
            return
        with s.store.transaction(s.app.owner) as tx:
            current = tx.get_work(victim.id)
            resumed = tx.transition_work(
                victim.id,
                WorkState.QUEUED,
                expected_revision=current.revision,
                reason="external_resume",
            )
            injected.append(resumed.model_dump(mode="json"))

    s.after_model_reply = resume_after_model_selection
    try:
        race = await s.event("race_request")
    finally:
        s.after_model_reply = None
    after_race = work_map(s)
    race_tools = foreground_tools(s, race)
    controls = [index for index, item in enumerate(race_tools) if item["name"] == "control_work"]
    reread = bool(
        controls
        and any(item["name"] == "read_work_facts" for item in race_tools[controls[0] + 1 :])
    )
    s.check(
        "revision_race_reread",
        bool(injected)
        and after_race[victim.id] == injected[0]
        and after_race[other.id]["state"] == "cancelled"
        and reread,
        race,
        {
            "injected_revision": injected,
            "after": after_race,
            "tool_names": [item["name"] for item in race_tools],
        },
    )

    await seed_paused(s.spec["seed_titles"][1])
    await seed_paused(s.spec["seed_titles"][2])
    prior = work_map(s)
    ambiguous = await s.event("ambiguous")
    s.check(
        "only_essential_clarification",
        work_map(s) == prior
        and useful_text(ambiguous, ["说的是|哪(?:一)?(?:个|项|件)|(?:是|选).{1,80}还是"])
        and not any(
            item.get("kind") == "control_work" and item.get("committed")
            for item in foreground_observations(s, ambiguous)
        )
        and not ((ambiguous.get("decision") or {}).get("result", {}).get("controlled_work_ids")),
        ambiguous,
        {"matching_paused": [item["id"] for item in prior.values() if item["state"] == "paused"]},
    )


async def time03(s):
    s.clock = dt(s.spec["anchor"])
    created = await s.event("create", event_time=s.clock)
    first = s.rules()
    updated = await s.event("update", event_time=s.clock + timedelta(seconds=1))
    revised = s.rules()
    same = bool(len(first) == len(revised) == 1 and first[0].id == revised[0].id)
    s.check(
        "recurring_rule_updated_in_place",
        same
        and first[0].goal == revised[0].goal
        and first[0].time_zone == revised[0].time_zone == "Asia/Shanghai"
        and first[0].execution_kind == revised[0].execution_kind
        and first[0].timing.kind == revised[0].timing.kind == "weekly"
        and list(first[0].timing.weekdays)
        == list(revised[0].timing.weekdays)
        == s.spec["retained_weekdays"]
        and revised[0].timing.local_time.isoformat() == s.spec["updated_local_time"]
        and revised[0].revision == first[0].revision + 1,
        created,
        updated,
        {"before": s.snapshot(first), "after": s.snapshot(revised)},
    )
    cancelled = await s.event("cancel", event_time=s.clock + timedelta(seconds=2))
    retired = next((rule for rule in s.rules() if revised and rule.id == revised[0].id), None)
    count = len(s.spool())
    if revised and revised[0].next_due_at:
        await s.wake(revised[0].next_due_at)
    s.check(
        "cancel_stops_future_receipts",
        len(s.spool()) == count
        and bool(retired and retired.state.value == "cancelled")
        and not s.app.coordinator.scheduler.occurrences(s.app.owner, retired.id),
        cancelled,
        {"rules": s.snapshot(), "spool_count": len(s.spool())},
    )
    extra = await s.event("cross_zone", event_time=s.clock)
    named_zone_rules = s.rules()
    cross = next((rule for rule in named_zone_rules if rule.time_zone == s.spec["zone"]), None)
    expected_due = dt(s.spec["expected_named_zone_due"])
    s.check(
        "named_zone_calculation",
        bool(
            cross
            and cross.timing.kind == "weekly"
            and cross.execution_kind.value == "explicit_reminder"
            and list(cross.timing.weekdays) == s.spec["cross_zone_weekdays"]
            and cross.timing.local_time.isoformat() == s.spec["cross_zone_local_time"]
            and cross.next_due_at == expected_due
            and useful_text(
                extra, [re.escape(s.spec["zone"]) + "|" + re.escape(s.spec["zone_label"])]
            )
            and clock_display(
                response(extra), s.spec["cross_zone_local_time"], seconds_required=False
            )
        ),
        extra,
        {
            "saved_rules": s.snapshot(named_zone_rules),
            "expected_local_due": expected_due.isoformat(),
            "expected_utc_due": expected_due.astimezone(UTC).isoformat(),
        },
    )
    if not cross or not cross.next_due_at:
        return
    await s.close()
    await s.open()
    scheduler = s.app.coordinator.scheduler
    stamp = cross.next_due_at
    await asyncio.gather(*(asyncio.to_thread(scheduler.wake, s.app.owner, stamp) for _ in range(3)))
    await s.app.dispatcher.tick()
    facts = s.occurrence_proof(cross)
    native = schedule_native_receipts(facts, s.spool())
    occurrences = scheduler.occurrences(s.app.owner, cross.id)
    s.check(
        "concurrent_restart_one_occurrence",
        len(native) == len(occurrences) == 1
        and stamp == expected_due
        and cross.execution_kind.value == "explicit_reminder"
        and spool_text(native[0]).removeprefix("【Pot】") == s.spec["cross_zone_payload"]
        and occurrences[0].delivery_id is not None
        and occurrences[0].scheduled_at == stamp
        and len(facts[0]["occurrences"][0]["deliveries"]) == 1,
        {"schedule_facts": facts, "accepted_native_receipts": native},
    )


async def time04(s):
    work = await s.event("work", latency=True)
    s.headroom = 150
    await s.app.execute_tick()
    quiet = await s.event("quiet")
    direct = await s.event("direct")
    condition = await s.event("conditional")
    rules = s.rules()
    reminders = [r for r in rules if r.execution_kind.value == "explicit_reminder"]
    due_work = [r for r in rules if r.execution_kind.value == "goal"]
    if len(reminders) != 1 or len(due_work) != 1:
        for check in s.row["checks"]:
            s.check(check, False, quiet, direct, condition, {"rules": s.snapshot(rules)})
        return
    plain, check = reminders[0], due_work[0]
    calls = len(s.model_calls)
    with s.store.transaction(s.app.owner) as tx:
        quiet_row = tx.conn.execute(
            "SELECT quiet FROM ga_conversation_policy WHERE group_id=%s AND subject_id=%s",
            (s.group, OWNER),
        ).fetchone()
        work_before = tx.get_work(work["work_id"]) if work["work_id"] else None
    direct_occurrences = await s.wake(plain.next_due_at)
    facts = s.occurrence_proof(plain)
    just_literal = schedule_native_receipts(facts, s.spool())
    literal_body = (
        spool_text(just_literal[0]).removeprefix("【Pot】") if len(just_literal) == 1 else None
    )
    holds = (
        len(s.model_calls) == calls
        and len(just_literal) == 1
        and literal_body == s.spec["direct_payload"]
        and len(direct_occurrences) == 1
        and direct_occurrences[0].work_id is None
        and direct_occurrences[0].delivery_id
    )
    s.check(
        "prepared_due_while_worker_busy_without_model",
        holds
        and work_before is not None
        and work_before.state not in TERMINAL
        and s.headroom == 150,
        work,
        direct,
        {
            "schedule_facts": facts,
            "receipts": just_literal,
            "work_before_due": work_before.model_dump(mode="json") if work_before else None,
            "simulated_headroom_mib": s.headroom,
        },
    )
    s.check(
        "quiet_respects_explicit_reminder",
        holds and bool(quiet_row and quiet_row["quiet"]) and not quiet["receipts"],
        quiet,
        {"policy": quiet_row, "accepted_receipts": just_literal},
    )
    conditional_occurrences = await s.wake(check.next_due_at)
    actual_occurrence = conditional_occurrences[0] if len(conditional_occurrences) == 1 else None
    s.check(
        "rules_occurrences_receipts_distinct",
        len(facts) == 1
        and len(direct_occurrences) == 1
        and len(conditional_occurrences) == 1
        and bool(
            actual_occurrence and actual_occurrence.work_id and not actual_occurrence.delivery_id
        )
        and actual_occurrence.schedule_id == check.id
        and direct_occurrences[0].schedule_id == plain.id
        and direct_occurrences[0].id != actual_occurrence.id,
        {
            "literal": facts,
            "conditional_rule": check.model_dump(mode="json"),
            "conditional_occurrence": actual_occurrence.model_dump(mode="json")
            if actual_occurrence
            else None,
        },
    )
    s.headroom = 1300
    output = await s.settle(actual_occurrence.work_id if actual_occurrence else None)
    s.check(
        "conditional_work_uses_due_facts_and_silent_completion",
        bool(
            output
            and output["work"]["state"] == "completed"
            and not output["spool"]
            and any(
                item["check_key"] == "silent_result" and item["passed"]
                for item in output["validation"]
            )
        ),
        condition,
        output or {"execution": "missing"},
    )


def essential_artifact_input_reply(text, subject):
    if not re.search(subject, text):
        return False
    action = re.compile(
        r"拍(?:照|一下|张照|一张照)|上传|发(?:来|上来|给我|到这里|一下)|传(?:来|给我)"
        r"|提供|告诉我|贴(?:来|上|出)|补充|直接(?:念|报|说)(?:出|一遍|一下)"
    )
    negated = re.compile(r"(?:没有|没|尚未|未曾|未|不能|无法)\s*$")
    clauses = re.split(r"[，,。；;！？!?\n]", text)
    if any(
        not negated.search(clause[: request.start()])
        for clause in clauses
        for request in action.finditer(clause)
    ):
        return True

    colloquial = re.compile(r"发\s*我(?!\s*[了过的])")
    directed = re.compile(
        r"^(?:请|麻烦|能否|可否|能不能|可以|你可以|你(?:先|再|直接)|(?:你\s*)?把)"
    )
    contrary_directive = re.compile(
        r"^(?:(?:请|麻烦|你(?:先|再|直接|可以)?|能否|可否|能不能|可以)\s*)?"
        r"(?:别|不要|不必|无需|不用|不能|没有|没|尚未|未曾)"
    )
    for clause in clauses:
        if re.search(r"(?:了|过)[吗嘛吧]$", clause.strip()):
            continue
        for request in colloquial.finditer(clause):
            prefix = clause[: request.start()].strip()
            if prefix and not directed.match(prefix):
                continue
            if not contrary_directive.search(prefix) and not negated.search(prefix):
                return True
    return False


def foreground_observations(s, turn):
    return [
        value
        for record in s.model_calls
        if record["source_event_id"] == turn["event_id"]
        for value in record.get("tool_observations", ())
    ]


def inject_first_public_read(s, calls=None):
    client = s.app.coordinator.foreground_tools.client
    if calls is None:
        calls = []

    def wrap(method, operation):
        async def wrapped(arguments):
            signature = arguments.model_dump(mode="json")
            record = {"operation": operation, "arguments": signature, "fault_injected": not calls}
            calls.append(record)
            if record["fault_injected"]:
                raise DomainError(
                    "not_found", "Synthetic first public source has no usable content"
                )
            result = await method(arguments)
            record["result"] = result.model_dump(mode="json")
            return result

        return wrapped

    client.search = wrap(client.search, "web_search")
    client.fetch = wrap(client.fetch, "web_fetch")
    client.search_received = wrap(client.search_received, "web_search")
    client.fetch_received = wrap(client.fetch_received, "web_fetch")
    return calls


def successful_public_calls(calls, spec):
    return [
        call
        for call in calls
        if call["operation"] == "web_fetch"
        and call.get("result")
        and any(
            official_source(url, spec)
            for url in re.findall(
                r'https://[^"\s]+', json.dumps(call["result"], ensure_ascii=False)
            )
        )
    ]


def explicit_changed_fallback(calls):
    return (
        len(calls) > 1
        and calls[0]["fault_injected"]
        and any(
            (call["operation"], call["arguments"]) != (calls[0]["operation"], calls[0]["arguments"])
            and call.get("result")
            for call in calls[1:]
        )
    )


async def search01(s):
    calls = inject_first_public_read(s)
    request = await s.event("request", latency=True)
    work = await s.settle(request["work_id"])
    direct_receipts = foreground_observations(s, request)
    backend = (
        [] if not work else successful_tools(work, {"web_fetch", "web_search", "browser_open"})
    )
    fetched = {
        url
        for call in calls
        if call["operation"] == "web_fetch" and call.get("result")
        for url in fetch_source_urls(call["result"])
    }
    fetched.update(
        url
        for item in backend
        if item["tool_name"] in {"web_fetch", "browser_open"}
        for url in fetch_source_urls(item["output"])
    )
    official = {url for url in fetched if official_source(url, s.spec)}
    search = any(call["operation"] == "web_search" and call.get("result") for call in calls) or any(
        item["tool_name"] == "web_search" for item in backend
    )
    inspected = await s.artifacts(work) if work and work["work"]["state"] == "completed" else []
    delivered_files = [
        artifact for artifact in inspected if artifact["accepted_sha256"] == artifact["sha256"]
    ]
    output_segments = (
        [response(request)]
        + [spool_text(row) for row in (work or {}).get("spool", [])]
        + [item["decoded"].get("text", "") for item in delivered_files]
    )
    output_links = [url for item in delivered_files for url in item["decoded"].get("links", [])]
    output_links.extend(url for text in output_segments for url in markdown_urls(text))
    accepted_verified_links = {url for url in output_links if page_url_key(url) in official}
    evidence = {
        "public_provider_reads": calls,
        "foreground_observations": direct_receipts,
        "execution": work,
        "delivered_inspected_outputs": delivered_files,
        "official_fetched_pages": sorted(official),
        "accepted_verified_links": sorted(accepted_verified_links),
        "ledger": s.ledger([request["event_id"]]),
    }
    s.check(
        "real_search_and_verified_official_source", bool(official and search), request, evidence
    )
    s.check("fallback_after_injected_failure", explicit_changed_fallback(calls), evidence)
    s.check(
        "useful_ack_latency",
        accepted_by(request) and useful_text(request, s.spec["useful_patterns"]),
        request,
    )
    s.check(
        "correct_final_steps",
        bool(
            official
            and any(s.spec["correct_text"] in " ".join(part.split()) for part in output_segments)
            and accepted_verified_links
        ),
        request,
        evidence,
    )


def original_source_outbox(s, event_id):
    with s.store.transaction(s.app.owner) as tx:
        return tx.conn.execute(
            "SELECT d.id AS intent_id,d.idempotency_key,d.payload_hash,p.position,"
            "p.platform_message_id,p.state AS part_state "
            "FROM ga_deliveries d LEFT JOIN ga_delivery_parts p ON d.group_id=p.group_id AND d.id=p.intent_id "
            "WHERE d.group_id=%s AND d.request_event_id=%s ORDER BY d.created_at,p.position",
            (s.group, event_id),
        ).fetchall()


def verified_finding(wire, observed_pages, spec):
    if wire["payload"]["kind"] != "text":
        return False
    text = spool_text(wire)
    return bool(
        re.search(
            r"(?<!未)(?<!没)(?:已经|已|刚才|刚刚|实际|确实)\s*(?:亲自|在线)?"
            r"\s*(?:打开|查到|查阅|读取|核对)",
            text,
        )
        and any(
            official_source(url, spec) and page_url_key(url) in observed_pages
            for url in markdown_urls(text)
        )
    )


def restart_witness(before, after, source_intents, observed_parts, observed_pages, spec):
    """Permit the rest of a send or an already-queued distinct verified finding."""
    prior = {item["receipt"]["platform_message_id"]: item for item in before}
    observed = {item["receipt"]["platform_message_id"]: item for item in after}
    intent_signatures = {
        row["intent_id"]: (row["idempotency_key"], row["payload_hash"]) for row in source_intents
    }
    observed_signatures = {
        row["intent_id"]: (row["idempotency_key"], row["payload_hash"]) for row in observed_parts
    }
    parts = {
        row["platform_message_id"]: (row["intent_id"], row["position"])
        for row in observed_parts
        if row["part_state"] == "accepted" and row["platform_message_id"]
    }
    resumed_intents = {parts[native][0] for native in prior if native in parts}
    new_parts = set(observed) - set(prior)
    separate = {
        native
        for native in new_parts
        if native in parts and parts[native][0] not in resumed_intents
    }
    verified_separate = {
        native
        for native in separate
        if observed_signatures[parts[native][0]][0].startswith("reply:")
        and verified_finding(observed[native], observed_pages, spec)
    }
    reconciled = (
        bool(prior)
        and len(prior) == len(before)
        and len(observed) == len(after)
        and all(observed.get(native) == item for native, item in prior.items())
        and intent_signatures == observed_signatures
        and set(observed) == set(parts)
        and len(set(parts.values())) == len(parts)
        and all(item["receipt"]["state"] == "accepted" for item in observed.values())
        and separate == verified_separate
    )
    return {"reconciled": reconciled, "verified_separate_reply_ids": sorted(verified_separate)}


async def work05(s):
    calls = inject_first_public_read(s)
    delegated = await s.event("work", dispatch=False)
    injected = await s.fault_dispatch()
    before = s.spool()
    before_outbox = original_source_outbox(s, delegated["event_id"])
    foreground_pages = {
        url
        for observation in foreground_observations(s, delegated)
        if observation.get("kind") in {"web_fetch", "browser_open"}
        and observation.get("status") == "observed"
        and isinstance(observation.get("evidence"), dict)
        for url in fetch_source_urls(observation["evidence"])
    }
    await s.close()
    await s.open()
    inject_first_public_read(s, calls)
    await s.app.dispatcher.tick()
    original = (
        await s.app.channel.lookup_delivery(s.group, before[0]["receipt"]["idempotency_key"])
        if before
        else None
    )
    original_event = None
    if delegated["persisted"]:
        with s.store.transaction(s.app.owner) as tx:
            original_event = tx.get_event(delegated["event_id"])
    if original_event:
        s.app.channel.append_incoming(original_event)
        await s.app.poll_intake()
        await s.app.process_pending(batched=False)
        await s.app.dispatcher.tick()
    after = s.spool()
    after_outbox = original_source_outbox(s, delegated["event_id"])
    reconciliation = restart_witness(
        before, after, before_outbox, after_outbox, foreground_pages, s.spec
    )
    result = await s.settle(delegated["work_id"])
    verified = successful_public_calls(calls, s.spec)
    work_count = len(s.work_state())
    reconciled_once = bool(
        injected
        and original
        and original.state == "accepted"
        and reconciliation["reconciled"]
        and work_count == int(bool(delegated["work_id"]))
    )
    s.check(
        "failure_visible_and_fallback_advances",
        explicit_changed_fallback(calls) and bool(verified),
        delegated,
        {"provider_calls": calls},
    )
    s.check(
        "restart_and_duplicate_at_most_once",
        reconciled_once,
        delegated,
        {
            "before": before,
            "after": after,
            "same_receipt": bool(original),
            "before_outbox": before_outbox,
            "after_outbox": after_outbox,
            "foreground_fetched_pages": sorted(foreground_pages),
            "reconciliation": reconciliation,
            "work_count_after_replay": work_count,
        },
    )
    missing = await s.event("missing")
    missing_orders = [
        work for work in s.work_state() if work.request_event_id == missing["event_id"]
    ]
    missing_output = await s.settle(missing_orders[0].id) if len(missing_orders) == 1 else None
    clarification = response(missing) + "\n".join(
        spool_text(row) for row in (missing_output or {}).get("spool", [])
    )
    s.check(
        "only_actual_missing_input",
        essential_artifact_input_reply(clarification, s.spec["missing_pattern"])
        and len(missing_orders) <= 1
        and (
            not missing_orders
            or bool(
                missing_output
                and missing_output["work"]["request_event_id"] == missing["event_id"]
                and missing_output["work"]["state"] != "completed"
            )
        ),
        missing,
        missing_output or {"asked_from_foreground": True},
        {"work_ids_created_by_the_missing_input_turn": [work.id for work in missing_orders]},
    )
    final_receipts = result["spool"] if result else after
    final_text = "\n".join(spool_text(item) for item in final_receipts)
    fetched_result_pages = set(foreground_pages)
    if result:
        for receipt in successful_tools(result, {"web_fetch", "browser_open"}):
            fetched_result_pages.update(fetch_source_urls(receipt["output"]))
    accepted_verified_links = {
        url
        for url in markdown_urls(final_text)
        if official_source(url, s.spec) and page_url_key(url) in fetched_result_pages
    }
    s.check(
        "no_false_completion_or_spin",
        reconciled_once
        and bool(verified)
        and (not result or result["work"]["state"] == "completed")
        and len(final_receipts) == 1
        and all(item["receipt"]["state"] == "accepted" for item in final_receipts)
        and bool(accepted_verified_links)
        and not any(
            left["operation"] == middle["operation"] == right["operation"]
            and left["arguments"] == middle["arguments"] == right["arguments"]
            and not any(item.get("result") for item in (left, middle, right))
            for left, middle, right in zip(calls, calls[1:], calls[2:], strict=False)
        ),
        delegated,
        result or {"direct_verified_call_count": len(verified), "public_calls": len(calls)},
        {"accepted_verified_links": sorted(accepted_verified_links)},
    )


def verified_docx_image_captions(artifact, imports, conversions, expected):
    def receipt_pages(row):
        return {
            key
            for url in markdown_urls(row["output"].get("page_citation") or "")
            if (key := page_url_key(url)) is not None
        }

    imported_pages = {page for row in imports for page in receipt_pages(row)}
    converted_pages = {}
    for row in conversions:
        info = row["output"]
        if info.get("format") == "png" and info.get("sha256"):
            converted_pages.setdefault(info["sha256"], set()).update(receipt_pages(row))

    images = artifact["decoded"].get("embedded_images", [])
    provenance = []
    for image in images:
        if image.get("content_type") != "image/png":
            continue
        caption = set(image.get("caption_links", [])) | markdown_urls(image.get("caption_text", ""))
        caption_pages = {key for url in caption if (key := page_url_key(url)) is not None}
        matched = caption_pages & converted_pages.get(image["sha256"], set()) & imported_pages
        provenance.append({"embedded_sha256": image["sha256"], "matched_pages": sorted(matched)})
    matched_pages = {page for row in provenance for page in row["matched_pages"]}
    distinct_hashes = {row["embedded_sha256"] for row in provenance}
    verified = (
        bool(artifact.get("sha256"))
        and artifact.get("accepted_sha256") == artifact["sha256"]
        and len(images) == len(provenance) == len(distinct_hashes) == expected
        and len(matched_pages) == expected
        and all(len(row["matched_pages"]) == 1 for row in provenance)
    )
    return {
        "verified": verified,
        "image_source_pairs": provenance,
        "verified_source_link_count": len(matched_pages),
        "embedded_png_count": len(distinct_hashes),
    }


async def search02(s):
    start = await s.event("request", latency=True)
    await s.app.execute_tick()
    followup = await s.event("followup")
    works = s.work_state()
    s.check(
        "followup_same_work",
        bool(start["work_id"])
        and len(works) == 1
        and works[0].id == start["work_id"]
        and followup["work_id"] in (None, start["work_id"])
        and work_fact_has(work_facts_observed(s, followup), start["work_id"], works),
        start,
        followup,
        {"work_ids": [work.id for work in works]},
    )
    result = await s.settle(start["work_id"])
    if not result:
        return
    files = await s.artifacts(result)
    imports = successful_tools(result, {"web_image_import"})
    conversions = successful_tools(result, {"image_convert"})
    expected = s.spec["expected_count"]
    artifacts = [item for item in files if item["format"] == s.spec["must_export"]]
    s.check(
        "real_download_conversion_dedup",
        len(imports) >= expected
        and len(conversions) >= expected
        and len({json.dumps(item["output"], sort_keys=True, default=str) for item in imports})
        >= expected,
        result,
    )
    binding = (
        verified_docx_image_captions(artifacts[0], imports, conversions, expected)
        if len(artifacts) == 1
        else {"verified": False}
    )
    matching_files = binding["verified"]
    s.check(
        "source_manifest_and_delivered_file",
        matching_files,
        {"files": artifacts, "execution": result, "caption_provenance": binding},
    )
    attempts = result["work"]["state"]
    delivered = [spool_text(item) for item in result["spool"] if item["payload"]["kind"] == "text"]
    if matching_files:
        truthfully_reported = attempts == "completed"
    elif attempts in {"failed", "waiting_input", "paused"}:
        truthfully_reported = any(
            re.search(r"未完成|暂未交付|只找到|只交付|缺少|目前无法", text) for text in delivered
        )
    else:
        truthfully_reported = False
    s.check(
        "accurate_partial_status",
        truthfully_reported,
        {"import_receipts": len(imports), "execution": result},
    )


async def file01(s):
    obsolete = await s.event(
        "obsolete",
        process=False,
        text="旧的资料版本",
        attachment=(s.spec["filename"], s.spec["obsolete"].encode(), "file"),
    )
    s.record_read_only_fixture(obsolete, "blind_attachment_fixture")
    current = await s.event(
        "current",
        process=False,
        text="这是最新的资料版本",
        attachment=(s.spec["filename"], s.spec["current"].encode(), "file"),
    )
    s.record_read_only_fixture(current, "blind_attachment_fixture")
    request = await s.event("request", quote=current["event_id"], latency=True)
    result = await s.settle(request["work_id"])
    if not result:
        return
    artifacts = await s.artifacts(result)
    by_format = {item["format"]: item for item in artifacts}
    right_files = set(by_format) == {"docx", "pdf", "pptx"}
    source_versions = json.dumps(result["work"], ensure_ascii=False)
    s.check(
        "quoted_current_version",
        current["event_id"] in source_versions
        and obsolete["event_id"]
        not in json.dumps(result["work"].get("input_versions"), ensure_ascii=False),
        obsolete,
        current,
        request,
        result,
    )
    original = await s.app.channel.attachment_handles(current["event_id"])
    s.check(
        "original_and_new_version_survive",
        bool(original)
        and original[0].sha256 == current["attachment"]["sha256"]
        and len({item["version_id"] for item in artifacts}) == 3
        and all(item["sha256"] != original[0].sha256 for item in artifacts),
        current,
        {"artifacts": artifacts},
    )
    s.check(
        "parsed_word_pdf_ppt_content",
        right_files
        and all(
            all(
                value in json.dumps(item["decoded"], ensure_ascii=False)
                for value in s.spec["required"]
            )
            and not any(
                value in json.dumps(item["decoded"], ensure_ascii=False)
                for value in s.spec["absent"]
            )
            for item in artifacts
        )
        and len(by_format["pptx"]["decoded"].get("units", [])) == 1,
        {"artifacts": artifacts},
    )
    s.check(
        "three_accepted_file_receipts",
        right_files
        and all(item["accepted_sha256"] == item["sha256"] for item in artifacts)
        and result["work"]["state"] == "completed",
        result,
        {"artifacts": artifacts},
    )


def fixture_image(s, *, regular=False):
    from PIL import Image, ImageDraw, ImageFont

    photo = Image.new("RGB", (680, 440), "rgb(244, 238, 218)")
    draw = ImageDraw.Draw(photo)
    if regular:
        draw.rectangle((85, 120, 310, 330), fill=(23, 122, 184), outline=(27, 33, 55), width=7)
        draw.ellipse((365, 180, 540, 355), fill=(195, 44, 52))
    else:
        paths = [
            Path("/System/Library/Fonts/PingFang.ttc"),
            Path("/System/Library/Fonts/STHeiti Medium.ttc"),
            Path("/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc"),
        ]
        font_path = next((path for path in paths if path.is_file()), None)
        if not font_path:
            raise RuntimeError("Independent image fixture requires a CJK font")
        font = ImageFont.truetype(str(font_path), 60)
        draw.text((88, 105), s.spec["image_name"], font=font, fill=(0, 0, 0))
        draw.text((88, 220), s.spec["image_code"], font=font, fill=(0, 0, 0))
    stream = io.BytesIO()
    photo.save(stream, format="PNG")
    return stream.getvalue()


def native_audio(s):
    if not Path("/usr/bin/say").exists():
        return None
    target = s.root / "fixture-tone.wav"
    spoken = subprocess.run(
        [
            "/usr/bin/say",
            "-v",
            "Tingting",
            "--file-format=WAVE",
            "--data-format=LEI16@16000",
            "-o",
            str(target),
            s.spec["speech_text"],
        ],
        check=False,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        timeout=15,
    )
    return target.read_bytes() if spoken.returncode == 0 and target.is_file() else None


def identity_limitation_is_explicit(text):
    return bool(re.search(r"不足|无法|不能|没法|看不出|(?:确定|确认|辨认|识别)不了", text))


async def media01(s):
    source = await s.event(
        "image_seed",
        text="",
        process=False,
        attachment=("synthetic-notice.png", fixture_image(s), "image"),
    )
    s.record_read_only_fixture(source, "blind_image_fixture")
    first = await s.event("text_image", quote=source["event_id"])
    got = await s.settle(first["work_id"])
    photo = await s.event(
        "photo", attachment=("synthetic-surface.png", fixture_image(s, regular=True), "image")
    )
    photo_result = await s.settle(photo["work_id"])
    merged = response(first) + "\n".join(spool_text(row) for row in (got or {}).get("spool", []))
    descriptions = response(photo) + "\n".join(
        spool_text(row) for row in (photo_result or {}).get("spool", [])
    )
    observed = bool(got and successful_tools(got, {"image_analyze", "media_ocr", "provider_ocr"}))
    s.check(
        "actual_image_text_and_description",
        observed
        and s.spec["image_name"] in merged
        and s.spec["image_code"] in merged
        and pattern_all(descriptions, ["蓝", "红", "矩形|长方形|方块|方形", "圆"]),
        source,
        first,
        photo,
        got or {"image_work": "missing"},
        photo_result or {"photo_work": "missing"},
    )
    s.check(
        "identity_inference_bounded",
        identity_limitation_is_explicit(merged) and s.spec["image_name"] not in descriptions,
        first,
        photo,
    )
    speech = native_audio(s)
    if not speech:
        s.block(
            "actual_asr_code_and_downstream_provenance",
            "The synthetic Mandarin fixture could not be rendered",
        )
        return
    audio = await s.event("speech", text="", attachment=("synthetic-speech.wav", speech, "audio"))
    question = await s.event("speech_followup")
    with s.store.transaction(s.app.owner) as tx:
        transcripts = tx.conn.execute(
            "SELECT id,body FROM ga_events WHERE group_id=%s AND body->>'trusted_actor_origin'="
            "'authenticated_audio_transcription' AND body->>'reply_to'=%s",
            (s.group, audio["event_id"]),
        ).fetchall()
    bound_evidence = [
        entry
        for row in transcripts
        for entry in row["body"].get("attachments", [])
        if entry.get("kind") == "audio_transcription"
        and entry.get("source_event_id") == audio["event_id"]
        and entry.get("request_ids")
        and entry.get("budget_receipts")
        and pattern_all(row["body"].get("text", ""), s.spec["speech_patterns"])
    ]
    s.check(
        "actual_asr_code_and_downstream_provenance",
        len(transcripts) == len(bound_evidence) == 1
        and useful_text(question, s.spec["speech_patterns"]),
        audio,
        question,
        {"transcripts": transcripts, "bound_provider_evidence": bound_evidence},
    )


async def proactive01(s):
    observer = s.app.observer
    if not observer:
        return

    async def add(key, finish=False, *, title=None, due_in_seconds=720):
        event = await s.event(key, process=False)
        s.record_read_only_fixture(event, "blind_task_observation_seed")
        with s.store.transaction(s.app.owner) as tx:
            body = s.spec[key]
            task = tx.create_team_task(
                title or body[:6],
                due_at=datetime.now(UTC) + timedelta(seconds=due_in_seconds),
                source_event_id=event["event_id"],
            )
            return tx.update_team_task(
                task.id,
                expected_revision=task.revision,
                progress=body,
                status=TeamTaskState.COMPLETED if finish else task.status,
                source_event_id=event["event_id"],
            )

    pending = await add("actionable")
    await add("completed", finish=True)
    removed = await add("withdrawn")
    s.app.coordinator.memory.forget(s.app.owner, task_ids=[removed.id])
    for _ in range(7):
        if await observer.tick() is None:
            break
    with s.store.transaction(s.app.owner) as tx:
        shadow = tx.conn.execute(
            "SELECT resource_id,state,delivery_id,reason_code,proposal_sha256 "
            "FROM ga_react_observations WHERE group_id=%s",
            (s.group,),
        ).fetchall()
    s.check(
        "model_shadow_proposal_or_silence",
        any(
            row["resource_id"] == pending.id
            and row["state"] in {"shadow", "silent"}
            and row["delivery_id"] is None
            for row in shadow
        )
        and not s.spool(),
        {"observations": shadow},
    )
    await s.close()
    s.config = s.config.model_copy(
        update={
            "conversation": s.config.conversation.model_copy(update={"proactive_mode": "active"})
        }
    )
    await s.open()
    current = await add(
        "active_actionable",
        title=s.spec["active_title"],
        due_in_seconds=s.spec["active_due_seconds"],
    )
    for _ in range(5):
        await s.app.observer.tick()
        await s.app.dispatcher.tick()
    before_quiet = s.spool()
    with s.store.transaction(s.app.owner) as tx:
        active = tx.conn.execute(
            "SELECT o.resource_id,o.state,o.reason_code,o.delivery_id,o.proposal_sha256,"
            "d.state AS delivery_state,p.state AS part_state,p.platform_message_id "
            "FROM ga_react_observations o "
            "LEFT JOIN ga_deliveries d ON d.group_id=o.group_id AND d.id=o.delivery_id "
            "LEFT JOIN ga_delivery_parts p ON p.group_id=d.group_id AND p.intent_id=d.id "
            "WHERE o.group_id=%s AND o.resource_id=%s",
            (s.group, current.id),
        ).fetchall()
    accepted_ids = {
        row["platform_message_id"]
        for row in active
        if row["proposal_sha256"]
        and row["part_state"] == row["delivery_state"] == "accepted"
        and row["platform_message_id"]
    }
    matching = [
        row
        for row in before_quiet
        if row["receipt"]["state"] == "accepted"
        and row["receipt"]["platform_message_id"] in accepted_ids
        and pattern_all(spool_text(row), s.spec["active_notice_patterns"])
    ]
    s.check(
        "enabled_valid_notice_once",
        current.id != pending.id
        and current.title != pending.title
        and current.due_at < pending.due_at
        and len(matching) == len(accepted_ids) == len(before_quiet) == 1,
        {
            "observed_task_id": current.id,
            "active_observations": active,
            "task": current.model_dump(mode="json"),
            "spool": before_quiet,
        },
    )
    quiet = await s.event("quiet")
    for _ in range(3):
        await s.app.observer.tick()
        await s.app.dispatcher.tick()
    s.check(
        "completed_withdrawn_expired_and_quiet_suppressed",
        not any(value in spool_text(row) for row in s.spool() for value in s.spec["suppress"])
        and before_quiet == s.spool(),
        quiet,
        {"spool": s.spool()},
    )


async def await_proof(path, max_wait=150):
    if path is None:
        return None
    end = time.monotonic() + max_wait
    while not path.is_file() and time.monotonic() < end:
        await asyncio.sleep(1)
    return read_json(path) if path.is_file() else None


async def ops02(s):
    import httpx

    request = await s.event("request", latency=True)
    wire = []
    model = s.app.supervisor.factory.__self__.model
    client = model._client
    transport = client._transport

    class OneThrottle(httpx.AsyncBaseTransport):
        async def handle_async_request(self, outgoing):
            if outgoing.url.host != "api.deepseek.com" or outgoing.url.path != "/chat/completions":
                return await transport.handle_async_request(outgoing)
            proof = {"start_monotonic": time.monotonic(), "relationship": "candidate_work"}
            wire.append(proof)
            if len(wire) == 1:
                proof.update(status=429, injected=True, retry_after_seconds=1)
                return httpx.Response(429, headers={"Retry-After": "1"}, request=outgoing)
            returned = await transport.handle_async_request(outgoing)
            proof.update(status=returned.status_code, injected=False)
            return returned

    client._transport = OneThrottle()
    try:
        result = await s.settle(request["work_id"])
    finally:
        client._transport = transport
    with s.store.transaction(s.app.owner) as tx:
        ledger = tx.conn.execute(
            "SELECT id,work_id,state,operation,reason,actual_amount,actual_units,evidence_ref,"
            "created_at,updated_at FROM ga_budget_reservations WHERE group_id=%s AND work_id=%s ORDER BY created_at,id",
            (s.group, request["work_id"]),
        ).fetchall()
    recovered = bool(
        len(wire) >= 2
        and wire[0]["status"] == 429
        and wire[0]["injected"]
        and wire[1]["start_monotonic"] - wire[0]["start_monotonic"] >= 0.95
        and any(item.get("status") == 200 and item["injected"] is False for item in wire[1:])
        and result
        and result["work"]["state"] == "completed"
    )
    s.check(
        "genuine_provider_rate_limit_and_resume",
        recovered,
        {
            "boundary": "controlled HTTP 429 injected through real adapter; later HTTP is real DeepSeek",
            "requests": wire,
        },
        result or {"execution": "missing"},
    )
    s.check(
        "same_work_and_attempt_charge_records",
        bool(
            result
            and result["work"]["id"] == request["work_id"]
            and result["attempts"]
            and ledger
            and any(
                row["state"] == "settled" and row["actual_amount"] is not None for row in ledger
            )
            and any(row["state"] == "released" and row["actual_amount"] is None for row in ledger)
            and all(row["work_id"] == request["work_id"] for row in ledger)
            and len({row["id"] for row in ledger}) == len(ledger)
        ),
        request,
        result or {"execution": "missing"},
        {"ledger": ledger},
    )
    replay = await await_proof(s.args.replay_evidence)
    if not replay:
        s.block(
            "migration_and_long_task_past_old_limits",
            "Source-bound local PostgreSQL migration proof unavailable",
        )
        return
    expected = bindings()
    report_path = s.args.replay_evidence
    xml_path = report_path.parent / "pytest.xml"
    if (
        not all(replay.get(key) == value for key, value in expected.items())
        or replay.get("layer") != "replay"
        or replay.get("consistent") is not True
        or replay.get("environment", {}).get("postgres", {}).get("database") != "pot_replay"
        or not xml_path.is_file()
        or sha256(xml_path) != replay["pytest"]["xml_sha256"]
    ):
        s.block(
            "migration_and_long_task_past_old_limits",
            "Replay environment or source binding differs",
        )
        return
    nodes = coverage()["OPS-02"]["replay"]
    results = junit_status(xml_path)
    selected = {node: results.get(node, []) for node in nodes}
    s.check(
        "migration_and_long_task_past_old_limits",
        all(
            items and all(item["status"] == "passed" for item in items)
            for items in selected.values()
        )
        and bool(result and result["work"]["state"] == "completed"),
        {"isolated_postgres_replay_sha256": sha256(report_path), "replay_cases": selected},
        result or {"execution": "missing"},
    )


HANDLERS = {
    "OPS-02": ops02,
    "PROACTIVE-01": proactive01,
    "FILE-01": file01,
    "MEDIA-01": media01,
    "SEARCH-02": search02,
    "SEARCH-01": search01,
    "WORK-05": work05,
    "TIME-04": time04,
    "TIME-03": time03,
    "WORK-03": work03,
    "WORK-02": work02,
    "GROUP-03": group03,
    "GROUP-01": group01,
    "CHAT-05": chat05,
    "CHAT-04": chat04,
    "CHAT-01": chat01,
    "CHAT-02": chat02,
    "CHAT-03": chat03,
    "GROUP-02": group02,
    "WORK-01": work01,
    "WORK-04": work04,
    "TIME-01": time01,
    "TIME-02": time02,
}


async def evaluate_infrastructure(args, identity, destination, spec, row):
    """Exercise infrastructure on an isolated candidate rather than provider text."""
    if identity == "OPS-01":
        ops_proof = await await_proof(args.ops01_proof)
        environment = (ops_proof or {}).get("environment", {})
        if (
            ops_proof
            and ops_proof.get("format_version") == 1
            and ops_proof.get("case_id") == identity
            and ops_proof.get("source") == bindings()["source"]
            and ops_proof.get("worker_image") == args.worker_image
            and environment.get("aggregate_memory_bytes") == spec["host_bytes"]
            and environment.get("channel") == "LocalChannel"
            and environment.get("minigame") == "synthetic_local_service"
            and set(ops_proof.get("checks", ())) == set(row["checks"])
        ):
            row["checks"] = ops_proof["checks"]
            row["infrastructure_proof_sha256"] = sha256(args.ops01_proof)
        return
    from groupagent.evolution import DevelopmentManifest
    from groupagent.evolution.__main__ import _activation_report
    from groupagent.evolution.development import (
        EvaluationSuite,
        evaluate_code,
        write_source_archive,
    )
    from groupagent.evolution.models import CodeBundle, CodeProposal, canonical, digest

    destination.mkdir(mode=0o700)
    filename = "src/probe/capacity.py"
    candidate_source = destination / "source" / filename
    candidate_source.parent.mkdir(parents=True)
    original = "def reserve(headroom):\n    return headroom - 128\n"
    candidate_source.write_text(original)
    manifest_path = destination / "manifest.json"
    write_json(
        manifest_path,
        {
            "name": "headroom",
            "files": {filename: digest(original.encode())},
            "editable": [filename],
        },
    )
    manifest = DevelopmentManifest.load(manifest_path)
    suite = EvaluationSuite.model_validate(
        {
            "module": "probe.capacity",
            "function": "reserve",
            "cases": [
                {"name": "deficient", "argument": 112, "expected": 0, "baseline_expected": -16},
                {"name": "sufficient", "argument": 410, "expected": 154, "baseline_expected": 282},
            ],
        }
    )

    def bundle(candidate):
        proposal = CodeProposal.model_validate(
            {
                "name": "safe-headroom",
                "problem": "Keep transfer and host space in reserve.",
                "baseline_sha256": manifest.sha256,
                "edits": [
                    {
                        "path": filename,
                        "text": candidate,
                        "before_sha256": digest(original.encode()),
                    }
                ],
            }
        )
        return CodeBundle(
            version_id=uuid4().hex,
            body_sha256=digest(canonical(proposal.model_dump(mode="json"))),
            proposal=proposal,
        )

    async def evaluate(text):
        return await evaluate_code(
            manifest, bundle(text), suite, image=args.worker_image, docker_host=args.docker_host
        )

    tamper = (
        "from pathlib import Path\n"
        "def reserve(headroom):\n"
        "    Path('/work/probe-report.json').write_text('{\"passed\":true}')\n"
        "    return 999\n"
    )
    bad_report, _, _ = await evaluate(tamper)
    good_report, good_files, diff = await evaluate(
        "def reserve(headroom):\n    return max(0, headroom - 256)\n"
    )
    saved = destination / "report.json"
    write_json(saved, good_report.model_dump(mode="json"))
    archive = destination / "candidate.zip"
    write_source_archive(archive, good_files)
    activation_read = _activation_report(destination)
    intact = candidate_source.read_text() == original
    mutation_rejected = False
    archive.unlink()
    write_source_archive(archive, {filename: b"def reserve(headroom): return 500\n"})
    try:
        _activation_report(destination)
    except ValueError:
        mutation_rejected = True
    archive.unlink()
    write_source_archive(archive, good_files)
    proof = {
        "protected_assessment_order": spec["assessment_order"],
        "worker_image": args.worker_image,
        "failed_candidate": bad_report.model_dump(mode="json"),
        "positive_candidate": good_report.model_dump(mode="json"),
        "rollback_source_unchanged": intact,
        "post_evaluation_tamper_rejected": mutation_rejected,
        "candidate_archive_sha256": sha256(archive),
    }
    checks = {
        "independent_improvement_and_rollback": good_report.passed
        and activation_read == good_report
        and intact,
        "tamper_rejected": not bad_report.passed and mutation_rejected and intact,
        "real_sandbox_and_release_gate": good_report.passed
        and activation_read == good_report
        and filename in diff,
    }
    for key, condition in checks.items():
        row["checks"][key] = {"status": "passed" if condition else "failed", "evidence": [proof]}
    evidence = await await_proof(args.replay_evidence)
    xml_path = args.replay_evidence.parent / "pytest.xml" if args.replay_evidence else None
    policy_nodes = [
        node for node in coverage()["EVOLVE-01"]["replay"] if "strategy_approval" in node
    ]
    if (
        evidence
        and xml_path
        and xml_path.is_file()
        and evidence.get("consistent") is True
        and all(evidence.get(key) == value for key, value in bindings().items())
        and sha256(xml_path) == evidence["pytest"]["xml_sha256"]
    ):
        checks_from_run = junit_status(xml_path)
        permissions = {node: checks_from_run.get(node, []) for node in policy_nodes}
        passed = bool(permissions) and all(
            rows and all(item["status"] == "passed" for item in rows)
            for rows in permissions.values()
        )
        row["checks"]["authorization_scope"] = {
            "status": "passed" if passed else "failed",
            "evidence": [
                {
                    "postgres_strategy_scope_checks": permissions,
                    "replay_report_sha256": sha256(args.replay_evidence),
                }
            ],
        }


def latency_report(samples):
    values = sorted(item["first_accepted_seconds"] for item in samples)
    if not values:
        return {"samples": [], "p50": None, "p95": None, "max": None}
    return {
        "samples": samples,
        "p50": values[math.ceil(len(values) * 0.50) - 1],
        "p95": values[math.ceil(len(values) * 0.95) - 1],
        "max": values[-1],
    }


def error_info(error):
    frames = traceback.extract_tb(error.__traceback__)
    last = frames[-1] if frames else None
    return {
        "type": type(error).__name__,
        "code": getattr(error, "code", None),
        "file": Path(last.filename).name if last else None,
        "line": last.lineno if last else None,
    }


async def run(args):
    table = coverage()
    fixture = read_json(args.dataset)
    locked = read_json(BLIND_LOCK)
    dataset_sha = sha256(args.dataset)
    if (
        dataset_sha != locked["dataset_sha256"]
        or fixture.get("format_version") != 1
        or list(fixture["cases"]) != locked["case_ids"]
        or list(table) != locked["case_ids"]
    ):
        raise ValueError("Independent case inputs differ from the frozen dataset lock")
    if args.timeout <= 0:
        raise ValueError("Work observation timeout must be positive")
    if not re.fullmatch(r"sha256:[0-9a-f]{64}", args.worker_image):
        raise ValueError("Worker image must be an immutable SHA-256 image ID")
    requested = args.cases.split(",") if args.cases else list(table)
    if not requested or not set(requested) <= table.keys() or len(set(requested)) != len(requested):
        raise ValueError("Provide comma-separated, unique catalog IDs")
    destination = args.output.absolute()
    if destination != destination.resolve() or args.dataset.stat().st_mode & 0o077:
        raise ValueError("Use a private dataset and a canonical output directory")
    worker_source = observe_worker_source(args.worker_image, args.docker_host)
    destination.mkdir(parents=True, exist_ok=False, mode=0o700)
    started = bindings()
    runner_hash = sha256(Path(__file__))
    inspector = Path(__file__).with_name("runtime_acceptance_checks.py")
    inspector_hash = sha256(inspector)
    dsn = args.dsn_file.read_text().strip()
    postgres = postgres_identity(dsn)
    if postgres["database"] != "pot_blind":
        raise ValueError("The blind runner requires its own pot_blind PostgreSQL database")
    store = PostgresStore(dsn)
    report = {
        "format_version": 1,
        "layer": "blind",
        **started,
        "dataset_sha256": dataset_sha,
        "runner_sha256": runner_hash,
        "inspector_sha256": inspector_hash,
        "environment": {
            "model": {
                "provider": "api.deepseek.com",
                "model": args.model,
                "thinking": "disabled",
                "vision": args.vision_model,
                "asr": args.asr_model,
            },
            "postgres": postgres,
            "worker_image": args.worker_image,
            "worker_source": worker_source,
            "platform": platform.platform(),
            "python": platform.python_version(),
            "channel": "LocalChannel",
            "scope": "Real provider and local transport receipts; fault timing is explicitly injected, no WeChat",
            "admission_samples": "Synthetic headroom on local host; Docker worker uses real cgroup limits",
            "case_concurrency": args.concurrency,
        },
        "selected": requested,
        "cases": [],
        "latency": latency_report([]),
        "consistent": False,
        "passed": False,
    }
    semaphore = asyncio.Semaphore(args.concurrency)

    async def independent_case(identity, rules):
        samples = []
        row = {
            "id": identity,
            "level": rules["blind_level"],
            "status": "missing",
            "checks": {
                check: {"status": "missing", "evidence": []} for check in rules["blind_checks"]
            },
            "turns": [],
            "work_proofs": [],
        }
        if identity in requested:
            handler = HANDLERS.get(identity)
            if rules["blind_level"] == "isolated_infrastructure":
                try:
                    await evaluate_infrastructure(
                        args, identity, destination / identity, fixture["cases"][identity], row
                    )
                except Exception as error:
                    row["runner_error"] = error_info(error)
                incomplete = {
                    "status": "blocked",
                    "reason": "Independent infrastructure evidence unavailable",
                    "evidence": [],
                }
            elif handler:
                scenario = Scenario(
                    args, store, destination / identity, identity, fixture["cases"][identity], row
                )
                scenario.root.mkdir(mode=0o700)
                try:
                    await scenario.open()
                    await handler(scenario)
                except Exception as error:
                    row["runner_error"] = error_info(error)
                finally:
                    try:
                        await scenario.close()
                    except Exception as error:
                        row["cleanup_error"] = error_info(error)
                    row["model_rounds"] = scenario.model_calls
                    row["configuration_sha256"] = hashlib.sha256(
                        scenario.config.model_dump_json().encode()
                    ).hexdigest()
                    samples.extend({"case": identity, **item} for item in scenario.when)
                incomplete = {
                    "status": "blocked",
                    "reason": "Scenario did not produce the required evidence",
                    "evidence": [],
                }
            else:
                incomplete = {
                    "status": "blocked",
                    "reason": "Required scenario and its real-tool or infrastructure assertions have no runnable blind binding",
                    "evidence": [],
                }
            for check, result in row["checks"].items():
                if result["status"] == "missing":
                    row["checks"][check] = incomplete
        row["status"] = blind_status(row, set(rules["blind_checks"]))
        if identity in requested:
            case_root = destination / identity
            case_root.mkdir(exist_ok=True, mode=0o700)
            write_json(case_root / "evidence.json", row)
            row["evidence_sha256"] = sha256(case_root / "evidence.json")
        return identity, row, samples

    async def admitted_case(identity, rules):
        async with semaphore:
            return await independent_case(identity, rules)

    case_tasks = [
        asyncio.create_task(admitted_case(identity, rules)) for identity, rules in table.items()
    ]
    results = {}
    samples = []
    try:
        for progress in asyncio.as_completed(case_tasks):
            identity, row, case_samples = await progress
            samples.extend(case_samples)
            results[identity] = row
            report["cases"] = [results[key] for key in table if key in results]
            report["latency"] = latency_report(
                sorted(samples, key=lambda sample: (sample["case"], sample["turn"]))
            )
            write_json(destination / "report.json", report)
            print(json.dumps({"case": identity, "status": row["status"]}), flush=True)
    finally:
        for task in case_tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*case_tasks, return_exceptions=True)
    report["consistent"] = (
        started == bindings()
        and runner_hash == sha256(Path(__file__))
        and inspector_hash == sha256(inspector)
    )
    report["passed"] = report["consistent"] and all(
        row["status"] == "passed" for row in report["cases"]
    )
    write_json(destination / "report.json", report)
    print_summary("blind", report["cases"])
    return 0 if report["passed"] else 1


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--dsn-file", type=Path, required=True)
    parser.add_argument("--secrets", type=Path, required=True)
    parser.add_argument("--worker-image", required=True)
    parser.add_argument("--docker-host", required=True)
    parser.add_argument("--model", default="deepseek-flash")
    parser.add_argument("--vision-model", default="deepseek-flash")
    parser.add_argument("--asr-model", default="tools/asr")
    parser.add_argument("--timeout", type=float, default=140)
    parser.add_argument(
        "--concurrency",
        type=int,
        choices=(1, 2),
        default=1,
        help="Maximum independently scoped synthetic cases in progress",
    )
    parser.add_argument(
        "--replay-evidence", type=Path, help="Source-bound replay report for OPS-02 migration"
    )
    parser.add_argument(
        "--ops01-proof", type=Path, help="Independent whole-stack 2 GiB rehearsal report"
    )

    parser.add_argument(
        "--cases", help="Comma-separated partial diagnostic run; omitted cases block release"
    )
    return asyncio.run(run(parser.parse_args()))


if __name__ == "__main__":
    sys.exit(main())
