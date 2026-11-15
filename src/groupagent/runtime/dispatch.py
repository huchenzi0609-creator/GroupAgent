"""Ordered, durable text/file delivery and receipt reconciliation."""

from __future__ import annotations

import asyncio
import hashlib
import time
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from typing import Literal, Protocol
from uuid import uuid4

from groupagent.channels import ChannelReceipt
from groupagent.conversation.quiet import is_quiet
from groupagent.domain import ActorContext, DeliveryIntent, DeliveryState, DomainError

from .artifacts import ArtifactRegistry
from .contracts import ActorResolver, ProcessLease
from .notices import check_notice
from .state import RuntimeState


class DeliveryPort(Protocol):
    async def send_text(self, group_id: str, text: str, idempotency_key: str) -> ChannelReceipt: ...
    async def send_file(
        self, group_id: str, name: str, content: bytes, idempotency_key: str
    ) -> ChannelReceipt: ...
    async def lookup_delivery(self, group_id: str, key: str) -> ChannelReceipt | None: ...

    async def send_media(
        self,
        group_id: str,
        name: str,
        content: bytes,
        *,
        kind: Literal["image", "voice"],
        idempotency_key: str,
        duration_seconds: float | None = None,
        allow_file_fallback: bool = False,
    ) -> ChannelReceipt: ...


def split_text(text: str, *, maximum: int = 1800) -> tuple[str, ...]:
    """Keep every character; prefer paragraph/sentence boundaries, never truncate."""
    if maximum < 50:
        raise ValueError("Text segment limit is too small")
    parts = []
    while len(text) > maximum:
        split = max(
            text.rfind("\n", maximum // 2, maximum),
            text.rfind("。", maximum // 2, maximum),
            text.rfind(". ", maximum // 2, maximum),
        )
        split = split + 1 if split >= 0 else maximum
        # Keep combining marks and ZWJ sequences with their preceding character.
        import unicodedata

        while split < len(text) and (
            unicodedata.combining(text[split])
            or text[split] == "\u200d"
            or text[split - 1] == "\u200d"
        ):
            split += 1
        parts.append(text[:split])
        text = text[split:]
    if text or not parts:
        parts.append(text)
    return tuple(parts)


class Dispatcher:
    def __init__(
        self,
        state: RuntimeState,
        service_actor: ActorContext,
        actor_resolver: ActorResolver,
        channel: DeliveryPort,
        artifacts: ArtifactRegistry,
        *,
        badge: str = "【机器人组长】",
        chunk_characters: int = 1800,
        after_submit: Callable[[DeliveryIntent, int], Awaitable[None]] | None = None,
        time_zone: str = "Asia/Shanghai",
        quiet_hours: list[str] | None = None,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.state, self.service_actor, self.actor_resolver = state, service_actor, actor_resolver
        self.channel, self.artifacts = channel, artifacts
        self.badge, self.chunk_characters = badge, chunk_characters
        self.owner_id = "sender:" + uuid4().hex
        self._lock = asyncio.Lock()
        self._lease: ProcessLease | None = None
        self._after_submit = after_submit
        self.time_zone, self.quiet_hours = time_zone, quiet_hours or []
        self._clock, self._retry_at, self._channel_failures = clock, 0.0, 0

    async def tick(self, *, limit: int = 20) -> int:
        async with self._lock:
            if self._clock() < self._retry_at:
                return 0
            lease = self.state.claim_process(
                self.service_actor, "sender", self.owner_id, ttl_seconds=30
            )
            if lease is None:
                return 0
            self._lease = lease
            stop = asyncio.Event()

            async def heartbeats():
                while not stop.is_set():
                    try:
                        await asyncio.wait_for(stop.wait(), timeout=8)
                    except TimeoutError:
                        if self._lease is None:
                            raise DomainError("stale_lease", "Sender lease disappeared") from None
                        self._lease = self.state.heartbeat_process(
                            self.service_actor, self._lease, ttl_seconds=30
                        )

            heartbeat = asyncio.create_task(heartbeats())
            count = 0
            try:
                with self.state.store.transaction(self.service_actor) as tx:
                    rows = tx.conn.execute(
                        "SELECT id,created_by FROM ga_deliveries WHERE group_id=%s AND state IN ('pending','dispatching','unknown') ORDER BY created_at,id LIMIT %s",
                        (tx.group_id, min(max(limit, 1), 100)),
                    ).fetchall()
                for row in rows:
                    actor = None
                    try:
                        actor = self.actor_resolver(row["created_by"])
                        with self.state.store.transaction(actor) as tx:
                            tx.recover_expired_deliveries()
                            intent = tx.get_delivery(row["id"])
                        if intent.policy_revision != actor.policy_revision:
                            self.state.alert(
                                self.service_actor, "delivery_policy_changed", resource_id=intent.id
                            )
                            continue
                        if intent.state == DeliveryState.PENDING:
                            count += int(await self._dispatch(actor, intent))
                        elif intent.state == DeliveryState.UNKNOWN:
                            count += int(await self._reconcile(actor, intent))
                    except DomainError as error:
                        if actor is not None and error.code in {
                            "source_unavailable",
                            "validation_failed",
                            "invalid_delivery",
                            "quota_exceeded",
                        }:
                            try:
                                with self.state.store.transaction(actor) as tx:
                                    current = tx.get_delivery(row["id"])
                                    if current.state == DeliveryState.PENDING:
                                        tx.cancel_pending_delivery(
                                            current.id, expected_revision=current.revision
                                        )
                            except DomainError:
                                pass
                        self.state.alert(self.service_actor, error.code, resource_id=row["id"])
                        if error.code in {
                            "auth_required",
                            "invalid_credentials",
                            "channel_unavailable",
                            "profile_unavailable",
                        }:
                            self._channel_failures += 1
                            delay = (
                                30
                                if error.code in {"auth_required", "invalid_credentials"}
                                else min(30, 2 ** min(self._channel_failures, 5))
                            )
                            self._retry_at = self._clock() + delay
                            break
                if count:
                    self._channel_failures = 0
                return count
            finally:
                stop.set()
                await heartbeat
                if self._lease:
                    self.state.release_process(self.service_actor, self._lease)
                self._lease = None

    def _fence(self) -> None:
        if self._lease is None:
            raise DomainError("stale_lease", "Sender has no process lease")
        self.state.assert_process(self.service_actor, self._lease)

    def _payloads(
        self, actor: ActorContext, intent: DeliveryIntent
    ) -> tuple[tuple[str, str, bytes | None], ...]:
        if intent.payload_ref.startswith("text:"):
            text = intent.payload_ref[5:]
            if hashlib.sha256(text.encode()).hexdigest() != intent.payload_hash:
                raise DomainError("source_unavailable", "Outgoing text was changed or removed")
            chunks = split_text(text, maximum=self.chunk_characters)
            return tuple(
                (
                    "text",
                    self.badge + (f"（{i + 1}/{len(chunks)}）" if len(chunks) > 1 else "") + part,
                    None,
                )
                for i, part in enumerate(chunks)
            )
        if intent.payload_ref.startswith("artifact:"):
            version, data = self.artifacts.read(
                actor, intent.payload_ref[9:], require_validated=True
            )
            if version.sha256 != intent.payload_hash:
                raise DomainError("validation_failed", "Delivery artifact hash does not match")
            if len(data) > 25 * 1024 * 1024:
                raise DomainError("quota_exceeded", "Delivery file exceeds the product limit")
            kind = (
                "image"
                if version.format in {"png", "jpeg", "jpg", "webp", "gif"}
                else "voice"
                if version.format in {"wav", "ogg", "mp3"}
                else "file"
            )
            return ((kind, version.name, data),)
        raise DomainError("invalid_delivery", "Unknown outgoing payload reference")

    @staticmethod
    def _hash(payload: tuple[str, str, bytes | None]) -> str:
        kind, name, data = payload
        return hashlib.sha256(
            kind.encode() + b"\0" + name.encode() + b"\0" + (data or b"")
        ).hexdigest()

    def _prepare(
        self,
        actor: ActorContext,
        intent: DeliveryIntent,
        payloads: tuple[tuple[str, str, bytes | None], ...],
    ) -> None:
        with self.state.store.transaction(actor) as tx:
            current = tx.get_delivery(intent.id)
            check_notice(tx, intent.id)
            if (
                current.payload_hash != intent.payload_hash
                or current.payload_ref != intent.payload_ref
            ):
                raise DomainError("source_unavailable", "Outgoing source changed before submission")
            if (
                current.request_event_id
                and getattr(tx.get_event(current.request_event_id), "content_state", "active")
                != "active"
            ):
                raise DomainError("source_unavailable", "Reply source was removed")
            for index, payload in enumerate(payloads):
                key = (
                    "part:"
                    + hashlib.sha256(intent.idempotency_key.encode()).hexdigest()
                    + ":"
                    + str(index)
                )
                digest = self._hash(payload)
                tx.conn.execute(
                    "INSERT INTO ga_delivery_parts(group_id,intent_id,position,idempotency_key,payload_hash) VALUES(%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING",
                    (actor.group_id, intent.id, index, key, digest),
                )
                stored = tx.conn.execute(
                    "SELECT payload_hash FROM ga_delivery_parts WHERE group_id=%s AND intent_id=%s AND position=%s",
                    (actor.group_id, intent.id, index),
                ).fetchone()
                if stored is None or stored["payload_hash"] != digest:
                    raise DomainError("idempotency_conflict", "Outgoing segment contents changed")
            row = tx.conn.execute(
                "SELECT count(*) AS n FROM ga_delivery_parts WHERE group_id=%s AND intent_id=%s",
                (actor.group_id, intent.id),
            ).fetchone()
            if row is None or row["n"] != len(payloads):
                raise DomainError("idempotency_conflict", "Outgoing segment count changed")

    def _parts(self, actor: ActorContext, intent_id: str) -> list[dict]:
        with self.state.store.transaction(actor) as tx:
            tx.get_delivery(intent_id)
            return tx.conn.execute(
                "SELECT * FROM ga_delivery_parts WHERE group_id=%s AND intent_id=%s ORDER BY position",
                (actor.group_id, intent_id),
            ).fetchall()

    def _record(
        self,
        actor: ActorContext,
        intent_id: str,
        part: dict,
        receipt: ChannelReceipt | None,
        *,
        unknown: bool = False,
        expected_payload_ref: str | None = None,
    ) -> None:
        state = "unknown" if unknown else "submitted"
        message_id = job_id = None
        if receipt is not None:
            if (
                receipt.group_id != actor.group_id
                or receipt.idempotency_key != part["idempotency_key"]
            ):
                raise DomainError("scope_violation", "Receipt has another group or purpose")
            if receipt.accepted:
                state = "accepted"
            elif receipt.delivery_state == DeliveryState.REJECTED:
                state = "rejected"
            elif receipt.delivery_state == DeliveryState.UNKNOWN:
                state = "unknown"
            message_id, job_id = receipt.platform_message_id, receipt.job_id or receipt.send_id
        with self.state.store.transaction(actor) as tx:
            if receipt is None and not unknown:
                check_notice(tx, intent_id)
                current = tx._lock_delivery_work(intent_id)
                if (
                    self._lease is None
                    or tx.conn.execute(
                        "SELECT 1 FROM ga_process_leases WHERE group_id=%s AND name=%s AND owner_id=%s AND fencing_token=%s AND expires_at>clock_timestamp() FOR SHARE",
                        (
                            actor.group_id,
                            self._lease.name,
                            self._lease.owner_id,
                            self._lease.fencing_token,
                        ),
                    ).fetchone()
                    is None
                ):
                    raise DomainError("stale_lease", "Sender lost authority before submission")
                if (
                    current["state"] != "dispatching"
                    or current["policy_revision"] != actor.policy_revision
                    or current["payload_ref"] != expected_payload_ref
                ):
                    raise DomainError("source_unavailable", "Delivery changed before submission")
                if (
                    current["request_event_id"]
                    and getattr(
                        tx.get_event(current["request_event_id"]), "content_state", "active"
                    )
                    != "active"
                ):
                    raise DomainError("source_unavailable", "Reply source was removed")
                if current["work_id"]:
                    work = tx.get_work(current["work_id"])
                    if (
                        work.state.value != "delivering"
                        or work.goal_revision != current["goal_revision"]
                    ):
                        raise DomainError("cancelled", "Work no longer authorizes sending")
                if current["payload_ref"].startswith("artifact:"):
                    self.artifacts._row(tx, current["payload_ref"][9:])
            old = tx.conn.execute(
                "SELECT * FROM ga_delivery_parts WHERE group_id=%s AND intent_id=%s AND position=%s FOR UPDATE",
                (actor.group_id, intent_id, part["position"]),
            ).fetchone()
            if old is None:
                raise DomainError("not_found", "Delivery part disappeared")
            if old["state"] == "accepted":
                if state != "accepted" or old["platform_message_id"] != message_id:
                    raise DomainError("idempotency_conflict", "Accepted receipt cannot change")
                return
            tx.conn.execute(
                "UPDATE ga_delivery_parts SET state=%s,platform_message_id=%s,platform_job_id=%s,updated_at=clock_timestamp() WHERE group_id=%s AND intent_id=%s AND position=%s",
                (state, message_id, job_id, actor.group_id, intent_id, part["position"]),
            )
            if receipt is not None and receipt.accepted and receipt.fallback_reason:
                parent = tx.get_delivery(intent_id)
                if parent.work_id:
                    work = tx.get_work(parent.work_id)
                    if (
                        work.state.value == "delivering"
                        and work.goal_revision == parent.goal_revision
                        and not work.scope.get("privacy_redacted")
                    ):
                        text = "这段朗读已改为音频文件发送，可以打开播放。"
                        notice = tx._insert_delivery(
                            work.id,
                            "text:" + text,
                            hashlib.sha256(text.encode()).hexdigest(),
                            "media-fallback:" + intent_id,
                            goal_revision=work.goal_revision,
                        )
                        bundle = tx.conn.execute(
                            "SELECT bundle_id FROM ga_delivery_bundles WHERE group_id=%s AND intent_id=%s",
                            (tx.group_id, intent_id),
                        ).fetchone()
                        if (
                            bundle
                            and not tx.conn.execute(
                                "SELECT 1 FROM ga_delivery_bundles WHERE group_id=%s AND intent_id=%s",
                                (tx.group_id, notice.id),
                            ).fetchone()
                        ):
                            position = tx.conn.execute(
                                "SELECT COALESCE(max(position),-1)+1 AS n FROM ga_delivery_bundles WHERE group_id=%s AND bundle_id=%s",
                                (tx.group_id, bundle["bundle_id"]),
                            ).fetchone()
                            assert position is not None
                            tx.conn.execute(
                                "INSERT INTO ga_delivery_bundles(group_id,bundle_id,intent_id,position) VALUES(%s,%s,%s,%s)",
                                (tx.group_id, bundle["bundle_id"], notice.id, position["n"]),
                            )

    async def _dispatch(self, actor: ActorContext, intent: DeliveryIntent) -> bool:
        with self.state.store.transaction(actor) as tx:
            notice = tx.conn.execute(
                "SELECT work_id FROM ga_progress_notices WHERE group_id=%s AND intent_id=%s",
                (tx.group_id, intent.id),
            ).fetchone()
            work_id = intent.work_id or (notice["work_id"] if notice else None)
            scope = tx.get_work(work_id).scope if work_id else {}
            autonomous = bool(
                scope.get("origin_schedule_id") or scope.get("origin_trigger_rule_id") or notice
            )
            if intent.request_event_id:
                autonomous = autonomous or bool(
                    tx.conn.execute(
                        "SELECT 1 FROM ga_participation_log WHERE group_id=%s AND event_id=%s AND proactive",
                        (tx.group_id, intent.request_event_id),
                    ).fetchone()
                )
            if autonomous:
                policies = tx.conn.execute(
                    "SELECT * FROM ga_conversation_policy WHERE group_id=%s AND subject_id IN ('*',%s)",
                    (tx.group_id, actor.member_id),
                ).fetchall()
                if any(
                    row["subject_id"] == "*" and row["mode"] in {"paused", "disabled"}
                    for row in policies
                ) or is_quiet(policies, datetime.now(UTC), self.time_zone, self.quiet_hours):
                    return False
            predecessor = tx.conn.execute(
                "SELECT 1 FROM ga_delivery_bundles b JOIN ga_delivery_bundles earlier ON earlier.group_id=b.group_id AND earlier.bundle_id=b.bundle_id AND earlier.position<b.position JOIN ga_deliveries d ON d.group_id=earlier.group_id AND d.id=earlier.intent_id WHERE b.group_id=%s AND b.intent_id=%s AND d.state NOT IN ('accepted','rejected') LIMIT 1",
                (actor.group_id, intent.id),
            ).fetchone()
            if predecessor:
                return False
        payloads = self._payloads(actor, intent)
        self._prepare(actor, intent, payloads)
        with self.state.store.transaction(actor) as tx:
            claimed = tx.claim_delivery(self.owner_id, ttl_seconds=300, intent_id=intent.id)
        if claimed is None:
            return False
        try:
            for part in self._parts(actor, intent.id):
                if part["state"] in {"accepted", "rejected"}:
                    continue
                self._fence()
                # Always consult the original native key before any resubmission.
                existing = await self.channel.lookup_delivery(
                    actor.group_id, part["idempotency_key"]
                )
                if existing is not None:
                    self._record(actor, intent.id, part, existing)
                    if not existing.accepted:
                        break
                    continue
                if part["state"] != "pending":
                    break
                # Persist 'submitted' before the external call. A crash here
                # is reconciled by key; no new random send key is invented.
                self._record(actor, intent.id, part, None, expected_payload_ref=intent.payload_ref)
                self._fence()
                kind, value, data = payloads[part["position"]]
                if kind == "text":
                    receipt = await self.channel.send_text(
                        actor.group_id, value, part["idempotency_key"]
                    )
                elif kind in {"image", "voice"}:
                    version = self.artifacts.get(actor, intent.payload_ref[9:])
                    duration = version.provenance.get("media_metadata", {}).get("duration_seconds")
                    receipt = await self.channel.send_media(
                        actor.group_id,
                        value,
                        data or b"",
                        kind=kind,
                        idempotency_key=part["idempotency_key"],
                        duration_seconds=duration,
                        allow_file_fallback=True,
                    )
                else:
                    receipt = await self.channel.send_file(
                        actor.group_id, value, data or b"", part["idempotency_key"]
                    )
                if self._after_submit:
                    await self._after_submit(intent, part["position"])
                self._record(actor, intent.id, part, receipt)
                if not receipt.accepted:
                    break
        except (Exception, asyncio.CancelledError):
            with self.state.store.transaction(actor) as tx:
                tx.finish_delivery(
                    intent.id,
                    claimed.claim_token,
                    DeliveryState.UNKNOWN,
                    reason="sender_outcome_unknown",
                )
            raise
        return self._finish(actor, claimed, reconcile=False)

    def _finish(self, actor: ActorContext, intent: DeliveryIntent, *, reconcile: bool) -> bool:
        parts = self._parts(actor, intent.id)
        if not parts:
            return False
        all_accepted = all(part["state"] == "accepted" for part in parts)
        any_rejected = any(part["state"] == "rejected" for part in parts)
        if any_rejected and any(part["state"] == "pending" for part in parts):
            with self.state.store.transaction(actor) as tx:
                tx.conn.execute(
                    "UPDATE ga_delivery_parts SET state='rejected',reason='earlier_segment_rejected' WHERE group_id=%s AND intent_id=%s AND state='pending'",
                    (actor.group_id, intent.id),
                )
            parts = self._parts(actor, intent.id)
        state = (
            DeliveryState.ACCEPTED
            if all_accepted
            else DeliveryState.REJECTED
            if any_rejected and all(part["state"] in {"accepted", "rejected"} for part in parts)
            else DeliveryState.UNKNOWN
        )
        if reconcile and state == DeliveryState.UNKNOWN:
            return False
        first = next((part for part in parts if part["platform_message_id"]), parts[0])
        with self.state.store.transaction(actor) as tx:
            if reconcile:
                tx.reconcile_delivery(
                    intent.id,
                    state,
                    evidence_ref="native-parts:" + intent.id,
                    platform_message_id=first["platform_message_id"],
                    platform_job_id=first["platform_job_id"],
                    reason="parts_reconciled",
                )
            else:
                tx.finish_delivery(
                    intent.id,
                    intent.claim_token,
                    state,
                    platform_message_id=first["platform_message_id"],
                    platform_job_id=first["platform_job_id"],
                    evidence_ref="native-parts:" + intent.id,
                    reason="parts_checked",
                )
            if state == DeliveryState.ACCEPTED and intent.payload_ref.startswith("artifact:"):
                self.artifacts.mark_shared_in_tx(tx, intent.payload_ref[9:])
        return state == DeliveryState.ACCEPTED

    async def _reconcile(self, actor: ActorContext, intent: DeliveryIntent) -> bool:
        parts = self._parts(actor, intent.id)
        for part in parts:
            if part["state"] in {"accepted", "rejected", "pending"}:
                continue
            self._fence()
            receipt = await self.channel.lookup_delivery(actor.group_id, part["idempotency_key"])
            if receipt is not None:
                self._record(actor, intent.id, part, receipt)
        parts = self._parts(actor, intent.id)
        # After earlier submitted parts are positively accepted, only proven
        # unsubmitted parts may continue with the exact same stored keys.
        if (
            parts
            and all(p["state"] in {"accepted", "pending"} for p in parts)
            and any(p["state"] == "pending" for p in parts)
        ):
            with self.state.store.transaction(actor) as tx:
                row = tx._lock_delivery_work(intent.id)
                if row["state"] == "unknown":
                    tx.conn.execute(
                        "UPDATE ga_deliveries SET state='pending',revision=revision+1,reason='unsubmitted_parts_remaining' WHERE group_id=%s AND id=%s",
                        (actor.group_id, intent.id),
                    )
                    tx._audit("delivery_unsubmitted_parts_recovered", intent.id)
            return False
        return self._finish(actor, intent, reconcile=True)

    def is_bot_message(self, actor: ActorContext, platform_id: str) -> bool:
        with self.state.store.transaction(actor) as tx:
            return bool(
                tx.conn.execute(
                    "SELECT 1 FROM ga_delivery_parts WHERE group_id=%s AND platform_message_id=%s AND state='accepted' LIMIT 1",
                    (actor.group_id, platform_id),
                ).fetchone()
            )
