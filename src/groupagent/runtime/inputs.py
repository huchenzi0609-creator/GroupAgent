"""Trusted attachment intake. Parsing stays in the isolated media/document worker.

This service consumes already persisted channel events. It is not a model tool and
never accepts a caller-supplied path, role, or unscoped attachment locator.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
from contextlib import suppress
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Protocol
from uuid import NAMESPACE_URL, uuid4, uuid5

from psycopg.types.json import Jsonb

from groupagent.artifacts import ArtifactScope, ArtifactStore, ArtifactVersion
from groupagent.artifacts.isolated import IsolatedDocumentService
from groupagent.billing import BudgetLedger
from groupagent.channels.attachments import preferred_attachment_variants
from groupagent.channels.models import AttachmentContent, AttachmentHandle
from groupagent.domain import (
    ActorContext,
    DomainError,
    EventKind,
    ExecutionLease,
    GroupEvent,
    WorkOrder,
)
from groupagent.media import (
    Capability,
    IsolatedMediaWorker,
    MediaHTTPProvider,
    MediaRef,
    MediaService,
    MediaSource,
    ProviderRoute,
    Transcript,
)
from groupagent.memory.common import memory_lock, source_rows
from groupagent.memory.members import MemberService
from groupagent.persistence import PostgresStore, UnitOfWork
from groupagent.research.image_transforms import ImageTransformService, resolve_public_image
from groupagent.tools import ToolContext, ToolScope

from .artifacts import ArtifactRegistry
from .contracts import ActorResolver
from .state import source_fingerprint

_TRUSTED_ORIGINS = {"verified_channel_metadata", "agentwechat_authenticated_message_metadata"}
_IMAGE_FORMATS = {"png", "jpeg", "jpg", "gif", "webp"}
_AUDIO_FORMATS = {"wav", "ogg", "mp3", "flac", "m4a", "aac"}
_DOCUMENT_FORMATS = {"md", "docx", "pdf", "pptx"}
_VIDEO_FORMATS = {"mp4", "mov", "mkv", "webm", "avi", "3gp", "m4v", "mpeg", "mpg"}


class AttachmentChannel(Protocol):
    async def attachment_handles(self, message_id: str) -> tuple[AttachmentHandle, ...]: ...
    async def fetch_attachment(self, handle: AttachmentHandle) -> AttachmentContent: ...


def _digest(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":")).encode()
    ).hexdigest()


def _attachment_key(handle: AttachmentHandle) -> str:
    return "channel:" + _digest(
        [
            handle.account_id,
            handle.group_id,
            handle.source_epoch,
            handle.message_id,
            handle.asset_id,
            handle.kind,
            handle.variant,
        ]
    )


def _reject_video(handle: AttachmentHandle) -> None:
    if (handle.content_type or "").lower().startswith("video/") or Path(
        handle.filename or ""
    ).suffix.lower().lstrip(".") in _VIDEO_FORMATS:
        raise DomainError("unsupported_video", "Video attachments are never downloaded")


def _decision(event: GroupEvent, kind: str, reason: str) -> dict[str, Any]:
    return {
        "kind": kind,
        "result": {
            "event_id": event.id,
            "decision": {"kind": kind, "reason_code": reason},
            "work_id": None,
            "delivery_id": None,
            "duplicate": False,
        },
    }


class _InputBudget:
    """One internal ASR scope; ordinary work billing is never rewritten."""

    def __init__(
        self,
        owner: InputProcessor,
        actor: ActorContext,
        event: GroupEvent,
        attachment: str,
        token: str,
        fingerprint: str,
        prefix: str,
    ):
        self.owner, self.actor, self.event = owner, actor, event
        self.attachment, self.token, self.fingerprint, self.prefix = (
            attachment,
            token,
            fingerprint,
            prefix,
        )
        self.fixture_only = getattr(owner.ledger, "fixture_only", False)
        self.reservations: set[str] = set()

    def _actor(self, actor: ActorContext, *, active: bool) -> ActorContext:
        fresh = self.owner.actor_resolver(self.actor.member_id)
        if (actor.group_id, actor.member_id) != (self.actor.group_id, self.actor.member_id) or (
            fresh.group_id,
            fresh.member_id,
        ) != (self.actor.group_id, self.actor.member_id):
            raise DomainError("permission_denied", "Input billing identity changed")
        if active:
            if fresh != self.actor or actor != self.actor:
                raise DomainError("policy_changed", "Input billing authority changed")
            self.owner._guard_claim(
                self.actor, self.event, self.attachment, self.token, self.fingerprint
            )
        return fresh

    def reserve(
        self,
        actor: ActorContext,
        key: str,
        operation: str,
        price_id: str,
        estimated_units: dict[str, Decimal],
        *,
        work_id: str | None = None,
        **kwargs: Any,
    ):
        self._actor(actor, active=True)
        if (
            work_id != "input:" + self.event.id
            or operation != "media.asr"
            or not key.startswith(self.prefix + ":asr-")
            or kwargs
        ):
            raise DomainError(
                "scope_violation", "Input budget only authorizes this audio transcription"
            )
        reservation = self.owner.ledger.reserve(
            actor, key, operation, price_id, estimated_units, source_event_id=self.event.id
        )
        self.reservations.add(reservation.id)
        return reservation

    def begin(self, actor: ActorContext, identifier: str) -> bool:
        self._actor(actor, active=True)
        if identifier not in self.reservations:
            raise DomainError("scope_violation", "Reservation belongs to another input attempt")
        return self.owner.ledger.begin(actor, identifier)

    def settle(
        self,
        actor: ActorContext,
        identifier: str,
        actual_units: dict[str, Decimal],
        evidence_ref: str,
    ):
        if identifier not in self.reservations:
            raise DomainError("scope_violation", "Reservation belongs to another input attempt")
        return self.owner.ledger.settle(
            self._actor(actor, active=False), identifier, actual_units, evidence_ref
        )

    def unknown(self, actor: ActorContext, identifier: str, reason: str) -> None:
        if identifier not in self.reservations:
            raise DomainError("scope_violation", "Reservation belongs to another input attempt")
        self.owner.ledger.unknown(self._actor(actor, active=False), identifier, reason)


class InputProcessor:
    """Durable, bounded audio preprocessing and explicitly authorized work imports."""

    claim_seconds = 60
    heartbeat_seconds = 15

    def __init__(
        self,
        store: PostgresStore,
        artifacts: ArtifactStore,
        worker: IsolatedMediaWorker,
        providers: dict[str, MediaHTTPProvider],
        routes: dict[str, ProviderRoute],
        ledger: BudgetLedger,
        channel: AttachmentChannel,
        workspace_root: Path,
        actor_resolver: ActorResolver,
        *,
        max_total_input_bytes: int = 100 * 1024 * 1024,
        max_images: int = 10,
        max_input_files: int = 20,
        max_image_bytes: int = 15 * 1024 * 1024,
        max_document_bytes: int = 30 * 1024 * 1024,
        max_image_pixels: int = 40_000_000,
    ):
        if any(
            type(value) is not int or not 0 < value <= maximum
            for value, maximum in (
                (max_total_input_bytes, 100 * 1024 * 1024),
                (max_images, 10),
                (max_input_files, 20),
                (max_image_bytes, 15 * 1024 * 1024),
                (max_document_bytes, 30 * 1024 * 1024),
                (max_image_pixels, 40_000_000),
            )
        ):
            raise ValueError("Input file, byte, image and pixel limits exceed supported bounds")
        root = workspace_root.absolute()
        if root != root.resolve() or root.is_symlink():
            raise DomainError("invalid_path", "Input workspace root must be canonical")
        root.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.store, self.artifacts, self.worker, self.providers, self.routes = (
            store,
            artifacts,
            worker,
            providers,
            routes,
        )
        self.ledger, self.channel, self.workspace_root, self.actor_resolver = (
            ledger,
            channel,
            root,
            actor_resolver,
        )
        self.registry = ArtifactRegistry(store, artifacts)
        self.max_total_input_bytes, self.max_images = max_total_input_bytes, max_images
        self.max_input_files, self.max_image_bytes = max_input_files, max_image_bytes
        self.max_document_bytes, self.max_image_pixels = max_document_bytes, max_image_pixels
        self._active_audio: dict[str, set[asyncio.Task[GroupEvent]]] = {}
        self._quiesced_events: set[str] = set()

    def _identity(self, event: GroupEvent) -> ActorContext:
        actor = self.actor_resolver(event.actor_id)
        if (actor.group_id, actor.member_id) != (event.group_id, event.actor_id):
            raise DomainError("permission_denied", "Input sender does not match trusted identity")
        return actor

    @staticmethod
    def _source(
        tx: UnitOfWork,
        event_id: str,
        fingerprint: str | None = None,
        *,
        allow_expired: bool = False,
    ) -> GroupEvent:
        source_rows(tx, [event_id], require_complete=True, allow_expired=allow_expired)
        canonical = tx.get_event(event_id)
        if fingerprint is not None and source_fingerprint(canonical) != fingerprint:
            raise DomainError("source_unavailable", "Attachment source changed after authorization")
        return canonical

    def _canonical(self, event: GroupEvent) -> tuple[ActorContext, GroupEvent]:
        actor = self._identity(event)
        with self.store.transaction(actor) as tx:
            canonical = self._source(tx, event.id)
        if canonical.actor_id != actor.member_id or (
            canonical.kind == EventKind.AUDIO
            and canonical.trusted_actor_origin not in _TRUSTED_ORIGINS
        ):
            raise DomainError(
                "permission_denied", "Only authenticated channel input can be preprocessed"
            )
        # All subsequent bytes, timestamps and metadata come from this stored event.
        return actor, canonical

    def _handles(self, event: GroupEvent) -> tuple[AttachmentHandle, ...]:
        if event.kind == EventKind.UNSUPPORTED_VIDEO or any(
            item.get("kind") == "unsupported_video" for item in event.attachments
        ):
            raise DomainError("unsupported_video", "Video inputs are never fetched or parsed")
        handles = []
        for item in event.attachments:
            if item.get("kind") not in {"image", "audio", "file"}:
                continue
            try:
                handle = AttachmentHandle.model_validate(item.get("handle"))
            except ValueError:
                raise DomainError(
                    "invalid_attachment", "Attachment needs a verified channel handle"
                ) from None
            _reject_video(handle)
            if (
                handle.group_id != event.group_id
                or handle.kind != item["kind"]
                or (event.channel_account_id and handle.account_id != event.channel_account_id)
                or (event.source_epoch and handle.source_epoch != event.source_epoch)
                or (
                    handle.platform_message_id is not None
                    and handle.platform_message_id != event.platform_message_id
                )
            ):
                raise DomainError(
                    "scope_violation", "Attachment metadata does not match its source event"
                )
            handles.append(handle)
        if (
            not handles
            or len(handles) > self.max_input_files
            or len({_attachment_key(h) for h in handles}) != len(handles)
        ):
            raise DomainError(
                "invalid_attachment", "Attachment selection exceeds the distinct file limit"
            )
        return tuple(handles)

    def _check_input_size(self, kind: str, name: str | None, size: int | None) -> None:
        suffix = Path(name or "").suffix.lower().lstrip(".")
        if kind == "image" or suffix in _IMAGE_FORMATS:
            maximum = self.max_image_bytes
        elif kind == "audio" or suffix in _AUDIO_FORMATS:
            maximum = 25 * 1024 * 1024
        else:
            maximum = self.max_document_bytes
        if size is not None and size > min(maximum, self.max_total_input_bytes):
            raise DomainError("input_limit", "Selected attachment exceeds its input byte limit")

    def _check_handle_size(self, handle: AttachmentHandle) -> None:
        self._check_input_size(
            handle.kind,
            handle.filename,
            handle.size_bytes if handle.size_bytes is not None else handle.advertised_size_bytes,
        )

    async def _download(self, event: GroupEvent, reference: AttachmentHandle) -> AttachmentContent:
        _reject_video(reference)
        self._check_handle_size(reference)
        handles = await self.channel.attachment_handles(reference.message_id)
        candidates = [
            h
            for h in handles
            if (h.account_id, h.group_id, h.external_group_id, h.source_epoch, h.message_id, h.kind)
            == (
                reference.account_id,
                reference.group_id,
                reference.external_group_id,
                reference.source_epoch,
                reference.message_id,
                reference.kind,
            )
            and (reference.asset_id is None or h.asset_id == reference.asset_id)
            and (reference.variant == "best" or h.variant == reference.variant)
        ]
        if not candidates:
            raise DomainError(
                "attachment_unavailable", "No matching attachment exists for this source"
            )
        choices = preferred_attachment_variants(
            (h for h in candidates if h.availability == "available"), reference.variant
        )
        if len(choices) != 1:
            if not choices:
                pending = [h for h in candidates if h.availability == "pending"]
                request = getattr(self.channel, "request_attachment_download", None)
                if len(pending) == 1 and request is not None:
                    self._check_handle_size(pending[0])
                    await request(pending[0])
                raise DomainError(
                    "attachment_pending" if pending else "attachment_unavailable",
                    "Attachment bytes are not yet available",
                    retryable=bool(pending),
                )
            raise DomainError(
                "ambiguous_attachment", "Attachment metadata contains multiple matching assets"
            )
        handle = choices[0]
        if reference.sha256 is not None and reference.sha256 != handle.sha256:
            raise DomainError(
                "attachment_version_conflict",
                "Attachment bytes changed since this handle was issued",
            )
        _reject_video(handle)
        self._check_handle_size(handle)
        if (
            handle.platform_message_id is not None
            and handle.platform_message_id != event.platform_message_id
        ):
            raise DomainError("scope_violation", "Attachment platform identity changed")
        content = await self.channel.fetch_attachment(handle)
        self._check_input_size(handle.kind, handle.filename, len(content.content))
        digest = hashlib.sha256(content.content).hexdigest()
        if (
            content.handle != handle
            or content.sha256 != digest
            or (handle.sha256 and handle.sha256 != digest)
            or (handle.size_bytes is not None and handle.size_bytes != len(content.content))
        ):
            raise DomainError(
                "validation_failed", "Downloaded attachment does not match its metadata"
            )
        return content

    @staticmethod
    def _media_source(
        event: GroupEvent, handle: AttachmentHandle, work_id: str, sequence: int = 0
    ) -> MediaSource:
        return MediaSource(
            group_id=event.group_id,
            work_id=work_id,
            source_event_id=event.id,
            source_message_id=event.platform_message_id or handle.message_id,
            member_id=event.actor_id,
            attachment_id=handle.asset_id or _attachment_key(handle),
            quote_message_id=next(
                (
                    str(item["server_id"])
                    for item in event.attachments
                    if item.get("kind") == "quote" and item.get("server_id")
                ),
                event.reply_to,
            ),
            declared_kind=handle.kind,
            sequence=sequence,
        )

    def _cache_row(
        self,
        tx: UnitOfWork,
        event: GroupEvent,
        attachment: str,
        fingerprint: str,
        *,
        create: bool = False,
        additional_fingerprints: dict[str, str] | None = None,
    ) -> dict[str, Any] | None:
        expected = {**(additional_fingerprints or {}), event.id: fingerprint}
        if create:
            tx.conn.execute(
                "INSERT INTO ga_media_inputs(group_id,event_id,attachment_id,body,source_fingerprints) VALUES(%s,%s,%s,'{}',%s) ON CONFLICT DO NOTHING",
                (tx.group_id, event.id, attachment, Jsonb(expected)),
            )
        row = tx.conn.execute(
            "SELECT *,clock_timestamp() AS now FROM ga_media_inputs WHERE group_id=%s AND event_id=%s AND attachment_id=%s FOR UPDATE",
            (tx.group_id, event.id, attachment),
        ).fetchone()
        if row and (not row["available"] or row["source_fingerprints"] != expected):
            raise DomainError("source_unavailable", "Cached input was withdrawn or changed")
        return row

    @staticmethod
    def _put(tx: UnitOfWork, event: GroupEvent, attachment: str, body: dict[str, Any]) -> None:
        tx.conn.execute(
            "UPDATE ga_media_inputs SET body=%s WHERE group_id=%s AND event_id=%s AND attachment_id=%s AND available",
            (Jsonb(body), tx.group_id, event.id, attachment),
        )

    @staticmethod
    def _active_claim(row: dict[str, Any] | None, token: str) -> dict[str, Any]:
        body = row["body"] if row else {}
        if (
            row is None
            or body.get("state") != "processing"
            or body.get("claim") != token
            or body.get("claim_until", 0) <= row["now"].timestamp()
        ):
            raise DomainError("stale_lease", "Input preprocessing claim expired or changed")
        return dict(body)

    def _guard_claim(
        self,
        actor: ActorContext,
        event: GroupEvent,
        attachment: str,
        token: str,
        fingerprint: str,
        *,
        update: dict[str, Any] | None = None,
        renew: bool = False,
    ) -> dict[str, Any]:
        if self.actor_resolver(actor.member_id) != actor:
            raise DomainError("policy_changed", "Input preprocessing authority changed")
        with self.store.transaction(actor) as tx:
            memory_lock(tx)
            self._source(tx, event.id, fingerprint)
            row = self._cache_row(tx, event, attachment, fingerprint)
            body = self._active_claim(row, token)
            if renew:
                assert row is not None
                body["claim_until"] = row["now"].timestamp() + self.claim_seconds
            if update:
                body.update(update)
            if renew or update:
                self._put(tx, event, attachment, body)
            return body

    def _claim(
        self, actor: ActorContext, event: GroupEvent, attachment: str, fingerprint: str, token: str
    ) -> tuple[dict[str, Any], GroupEvent | None]:
        with self.store.transaction(actor) as tx:
            memory_lock(tx)
            self._source(tx, event.id, fingerprint)
            decision = tx.get_decision(event.id)
            row = self._cache_row(tx, event, attachment, fingerprint, create=True)
            assert row is not None
            body = dict(row["body"])
            if body.get("state") == "complete":
                return body, self._source(tx, body["derived_event_id"])
            echo = bool(
                event.platform_message_id
                and (
                    tx.is_bot_message(event.platform_message_id)
                    or tx.conn.execute(
                        "SELECT 1 FROM ga_delivery_parts WHERE group_id=%s AND platform_message_id=%s AND state='accepted' LIMIT 1",
                        (actor.group_id, event.platform_message_id),
                    ).fetchone()
                )
            )
            if echo:
                tx.record_decision(event.id, _decision(event, "ignore", "bot_echo"))
                return body, event
            if decision is not None:
                raise DomainError(
                    "already_processed", "Audio source already has a durable decision"
                )
            reconciliation = body.get("reconciliation_reservation_id")
            if isinstance(reconciliation, str) and reconciliation:
                raise DomainError(
                    "provider_reconciliation_required",
                    "Audio preprocessing already has a paid reservation; reconcile before retry",
                    effect_state="unknown",
                    resource_ref=reconciliation,
                )
            if (
                body.get("state") == "processing"
                and body.get("claim_until", 0) > row["now"].timestamp()
            ):
                raise DomainError(
                    "input_busy", "Another processor owns this audio input", retryable=True
                )
            body.update(
                state="processing",
                claim=token,
                claim_until=row["now"].timestamp() + self.claim_seconds,
            )
            self._put(tx, event, attachment, body)
            return body, None

    def _attempt(
        self, actor: ActorContext, event: GroupEvent, attachment: str, token: str, fingerprint: str
    ) -> int:
        with self.store.transaction(actor) as tx:
            memory_lock(tx)
            self._source(tx, event.id, fingerprint)
            body = self._active_claim(self._cache_row(tx, event, attachment, fingerprint), token)
            attempt = int(body.get("attempts", 0)) + 1
            if attempt > 2:
                raise DomainError(
                    "input_retry_exhausted", "Audio preprocessing has reached its two-attempt limit"
                )
            body["attempts"] = attempt
            self._put(tx, event, attachment, body)
            return attempt

    def _failed(
        self,
        actor: ActorContext,
        event: GroupEvent,
        attachment: str,
        token: str,
        code: str,
        *,
        unstarted_attempt: tuple[int, str] | None = None,
    ) -> str | None:
        # Do not restore invalidated rows or replace a newer process's claim.
        reservation_id = None
        with self.store.transaction(actor) as tx:
            memory_lock(tx)
            row = tx.conn.execute(
                "SELECT body FROM ga_media_inputs WHERE group_id=%s AND event_id=%s AND attachment_id=%s AND available FOR UPDATE",
                (actor.group_id, event.id, attachment),
            ).fetchone()
            if row and row["body"].get("claim") == token:
                body = dict(row["body"])
                if unstarted_attempt is not None:
                    attempt, prefix = unstarted_attempt
                    if body.get("attempts") == attempt:
                        # Reserve takes the same memory lock. Check the durable
                        # ledger, including reserved/unknown/settled/released rows,
                        # rather than inferring nonpayment from a local set.
                        paid_prefix = prefix + ":asr-"
                        reservation = tx.conn.execute(
                            "SELECT id FROM ga_budget_reservations WHERE group_id=%s AND left(key,length(%s))=%s LIMIT 1",
                            (actor.group_id, paid_prefix, paid_prefix),
                        ).fetchone()
                        if reservation is None:
                            body["attempts"] = attempt - 1
                        else:
                            reservation_id = str(reservation["id"])
                            code = "provider_reconciliation_required"
                            body["reconciliation_reservation_id"] = reservation_id
                body.update(state="failed", error_code=code, claim_until=0)
                self._put(tx, event, attachment, body)
        return reservation_id

    def _finish(
        self,
        actor: ActorContext,
        event: GroupEvent,
        attachment: str,
        token: str,
        fingerprint: str,
        reference: MediaRef,
        transcript: Transcript,
    ) -> GroupEvent:
        source_key = "audio-transcription:" + _digest([event.group_id, event.id])
        derived = GroupEvent(
            id=str(uuid5(NAMESPACE_URL, source_key)),
            source_key=source_key,
            group_id=event.group_id,
            actor_id=event.actor_id,
            kind=EventKind.QUOTE,
            text=transcript.text,
            occurred_at=event.occurred_at,
            received_at=datetime.now(UTC),
            reply_to=event.id,
            channel_account_id=event.channel_account_id,
            source_epoch=event.source_epoch,
            trusted_actor_origin="authenticated_audio_transcription",
            policy_revision=actor.policy_revision,
            attachments=[
                {
                    "kind": "audio_transcription",
                    "source_event_id": event.id,
                    "media_ref": reference.model_dump(mode="json"),
                    "confidence": transcript.confidence,
                    "uncertainty": [
                        item.model_dump(mode="json") for item in transcript.uncertainty
                    ],
                    "needs_clarification": transcript.needs_clarification,
                    "provider": transcript.provider,
                    "model": transcript.model,
                    "request_ids": list(transcript.request_ids),
                    "budget_receipts": list(transcript.budget_receipts),
                    "untrusted_content": True,
                }
            ],
        )
        with self.store.transaction(actor) as tx:
            memory_lock(tx)
            self._source(tx, event.id, fingerprint)
            if tx.get_decision(event.id) is not None:
                raise DomainError(
                    "already_processed", "Audio source was decided during preprocessing"
                )
            body = self._active_claim(self._cache_row(tx, event, attachment, fingerprint), token)
            stream = "audio-transcription:" + event.id
            cursor = tx.get_cursor(stream)
            inserted = tx.ingest_event(derived, stream, "1", cursor.revision if cursor else 0)
            tx.record_decision(event.id, _decision(event, "observe", "audio_transcribed"))
            body.update(
                state="complete",
                derived_event_id=inserted.event.id,
                transcript=transcript.model_dump(mode="json"),
                claim_until=0,
            )
            self._put(tx, event, attachment, body)
            return inserted.event

    async def preprocess_audio(self, event: GroupEvent) -> GroupEvent:
        if event.id in self._quiesced_events:
            raise DomainError("source_unavailable", "Input source was withdrawn for cleanup")
        task = asyncio.create_task(self._preprocess_audio(event))
        self._active_audio.setdefault(event.id, set()).add(task)
        try:
            return await task
        except asyncio.CancelledError:
            caller = asyncio.current_task()
            if event.id in self._quiesced_events and (caller is None or not caller.cancelling()):
                raise DomainError(
                    "source_unavailable", "Input processing stopped for source cleanup"
                ) from None
            raise
        finally:
            active = self._active_audio.get(event.id)
            if active is not None:
                active.discard(task)
                if not active:
                    self._active_audio.pop(event.id, None)

    async def quiesce(
        self, event_ids: list[str] | tuple[str, ...], *, timeout_seconds: float = 10
    ) -> None:
        """Trusted cleanup barrier; no scratch purge until owned writers stop."""
        if not 0 < timeout_seconds <= 60:
            raise ValueError("Cleanup timeout must be within sixty seconds")
        self._quiesced_events.update(event_ids)
        tasks = {
            task
            for identifier in event_ids
            for task in self._active_audio.get(identifier, ())
            if not task.done()
        }
        if asyncio.current_task() in tasks:
            raise DomainError(
                "cleanup_busy", "A writer cannot await its own cleanup", retryable=True
            )
        for task in tasks:
            task.cancel()
        if tasks:
            done, pending = await asyncio.wait(tasks, timeout=timeout_seconds)
            await asyncio.gather(*done, return_exceptions=True)
            if pending:
                raise DomainError(
                    "cleanup_busy",
                    "Input writer has not stopped; retain cleanup job",
                    retryable=True,
                )

    async def _preprocess_audio(self, event: GroupEvent) -> GroupEvent:
        actor, event = await asyncio.to_thread(self._canonical, event)
        if event.kind != EventKind.AUDIO:
            return event
        handles = self._handles(event)
        if len(handles) != 1 or handles[0].kind != "audio":
            raise DomainError(
                "invalid_attachment", "A voice message needs exactly one audio attachment"
            )
        handle = handles[0]
        self._check_handle_size(handle)
        attachment, fingerprint, token = (
            _attachment_key(handle),
            source_fingerprint(event),
            uuid4().hex,
        )
        body, completed = await asyncio.to_thread(
            self._claim, actor, event, attachment, fingerprint, token
        )
        if completed:
            return completed
        directory = self.workspace_root / ("input-" + _digest([actor.group_id, event.id]))
        directory.mkdir(mode=0o700, exist_ok=True)
        if directory.is_symlink() or directory.resolve() != directory:
            raise DomainError("invalid_path", "Input workspace cannot be redirected")
        context = ToolContext(
            ToolScope(actor.group_id, "input:" + event.id, actor, directory, frozenset(), event.id),
            "audio-input",
            "audio-input:" + _digest([actor.group_id, event.id]),
            asyncio.Event(),
        )

        async def guard(ctx: ToolContext, identifiers: tuple[str, ...]) -> None:
            if identifiers != (event.id,):
                raise DomainError("scope_violation", "Input preprocessing source differs")
            await asyncio.to_thread(self._guard_claim, actor, event, attachment, token, fingerprint)

        async def heartbeat() -> None:
            while True:
                await asyncio.sleep(self.heartbeat_seconds)
                try:
                    await asyncio.to_thread(
                        self._guard_claim, actor, event, attachment, token, fingerprint, renew=True
                    )
                except Exception:
                    context.cancelled.set()
                    return

        ticker = asyncio.create_task(heartbeat())
        unstarted_attempt: tuple[int, str] | None = None
        try:
            service = MediaService(
                self.artifacts,
                self.worker,
                providers=self.providers,
                source_guard=guard,
                max_image_bytes=self.max_image_bytes,
                max_image_pixels=self.max_image_pixels,
                max_images=self.max_images,
            )
            if isinstance(body.get("reference"), dict):
                reference = MediaRef.model_validate(body["reference"])
                self._check_input_size(
                    "audio", None, len(await service._content(context, reference))
                )
            else:
                downloaded = await self._download(event, handle)
                await guard(context, (event.id,))
                reference = await service.ingest_audio(
                    context,
                    self._media_source(event, downloaded.handle, context.work_id),
                    downloaded.content,
                )
                await asyncio.to_thread(
                    self._guard_claim,
                    actor,
                    event,
                    attachment,
                    token,
                    fingerprint,
                    update={"reference": reference.model_dump(mode="json")},
                )
            route = self.routes.get("asr")
            if route is None:
                asr_routes = {
                    item.route_id: item
                    for item in self.routes.values()
                    if item.capability == Capability.ASR
                }
                route = next(iter(asr_routes.values())) if len(asr_routes) == 1 else None
            if route is None or route.capability != Capability.ASR:
                raise DomainError(
                    "provider_unconfigured",
                    "Audio input needs one explicit ASR route or an asr default",
                )
            attempt = await asyncio.to_thread(
                self._attempt, actor, event, attachment, token, fingerprint
            )
            prefix = context.idempotency_key + ":attempt-" + str(attempt)
            unstarted_attempt = (attempt, prefix)
            context = ToolContext(context.scope, context.call_id, prefix, context.cancelled)
            service.budget = _InputBudget(
                self, actor, event, attachment, token, fingerprint, prefix
            )
            members = await asyncio.to_thread(MemberService(self.store).list, actor)
            names = tuple(
                dict.fromkeys(
                    member.display_name
                    for member in members
                    if member.display_name != "群昵称待补充"
                )
            )
            transcript = await service.transcribe(context, reference, route, critical_terms=names)
            await guard(context, (event.id,))
            return await asyncio.to_thread(
                self._finish, actor, event, attachment, token, fingerprint, reference, transcript
            )
        except BaseException as error:
            context.cancelled.set()
            deferred = (
                isinstance(error, DomainError)
                and error.code == "resource_deferred"
                and error.effect_state == "not_started"
            )
            reservation_id = None
            with suppress(Exception):
                reservation_id = await asyncio.to_thread(
                    self._failed,
                    actor,
                    event,
                    attachment,
                    token,
                    error.code if isinstance(error, DomainError) else "input_incomplete",
                    unstarted_attempt=unstarted_attempt if deferred else None,
                )
            if reservation_id is not None:
                # A later local worker may be unstarted while this ASR attempt
                # already owns a paid/unknown call. Never silently replay it or
                # release its hold merely because the last operation was denied.
                raise DomainError(
                    "provider_reconciliation_required",
                    "Audio preprocessing already has a paid reservation; reconcile before retry",
                    effect_state="unknown",
                    resource_ref=reservation_id,
                ) from error
            raise
        finally:
            ticker.cancel()
            with suppress(asyncio.CancelledError):
                await ticker

    def _work_guard(
        self, work: WorkOrder, lease: ExecutionLease, context: ToolContext
    ) -> WorkOrder:
        actor = context.actor
        if (
            (actor.group_id, context.work_id, work.group_id, work.id)
            != (lease.group_id, lease.work_id, lease.group_id, lease.work_id)
            or self.actor_resolver(actor.member_id) != actor
            or context.cancelled.is_set()
        ):
            raise DomainError(
                "scope_violation", "Import context does not match its execution lease"
            )
        with self.store.transaction(actor) as tx:
            memory_lock(tx)
            tx.assert_lease(lease)
            current = tx.get_work(work.id)
            expected = current.scope.get("source_fingerprints", {})
            if not isinstance(expected, dict):
                raise DomainError("invalid_input", "Work source fingerprints are invalid")
            retained = current.scope.get("retained_fact_source_ids", [])
            for identifier, fingerprint in expected.items():
                source_rows(
                    tx,
                    [identifier],
                    allow_expired=isinstance(retained, list) and identifier in retained,
                    require_complete=True,
                )
                if tx.event_fingerprint(identifier) != fingerprint:
                    raise DomainError(
                        "source_unavailable", "Work source changed after authorization"
                    )
            self._source(tx, current.request_event_id)
            return current

    def _work_event(self, context: ToolContext, work: WorkOrder, event_id: str) -> GroupEvent:
        with self.store.transaction(context.actor) as tx:
            expected = work.scope.get("source_fingerprints", {})
            if (
                not isinstance(expected, dict)
                or event_id not in expected
                or not isinstance(expected[event_id], str)
            ):
                raise DomainError(
                    "permission_denied", "Attachment source is outside authorized work evidence"
                )
            return self._source(tx, event_id, str(expected[event_id]))

    @staticmethod
    def _public_image_input(original: ArtifactVersion, store: ArtifactStore) -> dict[str, Any]:
        """Expose bounded citations only from an authorized immutable image version."""
        if original.format not in _IMAGE_FORMATS:
            return {}
        scope = ArtifactScope(original.group_id, original.work_id, original.created_by)
        lineage = resolve_public_image(store, scope, original)
        if lineage is None:
            return {}
        provenance = lineage.public_image
        return {
            "public_image": provenance,
            "page_citation": lineage.page_citation,
            "image_citation": lineage.image_citation,
        }

    async def _import_bytes(
        self,
        context: ToolContext,
        documents: IsolatedDocumentService,
        media: MediaService,
        source: MediaSource,
        name: str,
        content: bytes,
        *,
        extra: dict[str, Any] | None = None,
    ) -> ArtifactVersion:
        self._check_input_size(source.declared_kind, name, len(content))
        suffix = Path(name).suffix.lower().lstrip(".")
        if source.declared_kind == "image" or suffix in _IMAGE_FORMATS:
            reference = await media.ingest_image(
                context, source, content, research_provenance=(extra or {}).get("public_image")
            )
            return self.artifacts.get(media.scope(context), reference.file_id, reference.version_id)
        if source.declared_kind == "audio" or suffix in _AUDIO_FORMATS:
            reference = await media.ingest_audio(context, source, content)
            return self.artifacts.get(media.scope(context), reference.file_id, reference.version_id)
        if suffix not in _DOCUMENT_FORMATS:
            raise DomainError(
                "unsupported_format",
                "Only supported images, audio and four document formats can be imported",
            )
        return await documents.import_document(
            context,
            name,
            content,
            provenance={**source.model_dump(mode="json"), **(extra or {}), "purpose": "input"},
        )

    async def import_for_work(
        self,
        work: WorkOrder,
        lease: ExecutionLease,
        context: ToolContext,
        documents: IsolatedDocumentService,
        media: MediaService,
    ) -> list[dict[str, Any]]:
        current = await asyncio.to_thread(self._work_guard, work, lease, context)
        raw = current.scope.get("attachment_event_ids", [])
        if (
            not isinstance(raw, list)
            or len(raw) > self.max_input_files
            or any(not isinstance(value, str) for value in raw)
            or len(set(raw)) != len(raw)
        ):
            raise DomainError(
                "invalid_input",
                "Work attachments exceed the distinct authorized file limit",
            )
        event_ids = [str(value) for value in raw]
        if len(current.input_versions) > self.max_input_files:
            raise DomainError("invalid_input", "Too many explicit artifact inputs")
        imported: list[dict[str, Any]] = []
        total_bytes, images = 0, 0

        def admit(content: bytes, *, is_image: bool) -> None:
            nonlocal total_bytes, images
            total_bytes += len(content)
            images += int(is_image)
            if total_bytes > self.max_total_input_bytes or images > self.max_images:
                raise DomainError(
                    "input_limit", "Total selected inputs exceed the work byte/image limit"
                )

        for event_id in event_ids:
            event = await asyncio.to_thread(self._work_event, context, current, event_id)
            for sequence, handle in enumerate(self._handles(event)):
                if len(imported) >= self.max_input_files:
                    raise DomainError("input_limit", "Work exceeds its selected file limit")
                attachment, fingerprint = _attachment_key(handle), source_fingerprint(event)
                cache_key = current.id + ":" + str(lease.goal_revision)
                with self.store.transaction(context.actor) as tx:
                    memory_lock(tx)
                    self._source(tx, event.id, fingerprint)
                    row = self._cache_row(tx, event, attachment, fingerprint, create=True)
                    assert row is not None
                    cached = row["body"].get("imports", {}).get(cache_key)
                    reference = row["body"].get("reference")
                if cached:
                    await asyncio.to_thread(self._work_guard, current, lease, context)
                    cached_version, cached_bytes = self.registry.read(
                        context.actor, cached["version_id"]
                    )
                    self._check_input_size(
                        "image" if cached_version.format in _IMAGE_FORMATS else "file",
                        cached_version.name,
                        len(cached_bytes),
                    )
                    admit(cached_bytes, is_image=cached_version.format in _IMAGE_FORMATS)
                    imported.append(cached)
                    continue
                if reference:
                    media_ref = MediaRef.model_validate(reference)
                    if (
                        media_ref.source.group_id,
                        media_ref.source.work_id,
                        media_ref.source.source_event_id,
                        media_ref.source.member_id,
                    ) != (event.group_id, "input:" + event.id, event.id, event.actor_id):
                        raise DomainError(
                            "scope_violation", "Cached input reference does not match its source"
                        )
                    original = self.artifacts.get(
                        ArtifactScope(event.group_id, "input:" + event.id, event.actor_id),
                        media_ref.file_id,
                        media_ref.version_id,
                    )
                    content = self.artifacts.read(
                        ArtifactScope(event.group_id, "input:" + event.id, event.actor_id),
                        media_ref.file_id,
                        media_ref.version_id,
                    )
                    name, actual_handle = original.name, handle
                else:
                    downloaded = await self._download(event, handle)
                    name = downloaded.handle.filename or (
                        "attachment." + ("bin" if handle.kind == "file" else handle.kind)
                    )
                    content, actual_handle = downloaded.content, downloaded.handle
                await asyncio.to_thread(self._work_guard, current, lease, context)
                await asyncio.to_thread(self._work_event, context, current, event_id)
                self._check_input_size(actual_handle.kind, name, len(content))
                admit(
                    content,
                    is_image=actual_handle.kind == "image"
                    or Path(name).suffix.lower().lstrip(".") in _IMAGE_FORMATS,
                )
                source = self._media_source(event, actual_handle, context.work_id, sequence)
                version = await self._import_bytes(context, documents, media, source, name, content)
                await asyncio.to_thread(self._work_guard, current, lease, context)
                self.registry.register(
                    context.actor, lease, version, source_event_ids=(event.id,), purpose="input"
                )
                item = {
                    "file_id": version.file_id,
                    "version_id": version.version_id,
                    "name": version.name,
                    "format": version.format,
                    "source_event_id": event.id,
                    "source_message_id": source.source_message_id,
                    "member_id": source.member_id,
                    "quote_message_id": source.quote_message_id,
                    "reply_to_event_id": event.reply_to,
                    "attachment_id": source.attachment_id,
                    "untrusted_content": True,
                }
                with self.store.transaction(context.actor) as tx:
                    memory_lock(tx)
                    tx.assert_lease(lease)
                    self._source(tx, event.id, fingerprint)
                    row = self._cache_row(tx, event, attachment, fingerprint)
                    assert row is not None
                    body = dict(row["body"])
                    body["imports"] = {**body.get("imports", {}), cache_key: item}
                    self._put(tx, event, attachment, body)
                imported.append(item)
        for original_id in dict.fromkeys(current.input_versions):
            if len(imported) >= self.max_input_files:
                raise DomainError("input_limit", "Work exceeds its selected file limit")
            await asyncio.to_thread(self._work_guard, current, lease, context)
            original, content = self.registry.read(context.actor, original_id)
            self._check_input_size(
                "image" if original.format in _IMAGE_FORMATS else "file",
                original.name,
                len(content),
            )
            admit(content, is_image=original.format in _IMAGE_FORMATS)
            with self.store.transaction(context.actor) as tx:
                memory_lock(tx)
                catalog = self.registry._row(tx, original_id)
                dependencies = tuple(catalog["source_event_ids"])
                retained = current.scope.get("retained_fact_source_ids", [])
                allowed_expired = (
                    set(retained)
                    if isinstance(retained, list)
                    and catalog.get("validation")
                    and catalog["validation"].get("passed") is True
                    else set()
                )
                expected = current.scope.get("source_fingerprints", {})
                if not isinstance(expected, dict) or not set(dependencies).issubset(expected):
                    raise DomainError(
                        "permission_denied",
                        "Artifact input dependencies were not authorized for this work",
                    )
                for dependency in dependencies:
                    self._source(
                        tx,
                        dependency,
                        str(expected[dependency]),
                        allow_expired=dependency in allowed_expired,
                    )
                event = self._source(tx, current.request_event_id)
                fingerprint = source_fingerprint(event)
                attachment = "artifact:" + original_id
                cache_key = current.id + ":" + str(lease.goal_revision)
                dependency_fingerprints = {key: str(expected[key]) for key in dependencies}
                row = self._cache_row(
                    tx,
                    event,
                    attachment,
                    fingerprint,
                    create=True,
                    additional_fingerprints=dependency_fingerprints,
                )
                assert row is not None
                cached = row["body"].get("imports", {}).get(cache_key)
            original_source = {
                key: value
                for key, value in original.provenance.items()
                if key in MediaSource.model_fields
            }
            source = (
                MediaSource.model_validate(original_source).model_copy(
                    update={"work_id": current.id}
                )
                if original_source
                else MediaSource(
                    group_id=event.group_id,
                    work_id=current.id,
                    source_event_id=event.id,
                    source_message_id=event.platform_message_id or event.id,
                    member_id=event.actor_id,
                    attachment_id="artifact:" + original.file_id + ":" + original.version_id,
                    declared_kind="file",
                )
            )
            if source.group_id != context.group_id or source.source_event_id not in {
                *dependencies,
                event.id,
            }:
                raise DomainError(
                    "scope_violation", "Artifact source association does not match its catalog"
                )
            public_image = self._public_image_input(original, self.artifacts)
            if cached:
                cached_version, cached_bytes = self.registry.read(
                    context.actor, cached["version_id"]
                )
                if (
                    (cached_version.group_id, cached_version.work_id, cached_version.created_by)
                    != (context.group_id, current.id, context.actor.member_id)
                    or cached_version.sha256 != original.sha256
                    or hashlib.sha256(cached_bytes).hexdigest() != original.sha256
                ):
                    raise DomainError(
                        "validation_failed",
                        "Cached artifact input does not match its original version",
                    )
                cached_lineage = (
                    resolve_public_image(
                        self.artifacts,
                        ArtifactScope(
                            cached_version.group_id,
                            cached_version.work_id,
                            cached_version.created_by,
                        ),
                        cached_version,
                    )
                    if public_image
                    else None
                )
                if public_image and cached_lineage is None:
                    # Preserve the immutable legacy copy; the common trusted
                    # import below produces a new verifiable input version.
                    cached = None
            if cached:
                imported.append(
                    {
                        "file_id": cached_version.file_id,
                        "version_id": cached_version.version_id,
                        "name": cached_version.name,
                        "format": cached_version.format,
                        "sha256": cached_version.sha256,
                        "from_version": original_id,
                        "untrusted_content": True,
                        **public_image,
                    }
                )
                continue
            if public_image:
                ref = await ImageTransformService(media).copy_verified(
                    context,
                    original,
                    content,
                    source=source,
                    source_event_ids=tuple(dependencies),
                )
                version = self.artifacts.get(media.scope(context), ref.file_id, ref.version_id)
            else:
                version = await self._import_bytes(
                    context,
                    documents,
                    media,
                    source,
                    original.name,
                    content,
                    extra={
                        "input_version_id": original_id,
                        "source_event_ids": list(dependencies),
                    },
                )
            await asyncio.to_thread(self._work_guard, current, lease, context)
            self.registry.read(context.actor, original_id)
            self.registry.register(
                context.actor, lease, version, source_event_ids=dependencies, purpose="input"
            )
            item = {
                "file_id": version.file_id,
                "version_id": version.version_id,
                "name": version.name,
                "format": version.format,
                "sha256": version.sha256,
                "from_version": original_id,
                "untrusted_content": True,
                **public_image,
            }
            with self.store.transaction(context.actor) as tx:
                memory_lock(tx)
                tx.assert_lease(lease)
                self._source(tx, event.id, fingerprint)
                for dependency in dependencies:
                    self._source(
                        tx,
                        dependency,
                        str(expected[dependency]),
                        allow_expired=dependency in allowed_expired,
                    )
                self.registry._row(tx, original_id)
                row = self._cache_row(
                    tx,
                    event,
                    attachment,
                    fingerprint,
                    additional_fingerprints=dependency_fingerprints,
                )
                assert row is not None
                body = dict(row["body"])
                body["imports"] = {**body.get("imports", {}), cache_key: item}
                self._put(tx, event, attachment, body)
            imported.append(item)
        await asyncio.to_thread(self._work_guard, current, lease, context)
        return imported
