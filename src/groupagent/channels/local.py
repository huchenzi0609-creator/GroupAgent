"""Durable local acceptance transport. Receipts mean local spool acceptance only."""

from __future__ import annotations

import asyncio
import fcntl
import hashlib
import json
import os
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal
from uuid import uuid4

from groupagent.domain import DomainError, GroupEvent

from .models import (
    AgentWechatConfig,
    AttachmentContent,
    AttachmentHandle,
    ChannelCursor,
    ChannelEvent,
    ChannelReceipt,
    EventBatch,
    MemberPage,
    MemberRecord,
    SkippedChannelEvent,
)


class LocalChannel:
    live_channel = False

    def __init__(
        self, root: Path, config: AgentWechatConfig, *, members: tuple[MemberRecord, ...] = ()
    ) -> None:
        self.config, self.members_fixture = config, members
        self.root = root.absolute()
        if self.root != self.root.resolve() or self.root.is_symlink():
            raise DomainError("unsafe_path", "Local channel root must be canonical")
        self.root.mkdir(mode=0o700, parents=True, exist_ok=True)
        self.root.chmod(0o700)
        self._issued: set[str] = set()
        with self._locked():
            manifest = self.root / "identity.json"
            identity = {
                "account": config.account_id,
                "group": config.internal_group_id,
                "external": config.external_group_id,
                "epoch": config.source_epoch,
            }
            if manifest.exists():
                self._safe(manifest)
                old = json.loads(manifest.read_text())
                if any(old.get(k) != v for k, v in identity.items()):
                    raise DomainError("scope_violation", "Local spool is bound to another identity")
                self.server_id = old["server_id"]
            else:
                self.server_id = "local-spool:" + uuid4().hex
                self._atomic(manifest, {**identity, "server_id": self.server_id})
            for name in ("incoming", "outgoing", "assets", "tombstones"):
                target = self.root / name
                if target.exists():
                    self._safe(target, directory=True)
                target.mkdir(mode=0o700, exist_ok=True)

    @staticmethod
    def _safe(path: Path, *, directory: bool = False) -> None:
        if path.is_symlink() or path != path.resolve():
            raise DomainError("unsafe_path", "Local channel paths cannot be redirected")
        if path.exists() and (
            path.stat().st_uid != os.getuid()
            or (not directory and (not path.is_file() or path.stat().st_nlink != 1))
        ):
            raise DomainError("unsafe_path", "Local channel requires owned regular files")

    @contextmanager
    def _locked(self):
        path = self.root / "channel.lock"
        self._safe(path)
        fd = os.open(path, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
        try:
            if os.fstat(fd).st_nlink != 1:
                raise DomainError("unsafe_path", "Local channel lock cannot have aliases")
            fcntl.flock(fd, fcntl.LOCK_EX)
            yield
        finally:
            os.close(fd)

    def _atomic(self, path: Path, value: dict[str, Any]) -> None:
        self._safe(path)
        temp = path.with_name(path.name + "." + uuid4().hex + ".pending")
        fd = os.open(temp, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(value, f, ensure_ascii=False)
            f.flush()
            os.fsync(f.fileno())
        temp.replace(path)

    def append_incoming(self, event: GroupEvent) -> int:
        if event.group_id != self.config.internal_group_id or event.channel_account_id not in {
            None,
            self.config.account_id,
        }:
            raise DomainError("scope_violation", "Local input belongs to another channel")
        if len(event.model_dump_json().encode()) > 256 * 1024:
            raise DomainError("quota_exceeded", "Local input exceeds its size limit")
        with self._locked():
            if any(
                self._deleted(kind, value)
                for kind, value in (
                    ("event", event.id),
                    ("source", event.source_key),
                    ("message", event.platform_message_id),
                )
            ):
                raise DomainError("source_unavailable", "Deleted local source cannot be replayed")
            counter = self.root / "sequence.json"
            self._safe(counter)
            previous = json.loads(counter.read_text())["value"] if counter.exists() else 0
            sequence = previous + 1
            self._atomic(
                self.root / "incoming" / f"{sequence:020}.json",
                {"sequence": sequence, "event": event.model_dump(mode="json")},
            )
            self._atomic(counter, {"value": sequence})
            return sequence

    def _cursor(self, value: int) -> ChannelCursor:
        return ChannelCursor(
            account_id=self.config.account_id,
            group_id=self.config.internal_group_id,
            external_group_id=self.config.external_group_id,
            source_epoch=self.config.source_epoch,
            daemon_server_id=self.server_id,
            value=str(value),
        )

    def _check_cursor(self, cursor: ChannelCursor) -> None:
        if cursor.model_copy(update={"value": "0"}) != self._cursor(0):
            raise DomainError("source_epoch_mismatch", "Local cursor belongs to another spool")

    async def consume(
        self, after_cursor: ChannelCursor | None = None, *, limit: int = 100
    ) -> EventBatch:
        def read():
            if not 1 <= limit <= 1000:
                raise DomainError("invalid_input", "Invalid local batch limit")
            if after_cursor:
                self._check_cursor(after_cursor)
            start = int(after_cursor.value) if after_cursor else 0
            with self._locked():
                counter = self.root / "sequence.json"
                self._safe(counter)
                total = json.loads(counter.read_text())["value"] if counter.exists() else 0
                if start > total:
                    raise DomainError("channel_gap", "Local cursor is ahead of retained events")
                entries = []
                skipped = []
                for sequence in range(start + 1, min(total, start + limit) + 1):
                    path = self.root / "incoming" / f"{sequence:020}.json"
                    self._safe(path)
                    if not path.is_file():
                        raise DomainError("channel_gap", "Local event stream is incomplete")
                    body = json.loads(path.read_text())
                    if body.get("deleted"):
                        skipped.append(
                            SkippedChannelEvent(
                                cursor=str(sequence),
                                kind="privacy_tombstone",
                                reason="control_event",
                            )
                        )
                        continue
                    entries.append(
                        ChannelEvent(
                            event=GroupEvent.model_validate(body["event"]),
                            cursor=self._cursor(sequence),
                        )
                    )
                cursor = self._cursor(min(total, start + limit))
                self._issued.add(cursor.serialize())
                return EventBatch(
                    entries=tuple(entries), next_cursor=cursor, skipped=tuple(skipped)
                )

        return await asyncio.to_thread(read)

    async def ack_committed(self, cursor: ChannelCursor) -> None:
        self._check_cursor(cursor)
        if cursor.serialize() not in self._issued:
            raise DomainError("invalid_cursor", "Only emitted local cursors can be acknowledged")

        def ack():
            with self._locked():
                path = self.root / "ack.json"
                self._safe(path)
                old = json.loads(path.read_text())["value"] if path.exists() else 0
                if int(cursor.value) < old:
                    raise DomainError("invalid_cursor", "Acknowledgement cannot regress")
                self._atomic(path, {"value": int(cursor.value)})
                self._issued.discard(cursor.serialize())

        await asyncio.to_thread(ack)

    def _receipt_path(self, group_id: str, key: str) -> Path:
        if group_id != self.config.internal_group_id or not key or len(key) > 300:
            raise DomainError("scope_violation", "Local send target or key is invalid")
        return self.root / "outgoing" / (hashlib.sha256(key.encode()).hexdigest() + ".json")

    async def lookup_delivery(self, group_id: str, key: str) -> ChannelReceipt | None:
        def lookup():
            with self._locked():
                path = self._receipt_path(group_id, key)
                self._safe(path)
                return (
                    ChannelReceipt.model_validate(json.loads(path.read_text())["receipt"])
                    if path.exists()
                    else None
                )

        return await asyncio.to_thread(lookup)

    async def send_text(self, group_id: str, text: str, idempotency_key: str) -> ChannelReceipt:
        return await asyncio.to_thread(
            self._send, group_id, idempotency_key, {"kind": "text", "text": text}, None
        )

    async def send_file(
        self, group_id: str, name: str, content: bytes, idempotency_key: str
    ) -> ChannelReceipt:
        if (
            Path(name).name != name
            or any(c in name for c in ("/", "\\", "\x00"))
            or len(content) > self.config.max_send_bytes
        ):
            raise DomainError("invalid_attachment", "Local file name or size is invalid")
        if Path(name).suffix.lower() in {".mp4", ".mov", ".mkv", ".webm", ".avi"}:
            raise DomainError("unsupported_video", "Video delivery is not enabled")
        return await asyncio.to_thread(
            self._send,
            group_id,
            idempotency_key,
            {"kind": "file", "name": name, "sha256": hashlib.sha256(content).hexdigest()},
            content,
        )

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
    ) -> ChannelReceipt:
        if kind not in {"image", "voice"} or len(content) > 25 * 1024 * 1024:
            raise DomainError("unsupported_media", "Local media kind or size is not supported")
        fallback = kind == "voice" and (duration_seconds is None or duration_seconds > 60)
        if fallback and not allow_file_fallback:
            raise DomainError("voice_duration_limit", "Validated short speech duration is required")
        payload = {
            "kind": "file" if fallback else kind,
            "name": name,
            "sha256": hashlib.sha256(content).hexdigest(),
            "requested_kind": kind,
            "wire_kind": "file" if fallback else kind,
            "fallback_reason": "local_voice_duration" if fallback else None,
        }
        receipt = await asyncio.to_thread(self._send, group_id, idempotency_key, payload, content)
        return receipt.model_copy(
            update={
                "requested_kind": kind,
                "wire_kind": "file" if fallback else kind,
                "fallback_reason": "local_voice_duration" if fallback else None,
            }
        )

    def add_attachment(
        self, message_id: str, name: str, content: bytes, *, kind: Literal["image", "audio", "file"]
    ) -> AttachmentHandle:
        if (
            kind not in {"image", "audio", "file"}
            or len(content) > 30 * 1024 * 1024
            or Path(name).name != name
        ):
            raise DomainError(
                "invalid_attachment", "Local attachment kind, size or name is invalid"
            )
        if Path(name).suffix.lower() in {".mp4", ".mov", ".webm", ".mkv", ".avi"}:
            raise DomainError("unsupported_video", "Local video ingestion is disabled")
        digest = hashlib.sha256(content).hexdigest()
        identifier = hashlib.sha256((message_id + ":" + name + ":" + digest).encode()).hexdigest()
        handle = AttachmentHandle(
            account_id=self.config.account_id,
            group_id=self.config.internal_group_id,
            external_group_id=self.config.external_group_id,
            source_epoch=self.config.source_epoch,
            message_id=message_id,
            asset_id=identifier,
            kind=kind,
            variant="original",
            availability="available",
            size_bytes=len(content),
            sha256=digest,
            filename=name,
        )
        with self._locked():
            if self._deleted("message", message_id):
                raise DomainError("source_unavailable", "Deleted attachment cannot be replayed")
            target = self.root / "assets" / (identifier + ".payload")
            self._safe(target)
            if not target.exists():
                fd = os.open(target, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
                with os.fdopen(fd, "wb") as output:
                    output.write(content)
            self._atomic(
                self.root / "assets" / (identifier + ".json"),
                {"handle": handle.model_dump(mode="json")},
            )
        return handle

    async def attachment_handles(self, message_id: str) -> tuple[AttachmentHandle, ...]:
        def find():
            with self._locked():
                found = []
                for path in (self.root / "assets").glob("*.json"):
                    self._safe(path)
                    handle = AttachmentHandle.model_validate(json.loads(path.read_text())["handle"])
                    if handle.message_id == message_id:
                        found.append(handle)
                return tuple(found)

        return await asyncio.to_thread(find)

    async def fetch_attachment(self, handle: AttachmentHandle) -> AttachmentContent:
        if (
            (handle.account_id, handle.group_id, handle.external_group_id, handle.source_epoch)
            != (
                self.config.account_id,
                self.config.internal_group_id,
                self.config.external_group_id,
                self.config.source_epoch,
            )
            or not handle.asset_id
            or not __import__("re").fullmatch(r"[a-f0-9]{64}", handle.asset_id)
        ):
            raise DomainError(
                "scope_violation", "Local attachment reference is outside this channel"
            )

        asset_id = handle.asset_id
        assert asset_id is not None

        def read():
            with self._locked():
                manifest = self.root / "assets" / (asset_id + ".json")
                target = self.root / "assets" / (asset_id + ".payload")
                self._safe(manifest)
                self._safe(target)
                if not manifest.exists() or not target.exists():
                    raise DomainError("attachment_expired", "Local attachment is unavailable")
                canonical = AttachmentHandle.model_validate(
                    json.loads(manifest.read_text())["handle"]
                )
                if canonical != handle:
                    raise DomainError("scope_violation", "Local attachment metadata differs")
                data = target.read_bytes()
                if (
                    len(data) != canonical.size_bytes
                    or hashlib.sha256(data).hexdigest() != canonical.sha256
                ):
                    raise DomainError("validation_failed", "Local attachment content changed")
                return AttachmentContent(handle=canonical, content=data, sha256=canonical.sha256)

        return await asyncio.to_thread(read)

    def _send(
        self, group_id: str, key: str, payload: dict[str, Any], content: bytes | None
    ) -> ChannelReceipt:
        fingerprint = hashlib.sha256(
            json.dumps(payload, sort_keys=True, ensure_ascii=False).encode()
        ).hexdigest()
        with self._locked():
            path = self._receipt_path(group_id, key)
            self._safe(path)
            if path.exists():
                old = json.loads(path.read_text())
                if old["fingerprint"] != fingerprint:
                    raise DomainError(
                        "idempotency_conflict", "Local send key was reused for different content"
                    )
                return ChannelReceipt.model_validate(old["receipt"])
            if self._deleted("delivery", key):
                raise DomainError("source_unavailable", "Deleted local delivery cannot be sent")
            if content is not None:
                target = path.with_suffix(".payload")
                self._safe(target)
                if target.exists():
                    if hashlib.sha256(target.read_bytes()).hexdigest() != payload["sha256"]:
                        raise DomainError(
                            "idempotency_conflict", "Unpublished local payload differs"
                        )
                else:
                    fd = os.open(target, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
                    with os.fdopen(fd, "wb") as stream:
                        stream.write(content)
                        stream.flush()
                        os.fsync(stream.fileno())
            receipt = ChannelReceipt(
                account_id=self.config.account_id,
                group_id=group_id,
                idempotency_key=key,
                wire_idempotency_key=key,
                state="accepted",
                platform_message_id="local-only:" + path.stem,
                observed_at=datetime.now(UTC),
                requested_kind=payload.get("requested_kind"),
                wire_kind=payload.get("wire_kind"),
                fallback_reason=payload.get("fallback_reason"),
            )
            self._atomic(
                path,
                {
                    "transport": "local-only",
                    "fingerprint": fingerprint,
                    "payload": payload,
                    "receipt": receipt.model_dump(mode="json"),
                },
            )
            return receipt

    def _marker_path(self, kind: str, value: str) -> Path:
        digest = hashlib.sha256(json.dumps([kind, value]).encode()).hexdigest()
        return self.root / "tombstones" / (digest + ".json")

    def _deleted(self, kind: str, value: str | None) -> bool:
        if not value:
            return False
        path = self._marker_path(kind, value)
        self._safe(path)
        return path.exists()

    async def purge_event(
        self,
        group_id: str,
        event_id: str | None,
        *,
        source_key: str | None = None,
        platform_message_id: str | None = None,
        replacement: GroupEvent | None = None,
        delivery_keys: tuple[str, ...] = (),
        reason: str,
    ) -> str:
        """Trusted local retention hook. Receipts survive; payloads do not."""
        if group_id != self.config.internal_group_id or reason not in {
            "expired",
            "forgotten",
            "recalled",
        }:
            raise DomainError("scope_violation", "Invalid local cleanup scope")
        if replacement is not None and (
            replacement.group_id != group_id
            or replacement.id != event_id
            or (
                replacement.content_state == "active"
                and not any(item.get("privacy_redacted") for item in replacement.attachments)
            )
        ):
            raise DomainError("scope_violation", "Replacement must be a canonical redacted event")

        def purge():
            with self._locked():
                for kind, value in (
                    ("event", event_id),
                    ("source", source_key),
                    ("message", platform_message_id),
                    *(("delivery", key) for key in delivery_keys),
                ):
                    if value:
                        self._atomic(
                            self._marker_path(kind, value), {"kind": kind, "reason": reason}
                        )
                for path in (self.root / "incoming").glob("*.json"):
                    self._safe(path)
                    body = json.loads(path.read_text())
                    event = body.get("event", {})
                    if not event or not (
                        (event_id and event.get("id") == event_id)
                        or (source_key and event.get("source_key") == source_key)
                        or (
                            platform_message_id
                            and event.get("platform_message_id") == platform_message_id
                        )
                    ):
                        continue
                    updated: dict[str, Any] = {"sequence": body["sequence"], "deleted": True}
                    if replacement is not None and replacement.content_state == "active":
                        updated = {
                            "sequence": body["sequence"],
                            "event": replacement.model_dump(mode="json"),
                        }
                    self._atomic(path, updated)
                if platform_message_id:
                    for path in (self.root / "assets").glob("*.json"):
                        self._safe(path)
                        body = json.loads(path.read_text())
                        if body.get("handle", {}).get("message_id") != platform_message_id:
                            continue
                        payload = path.with_suffix(".payload")
                        self._safe(payload)
                        payload.unlink(missing_ok=True)
                        path.unlink()
                for key in delivery_keys:
                    path = self._receipt_path(group_id, key)
                    self._safe(path)
                    payload = path.with_suffix(".payload")
                    self._safe(payload)
                    payload.unlink(missing_ok=True)
                    if path.exists():
                        old = json.loads(path.read_text())
                        self._atomic(
                            path,
                            {
                                "transport": "local-only",
                                "fingerprint": old["fingerprint"],
                                "receipt": old["receipt"],
                                "payload": {},
                                "deleted": True,
                            },
                        )
                for directory in ("incoming", "outgoing", "assets", "tombstones"):
                    fd = os.open(
                        self.root / directory, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
                    )
                    try:
                        os.fsync(fd)
                    finally:
                        os.close(fd)
                return (
                    "local-spool-purge:"
                    + hashlib.sha256(
                        json.dumps([group_id, event_id, sorted(delivery_keys)]).encode()
                    ).hexdigest()
                )

        return await asyncio.to_thread(purge)

    async def members(self, *, cursor: str | None = None, limit: int = 100) -> MemberPage:
        offset = int(cursor or 0)
        values = self.members_fixture[offset : offset + limit]
        end = offset + len(values)
        return MemberPage(
            members=values,
            next_cursor=str(end) if end < len(self.members_fixture) else None,
            source_generation=self.server_id,
            partial=False,
        )

    async def aclose(self) -> None:
        return None
