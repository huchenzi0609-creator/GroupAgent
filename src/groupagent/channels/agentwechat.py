"""Scoped AgentWechat HTTP v1 adapter, pinned to the documented v2.2.1 subset.

There are no login/profile discovery, arbitrary URL, video, group mutation, or
read-state operations here. Callers inject credentials and trusted scope.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import math
import re
from datetime import UTC, datetime
from pathlib import PurePath
from typing import Any, Literal, TypeVar
from urllib.parse import quote, urlsplit
from uuid import NAMESPACE_URL, uuid5

import httpx
from pydantic import BaseModel, JsonValue, SecretStr, ValidationError

from groupagent.domain import DomainError, EventKind, GroupEvent

from .attachments import preferred_attachment_variants
from .models import (
    AgentWechatConfig,
    AttachmentContent,
    AttachmentHandle,
    ChannelCapabilities,
    ChannelCursor,
    ChannelEvent,
    ChannelReceipt,
    EventBatch,
    MemberPage,
    MemberRecord,
    SkippedChannelEvent,
    TransferReceipt,
    stable_key,
    valid_identifier,
    wire_key,
)
from .wire import NativeAsset, NativeAssets, NativeEvents, NativeLinkCard, NativeMessage

T = TypeVar("T", bound=BaseModel)
_VIDEO_SUFFIXES = {".mp4", ".mov", ".m4v", ".webm", ".mkv", ".avi", ".3gp", ".3g2", ".mj2"}
_FILE_MIME = {
    ".md": "text/markdown",
    ".pdf": "application/pdf",
    ".docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    ".pptx": "application/vnd.openxmlformats-officedocument.presentationml.presentation",
}
_ASSET_STATES = {"pending", "available", "expired", "unsupported"}
_VARIANTS = {"best", "thumbnail", "standard", "original", "normal", "encoded"}
_CONTROL_EVENTS = {
    "thread.updated",
    "thread.read-state",
    "send.updated",
    "operation.updated",
    "job.updated",
    "media.updated",
    "media.auto-download",
    "media.ready",
    "media.writeback_examined",
    "account.updated",
    "account.state",
    "account.connection",
    "account.push",
    "account.control_observed",
    "account.settings_updated",
    "credentials.projected",
    "contacts.updated",
    "contacts.sync",
    "metadata.changed",
    "sync.started",
    "sync.received",
    "sync.committed",
    "sync.completed",
    "sync.progress",
    "sync.failed",
    "group.refresh_blocked",
    "profile.recovery-pending",
    "projection.blocked",
}
_KNOWN_ERROR_CODES = {
    "not_found",
    "unauthorized",
    "forbidden",
    "event_cursor_expired",
    "pagination_cursor_expired",
    "invalid_cursor",
    "idempotency_conflict",
    "idempotency_key_conflict",
    "media_expired",
    "media_unavailable",
    "media_pending",
    "media_unsupported",
    "message_recalled",
    "account_offline",
    "account_not_found",
    "auth_required",
    "media_asset_not_found",
    "media_not_cached",
    "media_preview_unavailable",
    "media_variant_unavailable",
    "profile_keys_missing",
    "profile_unavailable",
}


def _schema(model: type[T], value: Any) -> T:
    try:
        return model.model_validate(value)
    except (ValidationError, ValueError, TypeError):
        raise DomainError(
            "invalid_channel_response", "AgentWechat response does not match the pinned contract"
        ) from None


def _identifier(value: Any) -> str:
    if not isinstance(value, str) or not value or len(value) > 4096:
        raise DomainError("invalid_channel_response", "Channel identity is missing or invalid")
    try:
        return valid_identifier(value)
    except ValueError as exc:
        raise DomainError("invalid_channel_response", "Channel identity is invalid") from exc


def _server_id(value: Any) -> str:
    if (
        not isinstance(value, str)
        or not re.fullmatch(r"[1-9][0-9]{0,19}", value)
        or int(value) > 2**64 - 1
    ):
        raise DomainError(
            "invalid_channel_response", "Platform message ID must be a positive decimal string"
        )
    return value


def _timestamp(value: Any) -> datetime:
    try:
        if not isinstance(value, str):
            raise ValueError("missing time")
        result = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if result.tzinfo is None:
            raise ValueError("naive time")
        return result.astimezone(UTC)
    except (ValueError, TypeError) as exc:
        raise DomainError(
            "timestamp_unavailable", "Message time must be an aware RFC3339 timestamp"
        ) from exc


def _filename(value: str) -> str:
    if (
        not value
        or len(value.encode()) > 240
        or any(c in value for c in ("/", "\\", "\x00"))
        or any(ord(c) < 32 for c in value)
    ):
        raise DomainError("invalid_attachment", "Attachment filename must not contain a path")
    return value


class AgentWechatAdapter:
    def __init__(
        self, config: AgentWechatConfig, token: str, *, client: httpx.AsyncClient | None = None
    ):
        if (
            not isinstance(token, str)
            or not token
            or any(ord(c) < 33 or ord(c) > 126 for c in token)
        ):
            raise DomainError("invalid_credentials", "An explicit bearer token is required")
        self.config = config
        self._token = SecretStr(token)
        self._owned_client = client is None
        self._client = client or httpx.AsyncClient(
            timeout=config.timeout_seconds,
            follow_redirects=False,
            trust_env=False,
        )
        self._runtime: ChannelCapabilities | None = None
        self._handshake_lock = asyncio.Lock()
        self._send_lock = asyncio.Lock()
        self._issued_cursors: set[str] = set()
        self._last_ack = -1
        self._file_requests: dict[str, str] = {}
        self._media_modes: dict[str, tuple[str, str, str | None]] = {}

    async def __aenter__(self) -> AgentWechatAdapter:
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        if self._owned_client:
            await self._client.aclose()

    def _path(self, suffix: str) -> str:
        return "/v1/accounts/" + quote(self.config.account_id, safe="") + "/" + suffix

    def _scope(self, group_id: str) -> None:
        if group_id != self.config.internal_group_id:
            raise DomainError("scope_denied", "Channel operation is outside the configured group")

    def _cursor(self, value: str, server_id: str) -> ChannelCursor:
        return _schema(
            ChannelCursor,
            {
                "account_id": self.config.account_id,
                "group_id": self.config.internal_group_id,
                "external_group_id": self.config.external_group_id,
                "source_epoch": self.config.source_epoch,
                "daemon_server_id": server_id,
                "value": value,
            },
        )

    def _check_cursor(self, cursor: ChannelCursor) -> None:
        if (cursor.account_id, cursor.group_id, cursor.external_group_id) != (
            self.config.account_id,
            self.config.internal_group_id,
            self.config.external_group_id,
        ):
            raise DomainError("scope_denied", "Cursor belongs to another account or group")
        if cursor.source_epoch != self.config.source_epoch:
            raise DomainError(
                "source_epoch_mismatch", "Cursor cannot cross a profile or source epoch reset"
            )

    async def _request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        body: dict[str, Any] | None = None,
        files: dict[str, Any] | None = None,
        key: str | None = None,
        mutation: bool = False,
        allow_not_found: bool = False,
        binary_limit: int | None = None,
    ) -> Any:
        headers = {
            "Authorization": "Bearer " + self._token.get_secret_value(),
            "Accept": "application/json",
        }
        if key:
            headers["Idempotency-Key"] = key
        if binary_limit is not None:
            headers["Accept"] = "application/octet-stream"
        limit = binary_limit or self.config.max_json_bytes
        try:
            async with self._client.stream(
                method,
                self.config.base_url + path,
                headers=headers,
                params=params,
                json=body,
                files=files,
                follow_redirects=False,
                timeout=self.config.timeout_seconds,
            ) as response:
                if 300 <= response.status_code < 400:
                    raise DomainError(
                        "redirect_rejected",
                        "Channel redirects are not followed",
                        effect_state="unknown" if mutation else None,
                    )
                length = response.headers.get("content-length")
                if length and (not length.isdecimal() or int(length) > limit):
                    raise DomainError(
                        "channel_response_too_large",
                        "Channel response exceeds the configured limit",
                        effect_state="unknown" if mutation else None,
                    )
                chunks = bytearray()
                async for chunk in response.aiter_bytes():
                    chunks.extend(chunk)
                    if len(chunks) > limit:
                        raise DomainError(
                            "channel_response_too_large",
                            "Channel response exceeds the configured limit",
                            effect_state="unknown" if mutation else None,
                        )
                if response.status_code >= 400:
                    code = "channel_http_error"
                    try:
                        raw_code = json.loads(chunks).get("error", {}).get("code")
                        if raw_code in _KNOWN_ERROR_CODES:
                            code = raw_code
                    except (ValueError, AttributeError, TypeError):
                        pass
                    if allow_not_found and response.status_code == 404 and code == "not_found":
                        return None
                    if code in {"event_cursor_expired", "pagination_cursor_expired"}:
                        raise DomainError(
                            "channel_gap",
                            "Channel cursor expired; an explicit bounded recovery is required",
                        )
                    if response.status_code in {401, 403}:
                        raise DomainError(
                            "channel_permission_denied",
                            "AgentWechat refused the configured token scope",
                        )
                    raise DomainError(
                        code,
                        f"AgentWechat request failed with HTTP {response.status_code}",
                        retryable=not mutation and response.status_code >= 500,
                        effect_state="unknown"
                        if mutation and response.status_code >= 500
                        else None,
                    )
                if binary_limit is not None:
                    if response.status_code != 200:
                        raise DomainError(
                            "invalid_attachment", "Expected complete cached attachment bytes"
                        )
                    return bytes(chunks)
                try:
                    value = json.loads(chunks)
                except (ValueError, UnicodeError) as exc:
                    raise DomainError(
                        "invalid_channel_response",
                        "AgentWechat returned invalid JSON",
                        effect_state="unknown" if mutation else None,
                    ) from exc
                if not isinstance(value, dict):
                    raise DomainError(
                        "invalid_channel_response",
                        "Expected an AgentWechat JSON object",
                        effect_state="unknown" if mutation else None,
                    )
                return value
        except httpx.HTTPError:
            raise DomainError(
                "delivery_unknown" if mutation else "channel_unavailable",
                "Channel response was lost; inspect the same key before retrying"
                if mutation
                else "AgentWechat is unavailable",
                retryable=not mutation,
                effect_state="unknown" if mutation else None,
                resource_ref=key,
            ) from None

    async def capabilities(self) -> ChannelCapabilities:
        status = await self._request("GET", "/v1/status")
        identity = status.get("version")
        if (
            not isinstance(identity, dict)
            or identity.get("product") != "agent-wechat"
            or type(identity.get("apiVersion")) is not int
            or identity.get("apiVersion") != 1
        ):
            raise DomainError("incompatible_channel", "AgentWechat HTTP API v1 is required")
        version = identity.get("version")
        if (
            not isinstance(version, str)
            or version.removeprefix("v") != self.config.expected_version
        ):
            raise DomainError(
                "channel_version_mismatch",
                "Daemon version differs from the reviewed adapter contract",
            )
        server_id = _identifier(status.get("serverId"))
        if (
            self.config.expected_daemon_server_id is not None
            and server_id != self.config.expected_daemon_server_id
        ):
            raise DomainError(
                "source_epoch_mismatch",
                "Daemon storage identity differs from the persisted deployment binding",
            )
        backend = identity.get("backend")
        if backend not in {"native", "simulator"} or status.get("state") != "ready":
            raise DomainError("channel_not_ready", "Daemon backend is unsupported or not ready")
        features = identity.get("capabilities")
        if not isinstance(features, list) or not all(
            isinstance(feature, str) for feature in features
        ):
            raise DomainError("invalid_channel_response", "Daemon capabilities are invalid")
        if not {"events", "text-send"}.issubset(features):
            raise DomainError(
                "unsupported_capability", "Required event and text-send capabilities are absent"
            )
        remote = await self._request("GET", self._path("capabilities"))
        upload_limit = remote.get("mediaUploadMaxBytes")
        send_limits = remote.get("mediaSendMaxBytes")
        kinds = remote.get("mediaKinds")
        file_limit = send_limits.get("file") if isinstance(send_limits, dict) else None
        if (
            type(upload_limit) is not int
            or upload_limit <= 0
            or type(file_limit) is not int
            or file_limit <= 0
            or not isinstance(kinds, list)
            or not all(isinstance(kind, str) for kind in kinds)
        ):
            raise DomainError(
                "invalid_channel_response", "Runtime media limits are missing or invalid"
            )
        result = ChannelCapabilities(
            account_id=self.config.account_id,
            group_id=self.config.internal_group_id,
            reported_version=version,
            api_version=1,
            backend=backend,
            daemon_server_id=server_id,
            media_upload_max_bytes=min(upload_limit, self.config.max_send_bytes),
            file_send_max_bytes=min(file_limit, self.config.max_send_bytes),
            image_send_max_bytes=self._runtime_kind_limit(
                send_limits, kinds, "image", 15 * 1024 * 1024
            ),
            voice_send_max_bytes=self._runtime_kind_limit(
                send_limits, kinds, "voice", self.config.max_send_bytes
            ),
            native_voice_reported="voice" in kinds,
            send_kinds=tuple(
                ["text", "file"] + [kind for kind in ("image", "voice") if kind in kinds]
            ),
            metadata_media_kinds=tuple(kind for kind in kinds if kind != "video"),
            upstream_features=tuple(features),
            quoted_text_send_reported=remote.get("quotedTextSend") is True,
        )
        if self._runtime is not None and self._runtime.daemon_server_id != server_id:
            raise DomainError(
                "source_epoch_mismatch", "Daemon storage identity changed; do not reuse its cursors"
            )
        self._runtime = result
        return result

    def _runtime_kind_limit(
        self, limits: dict[str, Any], kinds: list[str], kind: str, maximum: int
    ) -> int | None:
        if kind not in kinds:
            return None
        value = limits.get(kind)
        if type(value) is not int or value <= 0:
            raise DomainError(
                "invalid_channel_response", "Advertised media kind has no positive byte limit"
            )
        return min(value, maximum, self.config.max_send_bytes)

    async def _ensure_runtime(self) -> ChannelCapabilities:
        async with self._handshake_lock:
            return self._runtime or await self.capabilities()

    async def consume(
        self, after_cursor: ChannelCursor | None = None, *, limit: int = 100
    ) -> EventBatch:
        if type(limit) is not int or not 1 <= limit <= 200:
            raise DomainError("invalid_input", "Event page limit must be 1-200")
        if after_cursor:
            self._check_cursor(after_cursor)
        runtime = await self.capabilities()
        if after_cursor and after_cursor.daemon_server_id != runtime.daemon_server_id:
            raise DomainError(
                "source_epoch_mismatch", "Cursor belongs to another daemon storage identity"
            )
        after = after_cursor.value if after_cursor else "0"
        page = _schema(
            NativeEvents,
            await self._request(
                "GET", self._path("events"), params={"after": after, "limit": limit}
            ),
        )
        if len(page.items) > limit:
            raise DomainError("invalid_channel_response", "Event page exceeded its requested limit")
        entries = []
        skipped = []
        previous = int(after)
        for raw in page.items:
            cursor = self._cursor(raw.cursor, runtime.daemon_server_id)
            if int(cursor.value) <= previous:
                raise DomainError("channel_gap", "Event cursors are not strictly increasing")
            previous = int(cursor.value)
            _timestamp(raw.createdAt)
            if raw.data.get("accountId", self.config.account_id) != self.config.account_id:
                raise DomainError("scope_denied", "Event metadata belongs to another account")
            if raw.kind in {"message.committed", "message.recalled"}:
                thread_id = _identifier(raw.data.get("threadId"))
                if thread_id != self.config.external_group_id:
                    skipped.append(
                        SkippedChannelEvent(
                            cursor=cursor.value, kind=raw.kind, reason="outside_group"
                        )
                    )
                    continue
                if raw.kind == "message.recalled":
                    event = self._recall_event(
                        _server_id(raw.data.get("serverId")),
                        _timestamp(raw.data.get("recalledAt") or raw.data.get("observedAt")),
                    )
                else:
                    message_id = _identifier(raw.data.get("messageId"))
                    event = await self.read_message(message_id)
                entries.append(ChannelEvent(event=event, cursor=cursor))
            elif raw.kind == "group.updated":
                group_id = raw.data.get("groupId") or raw.data.get("threadId")
                _identifier(group_id)
                skipped.append(
                    SkippedChannelEvent(
                        cursor=cursor.value,
                        kind=raw.kind,
                        reason="control_event"
                        if group_id == self.config.external_group_id
                        else "outside_group",
                    )
                )
            elif raw.kind in _CONTROL_EVENTS:
                skipped.append(
                    SkippedChannelEvent(cursor=cursor.value, kind=raw.kind, reason="control_event")
                )
            else:
                raise DomainError(
                    "unsupported_event", "Unknown channel event kind requires an adapter update"
                )
        next_cursor = self._cursor(page.nextCursor, runtime.daemon_server_id)
        if int(next_cursor.value) != previous:
            raise DomainError("channel_gap", "Page cursor does not match its final event")
        self._issued_cursors.add(next_cursor.serialize())
        self._issued_cursors.update(entry.cursor.serialize() for entry in entries)
        return EventBatch(entries=tuple(entries), next_cursor=next_cursor, skipped=tuple(skipped))

    async def ack_committed(self, cursor: ChannelCursor) -> None:
        """Called by the durable control layer only AFTER committing the batch."""
        self._check_cursor(cursor)
        if cursor.serialize() not in self._issued_cursors or int(cursor.value) < self._last_ack:
            raise DomainError(
                "invalid_cursor", "Only an emitted, nonregressing cursor may be acknowledged"
            )
        runtime = await self.capabilities()
        if runtime.daemon_server_id != cursor.daemon_server_id:
            raise DomainError(
                "source_epoch_mismatch", "Cursor belongs to another daemon storage identity"
            )
        consumer = (
            "ga-"
            + hashlib.sha256(
                json.dumps([self.config.external_group_id, self.config.consumer_name]).encode()
            ).hexdigest()
        )
        await self._request(
            "POST",
            self._path("consumers/" + consumer + "/ack"),
            body={"cursor": cursor.value},
        )
        self._last_ack = int(cursor.value)
        self._issued_cursors = {
            value
            for value in self._issued_cursors
            if int(ChannelCursor.model_validate_json(value).value) > self._last_ack
        }

    async def _raw_message(self, message_id: str) -> NativeMessage:
        _identifier(message_id)
        message = _schema(
            NativeMessage,
            await self._request("GET", self._path("messages/" + quote(message_id, safe=""))),
        )
        if (
            message.id != message_id
            or message.accountId != self.config.account_id
            or message.threadId != self.config.external_group_id
        ):
            raise DomainError(
                "scope_denied", "Message identity does not match the configured account and group"
            )
        if message.direction not in {None, "incoming", "outgoing"}:
            raise DomainError("invalid_channel_response", "Unknown message direction")
        if message.serverId is not None:
            _server_id(message.serverId)
        return message

    async def read_message(self, message_id: str) -> GroupEvent:
        await self._ensure_runtime()
        message = await self._raw_message(message_id)
        attachments: list[dict[str, JsonValue]] = []
        reply_to = None
        if message.recall is not None:
            return self._recall_event(
                _server_id(message.serverId),
                _timestamp(message.recall.get("recalledAt") or message.recall.get("observedAt")),
            )
        # Only the documented system-notice type may lack a sender. A metadata-only
        # unsupported-content event must still preserve a verified real identity.
        sender = _identifier(
            "system:agentwechat"
            if message.senderId is None and message.type == 10000
            else message.senderId
        )
        platform_id = _server_id(message.serverId) if message.serverId is not None else None
        source = stable_key(self.config, platform_id or message.id, server=platform_id is not None)
        occurred_at = _timestamp(message.createdAt)
        if message.type in {43, 62}:
            kind = EventKind.UNSUPPORTED_VIDEO
            text = ""
            attachments.append(
                {
                    "kind": "unsupported_video",
                    "availability": "unsupported",
                    "reason": "video_out_of_scope",
                }
            )
        elif message.type == 1:
            kind, text = EventKind.TEXT, message.body or ""
        elif message.type == 10000:
            kind, text = EventKind.SYSTEM, message.body or ""
        elif message.type == 49 and message.appMessageType == 5:
            link = self._link_card(message.link)
            if link is None:
                kind, text = EventKind.SYSTEM, ""
                attachments.append(self._unsupported_content(message, "link_metadata_unavailable"))
            else:
                kind = EventKind.TEXT
                title, url = link
                text = "[Link]" + (" " + title if title else "") + ("\n" + url if url else "")
                attachments.append(
                    {
                        "kind": "link",
                        "title": title,
                        "url": url,
                        "fetched": False,
                        "untrusted_content": True,
                    }
                )
        elif message.type in {3, 34, 47} or (
            message.type == 49 and message.appMessageType in {6, 8}
        ):
            kind = (
                EventKind.AUDIO
                if message.type == 34
                else EventKind.FILE
                if message.appMessageType == 6
                else EventKind.IMAGE
            )
            text = message.body or ""
            attachments.append(self._unresolved_handle(message, kind.value).as_attachment())
        elif message.type == 49 and message.appMessageType == 57:
            kind, text = EventKind.QUOTE, message.body or ""
            quoted = message.quote
            if quoted is None or quoted.state not in {
                "embedded",
                "missing",
                "unsupported",
                "recalled",
            }:
                raise DomainError(
                    "invalid_channel_response", "Quote metadata is absent or has an unknown state"
                )
            quote_id = _server_id(quoted.serverId)
            reply_to = str(uuid5(NAMESPACE_URL, stable_key(self.config, quote_id, server=True)))
            attachments.append(
                {
                    "kind": "quote",
                    "server_id": quote_id,
                    "sender_id": quoted.senderId,
                    "state": quoted.state,
                    "text": quoted.embeddedText if quoted.state == "embedded" else None,
                    "media_availability": "unsupported_video"
                    if quoted.type in {43, 62}
                    else "not_fetched",
                }
            )
            if (
                quoted.state == "embedded"
                and quoted.type not in {43, 62}
                and (quoted.embeddedImage or quoted.embeddedSticker)
            ):
                attachments.append(self._unresolved_handle(message, "image").as_attachment())
        elif message.type == 49 and message.appMessageType == 19:
            kind, text = EventKind.FORWARDED, message.body or ""
            attachments.append(
                {
                    "kind": "forwarded",
                    "availability": "unavailable",
                    "reason": "embedded_attachments_not_retrieved",
                }
            )
        else:
            kind, text = EventKind.SYSTEM, ""
            attachments.append(self._unsupported_content(message, "message_content_not_supported"))
        if len(text.encode()) > 256 * 1024:
            raise DomainError("message_too_large", "Normalized message text exceeds its limit")
        return GroupEvent(
            id=str(uuid5(NAMESPACE_URL, source)),
            source_key=source,
            group_id=self.config.internal_group_id,
            actor_id=sender,
            kind=kind,
            text=text,
            occurred_at=occurred_at,
            received_at=datetime.now(UTC),
            reply_to=reply_to,
            attachments=attachments,
            channel_account_id=self.config.account_id,
            source_epoch=None if platform_id else self.config.source_epoch,
            platform_message_id=platform_id,
            trusted_actor_origin="agentwechat_authenticated_message_metadata",
            policy_revision=self.config.policy_revision,
        )

    @staticmethod
    def _unsupported_content(message: NativeMessage, reason: str) -> dict[str, JsonValue]:
        # No body, XML, location, payment or arbitrary card fields are persisted.
        return {
            "kind": "unsupported_content",
            "message_type": message.type,
            "app_message_type": message.appMessageType,
            "availability": "unsupported",
            "reason": reason,
        }

    @staticmethod
    def _link_card(raw: Any) -> tuple[str | None, str | None] | None:
        """Project documented card fields only; do not fetch or derive URLs from XML."""
        try:
            card = NativeLinkCard.model_validate(raw)
            title = card.title.strip() if card.title else None
            url = card.url.strip() if card.url else None
            if title:
                title.encode("utf-8")
                if any(ord(char) < 32 or ord(char) == 127 for char in title):
                    return None
            if url:
                url.encode("utf-8")
                parsed = urlsplit(url)
                if (
                    parsed.scheme not in {"http", "https"}
                    or not parsed.hostname
                    or parsed.username is not None
                    or parsed.password is not None
                    or "\\" in url
                    or any(char.isspace() or ord(char) < 32 or ord(char) == 127 for char in url)
                    or parsed.port is not None
                    and not 1 <= parsed.port <= 65535
                    or PurePath(parsed.path).suffix.lower() in _VIDEO_SUFFIXES
                ):
                    return None
            return (title, url) if title or url else None
        except (ValueError, UnicodeError):
            return None

    def _recall_event(self, platform_id: str, occurred_at: datetime) -> GroupEvent:
        original = stable_key(self.config, platform_id, server=True)
        source = original + ":recall"
        return GroupEvent(
            id=str(uuid5(NAMESPACE_URL, source)),
            source_key=source,
            group_id=self.config.internal_group_id,
            actor_id="system:agentwechat",
            kind=EventKind.RECALL,
            text="[Message recalled]",
            occurred_at=occurred_at,
            received_at=datetime.now(UTC),
            reply_to=str(uuid5(NAMESPACE_URL, original)),
            channel_account_id=self.config.account_id,
            platform_message_id=platform_id,
            trusted_actor_origin="agentwechat_authenticated_recall_metadata",
            policy_revision=self.config.policy_revision,
        )

    def _unresolved_handle(self, message: NativeMessage, kind: str) -> AttachmentHandle:
        """An immutable message reference; availability belongs to a later metadata read."""
        return _schema(
            AttachmentHandle,
            {
                "account_id": self.config.account_id,
                "group_id": self.config.internal_group_id,
                "external_group_id": self.config.external_group_id,
                "source_epoch": self.config.source_epoch,
                "message_id": message.id,
                "platform_message_id": message.serverId,
                "asset_id": None,
                "kind": kind,
                "variant": "best",
                "availability": "unresolved",
            },
        )

    async def attachment_handles(self, message_id: str) -> tuple[AttachmentHandle, ...]:
        await self._ensure_runtime()
        message = await self._raw_message(message_id)
        if message.recall is not None:
            raise DomainError("message_recalled", "Recalled attachments cannot be fetched")
        if message.type in {43, 62} or (
            message.quote is not None and message.quote.type in {43, 62}
        ):
            raise DomainError("unsupported_video", "Video metadata is not requested")
        return tuple(self._handle(message, asset) for asset in await self._assets(message))

    async def _assets(self, message: NativeMessage) -> list[NativeAsset]:
        if message.type in {43, 62}:
            raise DomainError("unsupported_video", "Video attachments are never requested")
        supported_source = (
            message.type in {3, 34, 47}
            or (message.type == 49 and message.appMessageType in {6, 8})
            or (
                message.type == 49
                and message.appMessageType == 57
                and message.quote is not None
                and message.quote.state == "embedded"
                and (message.quote.embeddedImage or message.quote.embeddedSticker)
            )
        )
        if not supported_source:
            raise DomainError(
                "unsupported_attachment", "This message has no supported attachment source"
            )
        result = _schema(
            NativeAssets,
            await self._request(
                "GET", self._path("messages/" + quote(message.id, safe="") + "/media")
            ),
        )
        if len(result.items) > 20 or len({asset.id for asset in result.items}) != len(result.items):
            raise DomainError(
                "invalid_attachment", "Media metadata has too many or duplicate assets"
            )
        for asset in result.items:
            _identifier(asset.id)
            if asset.accountId != self.config.account_id or asset.messageId != message.id:
                raise DomainError(
                    "scope_denied", "Attachment does not belong to this message/account"
                )
            if (
                asset.state not in _ASSET_STATES
                or asset.variant not in _VARIANTS
                or asset.kind not in {"image", "voice", "file", "video", "sticker"}
            ):
                raise DomainError(
                    "unsupported_attachment", "Unknown attachment kind, variant or state"
                )
            expected_kinds = (
                {"voice"}
                if message.type == 34
                else {"file"}
                if message.appMessageType == 6
                else {"sticker", "image"}
                if message.type == 47 or message.appMessageType in {8, 57}
                else {"image"}
                if message.type == 3
                else set()
            )
            if not self._is_video_asset(asset) and asset.kind not in expected_kinds:
                raise DomainError(
                    "invalid_attachment", "Attachment kind disagrees with its source message"
                )
        return result.items

    @staticmethod
    def _is_video_asset(asset: NativeAsset) -> bool:
        return (
            asset.kind == "video"
            or (asset.contentType or "").lower().startswith("video/")
            or PurePath(asset.filename or "").suffix.lower() in _VIDEO_SUFFIXES
        )

    def _handle(self, message: NativeMessage, asset: NativeAsset) -> AttachmentHandle:
        if self._is_video_asset(asset):
            raise DomainError("unsupported_video", "Video attachments are outside this adapter")
        if asset.filename is not None:
            _filename(asset.filename)
        if asset.sha256 is not None and not re.fullmatch(r"[a-f0-9]{64}", asset.sha256):
            raise DomainError("invalid_attachment", "Invalid attachment hash")
        if asset.state == "available" and (
            asset.byteLength is None or asset.sha256 is None or asset.contentType is None
        ):
            raise DomainError(
                "invalid_attachment", "Available attachments require size, hash and media type"
            )
        kind = (
            "audio"
            if asset.kind == "voice"
            else "image"
            if asset.kind in {"image", "sticker"}
            else "file"
        )
        return _schema(
            AttachmentHandle,
            {
                "account_id": self.config.account_id,
                "group_id": self.config.internal_group_id,
                "external_group_id": self.config.external_group_id,
                "source_epoch": self.config.source_epoch,
                "message_id": message.id,
                "platform_message_id": message.serverId,
                "asset_id": asset.id,
                "kind": kind,
                "variant": asset.variant,
                "availability": asset.state,
                "content_type": asset.contentType,
                "size_bytes": asset.byteLength,
                "advertised_size_bytes": asset.advertisedByteLength,
                "sha256": asset.sha256,
                "filename": asset.filename,
                "reason": asset.reason,
            },
        )

    async def _fresh_handle(self, handle: AttachmentHandle) -> AttachmentHandle:
        if (handle.account_id, handle.group_id, handle.external_group_id, handle.source_epoch) != (
            self.config.account_id,
            self.config.internal_group_id,
            self.config.external_group_id,
            self.config.source_epoch,
        ):
            raise DomainError("scope_denied", "Attachment handle is outside the configured scope")
        await self._ensure_runtime()
        message = await self._raw_message(handle.message_id)
        if (
            handle.platform_message_id is not None
            and handle.platform_message_id != message.serverId
        ):
            raise DomainError(
                "scope_denied", "Local attachment reference now identifies another platform message"
            )
        if message.type in {43, 62} or (
            message.quote is not None and message.quote.type in {43, 62}
        ):
            raise DomainError("unsupported_video", "Video attachments are never fetched")
        if message.recall is not None:
            raise DomainError("message_recalled", "Recalled attachment cannot be fetched")
        assets = await self._assets(message)
        if any(self._is_video_asset(asset) for asset in assets):
            raise DomainError("unsupported_video", "Video attachments are never fetched")
        candidates = [
            self._handle(message, asset)
            for asset in assets
            if (handle.asset_id is None or asset.id == handle.asset_id)
            and (handle.variant == "best" or asset.variant == handle.variant)
        ]
        available = [value for value in candidates if value.availability == "available"]
        choices = preferred_attachment_variants(available or candidates, handle.variant)
        if not choices:
            raise DomainError(
                "scope_denied", "Attachment is not listed under the scoped source message"
            )
        if len(choices) != 1:
            raise DomainError(
                "ambiguous_attachment", "Attachment metadata contains multiple matching assets"
            )
        fresh = choices[0]
        if fresh.kind != handle.kind:
            raise DomainError("scope_denied", "Attachment kind disagrees with its source message")
        return fresh

    async def fetch_attachment(self, handle: AttachmentHandle) -> AttachmentContent:
        fresh = await self._fresh_handle(handle)
        if handle.sha256 is not None and handle.sha256 != fresh.sha256:
            raise DomainError(
                "attachment_version_conflict",
                "Attachment bytes changed since this handle was issued",
            )
        if fresh.availability != "available":
            raise DomainError(
                "media_" + fresh.availability,
                "Attachment bytes are not available in the daemon cache",
            )
        if fresh.size_bytes is None or fresh.size_bytes > self.config.max_attachment_bytes:
            raise DomainError(
                "attachment_too_large", "Attachment exceeds this adapter's byte limit"
            )
        assert fresh.asset_id is not None
        content = await self._request(
            "GET",
            self._path("media/" + quote(fresh.asset_id, safe="") + "/content"),
            binary_limit=self.config.max_attachment_bytes,
        )
        digest = hashlib.sha256(content).hexdigest()
        if len(content) != fresh.size_bytes or digest != fresh.sha256:
            raise DomainError(
                "media_integrity_failed", "Downloaded bytes do not match attachment metadata"
            )
        return AttachmentContent(handle=fresh, content=content, sha256=digest)

    async def request_attachment_download(self, handle: AttachmentHandle) -> TransferReceipt:
        fresh = await self._fresh_handle(handle)
        if fresh.availability != "pending":
            raise DomainError(
                "invalid_transfer_state", "Only pending supported attachments start a download"
            )
        expected_size = (
            fresh.size_bytes if fresh.size_bytes is not None else fresh.advertised_size_bytes
        )
        if expected_size is None:
            raise DomainError(
                "attachment_size_unknown", "An uncached download needs an advertised byte bound"
            )
        if expected_size > self.config.max_attachment_bytes:
            raise DomainError(
                "attachment_too_large", "Attachment exceeds this adapter's byte limit"
            )
        await self.capabilities()
        job = await self._request(
            "POST",
            self._path("messages/" + quote(fresh.message_id, safe="") + "/media-transfers"),
            body={
                "messageId": fresh.message_id,
                "assetId": fresh.asset_id,
                "quality": fresh.variant,
            },
            mutation=True,
        )
        assert fresh.asset_id is not None
        if (
            job.get("accountId") != self.config.account_id
            or job.get("kind") != "download"
            or job.get("assetId") != fresh.asset_id
        ):
            raise DomainError("scope_denied", "Transfer receipt refers to another attachment")
        if job.get("state") not in {
            "queued",
            "running",
            "available",
            "needs_review",
            "cancelled",
            "expired",
            "unsupported",
            "failed",
        }:
            raise DomainError("unknown_transfer_state", "Transfer returned an unknown state")
        return TransferReceipt(
            group_id=self.config.internal_group_id,
            message_id=fresh.message_id,
            asset_id=fresh.asset_id,
            job_id=_identifier(job.get("id")),
            state=job["state"],
        )

    def _member(self, raw: dict[str, Any], *, present: bool) -> MemberRecord:
        member_id = _identifier(raw.get("id"))
        if raw.get("username") != member_id:
            raise DomainError("invalid_channel_response", "Roster member identity is inconsistent")
        name = raw.get("roomDisplayName")
        if name is not None and not isinstance(name, str):
            raise DomainError("invalid_channel_response", "Group display name is invalid")
        name = name.strip() if name and name.strip() else None
        return MemberRecord(
            group_id=self.config.internal_group_id,
            member_id=member_id,
            room_display_name=name,
            membership_state="present" if present else "unconfirmed",
            name_source="roomDisplayName" if name else "unknown",
        )

    async def members(self, *, cursor: str | None = None, limit: int = 100) -> MemberPage:
        if (
            type(limit) is not int
            or not 1 <= limit <= 200
            or (cursor is not None and len(cursor) > 8192)
        ):
            raise DomainError("invalid_input", "Invalid roster pagination")
        await self._ensure_runtime()
        params: dict[str, Any] = {"limit": limit}
        if cursor is not None:
            params["cursor"] = cursor
        page = await self._request(
            "GET",
            self._path("groups/" + quote(self.config.external_group_id, safe="") + "/members"),
            params=params,
        )
        items = page.get("items")
        next_cursor = page.get("nextCursor")
        if (
            not isinstance(items, list)
            or len(items) > limit
            or not all(isinstance(item, dict) for item in items)
            or (next_cursor is not None and not isinstance(next_cursor, str))
        ):
            raise DomainError("invalid_channel_response", "Invalid roster page")
        members = []
        for item in items:
            if item.get("deleted") is True:
                members.append(
                    MemberRecord(
                        group_id=self.config.internal_group_id,
                        member_id=_identifier(item.get("id")),
                        room_display_name=None,
                        membership_state="unconfirmed",
                        name_source="unknown",
                    )
                )
            else:
                members.append(self._member(item, present=True))
        coverage = page.get("coverage")
        partial = not (isinstance(coverage, dict) and coverage.get("partial") is False)
        return MemberPage(
            members=tuple(members),
            next_cursor=next_cursor,
            source_generation=_identifier(page.get("sourceGeneration")),
            partial=partial,
        )

    async def resolve_member(self, member_id: str) -> MemberRecord:
        _identifier(member_id)
        await self._ensure_runtime()
        result = await self._request(
            "GET",
            self._path(
                "groups/"
                + quote(self.config.external_group_id, safe="")
                + "/members/"
                + quote(member_id, safe="")
            ),
        )
        if (
            result.get("groupId") != self.config.external_group_id
            or result.get("username") != member_id
        ):
            raise DomainError("scope_denied", "Member lookup returned another group or identity")
        if result.get("coverage") != "cached" or result.get("state") not in {
            "present",
            "unconfirmed",
        }:
            raise DomainError("invalid_channel_response", "Unknown member lookup state or coverage")
        if result["state"] == "unconfirmed" and result.get("member") is None:
            return MemberRecord(
                group_id=self.config.internal_group_id,
                member_id=member_id,
                room_display_name=None,
                membership_state="unconfirmed",
                name_source="unknown",
            )
        member = result.get("member")
        if (
            result["state"] != "present"
            or not isinstance(member, dict)
            or member.get("id") != member_id
        ):
            raise DomainError("invalid_channel_response", "Inconsistent member lookup")
        return self._member(member, present=True)

    def _key(self, key: str) -> str:
        try:
            return wire_key(self.config, key)
        except ValueError as exc:
            raise DomainError(
                "invalid_idempotency_key", "A stable nonempty idempotency key is required"
            ) from exc

    def _receipt(self, raw: dict[str, Any], key: str) -> ChannelReceipt:
        is_job = "kind" in raw
        destination = raw.get("threadId") if is_job else raw.get("destination")
        if (
            raw.get("accountId") != self.config.account_id
            or destination != self.config.external_group_id
        ):
            raise DomainError(
                "scope_denied",
                "Send receipt belongs to another account or group",
                effect_state="unknown",
            )
        if is_job and raw.get("kind") != "send":
            raise DomainError("invalid_receipt", "Expected a send job", effect_state="unknown")
        raw_id = _identifier(raw.get("id"))
        platform_id = raw.get("serverMessageId") if is_job else raw.get("serverId")
        if platform_id is not None:
            platform_id = _server_id(platform_id)
        try:
            return ChannelReceipt.model_validate(
                {
                    "account_id": self.config.account_id,
                    "group_id": self.config.internal_group_id,
                    "idempotency_key": key,
                    "wire_idempotency_key": self._key(key),
                    "state": raw.get("state"),
                    "send_id": None if is_job else raw_id,
                    "job_id": raw_id if is_job else None,
                    "platform_message_id": platform_id,
                    "local_message_id": raw.get("localMessageId"),
                    "reason": raw.get("reason") if is_job else raw.get("errorCode"),
                    "observed_at": datetime.now(UTC),
                }
            )
        except (ValidationError, ValueError, TypeError) as exc:
            raise DomainError(
                "invalid_receipt", "Unknown or inconsistent send state", effect_state="unknown"
            ) from exc

    async def send_text(self, group_id: str, text: str, idempotency_key: str) -> ChannelReceipt:
        self._scope(group_id)
        key = self._key(idempotency_key)
        if not isinstance(text, str) or not text.strip() or len(text.encode()) > 64 * 1024:
            raise DomainError("invalid_input", "Text send must contain 1-65536 UTF-8 bytes")
        await self.capabilities()
        raw = await self._request(
            "POST",
            self._path("sends"),
            body={"threadId": self.config.external_group_id, "kind": "text", "text": text},
            key=key,
            mutation=True,
        )
        return self._receipt(raw, idempotency_key)

    async def lookup_delivery(self, group_id: str, key: str) -> ChannelReceipt | None:
        self._scope(group_id)
        wire = self._key(key)
        await self.capabilities()
        raw = await self._request(
            "GET",
            self._path("sends/by-idempotency-key"),
            params={"key": wire},
            allow_not_found=True,
        )
        if raw is None:
            return None
        receipt = self._receipt(raw, key)
        mode = self._media_modes.get(wire)
        if mode:
            receipt = receipt.model_copy(
                update={"requested_kind": mode[0], "wire_kind": mode[1], "fallback_reason": mode[2]}
            )
        return receipt

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
        """Send registry-validated media; full decoding remains in the isolated worker.

        Voice duration is trusted validated metadata, not an untrusted header parsed
        in this channel process. A fallback is explicit and recorded as a file send.
        """
        self._scope(group_id)
        key = self._key(idempotency_key)
        _filename(name)
        suffix = PurePath(name).suffix.lower()
        if suffix in _VIDEO_SUFFIXES or kind not in {"image", "voice"}:
            raise DomainError(
                "unsupported_video"
                if suffix in _VIDEO_SUFFIXES or kind == "video"
                else "unsupported_attachment",
                "Only reviewed image and voice send kinds are supported",
            )
        if not isinstance(content, bytes) or not content:
            raise DomainError("invalid_attachment", "Nonempty validated media bytes are required")
        if len(content) > self.config.max_send_bytes:
            raise DomainError(
                "attachment_too_large", "Media exceeds the project delivery byte limit"
            )
        if (
            content.startswith((b"\x1aE\xdf\xa3", b"FLV"))
            or (content[:4] == b"RIFF" and content[8:12] == b"AVI ")
            or content[4:8] == b"ftyp"
            or (content.startswith(b"OggS") and b"theora" in content[:65536])
        ):
            raise DomainError("unsupported_video", "Video containers are never staged")
        if kind == "image":
            signatures = {
                ".png": (content.startswith(b"\x89PNG\r\n\x1a\n"), "image/png"),
                ".jpg": (content.startswith(b"\xff\xd8\xff"), "image/jpeg"),
                ".jpeg": (content.startswith(b"\xff\xd8\xff"), "image/jpeg"),
                ".gif": (content.startswith((b"GIF87a", b"GIF89a")), "image/gif"),
                ".webp": (content.startswith(b"RIFF") and content[8:12] == b"WEBP", "image/webp"),
            }
            valid, mime = signatures.get(suffix, (False, ""))
            validated_duration = 0.0
        else:
            valid = (
                suffix == ".wav"
                and len(content) >= 44
                and content.startswith(b"RIFF")
                and content[8:12] == b"WAVE"
            ) or (suffix == ".ogg" and content.startswith(b"OggS") and b"OpusHead" in content[:128])
            mime = "audio/wav" if suffix == ".wav" else "audio/ogg"
            if (
                not isinstance(duration_seconds, (float, int))
                or isinstance(duration_seconds, bool)
                or not math.isfinite(duration_seconds)
                or not 0 < duration_seconds <= 600
            ):
                raise DomainError(
                    "media_metadata_required",
                    "Voice send requires validated duration within ten minutes",
                )
            validated_duration = float(duration_seconds)
        if not valid:
            raise DomainError(
                "media_format_mismatch",
                "Media signature does not match its supported filename/kind",
            )
        runtime = await self.capabilities()
        wire_kind: Literal["image", "voice", "file"] = kind
        fallback: str | None = None
        limit = runtime.image_send_max_bytes if kind == "image" else runtime.voice_send_max_bytes
        if kind == "voice" and (limit is None or validated_duration > 60 or len(content) > limit):
            fallback = (
                "native_voice_unavailable"
                if limit is None
                else "native_voice_duration_limit"
                if validated_duration > 60
                else "native_voice_size_limit"
            )
            if allow_file_fallback is not True:
                raise DomainError(
                    fallback,
                    "Native voice cannot carry this artifact; an explicit audio-file fallback is required",
                )
            wire_kind, limit = "file", runtime.file_send_max_bytes
        if limit is None:
            raise DomainError(
                "channel_capability_unavailable", "Runtime does not advertise this media send kind"
            )
        if len(content) > min(runtime.media_upload_max_bytes, limit):
            raise DomainError(
                "attachment_too_large", "Media exceeds the runtime kind/staging limit"
            )
        fingerprint = hashlib.sha256(
            json.dumps(
                [
                    kind,
                    wire_kind,
                    name,
                    round(validated_duration * 1000) if kind == "voice" else None,
                ],
                separators=(",", ":"),
            ).encode()
            + b"\0"
            + content
        ).hexdigest()
        async with self._send_lock:
            previous = self._file_requests.get(key)
            if previous is not None and previous != fingerprint:
                raise DomainError(
                    "idempotency_conflict",
                    "This media-send key is bound to another payload or mode",
                )
            existing = await self.lookup_delivery(group_id, idempotency_key)
            if existing is not None:
                if previous is None:
                    raise DomainError(
                        "delivery_requires_reconciliation",
                        "An earlier media send exists; reconcile the durable intent via lookup_delivery",
                        effect_state="unknown",
                        resource_ref=key,
                    )
                return existing
            self._file_requests[key] = fingerprint
            self._media_modes[key] = (kind, wire_kind, fallback)
            uploaded = await self._request(
                "POST", self._path("uploads"), files={"file": (name, content, mime)}, mutation=True
            )
            if (
                uploaded.get("name") != name
                or uploaded.get("byteLength") != len(content)
                or uploaded.get("sha256") != hashlib.sha256(content).hexdigest()
            ):
                raise DomainError(
                    "media_integrity_failed",
                    "Staged media does not match the validated artifact bytes",
                )
            source_id = _identifier(uploaded.get("id"))
            raw = await self._request(
                "POST",
                self._path("sends"),
                key=key,
                mutation=True,
                body={
                    "threadId": self.config.external_group_id,
                    "kind": wire_kind,
                    "sourceId": source_id,
                    "standardSourceId": None,
                    "thumbnailSourceId": None,
                    "playSeconds": None,
                    "durationMs": round(validated_duration * 1000)
                    if wire_kind == "voice"
                    else None,
                },
            )
            return self._receipt(raw, idempotency_key).model_copy(
                update={"requested_kind": kind, "wire_kind": wire_kind, "fallback_reason": fallback}
            )

    async def send_file(
        self, group_id: str, name: str, content: bytes, idempotency_key: str
    ) -> ChannelReceipt:
        self._scope(group_id)
        key = self._key(idempotency_key)
        _filename(name)
        suffix = PurePath(name).suffix.lower()
        if suffix in _VIDEO_SUFFIXES:
            raise DomainError("unsupported_video", "Video sending is outside this adapter")
        if suffix not in _FILE_MIME:
            raise DomainError(
                "unsupported_attachment", "This send supports MD, DOCX, PDF and PPTX files"
            )
        if not isinstance(content, bytes) or not content:
            raise DomainError("invalid_attachment", "Nonempty attachment bytes are required")
        runtime = await self.capabilities()
        if len(content) > min(runtime.media_upload_max_bytes, runtime.file_send_max_bytes):
            raise DomainError(
                "attachment_too_large", "File exceeds the runtime or project send limit"
            )
        async with self._send_lock:
            fingerprint = hashlib.sha256(name.encode() + b"\x00" + content).hexdigest()
            previous = self._file_requests.get(key)
            if previous is not None and previous != fingerprint:
                raise DomainError(
                    "idempotency_conflict", "The same file-send key cannot describe different bytes"
                )
            existing = await self.lookup_delivery(group_id, idempotency_key)
            if existing is not None:
                if previous is None:
                    raise DomainError(
                        "delivery_requires_reconciliation",
                        "An earlier file send exists; reconcile its durable intent with lookup_delivery",
                        effect_state="unknown",
                        resource_ref=key,
                    )
                return existing
            self._file_requests[key] = fingerprint
            upload = await self._request(
                "POST",
                self._path("uploads"),
                files={"file": (name, content, _FILE_MIME[suffix])},
                mutation=True,
            )
            source_id = _identifier(upload.get("id"))
            if (
                upload.get("byteLength") != len(content)
                or upload.get("sha256") != hashlib.sha256(content).hexdigest()
                or upload.get("name") != name
            ):
                raise DomainError(
                    "media_integrity_failed", "Uploaded source does not match supplied file bytes"
                )
            raw = await self._request(
                "POST",
                self._path("sends"),
                key=key,
                mutation=True,
                body={
                    "threadId": self.config.external_group_id,
                    "kind": "file",
                    "sourceId": source_id,
                    "standardSourceId": None,
                    "thumbnailSourceId": None,
                    "playSeconds": None,
                    "durationMs": None,
                },
            )
            return self._receipt(raw, idempotency_key)
