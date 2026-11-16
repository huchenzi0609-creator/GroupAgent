"""Trusted configuration and scoped, credential-free channel values."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime
from typing import Annotated, Literal
from urllib.parse import urlsplit

from pydantic import BaseModel, ConfigDict, Field, JsonValue, field_validator, model_validator

from groupagent.domain import DeliveryState, GroupEvent

UPSTREAM_COMMIT = "811d090770dd7b682b5bf69d54fc6fbd522cbe9b"
Identifier = Annotated[str, Field(strict=True, min_length=1, max_length=4096)]


class ChannelModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, hide_input_in_errors=True)


def valid_identifier(value: str) -> str:
    if value in {".", ".."} or any(ord(c) < 32 or ord(c) == 127 for c in value):
        raise ValueError("Invalid channel identifier")
    return value


class AgentWechatConfig(ChannelModel):
    base_url: str
    account_id: Identifier
    external_group_id: Identifier
    internal_group_id: Identifier
    source_epoch: Identifier
    policy_revision: int = Field(default=1, strict=True, ge=1)
    expected_version: str = "2.2.1"
    expected_daemon_server_id: str | None = None
    consumer_name: str = Field(default="groupagent", min_length=1, max_length=100)
    timeout_seconds: float = Field(default=30, gt=0, le=60)
    max_json_bytes: int = Field(default=4 * 1024 * 1024, strict=True, gt=0, le=16 * 1024 * 1024)
    max_attachment_bytes: int = Field(
        default=30 * 1024 * 1024, strict=True, gt=0, le=30 * 1024 * 1024
    )
    max_send_bytes: int = Field(default=25 * 1024 * 1024, strict=True, gt=0, le=25 * 1024 * 1024)

    @field_validator(
        "account_id", "external_group_id", "internal_group_id", "source_epoch", "consumer_name"
    )
    @classmethod
    def identifier(cls, value: str) -> str:
        return valid_identifier(value)

    @field_validator("external_group_id")
    @classmethod
    def ordinary_group(cls, value: str) -> str:
        if not value.endswith("@chatroom"):
            raise ValueError("An explicit ordinary WeChat group is required")
        return value

    @field_validator("base_url")
    @classmethod
    def secure_endpoint(cls, value: str) -> str:
        parsed = urlsplit(value)
        if (
            parsed.scheme not in {"http", "https"}
            or not parsed.hostname
            or parsed.username
            or parsed.password
            or parsed.query
            or parsed.fragment
            or parsed.path not in {"", "/"}
            or (
                parsed.scheme == "http" and parsed.hostname not in {"localhost", "127.0.0.1", "::1"}
            )
        ):
            raise ValueError("Use HTTPS or loopback HTTP, without credentials, query or URL path")
        return value.rstrip("/")


class ChannelCursor(ChannelModel):
    account_id: Identifier
    group_id: Identifier
    external_group_id: Identifier
    source_epoch: Identifier
    daemon_server_id: Identifier
    value: str

    @field_validator("value")
    @classmethod
    def decimal_cursor(cls, value: str) -> str:
        if (
            not value.isascii()
            or not value.isdecimal()
            or len(value) > 19
            or int(value) > 2**63 - 1
        ):
            raise ValueError("Event cursor must be a nonnegative signed-64-bit decimal string")
        return str(int(value))

    def serialize(self) -> str:
        return self.model_dump_json()


class ChannelCapabilities(ChannelModel):
    account_id: str
    group_id: str
    reported_version: str
    api_version: int
    backend: str
    daemon_server_id: str
    upstream_commit: str = UPSTREAM_COMMIT
    receive_kinds: tuple[str, ...] = (
        "text",
        "image",
        "audio",
        "file",
        "quote",
        "system",
        "unsupported_video",
    )
    send_kinds: tuple[str, ...] = ("text", "file")
    media_upload_max_bytes: int
    file_send_max_bytes: int
    image_send_max_bytes: int | None = None
    voice_send_max_bytes: int | None = None
    native_voice_reported: bool = False
    metadata_media_kinds: tuple[str, ...]
    upstream_features: tuple[str, ...]
    quoted_text_send_reported: bool
    roster_refresh_supported: bool = True
    incoming_group_updates: Literal["partial", "unknown"] = "unknown"
    video_processing: Literal[False] = False
    recipient_read_receipts: Literal[False] = False
    live_acceptance_verified: Literal[False] = False
    limitations: tuple[str, ...] = (
        "HTTP contract fixtures do not establish live WeChat or recipient-client acceptance",
        "source_epoch is deployment-managed and must change on profile/account reset or ID remapping",
        "Saved roster absence is unconfirmed; roomDisplayName is the only exposed name",
        "Roster reads are cached; remote refresh completion does not establish name completeness",
        "Accepted means WeChat accepted a send, not device delivery or recipient reading",
    )


class ChannelEvent(ChannelModel):
    event: GroupEvent
    cursor: ChannelCursor


class SkippedChannelEvent(ChannelModel):
    cursor: str
    kind: str
    reason: Literal["outside_group", "control_event"]


class EventBatch(ChannelModel):
    entries: tuple[ChannelEvent, ...]
    next_cursor: ChannelCursor
    skipped: tuple[SkippedChannelEvent, ...] = ()
    roster_updates: tuple[ChannelCursor, ...] = ()

    @property
    def events(self) -> tuple[GroupEvent, ...]:
        return tuple(entry.event for entry in self.entries)


class MemberRecord(ChannelModel):
    group_id: str
    member_id: str
    room_display_name: str | None
    membership_state: Literal["present", "unconfirmed"]
    name_source: Literal["roomDisplayName", "unknown"]
    coverage: Literal["cached"] = "cached"


class MemberPage(ChannelModel):
    members: tuple[MemberRecord, ...]
    next_cursor: str | None
    source_generation: str
    partial: bool


class MemberRefreshReceipt(ChannelModel):
    """A completed native metadata request, never a claim of complete names."""

    group_id: str
    operation_id: str
    state: Literal["succeeded"] = "succeeded"
    complete_roster: bool
    member_count: int = Field(strict=True, ge=0, le=100000)
    source: Literal["remote_refresh"] = "remote_refresh"
    reused: bool = False


class AttachmentHandle(ChannelModel):
    account_id: str
    group_id: str
    external_group_id: str
    source_epoch: str
    message_id: str
    platform_message_id: str | None = None
    asset_id: str | None = None
    kind: Literal["image", "audio", "file"]
    variant: str
    availability: Literal["unresolved", "pending", "available", "expired", "unsupported"]
    content_type: str | None = None
    size_bytes: int | None = None
    advertised_size_bytes: int | None = None
    sha256: str | None = None
    filename: str | None = None
    reason: str | None = None

    def as_attachment(self) -> dict[str, JsonValue]:
        return {"kind": self.kind, "handle": self.model_dump(mode="json")}


class AttachmentContent(ChannelModel):
    handle: AttachmentHandle
    content: bytes = Field(repr=False)
    sha256: str


class TransferReceipt(ChannelModel):
    group_id: str
    message_id: str
    asset_id: str
    job_id: str
    state: str


class ChannelReceipt(ChannelModel):
    account_id: str
    group_id: str
    idempotency_key: str
    wire_idempotency_key: str
    state: Literal[
        "queued",
        "submitting",
        "running",
        "accepted",
        "failed",
        "needs_review",
        "cancelled",
        "expired",
        "unsupported",
    ]
    send_id: str | None = None
    job_id: str | None = None
    platform_message_id: str | None = None
    local_message_id: str | None = None
    reason: str | None = None
    observed_at: datetime
    recipient_read: None = None
    requested_kind: Literal["image", "voice"] | None = None
    wire_kind: Literal["image", "voice", "file"] | None = None
    fallback_reason: str | None = None

    @property
    def accepted(self) -> bool:
        return self.state == "accepted"

    @property
    def delivery_state(self) -> DeliveryState:
        if self.state == "accepted":
            return DeliveryState.ACCEPTED
        if self.state == "needs_review":
            return DeliveryState.UNKNOWN
        if self.state in {"failed", "cancelled", "expired", "unsupported"}:
            return DeliveryState.REJECTED
        return DeliveryState.PENDING

    @model_validator(mode="after")
    def accepted_identity(self) -> ChannelReceipt:
        if self.accepted and not self.platform_message_id:
            raise ValueError("Accepted receipt needs a stable platform message ID")
        return self


def stable_key(config: AgentWechatConfig, identity: str, *, server: bool) -> str:
    parts = [
        config.account_id,
        config.external_group_id,
        "server" if server else config.source_epoch,
        identity,
    ]
    digest = hashlib.sha256(
        json.dumps(parts, ensure_ascii=False, separators=(",", ":")).encode()
    ).hexdigest()
    return "agentwechat:" + ("server:" if server else "local:") + digest


def wire_key(config: AgentWechatConfig, key: str) -> str:
    if (
        not isinstance(key, str)
        or not key.strip()
        or len(key) > 200
        or any(ord(c) < 32 or ord(c) == 127 for c in key)
    ):
        raise ValueError("A stable idempotency key of 1-200 characters is required")
    values = [config.account_id, config.external_group_id, key]
    return "ga-" + hashlib.sha256(json.dumps(values, separators=(",", ":")).encode()).hexdigest()
