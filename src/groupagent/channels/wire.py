"""The used subset of the pinned v2.2.1 generated camelCase contracts.

Additive upstream fields are ignored, never projected as authorization or paths.
Unknown discriminants are checked explicitly by the adapter.
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator


class WireModel(BaseModel):
    model_config = ConfigDict(extra="ignore", strict=True, hide_input_in_errors=True)


class NativeQuote(WireModel):
    serverId: str
    type: int
    state: str = "missing"
    senderId: str | None = None
    embeddedText: str | None = None
    embeddedImage: bool = False
    embeddedSticker: bool = False
    appMessageType: int | None = None


class NativeLinkCard(WireModel):
    # Reviewed LinkCard subset; description/sourceName are deliberately not kept.
    title: str | None = Field(max_length=1024)
    url: str | None = Field(max_length=8192)


class NativeMessage(WireModel):
    id: str
    accountId: str
    threadId: str
    serverId: str | None
    senderId: str | None
    type: int
    appMessageType: int | None = None
    quote: NativeQuote | None = None
    # Parse supported link content separately from the authenticated envelope.
    # Malformed/unsupported card content becomes a metadata-only event.
    link: Any = None
    recall: dict[str, Any] | None = None
    direction: str | None
    body: str | None
    createdAt: str | None
    version: str
    mentions: dict[str, Any] = Field(default_factory=dict)
    media: list[Any] = Field(default_factory=list)


class NativeEvent(WireModel):
    cursor: str
    kind: str
    data: dict[str, Any]
    createdAt: str


class NativeEvents(WireModel):
    items: list[NativeEvent]
    nextCursor: str


class NativeAsset(WireModel):
    id: str
    accountId: str
    messageId: str | None
    kind: str
    variant: str
    state: str
    contentType: str | None
    byteLength: int | None
    advertisedByteLength: int | None = None
    sha256: str | None
    filename: str | None
    reason: str | None
    jobId: str | None

    @field_validator("byteLength", "advertisedByteLength")
    @classmethod
    def nonnegative_size(cls, value: int | None) -> int | None:
        if value is not None and value < 0:
            raise ValueError("Negative attachment length")
        return value


class NativeAssets(WireModel):
    items: list[NativeAsset]
