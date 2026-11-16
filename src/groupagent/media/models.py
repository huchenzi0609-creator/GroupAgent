"""Independent media capabilities and provenance; model input never grants scope."""

from __future__ import annotations

from enum import StrEnum
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field


class MediaModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, hide_input_in_errors=True)


class Capability(StrEnum):
    VISION = "vision"
    OCR = "ocr"
    ASR = "asr"
    TTS = "tts"
    IMAGE_GENERATION = "image_generation"


class MediaSource(MediaModel):
    group_id: str = Field(min_length=1)
    work_id: str = Field(min_length=1)
    source_event_id: str = Field(min_length=1)
    source_message_id: str = Field(min_length=1)
    member_id: str = Field(min_length=1)
    attachment_id: str = Field(min_length=1)
    quote_message_id: str | None = None
    declared_kind: Literal["text", "image", "audio", "file", "unsupported_video"]
    available: bool = True
    unavailable_reason: str | None = None
    sequence: int = Field(default=0, ge=0, le=19)
    origin_kind: Literal["channel", "generated", "research", "synthetic_fixture"] = "channel"
    source_url: str | None = None


class MediaRef(MediaModel):
    file_id: str
    version_id: str
    sha256: str
    format: str
    source: MediaSource
    width: int | None = None
    height: int | None = None
    duration_seconds: float | None = None
    static_first_frame: bool = False
    budget_receipts: tuple[str, ...] = ()


class ImageInfo(MediaModel):
    format: Literal["png", "jpeg", "webp", "gif"]
    mime_type: str
    width: int
    height: int
    byte_length: int
    source_sha256: str
    pixel_sha256: str
    static_first_frame: bool
    frame_count: int
    preview_sha256: str
    alpha_composited_on_white: bool = False


class AudioInfo(MediaModel):
    format: str
    codec: str
    sample_rate: int
    channels: int
    duration_seconds: float
    byte_length: int
    sha256: str
    normalized_sha256: str | None = None
    chunks: tuple[str, ...] = ()


class OCRWord(MediaModel):
    text: str
    confidence: float = Field(ge=0, le=1)
    box: tuple[int, int, int, int]
    pdf_box: tuple[float, float, float, float] | None = None


class OCRPage(MediaModel):
    page_number: int = Field(ge=1)
    text: str
    method: Literal["embedded_text", "tesseract", "deepseek_ocr"]
    confidence: float | None = None
    segmentation_mode: Literal[3, 4, 6, 11] | None = None
    attempted_segmentation_modes: tuple[Literal[3, 4, 6, 11], ...] = ()
    preprocessing: Literal["none", "dark_ink_threshold_32"] = "none"
    words: tuple[OCRWord, ...] = ()
    complete: bool = True
    width_pixels: int | None = None
    height_pixels: int | None = None
    width_points: float | None = None
    height_points: float | None = None


class OCRResult(MediaModel):
    pages: tuple[OCRPage, ...]
    source_sha256: str
    engine: str
    source: MediaSource | None = None
    input_sha256: str | None = None
    static_first_frame: bool = False
    budget_receipts: tuple[str, ...] = ()
    untrusted_content: Literal[True] = True


class CriticalUncertainty(MediaModel):
    field: Literal["name", "number", "date_or_time"]
    text: str
    reason: Literal["confidence_unavailable", "low_confidence"]


class Transcript(MediaModel):
    text: str
    source: MediaSource
    duration_seconds: float
    confidence: float | None
    uncertainty: tuple[CriticalUncertainty, ...] = ()
    needs_clarification: bool = False
    provider: str
    model: str
    request_ids: tuple[str, ...] = ()
    budget_receipts: tuple[str, ...] = ()
    untrusted_content: Literal[True] = True


class VisionImageBinding(MediaModel):
    """Host-verified immutable input and the actual normalized provider view."""

    image_index: int = Field(ge=0, le=9)
    file_id: str = Field(min_length=1)
    version_id: str = Field(min_length=1)
    source: MediaSource
    source_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    normalized_view_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    normalized_pixel_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    width: int = Field(gt=0)
    height: int = Field(gt=0)
    static_first_frame: bool
    alpha_composited_on_white: bool


class VisionBatchResult(MediaModel):
    """One bounded provider view, using indices from the original ordered input."""

    batch_index: int = Field(ge=0, le=9)
    image_indices: tuple[Annotated[int, Field(ge=0, le=9)], ...] = Field(
        min_length=1, max_length=10, description="Zero-based indices in the original ordered input."
    )
    complete: bool
    text: str = ""
    request_id: str | None = None
    budget_receipts: tuple[str, ...] = ()
    error_code: str | None = None


class VisionResult(MediaModel):
    text: str
    sources: tuple[MediaSource, ...]
    # The normalized PNG bytes actually sent to the provider, not source files.
    image_sha256: tuple[str, ...]
    # True for GIF first-frame views; False does not imply identical source bytes.
    static_views: tuple[bool, ...]
    provider: str
    model: str
    request_id: str
    image_bindings: tuple[VisionImageBinding, ...] = Field(default=(), max_length=10)
    budget_receipts: tuple[str, ...] = ()
    # Older receipts remain readable. A split call establishes observations only
    # for the images in each view, never an invented joint visual comparison.
    analysis_scope: Literal["joint_view", "bounded_views"] = "joint_view"
    complete: bool = True
    requested_image_count: int | None = Field(default=None, ge=1, le=10)
    unprocessed_image_indices: tuple[Annotated[int, Field(ge=0, le=9)], ...] = ()
    batches: tuple[VisionBatchResult, ...] = Field(default=(), max_length=10)
    untrusted_content: Literal[True] = True


class ProviderRoute(MediaModel):
    route_id: str = Field(min_length=1)
    provider: Literal["deepseek", "openai"]
    capability: Capability
    model: str
    price_id: str = Field(min_length=1)


class ProviderResponse(MediaModel):
    request_id: str
    text: str = ""
    content: bytes = Field(default=b"", repr=False)
    content_type: str | None = None
    duration_seconds: float | None = None
    confidence: float | None = None
    usage: dict[str, float] = Field(default_factory=dict)
    pages: tuple[OCRPage, ...] = ()
    budget_reservation_id: str | None = None
