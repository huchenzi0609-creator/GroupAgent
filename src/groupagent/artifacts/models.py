"""Small, serializable contracts for the filesystem artifact worker."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from typing import Any, Literal
from uuid import uuid4

from groupagent.domain import DomainError


class ArtifactError(DomainError, ValueError):
    def __init__(self, code: str, message: str):
        super().__init__(code, message)


@dataclass(frozen=True)
class ArtifactScope:
    """A trusted caller supplies these values; documents cannot grant access."""

    group_id: str
    work_id: str
    actor_id: str

    def __post_init__(self) -> None:
        if any(
            not isinstance(v, str) or not v.strip() or len(v) > 256 for v in asdict(self).values()
        ):
            raise ArtifactError("invalid_scope", "Group, work and actor identities are required")


@dataclass(frozen=True)
class ArtifactVersion:
    file_id: str
    version_id: str
    group_id: str
    work_id: str
    name: str
    format: str
    sha256: str
    size_bytes: int
    parent_version_id: str | None
    created_by: str
    created_at: str
    provenance: dict[str, Any] = field(default_factory=dict)

    @property
    def artifact_version_id(self) -> str:
        return self.version_id

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ValidationRecord:
    file_id: str
    version_id: str
    sha256: str
    format: str
    structural_passed: bool
    content_passed: bool
    media_decoded: bool = False
    rendered: bool = False
    layout_passed: bool = False
    visual_reviewed: bool = False
    page_count: int | None = None
    checks: tuple[str, ...] = ()
    issues: tuple[str, ...] = ()
    failed_expectations: tuple[dict[str, Any], ...] = ()
    renderer: str | None = None
    preview_sha256: tuple[str, ...] = ()
    limitations: tuple[str, ...] = ()
    reviewed_by: str | None = None
    review_note: str | None = None
    parent_validation_id: str | None = None
    validation_id: str = field(default_factory=lambda: uuid4().hex)
    created_at: str = field(default_factory=lambda: datetime.now(UTC).isoformat())

    @property
    def passed(self) -> bool:
        if not self.structural_passed or not self.content_passed or self.issues:
            return False
        if self.format in {
            "png",
            "jpg",
            "jpeg",
            "webp",
            "gif",
            "wav",
            "ogg",
            "mp3",
            "flac",
            "m4a",
            "aac",
        }:
            return self.media_decoded
        return self.format == "md" or (self.rendered and self.layout_passed)

    @property
    def status(self) -> str:
        if self.passed:
            return "validated"
        if self.issues or not self.structural_passed:
            return "failed"
        if self.rendered:
            return "layout_unverified" if self.content_passed else "content_unverified"
        return "structural_only"

    def to_dict(self) -> dict[str, Any]:
        return {
            **asdict(self),
            "passed": self.passed,
            "status": self.status,
            "failure_summary": self.failure_summary,
        }

    @property
    def failure_summary(self) -> str:
        if self.passed:
            return ""
        parts = ["Document validation failed: " + ", ".join(self.issues[:12])]
        for item in self.failed_expectations[:8]:
            parts.append(
                f"expected_text[{item['index']}] missing in {item['stage']}: "
                + json.dumps(item["expected"], ensure_ascii=False)
            )
        if self.failed_expectations:
            parts.append(
                "expected_text is a literal check (whitespace ignored), not a semantic claim or a table-row equality. Required user literals must be present; do not invent phrasing for semantic criteria."
            )
        if "layout_policy_unsupported" in self.issues:
            parts.append(
                "This layout is outside the verified profile; use document_generate for the supported report/deck layout. Do not spoof document metadata to bypass layout checks."
            )
        return "\n".join(parts)[:2400]

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> ValidationRecord:
        fields = {
            k: v for k, v in value.items() if k not in {"passed", "status", "failure_summary"}
        }
        for key in ("checks", "issues", "preview_sha256", "limitations", "failed_expectations"):
            fields[key] = tuple(fields.get(key, ()))
        return cls(**fields)


@dataclass(frozen=True)
class TableData:
    headers: tuple[str, ...]
    rows: tuple[tuple[str, ...], ...]


@dataclass(frozen=True)
class ImageData:
    content: bytes
    alt_text: str
    width_inches: float = 5.5


@dataclass(frozen=True)
class ChartSeries:
    name: str
    values: tuple[float, ...]


@dataclass(frozen=True)
class ChartData:
    kind: Literal["bar", "line", "pie"]
    title: str
    categories: tuple[str, ...]
    series: tuple[ChartSeries, ...]


@dataclass(frozen=True)
class Section:
    title: str
    paragraphs: tuple[str, ...]
    tables: tuple[TableData, ...] = ()
    images: tuple[ImageData, ...] = ()
    charts: tuple[ChartData, ...] = ()


@dataclass(frozen=True)
class Report:
    title: str
    sections: tuple[Section, ...]
    subtitle: str = ""


@dataclass(frozen=True)
class Slide:
    title: str
    bullets: tuple[str, ...]
    table: TableData | None = None
    image: ImageData | None = None
    chart: ChartData | None = None


@dataclass(frozen=True)
class Deck:
    title: str
    slides: tuple[Slide, ...]


@dataclass(frozen=True)
class DocumentUnit:
    locator: str
    text: str
    editable: bool = False


@dataclass(frozen=True)
class DocumentRead:
    file_id: str
    version_id: str
    format: str
    units: tuple[DocumentUnit, ...]
    warnings: tuple[str, ...] = ()
    total_units: int | None = None
    complete: bool = True
    ocr_pages: tuple[int, ...] = ()

    @property
    def text(self) -> str:
        return "\n\n".join(unit.text for unit in self.units)
