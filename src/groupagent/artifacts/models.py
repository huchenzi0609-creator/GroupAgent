"""Small, serializable contracts for the filesystem artifact worker."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4


class ArtifactError(ValueError):
    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code


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
    rendered: bool = False
    layout_passed: bool = False
    visual_reviewed: bool = False
    page_count: int | None = None
    checks: tuple[str, ...] = ()
    issues: tuple[str, ...] = ()
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
        return {**asdict(self), "passed": self.passed, "status": self.status}

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> ValidationRecord:
        fields = {k: v for k, v in value.items() if k not in {"passed", "status"}}
        for key in ("checks", "issues", "preview_sha256", "limitations"):
            fields[key] = tuple(fields.get(key, ()))
        return cls(**fields)


@dataclass(frozen=True)
class TableData:
    headers: tuple[str, ...]
    rows: tuple[tuple[str, ...], ...]


@dataclass(frozen=True)
class Section:
    title: str
    paragraphs: tuple[str, ...]
    tables: tuple[TableData, ...] = ()


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


@dataclass(frozen=True)
class Deck:
    title: str
    slides: tuple[Slide, ...]


@dataclass(frozen=True)
class DocumentUnit:
    locator: str
    text: str


@dataclass(frozen=True)
class DocumentRead:
    file_id: str
    version_id: str
    format: str
    units: tuple[DocumentUnit, ...]
    warnings: tuple[str, ...] = ()

    @property
    def text(self) -> str:
        return "\n\n".join(unit.text for unit in self.units)
