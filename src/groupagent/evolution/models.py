"""Bounded proposals and version-specific independent evaluation records."""

from __future__ import annotations

import hashlib
import json
from typing import Annotated, Literal

from pydantic import Field, JsonValue, StringConstraints, field_validator, model_validator

from groupagent.security import redact_credentials
from groupagent.tools import ToolData

Slug = Annotated[str, StringConstraints(pattern=r"^[a-z][a-z0-9_-]{0,63}$")]
Digest = Annotated[str, StringConstraints(pattern=r"^[a-f0-9]{64}$")]
VersionId = Annotated[str, StringConstraints(pattern=r"^[a-f0-9]{32}$")]
Kind = Literal["strategy", "code"]
Step = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=500)]


def digest(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def canonical(value: JsonValue) -> bytes:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
    ).encode("utf-8")


def reviewed_text(value: str) -> str:
    try:
        value.encode("utf-8")
    except UnicodeError as exc:
        raise ValueError("Proposal text must be valid UTF-8") from exc
    if (
        any(ord(char) < 32 and char not in "\n\r\t" for char in value)
        or redact_credentials(value) != value
    ):
        raise ValueError("Proposal text contains blocked credentials or control characters")
    return value


class StrategyProposal(ToolData):
    name: Slug
    purpose: Step
    procedure: tuple[Step, ...] = Field(min_length=1, max_length=12)
    verification: tuple[Step, ...] = Field(min_length=1, max_length=8)

    @field_validator("purpose")
    @classmethod
    def safe_purpose(cls, value: str) -> str:
        return reviewed_text(value)

    @field_validator("procedure", "verification")
    @classmethod
    def safe_steps(cls, values: tuple[str, ...]) -> tuple[str, ...]:
        return tuple(reviewed_text(value) for value in values)


class FileEdit(ToolData):
    path: str = Field(min_length=1, max_length=240)
    before_sha256: Digest | None
    text: str | None = Field(max_length=24_000)

    @field_validator("text")
    @classmethod
    def safe_file(cls, value: str | None) -> str | None:
        return reviewed_text(value) if value is not None else None


class CodeProposal(ToolData):
    name: Slug
    problem: Step
    baseline_sha256: Digest
    edits: tuple[FileEdit, ...] = Field(min_length=1, max_length=8)

    @field_validator("problem")
    @classmethod
    def safe_problem(cls, value: str) -> str:
        return reviewed_text(value)

    @model_validator(mode="after")
    def distinct_files(self) -> CodeProposal:
        if len({item.path for item in self.edits}) != len(self.edits):
            raise ValueError("Each file can be edited once per proposal")
        if sum(len(item.text.encode("utf-8")) for item in self.edits if item.text) > 64_000:
            raise ValueError("Code proposal exceeds its byte limit")
        return self


class Version(ToolData):
    id: VersionId
    kind: Kind
    name: Slug
    version: int = Field(ge=1)
    body_sha256: Digest
    state: Literal["proposed", "evaluated", "rejected", "active"]


class EvaluationReport(ToolData):
    version_id: VersionId
    body_sha256: Digest
    suite_sha256: Digest
    checks: dict[Slug, bool] = Field(min_length=1, max_length=32)

    @property
    def passed(self) -> bool:
        return len(self.checks) >= 2 and all(self.checks.values())


class CodeEvaluationReport(EvaluationReport):
    baseline_sha256: Digest
    candidate_sha256: Digest
    image: Digest


class CodeBundle(ToolData):
    version_id: VersionId
    body_sha256: Digest
    proposal: CodeProposal

    @model_validator(mode="after")
    def intact(self) -> CodeBundle:
        if digest(canonical(self.proposal.model_dump(mode="json"))) != self.body_sha256:
            raise ValueError("Proposal body does not match its recorded digest")
        return self
