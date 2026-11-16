"""Serializable worker requests. Authority and credentials are never model fields."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

from groupagent.domain import DomainError


class UnpublishedDraftError(DomainError):
    """The trusted controller proved no workspace outputs were published."""


class DraftPublicationUnknown(DomainError):
    """Publication started and its rollback could not be verified."""


class SandboxData(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")


class SandboxLimits(SandboxData):
    memory_mb: int = Field(default=512, ge=128, le=4096)
    cpus: float = Field(default=1, gt=0, le=4)
    cpu_shares: int = Field(default=1024, ge=2, le=262144)
    oom_score_adj: int = Field(default=0, ge=0, le=1000)
    pids: int = Field(default=64, ge=16, le=512)
    workspace_mb: int = Field(default=128, ge=16, le=1024)
    temp_mb: int = Field(default=64, ge=16, le=512)
    max_file_bytes: int = Field(default=64 * 1024 * 1024, ge=1, le=256 * 1024 * 1024)
    max_input_bytes: int = Field(default=128 * 1024 * 1024, ge=1, le=512 * 1024 * 1024)
    max_artifact_bytes: int = Field(default=64 * 1024 * 1024, ge=1, le=256 * 1024 * 1024)
    max_files: int = Field(default=256, ge=1, le=4096)


class ProcessSpec(SandboxData):
    program: str = Field(min_length=1, max_length=64)
    args: tuple[str, ...] = Field(default=(), max_length=256)
    input_paths: tuple[str, ...] = Field(default=(), max_length=256)
    output_paths: tuple[str, ...] = Field(default=(), max_length=256)
    stdin: str = Field(default="", max_length=256_000)
    timeout_seconds: float = Field(default=60, gt=0, le=600)
    max_output_bytes: int = Field(default=256_000, ge=1, le=2_000_000)


class ProcessOutput(SandboxData):
    path: str
    size_bytes: int = Field(ge=0)
    sha256: str


class OutputContractIssue(SandboxData):
    """An index into the declared outputs, never exception text or worker paths."""

    output_index: int = Field(ge=0, le=4095)
    kind: Literal["missing", "invalid_file", "unreadable", "total_size_exceeded"]


class ProcessResult(SandboxData):
    backend: Literal["docker"] = "docker"
    image: str
    exit_code: int | None
    stdout: str = ""
    stderr: str = ""
    timed_out: bool = False
    cancelled: bool = False
    output_limited: bool = False
    error_code: Literal["output_contract_failed", "worker_failed"] | None = None
    output_issues: tuple[OutputContractIssue, ...] = ()
    # Set only after the controller validates and publishes the whole manifest.
    # A worker's stdout cannot establish this host-side fact.
    publication_state: Literal["unpublished", "published"] = "unpublished"
    outputs: tuple[ProcessOutput, ...] = ()
    elapsed_seconds: float = Field(ge=0)
    resource_usage: dict[str, int] = Field(default_factory=dict)

    @property
    def succeeded(self) -> bool:
        return self.exit_code == 0 and not (
            self.timed_out or self.cancelled or self.output_limited or self.error_code
        )
