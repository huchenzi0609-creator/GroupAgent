"""Engine candidates are not completed business work or accepted deliveries."""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, JsonValue, model_validator

from groupagent.domain import ActorContext
from groupagent.tools.models import InputRequest, ToolLedgerEntry, ToolReceipt


class EngineData(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class ExecutionBudget(EngineData):
    # None disables cumulative work limits. Per-call resource bounds and
    # conservative monetary admission remain in force for every request.
    max_rounds: int | None = Field(default=None, ge=1)
    max_tokens: int | None = Field(default=None, ge=1)
    max_tool_calls: int | None = Field(default=None, ge=1)
    max_active_seconds: float | None = Field(default=None, gt=0, allow_inf_nan=False)
    max_output_tokens: int = Field(default=4096, ge=1, le=65_536)
    model_timeout_seconds: float = Field(default=60, gt=0, allow_inf_nan=False)
    max_input_bytes: int = Field(default=1_000_000, ge=1)
    additional_retry_limit: int = Field(default=2, ge=0, le=5)
    max_retry_after_seconds: float = Field(default=30, ge=0, le=60)


class ExecutionRequest(EngineData):
    work_id: str = Field(min_length=1)
    attempt_id: str = Field(min_length=1)
    actor: ActorContext
    objective: str = Field(min_length=1, max_length=200_000)
    workspace: Path
    allowed_tools: frozenset[str] = frozenset()
    visible_tools: frozenset[str] | None = None
    required_tools: frozenset[str] = frozenset()
    revision: int = Field(default=1, ge=1)
    budget: ExecutionBudget = Field(default_factory=ExecutionBudget)
    trace_id: str = Field(min_length=1)

    @model_validator(mode="after")
    def check_required_tools(self) -> ExecutionRequest:
        if not self.required_tools.issubset(self.allowed_tools):
            raise ValueError("Required tools must be in the allowlist")
        if self.visible_tools is not None and not self.visible_tools.issubset(self.allowed_tools):
            raise ValueError("Visible tools must be in the allowlist")
        return self


class ModelToolCall(EngineData):
    id: str = Field(min_length=1, max_length=256)
    name: str = Field(min_length=1, max_length=64)
    arguments: dict[str, JsonValue]


class ModelMessage(EngineData):
    role: Literal["system", "user", "assistant", "tool"]
    content: str | None = None
    tool_calls: tuple[ModelToolCall, ...] = ()
    tool_call_id: str | None = None


class TokenUsage(EngineData):
    input_tokens: int = Field(ge=0)
    output_tokens: int = Field(ge=0)

    @property
    def total(self) -> int:
        return self.input_tokens + self.output_tokens


class ModelRequest(EngineData):
    messages: tuple[ModelMessage, ...]
    tools: tuple[dict[str, Any], ...]
    max_output_tokens: int = Field(ge=1)


class ModelReply(EngineData):
    text: str | None = None
    tool_calls: tuple[ModelToolCall, ...] = ()
    finish_reason: Literal["stop", "tool_calls", "length", "content_filter"] = "stop"
    usage: TokenUsage
    needs_input: bool = False


class RunState(StrEnum):
    RUNNING = "running"
    CANDIDATE = "candidate"
    WAITING_INPUT = "waiting_input"
    PAUSED = "paused"
    FAILED = "failed"
    CANCELLED = "cancelled"


class UsageTotals(EngineData):
    rounds: int = Field(default=0, ge=0)
    tokens: int = Field(default=0, ge=0)
    tool_calls: int = Field(default=0, ge=0)
    active_seconds: float = Field(default=0, ge=0)


class PendingInput(EngineData):
    revision: int = Field(ge=1)
    input_id: str
    text: str


class EngineCheckpoint(EngineData):
    version: Literal[1] = 1
    request: ExecutionRequest
    revision: int = Field(ge=1)
    state: RunState
    sequence: int = Field(ge=0)
    messages: tuple[ModelMessage, ...]
    pending_inputs: tuple[PendingInput, ...] = ()
    usage: UsageTotals
    receipts: tuple[ToolReceipt, ...] = ()
    ledger: tuple[ToolLedgerEntry, ...] = ()
    unresolved_tools: frozenset[str] = frozenset()
    safe_to_resume: bool
    backend_state: dict[str, JsonValue] = Field(default_factory=dict)
    candidate_text: str | None = None
    error_code: str | None = None
    input_request: InputRequest | None = None


class RunSnapshot(EngineData):
    run_id: str
    work_id: str
    attempt_id: str
    group_id: str
    revision: int
    state: RunState
    usage: UsageTotals
    candidate_text: str | None = None
    receipts: tuple[ToolReceipt, ...] = ()
    error_code: str | None = None
    last_sequence: int
    checkpoint: EngineCheckpoint


class StartedPayload(EngineData):
    type: Literal["started"] = "started"
    model: str
    revision: int
    resumed: bool = False


class InputPayload(EngineData):
    type: Literal["input"] = "input"
    revision: int
    input_id: str


class ProgressPayload(EngineData):
    type: Literal["progress"] = "progress"
    reason: str
    usage: UsageTotals


class RequestedPayload(EngineData):
    type: Literal["tool_requested"] = "tool_requested"
    call_id: str
    name: str
    revision: int
    idempotency_key: str


class FinishedToolPayload(EngineData):
    type: Literal["tool_finished"] = "tool_finished"
    receipt: ToolReceipt


class ArtifactPayload(EngineData):
    type: Literal["artifact"] = "artifact"
    artifact_id: str
    version_id: str | None = None


class CheckpointPayload(EngineData):
    type: Literal["checkpoint"] = "checkpoint"
    revision: int
    durable: bool
    safe_to_resume: bool


class TerminalPayload(EngineData):
    type: Literal["terminal"] = "terminal"
    state: RunState
    revision: int
    error_code: str | None = None
    candidate_text: str | None = None
    usage: UsageTotals


EventPayload = (
    StartedPayload
    | InputPayload
    | ProgressPayload
    | RequestedPayload
    | FinishedToolPayload
    | ArtifactPayload
    | CheckpointPayload
    | TerminalPayload
)


EngineEventKind = Literal[
    "run_started",
    "input_consumed",
    "tool_requested",
    "tool_finished",
    "artifact_candidate",
    "progress",
    "needs_input",
    "checkpoint_saved",
    "run_finished",
    "run_failed",
    "run_cancelled",
    "run_paused",
]


class EngineEvent(EngineData):
    work_id: str
    attempt_id: str
    trace_id: str
    sequence: int = Field(ge=1)
    time: datetime
    kind: EngineEventKind
    payload: EventPayload = Field(discriminator="type")
