"""Trusted tool contracts; model arguments never carry authority."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import Path
from typing import Any, Generic, TypeVar

from pydantic import BaseModel, ConfigDict, Field, JsonValue

from groupagent.domain import ActorContext, Role


class ToolData(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class EffectState(StrEnum):
    NOT_STARTED = "not_started"
    NO_EFFECT = "no_effect"
    SUCCEEDED = "succeeded"
    UNKNOWN = "unknown"


class IdempotencyMode(StrEnum):
    READ_ONLY = "read_only"
    NATIVE_KEY = "native_key"
    MANAGED_TRANSACTION = "managed_transaction"
    RECONCILE_BEFORE_RETRY = "reconcile_before_retry"
    NON_RETRYABLE = "non_retryable"


class ToolReceipt(ToolData):
    call_id: str
    tool_name: str
    idempotency_key: str
    success: bool
    effect_state: EffectState
    output: dict[str, JsonValue] | None = None
    error_code: str | None = None
    message: str | None = None
    replayed: bool = False


class ToolLedgerEntry(ToolData):
    fingerprint: str = Field(pattern=r"^[a-f0-9]{64}$")
    receipt: ToolReceipt


@dataclass(frozen=True)
class ToolScope:
    """A grant constructed by the control plane, never deserialized from a model reply."""

    group_id: str
    work_id: str
    actor: ActorContext
    workspace: Path
    allowed_tools: frozenset[str]
    trace_id: str


@dataclass(frozen=True)
class ToolContext:
    scope: ToolScope
    call_id: str
    idempotency_key: str
    cancelled: asyncio.Event = field(repr=False)
    deadline_monotonic: float | None = None

    @property
    def actor(self) -> ActorContext:
        return self.scope.actor

    @property
    def group_id(self) -> str:
        return self.scope.group_id

    @property
    def work_id(self) -> str:
        return self.scope.work_id

    @property
    def workspace(self) -> Path:
        return self.scope.workspace


InputT = TypeVar("InputT", bound=BaseModel)
OutputT = TypeVar("OutputT", bound=BaseModel)


@dataclass(frozen=True)
class ToolDefinition(Generic[InputT, OutputT]):
    name: str
    description: str
    input_model: type[InputT]
    output_model: type[OutputT]
    handler: Callable[[InputT, ToolContext], Awaitable[OutputT]] = field(repr=False)
    version: str = "1"
    required_roles: frozenset[Role] = frozenset({Role.OWNER, Role.MEMBER})
    resource_scopes: tuple[str, ...] = ("group", "work", "workspace")
    risk_class: str = "local_draft"
    timeout_seconds: float = 60.0
    max_input_bytes: int = 256_000
    max_output_bytes: int = 256_000
    max_artifact_bytes: int = 10 * 1024 * 1024
    cost_meter: str = "no_provider_charge"
    idempotency_mode: IdempotencyMode = IdempotencyMode.NON_RETRYABLE
    input_path_fields: tuple[str, ...] = ()
    output_path_fields: tuple[str, ...] = ()

    def describe(self) -> dict[str, Any]:
        return {
            "type": "function",
            "function": {
                "name": self.name,
                "description": self.description,
                "parameters": self.input_model.model_json_schema(),
            },
        }


class PolicyDecision(ToolData):
    """Fresh authority returned by a trusted policy provider before every action."""

    actor: ActorContext
    allowed_tools: frozenset[str]
    active: bool = True


PolicyProvider = Callable[[ToolScope], Awaitable[PolicyDecision]]


class WorkspaceReadInput(ToolData):
    path: str = Field(min_length=1, max_length=1024)


class WorkspaceReadOutput(ToolData):
    path: str
    text: str


class WorkspaceWriteInput(ToolData):
    path: str = Field(min_length=1, max_length=1024)
    text: str = Field(max_length=128_000)


class WorkspaceWriteOutput(ToolData):
    path: str
    bytes_written: int
    sha256: str
