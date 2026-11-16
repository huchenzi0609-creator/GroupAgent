"""Trusted tool contracts; model arguments never carry authority."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import Path
from typing import Annotated, Any, ClassVar, Generic, Literal, TypeVar

from pydantic import BaseModel, ConfigDict, Field, JsonValue, field_validator

from groupagent.domain import ActorContext, DomainError, Role


class ToolData(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    # Reviewed static hints only. Never use validation exception text or raw inputs.
    validation_hints: ClassVar[dict[str, str]] = {}


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


class ToolFailureOrigin(StrEnum):
    AUTHORIZATION = "authorization"
    ARGUMENTS = "arguments"
    CAPABILITY = "capability"
    EXECUTION = "execution"
    ADMISSION = "admission"
    PRECONDITION = "precondition"


class ToolFailureDisposition(StrEnum):
    REPAIR = "repair"
    FENCE = "fence"
    RECONCILE = "reconcile"
    DEFER = "defer"
    REQUEST_INPUT = "request_input"


class ToolFailureMetadata(ToolData):
    """Trusted gateway/handler classification, never parsed from model output.

    The business effect state remains an independent, stronger constraint:
    UNKNOWN can only reconcile, regardless of a suggested repair disposition.
    """

    origin: ToolFailureOrigin
    disposition: ToolFailureDisposition


class InputRequest(ToolData):
    question: str = Field(min_length=1, max_length=2000)
    required_inputs: tuple[Annotated[str, Field(min_length=1, max_length=200)], ...] = Field(
        default=(), max_length=8
    )

    @field_validator("question")
    @classmethod
    def meaningful_question(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("A concrete question is required")
        return value.strip()

    @field_validator("required_inputs")
    @classmethod
    def meaningful_inputs(cls, values: tuple[str, ...]) -> tuple[str, ...]:
        if any(not value.strip() for value in values):
            raise ValueError("Required input names must be concrete")
        return tuple(dict.fromkeys(value.strip() for value in values))


class ToolControl(ToolData):
    kind: Literal["request_input"] = "request_input"
    input_request: InputRequest


class ToolInputIssue(ToolData):
    field: tuple[str | int, ...]
    kind: str
    hint: str | None = None


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
    # Set by a trusted ToolDefinition capability, never parsed from tool output.
    control: ToolControl | None = None
    input_issues: tuple[ToolInputIssue, ...] = ()
    # Added only by the host Gateway from reviewed registration + fresh policy.
    # Model arguments and output dictionaries cannot populate receipt metadata.
    required_tool_evidence: frozenset[Literal["process_run", "shell_command"]] = frozenset()
    # Only a registered host toolset loader can populate this presentation metadata.
    loaded_tools: frozenset[str] = frozenset()
    # Optional for legacy receipts. New dispatch supplies trusted failure origin
    # so an unsupported argument cannot masquerade as revoked authority.
    failure: ToolFailureMetadata | None = None


LEGACY_FENCED_TOOL_ERRORS = frozenset(
    {
        "permission_denied",
        "scope_violation",
        "tool_not_registered",
        "policy_unavailable",
        "idempotency_conflict",
        "stale_lease",
        "policy_changed",
        "source_unavailable",
        "auth_required",
        "cancelled",
    }
)
AUTHORITY_FENCED_TOOL_ERRORS = frozenset(
    {
        "stale_lease",
        "policy_changed",
        "source_unavailable",
        "auth_required",
        "policy_unavailable",
        "idempotency_conflict",
        "cancelled",
    }
)


def failure_disposition(receipt: ToolReceipt) -> ToolFailureDisposition | None:
    """Effect uncertainty wins; old unclassified authority failures stay conservative."""
    if receipt.effect_state == EffectState.UNKNOWN:
        return ToolFailureDisposition.RECONCILE
    if not receipt.success and receipt.error_code in AUTHORITY_FENCED_TOOL_ERRORS:
        return ToolFailureDisposition.FENCE
    if receipt.control is not None:
        return ToolFailureDisposition.REQUEST_INPUT
    if receipt.success:
        return None
    if receipt.failure is not None:
        return receipt.failure.disposition
    if receipt.error_code in LEGACY_FENCED_TOOL_ERRORS:
        return ToolFailureDisposition.FENCE
    if (
        receipt.error_code == "resource_deferred"
        and receipt.effect_state == EffectState.NOT_STARTED
    ):
        return ToolFailureDisposition.DEFER
    return ToolFailureDisposition.REPAIR


ISOLATED_PROCESS_TOOLS = frozenset({"process_run", "shell_command"})


def successful_required_tools(
    receipts: Iterable[ToolReceipt], allowed_tools: frozenset[str]
) -> frozenset[str]:
    successful = set()
    for receipt in receipts:
        if (
            not receipt.success
            or receipt.effect_state not in {EffectState.SUCCEEDED, EffectState.NO_EFFECT}
            or receipt.tool_name not in allowed_tools
        ):
            continue
        successful.add(receipt.tool_name)
        if (
            receipt.tool_name in ISOLATED_PROCESS_TOOLS
            and receipt.effect_state == EffectState.SUCCEEDED
        ):
            successful.update(
                receipt.required_tool_evidence & allowed_tools & ISOLATED_PROCESS_TOOLS
            )
    return frozenset(successful)


class ToolLedgerEntry(ToolData):
    fingerprint: str = Field(pattern=r"^[a-f0-9]{64}$")
    receipt: ToolReceipt


class ToolFailure(DomainError):
    """Reviewed handlers provide diagnostics and explicit failure provenance.

    ``not_started`` is a host attestation that no effectful operation began;
    ``no_effect`` may also attest a confirmed unpublished draft/rollback. Model
    arguments and output dictionaries cannot supply either proof or disposition.
    """

    def __init__(
        self,
        code: str,
        message: str,
        *,
        output: BaseModel | None = None,
        no_effect: bool = False,
        not_started: bool = False,
        origin: ToolFailureOrigin | None = None,
        disposition: ToolFailureDisposition | None = None,
    ):
        super().__init__(code, message)
        self.output = output
        self.no_effect = no_effect or not_started
        self.not_started = not_started
        if (origin is None) != (disposition is None):
            raise ValueError("Failure origin and disposition must be supplied together")
        self.failure = (
            ToolFailureMetadata(origin=origin, disposition=disposition)
            if origin is not None and disposition is not None
            else None
        )


class ToolInputRequired(DomainError):
    """Trusted precondition failure after no effect or a confirmed DB rollback."""

    def __init__(self, question: str, *, required_inputs: tuple[str, ...] = ()):
        super().__init__("input_required", "Confirmed input is required before this action")
        self.input_request = InputRequest(question=question, required_inputs=required_inputs)


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
    concurrency_safe: bool = False
    timeout_seconds: float = 60.0
    max_input_bytes: int = 256_000
    max_output_bytes: int = 256_000
    max_artifact_bytes: int = 10 * 1024 * 1024
    cost_meter: str = "no_provider_charge"
    idempotency_mode: IdempotencyMode = IdempotencyMode.NON_RETRYABLE
    input_path_fields: tuple[str, ...] = ()
    output_path_fields: tuple[str, ...] = ()
    control_kind: Literal["request_input"] | None = None
    may_request_input: bool = False
    required_tool_family: Literal["isolated_process"] | None = None
    toolset_loader: bool = False

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
