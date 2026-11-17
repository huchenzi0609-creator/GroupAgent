"""Typed business contracts shared by control, execution and persistence.

ActorContext is a trusted boundary value: adapters must obtain it from identity
and policy configuration, never from a model's claimed role.
"""

from __future__ import annotations

from datetime import UTC, datetime
from enum import StrEnum
from typing import Annotated, Any, Literal

from pydantic import (
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    JsonValue,
    field_validator,
    model_serializer,
)

Identifier = Annotated[str, Field(min_length=1)]


class DomainError(Exception):
    def __init__(
        self,
        code: str,
        message: str,
        *,
        retryable: bool = False,
        effect_state: str | None = None,
        trace_id: str | None = None,
        resource_ref: str | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.retryable = retryable
        self.effect_state = effect_state
        self.trace_id = trace_id
        self.resource_ref = resource_ref


class DomainModel(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    @field_validator("*")
    @classmethod
    def normalize_utc(cls, value: Any) -> Any:
        if isinstance(value, datetime) and value.tzinfo is not None:
            return value.astimezone(UTC)
        return value


class Role(StrEnum):
    OWNER = "owner"
    MAINTAINER = "maintainer"
    MEMBER = "member"
    OBSERVER = "observer"


class EventKind(StrEnum):
    TEXT = "text"
    IMAGE = "image"
    AUDIO = "audio"
    FILE = "file"
    QUOTE = "quote"
    FORWARDED = "forwarded"
    RECALL = "recall"
    MEMBERSHIP = "membership"
    SYSTEM = "system"
    UNSUPPORTED_VIDEO = "unsupported_video"


class ActorContext(DomainModel):
    group_id: Identifier
    member_id: Identifier
    role: Role
    policy_revision: int = Field(default=1, ge=1)


class GroupEvent(DomainModel):
    schema_version: Literal["groupagent.event.v0.1"] = "groupagent.event.v0.1"
    id: Identifier
    source_key: Identifier
    group_id: Identifier
    actor_id: Identifier
    kind: EventKind
    text: str = ""
    occurred_at: AwareDatetime
    received_at: AwareDatetime
    reply_to: str | None = None
    # Authenticated channel metadata only; names/text never populate this list.
    mentioned_member_ids: list[str] = Field(default_factory=list)
    attachments: list[dict[str, JsonValue]] = Field(default_factory=list)
    channel_account_id: str | None = None
    source_epoch: str | None = None
    payload_ref: str | None = None
    platform_message_id: str | None = None
    trusted_actor_origin: str = "verified_channel_metadata"
    policy_revision: int = Field(default=1, ge=1)
    content_state: Literal["active", "forgotten", "expired", "recalled"] = "active"

    @model_serializer(mode="wrap")
    def _compatible_recipient_metadata(self, handler):
        # Source witnesses hash this canonical projection. Adding absent
        # recipient metadata must not invalidate historical work fingerprints.
        value = handler(self)
        if not self.mentioned_member_ids:
            value.pop("mentioned_member_ids", None)
        return value


class EventCursor(DomainModel):
    group_id: Identifier
    stream_id: Identifier
    cursor: str | None
    revision: int = Field(ge=0)


class IngestResult(DomainModel):
    event: GroupEvent
    inserted: bool
    cursor_revision: int = Field(ge=0)


class WorkState(StrEnum):
    QUEUED = "queued"
    RUNNING = "running"
    WAITING_INPUT = "waiting_input"
    WAITING_EXTERNAL = "waiting_external"
    VALIDATING = "validating"
    DELIVERING = "delivering"
    PAUSED = "paused"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


WORK_TRANSITIONS: dict[WorkState, frozenset[WorkState]] = {
    WorkState.QUEUED: frozenset(
        {
            WorkState.RUNNING,
            WorkState.WAITING_INPUT,
            WorkState.PAUSED,
            WorkState.FAILED,
            WorkState.CANCELLED,
        }
    ),
    WorkState.RUNNING: frozenset(
        {
            WorkState.QUEUED,
            WorkState.WAITING_INPUT,
            WorkState.WAITING_EXTERNAL,
            WorkState.VALIDATING,
            WorkState.PAUSED,
            WorkState.FAILED,
            WorkState.CANCELLED,
        }
    ),
    WorkState.WAITING_INPUT: frozenset(
        {WorkState.QUEUED, WorkState.PAUSED, WorkState.FAILED, WorkState.CANCELLED}
    ),
    WorkState.WAITING_EXTERNAL: frozenset(
        {WorkState.QUEUED, WorkState.PAUSED, WorkState.FAILED, WorkState.CANCELLED}
    ),
    WorkState.VALIDATING: frozenset(
        {
            WorkState.QUEUED,
            WorkState.DELIVERING,
            WorkState.PAUSED,
            WorkState.FAILED,
            WorkState.CANCELLED,
        }
    ),
    WorkState.DELIVERING: frozenset(
        {WorkState.COMPLETED, WorkState.PAUSED, WorkState.FAILED, WorkState.CANCELLED}
    ),
    WorkState.PAUSED: frozenset({WorkState.QUEUED, WorkState.FAILED, WorkState.CANCELLED}),
    WorkState.FAILED: frozenset({WorkState.QUEUED}),
    WorkState.COMPLETED: frozenset(),
    WorkState.CANCELLED: frozenset(),
}


def check_transition(current: WorkState, target: WorkState) -> None:
    if target not in WORK_TRANSITIONS[current]:
        raise DomainError("invalid_transition", f"Cannot move work from {current} to {target}")


class WorkOrder(DomainModel):
    id: Identifier
    group_id: Identifier
    requester_id: Identifier
    request_event_id: Identifier
    goal: Identifier
    acceptance_criteria: list[str] = Field(default_factory=list)
    input_versions: list[str] = Field(default_factory=list)
    scope: dict[str, JsonValue] = Field(default_factory=dict)
    budget: dict[str, JsonValue] = Field(default_factory=dict)
    required_checks: list[str] = Field(default_factory=lambda: ["result"])
    state: WorkState = WorkState.QUEUED
    revision: int = Field(default=1, ge=1)
    goal_revision: int = Field(default=1, ge=1)
    policy_revision: int = Field(default=1, ge=1)
    pause_reason: str | None = None
    created_at: AwareDatetime
    updated_at: AwareDatetime


class ExecutionLease(DomainModel):
    group_id: Identifier
    work_id: Identifier
    attempt_id: Identifier
    attempt_no: int = Field(ge=1)
    worker_id: Identifier
    fencing_token: int = Field(ge=1)
    goal_revision: int = Field(ge=1)
    expires_at: AwareDatetime


class DeliveryState(StrEnum):
    PENDING = "pending"
    DISPATCHING = "dispatching"
    ACCEPTED = "accepted"
    UNKNOWN = "unknown"
    REJECTED = "rejected"


class DeliveryIntent(DomainModel):
    id: Identifier
    group_id: Identifier
    work_id: str | None = None
    request_event_id: str | None = None
    goal_revision: int | None = None
    created_by: Identifier
    policy_revision: int = Field(default=1, ge=1)
    payload_ref: Identifier
    payload_hash: Identifier
    idempotency_key: Identifier
    state: DeliveryState = DeliveryState.PENDING
    revision: int = Field(default=1, ge=1)
    claim_token: int = Field(default=0, ge=0)
    worker_id: str | None = None
    claim_expires_at: AwareDatetime | None = None
    platform_message_id: str | None = None
    platform_job_id: str | None = None
    accepted_at: AwareDatetime | None = None
    evidence_ref: str | None = None
    reason: str | None = None
    created_at: AwareDatetime
    updated_at: AwareDatetime


class TeamTaskState(StrEnum):
    OPEN = "open"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    CANCELLED = "cancelled"


class TeamTask(DomainModel):
    id: Identifier
    group_id: Identifier
    title: Identifier
    owner_id: Identifier | None
    assignment_kind: str
    due_at: AwareDatetime | None = None
    status: TeamTaskState = TeamTaskState.OPEN
    progress: str = ""
    evidence_refs: list[str] = Field(default_factory=list)
    source_event_id: str | None = None
    source_event_ids: list[str] = Field(default_factory=list)
    field_sources: dict[str, list[str]] = Field(default_factory=dict)
    created_by: str | None = None
    content_state: Literal["active", "forgotten"] = "active"
    provenance_kind: Literal["source_bound", "independent", "legacy_unattributed"] = "independent"
    revision: int = Field(default=1, ge=1)
    created_at: AwareDatetime
    updated_at: AwareDatetime
