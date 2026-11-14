from __future__ import annotations

from typing import Annotated, Literal

from pydantic import (
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    JsonValue,
    TypeAdapter,
    field_validator,
)

from .quiet import validate_windows


class ActionData(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, hide_input_in_errors=True)


class ClaimTask(ActionData):
    op: Literal["claim_task"]
    title: str = Field(min_length=1, max_length=300)
    task_id: str | None = None
    expected_revision: int | None = Field(default=None, ge=1)
    due_at: AwareDatetime | None = None
    due_note: str | None = Field(default=None, max_length=200)


class AssignTask(ActionData):
    op: Literal["assign_task"]
    title: str = Field(min_length=1, max_length=300)
    member_id: str
    member_name: str = Field(min_length=1, max_length=200)
    task_id: str | None = None
    expected_revision: int | None = Field(default=None, ge=1)
    due_at: AwareDatetime | None = None
    due_note: str | None = Field(default=None, max_length=200)


class UpdateTask(ActionData):
    op: Literal["update_task"]
    task_id: str
    expected_revision: int = Field(ge=1)
    title: str | None = Field(default=None, max_length=300)
    status: Literal["open", "in_progress", "completed", "cancelled"] | None = None
    progress: str | None = Field(default=None, max_length=3000)
    due_at: AwareDatetime | None = None
    due_note: str | None = Field(default=None, max_length=200)
    clear_due: bool = False
    evidence_refs: list[str] | None = Field(default=None, max_length=20)


class TaskBatch(ActionData):
    op: Literal["task_batch"]
    actions: list[Annotated[ClaimTask | AssignTask | UpdateTask, Field(discriminator="op")]] = (
        Field(min_length=2, max_length=10)
    )


class CreateSchedule(ActionData):
    op: Literal["create_schedule"]
    title: str = Field(min_length=1, max_length=300)
    goal: str = Field(min_length=1, max_length=3000)
    timing: dict[str, JsonValue]
    valid_until: AwareDatetime | None = None


class ChangeSchedule(ActionData):
    op: Literal["change_schedule"]
    schedule_id: str
    expected_revision: int = Field(ge=1)
    timing: dict[str, JsonValue] | None = None
    goal: str | None = Field(default=None, max_length=3000)
    valid_until: AwareDatetime | None = None


class ControlSchedule(ActionData):
    op: Literal["control_schedule"]
    schedule_id: str
    expected_revision: int = Field(ge=1)
    command: Literal["pause", "resume", "cancel"]


class Remember(ActionData):
    op: Literal["remember"]
    predicate: str = Field(min_length=1, max_length=200)
    value: JsonValue
    kind: Literal["fact", "decision"] = "fact"


class CorrectMemory(ActionData):
    op: Literal["correct_memory"]
    fact_id: str
    expected_revision: int = Field(ge=1)
    value: JsonValue


class Forget(ActionData):
    op: Literal["forget"]
    event_ids: list[str] = Field(default_factory=list, max_length=100)
    fact_ids: list[str] = Field(default_factory=list, max_length=100)
    task_ids: list[str] = Field(default_factory=list, max_length=100)


class Quiet(ActionData):
    op: Literal["quiet"]
    scope: Literal["self", "group"] = "self"
    enabled: bool
    until: AwareDatetime | None = None
    hours: list[str] | None = None

    @field_validator("hours")
    @classmethod
    def windows(cls, value: list[str] | None) -> list[str] | None:
        return validate_windows(value) if value is not None else None


class Participation(ActionData):
    op: Literal["participation"]
    mode: Literal["natural", "on_request", "paused", "disabled"]


class ConfirmName(ActionData):
    op: Literal["confirm_name"]
    member_id: str
    name: str = Field(min_length=1, max_length=200)
    expected_revision: int = Field(ge=1)


Action = Annotated[
    ClaimTask
    | AssignTask
    | UpdateTask
    | CreateSchedule
    | ChangeSchedule
    | ControlSchedule
    | Remember
    | CorrectMemory
    | Forget
    | Quiet
    | Participation
    | ConfirmName
    | TaskBatch,
    Field(discriminator="op"),
]
ACTION_ADAPTER: TypeAdapter[Action] = TypeAdapter(Action)


class OutputRequirement(ActionData):
    format: Literal["text", "md", "docx", "pdf", "pptx", "png", "jpeg", "wav", "ogg"]
    exact_units: int | None = Field(default=None, ge=1, le=200)
    expected_text: list[Annotated[str, Field(max_length=1000)]] = Field(
        default_factory=list,
        max_length=30,
        description="User-required visible text snippets only. A PPTX topic/theme is document title metadata, not automatically visible slide text when different per-slide titles are explicitly given. Do not turn metadata naming or inferred summaries into extra visible-text requirements.",
    )
