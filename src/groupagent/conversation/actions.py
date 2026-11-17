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
    model_validator,
)
from pydantic_core import PydanticCustomError

from groupagent.scheduling.models import PreparedMessage

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
    goal: str | None = Field(default=None, min_length=1, max_length=3000)
    prepared_message: PreparedMessage | None = None
    fact_source_ids: list[str] = Field(default_factory=list, max_length=20)
    allow_silent_result: bool = Field(
        default=False,
        strict=True,
        description="Enable only for a future goal when the requester explicitly allows no message "
        "if the completed check finds nothing to report.",
    )
    timing: dict[str, JsonValue]
    valid_until: AwareDatetime | None = None

    @model_validator(mode="after")
    def single_payload(self) -> CreateSchedule:
        if (self.goal is None) == (self.prepared_message is None):
            raise ValueError("Provide one future goal or prepared message")
        if self.allow_silent_result and self.prepared_message is not None:
            raise ValueError("Only a future goal may allow a silent result")
        return self


class ChangeSchedule(ActionData):
    op: Literal["change_schedule"]
    schedule_id: str
    expected_revision: int = Field(ge=1)
    timing: dict[str, JsonValue] | None = None
    goal: str | None = Field(default=None, max_length=3000)
    prepared_message: PreparedMessage | None = None
    fact_source_ids: list[str] | None = Field(default=None, max_length=20)
    allow_silent_result: bool | None = Field(
        default=None,
        strict=True,
        description="Omit to keep the existing setting. Set true only for an explicitly "
        "authorized future goal. Set false to require a delivered result.",
    )
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
    exact_units: int | None = Field(
        default=None,
        ge=1,
        le=200,
        strict=True,
        description="An explicitly required exact page/slide count. Do not use for an upper/lower bound or an estimate; use min_units/max_units instead. Omit when count is unspecified.",
    )
    min_units: int | None = Field(
        default=None,
        ge=1,
        le=200,
        strict=True,
        description="An explicitly required minimum page/slide count (at least). May accompany max_units; never combine with exact_units.",
    )
    max_units: int | None = Field(
        default=None,
        ge=1,
        le=200,
        strict=True,
        description="An explicitly required maximum page/slide count (at most). A shorter valid document satisfies this bound; never turn it into an exact count or combine with exact_units.",
    )
    expected_text: list[Annotated[str, Field(max_length=1000)]] = Field(
        default_factory=list,
        max_length=30,
        description="User-required visible text snippets only. Title declaration delimiters such as 《Title》 or quotation marks name Title; omit those delimiter characters unless explicitly requested as literal content. A PPTX topic/theme is document title metadata, not automatically visible slide text when different per-slide titles are explicitly given. Do not turn metadata naming or inferred summaries into extra visible-text requirements.",
    )

    @model_validator(mode="after")
    def coherent_units(self) -> OutputRequirement:
        if self.exact_units is not None and (
            self.min_units is not None or self.max_units is not None
        ):
            raise PydanticCustomError(
                "exact_units_conflicts_with_range",
                "Choose an exact count or minimum/maximum bounds",
            )
        if (
            self.min_units is not None
            and self.max_units is not None
            and self.min_units > self.max_units
        ):
            raise PydanticCustomError(
                "min_units_exceeds_max_units", "Minimum count cannot exceed maximum count"
            )
        return self

    def accepts_unit_count(self, count: int | None) -> bool:
        """Compare measured pages/slides to the accepted, typed requirement."""
        if all(value is None for value in (self.exact_units, self.min_units, self.max_units)):
            return True
        if type(count) is not int or count < 1:
            return False
        return (
            (self.exact_units is None or count == self.exact_units)
            and (self.min_units is None or count >= self.min_units)
            and (self.max_units is None or count <= self.max_units)
        )
