"""Control-plane selections, independent of conversation and execution backends.

A selector records the user's resource boundary. Resolution adds database
identities and revisions; interpretation must never replace a plural or filtered
selection with one convenient work ID.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Annotated, Any, Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationInfo, field_validator, model_validator

from groupagent.domain import WorkState

_WorkId = Annotated[str, Field(min_length=1, max_length=200)]
WorkCapability = Literal[
    "public_research",
    "public_images",
    "documents",
    "image_analysis",
    "audio_transcription",
    "compute",
    "team_tasks",
    "scheduling",
]


class WorkSelector(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, hide_input_in_errors=True)

    basis: Literal["focus", "quote", "ids", "query"]
    states: tuple[WorkState, ...] = Field(
        default=(),
        max_length=len(WorkState),
        description="Preserve every requested target-state restriction; an empty set means no state restriction. The requested operation is separate from the target state.",
    )
    requester_scope: Literal["self", "any"] = Field(
        default="self",
        description="Filter the original TASK REQUESTER: self means work initiated by the current member; any means no requester filter. Referring to a set I selected does not imply I initiated its tasks. This filter grants no control authority.",
    )
    quantifier: Literal["one", "all"] = "one"
    ids: tuple[_WorkId, ...] = Field(default=(), max_length=1000)
    title_query: str | None = Field(default=None, min_length=1, max_length=300)
    exclude_ids: tuple[_WorkId, ...] = Field(default=(), max_length=1000)

    @model_validator(mode="before")
    @classmethod
    def legacy_ownership(cls, value: Any, info: ValidationInfo) -> Any:
        if not isinstance(value, Mapping):
            return value
        data = dict(value)
        if "ownership" in data:
            if "requester_scope" in data:
                raise ValueError("Use one requester filter, not both current and legacy names")
            legacy = data.pop("ownership")
            mapped = (
                {"mine": "self", "group": "any"}.get(legacy) if isinstance(legacy, str) else None
            )
            if mapped is None:
                raise ValueError("Unknown legacy ownership filter")
            data["requester_scope"] = mapped
        # Model-level before validation materializes JSON as Python objects.
        # Preserve JSON array/enum semantics during strict candidate checking.
        if info.mode == "json":
            for name in ("ids", "exclude_ids"):
                if isinstance(data.get(name), list):
                    data[name] = tuple(data[name])
            if isinstance(data.get("states"), list):
                data["states"] = tuple(
                    WorkState(state) if isinstance(state, str) else state
                    for state in data["states"]
                )
        return data

    @property
    def ownership(self) -> Literal["mine", "group"]:
        """Internal compatibility for stored decisions and the application adapter."""
        return "mine" if self.requester_scope == "self" else "group"

    @field_validator("title_query")
    @classmethod
    def meaningful_title(cls, value: str | None) -> str | None:
        if value is not None and not value.strip():
            raise ValueError("A title query must identify actual text")
        return value

    @model_validator(mode="after")
    def coherent(self) -> WorkSelector:
        if self.basis == "ids" and not self.ids:
            raise ValueError("An ID selection needs explicit work IDs")
        if self.basis != "ids" and self.ids:
            raise ValueError("Work IDs belong only to an ID selection")
        if len(set(self.ids)) != len(self.ids) or len(set(self.exclude_ids)) != len(
            self.exclude_ids
        ):
            raise ValueError("A selection cannot repeat work identities")
        if len(set(self.states)) != len(self.states):
            raise ValueError("A selection cannot repeat states")
        if self.basis == "ids" and self.quantifier == "one" and len(self.ids) != 1:
            raise ValueError("A singular selection needs one work ID")
        if (
            self.basis == "query"
            and self.quantifier == "one"
            and not self.states
            and self.title_query is None
        ):
            raise ValueError("A singular query needs a state or title restriction")
        return self


class WorkRef(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, hide_input_in_errors=True)

    id: _WorkId
    revision: int = Field(ge=1, strict=True)
    state: WorkState


class ResolvedWorkSet(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, hide_input_in_errors=True)

    group_id: str = Field(min_length=1)
    actor_id: str = Field(min_length=1)
    source_event_id: str = Field(min_length=1)
    selector: WorkSelector
    refs: tuple[WorkRef, ...] = Field(min_length=1, max_length=1000)

    @property
    def ids(self) -> tuple[str, ...]:
        return tuple(ref.id for ref in self.refs)
