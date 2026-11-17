"""Scoped identity and memory contracts; source IDs are always retained as strings."""

from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import AwareDatetime, Field, JsonValue, computed_field

from groupagent.domain import DomainModel, Identifier


class MemberIdentity(DomainModel):
    group_id: Identifier
    member_id: Identifier
    room_name: str | None = None
    confirmed_name: str | None = None
    confirmed_by: str | None = None
    membership_state: Literal["unknown", "present", "left"] = "unknown"
    membership_event_id: str | None = None
    revision: int = 1
    updated_at: AwareDatetime

    @computed_field  # type: ignore[prop-decorator]
    @property
    def display_name(self) -> str:
        return self.room_name or self.confirmed_name or "群昵称待补充"

    @computed_field  # type: ignore[prop-decorator]
    @property
    def name_source(self) -> str:
        return (
            "room_display_name"
            if self.room_name
            else "owner_confirmed"
            if self.confirmed_name
            else "unknown"
        )


class RosterMember(DomainModel):
    group_id: Identifier
    member_id: Identifier
    room_display_name: str | None = None
    membership_state: Literal["present", "unconfirmed"] = "unconfirmed"
    name_source: Literal["roomDisplayName", "unknown"] = "unknown"
    coverage: Literal["cached", "verified"] = "cached"


class RosterSnapshot(DomainModel):
    members: list[RosterMember]
    partial: bool = True
    source_generation: str


class RosterSyncResult(DomainModel):
    run_key: str
    state: Literal["fetching", "complete", "failed"]
    updated_members: int = 0
    partial: bool = True
    error_code: str | None = None
    cached: bool = False
    memory_epoch_before: int | None = Field(default=None, exclude=True)
    memory_epoch_after: int | None = Field(default=None, exclude=True)


class Topic(DomainModel):
    id: str
    group_id: str
    title: str
    visibility: Literal["group", "participants"]
    participant_ids: list[str]
    created_by: str
    revision: int
    created_at: AwareDatetime


class Fact(DomainModel):
    id: str
    group_id: str
    topic_id: str | None
    subject_id: str | None
    kind: Literal["fact", "decision"]
    predicate: str
    value: JsonValue
    source_event_ids: list[str]
    created_by: str
    request_key: str | None
    state: Literal["active", "invalidated", "forgotten"]
    revision: int
    created_at: AwareDatetime
    updated_at: AwareDatetime


class SourceReference(DomainModel):
    event_id: str
    actor_id: str
    occurred_at: AwareDatetime
    state: str


class MemoryHit(DomainModel):
    kind: Literal["event", "fact", "decision", "summary", "team_task"]
    id: str
    text: str
    source_refs: list[SourceReference] = Field(default_factory=list)
    author_id: str | None = None
    topic_id: str | None = None
    validity: str = "active"
    retrieval_reason: str
    incomplete: bool = False
    source_task_revisions: dict[str, int] = Field(default_factory=dict)
    provenance_kind: str | None = None


class SummaryInput(DomainModel):
    group_id: str
    topic_id: str | None
    events: list[dict[str, JsonValue]]
    estimated_tokens: int


class SummaryDraft(DomainModel):
    text: str = Field(min_length=1, max_length=24000)
    covered_event_ids: list[str] = Field(min_length=1)
    provider: str
    model: str
    usage: dict[str, int] = Field(default_factory=dict)


class MemorySummary(DomainModel):
    id: str
    group_id: str
    topic_id: str | None
    text: str
    source_event_ids: list[str]
    first_sequence: int
    last_sequence: int
    state: str
    incomplete: bool = True
    provider: str
    model: str
    usage: dict[str, int]
    created_by: str
    created_at: AwareDatetime


class CleanupJob(DomainModel):
    id: str
    group_id: str
    event_id: str | None
    work_id: str | None
    resource_kind: Literal["event_attachments", "work_artifacts", "backup_tombstone"]
    resource_refs: list[str]
    state: Literal["pending", "complete"]
    not_before: AwareDatetime
    revision: int
    evidence_ref: str | None
    created_at: AwareDatetime


class ForgetResult(DomainModel):
    event_ids: list[str]
    redacted_quote_ids: list[str] = Field(default_factory=list)
    fact_ids: list[str]
    task_ids: list[str] = Field(default_factory=list)
    invalidated_summaries: int
    cleanup_ids: list[str]
    backups_delete_after: datetime
