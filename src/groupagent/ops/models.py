from __future__ import annotations

from typing import Literal

from pydantic import AwareDatetime, Field

from groupagent.domain import DomainModel, TeamTaskState


class TableDigest(DomainModel):
    count: int
    sha256: str


class BackupManifest(DomainModel):
    version: Literal[1] = 1
    id: str
    group_ids: list[str]
    created_at: AwareDatetime
    expires_at: AwareDatetime
    source_identity: str
    build_ref: str
    postgres_version: str
    pg_dump_version: str
    archive_sha256: str
    archive_bytes: int
    schema_versions: dict[str, str]
    tables: dict[str, TableDigest]


class BackupRecord(DomainModel):
    id: str
    state: str
    manifest: BackupManifest | None
    archive_ref: str


class RestoreReport(DomainModel):
    id: str
    backup_id: str
    target_identity: str
    group_ids: list[str]
    verified_tables: int
    applied_tombstones: int
    quarantined: bool = True
    backup_created_at: AwareDatetime
    restored_at: AwareDatetime


class DeletionMarker(DomainModel):
    group_id: str
    source_key: str
    event_id: str | None
    platform_message_id: str | None
    channel_account_id: str | None
    reason: Literal["expired", "recalled", "forgotten"]
    task_id: str | None = None


class LegacyMember(DomainModel):
    member_id: str
    room_name: str | None = None
    confirmed_name: str | None = None
    name_source: str


class LegacyTask(DomainModel):
    legacy_id: str
    target_id: str
    title: str
    owner_id: str | None
    due_at: AwareDatetime
    status: TeamTaskState
    progress: str


class LegacyReview(DomainModel):
    kind: str
    legacy_key: str
    body: dict = Field(default_factory=dict)


class LegacyPlan(DomainModel):
    id: str
    source_sha256: str
    source_bytes: int
    source_system: str
    legacy_group_id: str
    group_id: str
    time_zone: str
    plan_sha256: str
    source_code_hashes: dict[str, str]
    members: list[LegacyMember]
    tasks: list[LegacyTask]
    reviews: list[LegacyReview]
    blockers: list[str]
    excluded_conversation_rows: int


class ImportResult(DomainModel):
    plan_id: str
    members: int
    tasks: int
    review_items: int
    already_applied: bool = False


class CleanupResult(DomainModel):
    completed: list[str]
    failed: list[str]
    tombstones_synced: int
    errors: dict[str, str] = Field(default_factory=dict)
    expired_artifact_versions: list[str] = Field(default_factory=list)
    deleted_audit_events: int = 0
