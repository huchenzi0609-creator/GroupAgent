"""Validated non-secret deployment configuration for the v0.1 contract."""

from __future__ import annotations

import json
from decimal import Decimal
from pathlib import Path
from typing import Annotated, Any, Literal
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from groupagent.identity import DEFAULT_BOT_BADGE, DEFAULT_BOT_NAME, LEGACY_BOT_ALIASES

PositiveInt = Annotated[int, Field(strict=True, gt=0)]


class ConfigError(ValueError):
    """A deployability error safe to display without exposing credentials."""


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class GroupConfig(StrictModel):
    enabled: bool = False
    internal_id: str = Field(min_length=1)
    external_group_id: str = Field(min_length=1)
    owner_ids: list[str] = Field(min_length=1)
    observer_ids: list[str] = Field(default_factory=list)
    maintainer_ids: list[str] = Field(default_factory=list)
    read_only_qa_roles: list[Literal["observer", "maintainer"]] = Field(default_factory=list)
    policy_revision: PositiveInt = 1
    time_zone: str = "Asia/Shanghai"
    participation_mode: Literal["natural", "on_request", "paused"] = "natural"
    quiet_hours: list[str] = Field(default_factory=list)
    roster_sync: Literal["on_startup"] = "on_startup"
    periodic_roster_refresh: Literal[False] = False
    bot_member_id: str | None = None
    shared_account: bool = True

    @field_validator("time_zone")
    @classmethod
    def valid_zone(cls, value: str) -> str:
        try:
            ZoneInfo(value)
        except ZoneInfoNotFoundError as exc:
            raise ValueError("Unknown IANA timezone") from exc
        return value

    @field_validator("quiet_hours")
    @classmethod
    def valid_quiet_hours(cls, value: list[str]) -> list[str]:
        from groupagent.conversation.quiet import validate_windows

        return validate_windows(value)

    @field_validator("internal_id", "external_group_id")
    @classmethod
    def nonblank_id(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("Group ID cannot be blank")
        return value.strip()

    @field_validator("owner_ids", "observer_ids", "maintainer_ids")
    @classmethod
    def valid_owners(cls, owners: list[str]) -> list[str]:
        owners = [x.strip() for x in owners]
        if any(not x.strip() for x in owners) or len(set(owners)) != len(owners):
            raise ValueError("Role IDs must be non-empty and unique")
        return owners

    @model_validator(mode="after")
    def disjoint_roles(self) -> GroupConfig:
        roles = self.owner_ids + self.observer_ids + self.maintainer_ids
        if len(set(roles)) != len(roles):
            raise ValueError("A member cannot have overlapping configured roles")
        return self


class FeatureConfig(StrictModel):
    natural_participation: bool = True
    image_understanding: bool = True
    audio_transcription: bool = True
    speech_reply_on_request: bool = False
    image_generation: Literal[False] = False
    document_editing: bool = True
    web_research: bool = True
    durable_schedules: bool = True
    video_analysis: Literal[False] = False
    video_generation: Literal[False] = False
    realtime_voice_call: Literal[False] = False


class ConversationConfig(StrictModel):
    idle_batch_seconds: PositiveInt = 2
    max_batch_wait_seconds: PositiveInt = 5
    proactive_messages_per_10_minutes: Annotated[int, Field(strict=True, ge=0)] = 2
    min_progress_interval_seconds: PositiveInt = 60
    hard_reply_character_limit: None = None
    short_reply_style: Literal["one_to_three_short_sentences"] = "one_to_three_short_sentences"

    @model_validator(mode="after")
    def coherent_batch(self) -> ConversationConfig:
        if self.idle_batch_seconds > self.max_batch_wait_seconds:
            raise ValueError("Idle batch window exceeds maximum wait")
        return self


class LimitsConfig(StrictModel):
    image_bytes: PositiveInt = 15 * 1024**2
    image_pixels: PositiveInt = 40_000_000
    images_per_work: PositiveInt = 10
    markdown_bytes: PositiveInt = 2 * 1024**2
    office_or_pdf_bytes: PositiveInt = 30 * 1024**2
    document_render_pages: PositiveInt = 200
    presentation_slides: PositiveInt = 100
    audio_file_bytes: PositiveInt = 25 * 1024**2
    audio_file_seconds: PositiveInt = 600
    input_files_per_work: PositiveInt = 20
    total_input_bytes_per_work: PositiveInt = 100 * 1024**2
    workspace_bytes: PositiveInt = 512 * 1024**2
    output_file_bytes: PositiveInt = 25 * 1024**2


class ExecutionConfig(StrictModel):
    max_model_rounds: PositiveInt = 24
    max_total_tokens: PositiveInt | None = None
    max_tool_calls: PositiveInt = 60
    max_active_seconds: PositiveInt = 1200
    max_subagents: Annotated[int, Field(strict=True, ge=0)] = 2
    max_concurrent_heavy_work: PositiveInt = 1
    tool_timeout_seconds: PositiveInt = 60
    browser_timeout_seconds: PositiveInt = 90
    render_timeout_seconds: PositiveInt = 180
    additional_retry_limit: Annotated[int, Field(strict=True, ge=0, le=5)] = 2
    lease_seconds: PositiveInt = 30
    heartbeat_seconds: PositiveInt = 10
    cancel_grace_seconds: PositiveInt = 10

    @model_validator(mode="after")
    def coherent_lease(self) -> ExecutionConfig:
        if self.heartbeat_seconds >= self.lease_seconds:
            raise ValueError("Heartbeat must be shorter than lease")
        return self


class ScheduleConfig(StrictModel):
    default_valid_days: PositiveInt = 30
    max_active_per_group: PositiveInt = 10
    min_interval_seconds: Annotated[int, Field(strict=True, ge=3600)] = 3600
    missed_one_shot_grace_seconds: PositiveInt = 86400
    misfire_policy: Literal["latest_only"] = "latest_only"
    overlap_policy: Literal["coalesce"] = "coalesce"


class RetentionConfig(StrictModel):
    raw_message_days: PositiveInt = 30
    raw_attachment_days: PositiveInt = 30
    generated_artifact_days: PositiveInt = 90
    audit_days: PositiveInt = 90
    backup_days: PositiveInt = 7
    project_facts: Literal["until_archive_or_authorized_deletion"] = (
        "until_archive_or_authorized_deletion"
    )
    owner_approval_required_before_production: bool = True


class SpendConfig(StrictModel):
    currency: Literal["CNY", "USD"] = "CNY"
    group_daily_limit: Annotated[Decimal | None, Field(gt=0, allow_inf_nan=False)] = None
    session_total_limit: Annotated[Decimal | None, Field(gt=0, allow_inf_nan=False)] = None
    pricing_snapshot_id: str | None = None
    production_requires_explicit_limit: Literal[True] = True

    @field_validator("pricing_snapshot_id")
    @classmethod
    def nonblank_snapshot(cls, value: str | None) -> str | None:
        if value is not None:
            if not value.strip():
                raise ValueError("Pricing snapshot ID cannot be blank")
            return value.strip()
        return value


class ModelRoutes(StrictModel):
    text_agent: str = Field(default="CONFIGURE_AND_VALIDATE", min_length=1)
    vision: str = Field(default="CONFIGURE_AND_VALIDATE", min_length=1)
    speech_to_text: str = Field(default="CONFIGURE_AND_VALIDATE", min_length=1)
    text_to_speech: str = Field(default="CONFIGURE_AND_VALIDATE", min_length=1)
    image_generation: str = Field(default="NOT_IN_CURRENT_SCOPE", min_length=1)

    @field_validator("text_agent", "vision", "speech_to_text", "text_to_speech", "image_generation")
    @classmethod
    def nonblank_route(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("Model route cannot be blank")
        return value.strip()


class EngineConfig(StrictModel):
    candidate: Literal["deepseek_harness"] = "deepseek_harness"
    approved_runtime_version: str | None = None
    extra_session_log_upload: Literal[False] = False


class ResourceConfig(StrictModel):
    """An opt-in host budget; neighbouring services keep their own resources."""

    profile: Literal["standard", "server_2gb"] = "standard"
    worker_memory_mb: Annotated[int, Field(strict=True, ge=512, le=4096)] = 1024
    reserve_memory_mb: Annotated[int, Field(strict=True, ge=64, le=4096)] = 128
    transfer_memory_mb: Annotated[int, Field(strict=True, ge=32, le=1024)] = 128
    worker_cpus: float = Field(default=1, gt=0, le=4)
    worker_cpu_shares: Annotated[int, Field(strict=True, ge=2, le=262144)] = 1024
    worker_oom_score_adj: Annotated[int, Field(strict=True, ge=0, le=1000)] = 0
    worker_pids: Annotated[int, Field(strict=True, ge=32, le=512)] = 256
    worker_workspace_mb: Annotated[int, Field(strict=True, ge=32, le=512)] = 512
    worker_temp_mb: Annotated[int, Field(strict=True, ge=32, le=512)] = 128
    retry_seconds: Annotated[int, Field(strict=True, ge=1, le=60)] = 5
    maintenance_interval_seconds: Annotated[int, Field(strict=True, ge=60, le=3600)] = 60

    @model_validator(mode="before")
    @classmethod
    def profile_defaults(cls, value: Any) -> Any:
        if isinstance(value, dict) and value.get("profile") == "server_2gb":
            value = dict(value)
            for key, default in {
                "worker_memory_mb": 512,
                "worker_cpus": 0.75,
                "worker_cpu_shares": 128,
                "worker_oom_score_adj": 500,
                "worker_pids": 192,
                "worker_workspace_mb": 128,
                "worker_temp_mb": 64,
                "maintenance_interval_seconds": 180,
            }.items():
                value.setdefault(key, default)
        return value

    @property
    def enabled(self) -> bool:
        return self.profile == "server_2gb"


class RuntimeConfig(StrictModel):
    channel: Literal["local", "agentwechat"] = "local"
    channel_base_url: str = "http://127.0.0.1:8788"
    channel_account_id: str = "local-account"
    source_epoch: str = "local-v1"
    allow_live_send: bool = False
    engine: Literal["harness", "scoped"] = "harness"
    worker_image: str | None = None
    harness_image: str | None = None
    docker_host: str | None = None
    poll_seconds: float = Field(default=0.25, gt=0, le=30)
    early_ack_enabled: bool = False
    early_ack_seconds: float = Field(default=2.0, gt=0, le=8)
    broker_control_socket: str | None = None
    message_chunk_characters: int = Field(default=1800, ge=100, le=4000)
    bot_name: str = Field(default=DEFAULT_BOT_NAME, min_length=1, max_length=64)
    bot_aliases: list[str] = Field(default_factory=lambda: list(LEGACY_BOT_ALIASES), max_length=20)
    bot_badge: str = Field(default=DEFAULT_BOT_BADGE, min_length=1, max_length=66)
    browser_hosts: list[str] = Field(default_factory=list, max_length=30)
    image_generation_price_id: str | None = None
    semantic_validation: bool = True
    local_member_names: dict[str, str] = Field(default_factory=dict)
    pg_bin_dir: str | None = None

    @field_validator("broker_control_socket")
    @classmethod
    def local_broker_socket(cls, value: str | None) -> str | None:
        if value is not None and (
            not Path(value).is_absolute() or "\x00" in value or len(value.encode()) > 104
        ):
            raise ValueError("The broker control socket must be a bounded absolute Unix path")
        return value

    @field_validator("bot_name", "bot_badge")
    @classmethod
    def printable_identity(cls, value: str) -> str:
        value = value.strip()
        if not value or not value.isprintable():
            raise ValueError("Bot identity must be nonblank printable text")
        return value

    @field_validator("bot_aliases")
    @classmethod
    def valid_bot_aliases(cls, values: list[str]) -> list[str]:
        aliases = [value.strip() for value in values]
        if any(not value or not value.isprintable() or len(value) > 64 for value in aliases):
            raise ValueError(
                "Bot aliases must be nonblank printable names of at most 64 characters"
            )
        if len({value.casefold() for value in aliases}) != len(aliases):
            raise ValueError("Bot aliases must be unique ignoring case")
        return aliases

    @model_validator(mode="after")
    def coherent_bot_identity(self) -> RuntimeConfig:
        if self.bot_badge != f"【{self.bot_name}】":
            raise ValueError("Bot badge must match the configured bot name")
        return self

    @field_validator("worker_image", "harness_image")
    @classmethod
    def immutable_image(cls, value: str | None) -> str | None:
        import re

        if value is not None and not re.fullmatch(
            r"(?:sha256:[a-f0-9]{64}|[A-Za-z0-9./:_-]+@sha256:[a-f0-9]{64})", value
        ):
            raise ValueError("A worker image ID or digest is required")
        return value


class AppConfig(StrictModel):
    schema_version: Literal["groupagent.config.v0.1"] = "groupagent.config.v0.1"
    deployment_mode: Literal["development", "test", "production"] = "development"
    group: GroupConfig
    features: FeatureConfig = Field(default_factory=FeatureConfig)
    conversation: ConversationConfig = Field(default_factory=ConversationConfig)
    limits: LimitsConfig = Field(default_factory=LimitsConfig)
    execution: ExecutionConfig = Field(default_factory=ExecutionConfig)
    schedule: ScheduleConfig = Field(default_factory=ScheduleConfig)
    retention: RetentionConfig = Field(default_factory=RetentionConfig)
    spend: SpendConfig = Field(default_factory=SpendConfig)
    model_routes: ModelRoutes = Field(default_factory=ModelRoutes)
    engine: EngineConfig = Field(default_factory=EngineConfig)
    runtime: RuntimeConfig = Field(default_factory=RuntimeConfig)
    resources: ResourceConfig = Field(default_factory=ResourceConfig)

    @model_validator(mode="before")
    @classmethod
    def low_memory_defaults(cls, value: Any) -> Any:
        if isinstance(value, dict):
            resource = value.get("resources")
            profile = (
                resource.profile
                if isinstance(resource, ResourceConfig)
                else (resource.get("profile") if isinstance(resource, dict) else None)
            )
            if profile == "server_2gb":
                value = dict(value)
                raw_runtime = value.get("runtime", {})
                runtime = (
                    raw_runtime.model_dump(exclude_unset=True)
                    if isinstance(raw_runtime, RuntimeConfig)
                    else dict(raw_runtime)
                )
                runtime.setdefault("engine", "scoped")
                value["runtime"] = runtime
                raw_limits = value.get("limits", {})
                limits = (
                    raw_limits.model_dump(exclude_unset=True)
                    if isinstance(raw_limits, LimitsConfig)
                    else dict(raw_limits)
                )
                for key, default in {
                    "image_bytes": 8 * 1024**2,
                    "image_pixels": 12_000_000,
                    "images_per_work": 4,
                    "office_or_pdf_bytes": 16 * 1024**2,
                    "input_files_per_work": 8,
                    "total_input_bytes_per_work": 32 * 1024**2,
                    "workspace_bytes": 128 * 1024**2,
                }.items():
                    limits.setdefault(key, default)
                value["limits"] = limits
        return value

    @model_validator(mode="after")
    def resource_contract(self) -> AppConfig:
        if self.resources.enabled and (
            self.runtime.engine != "scoped" or self.execution.max_concurrent_heavy_work != 1
        ):
            raise ValueError("The 2GB profile requires scoped execution and one active heavy work")
        return self

    def production_blockers(self) -> list[str]:
        blockers = []
        if not self.group.enabled:
            blockers.append("group_disabled")
        if self.group.external_group_id.startswith("CONFIGURE"):
            blockers.append("group_not_configured")
        if self.spend.group_daily_limit is None or not self.spend.pricing_snapshot_id:
            blockers.append("spend_limit_or_price_snapshot_missing")
        if self.retention.owner_approval_required_before_production:
            blockers.append("retention_not_approved")
        if not (
            self.engine.approved_runtime_version and self.engine.approved_runtime_version.strip()
        ):
            blockers.append("engine_not_validated")
        routes = {
            "text_agent": True,
            "vision": self.features.image_understanding,
            "speech_to_text": self.features.audio_transcription,
            "text_to_speech": self.features.speech_reply_on_request,
            "image_generation": self.features.image_generation,
        }
        for name, required in routes.items():
            if required and getattr(self.model_routes, name).startswith("CONFIGURE"):
                blockers.append("model_route_missing:" + name)
        return blockers

    def require_production_ready(self) -> None:
        blockers = self.production_blockers()
        if blockers:
            raise ConfigError("Production configuration incomplete: " + ", ".join(blockers))


def load_config(path: str | Path) -> AppConfig:
    config = AppConfig.model_validate(json.loads(Path(path).read_text(encoding="utf-8")))
    if config.deployment_mode == "production":
        config.require_production_ready()
    return config
