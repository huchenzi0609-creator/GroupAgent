"""Durable intake and decision application, separate from long-running execution.

Only trusted channel adapters call receive(..., verified_channel=True). This is
not an unauthenticated public HTTP endpoint. A model supplies a candidate decision,
never actor identity, role, group scope or a successful delivery receipt.
"""

from __future__ import annotations

from typing import Any, Literal, NoReturn, Protocol

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    JsonValue,
    model_validator,
)
from pydantic_core import PydanticCustomError

from groupagent.config import AppConfig
from groupagent.control import WorkCapability, WorkSelector
from groupagent.domain import ActorContext, DomainError, EventKind, GroupEvent, Role, WorkState
from groupagent.persistence import PostgresStore

DECISION_CONTRACT_ERRORS = {
    "decision_action_kind": ("action", "Immediate actions require command or legacy reply"),
    "decision_action_required": ("action", "An immediate command requires an action"),
    "decision_response_required": ("response", "A reply or clarification requires text"),
    "decision_goal_required": ("goal", "Starting or updating work requires a goal"),
    "decision_capabilities_kind": (
        "work_capabilities",
        "Capability plans require start or update work",
    ),
    "decision_image_count_kind": (
        "public_image_count",
        "Image counts require start or update work",
    ),
    "decision_selector_kind": ("work_selector", "Selectors require work control or query"),
    "decision_target_required": ("target_work_id", "Updating work requires an exact target"),
    "decision_revision_required": (
        "expected_revision",
        "Updating work requires its current revision",
    ),
    "decision_control_required": ("control", "Work control requires an operation"),
    "decision_query_goal_forbidden": ("goal", "A work query cannot revise the goal"),
    "decision_query_control_forbidden": ("control", "A work query cannot control execution"),
    "decision_reference_conflict": (
        "work_selector",
        "Do not mix a selector and a legacy exact target",
    ),
    "decision_reference_required": (
        "work_selector",
        "Work control or query requires a resource reference",
    ),
}


def _invalid_decision(code: str) -> NoReturn:
    # Fixed codes and messages contain no candidate input or inferred identity.
    _, message = DECISION_CONTRACT_ERRORS[code]
    raise PydanticCustomError(code, message)


class RequirementEvidence(BaseModel):
    """A candidate citation; the host checks the actual authorized human source."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    event_id: str = Field(min_length=1, max_length=256)
    quote: str = Field(min_length=1, max_length=2000)


class Decision(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    kind: Literal[
        "ignore",
        "observe",
        "reply",
        "command",
        "clarify",
        "start_work",
        "update_work",
        "control_work",
        "query_work",
    ]
    reason_code: str = Field(min_length=1, max_length=100)
    response: str | None = Field(default=None, max_length=12000)
    goal: str | None = Field(default=None, max_length=12000)
    acceptance_criteria: list[str] = Field(default_factory=list, max_length=20)
    target_work_id: str | None = None
    expected_revision: int | None = Field(default=None, ge=1)
    work_selector: WorkSelector | None = None
    control: Literal["pause", "cancel", "resume", "retry"] | None = None
    directness: Literal["direct", "open", "proactive", "other", "none", "unknown"] = "unknown"
    confidence: float = Field(default=0, ge=0, le=1)
    action: dict[str, JsonValue] | None = None
    topic_id: str | None = None
    topic_title: str | None = Field(default=None, max_length=200)
    input_versions: list[str] = Field(default_factory=list, max_length=20)
    input_event_ids: list[str] = Field(default_factory=list, max_length=20)
    outputs: list[dict[str, JsonValue]] = Field(default_factory=list, max_length=10)
    work_capabilities: list[WorkCapability] = Field(
        default_factory=list,
        max_length=8,
        description="Requested capability families for this work plan, not permission or tool grants. The backend intersects the plan with trusted configuration and authorization.",
    )
    public_image_count: int | None = Field(
        default=None,
        ge=1,
        le=10,
        strict=True,
        description="An explicit user-requested image quantity only; omit when unspecified. Never choose a planning target here.",
    )
    public_image_count_evidence: RequirementEvidence | None = None

    @model_validator(mode="after")
    def coherent(self) -> Decision:
        if self.action is not None and self.kind not in {"reply", "command"}:
            _invalid_decision("decision_action_kind")
        if self.kind == "command" and self.action is None:
            _invalid_decision("decision_action_required")
        if self.kind in {"reply", "clarify"} and not (self.response and self.response.strip()):
            _invalid_decision("decision_response_required")
        if self.kind in {"start_work", "update_work"} and not (self.goal and self.goal.strip()):
            _invalid_decision("decision_goal_required")
        if self.kind not in {"start_work", "update_work"}:
            if self.work_capabilities:
                _invalid_decision("decision_capabilities_kind")
            if self.public_image_count is not None or self.public_image_count_evidence is not None:
                _invalid_decision("decision_image_count_kind")
        if self.work_selector is not None and self.kind not in {"control_work", "query_work"}:
            _invalid_decision("decision_selector_kind")
        if self.kind == "update_work":
            if not self.target_work_id:
                _invalid_decision("decision_target_required")
            if self.expected_revision is None:
                _invalid_decision("decision_revision_required")
        if self.kind == "control_work" and self.control is None:
            _invalid_decision("decision_control_required")
        if self.kind == "query_work":
            if self.goal is not None:
                _invalid_decision("decision_query_goal_forbidden")
            if self.control is not None:
                _invalid_decision("decision_query_control_forbidden")
        if self.kind in {"control_work", "query_work"}:
            legacy_target = self.target_work_id is not None or self.expected_revision is not None
            if self.work_selector is not None:
                if legacy_target:
                    _invalid_decision("decision_reference_conflict")
            elif not (self.target_work_id and self.expected_revision):
                _invalid_decision("decision_reference_required")
        return self


class CoordinationResult(BaseModel):
    event_id: str
    decision: Decision
    work_id: str | None = None
    delivery_id: str | None = None
    duplicate: bool = False
    action_result: dict[str, JsonValue] | None = None
    controlled_work_ids: list[str] = Field(default_factory=list)


class DecisionProvider(Protocol):
    async def decide(self, event: GroupEvent, context: dict[str, Any]) -> Decision: ...


class ObserveOnlyProvider:
    """Explicit shadow fallback. It does not pretend to understand natural dialogue."""

    async def decide(self, event: GroupEvent, context: dict[str, Any]) -> Decision:
        return Decision(kind="observe", reason_code="shadow_no_model")


class ScriptedDecisionProvider:
    """Synthetic replay only; not a natural-language acceptance result."""

    def __init__(self, decisions: dict[str, Decision]) -> None:
        self.decisions = decisions
        self.calls: list[str] = []

    async def decide(self, event: GroupEvent, context: dict[str, Any]) -> Decision:
        self.calls.append(event.source_key)
        return self.decisions.get(
            event.source_key, Decision(kind="observe", reason_code="synthetic_unmapped")
        )


class Coordinator:
    def __init__(self, config: AppConfig, store: PostgresStore, provider: DecisionProvider) -> None:
        self.config, self.store, self.provider = config, store, provider

    def accepts_event(self, event: GroupEvent) -> bool:
        return True

    def actor(self, event: GroupEvent) -> ActorContext:
        group = self.config.group
        if event.group_id != group.internal_id or not group.enabled:
            raise DomainError("scope_violation", "This group is not enabled")
        if event.actor_id in group.owner_ids:
            role = Role.OWNER
        elif event.actor_id in group.maintainer_ids:
            role = Role.MAINTAINER
        elif event.actor_id in group.observer_ids:
            role = Role.OBSERVER
        else:
            role = Role.MEMBER
        return ActorContext(
            group_id=group.internal_id,
            member_id=event.actor_id,
            role=role,
            policy_revision=group.policy_revision,
        )

    async def receive(
        self,
        event: GroupEvent,
        *,
        stream_id: str,
        cursor: str,
        verified_channel: bool = False,
    ) -> CoordinationResult:
        if not verified_channel:
            raise DomainError("permission_denied", "Unverified channel input is not admitted")
        actor = self.actor(event)
        with self.store.transaction(actor, trace_id=event.id) as tx:
            previous_cursor = tx.get_cursor(stream_id)
            ingested = tx.ingest_event(
                event,
                stream_id=stream_id,
                cursor=cursor,
                expected_cursor_revision=previous_cursor.revision if previous_cursor else 0,
            )
        # Canonical persisted event wins over a replay carrying altered actor/text.
        return await self.process(ingested.event)

    async def process(self, event: GroupEvent) -> CoordinationResult:
        actor = self.actor(event)
        with self.store.transaction(actor, trace_id=event.id) as tx:
            event = tx.get_event(event.id)
            if getattr(event, "content_state", "active") != "active":
                return CoordinationResult(
                    event_id=event.id,
                    decision=Decision(kind="ignore", reason_code="source_unavailable"),
                    duplicate=True,
                )
            if event.actor_id != actor.member_id:
                raise DomainError(
                    "permission_denied", "Event identity does not match its durable source"
                )
            previous = tx.get_decision(event.id)
            if previous:
                return CoordinationResult.model_validate(previous["result"]).model_copy(
                    update={"duplicate": True}
                )
            is_echo = bool(
                event.platform_message_id and tx.is_bot_message(event.platform_message_id)
            )
            works = [
                w.model_dump(mode="json")
                for w in tx.list_work(limit=20, active_first=True, newest_first=True)
            ]
        if is_echo:
            decision = Decision(kind="ignore", reason_code="verified_bot_receipt")
        elif event.kind == EventKind.UNSUPPORTED_VIDEO:
            decision = Decision(kind="observe", reason_code="unsupported_video")
        elif event.kind in {EventKind.SYSTEM, EventKind.MEMBERSHIP, EventKind.RECALL}:
            decision = Decision(kind="observe", reason_code="non_dialogue_event")
        elif self.config.group.participation_mode == "paused":
            decision = Decision(kind="observe", reason_code="group_paused")
        elif actor.role in {Role.OBSERVER, Role.MAINTAINER} and (
            actor.role.value not in self.config.group.read_only_qa_roles
        ):
            decision = Decision(kind="observe", reason_code="read_only_qa_not_authorized")
        else:
            decision = await self.provider.decide(
                event,
                {
                    "works": works,
                    "policy_revision": actor.policy_revision,
                    "read_only": actor.role in {Role.OBSERVER, Role.MAINTAINER},
                },
            )
            if actor.role in {Role.OBSERVER, Role.MAINTAINER} and decision.kind in {
                "start_work",
                "update_work",
                "control_work",
            }:
                decision = Decision(
                    kind="reply",
                    reason_code="read_only_scope",
                    response="你当前的身份可以查询信息，暂时不能执行工具或修改任务。",
                )
        with self.store.transaction(actor, trace_id=event.id) as tx:
            previous = tx.get_decision(event.id)
            if previous:
                return CoordinationResult.model_validate(previous["result"]).model_copy(
                    update={"duplicate": True}
                )
            work_id = delivery_id = None
            if decision.kind == "start_work":
                work = tx.create_work(
                    decision.goal or "",
                    request_event_id=event.id,
                    acceptance_criteria=decision.acceptance_criteria,
                    scope={
                        "allowed_group_ids": [event.group_id],
                        "workspace_write": True,
                        "external_account_changes": False,
                        "delivery_target": "origin_group",
                    },
                    budget=self.config.execution.model_dump(mode="json"),
                )
                work_id = work.id
            elif decision.kind == "update_work":
                work = tx.revise_work(
                    decision.target_work_id or "",
                    goal=decision.goal or "",
                    expected_revision=decision.expected_revision or 0,
                    acceptance_criteria=decision.acceptance_criteria or None,
                )
                work_id = work.id
            elif decision.kind == "control_work":
                target = {
                    "cancel": WorkState.CANCELLED,
                    "pause": WorkState.PAUSED,
                    "resume": WorkState.QUEUED,
                    "retry": WorkState.QUEUED,
                }[decision.control or "pause"]
                work = tx.transition_work(
                    decision.target_work_id or "",
                    target,
                    expected_revision=decision.expected_revision or 0,
                    reason="retry" if decision.control == "retry" else "user_request",
                )
                work_id = work.id
            if decision.kind in {"reply", "clarify"}:
                text = decision.response or ""
                delivery = tx.enqueue_reply(
                    event.id,
                    text,
                    read_only_authorized=actor.role.value in self.config.group.read_only_qa_roles,
                )
                delivery_id = delivery.id
            result = CoordinationResult(
                event_id=event.id, decision=decision, work_id=work_id, delivery_id=delivery_id
            )
            tx.record_decision(
                event.id, {"kind": decision.kind, "result": result.model_dump(mode="json")}
            )
            return result
