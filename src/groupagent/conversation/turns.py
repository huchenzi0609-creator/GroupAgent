"""Trusted routing evidence, in-session reports and persisted legacy turn records."""

from __future__ import annotations

import json
import re
from functools import lru_cache
from typing import Any, Literal, get_args

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StrictBool,
    ValidationInfo,
    model_validator,
)
from pydantic_core import PydanticCustomError

from groupagent.domain import GroupEvent
from groupagent.identity import BotIdentity, trusted_name_source

from .actions import ACTION_ADAPTER
from .text import outside_quotes

Recipient = Literal["agent", "other", "ambient"]
ActionKind = Literal["start_work", "update_work", "control_work", "business", "read_work_facts"]
InformationBlocker = Literal["public_research_unavailable", "read_only_scope"]


class TurnData(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


ReceiptPhase = Literal["observed", "queued", "committed"]
VERBATIM_REPLY_REASON = "agent_verbatim_reply"


class TurnReport(TurnData):
    tool_call_id: str = Field(min_length=1, max_length=256)
    phase: ReceiptPhase


class ActionCompletion(TurnData):
    finish_on_commit: StrictBool = Field(
        default=False,
        description=(
            "Set true when this is the only tool call and the host's committed action receipt "
            "fully answers the request. A rejection stays in the turn for correction. Omit if "
            "you still need to answer a separate question or carry out another action."
        ),
    )


class MemoryConsent(TurnData):
    expected_op: Literal["none", "remember", "correct_memory", "forget"]
    authorization_excerpt: str | None = Field(
        default=None,
        min_length=1,
        max_length=240,
        description="The current speaker's exact short excerpt expressing lasting authority and its scope.",
    )

    @model_validator(mode="after")
    def correct_evidence_shape(self) -> MemoryConsent:
        if (self.expected_op != "none") != bool(self.authorization_excerpt):
            raise ValueError("A memory effect needs a source excerpt; none carries no excerpt")
        return self


class FinishTurn(TurnData):
    disposition: Literal["reply", "observe", "proactive"]
    open_request: StrictBool = Field(
        default=False,
        description="The human genuinely asked the group for an answer or help without addressing a particular person.",
    )
    open_request_evidence: str | None = Field(
        default=None,
        max_length=160,
        description="Quote the current human's actual question or invitation to the group when open_request is true.",
    )
    response: str | None = Field(
        default=None,
        max_length=12000,
        description=(
            "Natural answer to the question, or new information in addition to the reports. "
            "Omit for an effect-only reply: the host already renders all accepted action "
            "acknowledgements, scheduling times and work phases."
        ),
    )
    verbatim_reply: str | None = Field(
        default=None,
        min_length=1,
        max_length=4000,
        description=(
            "When asked to send only an exact supplied phrase, copy that phrase alone from "
            "the current human message. Omit response and submit this reply separately from any writes."
        ),
    )
    reports: list[TurnReport] = Field(
        default_factory=list,
        max_length=20,
        description="Optional evidence references. The host acknowledges committed effects automatically.",
    )
    needs_input: StrictBool = False
    requested_silence: StrictBool = Field(
        default=False,
        description="The current human explicitly requested no visible response to the recorded action.",
    )
    blocker: (
        Literal[
            "tool_rejected",
            "read_only_scope",
            "web_research",
            "public_image_retrieval",
            "document_editing",
            "image_understanding",
            "face_identification",
            "audio_transcription",
            "video_analysis",
            "speech_reply_on_request",
        ]
        | None
    ) = None

    @model_validator(mode="after")
    def verbatim_reply_is_the_only_reply_body(self) -> FinishTurn:
        if self.verbatim_reply is not None and (
            self.disposition != "reply"
            or self.response is not None
            or self.needs_input
            or self.requested_silence
            or self.blocker is not None
        ):
            raise PydanticCustomError(
                "verbatim_reply_requires_only_body",
                "A verbatim reply is the only reply body and has no blocker",
            )
        return self


def receipt_phase(receipt: dict[str, Any]) -> ReceiptPhase | None:
    if receipt.get("receipt_phase") == "observed" or receipt.get("status") == "observed":
        return "observed"
    if receipt.get("committed") is True:
        if receipt.get("kind") == "start_work":
            return "queued"
        return "committed"
    return None


def receipt_message(receipt: dict[str, Any]) -> str | None:
    if receipt.get("committed") is not True:
        return None
    decision = receipt.get("result", {}).get("decision", {})
    if decision.get("kind") in {"observe", "ignore"}:
        return None
    text = receipt.get("host_message") or decision.get("response")
    return text if isinstance(text, str) and text.strip() else None


class RecipientEvidence(TurnData):
    recipient: Recipient = "ambient"
    source: Literal[
        "native_mention",
        "bot_reply",
        "human_reply",
        "human_address",
        "direct_address",
        "continuation",
        "unresolved",
    ] = "unresolved"
    invocation_root_event_id: str | None = None
    member_id: str | None = None

    @property
    def directed(self) -> bool:
        return self.source != "unresolved"


@lru_cache(maxsize=1)
def registered_business_operations() -> tuple[str, ...]:
    return tuple(sorted(ACTION_ADAPTER.json_schema()["discriminator"]["mapping"]))


class ActionRequirement(TurnData):
    kind: ActionKind
    operation: str | None = Field(default=None, max_length=80)

    @model_validator(mode="after")
    def registered_operation(self, info: ValidationInfo) -> ActionRequirement:
        if self.kind == "business":
            # Generic None survives only in trusted legacy stored contracts;
            # every newly requested model verdict must choose a registered op.
            if self.operation is None and not (info.context or {}).get("model_assessment"):
                return self
            if self.operation not in registered_business_operations():
                raise ValueError("business requires a registered apply_business_action op")
        elif self.operation is not None:
            raise ValueError("Named work/fact actions do not take a business operation")
        return self

    @classmethod
    def __get_pydantic_json_schema__(cls, core_schema: Any, handler: Any) -> dict[str, Any]:
        return {
            "title": cls.__name__,
            "oneOf": [
                {
                    "type": "object",
                    "additionalProperties": False,
                    "properties": {
                        "kind": {"const": "business"},
                        "operation": {
                            "type": "string",
                            "enum": list(registered_business_operations()),
                        },
                    },
                    "required": ["kind", "operation"],
                },
                {
                    "type": "object",
                    "additionalProperties": False,
                    "properties": {
                        "kind": {
                            "enum": [kind for kind in get_args(ActionKind) if kind != "business"]
                        },
                        "operation": {"type": "null", "default": None},
                    },
                    "required": ["kind"],
                },
            ],
        }


class TurnAssessment(TurnData):
    recipient: Recipient
    intent: Literal["conversation", "action", "observe"]
    required_actions: tuple[ActionRequirement, ...] = ()
    information_basis: Literal[
        "stable_knowledge", "provided_context", "public_verification", "user_input"
    ] = Field(
        default="stable_knowledge",
        description="Select how the answer can be grounded: stable concepts, sufficient supplied evidence, public facts requiring verification, or information only this user can supply.",
    )
    work_context_required: bool = Field(
        default=False,
        description="The current question explicitly concerns existing robot work or its metadata.",
    )

    @model_validator(mode="after")
    def action_has_evidence_requirement(self) -> TurnAssessment:
        if (self.intent == "action") != bool(self.required_actions):
            raise ValueError("Only an action turn has required host evidence")
        return self


class TurnContract(TurnAssessment):
    event_id: str
    evidence: RecipientEvidence
    # Decode earlier pending decisions and frozen delivery provenance.
    information_blocker: InformationBlocker | None = None
    reviewed_reply_required: StrictBool | None = None

    @property
    def visible(self) -> bool:
        return self.recipient == "agent" and (
            self.reviewed_reply_required
            if self.reviewed_reply_required is not None
            else self.intent != "observe"
        )

    @property
    def directness(self) -> Literal["direct", "open", "other", "none"]:
        if self.recipient == "other":
            return "other"
        if self.recipient == "ambient" or self.intent == "observe":
            return "none"
        return "direct"


def verified_human_addressee(text: str, context: dict[str, Any]) -> str | None:
    """A unique trusted human name is direction evidence, never authority."""
    outside = outside_quotes(text)
    identity = BotIdentity.from_context(context)
    if len(text) > 16000 or not outside or identity.directly_addressed(outside):
        return None
    requester = context.get("requester_member_id")
    if not isinstance(requester, str):
        return None
    names: dict[str, set[str]] = {}
    members = context.get("members", [])
    for member in members if isinstance(members, list) else ():
        if (
            not isinstance(member, dict)
            or not trusted_name_source(member.get("name_source"))
            or member.get("membership_state") not in {"active", "present", "unknown"}
        ):
            continue
        name, identifier = member.get("name"), member.get("member_id")
        if isinstance(name, str) and name and isinstance(identifier, str):
            names.setdefault(name, set()).add(identifier)
    excluded = {"我", "你", "他", "她", "它", "大家", "同学", "群昵称待补充"}
    excluded.update(identity.names)
    excluded = {name.casefold() for name in excluded}
    for name, identities in names.items():
        if len(identities) != 1 or requester in identities or name.casefold() in excluded:
            continue
        if re.match(
            r"^(?:(?:请|麻烦|拜托)\s*)?@?" + re.escape(name) + r"(?=[，,：:\s]|先|帮|来|你)",
            outside,
        ):
            return next(iter(identities))
    return None


def recipient_from_context(event: GroupEvent, context: dict[str, Any]) -> RecipientEvidence:
    saved = context.get("_recipient_evidence")
    if isinstance(saved, RecipientEvidence):
        return saved
    if isinstance(saved, dict):
        return RecipientEvidence.model_validate(saved)
    identity = BotIdentity.from_context(context)
    outside = outside_quotes(event.text)
    if identity.directly_addressed(outside):
        return RecipientEvidence(
            recipient="agent",
            source="direct_address",
            invocation_root_event_id=event.id,
        )
    # Only a host-projected speaker label is accepted; quoted text is not identity.
    recent = context.get("recent_messages", [])
    quoted = (
        next(
            (row for row in recent if isinstance(row, dict) and row.get("id") == event.reply_to),
            None,
        )
        if isinstance(recent, list) and event.reply_to
        else None
    )
    if quoted is not None:
        if quoted.get("speaker") == "assistant":
            return RecipientEvidence(
                recipient="agent", source="bot_reply", invocation_root_event_id=event.id
            )
        if quoted.get("speaker") == "human" and quoted.get("actor_id") not in {
            event.actor_id,
            context.get("requester_member_id"),
        }:
            return RecipientEvidence(
                recipient="other", source="human_reply", member_id=quoted.get("actor_id")
            )
    human = verified_human_addressee(event.text, context)
    if human:
        return RecipientEvidence(recipient="other", source="human_address", member_id=human)
    invocation = context.get("_invocation", {})
    if invocation.get("quoted_bot"):
        return RecipientEvidence(
            recipient="agent", source="bot_reply", invocation_root_event_id=event.id
        )
    if invocation.get("continuing_work"):
        return RecipientEvidence(recipient="agent", source="continuation")
    return RecipientEvidence()


def durable_effect_identity(receipt: dict[str, Any]) -> str | None:
    """Ignore proposals, acknowledgement text and observation times as progress."""
    if receipt.get("committed") is not True and receipt_phase(receipt) != "observed":
        return None
    kind, operation = receipt.get("kind"), receipt.get("operation")
    result = receipt.get("result")
    result = result if isinstance(result, dict) else receipt
    resources: list[dict[str, Any]] = []
    effects = receipt.get("work_effects", [])
    if isinstance(effects, list):
        for effect in effects:
            if isinstance(effect, dict) and effect.get("id"):
                resources.append(
                    {
                        key: effect[key]
                        for key in (
                            "id",
                            "goal_revision",
                            *(("state",) if kind == "control_work" else ()),
                        )
                        if key in effect
                    }
                )
    if not resources and result.get("work_id"):
        resources.append({"id": result["work_id"]})

    def business(value):
        if not isinstance(value, dict):
            return
        identifiers = {
            key: value[key]
            for key in (
                "task_id",
                "schedule_id",
                "fact_id",
                "member_id",
                "policy_subject",
                "revision",
            )
            if key in value
        }
        if identifiers:
            resources.append(identifiers)
        actions = value.get("actions", [])
        for action in actions if isinstance(actions, list) else ():
            business(action)

    if kind == "business":
        business(result.get("action_result", result))
    if kind == "read_work_facts":
        facts = receipt.get("facts", [])
        resources += (
            [
                {key: fact[key] for key in ("id", "goal_revision", "state") if key in fact}
                for fact in facts
                if isinstance(fact, dict)
            ]
            if isinstance(facts, list)
            else []
        )
    if kind == "read_group_context":
        resources.append({"resource": receipt.get("resource"), "facts": receipt.get("facts")})
    return json.dumps(
        {
            "kind": kind,
            "operation": operation,
            "resources": sorted({json.dumps(row, sort_keys=True) for row in resources}),
        },
        sort_keys=True,
    )


def durable_progress(receipts: list[dict[str, Any]]) -> tuple[str, ...]:
    return tuple(
        sorted(
            {
                identity
                for receipt in receipts
                if (identity := durable_effect_identity(receipt)) is not None
            }
        )
    )
