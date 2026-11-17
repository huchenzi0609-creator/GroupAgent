"""Explicit scripted protocol bridge for old controller fixtures, never production."""

import json
from uuid import uuid4

from conversation.turn_model_fixture import semantic_fixture_reply

from groupagent.conversation.work_queries import render_work_query
from groupagent.coordinator import Decision
from groupagent.execution import ScriptedFakeModel
from groupagent.execution.models import ModelReply, ModelToolCall, TokenUsage

_USAGE = TokenUsage(input_tokens=1, output_tokens=1)


class ForegroundScriptedModel(ScriptedFakeModel):
    """Keep legacy protocol scripts explicit while answering new assessment calls."""

    def __init__(self, replies, *, work_context_required=False, **kwargs):
        replies = list(replies)
        super().__init__(replies, **kwargs)
        self.future_replies = list(replies)
        self.work_context_required = work_context_required
        self.current_turn_requirements = None

    async def complete(self, request):
        marker = request.messages[0].content or ""
        assessed = semantic_fixture_reply(
            request,
            upcoming=self.future_replies[:1],
            required_actions=self.current_turn_requirements
            if marker.startswith("TURN_OUTCOME_V1")
            else None,
            work_context_required=self.work_context_required,
        )
        if assessed is not None:
            if marker.startswith("TURN_CONTRACT_V1"):
                # This script can contain several distinct user turns. A
                # completion audit must not borrow the next turn's actions.
                self.current_turn_requirements = json.loads(assessed.text)["required_actions"]
            return assessed
        result = await super().complete(request)
        self.future_replies.pop(0)
        return result


def natural_reply(text):
    return ModelReply(text=text, usage=_USAGE)


def candidate_reply(decision: Decision):
    if decision.kind in {"reply", "clarify"} and decision.action is None:
        return natural_reply(decision.response)
    if decision.kind in {"observe", "ignore"}:
        name = "observe"
        arguments = {"reason_code": decision.reason_code, "directness": decision.directness}
    elif decision.kind == "query_work":
        name = "read_work_facts"
        arguments = {
            "selector": decision.work_selector.model_dump(mode="json"),
            "include_evidence": True,
        }
    else:
        candidate = decision.model_dump(mode="json", exclude_unset=True)
        if candidate.get("action") is not None:
            candidate["kind"] = "command"
            candidate.pop("response", None)
        kind = candidate["kind"]
        name = (
            kind
            if kind in {"start_work", "update_work", "control_work"}
            else "apply_business_action"
        )
        arguments = {
            key: value
            for key, value in candidate.items()
            if key not in {"kind", "reason_code", "directness", "confidence"}
        }
    return ModelReply(
        tool_calls=(ModelToolCall(id=uuid4().hex, name=name, arguments=arguments),),
        finish_reason="tool_calls",
        usage=_USAGE,
    )


def observation_reply(request):
    observations = [message for message in request.messages if message.role == "tool"]
    if not observations:
        return None
    observation = json.loads(observations[-1].content)
    # The old deterministic fixture chooses its final wording here. The service
    # must preserve it; this helper is never part of the production provider.
    if "facts" in observation:
        return natural_reply(render_work_query(observation))
    if observation.get("status") == "observed":
        return natural_reply("")
    return natural_reply(observation.get("host_message") or "合成业务工具回执已读取。")
