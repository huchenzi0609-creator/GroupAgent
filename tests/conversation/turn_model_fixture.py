"""Explicit offline assessment bridge for typed protocol tests, not intent evidence."""

import json

from groupagent.execution.models import ModelReply, TokenUsage


def semantic_fixture_reply(
    request,
    *,
    upcoming=(),
    recipient=None,
    intent=None,
    outcome=None,
    required_actions=None,
    work_context_required=False,
    information_basis="stable_knowledge",
):
    system = request.messages[0].content or ""
    if system.startswith("TURN_CONTRACT_V1"):
        payload = json.loads(request.messages[1].content)
        evidence = payload["trusted_recipient"]
        direction = recipient or (
            evidence["recipient"] if evidence["source"] != "unresolved" else "agent"
        )
        requirements = []
        for step in upcoming:
            if not isinstance(step, ModelReply):
                continue
            for call in step.tool_calls:
                if call.name in {"start_work", "update_work", "control_work", "read_work_facts"}:
                    requirement = {"kind": call.name}
                elif call.name == "apply_business_action":
                    requirement = {"kind": "business", "operation": call.arguments["action"]["op"]}
                else:
                    continue
                if requirement not in requirements:
                    requirements.append(requirement)
        if required_actions is not None:
            requirements = required_actions
        mode = intent or ("action" if requirements else "conversation")
        if direction == "other" and not requirements:
            mode = "observe"
        value = {
            "recipient": direction,
            "intent": mode,
            "required_actions": requirements,
            "work_context_required": work_context_required,
            "information_basis": information_basis,
        }
    elif system.startswith("TURN_OUTCOME_V1"):
        payload = json.loads(request.messages[1].content)
        accepted = any(receipt.get("committed") for receipt in payload["host_receipts"])
        requirements = list(required_actions or [])
        if required_actions is None:
            for step in upcoming:
                if not isinstance(step, ModelReply):
                    continue
                for call in step.tool_calls:
                    if call.name in {
                        "start_work",
                        "update_work",
                        "control_work",
                        "read_work_facts",
                    }:
                        item = {"kind": call.name}
                    elif call.name == "apply_business_action":
                        item = {"kind": "business", "operation": call.arguments["action"]["op"]}
                    else:
                        continue
                    if item not in requirements:
                        requirements.append(item)
            for receipt in payload["host_receipts"]:
                item = {"kind": receipt["kind"]}
                if receipt.get("operation") is not None:
                    item["operation"] = receipt["operation"]
                if item not in requirements:
                    requirements.append(item)
        value = {
            "outcome": outcome
            or ("committed" if accepted or not payload["host_receipts"] else "blocked"),
            "explanation": "Explicit synthetic host-receipt assessment.",
            "required_actions": requirements,
            "reply_required": payload["trusted_recipient"]["recipient"] == "agent",
        }
    else:
        return None
    return ModelReply(text=json.dumps(value), usage=TokenUsage(input_tokens=1, output_tokens=1))
