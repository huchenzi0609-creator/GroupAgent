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
        }
    elif system.startswith("TURN_OUTCOME_V1"):
        payload = json.loads(request.messages[1].content)
        accepted = any(receipt.get("committed") for receipt in payload["host_receipts"])
        value = {
            "outcome": outcome or ("committed" if accepted else "blocked"),
            "explanation": "Explicit synthetic host-receipt assessment.",
        }
    else:
        return None
    return ModelReply(text=json.dumps(value), usage=TokenUsage(input_tokens=1, output_tokens=1))
