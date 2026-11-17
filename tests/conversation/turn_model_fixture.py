"""Translate legacy synthetic response scripts into the foreground finish protocol."""

import json
from uuid import uuid4

from groupagent.conversation.text import outside_quotes
from groupagent.conversation.turns import receipt_phase
from groupagent.execution.models import ModelReply, ModelToolCall


def finish_fixture_reply(request, reply):
    if reply.finish_reason not in {"stop", "tool_calls"}:
        return reply
    if reply.tool_calls and all(call.name != "observe" for call in reply.tool_calls):
        return reply
    observe = bool(reply.tool_calls)
    reports = []
    payload = json.loads(request.messages[1].content)
    invitation = (
        outside_quotes(payload.get("event", {}).get("text", ""))[:160]
        if not observe
        and payload.get("context", {}).get("trusted_routing", {}).get("recipient") == "ambient"
        else ""
    )
    observations = list(payload.get("prior_receipts", []))
    observations.extend(
        json.loads(message.content)
        for message in request.messages
        if message.role == "tool" and message.content
    )
    for row in observations:
        identifier = row.get("tool_call_id")
        if identifier and (phase := receipt_phase(row)):
            reports.append({"tool_call_id": identifier, "phase": phase})
    return ModelReply(
        tool_calls=(
            ModelToolCall(
                id=uuid4().hex,
                name="finish_turn",
                arguments={
                    "disposition": "observe" if observe else "reply",
                    "open_request": bool(invitation),
                    "open_request_evidence": invitation or None,
                    "response": None if observe else reply.text,
                    "reports": [] if observe else reports,
                },
            ),
        ),
        finish_reason="tool_calls",
        usage=reply.usage,
    )
