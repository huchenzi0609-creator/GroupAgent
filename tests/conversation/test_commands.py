from __future__ import annotations

import json
from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from groupagent.conversation.commands import is_task_declaration
from groupagent.conversation.provider import (
    _InvalidCandidate,
    decision_schema,
    reference_catalog,
    validate_candidate,
)
from groupagent.conversation.timeparse import normalize_time
from groupagent.coordinator import Decision

TASKS = [
    {"id": "own", "owner_id": "self", "revision": 2},
    {"id": "other", "owner_id": "another", "revision": 3},
]
CLAIM = {"op": "claim_task", "title": "Synthetic demonstration slides"}
UPDATE = {"op": "update_task", "task_id": "own", "expected_revision": 2, "progress": "Draft ready"}
ASSIGN = {
    "op": "assign_task",
    "title": "Synthetic notes",
    "member_id": "another",
    "member_name": "Peer",
}


@pytest.mark.parametrize("role", ["member", "owner"])
def test_task_declarations_depend_on_the_declarer_and_task_target_not_speech_recipient(role):
    assert is_task_declaration(CLAIM, role, "self", TASKS)
    assert is_task_declaration(UPDATE, role, "self", TASKS)
    assert not is_task_declaration({**UPDATE, "task_id": "other"}, role, "self", TASKS)
    assert is_task_declaration(ASSIGN, role, "self", TASKS) == (role == "owner")
    assert is_task_declaration(
        {"op": "task_batch", "actions": [CLAIM, UPDATE]}, role, "self", TASKS
    )
    assert not is_task_declaration(
        {"op": "task_batch", "actions": [CLAIM, {**UPDATE, "task_id": "other"}]},
        role,
        "self",
        TASKS,
    )


def test_declaration_predicate_does_not_admit_other_actions_malformed_data_or_readonly_roles():
    for action in ({"op": "quiet", "enabled": True}, {**CLAIM, "member_id": "another"}, None):
        assert not is_task_declaration(action, "owner", "self", TASKS)
    for role in ("observer", "maintainer"):
        assert not is_task_declaration(CLAIM, role, "self", TASKS)
    assert not is_task_declaration(CLAIM, "member", None, TASKS)


def validate(action, directness, **fields):
    stamp = datetime(2026, 11, 16, tzinfo=UTC)
    context = {
        "role": "member",
        "requester_member_id": "self",
        "tasks": TASKS,
        "model_event": {
            "id": "synthetic-current",
            "text": "I will prepare the slides for later this week",
        },
    }
    payload = {
        "kind": "command",
        "reason_code": "synthetic_declaration",
        "action": action,
        "directness": directness,
        "confidence": 0.95,
        **fields,
    }
    return validate_candidate(
        json.dumps(payload),
        context,
        reference_catalog(context, context["model_event"]),
        normalize_time(context["model_event"]["text"], stamp, "Asia/Shanghai"),
    )


@pytest.mark.parametrize("directness", ["other", "none"])
def test_self_declaration_survives_unaddressed_candidate_filter_without_generating_speech(
    directness,
):
    result = validate({**CLAIM, "due_note": "later this week"}, directness)
    assert result.kind == "command" and result.action["op"] == "claim_task"
    assert result.directness == directness and result.response is None
    ordinary = validate({"op": "quiet", "enabled": True}, directness)
    assert ordinary.kind == "observe" and ordinary.action is None


def test_live_command_has_action_and_backend_text_while_legacy_actions_stay_readable():
    with pytest.raises(ValidationError):
        Decision(kind="command", reason_code="synthetic")
    with pytest.raises(_InvalidCandidate) as error:
        validate(CLAIM, "direct", response="Unverified model confirmation")
    assert error.value.issues[0]["code"] == "command_result_is_rendered_by_backend"
    for kind in ("reply", "command"):
        value = Decision(
            kind=kind, reason_code="synthetic", response="Backend confirmation", action=CLAIM
        )
        assert Decision.model_validate_json(value.model_dump_json()) == value
    schema = decision_schema()
    conditions = {
        item["if"]["properties"]["kind"]["const"]: item["then"] for item in schema["allOf"]
    }
    assert "action" in conditions["command"]["required"]
    assert conditions["command"]["properties"]["response"] == {"type": "null"}
    assert conditions["reply"]["properties"]["action"] == {"type": "null"}
