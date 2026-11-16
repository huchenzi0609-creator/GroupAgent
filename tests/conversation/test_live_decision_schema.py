"""The model protocol is narrower than persisted legacy Decision records."""

import json
from datetime import UTC, datetime

import pytest
from jsonschema import Draft202012Validator
from pydantic import ValidationError

from groupagent.conversation.provider import (
    _InvalidCandidate,
    decision_schema,
    reference_catalog,
    validate_candidate,
)
from groupagent.conversation.timeparse import normalize_time
from groupagent.coordinator import Decision
from groupagent.scheduling.models import AtTiming

COMMON = {"reason_code": "synthetic_protocol", "directness": "direct", "confidence": 0.95}
SELECTOR = {"basis": "query", "states": ["paused"], "requester_scope": "any", "quantifier": "all"}
SAMPLES = [
    {"kind": "ignore"},
    {"kind": "observe"},
    {"kind": "reply", "response": "Synthetic explanation"},
    {"kind": "clarify", "response": "Which synthetic input is intended?"},
    {"kind": "command", "action": {"op": "quiet", "enabled": True}},
    {
        "kind": "start_work",
        "goal": "Research a synthetic topic",
        "work_capabilities": ["public_research"],
        "outputs": [{"format": "text"}],
    },
    {
        "kind": "update_work",
        "goal": "Revise the synthetic report",
        "target_work_id": "synthetic-work",
        "expected_revision": 3,
        "work_capabilities": ["documents"],
    },
    {"kind": "control_work", "control": "cancel", "work_selector": SELECTOR},
    {"kind": "query_work", "work_selector": SELECTOR},
]


def context():
    return {
        "role": "owner",
        "requester_member_id": "self",
        "works": [
            {"id": "synthetic-work", "revision": 3, "requester_id": "self", "state": "paused"}
        ],
        "model_event": {
            "id": "synthetic-current",
            "text": "Synthetic current request",
            "attachments": [],
        },
    }


def validate(value):
    ctx = context()
    return validate_candidate(
        json.dumps(value),
        ctx,
        reference_catalog(ctx, ctx["model_event"]),
        normalize_time(
            ctx["model_event"]["text"], datetime(2030, 1, 1, tzinfo=UTC), "Asia/Shanghai"
        ),
    )


@pytest.mark.parametrize("sample", SAMPLES, ids=lambda item: item["kind"])
def test_every_live_branch_is_a_complete_valid_object_and_preserves_backend_validation(sample):
    schema = decision_schema(reference_catalog(context(), context()["model_event"]))
    Draft202012Validator.check_schema(schema)
    value = {**COMMON, **sample}
    Draft202012Validator(schema).validate(value)
    assert validate(value).kind == sample["kind"]
    assert "allOf" not in schema and "properties" not in schema
    assert len(schema["oneOf"]) == len(SAMPLES)
    assert all(item["additionalProperties"] is False for item in schema["oneOf"])


def test_real_structural_failures_remain_invalid_in_live_schema_and_backend():
    validator = Draft202012Validator(decision_schema())
    missing_goal = {**COMMON, "kind": "start_work", "goal": None, "work_capabilities": []}
    reply_with_work_output = {
        **COMMON,
        "kind": "reply",
        "response": "Synthetic answer",
        "outputs": [{"format": "text"}],
    }
    assert not validator.is_valid(missing_goal)
    assert not validator.is_valid(reply_with_work_output)
    with pytest.raises(ValidationError) as error:
        validate(missing_goal)
    assert error.value.errors()[0]["type"] == "decision_goal_required"
    with pytest.raises(_InvalidCandidate) as error:
        validate(reply_with_work_output)
    assert error.value.issues == [
        {"path": "kind", "code": "work_fields_require_start_or_update_work"}
    ]


@pytest.mark.parametrize("goal", [None, "", "   "])
@pytest.mark.parametrize("kind", ["start_work", "update_work"])
def test_work_goal_is_required_nonblank_text_in_its_own_branch(kind, goal):
    value = {**COMMON, **next(item for item in SAMPLES if item["kind"] == kind), "goal": goal}
    validator = Draft202012Validator(decision_schema())
    assert not validator.is_valid(value)
    value.pop("goal")
    assert not validator.is_valid(value)


def test_related_fields_are_explicit_and_irrelevant_null_templates_are_rejected():
    validator = Draft202012Validator(decision_schema())
    for sample, extra in (
        (SAMPLES[2], {"action": None}),
        (SAMPLES[4], {"response": None}),
        (SAMPLES[-1], {"goal": None}),
        (SAMPLES[-2], {"target_work_id": None}),
    ):
        assert not validator.is_valid({**COMMON, **sample, **extra})
    legacy = Decision(
        kind="reply",
        reason_code="synthetic_legacy",
        response="Backend receipt",
        action={"op": "claim_task", "title": "Synthetic task"},
    )
    assert Decision.model_validate_json(legacy.model_dump_json()) == legacy


def test_dynamic_input_scopes_are_retained_inside_work_branches():
    value = {
        **COMMON,
        **SAMPLES[5],
        "input_event_ids": ["attachment-a"],
        "input_versions": ["version-a"],
    }
    validator = Draft202012Validator(
        decision_schema({"attachment_event_ids": ["attachment-a"], "file_versions": ["version-a"]})
    )
    validator.validate(value)
    assert not validator.is_valid({**value, "input_event_ids": ["plain-instruction-event"]})
    assert not validator.is_valid({**value, "input_versions": ["unknown-version"]})
    assert not Draft202012Validator(decision_schema({})).is_valid(value)


@pytest.mark.parametrize(
    "timing",
    [
        {"kind": "at", "at": "2030-01-01T09:00:00+08:00"},
        {"kind": "daily", "local_time": "09:00"},
        {"kind": "weekly", "local_time": "09:00", "weekdays": [0]},
        {"kind": "every", "interval_seconds": 3600},
    ],
)
def test_live_timing_requires_discriminator_but_internal_defaults_remain(timing):
    value = {
        **COMMON,
        "kind": "command",
        "action": {
            "op": "create_schedule",
            "title": "Synthetic schedule",
            "goal": "Synthetic scheduled work",
            "timing": timing,
        },
    }
    validator = Draft202012Validator(decision_schema())
    validator.validate(value)
    value["action"]["timing"] = {key: item for key, item in timing.items() if key != "kind"}
    assert not validator.is_valid(value)
    assert AtTiming(at=datetime(2030, 1, 1, tzinfo=UTC)).kind == "at"
