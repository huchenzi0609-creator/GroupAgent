from __future__ import annotations

import json
from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from groupagent.conversation.provider import decision_schema, reference_catalog, validate_candidate
from groupagent.conversation.timeparse import normalize_time
from groupagent.coordinator import Decision


def work(kind="start_work", **values):
    return {
        "kind": kind,
        "reason_code": "synthetic_capability_plan",
        "goal": "Retrieve a few public images",
        "directness": "direct",
        "confidence": 0.95,
        **(
            {"target_work_id": "synthetic-work", "expected_revision": 2}
            if kind == "update_work"
            else {}
        ),
        **values,
    }


@pytest.mark.parametrize("kind", ["start_work", "update_work"])
def test_work_plan_preserves_capability_families_and_chosen_public_image_quantity(kind):
    plan = Decision.model_validate(
        work(kind, work_capabilities=["public_images", "documents"], public_image_count=3)
    )
    assert plan.work_capabilities == ["public_images", "documents"]
    assert plan.public_image_count == 3 and plan.outputs == []
    legacy = Decision.model_validate(work(kind))
    assert legacy.work_capabilities == [] and legacy.public_image_count is None


@pytest.mark.parametrize("count", [0, -1, 11, True, 1.5, "3"])
def test_public_image_quantity_is_a_bounded_integer(count):
    with pytest.raises(ValidationError):
        Decision.model_validate(work(work_capabilities=["public_images"], public_image_count=count))


@pytest.mark.parametrize("capabilities", [["process_run"], ["all_tools"], ["compute"] * 9])
def test_work_plan_cannot_select_unknown_families_or_exceed_its_bound(capabilities):
    with pytest.raises(ValidationError):
        Decision.model_validate(work(work_capabilities=capabilities))


@pytest.mark.parametrize(
    "kind", ["ignore", "observe", "reply", "clarify", "control_work", "query_work"]
)
@pytest.mark.parametrize("fields", [{"work_capabilities": ["compute"]}, {"public_image_count": 2}])
def test_other_decision_kinds_cannot_carry_a_work_capability_plan(kind, fields):
    values = {
        "kind": kind,
        "reason_code": "synthetic_non_work",
        **({"response": "Synthetic reply"} if kind in {"reply", "clarify"} else {}),
        **({"work_selector": {"basis": "focus"}} if kind in {"control_work", "query_work"} else {}),
        **({"control": "pause"} if kind == "control_work" else {}),
        **fields,
    }
    with pytest.raises(ValidationError):
        Decision.model_validate(values)


def test_live_schema_requests_work_plan_without_changing_selector_or_output_contracts():
    schema = decision_schema()
    for then in schema["oneOf"]:
        kind = then["properties"]["kind"]["const"]
        if kind in {"start_work", "update_work"}:
            assert "work_capabilities" in then["required"]
        else:
            assert "work_capabilities" not in then["properties"]
            assert "public_image_count" not in then["properties"]
    assert schema["$defs"]["WorkSelector"]["required"] == [
        "basis",
        "states",
        "quantifier",
    ]
    work_branch = next(
        item for item in schema["oneOf"] if item["properties"]["kind"]["const"] == "start_work"
    )
    assert work_branch["properties"]["outputs"]["items"]["properties"]["format"]["enum"] == [
        "text",
        "md",
        "docx",
        "pdf",
        "pptx",
        "png",
        "jpeg",
        "wav",
        "ogg",
    ]


def test_candidate_plan_does_not_enable_capabilities_or_grant_tools():
    stamp = datetime(2026, 11, 16, tzinfo=UTC)
    context = {
        "role": "member",
        "requester_member_id": "synthetic-requester",
        "capabilities": {"web_research": False, "public_image_retrieval": False},
        "works": [],
        "model_event": {"id": "synthetic-current", "text": "Retrieve a few public images"},
    }
    result = validate_candidate(
        json.dumps(work(work_capabilities=["public_images"], public_image_count=3)),
        context,
        reference_catalog(context, context["model_event"]),
        normalize_time(context["model_event"]["text"], stamp, "Asia/Shanghai"),
    )
    assert result.work_capabilities == ["public_images"] and result.public_image_count == 3
    assert context["capabilities"] == {"web_research": False, "public_image_retrieval": False}
    assert result.action is None and result.input_versions == []
