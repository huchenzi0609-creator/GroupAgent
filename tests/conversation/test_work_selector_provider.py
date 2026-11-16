from __future__ import annotations

import json
from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from groupagent.conversation.provider import (
    _InvalidCandidate,
    decision_schema,
    reference_catalog,
    validate_candidate,
)
from groupagent.conversation.timeparse import normalize_time
from groupagent.coordinator import Decision
from groupagent.domain import EventKind, GroupEvent, WorkState


def candidate(**values):
    return {
        "kind": "control_work",
        "reason_code": "synthetic_selector_contract",
        "control": "cancel",
        "directness": "direct",
        "confidence": 0.95,
        **values,
    }


def validate(value, **context_values):
    stamp = datetime(2026, 11, 16, tzinfo=UTC)
    event = GroupEvent(
        id="synthetic-current",
        source_key="synthetic:selector",
        group_id="synthetic-group",
        actor_id="self",
        kind=EventKind.TEXT,
        text="Synthetic current control request",
        occurred_at=stamp,
        received_at=stamp,
    )
    context = {
        "role": "owner",
        "requester_member_id": "self",
        "works": [
            {"id": "visible-paused", "requester_id": "self", "revision": 2, "state": "paused"}
        ],
        "model_event": event.model_dump(mode="json"),
        **context_values,
    }
    return validate_candidate(
        json.dumps(value),
        context,
        reference_catalog(context, context["model_event"]),
        normalize_time(event.text, event.occurred_at, "Asia/Shanghai"),
    )


def test_plural_paused_selector_is_not_narrowed_to_a_truncated_prompt_work_list():
    result = validate(
        candidate(
            work_selector={
                "basis": "query",
                "states": ["paused"],
                "quantifier": "all",
                "ownership": "group",
            }
        ),
        context_incomplete=True,
    )
    assert result.target_work_id is None and result.expected_revision is None
    assert result.work_selector is not None
    assert result.work_selector.states == (WorkState.PAUSED,)
    assert result.work_selector.quantifier == "all" and result.work_selector.ownership == "group"


def test_provider_does_not_treat_one_old_visible_work_as_conversation_focus():
    with pytest.raises(_InvalidCandidate) as error:
        validate(candidate(work_selector={"basis": "focus"}))
    assert error.value.issues[0]["code"] == "current_work_focus_required"


def test_provider_preserves_bound_quote_without_converting_it_to_a_guessed_exact_id():
    result = validate(candidate(work_selector={"basis": "quote"}), quoted_work_id="visible-paused")
    assert result.work_selector is not None and result.work_selector.basis == "quote"
    assert result.target_work_id is None


@pytest.mark.parametrize("key", ["focused_work_ids", "_focus_work_ids"])
@pytest.mark.parametrize("kind", ["control_work", "query_work"])
def test_provider_accepts_trusted_plural_focus_without_replacing_it_with_visible_old_work(
    key, kind
):
    result = validate(
        candidate(
            kind=kind,
            control="cancel" if kind == "control_work" else None,
            work_selector={"basis": "focus", "quantifier": "all"},
        ),
        **{key: ["selected-a", "selected-b"]},
    )
    assert result.work_selector is not None and result.work_selector.basis == "focus"
    assert result.work_selector.quantifier == "all" and result.target_work_id is None


def test_empty_plural_focus_does_not_make_an_old_visible_singleton_current():
    with pytest.raises(_InvalidCandidate) as error:
        validate(candidate(work_selector={"basis": "focus"}), focused_work_ids=[])
    assert error.value.issues[0]["code"] == "current_work_focus_required"


def test_explicit_ids_may_use_host_visible_focus_pointers_without_loading_their_contents():
    result = validate(
        candidate(
            work_selector={
                "basis": "ids",
                "ids": ["selected-a", "selected-b"],
                "quantifier": "all",
            }
        ),
        focused_work_ids=["selected-a", "selected-b"],
    )
    assert result.work_selector is not None
    assert result.work_selector.ids == ("selected-a", "selected-b")


def test_member_group_selection_and_incompatible_resume_state_are_rejected():
    for value, context, code in (
        (
            candidate(work_selector={"basis": "query", "quantifier": "all", "ownership": "group"}),
            {"role": "member"},
            "group_selection_requires_owner",
        ),
        (
            candidate(control="resume", work_selector={"basis": "query", "states": ["running"]}),
            {},
            "control_does_not_match_work_state",
        ),
    ):
        with pytest.raises(_InvalidCandidate) as error:
            validate(value, **context)
        assert error.value.issues[0]["code"] == code


def test_live_schema_requires_selector_while_internal_legacy_decision_remains_readable():
    legacy = Decision.model_validate(candidate(target_work_id="legacy", expected_revision=2))
    assert legacy.work_selector is None
    schema = decision_schema()
    assert "WorkSelector" in schema["$defs"]
    condition = next(
        item["then"]
        for item in schema["allOf"]
        if item["if"]["properties"]["kind"]["const"] == "control_work"
    )
    assert "work_selector" in condition["required"]
    assert "target_work_id" not in condition["required"]
    assert condition["properties"]["target_work_id"] == {"type": "null"}


def test_selector_and_legacy_exact_target_cannot_disagree():
    with pytest.raises(ValidationError):
        Decision.model_validate(
            candidate(
                work_selector={"basis": "focus"},
                target_work_id="unrelated-old-work",
                expected_revision=2,
            )
        )
    with pytest.raises(ValidationError):
        Decision.model_validate(
            {
                "kind": "reply",
                "reason_code": "synthetic",
                "response": "Synthetic reply",
                "work_selector": {"basis": "focus"},
            }
        )


@pytest.mark.parametrize("role", ["member", "observer", "maintainer"])
def test_query_work_accepts_public_group_selector_without_a_write_grant(role):
    result = validate(
        {
            "kind": "query_work",
            "reason_code": "synthetic_status_query",
            "work_selector": {
                "basis": "query",
                "ownership": "group",
                "quantifier": "all",
                "states": ["paused"],
            },
            "directness": "direct",
            "confidence": 0.7,
        },
        role=role,
        read_only=True,
    )
    assert result.kind == "query_work" and result.response is None
    assert result.work_selector is not None and result.work_selector.ownership == "group"
    assert result.control is None and result.action is None and result.goal is None


def test_query_schema_and_candidate_require_backend_state_instead_of_model_status_prose():
    value = {
        "kind": "query_work",
        "reason_code": "synthetic_status_query",
        "work_selector": {"basis": "query", "quantifier": "all"},
        "directness": "direct",
    }
    with pytest.raises(_InvalidCandidate) as error:
        validate({**value, "response": "Synthetic unsupported running claim"})
    assert error.value.issues[0]["code"] == "work_query_is_rendered_by_backend"
    rendered = Decision.model_validate({**value, "response": "Backend-rendered current state"})
    assert Decision.model_validate_json(rendered.model_dump_json()) == rendered
    schema = decision_schema()
    condition = next(
        item["then"]
        for item in schema["allOf"]
        if item["if"]["properties"]["kind"]["const"] == "query_work"
    )
    assert "work_selector" in condition["required"]
    assert "expected_revision" not in condition["required"]
    for name in ("response", "goal", "action", "control", "target_work_id"):
        assert condition["properties"][name] == {"type": "null"}


def test_legacy_query_exact_reference_is_internal_read_compatibility():
    result = validate(
        {
            "kind": "query_work",
            "reason_code": "synthetic_legacy_status_query",
            "target_work_id": "visible-paused",
            "expected_revision": 2,
            "directness": "direct",
        },
        role="observer",
        read_only=True,
        works=[
            {
                "id": "visible-paused",
                "requester_id": "another",
                "revision": 2,
                "state": "paused",
            }
        ],
    )
    assert result.kind == "query_work" and result.target_work_id == "visible-paused"


@pytest.mark.parametrize(
    "fields",
    [{}, {"goal": "Start a different work"}, {"control": "cancel"}, {"action": {"op": "forget"}}],
)
def test_work_query_cannot_smuggle_mutation_fields_or_omit_its_target(fields):
    values = {
        "kind": "query_work",
        "reason_code": "synthetic_status_query",
        **({"work_selector": {"basis": "focus"}} if fields else {}),
        **fields,
    }
    with pytest.raises(ValidationError):
        Decision.model_validate(values)
