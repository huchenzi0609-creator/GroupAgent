from __future__ import annotations

from datetime import UTC, datetime

import pytest
from jsonschema import Draft202012Validator
from pydantic import ValidationError

from groupagent.control import WorkSelector
from groupagent.conversation.provider import decision_schema
from groupagent.conversation.references import resolve_work_selection
from groupagent.domain import ActorContext, DomainError, EventKind, GroupEvent, Role


def resolve(
    selector,
    role=Role.OWNER,
    requesters=("self", "another"),
    focus=("a", "b"),
    states=("paused", "paused"),
    readonly=False,
):
    actor = ActorContext(group_id="synthetic-group", member_id="self", role=role)
    stamp = datetime(2026, 11, 16, tzinfo=UTC)
    event = GroupEvent(
        id="synthetic-current",
        source_key="synthetic:scope",
        group_id=actor.group_id,
        actor_id=actor.member_id,
        kind=EventKind.TEXT,
        text="Synthetic current selection",
        occurred_at=stamp,
        received_at=stamp,
    )
    candidates = [
        {
            "id": identifier,
            "group_id": actor.group_id,
            "requester_id": requester,
            "goal": "Synthetic work",
            "revision": 2,
            "state": state,
        }
        for identifier, requester, state in zip(("a", "b"), requesters, states, strict=True)
    ]
    return resolve_work_selection(
        actor, event, selector, candidates, focus_id=focus, readonly=readonly
    )


def test_selected_set_membership_does_not_imply_the_owner_initiated_every_task():
    selected = resolve(WorkSelector(basis="focus", quantifier="all"))
    assert selected.ids == ("a", "b")
    assert resolve(WorkSelector(basis="focus", quantifier="all", requester_scope="self")).ids == (
        "a",
    )


@pytest.mark.parametrize("basis", ["focus", "ids"])
def test_member_can_control_explicit_own_set_but_never_another_requesters_target(basis):
    selector = WorkSelector(
        basis=basis,
        requester_scope="any",
        quantifier="all",
        ids=("a", "b") if basis == "ids" else (),
    )
    assert resolve(selector, role=Role.MEMBER, requesters=("self", "self")).ids == ("a", "b")
    with pytest.raises(DomainError) as error:
        resolve(selector, role=Role.MEMBER)
    assert error.value.code == "permission_denied"


def test_member_cannot_turn_an_explicit_set_permission_into_a_group_wide_query():
    with pytest.raises(DomainError) as error:
        resolve(
            WorkSelector(basis="query", quantifier="all"),
            role=Role.MEMBER,
            requesters=("self", "self"),
        )
    assert error.value.code == "permission_denied"


@pytest.mark.parametrize("legacy,current", [("mine", "self"), ("group", "any")])
def test_legacy_filter_is_readable_but_live_schema_only_exposes_requester_semantics(
    legacy, current
):
    selector = WorkSelector(basis="focus", ownership=legacy)
    assert selector.requester_scope == current and selector.ownership == legacy
    assert "ownership" not in selector.model_dump()
    with pytest.raises(ValidationError):
        WorkSelector(basis="focus", ownership=legacy, requester_scope=current)
    schema = decision_schema()["$defs"]["WorkSelector"]
    assert "requester_scope" not in schema["required"]
    assert "ownership" not in schema["properties"]
    assert schema["properties"]["requester_scope"]["enum"] == ["any", "self"]
    assert schema["properties"]["requester_scope"]["default"] == "any"
    parsed = WorkSelector.model_validate_json(
        '{"basis":"ids","ids":["a","b"],"states":["paused"],"quantifier":"all",'
        '"requester_scope":"' + current + '","exclude_ids":["b"]}',
        strict=True,
    )
    assert parsed.requester_scope == current and parsed.states == ("paused",)


@pytest.mark.parametrize("state_filter", [(), ("paused",)])
def test_public_query_without_requester_filter_keeps_every_matching_requester(state_filter):
    selector = WorkSelector(basis="query", quantifier="all", states=state_filter)
    assert selector.requester_scope == "any"
    assert resolve(selector, role=Role.MEMBER, readonly=True).ids == ("a", "b")
    assert resolve(
        WorkSelector(basis="query", quantifier="all"),
        role=Role.MEMBER,
        states=("validating", "paused"),
        readonly=True,
    ).ids == ("a", "b")


def test_optional_live_filter_defaults_to_any_and_stored_explicit_self_stays_narrow():
    value = {
        "kind": "query_work",
        "reason_code": "synthetic_default_filter",
        "directness": "direct",
        "confidence": 0.95,
        "work_selector": {"basis": "query", "states": [], "quantifier": "all"},
    }
    Draft202012Validator(decision_schema()).validate(value)
    unfiltered = WorkSelector.model_validate(value["work_selector"])
    assert unfiltered.requester_scope == "any"
    stored = '{"basis":"focus","states":[],"quantifier":"all","requester_scope":"self"}'
    explicit = WorkSelector.model_validate_json(stored, strict=True)
    assert explicit.requester_scope == "self"
    assert resolve(explicit).ids == ("a",)
    assert WorkSelector.model_validate_json(explicit.model_dump_json(), strict=True) == explicit
