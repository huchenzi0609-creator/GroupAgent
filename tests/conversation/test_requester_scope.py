from __future__ import annotations

from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from groupagent.control import WorkSelector
from groupagent.conversation.provider import decision_schema
from groupagent.conversation.references import resolve_work_selection
from groupagent.domain import ActorContext, DomainError, EventKind, GroupEvent, Role


def resolve(selector, role=Role.OWNER, requesters=("self", "another"), focus=("a", "b")):
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
            "state": "paused",
        }
        for identifier, requester in zip(("a", "b"), requesters, strict=True)
    ]
    return resolve_work_selection(actor, event, selector, candidates, focus_id=focus)


def test_selected_set_membership_does_not_imply_the_owner_initiated_every_task():
    selected = resolve(WorkSelector(basis="focus", quantifier="all", requester_scope="any"))
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
            WorkSelector(basis="query", quantifier="all", requester_scope="any"),
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
    assert "requester_scope" in schema["required"]
    assert "ownership" not in schema["properties"]
    assert schema["properties"]["requester_scope"]["enum"] == ["self", "any"]
    parsed = WorkSelector.model_validate_json(
        '{"basis":"ids","ids":["a","b"],"states":["paused"],"quantifier":"all",'
        '"requester_scope":"' + current + '","exclude_ids":["b"]}',
        strict=True,
    )
    assert parsed.requester_scope == current and parsed.states == ("paused",)
