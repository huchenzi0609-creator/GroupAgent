from __future__ import annotations

from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from groupagent.control import WorkSelector
from groupagent.conversation.references import resolve_work_selection
from groupagent.domain import ActorContext, DomainError, EventKind, GroupEvent, Role, WorkState


def actor(role: Role = Role.OWNER, member_id: str = "requester") -> ActorContext:
    return ActorContext(group_id="synthetic-group", member_id=member_id, role=role)


def event(who: ActorContext, **values) -> GroupEvent:
    stamp = datetime(2026, 11, 16, tzinfo=UTC)
    return GroupEvent(
        id="synthetic-current-event",
        source_key="synthetic:current-event",
        group_id=who.group_id,
        actor_id=who.member_id,
        kind=EventKind.TEXT,
        text="Synthetic current control request",
        occurred_at=stamp,
        received_at=stamp,
        **values,
    )


def work(identifier: str, *, state="paused", requester="requester", **values) -> dict:
    return {
        "id": identifier,
        "group_id": "synthetic-group",
        "requester_id": requester,
        "revision": 4,
        "state": state,
        "goal": "Synthetic report " + identifier,
        **values,
    }


def test_plural_state_selection_cannot_collapse_to_one_work_or_expand_to_running():
    who = actor()
    candidates = [work("paused-a"), work("running", state="running"), work("paused-b")]
    selector = WorkSelector(
        basis="query", states=(WorkState.PAUSED,), ownership="group", quantifier="all"
    )
    selected = resolve_work_selection(who, event(who), selector, candidates)
    assert selected.ids == ("paused-a", "paused-b")
    assert all(ref.revision == 4 and ref.state == WorkState.PAUSED for ref in selected.refs)
    assert selected.source_event_id == "synthetic-current-event"
    assert candidates[1]["state"] == "running"

    candidates[0]["state"] = "running"
    assert selected.refs[0].state == WorkState.PAUSED
    assert resolve_work_selection(who, event(who), selector, candidates).ids == ("paused-b",)
    with pytest.raises(ValidationError):
        selected.refs[0].revision = 5


@pytest.mark.parametrize("basis", ["focus", "quote"])
def test_missing_reference_never_falls_back_to_a_globally_unique_old_work(basis):
    who = actor()
    with pytest.raises(DomainError) as error:
        resolve_work_selection(who, event(who), WorkSelector(basis=basis), [work("old-paused")])
    assert error.value.code == "ambiguous_reference"


def test_quote_selects_its_own_work_and_never_a_different_focus():
    who = actor()
    selected = resolve_work_selection(
        who,
        event(who, reply_to="synthetic-delivery"),
        WorkSelector(basis="quote"),
        [work("quoted"), work("focus", state="running")],
        focus_id="focus",
        quoted_id="quoted",
    )
    assert selected.ids == ("quoted",)


def test_plural_focus_stays_bound_to_the_prior_selected_set_and_all_restrictions():
    who = actor()
    candidates = [work("selected-a"), work("selected-b"), work("unrelated-old-paused")]
    prior = resolve_work_selection(
        who,
        event(who),
        WorkSelector(basis="ids", ids=("selected-a", "selected-b"), quantifier="all"),
        candidates,
    )
    selected = resolve_work_selection(
        who,
        event(who),
        WorkSelector(
            basis="focus",
            quantifier="all",
            states=(WorkState.PAUSED,),
            exclude_ids=("selected-b",),
        ),
        candidates,
        focus_id=prior.ids,
    )
    assert selected.ids == ("selected-a",)
    candidates[0]["state"] = "running"
    with pytest.raises(DomainError) as error:
        resolve_work_selection(who, event(who), selected.selector, candidates, focus_id=prior.ids)
    assert error.value.code == "not_found"


@pytest.mark.parametrize(
    "focus, expected",
    [
        ((), "ambiguous_reference"),
        (("a", "b"), "ambiguous_reference"),
        (("a", "missing"), "not_found"),
    ],
)
def test_plural_focus_does_not_guess_one_or_silently_drop_missing_targets(focus, expected):
    who = actor()
    with pytest.raises(DomainError) as error:
        resolve_work_selection(
            who, event(who), WorkSelector(basis="focus"), [work("a"), work("b")], focus_id=focus
        )
    assert error.value.code == expected


def test_plural_focus_cannot_inherit_another_members_control_authority():
    who = actor(Role.MEMBER)
    with pytest.raises(DomainError) as error:
        resolve_work_selection(
            who,
            event(who),
            WorkSelector(basis="focus", quantifier="all", requester_scope="any"),
            [work("mine"), work("other", requester="another")],
            focus_id=("mine", "other"),
        )
    assert error.value.code == "permission_denied"


def test_all_predicates_and_exclusions_intersect_instead_of_overwriting_each_other():
    who = actor()
    candidates = [
        work("keep", goal="Climate REPORT"),
        work("exclude", goal="Climate report"),
        work("other-owner", requester="another", goal="Climate report"),
        work("other-title", goal="Recruitment poster"),
        work("running", state="running", goal="Climate report"),
    ]
    selected = resolve_work_selection(
        who,
        event(who),
        WorkSelector(
            basis="query",
            states=(WorkState.PAUSED,),
            quantifier="all",
            ownership="mine",
            title_query="climate report",
            exclude_ids=("exclude",),
        ),
        candidates,
    )
    assert selected.ids == ("keep",)


def test_singular_state_query_reports_ambiguity_instead_of_picking_latest():
    who = actor()
    with pytest.raises(DomainError) as error:
        resolve_work_selection(
            who,
            event(who),
            WorkSelector(basis="query", states=(WorkState.PAUSED,)),
            [work("a"), work("b")],
        )
    assert error.value.code == "ambiguous_reference"


@pytest.mark.parametrize("basis", ["focus", "quote", "ids", "query"])
def test_state_restriction_also_applies_to_explicit_references(basis):
    who = actor()
    selector = WorkSelector(
        basis=basis,
        states=(WorkState.PAUSED,),
        ids=("running",) if basis == "ids" else (),
    )
    with pytest.raises(DomainError) as error:
        resolve_work_selection(
            who,
            event(who),
            selector,
            [work("running", state="running")],
            focus_id="running",
            quoted_id="running",
        )
    assert error.value.code == "not_found"


def test_member_cannot_widen_to_group_or_mix_another_members_exact_work():
    who = actor(Role.MEMBER)
    candidates = [work("mine"), work("other", requester="another")]
    for selector in (
        WorkSelector(basis="query", ownership="group", quantifier="all"),
        WorkSelector(basis="ids", ids=("mine", "other"), quantifier="all", requester_scope="any"),
    ):
        with pytest.raises(DomainError) as error:
            resolve_work_selection(who, event(who), selector, candidates)
        assert error.value.code == "permission_denied"
    assert resolve_work_selection(
        who,
        event(who),
        WorkSelector(basis="query", quantifier="all", requester_scope="self"),
        candidates,
    ).ids == ("mine",)


def test_truncated_and_partly_missing_sets_never_produce_partial_success():
    who = actor()
    for selector, complete, code in (
        (WorkSelector(basis="query", quantifier="all"), False, "selection_incomplete"),
        (
            WorkSelector(basis="ids", ids=("known", "missing"), quantifier="all"),
            True,
            "not_found",
        ),
    ):
        with pytest.raises(DomainError) as error:
            resolve_work_selection(
                who, event(who), selector, [work("known")], candidates_complete=complete
            )
        assert error.value.code == code


@pytest.mark.parametrize("role", [Role.OBSERVER, Role.MAINTAINER])
def test_read_only_roles_never_resolve_a_control_grant(role):
    who = actor(role)
    with pytest.raises(DomainError) as error:
        resolve_work_selection(
            who, event(who), WorkSelector(basis="ids", ids=("known",)), [work("known")]
        )
    assert error.value.code == "permission_denied"


@pytest.mark.parametrize("role", [Role.MEMBER, Role.OBSERVER, Role.MAINTAINER])
def test_public_group_queries_do_not_require_mutation_authority_or_role_forgery(role):
    who = actor(role)
    selector = WorkSelector(basis="query", ownership="group", quantifier="all")
    selected = resolve_work_selection(
        who,
        event(who),
        selector,
        [work("mine"), work("another-members", requester="another")],
        readonly=True,
    )
    assert selected.ids == ("another-members", "mine")
    assert who.role == role
    with pytest.raises(DomainError) as error:
        resolve_work_selection(who, event(who), selector, [work("mine")])
    assert error.value.code == "permission_denied"


def test_readonly_does_not_expand_mine_or_bypass_group_source_and_completeness():
    who = actor(Role.OBSERVER)
    selector = WorkSelector(basis="query", quantifier="all", requester_scope="self")
    assert resolve_work_selection(
        who,
        event(who),
        selector,
        [work("mine"), work("other", requester="another")],
        readonly=True,
    ).ids == ("mine",)
    with pytest.raises(DomainError) as error:
        resolve_work_selection(
            who,
            event(who),
            selector,
            [work("foreign", group_id="another-group")],
            readonly=True,
        )
    assert error.value.code == "scope_violation"
    with pytest.raises(DomainError) as error:
        resolve_work_selection(
            who, event(who), selector, [work("mine")], readonly=True, candidates_complete=False
        )
    assert error.value.code == "selection_incomplete"


def test_foreign_actor_group_and_forwarded_source_cannot_authorize_selection():
    who = actor()
    current = event(who)
    for changed in (
        current.model_copy(update={"actor_id": "another"}),
        current.model_copy(update={"group_id": "another-group"}),
        current.model_copy(update={"kind": EventKind.FORWARDED}),
        current.model_copy(update={"content_state": "forgotten"}),
    ):
        with pytest.raises(DomainError):
            resolve_work_selection(
                who, changed, WorkSelector(basis="ids", ids=("known",)), [work("known")]
            )
    with pytest.raises(DomainError) as error:
        resolve_work_selection(
            who,
            current,
            WorkSelector(basis="ids", ids=("foreign",)),
            [work("foreign", group_id="another-group")],
        )
    assert error.value.code == "scope_violation"


@pytest.mark.parametrize(
    "values",
    [
        {"basis": "query"},
        {"basis": "ids"},
        {"basis": "ids", "ids": ["a", "b"]},
        {"basis": "focus", "ids": ["a"]},
        {"basis": "query", "title_query": "   "},
    ],
)
def test_incoherent_selector_cannot_reach_resolution(values):
    with pytest.raises(ValidationError):
        WorkSelector.model_validate(values)
