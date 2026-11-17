"""Pure work selection over an explicitly complete, authorized metadata catalog.

The caller loads canonical candidates and rechecks the resulting states and
revisions while applying a command. This resolver performs no reads or writes;
an old globally unique work is never a substitute for conversational focus.
"""

from __future__ import annotations

import re
import unicodedata
from collections.abc import Iterable, Mapping
from typing import Any

from pydantic import ValidationError

from groupagent.control import ResolvedWorkSet, WorkRef, WorkSelector
from groupagent.domain import ActorContext, DomainError, EventKind, GroupEvent, Role, WorkOrder


def _title(value: str) -> str:
    return " ".join(unicodedata.normalize("NFKC", value).casefold().split())


def authored_identifier(text: str, identifier: str) -> bool:
    """Recognize an exact opaque identifier supplied in the current message."""
    edge = r"[a-zA-Z0-9_@.:-]"
    return bool(identifier and re.search(rf"(?<!{edge}){re.escape(identifier)}(?!{edge})", text))


def resolve_work_selection(
    actor: ActorContext,
    event: GroupEvent,
    selector: WorkSelector,
    candidates: Iterable[WorkOrder | Mapping[str, Any]],
    focus_id: str | tuple[str, ...] | None = None,
    quoted_id: str | None = None,
    *,
    candidates_complete: bool = True,
    readonly: bool = False,
) -> ResolvedWorkSet:
    """Preserve the requested set, ownership, states, title and exclusions.

    ``focus_id`` (one ID or a previously selected set) and ``quoted_id`` come
    from trusted conversation/receipt associations, never from the mere
    presence of remaining candidates.
    ``candidates_complete`` must describe the requested catalog, not the much
    smaller context shown to a model. A truncated list cannot prove "all" or
    disambiguate "one". ``readonly`` accepts public metadata queries by readers;
    the caller still checks its configured query roles and public visibility.
    It never changes the actor's role or authorizes a mutation.
    """
    if event.group_id != actor.group_id or event.actor_id != actor.member_id:
        raise DomainError("scope_violation", "Work control must match the current actor and group")
    if not readonly and actor.role not in {Role.OWNER, Role.MEMBER}:
        raise DomainError("permission_denied", "The current role cannot control work")
    if event.content_state != "active":
        raise DomainError("source_unavailable", "Removed source cannot authorize work control")
    if event.kind in {
        EventKind.FORWARDED,
        EventKind.SYSTEM,
        EventKind.MEMBERSHIP,
        EventKind.RECALL,
        EventKind.UNSUPPORTED_VIDEO,
    }:
        raise DomainError("permission_denied", "Reference data cannot authorize work control")
    if (
        not readonly
        and selector.requester_scope == "any"
        and selector.basis == "query"
        and actor.role != Role.OWNER
    ):
        raise DomainError("permission_denied", "Group-wide selection requires the group owner")
    if not candidates_complete:
        raise DomainError("selection_incomplete", "A truncated catalog cannot resolve work control")

    rows: dict[str, tuple[WorkRef, str, str]] = {}
    for candidate in candidates:
        row = candidate.model_dump() if isinstance(candidate, WorkOrder) else candidate
        if row.get("group_id") != actor.group_id:
            raise DomainError("scope_violation", "Work metadata belongs to another group")
        requester = row.get("requester_id")
        goal = row.get("goal", "")
        if not isinstance(requester, str) or not requester or not isinstance(goal, str):
            raise DomainError("invalid_input", "Work metadata is incomplete")
        try:
            ref = WorkRef.model_validate({key: row.get(key) for key in ("id", "revision", "state")})
        except ValidationError:
            raise DomainError(
                "invalid_input", "Work metadata has an invalid identity or state"
            ) from None
        if ref.id in rows:
            raise DomainError("ambiguous_reference", "Work metadata repeats an identity")
        rows[ref.id] = (ref, requester, goal)

    if selector.basis == "focus":
        identifiers = (focus_id,) if isinstance(focus_id, str) else focus_id
        if not identifiers:
            raise DomainError(
                "ambiguous_reference", "No current work reference identifies the target"
            )
        selected_ids = set(identifiers)
    elif selector.basis == "quote":
        if quoted_id is None:
            raise DomainError(
                "ambiguous_reference", "No quoted work reference identifies the target"
            )
        selected_ids = {quoted_id}
    elif selector.basis == "ids":
        selected_ids = set(selector.ids)
    else:
        selected_ids = set(rows)

    if not selected_ids.issubset(rows):
        raise DomainError("not_found", "A selected work is unavailable in the current group")
    excluded = set(selector.exclude_ids)
    query = _title(selector.title_query) if selector.title_query is not None else None
    refs = tuple(
        ref
        for identifier in sorted(selected_ids)
        for ref, requester, goal in (rows[identifier],)
        if identifier not in excluded
        and (selector.requester_scope == "any" or requester == actor.member_id)
        and (not selector.states or ref.state in selector.states)
        and (query is None or query in _title(goal))
    )
    if not refs:
        raise DomainError("not_found", "No work matches every requested restriction")
    if (
        not readonly
        and actor.role != Role.OWNER
        and any(rows[ref.id][1] != actor.member_id for ref in refs)
    ):
        raise DomainError("permission_denied", "The selection includes another member's work")
    if selector.quantifier == "one" and len(refs) != 1:
        raise DomainError("ambiguous_reference", "Several works match the singular selection")
    if len(refs) > 1000:
        raise DomainError(
            "selection_incomplete", "The work selection exceeds its atomic size bound"
        )
    return ResolvedWorkSet(
        group_id=actor.group_id,
        actor_id=actor.member_id,
        source_event_id=event.id,
        selector=selector,
        refs=refs,
    )
