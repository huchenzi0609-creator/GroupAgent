"""Task declarations are business facts, separate from whom speech addresses.

This predicate identifies declarations which may be recorded without replying
to the group. It does not authorize a database mutation or reinterpret quoted
or forwarded source data; the canonical action and source gates still apply.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

from pydantic import ValidationError

from groupagent.domain import Role, TeamTask

from .actions import ACTION_ADAPTER, AssignTask, ClaimTask, TaskBatch, UpdateTask


def is_task_declaration(
    action: Any,
    role: str | Role | None,
    requester: str | None,
    tasks: Iterable[TeamTask | Mapping[str, Any]],
) -> bool:
    """Recognize self claims/progress and authorized owner assignments only."""
    if role not in {Role.OWNER, Role.MEMBER} or not requester:
        return False
    try:
        parsed = ACTION_ADAPTER.validate_python(action)
    except (ValidationError, TypeError):
        return False
    rows = [task.model_dump() if isinstance(task, TeamTask) else task for task in tasks]

    def owns(identifier: str, *, unassigned: bool = False) -> bool:
        matches = [row for row in rows if row.get("id") == identifier]
        return (
            len(matches) == 1
            and matches[0].get("content_state", "active") == "active"
            and (
                matches[0].get("owner_id") == requester
                or unassigned
                and matches[0].get("owner_id") is None
            )
        )

    def declares(item) -> bool:
        if isinstance(item, ClaimTask):
            return item.task_id is None or owns(item.task_id, unassigned=True)
        if isinstance(item, UpdateTask):
            return owns(item.task_id)
        if isinstance(item, AssignTask):
            return role == Role.OWNER
        if isinstance(item, TaskBatch):
            return all(declares(child) for child in item.actions)
        return False

    return declares(parsed)
