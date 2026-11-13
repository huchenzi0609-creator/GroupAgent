from datetime import UTC, datetime, timedelta, timezone

import pytest
from pydantic import ValidationError

from groupagent.domain import (
    ActorContext,
    DomainError,
    EventKind,
    GroupEvent,
    Role,
    WorkState,
    check_transition,
)


def test_event_preserves_platform_ids_and_normalizes_aware_times() -> None:
    at = datetime(2030, 1, 1, 8, tzinfo=timezone(timedelta(hours=8)))
    event = GroupEvent(
        id="900719925474099312345",
        source_key="account:900719925474099312345",
        group_id="group",
        actor_id="900719925474099312346",
        kind=EventKind.TEXT,
        occurred_at=at,
        received_at=at,
    )
    assert event.id == "900719925474099312345"
    assert event.occurred_at == datetime(2030, 1, 1, tzinfo=UTC)
    assert event.occurred_at.tzinfo == UTC
    assert event.text == ""
    assert event.attachments == []
    with pytest.raises(ValidationError):
        GroupEvent.model_validate({**event.model_dump(), "id": 900719925474099312345})
    with pytest.raises(ValidationError):
        GroupEvent.model_validate({**event.model_dump(), "occurred_at": datetime(2030, 1, 1)})


def test_actor_context_is_frozen_and_has_no_privilege_aliases() -> None:
    actor = ActorContext(group_id="group", member_id="member", role=Role.MEMBER)
    assert actor.policy_revision == 1
    with pytest.raises(ValidationError):
        actor.role = Role.OWNER
    with pytest.raises(ValidationError):
        ActorContext.model_validate({"group_id": "group", "member_id": "member", "role": "admin"})
    with pytest.raises(ValidationError):
        ActorContext(group_id="", member_id="member", role=Role.MEMBER)


@pytest.mark.parametrize("terminal", [WorkState.COMPLETED, WorkState.CANCELLED])
@pytest.mark.parametrize("target", list(WorkState))
def test_terminal_work_has_no_state_rewrite(terminal: WorkState, target: WorkState) -> None:
    with pytest.raises(DomainError, match="Cannot move") as caught:
        check_transition(terminal, target)
    assert caught.value.code == "invalid_transition"


def test_video_is_a_distinct_unsupported_event() -> None:
    assert EventKind.UNSUPPORTED_VIDEO.value == "unsupported_video"
    assert "video" not in {kind.value for kind in EventKind}
