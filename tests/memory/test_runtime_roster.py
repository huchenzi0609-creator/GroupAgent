"""Runtime observation sync uses technical authority and stable member IDs."""

from __future__ import annotations

import pytest

from groupagent.domain import DomainError, Role
from groupagent.memory import ChannelRosterProvider, MemberService, RosterMember, RosterSnapshot

pytestmark = pytest.mark.postgres


class SnapshotProvider:
    def __init__(self, actor, name, *, group_id=None):
        self.actor, self.name, self.group_id = actor, name, group_id
        self.calls = 0

    async def fetch(self, group_id):
        self.calls += 1
        return RosterSnapshot(
            members=[
                RosterMember(
                    group_id=self.group_id or group_id,
                    member_id=self.actor.member_id,
                    room_display_name=self.name,
                    membership_state="present",
                    name_source="roomDisplayName" if self.name else "unknown",
                )
            ],
            source_generation="synthetic-generation",
            partial=True,
        )


@pytest.mark.parametrize(
    "member_id,role",
    [
        ("service:runtime", Role.MEMBER),
        ("service:runtime", Role.OWNER),
        ("human-maintainer", Role.MAINTAINER),
    ],
)
async def test_runtime_roster_sync_requires_exact_technical_identity(
    members, actor, member_id, role
):
    provider = SnapshotProvider(actor, "群昵称")
    caller = actor.model_copy(update={"member_id": member_id, "role": role})
    with pytest.raises(DomainError) as error:
        await members.sync_runtime(caller, "event", provider, reason="member_query")
    assert error.value.code == "permission_denied" and provider.calls == 0


async def test_runtime_roster_observations_refresh_rename_once_and_preserve_partial_missing_name(
    store, members, maintenance, actor
):
    runtime = maintenance.model_copy(update={"member_id": "service:runtime"})
    initial = SnapshotProvider(actor, "旧群昵称")
    first = await members.sync_runtime(runtime, "query-1", initial, reason="member_query")
    replay = await MemberService(store).sync_runtime(
        runtime, "query-1", initial, reason="member_query"
    )
    assert first.state == "complete" and replay.cached and initial.calls == 1
    renamed = SnapshotProvider(actor, "新群昵称")
    changed = await members.sync_runtime(runtime, "cursor-2", renamed, reason="group_updated")
    assert changed.state == "complete"
    member = members.get(actor, actor.member_id)
    assert member.member_id == actor.member_id and member.room_name == "新群昵称"
    await members.sync_runtime(
        runtime, "cursor-3", SnapshotProvider(actor, None), reason="group_updated"
    )
    assert members.get(actor, actor.member_id).room_name == "新群昵称"


async def test_runtime_observation_rejects_foreign_snapshot_without_clearing_names(
    members, maintenance, actor
):
    runtime = maintenance.model_copy(update={"member_id": "service:runtime"})
    await members.sync_runtime(
        runtime, "initial", SnapshotProvider(actor, "群昵称"), reason="member_query"
    )
    result = await members.sync_runtime(
        runtime,
        "foreign",
        SnapshotProvider(actor, "foreign name", group_id="another-group"),
        reason="member_query",
    )
    assert result.state == "failed" and result.error_code == "scope_violation"
    assert members.get(actor, actor.member_id).room_name == "群昵称"


async def test_configured_channel_scope_is_checked_even_for_empty_roster():
    class Config:
        internal_group_id = "another-group"

    class Channel:
        config = Config()
        calls = 0

        async def members(self, **kwargs):
            self.calls += 1
            pytest.fail("foreign channel must be rejected before any metadata request")

    channel = Channel()
    with pytest.raises(DomainError) as error:
        await ChannelRosterProvider(channel).fetch("synthetic-group")
    assert error.value.code == "scope_violation" and channel.calls == 0
