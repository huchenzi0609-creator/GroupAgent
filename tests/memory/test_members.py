from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta

import pytest
from pydantic import ValidationError

from groupagent.channels.models import MemberPage, MemberRecord
from groupagent.domain import DomainError, EventKind, Role, WorkState
from groupagent.memory import ChannelRosterProvider, MemberService, RosterMember, RosterSnapshot
from groupagent.scheduling import EveryTiming, Scheduler

pytestmark = pytest.mark.postgres


class FixtureRoster:
    def __init__(self, snapshot=None, *, error=None):
        self.snapshot, self.error, self.calls = snapshot, error, 0

    async def fetch(self, group_id):
        self.calls += 1
        if self.error:
            raise self.error
        return self.snapshot


def roster(actor, *names, partial=True, coverage="cached"):
    return RosterSnapshot(
        members=[
            RosterMember(
                group_id=actor.group_id,
                member_id=identifier,
                room_display_name=name,
                membership_state="present",
                coverage=coverage,
                name_source="roomDisplayName" if name else "unknown",
            )
            for identifier, name in names
        ],
        partial=partial,
        source_generation="synthetic-generation",
    )


def test_unknown_member_keeps_stable_id_and_placeholder(members, actor, emit):
    event = emit(actor.model_copy(update={"member_id": "900719925474099312345"}))
    member = members.get(actor, event.actor_id)
    assert member.member_id == "900719925474099312345"
    assert member.display_name == "群昵称待补充" and member.membership_state == "unknown"


async def test_startup_sync_is_once_across_service_restart(store, members, owner, actor):
    provider = FixtureRoster(roster(actor, (actor.member_id, "小林")))
    first = await members.sync_startup(owner, "process-1", provider)
    replay = await MemberService(store).sync_startup(owner, "process-1", provider)
    assert first.state == "complete" and replay.cached and provider.calls == 1


async def test_failed_roster_does_not_clear_identity_or_repeat(members, owner, actor, emit):
    emit(actor)
    known = members.get(actor, actor.member_id)
    provider = FixtureRoster(error=OSError("synthetic failure"))
    result = await members.sync_startup(owner, "failed-run", provider)
    assert result.state == "failed" and result.error_code == "roster_unavailable"
    assert (await members.sync_startup(owner, "failed-run", provider)).cached
    assert provider.calls == 1 and members.get(actor, actor.member_id) == known


@pytest.mark.parametrize("partial", [True, False])
async def test_roster_absence_never_means_departure(members, owner, actor, emit, partial):
    emit(actor)
    await members.sync_startup(owner, "empty-run", FixtureRoster(roster(actor, partial=partial)))
    assert members.get(actor, actor.member_id).membership_state == "unknown"


async def test_partial_missing_name_keeps_existing_room_name(members, owner, actor):
    await members.sync_startup(
        owner, "start", FixtureRoster(roster(actor, (actor.member_id, "群昵称")))
    )
    await members.refresh(
        owner, "refresh", FixtureRoster(roster(actor, (actor.member_id, None), partial=True))
    )
    assert members.get(actor, actor.member_id).display_name == "群昵称"


@pytest.mark.parametrize("role", [Role.OWNER, Role.MEMBER, Role.OBSERVER, Role.MAINTAINER])
def test_owner_name_exception_permissions_and_room_priority(members, actor, owner, emit, role):
    emit(actor)
    context = owner if role == Role.OWNER else actor.model_copy(update={"role": role})
    member = members.get(actor, actor.member_id)
    if role != Role.OWNER:
        with pytest.raises(DomainError) as caught:
            members.confirm_name(
                context, actor.member_id, "人工姓名", expected_revision=member.revision
            )
        assert caught.value.code == "permission_denied"
    else:
        named = members.confirm_name(
            context, actor.member_id, "人工姓名", expected_revision=member.revision
        )
        assert named.display_name == "人工姓名" and named.name_source == "owner_confirmed"


async def test_room_name_wins_and_missing_room_uses_owner_exception(members, owner, actor, emit):
    emit(actor)
    members.confirm_name(owner, actor.member_id, "人工例外", expected_revision=1)
    await members.sync_startup(
        owner, "start", FixtureRoster(roster(actor, (actor.member_id, "群内昵称")))
    )
    assert members.get(actor, actor.member_id).display_name == "群内昵称"
    await members.refresh(
        owner, "clear", FixtureRoster(roster(actor, (actor.member_id, None), partial=False))
    )
    assert members.get(actor, actor.member_id).display_name == "人工例外"


def test_personal_nickname_cannot_be_used_as_roster_name(actor):
    with pytest.raises(ValidationError):
        RosterMember.model_validate(
            {"group_id": actor.group_id, "member_id": "member", "personal_name": "私人昵称"}
        )


async def test_same_names_and_rename_never_transfer_task_ownership(
    store, members, owner, actor, emit
):
    source = emit(actor)
    other = actor.model_copy(update={"member_id": "member-b"})
    emit(other)
    with store.transaction(actor) as tx:
        task = tx.create_team_task("PPT分工", source_event_id=source.id)
    await members.sync_startup(
        owner,
        "start",
        FixtureRoster(roster(actor, (actor.member_id, "同名"), (other.member_id, "同名"))),
    )
    with pytest.raises(DomainError) as caught:
        members.resolve_unique(actor, "同名")
    assert caught.value.code == "ambiguous_reference"
    await members.refresh(
        owner, "rename", FixtureRoster(roster(actor, (actor.member_id, "新群昵称")))
    )
    assert members.resolve_unique(actor, "同名").member_id == other.member_id
    with store.transaction(actor) as tx:
        assert tx.get_team_task(task.id).owner_id == actor.member_id


@pytest.mark.parametrize("role", [Role.MEMBER, Role.OBSERVER, Role.OWNER])
def test_reliable_departure_revokes_business_contexts(
    store, members, maintenance, actor, emit, role
):
    emit(actor)
    evidence = emit(maintenance, kind=EventKind.MEMBERSHIP)
    members.apply_membership(
        maintenance, actor.member_id, False, evidence.id, verified_channel=True
    )
    with pytest.raises(DomainError) as caught:
        with store.transaction(actor.model_copy(update={"role": role})):
            pytest.fail("departed identity was admitted")
    assert caught.value.code == "member_left"


def test_departure_pauses_work_schedule_and_revokes_lease(
    store, members, owner, maintenance, actor, emit
):
    source = emit(actor)
    with store.transaction(actor) as tx:
        work = tx.create_work("synthetic work", source.id)
        lease = tx.claim_work(work.id, "worker")
    schedule = Scheduler(store).create(
        actor, "reminder", "synthetic reminder", EveryTiming(interval_seconds=3600)
    )
    evidence = emit(maintenance, kind=EventKind.MEMBERSHIP)
    members.apply_membership(
        maintenance, actor.member_id, False, evidence.id, verified_channel=True
    )
    with store.transaction(owner) as tx:
        assert tx.get_work(work.id).state == WorkState.PAUSED
        with pytest.raises(DomainError) as caught:
            tx.assert_lease(lease)
        assert caught.value.code == "stale_lease"
    assert Scheduler(store).get(owner, schedule.id).state.value == "paused"


async def test_cached_roster_cannot_revive_departed_member(
    members, maintenance, owner, actor, emit
):
    emit(actor)
    evidence = emit(maintenance, kind=EventKind.MEMBERSHIP)
    members.apply_membership(
        maintenance, actor.member_id, False, evidence.id, verified_channel=True
    )
    await members.sync_startup(
        owner,
        "cached",
        FixtureRoster(roster(actor, (actor.member_id, "cached name"), coverage="verified")),
    )
    assert members.get(owner, actor.member_id).membership_state == "left"


def test_rejoin_requires_new_evidence_and_old_leave_replay_is_rejected(
    members, maintenance, actor, emit
):
    emit(actor)
    stamp = datetime.now(UTC)
    leave = emit(maintenance, kind=EventKind.MEMBERSHIP, at=stamp)
    join = emit(maintenance, kind=EventKind.MEMBERSHIP, at=stamp + timedelta(seconds=1))
    left = members.apply_membership(
        maintenance, actor.member_id, False, leave.id, verified_channel=True
    )
    assert (
        members.apply_membership(
            maintenance, actor.member_id, False, leave.id, verified_channel=True
        ).revision
        == left.revision
    )
    members.apply_membership(maintenance, actor.member_id, True, join.id, verified_channel=True)
    with pytest.raises(DomainError) as caught:
        members.apply_membership(
            maintenance, actor.member_id, False, leave.id, verified_channel=True
        )
    assert caught.value.code == "stale_membership_event"
    assert members.get(actor, actor.member_id).membership_state == "present"


@pytest.mark.parametrize("verified,kind", [(False, EventKind.MEMBERSHIP), (True, EventKind.TEXT)])
def test_unverified_membership_is_never_authority(
    members, maintenance, actor, emit, verified, kind
):
    emit(actor)
    evidence = emit(maintenance, kind=kind)
    with pytest.raises(DomainError):
        members.apply_membership(
            maintenance, actor.member_id, False, evidence.id, verified_channel=verified
        )
    assert members.get(actor, actor.member_id).membership_state == "unknown"


def test_maintenance_role_stays_technical_after_departure(store, members, maintenance, actor, emit):
    source = emit(actor)
    evidence = emit(maintenance, kind=EventKind.MEMBERSHIP)
    members.apply_membership(
        maintenance, actor.member_id, False, evidence.id, verified_channel=True
    )
    technical_actor = actor.model_copy(update={"role": Role.MAINTAINER})
    with store.transaction(technical_actor) as tx:
        assert tx.get_event(source.id).id == source.id
        with pytest.raises(DomainError) as caught:
            tx.create_work("not business authority", source.id)
        assert caught.value.code == "permission_denied"


async def test_two_startup_callers_fetch_only_once(members, maintenance, actor):
    started = asyncio.Event()
    release = asyncio.Event()

    class Provider:
        calls = 0

        async def fetch(self, group_id):
            self.calls += 1
            started.set()
            await release.wait()
            return roster(actor, (actor.member_id, "名字"))

    provider = Provider()
    first = asyncio.create_task(members.sync_startup(maintenance, "same-process", provider))
    await started.wait()
    second = await members.sync_startup(maintenance, "same-process", provider)
    assert second.state == "fetching" and second.cached and provider.calls == 1
    release.set()
    assert (await first).state == "complete"


async def test_channel_roster_provider_checks_pagination_scope(actor):
    class Channel:
        async def members(self, *, cursor=None, limit=100):
            return MemberPage(
                members=(
                    MemberRecord(
                        group_id="foreign",
                        member_id="x",
                        room_display_name="name",
                        membership_state="present",
                        name_source="roomDisplayName",
                    ),
                ),
                next_cursor=None,
                source_generation="1",
                partial=True,
            )

    with pytest.raises(DomainError) as caught:
        await ChannelRosterProvider(Channel()).fetch(actor.group_id)
    assert caught.value.code == "scope_violation"


def test_confirm_name_is_part_of_existing_atomic_transaction(
    store, memory, members, owner, actor, emit
):
    emit(actor)
    with pytest.raises(RuntimeError):
        with store.transaction(owner) as tx:
            memory.prepare_in_tx(tx)
            members.in_tx(tx).confirm_name(
                owner, actor.member_id, "rollback-name", expected_revision=1
            )
            raise RuntimeError("crash before coordinator decision")
    assert members.get(actor, actor.member_id).confirmed_name is None
