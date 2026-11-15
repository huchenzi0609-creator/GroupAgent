"""Stable member identity, bounded startup roster refresh and reliable revocation."""

from __future__ import annotations

import builtins
from typing import Protocol
from uuid import uuid4

from psycopg.types.json import Jsonb

from groupagent.channels.models import MemberPage
from groupagent.domain import ActorContext, DomainError, EventKind, Role
from groupagent.memory.common import BoundStore, memory_lock, technical
from groupagent.memory.models import MemberIdentity, RosterMember, RosterSnapshot, RosterSyncResult
from groupagent.persistence import PostgresStore, UnitOfWork


class RosterProvider(Protocol):
    async def fetch(self, group_id: str) -> RosterSnapshot: ...


class RosterChannel(Protocol):
    async def members(self, *, cursor: str | None = None, limit: int = 100) -> MemberPage: ...


class ChannelRosterProvider:
    def __init__(self, channel: RosterChannel, *, max_pages: int = 100) -> None:
        self.channel = channel
        self.max_pages = max_pages

    async def fetch(self, group_id: str) -> RosterSnapshot:
        members: dict[str, RosterMember] = {}
        cursor = None
        seen: set[str] = set()
        generation = None
        partial = False
        for _ in range(self.max_pages):
            page = await self.channel.members(cursor=cursor, limit=100)
            if generation is not None and generation != page.source_generation:
                raise DomainError("roster_changed", "Roster generation changed during pagination")
            generation = page.source_generation
            partial = partial or page.partial
            for member in page.members:
                if member.group_id != group_id:
                    raise DomainError("scope_violation", "Roster contains another group")
                members[member.member_id] = RosterMember.model_validate(member.model_dump())
            if page.next_cursor is None:
                return RosterSnapshot(
                    members=list(members.values()), partial=partial, source_generation=generation
                )
            if page.next_cursor in seen:
                raise DomainError("invalid_channel_response", "Roster cursor did not advance")
            seen.add(page.next_cursor)
            cursor = page.next_cursor
        raise DomainError("quota_exceeded", "Roster exceeds the configured page bound")


class MemberService:
    def __init__(self, store: PostgresStore) -> None:
        self.store = store

    def in_tx(self, tx: UnitOfWork) -> MemberService:
        return MemberService(BoundStore(tx))

    def get(self, actor: ActorContext, member_id: str) -> MemberIdentity:
        with self.store.transaction(actor) as tx:
            row = tx.conn.execute(
                "SELECT * FROM ga_members WHERE group_id=%s AND member_id=%s",
                (actor.group_id, member_id),
            ).fetchone()
            if row is None:
                raise DomainError("not_found", "Member identity was not found")
            return MemberIdentity.model_validate(row)

    def list(self, actor: ActorContext, *, include_left: bool = False) -> list[MemberIdentity]:
        with self.store.transaction(actor) as tx:
            rows = tx.conn.execute(
                "SELECT * FROM ga_members WHERE group_id=%s AND (%s OR membership_state<>'left') "
                "ORDER BY member_id LIMIT 10000",
                (actor.group_id, include_left),
            ).fetchall()
            return [MemberIdentity.model_validate(row) for row in rows]

    def resolve_name(self, actor: ActorContext, name: str) -> builtins.list[MemberIdentity]:
        if not name.strip() or name.strip() == "群昵称待补充":
            return []
        return [
            member
            for member in self.list(actor)
            if member.display_name.casefold() == name.strip().casefold()
        ]

    def resolve_unique(self, actor: ActorContext, name: str) -> MemberIdentity:
        matches = self.resolve_name(actor, name)
        if len(matches) != 1:
            raise DomainError(
                "ambiguous_reference" if matches else "not_found", "Member name is not unique"
            )
        return matches[0]

    def confirm_name(
        self, actor: ActorContext, member_id: str, name: str | None, *, expected_revision: int
    ) -> MemberIdentity:
        if actor.role != Role.OWNER:
            raise DomainError(
                "permission_denied", "Only the group owner may confirm a name exception"
            )
        if name is not None and (not name.strip() or len(name) > 200):
            raise DomainError("invalid_input", "Confirmed name is invalid")
        with self.store.transaction(actor) as tx:
            memory_lock(tx)
            row = tx.conn.execute(
                "SELECT * FROM ga_members WHERE group_id=%s AND member_id=%s FOR UPDATE",
                (actor.group_id, member_id),
            ).fetchone()
            if row is None:
                raise DomainError("not_found", "Member identity was not found")
            tx._revision(row, expected_revision)
            updated = tx.conn.execute(
                "UPDATE ga_members SET confirmed_name=%s, confirmed_by=%s, revision=revision+1, "
                "updated_at=clock_timestamp() WHERE group_id=%s AND member_id=%s RETURNING *",
                (
                    name.strip() if name else None,
                    actor.member_id if name else None,
                    actor.group_id,
                    member_id,
                ),
            ).fetchone()
            tx._audit("member_name_confirmed", member_id, revision=expected_revision + 1)
            tx.conn.execute(
                "UPDATE ga_memory_epochs SET epoch=epoch+1 WHERE group_id=%s", (actor.group_id,)
            )
            return MemberIdentity.model_validate(updated)

    async def sync_startup(
        self, actor: ActorContext, run_id: str, provider: RosterProvider
    ) -> RosterSyncResult:
        return await self._sync(actor, "startup:" + run_id, provider)

    async def refresh(
        self, actor: ActorContext, request_id: str, provider: RosterProvider
    ) -> RosterSyncResult:
        if actor.role != Role.OWNER:
            raise DomainError(
                "permission_denied", "Only the owner may explicitly refresh the roster"
            )
        return await self._sync(actor, "manual:" + request_id, provider)

    async def _sync(
        self, actor: ActorContext, key: str, provider: RosterProvider
    ) -> RosterSyncResult:
        if isinstance(self.store, BoundStore):
            raise DomainError(
                "invalid_transaction", "Roster provider calls must occur outside a transaction"
            )
        if not key.split(":", 1)[1].strip():
            raise DomainError("invalid_input", "Roster operation needs a stable run or request ID")
        token = uuid4().hex
        with self.store.transaction(actor) as tx:
            technical(tx)
            inserted = tx.conn.execute(
                "INSERT INTO ga_roster_runs(group_id,run_key,state,token) VALUES (%s,%s,'fetching',%s) "
                "ON CONFLICT DO NOTHING RETURNING token",
                (actor.group_id, key, token),
            ).fetchone()
            if inserted is None:
                old = tx.conn.execute(
                    "SELECT * FROM ga_roster_runs WHERE group_id=%s AND run_key=%s",
                    (actor.group_id, key),
                ).fetchone()
                assert old is not None
                return RosterSyncResult(
                    run_key=key, state=old["state"], **old["result"], cached=True
                )
        try:
            snapshot = await provider.fetch(actor.group_id)
            if any(member.group_id != actor.group_id for member in snapshot.members):
                raise DomainError("scope_violation", "Roster contains another group")
            if len({member.member_id for member in snapshot.members}) != len(snapshot.members):
                raise DomainError("invalid_channel_response", "Roster repeats a member ID")
        except Exception as error:
            code = error.code if isinstance(error, DomainError) else "roster_unavailable"
            with self.store.transaction(actor) as tx:
                tx.conn.execute(
                    "UPDATE ga_roster_runs SET state='failed',result=%s,finished_at=clock_timestamp() "
                    "WHERE group_id=%s AND run_key=%s AND token=%s",
                    (Jsonb({"error_code": code}), actor.group_id, key, token),
                )
                tx._audit("roster_sync_failed", key, error_code=code)
            return RosterSyncResult(run_key=key, state="failed", error_code=code)
        with self.store.transaction(actor) as tx:
            technical(tx)
            memory_lock(tx)
            names_changed = False
            for member in snapshot.members:
                # Cached presence/absence never overrides reliable departure;
                # missing names on a partial cache do not erase known room names.
                room_name = (
                    member.room_display_name.strip()
                    if member.room_display_name and member.room_display_name.strip()
                    else None
                )
                previous = tx.conn.execute(
                    "SELECT room_name FROM ga_members WHERE group_id=%s AND member_id=%s",
                    (actor.group_id, member.member_id),
                ).fetchone()
                effective_name = (
                    previous["room_name"]
                    if previous and snapshot.partial and room_name is None
                    else room_name
                )
                names_changed = (
                    names_changed or previous is None or previous["room_name"] != effective_name
                )
                tx.conn.execute(
                    "INSERT INTO ga_members(group_id,member_id,room_name,membership_state) "
                    "VALUES (%s,%s,%s,%s) ON CONFLICT (group_id,member_id) DO UPDATE SET "
                    "room_name=CASE WHEN %s AND EXCLUDED.room_name IS NULL THEN ga_members.room_name ELSE EXCLUDED.room_name END, "
                    "membership_state=CASE WHEN ga_members.membership_state='left' THEN 'left' "
                    "WHEN EXCLUDED.membership_state='present' THEN 'present' ELSE ga_members.membership_state END, "
                    "revision=ga_members.revision+1,updated_at=clock_timestamp()",
                    (
                        actor.group_id,
                        member.member_id,
                        room_name,
                        "present"
                        if member.coverage == "verified" and member.membership_state == "present"
                        else "unknown",
                        snapshot.partial,
                    ),
                )
            result = {"updated_members": len(snapshot.members), "partial": snapshot.partial}
            tx.conn.execute(
                "UPDATE ga_roster_runs SET state='complete',result=%s,finished_at=clock_timestamp() "
                "WHERE group_id=%s AND run_key=%s AND token=%s",
                (Jsonb(result), actor.group_id, key, token),
            )
            tx._audit(
                "roster_synced",
                key,
                updated_members=len(snapshot.members),
                partial=snapshot.partial,
            )
            if names_changed:
                tx.conn.execute(
                    "UPDATE ga_memory_epochs SET epoch=epoch+1 WHERE group_id=%s", (actor.group_id,)
                )
            return RosterSyncResult(
                run_key=key,
                state="complete",
                updated_members=len(snapshot.members),
                partial=snapshot.partial,
            )

    def apply_membership(
        self,
        actor: ActorContext,
        member_id: str,
        present: bool,
        event_id: str,
        *,
        verified_channel: bool = False,
    ) -> MemberIdentity:
        with self.store.transaction(actor) as tx:
            technical(tx)
            if verified_channel is not True:
                raise DomainError(
                    "permission_denied", "Membership changes require verified channel evidence"
                )
            event = tx.get_event(event_id)
            if (
                event.kind != EventKind.MEMBERSHIP
                or event.content_state != "active"
                or event.trusted_actor_origin
                not in {"verified_channel_metadata", "agentwechat_authenticated_message_metadata"}
            ):
                raise DomainError("permission_denied", "Reliable membership metadata is required")
            memory_lock(tx)
            previous = tx.conn.execute(
                "SELECT * FROM ga_members WHERE group_id=%s AND member_id=%s FOR UPDATE",
                (actor.group_id, member_id),
            ).fetchone()
            if previous and previous["membership_event_id"]:
                if previous["membership_event_id"] == event_id:
                    return MemberIdentity.model_validate(previous)
                prior_event = tx.get_event(previous["membership_event_id"])
                if prior_event.occurred_at > event.occurred_at:
                    raise DomainError(
                        "stale_membership_event",
                        "Older membership evidence cannot replace a newer state",
                    )
            row = tx.conn.execute(
                "INSERT INTO ga_members(group_id,member_id,membership_state,membership_event_id) "
                "VALUES (%s,%s,%s,%s) ON CONFLICT (group_id,member_id) DO UPDATE SET "
                "membership_state=EXCLUDED.membership_state,membership_event_id=EXCLUDED.membership_event_id, "
                "revision=ga_members.revision+1,updated_at=clock_timestamp() RETURNING *",
                (actor.group_id, member_id, "present" if present else "left", event_id),
            ).fetchone()
            if not present:
                works = tx.conn.execute(
                    "UPDATE ga_work_orders SET state='paused',pause_reason='member_left',revision=revision+1, "
                    "updated_at=clock_timestamp() WHERE group_id=%s AND requester_id=%s "
                    "AND state NOT IN ('completed','failed','cancelled') RETURNING id",
                    (actor.group_id, member_id),
                ).fetchall()
                for work in works:
                    tx._revoke(work["id"])
                tx.conn.execute(
                    "UPDATE ga_schedules SET state='paused',pause_reason='member_left',revision=revision+1 WHERE group_id=%s AND creator_id=%s AND state='active'",
                    (actor.group_id, member_id),
                )
                tx.conn.execute(
                    "UPDATE ga_deliveries SET state='rejected',reason='member_left',revision=revision+1 WHERE group_id=%s AND created_by=%s AND state='pending'",
                    (actor.group_id, member_id),
                )
            tx._audit("membership_changed", member_id, present=present, source_event_id=event_id)
            tx.conn.execute(
                "UPDATE ga_memory_epochs SET epoch=epoch+1 WHERE group_id=%s", (actor.group_id,)
            )
            return MemberIdentity.model_validate(row)
