"""Event-driven and explicitly requested roster freshness, without periodic polling."""

from __future__ import annotations

import asyncio
import re
from datetime import UTC, datetime
from typing import Any

from groupagent.conversation.participation import outside_quotes
from groupagent.domain import ActorContext, DomainError, EventKind, GroupEvent, Role
from groupagent.memory import ChannelRosterProvider, MemberService


def roster_query(event: GroupEvent, context: dict[str, Any]) -> bool:
    if event.kind != EventKind.TEXT or event.attachments or len(event.text) > 300:
        return False
    plain = outside_quotes(event.text)
    if plain != event.text.strip():
        return False
    if re.search(r"群昵称|群成员|成员名单|小组名单|群里.{0,10}(?:谁|哪些|成员)", plain):
        return bool(
            re.search(r"[?？]|查看|检查|刷新|看看|能.{0,8}(?:看|读)|有谁|有哪些|名单", plain)
        )
    if not re.search(r"再看看|再查|现在.{0,8}(?:看见|看到|行了)|我.{0,5}设置了", plain):
        return False
    return any(
        re.search(r"群昵称|群成员|成员名单", str(row.get("text", "")))
        for row in context.get("recent_messages", [])[-4:]
    )


class RosterRefresher:
    def __init__(self, members: MemberService, channel: Any, actor: ActorContext, fence) -> None:
        self.members, self.channel, self.actor, self.fence = members, channel, actor, fence
        self._lock = asyncio.Lock()
        self._pending: str | None = None
        self._results: dict[str, dict[str, Any]] = {}

    def changed(self, updates) -> None:
        if updates:
            self._pending = updates[-1].serialize()

    async def sync_pending(self) -> None:
        if self._pending is None or self._lock.locked():
            return
        async with self._lock:
            request_id, self._pending = self._pending, None
            self.fence()
            result = await self.members.sync_runtime(
                self.actor, request_id, ChannelRosterProvider(self.channel), reason="group_updated"
            )
            self.fence()
            if result.state == "failed":
                raise DomainError(result.error_code or "roster_unavailable", "Roster sync failed")

    async def query(
        self, actor: ActorContext, event: GroupEvent, context: dict[str, Any]
    ) -> dict[str, Any] | None:
        if actor.role not in {Role.OWNER, Role.MEMBER} or not roster_query(event, context):
            return None
        if actor.group_id != self.actor.group_id:
            raise DomainError("scope_denied", "Roster query belongs to another group")
        async with self._lock:
            self.fence()
            if event.id in self._results:
                return dict(self._results[event.id])
            status: dict[str, Any] = {"fresh": False, "source": "cached", "error_code": None}
            refresh = getattr(self.channel, "refresh_members", None)
            if not callable(refresh):
                status["error_code"] = "roster_refresh_unavailable"
            else:
                try:
                    await refresh(actor.group_id, timeout_seconds=5)
                    self.fence()
                    result = await self.members.sync_runtime(
                        self.actor,
                        event.id,
                        ChannelRosterProvider(self.channel),
                        reason="member_query",
                    )
                    status.update(
                        fresh=result.state == "complete",
                        source="remote_refresh",
                        error_code=result.error_code,
                    )
                except DomainError as error:
                    if error.code in {"stale_lease", "policy_changed", "restore_pending"}:
                        raise
                    status["error_code"] = error.code
            self.fence()
            status["checked_at"] = datetime.now(UTC).isoformat()
            self._results[event.id] = status
            if len(self._results) > 32:
                self._results.pop(next(iter(self._results)))
            return dict(status)
