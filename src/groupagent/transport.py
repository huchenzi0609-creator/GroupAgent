"""Durable channel intake; upstream ACKs follow database commits, never model calls.

This module composes the pinned adapter and trusted coordinator. It does not start
a receiver loop or enable production sending. A scheduler/operator controls polls.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Protocol

from pydantic import ValidationError

from groupagent.channels import AgentWechatConfig, ChannelCursor, EventBatch
from groupagent.coordinator import CoordinationResult, Coordinator
from groupagent.domain import ActorContext, DomainError, Role


@dataclass(frozen=True)
class IntakeResult:
    event_ids: tuple[str, ...]
    skipped_count: int
    cursor: str
    acknowledged: bool


class IntakePort(Protocol):
    config: AgentWechatConfig

    async def consume(
        self, after_cursor: ChannelCursor | None = None, *, limit: int = 100
    ) -> EventBatch: ...

    async def ack_committed(self, cursor: ChannelCursor) -> None: ...


class ChannelIntake:
    def __init__(self, adapter: IntakePort, coordinator: Coordinator) -> None:
        channel, group = adapter.config, coordinator.config.group
        if (
            not group.enabled
            or channel.internal_group_id != group.internal_id
            or channel.external_group_id != group.external_group_id
            or channel.policy_revision != group.policy_revision
        ):
            raise DomainError("scope_violation", "Channel and control policies do not match")
        self.adapter, self.coordinator = adapter, coordinator
        self.service_actor = ActorContext(
            group_id=group.internal_id,
            member_id="service:channel-intake",
            role=Role.MAINTAINER,
            policy_revision=group.policy_revision,
        )
        # Epoch deliberately is NOT in the stream key: a reset must not silently
        # discard its old cursor. Adapter scope validation requires explicit recovery.
        binding = [channel.account_id, group.internal_id, group.external_group_id]
        self.stream_id = "agentwechat:" + hashlib.sha256(json.dumps(binding).encode()).hexdigest()

    def _cursor(self, value: str | None) -> ChannelCursor | None:
        if value is None:
            return None
        try:
            cursor = ChannelCursor.model_validate_json(value)
        except ValidationError as exc:
            raise DomainError("invalid_cursor", "Stored channel cursor is invalid") from exc
        channel = self.adapter.config
        if (
            cursor.account_id != channel.account_id
            or cursor.group_id != channel.internal_group_id
            or cursor.external_group_id != channel.external_group_id
            or cursor.source_epoch != channel.source_epoch
        ):
            raise DomainError("source_epoch_mismatch", "Stored cursor needs explicit recovery")
        return cursor

    def _not_after(self, current: str | None, target: ChannelCursor) -> None:
        previous = self._cursor(current)
        if previous is not None and (
            previous.daemon_server_id != target.daemon_server_id
            or int(previous.value) > int(target.value)
        ):
            raise DomainError("version_conflict", "Channel cursor changed during intake")

    async def poll_once(self, *, limit: int = 100) -> IntakeResult:
        store = self.coordinator.store
        with store.transaction(self.service_actor) as tx:
            saved = tx.get_cursor(self.stream_id)
        batch = await self.adapter.consume(
            self._cursor(saved.cursor if saved else None), limit=limit
        )
        canonical_ids = []
        rejected_count = 0
        for item in batch.entries:
            if not self.coordinator.accepts_event(item.event):
                rejected_count += 1
                continue
            actor = self.coordinator.actor(item.event)
            try:
                with store.transaction(actor, trace_id=item.event.id) as tx:
                    current = tx.get_cursor(self.stream_id)
                    self._not_after(current.cursor if current else None, item.cursor)
                    ingested = tx.ingest_event(
                        item.event,
                        stream_id=self.stream_id,
                        cursor=item.cursor.serialize(),
                        expected_cursor_revision=current.revision if current else 0,
                    )
            except DomainError as error:
                if error.code != "member_left":
                    raise
                with store.transaction(self.service_actor, trace_id=item.event.id) as tx:
                    tx._audit(
                        "channel_event_rejected",
                        item.event.id,
                        reason="member_left",
                        source_digest=hashlib.sha256(item.event.source_key.encode()).hexdigest(),
                    )
                rejected_count += 1
                continue
            canonical_ids.append(ingested.event.id)
        # Foreign/control-only batches still advance, without persisting another
        # group's message body or fabricating an authorized group's message.
        with store.transaction(self.service_actor) as tx:
            current = tx.get_cursor(self.stream_id)
            self._not_after(current.cursor if current else None, batch.next_cursor)
            tx.advance_cursor(
                self.stream_id,
                batch.next_cursor.serialize(),
                expected_cursor_revision=current.revision if current else 0,
                reason="verified_channel_batch",
            )
        # Failure here leaves durable intake intact. A restart resumes that cursor;
        # it never rolls back committed events or asks a model to re-create work.
        await self.adapter.ack_committed(batch.next_cursor)
        return IntakeResult(
            event_ids=tuple(canonical_ids),
            skipped_count=len(batch.skipped) + rejected_count,
            cursor=batch.next_cursor.serialize(),
            acknowledged=True,
        )

    async def process_pending(self, *, limit: int = 100) -> list[CoordinationResult]:
        with self.coordinator.store.transaction(self.service_actor) as tx:
            events = tx.list_pending_events(limit=limit)
        return [await self.coordinator.process(event) for event in events]
