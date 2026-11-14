"""Finite event subscriptions backed by committed business/channel facts.

This is a trusted control-plane API, never a listener-code or model event port.
It admits ordinary WorkOrders atomically and performs no network/tool/send calls.
"""

from __future__ import annotations

import builtins
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from pathlib import PurePath
from typing import Any, Literal
from uuid import uuid4

from psycopg.types.json import Jsonb
from pydantic import AwareDatetime, Field, JsonValue

from groupagent.channels.models import AttachmentHandle
from groupagent.domain import (
    ActorContext,
    DomainError,
    DomainModel,
    EventKind,
    GroupEvent,
    Role,
    WorkState,
)
from groupagent.persistence import PostgresStore, UnitOfWork
from groupagent.scheduling.recurrence import utc

TriggerType = Literal["task_completed", "work_completed", "work_failed", "attachment_received"]
TriggerState = Literal["active", "paused", "cancelled", "ended"]
_TYPES = {"task_completed", "work_completed", "work_failed", "attachment_received"}
_HUMAN_ORIGINS = {
    "verified_channel_metadata",
    "agentwechat_authenticated_message_metadata",
    "authenticated_audio_transcription",
}
_VIDEO_EXTENSIONS = {
    ".mp4",
    ".mov",
    ".mkv",
    ".avi",
    ".webm",
    ".m4v",
    ".3gp",
    ".flv",
    ".wmv",
    ".mpeg",
    ".mpg",
    ".mts",
    ".m2ts",
    ".ts",
    ".vob",
    ".ogv",
}
Row = dict[str, Any]


class TriggerRule(DomainModel):
    id: str
    group_id: str
    creator_id: str
    creator_role: Role
    request_key: str
    event_type: TriggerType
    resource_id: str
    goal: str
    max_occurrences: int = Field(ge=1, le=10)
    count: int = Field(ge=0)
    expires_at: AwareDatetime
    state: TriggerState
    revision: int
    policy_revision: int
    derived_depth: int
    scope: dict[str, JsonValue]
    created_at: AwareDatetime


class TriggerOccurrence(DomainModel):
    group_id: str
    rule_id: str
    event_key: str
    work_id: str | None
    state: Literal["enqueued", "skipped"]
    reason: str | None
    created_at: AwareDatetime


def _text(value: str, name: str, maximum: int = 4096) -> None:
    if not isinstance(value, str) or not value.strip() or len(value) > maximum:
        raise DomainError("invalid_input", f"{name} must be bounded nonempty text")


class EventTriggerService:
    def __init__(
        self,
        store: PostgresStore,
        actor_resolver: Callable[[str], ActorContext],
        *,
        max_active_per_group: int = 10,
        default_valid_days: int = 30,
    ) -> None:
        if (
            type(max_active_per_group) is not int
            or not 1 <= max_active_per_group <= 10
            or type(default_valid_days) is not int
            or not 1 <= default_valid_days <= 30
        ):
            raise ValueError("Trigger limits must fit the 10-rule / 30-day defaults")
        self.store = store
        self.resolve_actor = actor_resolver
        self.max_active = max_active_per_group
        self.default_days = default_valid_days
        self._bound: UnitOfWork | None = None

    def in_tx(self, tx: UnitOfWork) -> EventTriggerService:
        bound = EventTriggerService(
            self.store,
            self.resolve_actor,
            max_active_per_group=self.max_active,
            default_valid_days=self.default_days,
        )
        bound._bound = tx
        return bound

    @contextmanager
    def _transaction(self, actor: ActorContext, *, mutate: bool = False) -> Iterator[UnitOfWork]:
        if self._bound is not None:
            if actor != self._bound.actor:
                raise DomainError("scope_violation", "Bound trigger authority cannot change")
            if mutate:
                self._lock(self._bound)
            yield self._bound
        else:
            with self.store.transaction(actor) as tx:
                if mutate:
                    self._lock(tx)
                yield tx

    @staticmethod
    def _lock(tx: UnitOfWork) -> None:
        # Same order as an interaction that already holds the privacy lock before
        # creating a schedule. Polling must not invert that order and deadlock it.
        for key in ("memory:" + tx.group_id, "groupagent:scheduler:" + tx.group_id):
            tx.conn.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s,0))", (key,))

    @staticmethod
    def _row(tx: UnitOfWork, identifier: str) -> Row:
        row = tx.conn.execute(
            "SELECT * FROM ga_trigger_rules WHERE group_id=%s AND id=%s FOR UPDATE",
            (tx.group_id, identifier),
        ).fetchone()
        if row is None:
            raise DomainError("not_found", "Trigger was not found in this group")
        return row

    def _quota(self, tx: UnitOfWork, excluding: str | None = None) -> None:
        count = tx.conn.execute(
            "SELECT (SELECT count(*) FROM ga_schedules WHERE group_id=%s AND state='active') + "
            "(SELECT count(*) FROM ga_trigger_rules WHERE group_id=%s AND state='active' "
            "AND (%s::text IS NULL OR id<>%s)) AS n",
            (tx.group_id, tx.group_id, excluding, excluding),
        ).fetchone()
        assert count is not None
        if count["n"] >= self.max_active:
            raise DomainError("quota_exceeded", "Time and event rules share the group quota")

    @staticmethod
    def _resource(tx: UnitOfWork, event_type: str, resource_id: str) -> None:
        tx._writer()
        if event_type == "task_completed":
            tx.conn.execute(
                "SELECT id FROM ga_team_tasks WHERE group_id=%s AND id=%s FOR SHARE",
                (tx.group_id, resource_id),
            )
            task = tx.get_team_task(resource_id)
            if tx.actor.role != Role.OWNER and task.owner_id != tx.actor.member_id:
                raise DomainError("permission_denied", "Members may follow only their own tasks")
        elif event_type in {"work_completed", "work_failed"}:
            tx.conn.execute(
                "SELECT id FROM ga_work_orders WHERE group_id=%s AND id=%s FOR SHARE",
                (tx.group_id, resource_id),
            )
            work = tx.get_work(resource_id)
            if tx.actor.role != Role.OWNER and work.requester_id != tx.actor.member_id:
                raise DomainError("permission_denied", "Members may follow only their own work")
            if work.scope.get("derived_depth", 0) != 0 or any(
                work.scope.get(key) for key in ("origin_schedule_id", "origin_trigger_rule_id")
            ):
                raise DomainError("derivation_limit", "Generated work cannot trigger another work")
        elif event_type == "attachment_received":
            kind, separator, value = resource_id.partition(":")
            if not separator or not value:
                raise DomainError("invalid_input", "Attachment filter requires member/work/group")
            if kind == "member":
                if value != tx.actor.member_id:
                    if tx.actor.role != Role.OWNER:
                        raise DomainError(
                            "permission_denied", "Members cannot monitor other members"
                        )
                    present = tx.conn.execute(
                        "SELECT 1 FROM ga_members WHERE group_id=%s AND member_id=%s "
                        "AND membership_state='present'",
                        (tx.group_id, value),
                    ).fetchone()
                    if not present:
                        raise DomainError("not_found", "Attachment member is not confirmed present")
            elif kind == "work":
                EventTriggerService._resource(tx, "work_completed", value)
            elif kind != "group" or value != tx.group_id or tx.actor.role != Role.OWNER:
                raise DomainError("permission_denied", "Only Owner may monitor this entire group")
        else:
            raise DomainError(
                "unsupported_trigger", "Only fixed business and attachment events exist"
            )

    @staticmethod
    def _source(tx: UnitOfWork, event_id: str) -> GroupEvent:
        event = tx.get_event(event_id)
        if event.content_state != "active" or any(
            a.get("privacy_redacted") for a in event.attachments
        ):
            raise DomainError("source_unavailable", "Trigger evidence was removed or expired")
        return event

    @staticmethod
    def _creation_origin(tx: UnitOfWork, event: GroupEvent, work_id: str | None) -> int:
        if (
            event.actor_id != tx.actor.member_id
            or event.kind in {EventKind.SYSTEM, EventKind.UNSUPPORTED_VIDEO}
            or event.trusted_actor_origin not in _HUMAN_ORIGINS
        ):
            raise DomainError("permission_denied", "A current human request must authorize a rule")
        linked = tx.conn.execute(
            "SELECT id,scope FROM ga_work_orders WHERE group_id=%s AND request_event_id=%s",
            (tx.group_id, event.id),
        ).fetchone()
        if linked and (
            linked["scope"].get("derived_depth", 0) != 0
            or linked["scope"].get("schedule_creation_allowed") is False
        ):
            raise DomainError("derivation_limit", "Derived work cannot create event rules")
        if work_id is None:
            return 0
        if (
            linked is None
            or linked["id"] != work_id
            or linked["scope"].get("schedule_creation_allowed") is not True
        ):
            raise DomainError("derivation_limit", "Work needs explicit bounded rule authorization")
        return 1

    def create(
        self,
        actor: ActorContext,
        event_type: TriggerType,
        resource_id: str,
        goal: str,
        *,
        source_event_id: str,
        request_key: str,
        max_occurrences: int = 1,
        expires_at: datetime | None = None,
        source_work_id: str | None = None,
        now: datetime | None = None,
    ) -> TriggerRule:
        now = utc(now or datetime.now(UTC))
        end = utc(expires_at) if expires_at else now + timedelta(days=self.default_days)
        for value, name in (
            (resource_id, "resource_id"),
            (goal, "goal"),
            (request_key, "request_key"),
        ):
            _text(value, name)
        if (
            event_type not in _TYPES
            or type(max_occurrences) is not int
            or not 1 <= max_occurrences <= 10
        ):
            raise DomainError("invalid_input", "Unsupported event or occurrence limit")
        if end <= now:
            raise DomainError("invalid_input", "Trigger authorization must expire in the future")
        with self._transaction(actor, mutate=True) as tx:
            tx._writer()
            source = self._source(tx, source_event_id)
            depth = self._creation_origin(tx, source, source_work_id)
            self._resource(tx, event_type, resource_id)
            old = tx.conn.execute(
                "SELECT * FROM ga_trigger_rules WHERE group_id=%s AND request_key=%s",
                (tx.group_id, request_key),
            ).fetchone()
            if old:
                tx._controller(old, "creator_id")
                if (
                    old["creator_id"],
                    old["event_type"],
                    old["resource_id"],
                    old["goal"],
                    old["max_occurrences"],
                    old["scope"].get("request_source_id"),
                ) != (
                    actor.member_id,
                    event_type,
                    resource_id,
                    goal,
                    max_occurrences,
                    source_event_id,
                ):
                    raise DomainError(
                        "idempotency_conflict", "Request key already names another rule"
                    )
                return TriggerRule.model_validate(old)
            self._quota(tx)
            scope = {
                "request_source_id": source.id,
                "source_fingerprints": {source.id: tx.event_fingerprint(source.id)},
                "not_before": now.isoformat(),
                "overlap_policy": "skip",
            }
            row = tx.conn.execute(
                "INSERT INTO ga_trigger_rules(id,group_id,creator_id,creator_role,request_key,event_type,"
                "resource_id,goal,max_occurrences,expires_at,policy_revision,derived_depth,scope,created_at) "
                "VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING *",
                (
                    str(uuid4()),
                    tx.group_id,
                    actor.member_id,
                    actor.role.value,
                    request_key,
                    event_type,
                    resource_id,
                    goal,
                    max_occurrences,
                    end,
                    actor.policy_revision,
                    depth,
                    Jsonb(scope),
                    now,
                ),
            ).fetchone()
            assert row is not None
            tx._audit("trigger_created", row["id"], event_type=event_type, resource_id=resource_id)
            return TriggerRule.model_validate(row)

    def get(self, actor: ActorContext, rule_id: str) -> TriggerRule:
        with self._transaction(actor) as tx:
            return TriggerRule.model_validate(self._row(tx, rule_id))

    def list(self, actor: ActorContext, *, state: TriggerState | None = None) -> list[TriggerRule]:
        with self._transaction(actor) as tx:
            rows = tx.conn.execute(
                "SELECT * FROM ga_trigger_rules WHERE group_id=%s AND (%s::text IS NULL OR state=%s) "
                "ORDER BY created_at,id LIMIT 1000",
                (tx.group_id, state, state),
            ).fetchall()
            return [TriggerRule.model_validate(row) for row in rows]

    def occurrences(self, actor: ActorContext, rule_id: str) -> builtins.list[TriggerOccurrence]:
        with self._transaction(actor) as tx:
            self._row(tx, rule_id)
            rows = tx.conn.execute(
                "SELECT * FROM ga_trigger_occurrences WHERE group_id=%s AND rule_id=%s "
                "ORDER BY created_at,event_key LIMIT 1000",
                (tx.group_id, rule_id),
            ).fetchall()
            return [TriggerOccurrence.model_validate(row) for row in rows]

    def _creator(self, tx: UnitOfWork, row: Row, *, reauthorize: bool = False) -> UnitOfWork:
        current = self.resolve_actor(row["creator_id"])
        if current.group_id != tx.group_id or current.member_id != row["creator_id"]:
            raise DomainError("scope_violation", "Creator resolver returned another identity")
        if current.role not in (Role.MEMBER, Role.OWNER):
            raise DomainError("permission_denied", "Creator no longer has business authority")
        # A later promotion never expands the authorization granted by this rule.
        effective = (
            Role.OWNER
            if row["creator_role"] == Role.OWNER and current.role == Role.OWNER
            else Role.MEMBER
        )
        actor = current.model_copy(update={"role": effective})
        origin = UnitOfWork(tx.conn, actor, tx.trace_id)
        origin._check_group()
        if not reauthorize and row["policy_revision"] != current.policy_revision:
            raise DomainError("policy_changed", "Creator must explicitly reauthorize this rule")
        for source_id, expected in row["scope"]["source_fingerprints"].items():
            self._source(origin, source_id)
            if origin.event_fingerprint(source_id) != expected:
                raise DomainError(
                    "source_changed", "Rule request no longer matches its authorization"
                )
        self._resource(origin, row["event_type"], row["resource_id"])
        return origin

    @staticmethod
    def _control_children(tx: UnitOfWork, rule_id: str, *, cancel: bool) -> None:
        works = tx.conn.execute(
            "SELECT w.* FROM ga_work_orders w JOIN ga_trigger_occurrences o ON o.group_id=w.group_id "
            "AND o.work_id=w.id WHERE o.group_id=%s AND o.rule_id=%s "
            "AND w.state NOT IN ('completed','failed','cancelled') ORDER BY w.id FOR UPDATE OF w",
            (tx.group_id, rule_id),
        ).fetchall()
        for work in works:
            if cancel or work["state"] != WorkState.PAUSED:
                tx.transition_work(
                    work["id"],
                    WorkState.CANCELLED if cancel else WorkState.PAUSED,
                    expected_revision=work["revision"],
                    reason="trigger_cancelled" if cancel else "trigger_paused",
                )

    def pause(self, actor: ActorContext, rule_id: str, *, expected_revision: int) -> TriggerRule:
        return self._stop(actor, rule_id, expected_revision, cancel=False)

    def cancel(self, actor: ActorContext, rule_id: str, *, expected_revision: int) -> TriggerRule:
        return self._stop(actor, rule_id, expected_revision, cancel=True)

    def _stop(
        self, actor: ActorContext, rule_id: str, revision: int, *, cancel: bool
    ) -> TriggerRule:
        with self._transaction(actor, mutate=True) as tx:
            row = self._row(tx, rule_id)
            tx._controller(row, "creator_id")
            tx._revision(row, revision)
            target = "cancelled" if cancel else "paused"
            if row["state"] == target:
                return TriggerRule.model_validate(row)
            if row["state"] == "cancelled":
                raise DomainError("invalid_transition", "Cancelled rules are closed")
            # Ended rules may still own in-flight final occurrences: stop those too.
            self._control_children(tx, rule_id, cancel=cancel)
            updated = tx.conn.execute(
                "UPDATE ga_trigger_rules SET state=%s,revision=revision+1 WHERE group_id=%s AND id=%s RETURNING *",
                (target, tx.group_id, rule_id),
            ).fetchone()
            tx._audit("trigger_" + target, rule_id)
            return TriggerRule.model_validate(updated)

    def resume(
        self,
        actor: ActorContext,
        rule_id: str,
        *,
        expected_revision: int,
        now: datetime | None = None,
    ) -> TriggerRule:
        now = utc(now or datetime.now(UTC))
        with self._transaction(actor, mutate=True) as tx:
            row = self._row(tx, rule_id)
            tx._controller(row, "creator_id")
            tx._revision(row, expected_revision)
            if row["state"] != "paused" or now >= row["expires_at"]:
                raise DomainError("invalid_transition", "Only unexpired paused rules may resume")
            if (
                row["policy_revision"] != actor.policy_revision
                and actor.member_id != row["creator_id"]
            ):
                raise DomainError(
                    "policy_changed", "Only the creator may reauthorize a changed policy"
                )
            origin = self._creator(tx, row, reauthorize=True)
            if row["count"] < row["max_occurrences"]:
                self._quota(tx, rule_id)
            children = tx.conn.execute(
                "SELECT w.* FROM ga_work_orders w JOIN ga_trigger_occurrences o ON o.group_id=w.group_id "
                "AND o.work_id=w.id WHERE o.group_id=%s AND o.rule_id=%s AND w.state='paused' "
                "AND w.pause_reason='trigger_paused' ORDER BY w.id FOR UPDATE OF w",
                (tx.group_id, rule_id),
            ).fetchall()
            for work in children:
                # Old-policy work needs its separate work reauthorization. Never silently reset it.
                if work["policy_revision"] == origin.actor.policy_revision:
                    origin.transition_work(
                        work["id"],
                        WorkState.QUEUED,
                        expected_revision=work["revision"],
                        reason="trigger_resumed",
                    )
            scope = {**row["scope"], "not_before": now.isoformat()}
            updated = tx.conn.execute(
                "UPDATE ga_trigger_rules SET state=%s,revision=revision+1,policy_revision=%s,scope=%s "
                "WHERE group_id=%s AND id=%s RETURNING *",
                (
                    "ended" if row["count"] >= row["max_occurrences"] else "active",
                    origin.actor.policy_revision,
                    Jsonb(scope),
                    tx.group_id,
                    rule_id,
                ),
            ).fetchone()
            tx._audit("trigger_resumed", rule_id)
            return TriggerRule.model_validate(updated)

    @staticmethod
    def _service(actor: ActorContext) -> None:
        if actor.role not in (Role.OWNER, Role.MAINTAINER):
            raise DomainError("permission_denied", "Only the trusted control service polls events")

    @staticmethod
    def _attachment(event: GroupEvent, index: int) -> AttachmentHandle | None:
        if (
            event.kind not in {EventKind.IMAGE, EventKind.AUDIO, EventKind.FILE, EventKind.QUOTE}
            or event.trusted_actor_origin not in _HUMAN_ORIGINS
            or any(item.get("kind") in {"unsupported_video", "video"} for item in event.attachments)
        ):
            return None
        if not 0 <= index < len(event.attachments):
            return None
        item = event.attachments[index]
        if item.get("privacy_redacted") or item.get("kind") not in {"image", "audio", "file"}:
            return None
        try:
            handle = AttachmentHandle.model_validate(item.get("handle"))
        except ValueError:
            return None
        if (
            handle.group_id != event.group_id
            or handle.kind != item["kind"]
            or handle.availability not in {"unresolved", "pending", "available"}
            or (event.channel_account_id and handle.account_id != event.channel_account_id)
            or (event.source_epoch and handle.source_epoch != event.source_epoch)
            or (
                handle.platform_message_id is not None
                and handle.platform_message_id != event.platform_message_id
            )
            or (handle.content_type or "").strip().lower().startswith("video/")
            or PurePath((handle.filename or "").strip()).suffix.lower() in _VIDEO_EXTENSIONS
        ):
            return None
        # Canonical handles are immutable and normally unresolved. Receiving the
        # authenticated message does not prove download or successful decoding.
        # The admitted work still uses InputProcessor and isolated format validation.
        return handle

    @staticmethod
    def _matches_attachment(tx: UnitOfWork, row: Row, event: GroupEvent) -> bool:
        kind, _, value = row["resource_id"].partition(":")
        if kind == "member":
            return event.actor_id == value
        if kind == "group":
            return event.group_id == value
        work = tx.get_work(value)
        source = tx.get_event(work.request_event_id)
        if event.reply_to and event.reply_to in {source.id, source.platform_message_id}:
            return True
        return bool(
            event.reply_to
            and tx.conn.execute(
                "SELECT 1 FROM ga_deliveries WHERE group_id=%s AND work_id=%s AND state='accepted' "
                "AND platform_message_id=%s",
                (tx.group_id, value, event.reply_to),
            ).fetchone()
        )

    def _facts(
        self, tx: UnitOfWork, row: Row, now: datetime, limit: int, event_key: str | None = None
    ) -> builtins.list[Row]:
        since = datetime.fromisoformat(row["scope"]["not_before"])
        end = min(now, row["expires_at"])
        if row["event_type"] != "attachment_received":
            return tx.conn.execute(
                "SELECT e.*, 'business:' || e.id AS event_key FROM ga_trigger_events e "
                "WHERE e.group_id=%s AND e.event_type=%s AND e.resource_id=%s AND e.occurred_at>=%s "
                "AND e.occurred_at<=%s AND (%s::text IS NULL OR 'business:' || e.id=%s) "
                "AND NOT EXISTS(SELECT 1 FROM ga_trigger_occurrences o WHERE o.group_id=e.group_id "
                "AND o.rule_id=%s AND o.event_key='business:' || e.id) ORDER BY e.occurred_at,e.id LIMIT %s",
                (
                    tx.group_id,
                    row["event_type"],
                    row["resource_id"],
                    since,
                    end,
                    event_key,
                    event_key,
                    row["id"],
                    limit,
                ),
            ).fetchall()
        # Iterate canonical attachment slots, excluding already handled slots before LIMIT.
        rows = tx.conn.execute(
            "SELECT e.id AS source_event_id,e.occurred_at,(a.ordinality-1)::int AS attachment_index, "
            "'attachment:' || e.id || ':' || (a.ordinality-1) AS event_key FROM ga_events e "
            "CROSS JOIN LATERAL jsonb_array_elements(e.body->'attachments') WITH ORDINALITY a(value,ordinality) "
            "WHERE e.group_id=%s AND e.memory_state='active' AND e.occurred_at>=%s AND e.occurred_at<=%s "
            "AND e.kind IN ('image','audio','file','quote') "
            "AND (%s::text IS NULL OR 'attachment:' || e.id || ':' || (a.ordinality-1)=%s) "
            "AND NOT EXISTS(SELECT 1 FROM ga_trigger_occurrences o WHERE o.group_id=e.group_id "
            "AND o.rule_id=%s AND o.event_key='attachment:' || e.id || ':' || (a.ordinality-1)) "
            "ORDER BY e.occurred_at,e.sequence,a.ordinality LIMIT %s",
            (tx.group_id, since, end, event_key, event_key, row["id"], limit),
        ).fetchall()
        return rows

    def _fact_reason(self, tx: UnitOfWork, row: Row, fact: Row) -> str | None:
        if row["event_type"] == "attachment_received":
            event = self._source(tx, fact["source_event_id"])
            if self._attachment(event, fact["attachment_index"]) is None:
                return "unsupported_or_unavailable_attachment"
            return None if self._matches_attachment(tx, row, event) else "outside_filter"
        if row["event_type"] == "task_completed":
            task = tx.get_team_task(row["resource_id"])
            if task.status != "completed" or task.owner_id != fact["subject_member_id"]:
                return "resource_changed"
            newer = tx.conn.execute(
                "SELECT 1 FROM ga_trigger_events WHERE group_id=%s AND event_type='task_completed' "
                "AND resource_id=%s AND resource_revision>%s LIMIT 1",
                (tx.group_id, row["resource_id"], fact["resource_revision"]),
            ).fetchone()
            if newer:
                return "superseded_completion"
        else:
            work = tx.get_work(row["resource_id"])
            expected = "completed" if row["event_type"] == "work_completed" else "failed"
            # A retry/re-failure gets a fresh fact; a stale failure never replays.
            if work.state != expected or work.revision != fact["resource_revision"]:
                return "resource_changed"
        if fact.get("source_event_id"):
            event = self._source(tx, fact["source_event_id"])
            if event.kind == EventKind.UNSUPPORTED_VIDEO:
                return "unsupported_video"
            if event.trusted_actor_origin in {"authorized_schedule", "authorized_trigger"}:
                return "derivation_limit"
        return None

    @staticmethod
    def _busy(tx: UnitOfWork, row: Row) -> bool:
        return bool(
            tx.conn.execute(
                "SELECT 1 FROM ga_trigger_occurrences o JOIN ga_work_orders w ON w.group_id=o.group_id "
                "AND w.id=o.work_id WHERE o.group_id=%s AND o.rule_id=%s AND "
                "(w.state NOT IN ('completed','failed','cancelled') OR EXISTS(SELECT 1 FROM ga_deliveries d "
                "WHERE d.group_id=w.group_id AND d.work_id=w.id AND d.state IN ('unknown','dispatching'))) LIMIT 1",
                (tx.group_id, row["id"]),
            ).fetchone()
        )

    def _admit(
        self, tx: UnitOfWork, row: Row, fact: Row, now: datetime, reason: str | None
    ) -> TriggerOccurrence:
        work_id = None
        if reason is None:
            task_dependencies: dict[str, JsonValue] = {}
            completion_sources: set[str] = set()
            if row["event_type"] == "task_completed":
                task = tx.get_team_task(row["resource_id"])
                task_dependencies[task.id] = task.revision
                completion_sources.update(task.field_sources.get("status", []))
                # Migration 009 records the immutable creation source. Since
                # field provenance was introduced, the actual completion may
                # have a different human authorization; bind that evidence too.
                for identifier in completion_sources:
                    self._source(tx, identifier)
                if completion_sources:
                    from groupagent.memory.common import source_rows

                    source_rows(tx, sorted(completion_sources), shared=True, require_complete=True)
            key = "trigger:" + row["id"] + ":" + fact["event_key"]
            event = GroupEvent(
                id=str(uuid4()),
                source_key=key,
                group_id=tx.group_id,
                actor_id=tx.actor.member_id,
                kind=EventKind.SYSTEM,
                text=row["goal"],
                occurred_at=fact["occurred_at"],
                received_at=now,
                trusted_actor_origin="authorized_trigger",
                payload_ref=key,
                policy_revision=tx.actor.policy_revision,
            )
            stream = "trigger:" + row["id"]
            cursor = tx.get_cursor(stream)
            source = tx.ingest_event(
                event, stream, fact["event_key"], cursor.revision if cursor else 0
            ).event
            source_ids = {row["scope"]["request_source_id"], source.id}
            source_ids.update(completion_sources)
            if fact.get("source_event_id"):
                source_ids.add(fact["source_event_id"])
            fingerprints: dict[str, JsonValue] = {
                identifier: tx.event_fingerprint(identifier) for identifier in sorted(source_ids)
            }
            work = tx.create_work(
                row["goal"],
                source.id,
                scope={
                    "allowed_group_ids": [tx.group_id],
                    "delivery_target": "origin_group",
                    "external_account_changes": False,
                    "origin_trigger_rule_id": row["id"],
                    "origin_trigger_event_key": fact["event_key"],
                    "derived_depth": 1,
                    "schedule_creation_allowed": False,
                    "source_fingerprints": fingerprints,
                    "source_task_revisions": task_dependencies,
                    "attachment_event_ids": [fact["source_event_id"]]
                    if row["event_type"] == "attachment_received"
                    else [],
                },
            )
            work_id = work.id
            tx.record_decision(
                source.id,
                {
                    "kind": "start_work",
                    "trigger_rule_id": row["id"],
                    "result": {
                        "event_id": source.id,
                        "decision": {
                            "kind": "start_work",
                            "reason_code": "trigger_occurrence",
                            "goal": row["goal"],
                        },
                        "work_id": work.id,
                        "delivery_id": None,
                        "duplicate": False,
                    },
                },
            )
            tx.conn.execute(
                "UPDATE ga_trigger_rules SET count=count+1,revision=revision+1, "
                "state=CASE WHEN count+1>=max_occurrences THEN 'ended' ELSE state END WHERE group_id=%s AND id=%s",
                (tx.group_id, row["id"]),
            )
        occurrence = tx.conn.execute(
            "INSERT INTO ga_trigger_occurrences(group_id,rule_id,event_key,work_id,state,reason) "
            "VALUES(%s,%s,%s,%s,%s,%s) RETURNING *",
            (
                tx.group_id,
                row["id"],
                fact["event_key"],
                work_id,
                "skipped" if reason else "enqueued",
                reason,
            ),
        ).fetchone()
        tx._audit(
            "trigger_occurrence",
            row["id"],
            event_key=fact["event_key"],
            work_id=work_id,
            reason=reason,
        )
        return TriggerOccurrence.model_validate(occurrence)

    def notify(
        self,
        service_actor: ActorContext,
        event_type: TriggerType,
        resource_id: str,
        *,
        event_key: str,
        now: datetime | None = None,
    ) -> builtins.list[TriggerOccurrence]:
        _text(event_key, "event_key")
        if event_type not in _TYPES:
            raise DomainError("unsupported_trigger", "Unknown event type")
        return self._poll(
            service_actor, utc(now or datetime.now(UTC)), 100, event_type, resource_id, event_key
        )

    def poll(
        self, service_actor: ActorContext, now: datetime | None = None, *, limit: int = 100
    ) -> builtins.list[TriggerOccurrence]:
        if type(limit) is not int or not 1 <= limit <= 1000:
            raise DomainError("invalid_input", "Poll limit must be 1-1000")
        return self._poll(service_actor, utc(now or datetime.now(UTC)), limit, None, None, None)

    def _poll(
        self,
        service_actor: ActorContext,
        now: datetime,
        limit: int,
        event_type: str | None,
        resource_id: str | None,
        event_key: str | None,
    ) -> builtins.list[TriggerOccurrence]:
        self._service(service_actor)
        changed: builtins.list[TriggerOccurrence] = []
        with self._transaction(service_actor, mutate=True) as tx:
            rules = tx.conn.execute(
                "SELECT * FROM ga_trigger_rules WHERE group_id=%s AND state IN ('active','paused') "
                "AND (%s::text IS NULL OR event_type=%s) AND (%s::text IS NULL OR resource_id=%s) "
                "ORDER BY created_at,id FOR UPDATE",
                (tx.group_id, event_type, event_type, resource_id, resource_id),
            ).fetchall()
            for row in rules:
                if now >= row["expires_at"] or (
                    row["state"] != "paused" and row["count"] >= row["max_occurrences"]
                ):
                    tx.conn.execute(
                        "UPDATE ga_trigger_rules SET state='ended',revision=revision+1 WHERE group_id=%s AND id=%s",
                        (tx.group_id, row["id"]),
                    )
                    continue
                if row["state"] == "paused" or len(changed) >= limit:
                    continue
                try:
                    with tx.conn.transaction():
                        origin = self._creator(tx, row)
                        facts = self._facts(origin, row, now, limit - len(changed), event_key)
                        pending: builtins.list[TriggerOccurrence] = []
                        for fact in facts:
                            if row["count"] >= row["max_occurrences"]:
                                break
                            reason = self._fact_reason(origin, row, fact)
                            if reason is None and self._busy(origin, row):
                                reason = "overlap_busy"
                            pending.append(self._admit(origin, row, fact, now, reason))
                            if reason is None:
                                row["count"] += 1
                    changed.extend(pending)
                except DomainError as error:
                    # No creator escalation when policy/source/membership becomes stale.
                    tx.conn.execute(
                        "UPDATE ga_trigger_rules SET state='paused',revision=revision+1 WHERE group_id=%s AND id=%s",
                        (tx.group_id, row["id"]),
                    )
                    tx._audit("trigger_paused", row["id"], reason=error.code)
        return changed
