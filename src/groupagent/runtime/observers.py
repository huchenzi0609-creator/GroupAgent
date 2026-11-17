"""Feed durable project observations into the same ReAct kernel as a conversation."""

from __future__ import annotations

import hashlib
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import uuid4
from zoneinfo import ZoneInfo

from psycopg.types.json import Jsonb

from groupagent.config import AppConfig
from groupagent.conversation.quiet import is_quiet
from groupagent.coordinator import Decision
from groupagent.domain import ActorContext, DeliveryIntent, DomainError, GroupEvent
from groupagent.memory.common import memory_lock, source_rows
from groupagent.persistence import PostgresStore, UnitOfWork
from groupagent.security import redact_credentials

from .work_facts import WorkFacts

Observer = Callable[[GroupEvent, dict[str, Any], dict[str, Any]], Awaitable[Decision]]
OBSERVER_PREFIX = "react-observer:"

_CANDIDATES = """
WITH candidates AS (
  SELECT 'transition:'||e.id::text AS key,
         CASE WHEN e.event_type='task_completed' THEN 'task' ELSE 'work' END AS resource_kind,
         e.resource_id,e.resource_revision,e.source_event_id,
         e.event_type AS signal,e.occurred_at AS observed_at
  FROM ga_trigger_events e
  WHERE e.group_id=%(group)s AND e.source_event_id IS NOT NULL
    AND e.occurred_at>=%(cutoff)s
    AND NOT (e.event_type='work_completed' AND EXISTS (
        SELECT 1 FROM ga_work_validations v WHERE v.group_id=e.group_id
        AND v.work_id=e.resource_id AND v.check_key='silent_result' AND v.passed))
  UNION ALL
  SELECT 'deadline:'||t.id||':'||t.revision::text, 'task', t.id, t.revision,
         t.source_event_id, 'deadline_approaching', t.due_at
  FROM ga_team_tasks t
  WHERE t.group_id=%(group)s AND t.content_state='active'
    AND t.status IN ('open','in_progress') AND t.source_event_id IS NOT NULL
    AND t.due_at BETWEEN %(now)s AND %(horizon)s
    AND (t.updated_at>=%(start)s OR t.due_at >= %(start)s + interval '2 hours')
)
SELECT c.*,COALESCE(o.failures,0) AS failures
FROM candidates c JOIN ga_events e ON e.group_id=%(group)s AND e.id=c.source_event_id
LEFT JOIN ga_react_observations o ON o.group_id=%(group)s AND o.observation_key=c.key
WHERE e.memory_state='active'
  AND (o.observation_key IS NULL OR o.state='deferred' AND o.next_attempt_at<=%(now)s
    OR o.state='evaluating' AND o.lease_until<=%(now)s)
ORDER BY c.observed_at,c.key LIMIT 1
"""


def _current_revision(tx: UnitOfWork, kind: str, identifier: str) -> int:
    if kind == "work":
        work = tx.get_work(identifier)
        if work.scope.get("privacy_redacted"):
            raise DomainError("source_unavailable", "Observation work was withdrawn")
        return work.revision
    if kind == "task":
        task = tx.get_team_task(identifier)
        if task.content_state != "active":
            raise DomainError("source_unavailable", "Observation task was withdrawn")
        return task.revision
    raise DomainError("invalid_input", "Unknown observation resource")


def _check_snapshot(tx: UnitOfWork, snapshot: dict[str, Any]) -> None:
    if (
        _current_revision(tx, snapshot["resource_kind"], snapshot["resource_id"])
        != snapshot["resource_revision"]
    ):
        raise DomainError("source_unavailable", "The observation has been superseded")
    fingerprints = snapshot["source_fingerprints"]
    if not fingerprints:
        raise DomainError("source_unavailable", "Observation has no source evidence")
    source_rows(tx, list(fingerprints), shared=True, require_complete=True)
    if any(
        tx.event_fingerprint(identifier) != digest for identifier, digest in fingerprints.items()
    ):
        raise DomainError("source_unavailable", "Observation evidence changed")


def guard_observer_delivery(tx: UnitOfWork, intent: DeliveryIntent) -> None:
    if not intent.idempotency_key.startswith(OBSERVER_PREFIX):
        return
    memory_lock(tx)
    row = tx.conn.execute(
        "SELECT * FROM ga_react_observations WHERE group_id=%s AND delivery_id=%s AND state='published'",
        (tx.group_id, intent.id),
    ).fetchone()
    if (
        row is None
        or intent.request_event_id is not None
        or intent.work_id is not None
        or intent.idempotency_key != OBSERVER_PREFIX + row["observation_key"]
    ):
        raise DomainError("source_unavailable", "The observation does not authorize this delivery")
    _check_snapshot(tx, row)


@dataclass(frozen=True)
class Observation:
    row: dict[str, Any]
    source: GroupEvent | None
    facts: dict[str, Any]
    token: str


class ProactiveObserver:
    """Evaluate only new witnessed facts. Action selection belongs to the model."""

    def __init__(
        self,
        store: PostgresStore,
        owner: ActorContext,
        config: AppConfig,
        observe: Observer,
        *,
        fence: Callable[[UnitOfWork], None],
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self.store, self.owner, self.config = store, owner, config
        self.observe, self.fence, self.clock = observe, fence, clock
        self.work_facts = WorkFacts(store)
        self.local_zone = ZoneInfo(config.group.time_zone)
        self._opened_at = clock()
        self._started_at: datetime | None = None

    def _start_observing(self) -> datetime:
        if self._started_at is not None:
            return self._started_at
        with self.store.transaction(self.owner) as tx:
            self.fence(tx)
            memory_lock(tx)
            row = tx.conn.execute(
                "SELECT (details->>'activated_at')::timestamptz AS activated_at FROM ga_audit_events "
                "WHERE group_id=%s AND action='react_observer_enabled' AND policy_revision=%s ORDER BY id LIMIT 1",
                (tx.group_id, self.owner.policy_revision),
            ).fetchone()
            if row is not None:
                started_at = row["activated_at"]
            else:
                # Starting shadow mode should not replay all previously
                # delivered work to the model. New transitions remain durable
                # and can still be evaluated after a controller restart.
                started_at = self._opened_at
                tx._audit(
                    "react_observer_enabled",
                    tx.group_id,
                    activated_at=started_at.isoformat(),
                )
        self._started_at = started_at
        return started_at

    def _fact(self, tx: UnitOfWork, row: dict[str, Any]) -> tuple[dict[str, Any], set[str]]:
        identifier = row["resource_id"]
        if row["resource_kind"] == "work":
            reply = self.work_facts.lookup_in_tx(tx, ids=(identifier,), limit=1)
            if reply["status"] != "matched":
                raise DomainError("source_unavailable", "Observation work is unavailable publicly")
            work = reply["facts"][0]
            return {"work": work}, self.work_facts.source_dependencies_in_tx(tx, (identifier,))
        task = tx.get_team_task(identifier)
        fields = ("id", "title", "owner_id", "status", "progress", "due_at", "revision")
        data = task.model_dump(mode="json", include=set(fields))
        data["title"] = redact_credentials(task.title)
        data["progress"] = redact_credentials(task.progress)
        if task.due_at is not None:
            data["due_at_local"] = task.due_at.astimezone(self.local_zone).isoformat()
        return {"task": data}, set(task.source_event_ids)

    def _claim(self) -> Observation | None:
        now = self.clock()
        started_at = self._start_observing()
        with self.store.transaction(self.owner) as tx:
            self.fence(tx)
            row = tx.conn.execute(
                _CANDIDATES,
                {
                    "group": tx.group_id,
                    "now": now,
                    "start": started_at,
                    "cutoff": max(
                        started_at, now - timedelta(days=self.config.retention.raw_message_days)
                    ),
                    "horizon": now + timedelta(hours=2),
                },
            ).fetchone()
            if row is None:
                return None
            token = uuid4().hex
            claimed = tx.conn.execute(
                "INSERT INTO ga_react_observations "
                "(group_id,observation_key,resource_kind,resource_id,resource_revision,source_event_id,"
                "state,lease_token,lease_until,next_attempt_at) VALUES(%s,%s,%s,%s,%s,%s,'evaluating',%s,%s,%s) "
                "ON CONFLICT (group_id,observation_key) DO UPDATE SET state='evaluating',"
                "lease_token=EXCLUDED.lease_token,lease_until=EXCLUDED.lease_until,"
                "updated_at=clock_timestamp() "
                "WHERE (ga_react_observations.state='deferred' AND ga_react_observations.next_attempt_at<=%s) "
                "OR (ga_react_observations.state='evaluating' AND ga_react_observations.lease_until<=%s) "
                "RETURNING observation_key",
                (
                    tx.group_id,
                    row["key"],
                    row["resource_kind"],
                    row["resource_id"],
                    row["resource_revision"],
                    row["source_event_id"],
                    token,
                    now + timedelta(minutes=3),
                    now,
                    now,
                    now,
                ),
            ).fetchone()
            if claimed is None:
                return None
            memory_lock(tx)
            try:
                facts, dependencies = self._fact(tx, row)
                identifiers = sorted({row["source_event_id"], *dependencies})
                source_rows(tx, identifiers, shared=True, require_complete=True)
                row["source_fingerprints"] = {
                    identifier: tx.event_fingerprint(identifier) for identifier in identifiers
                }
                _check_snapshot(tx, row)
            except DomainError as error:
                if error.code not in {"source_unavailable", "scope_violation", "not_found"}:
                    raise
                return Observation(row, None, {}, token)
            return Observation(row, tx.get_event(row["source_event_id"]), facts, token)

    def _ok_to_speak(self, tx: UnitOfWork, item: Observation, now: datetime) -> bool:
        source = item.source
        assert source is not None
        policies = tx.conn.execute(
            "SELECT * FROM ga_conversation_policy WHERE group_id=%s AND subject_id IN ('*',%s)",
            (tx.group_id, source.actor_id),
        ).fetchall()
        group = self.config.group
        mode = next(
            (row["mode"] for row in policies if row["subject_id"] == "*"), group.participation_mode
        )
        if mode != "natural" or is_quiet(policies, now, group.time_zone, group.quiet_hours):
            return False
        count = tx.conn.execute(
            "SELECT count(*) AS n FROM ga_deliveries WHERE group_id=%s "
            "AND (left(idempotency_key,%s)=%s OR EXISTS (SELECT 1 FROM ga_participation_log p "
            "WHERE p.group_id=ga_deliveries.group_id AND p.event_id=ga_deliveries.request_event_id "
            "AND p.proactive)) AND created_at>%s",
            (tx.group_id, len(OBSERVER_PREFIX), OBSERVER_PREFIX, now - timedelta(minutes=10)),
        ).fetchone()
        return bool(
            count and count["n"] < self.config.conversation.proactive_messages_per_10_minutes
        )

    def _settle(
        self, item: Observation, decision: Decision | None, *, error: DomainError | None = None
    ) -> str:
        now = self.clock()
        with self.store.transaction(self.owner) as tx:
            self.fence(tx)
            memory_lock(tx)
            current = tx.conn.execute(
                "SELECT state,lease_token,lease_until FROM ga_react_observations "
                "WHERE group_id=%s AND observation_key=%s FOR UPDATE",
                (tx.group_id, item.row["key"]),
            ).fetchone()
            if (
                current is None
                or current["state"] != "evaluating"
                or current["lease_token"] != item.token
                or current["lease_until"] <= now
            ):
                return "superseded"
            state, reason, proposal_hash, delivery_id = "silent", "no_update", None, None
            failures = item.row["failures"]
            wake_at = now
            if item.source is None:
                reason = "source_unavailable"
            elif error is not None:
                reason = error.code
                if error.retryable:
                    state = "deferred"
                    failures += 1
                    wake_at += timedelta(seconds=min(300, 5 * 2 ** min(failures - 1, 6)))
            else:
                try:
                    _check_snapshot(tx, item.row)
                except DomainError as revoked:
                    reason = revoked.code
                else:
                    text = redact_credentials(decision.response or "").strip() if decision else ""
                    if (
                        decision
                        and decision.kind == "reply"
                        and decision.directness == "proactive"
                        and text
                    ):
                        proposal_hash = hashlib.sha256(text.encode()).hexdigest()
                        mode = self.config.conversation.proactive_mode
                        if mode == "shadow":
                            state, reason = "shadow", "proactive_shadow"
                        elif mode == "active" and self._ok_to_speak(tx, item, now):
                            # The observation links this outbox item to its exact
                            # source set and current resource revision.
                            delivery = tx.enqueue_delivery(
                                None,
                                "text:" + text,
                                proposal_hash,
                                OBSERVER_PREFIX + item.row["key"],
                            )
                            state, reason, delivery_id = "published", "model_proposed", delivery.id
                        else:
                            reason = "participation_policy"
            tx.conn.execute(
                "UPDATE ga_react_observations SET state=%s,reason_code=%s,next_attempt_at=%s,"
                "failures=%s,source_fingerprints=%s,proposal_sha256=%s,delivery_id=%s,"
                "lease_token=NULL,lease_until=NULL,updated_at=clock_timestamp() "
                "WHERE group_id=%s AND observation_key=%s",
                (
                    state,
                    reason,
                    wake_at,
                    failures,
                    Jsonb(item.row.get("source_fingerprints", {})),
                    proposal_hash,
                    delivery_id,
                    tx.group_id,
                    item.row["key"],
                ),
            )
            return state

    async def tick(self) -> str | None:
        if self.config.conversation.proactive_mode == "off":
            return None
        item = self._claim()
        if item is None:
            return None
        if item.source is None:
            return self._settle(item, None)

        async def read_work_facts(*_args, **_kwargs):
            # This observation may read its current work, without discovering
            # unrelated member activity or gaining authority to mutate it.
            if item.row["resource_kind"] != "work":
                return {"status": "unavailable", "facts": []}
            with self.store.transaction(self.owner) as tx:
                self.fence(tx)
                _check_snapshot(tx, item.row)
                return self.work_facts.lookup_in_tx(tx, ids=(item.row["resource_id"],), limit=1)

        payload = {
            "signal": item.row["signal"],
            "observed_at": item.row["observed_at"].astimezone(self.local_zone).isoformat(),
            "now": self.clock().astimezone(self.local_zone).isoformat(),
            **item.facts,
        }
        try:
            decision = await self.observe(
                item.source, payload, {"_read_work_facts": read_work_facts}
            )
        except DomainError as error:
            return self._settle(item, None, error=error)
        except TimeoutError:
            return self._settle(
                item,
                None,
                error=DomainError("provider_timeout", "Observation timed out", retryable=True),
            )
        return self._settle(item, decision)
