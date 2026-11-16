"""Shared public work metadata, without checkpoints, execution or mutation authority."""

from __future__ import annotations

import re
from collections import Counter
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from typing import Any

from psycopg import IsolationLevel

from groupagent.domain import ActorContext, DomainError, WorkOrder, WorkState
from groupagent.memory.causality import after_boundary_sql
from groupagent.persistence import PostgresStore, UnitOfWork

from .recovery import automatic_recovery_pending
from .work_status import work_status_detail

_LIVE = ("queued", "running", "waiting_input", "waiting_external", "validating", "delivering")
_CLOSED = ("completed", "failed", "cancelled")
_INDEX_COLUMNS = (
    "w.id,w.group_id,w.requester_id,w.request_event_id,w.goal,w.state,w.revision,"
    "w.goal_revision,w.created_at,w.updated_at,w.scope->>'topic_id' AS topic_id"
)
_COLUMNS = (
    "w.id,w.group_id,w.requester_id,w.request_event_id,w.goal,w.state,w.revision,"
    "w.goal_revision,w.policy_revision,w.pause_reason,w.created_at,w.updated_at,"
    "w.input_versions,w.scope,w.required_checks,w.acceptance_criteria"
    ",origin.body->'source_event_ids' AS origin_source_event_ids"
)


def _cutoff(alias: str) -> str:
    return after_boundary_sql(alias).replace("%s", "%(through_sequence)s")


def _privacy_sets(tx: UnitOfWork) -> tuple[set[str], set[str]]:
    rows = tx.conn.execute(
        "SELECT t.id,r.event_id FROM ga_memory_topics t LEFT JOIN ga_topic_events r "
        "ON r.group_id=t.group_id AND r.topic_id=t.id "
        "WHERE t.group_id=%s AND t.visibility='participants'",
        (tx.group_id,),
    ).fetchall()
    return {row["event_id"] for row in rows if row["event_id"]}, {row["id"] for row in rows}


def _validate(
    *, ids: Sequence[str], title: str | None, states: Sequence[str], limit: int, offset: int
) -> tuple[tuple[str, ...], tuple[str, ...]]:
    if (
        type(limit) is not int
        or not 1 <= limit <= 1000
        or type(offset) is not int
        or not 0 <= offset <= 1_000_000
        or isinstance(ids, str)
        or len(ids) > 100
        or any(not isinstance(value, str) or not 1 <= len(value) <= 128 for value in ids)
        or title is not None
        and (not isinstance(title, str) or len(title) > 1000)
        or isinstance(states, str)
        or any(value not in {state.value for state in WorkState} for value in states)
    ):
        raise DomainError(
            "invalid_input", "Work lookup requires bounded IDs, title, states and page"
        )
    return tuple(dict.fromkeys(ids)), tuple(dict.fromkeys(states))


def _visible_rows(
    tx: UnitOfWork,
    *,
    ids: Sequence[str] = (),
    exclude_ids: Sequence[str] = (),
    title: str | None = None,
    states: Sequence[str] = (),
    requester_id: str | None = None,
    limit: int = 20,
    offset: int = 0,
    through_sequence: int | None = None,
    private_events: set[str] | None = None,
    private_topics: set[str] | None = None,
    recovery_only: bool = False,
    newest_created: bool = False,
    active_first: bool = False,
    index_only: bool = False,
) -> tuple[list[dict[str, Any]], int]:
    ids, states = _validate(ids=ids, title=title, states=states, limit=limit, offset=offset)
    exclude_ids, _ = _validate(ids=exclude_ids, title=None, states=(), limit=limit, offset=offset)
    if requester_id is not None and (not isinstance(requester_id, str) or not requester_id.strip()):
        raise DomainError("invalid_input", "Requester filter must be a member identifier")
    if through_sequence is not None and (type(through_sequence) is not int or through_sequence < 0):
        raise DomainError("invalid_input", "Work visibility boundary must be nonnegative")
    if private_events is None or private_topics is None:
        current_events, current_topics = _privacy_sets(tx)
        private_events = current_events if private_events is None else private_events
        private_topics = current_topics if private_topics is None else private_topics
    patterns = [
        "%" + word.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_") + "%"
        for word in (title or "").split()
    ]
    parameters = {
        "group_id": tx.group_id,
        "through_sequence": through_sequence,
        "private_events": sorted(private_events),
        "private_topics": sorted(private_topics),
        "ids": list(ids) or None,
        "exclude_ids": list(exclude_ids),
        "states": list(states) or None,
        "requester_id": requester_id,
        "patterns": patterns,
        "limit": limit,
        "offset": offset,
        "recovery_only": recovery_only,
    }
    # This is the one shared work-index policy. It retains the old context index's
    # public topic/source exclusions and also denies withdrawn secondary sources.
    # No artifact payload, transitive text hydration or checkpoint is selected.
    condition = (
        "FROM ga_work_orders w JOIN ga_events e "
        "ON e.group_id=w.group_id AND e.id=w.request_event_id "
        "LEFT JOIN ga_decisions origin "
        "ON origin.group_id=w.group_id AND origin.event_id=w.request_event_id "
        "WHERE w.group_id=%(group_id)s AND e.memory_state='active' "
        "AND (%(through_sequence)s::bigint IS NULL OR NOT " + _cutoff("e") + ") "
        "AND COALESCE(w.scope->>'privacy_redacted','false')<>'true' "
        "AND NOT COALESCE(w.scope->>'topic_id'=ANY(%(private_topics)s::text[]),false) "
        "AND NOT EXISTS (SELECT 1 FROM ("
        "SELECT w.request_event_id AS id UNION SELECT w.scope->>'request_source_id' "
        "WHERE jsonb_typeof(w.scope->'request_source_id')='string' "
        "UNION SELECT jsonb_object_keys(CASE WHEN jsonb_typeof(w.scope->'source_fingerprints')='object' "
        "THEN w.scope->'source_fingerprints' ELSE '{}'::jsonb END) "
        "UNION SELECT jsonb_array_elements_text(CASE WHEN jsonb_typeof(w.scope->'source_event_ids')='array' "
        "THEN w.scope->'source_event_ids' ELSE '[]'::jsonb END) "
        "UNION SELECT jsonb_array_elements_text(CASE WHEN jsonb_typeof(origin.body->'source_event_ids')='array' "
        "THEN origin.body->'source_event_ids' ELSE '[]'::jsonb END)) src "
        "LEFT JOIN ga_events s ON s.group_id=w.group_id AND s.id=src.id "
        "WHERE s.id IS NULL OR src.id=ANY(%(private_events)s::text[]) "
        "OR s.memory_state NOT IN ('active','expired') "
        "OR (s.memory_state='expired' AND NOT COALESCE(w.scope->'retained_fact_source_ids' ? src.id,false)) "
        "OR (%(through_sequence)s::bigint IS NOT NULL AND " + _cutoff("s") + ")) "
        "AND (%(ids)s::text[] IS NULL OR w.id=ANY(%(ids)s::text[])) "
        "AND NOT (w.id=ANY(%(exclude_ids)s::text[])) "
        "AND (%(states)s::text[] IS NULL OR w.state=ANY(%(states)s::text[])) "
        "AND (%(requester_id)s::text IS NULL OR w.requester_id=%(requester_id)s) "
        "AND NOT EXISTS (SELECT 1 FROM unnest(%(patterns)s::text[]) term(pattern) "
        "WHERE w.goal NOT ILIKE term.pattern) "
        "AND (NOT %(recovery_only)s OR jsonb_typeof(w.scope->'_runtime_recovery')='object')"
    )
    order = "created_at DESC,id DESC" if newest_created else "updated_at DESC,id DESC"
    if active_first:
        order = (
            "CASE WHEN state IN ('running','validating','delivering') THEN 0 "
            "WHEN state='queued' THEN 1 ELSE 2 END," + order
        )
    rows = tx.conn.execute(
        "WITH visible AS (SELECT "
        + (_INDEX_COLUMNS if index_only else _COLUMNS)
        + " "
        + condition
        + "), "
        "selected AS (SELECT * FROM visible ORDER BY "
        + order
        + " LIMIT %(limit)s OFFSET %(offset)s) "
        "SELECT selected.*,counts.matched_count FROM (SELECT count(*) AS matched_count FROM visible) counts "
        "LEFT JOIN selected ON true ORDER BY " + order + " NULLS LAST",
        parameters,
    ).fetchall()
    count = int(rows[0]["matched_count"]) if rows else 0
    return [dict(row) for row in rows if row["id"] is not None], count


def visible_work_index_in_tx(
    tx: UnitOfWork,
    *,
    private_events: set[str] | None = None,
    private_topics: set[str] | None = None,
    through_sequence: int | None = None,
    limit: int = 1000,
) -> tuple[list[dict[str, Any]], bool]:
    """Shared internal index; callers still hydrate evidence before mutations."""
    rows, count = _visible_rows(
        tx,
        limit=limit,
        through_sequence=through_sequence,
        private_events=private_events,
        private_topics=private_topics,
        newest_created=True,
        index_only=True,
    )
    keys = (
        "id",
        "group_id",
        "requester_id",
        "request_event_id",
        "goal",
        "state",
        "revision",
        "goal_revision",
        "created_at",
        "updated_at",
    )
    result = []
    for row in rows:
        result.append(
            {
                **{key: row[key] for key in keys},
                "topic_id": row["topic_id"],
                "_requested_at": row["created_at"].isoformat(),
            }
        )
    return result, count <= limit


class WorkFacts:
    """Current metadata reads share authorization, not execution ownership."""

    def __init__(self, store: PostgresStore):
        self.store = store

    @staticmethod
    def source_dependencies_in_tx(tx: UnitOfWork, ids: Sequence[str]) -> set[str]:
        """Metadata revocation dependencies for displayed public facts only.

        These identifiers grant no source-read authority and are not hydrated
        evidence or model inputs. Keep them on the host's private dependency
        path so later forgetting/recall can revoke replies derived from facts.
        """
        if not ids:
            return set()
        if isinstance(ids, str) or len(ids) > 1000:
            raise DomainError("invalid_input", "Fact dependency lookup requires bounded work IDs")
        rows = []
        for offset in range(0, len(ids), 100):
            selected, _ = _visible_rows(tx, ids=ids[offset : offset + 100], limit=100)
            rows.extend(selected)
        result: set[str] = set()
        for row in rows:
            result.add(row["request_event_id"])
            scope = row["scope"]
            for values in (scope.get("source_event_ids"), row["origin_source_event_ids"]):
                if isinstance(values, list):
                    result.update(value for value in values if isinstance(value, str) and value)
            fingerprints = scope.get("source_fingerprints")
            if isinstance(fingerprints, dict):
                result.update(value for value in fingerprints if isinstance(value, str) and value)
            request_source = scope.get("request_source_id")
            if isinstance(request_source, str) and request_source:
                result.add(request_source)
        return result

    @contextmanager
    def _read(self, actor: ActorContext) -> Iterator[UnitOfWork]:
        # Avoid PostgresStore.transaction's rejection-audit mutation. Even a
        # rejected fact read cannot create an audit row or start execution.
        with self.store.connect() as connection:
            connection.isolation_level = IsolationLevel.REPEATABLE_READ
            tx = UnitOfWork(connection, actor, "work-facts-read")
            tx._check_group()
            yield tx

    def overview(self, actor: ActorContext, **kwargs: Any) -> dict[str, Any]:
        with self._read(actor) as tx:
            return self.overview_in_tx(tx, **kwargs)

    def lookup(self, actor: ActorContext, **kwargs: Any) -> dict[str, Any]:
        with self._read(actor) as tx:
            return self.lookup_in_tx(tx, **kwargs)

    @staticmethod
    def _fact(tx: UnitOfWork, row: dict[str, Any]) -> dict[str, Any]:
        work = WorkOrder.model_validate(
            {key: row[key] for key in WorkOrder.model_fields if key in row}
        )
        events = tx.conn.execute(
            "SELECT e.kind,e.created_at,e.body#>>'{payload,name}' AS requested_name,"
            "e.body#>>'{payload,receipt,tool_name}' AS finished_name,"
            "e.body#>'{payload,receipt,success}' AS success,"
            "e.body#>>'{payload,receipt,error_code}' AS error_code "
            "FROM ga_runtime_events e JOIN ga_execution_attempts a "
            "ON a.group_id=e.group_id AND a.id=e.attempt_id AND a.work_id=e.work_id "
            "WHERE e.group_id=%s AND e.work_id=%s AND a.goal_revision=%s "
            "AND e.kind IN ('tool_requested','tool_finished') "
            "ORDER BY e.created_at DESC,e.sequence DESC LIMIT 5",
            (tx.group_id, work.id, work.goal_revision),
        ).fetchall()
        activity = []
        for event in events[:4]:
            name = (
                event["requested_name"]
                if event["kind"] == "tool_requested"
                else event["finished_name"]
            )
            if (
                not isinstance(name, str)
                or re.fullmatch(r"[A-Za-z_][A-Za-z0-9_-]{0,63}", name) is None
            ):
                continue
            item = {
                "tool_name": name,
                "observed_at": event["created_at"].isoformat(),
                "status": "requested"
                if event["kind"] == "tool_requested"
                else "succeeded"
                if event["success"] is True
                else "failed"
                if event["success"] is False
                else "unknown",
            }
            if isinstance(event["error_code"], str) and re.fullmatch(
                r"[a-z][a-z0-9_]{0,79}", event["error_code"]
            ):
                item["error_code"] = event["error_code"]
            activity.append(item)
        outputs = tx.conn.execute(
            "SELECT count(*) AS candidate_count,count(*) FILTER (WHERE "
            "COALESCE(o.validation,a.validation)->>'passed'='true') AS validated_count "
            "FROM ga_work_outputs o JOIN ga_artifact_catalog a "
            "ON a.group_id=o.group_id AND a.version_id=o.version_id "
            "WHERE o.group_id=%s AND o.work_id=%s AND o.goal_revision=%s AND a.available "
            "AND o.purpose IN ('candidate','result') "
            "AND (a.expires_at IS NULL OR a.expires_at>clock_timestamp()) "
            "AND NOT EXISTS (SELECT 1 FROM unnest(a.source_event_ids) src(id) "
            "LEFT JOIN ga_events s ON s.group_id=a.group_id AND s.id=src.id "
            "WHERE s.id IS NULL OR s.memory_state NOT IN ('active','expired') "
            "OR EXISTS(SELECT 1 FROM ga_topic_events r JOIN ga_memory_topics t "
            "ON t.group_id=r.group_id AND t.id=r.topic_id "
            "WHERE r.group_id=a.group_id AND r.event_id=src.id AND t.visibility='participants'))",
            (tx.group_id, work.id, work.goal_revision),
        ).fetchone()
        states = tx.delivery_states(work.id, goal_revision=work.goal_revision)
        counts = Counter(state.value for state in states)
        accepted_outputs = tx.conn.execute(
            "SELECT count(*) FILTER (WHERE d.state='accepted' AND a.version_id IS NOT NULL) "
            "AS artifact_accepted,count(*) FILTER (WHERE d.state='accepted' "
            "AND a.format IN ('png','jpg','jpeg','webp','gif')) AS image_accepted "
            "FROM ga_deliveries d LEFT JOIN ga_artifact_catalog a "
            "ON a.group_id=d.group_id AND d.payload_ref='artifact:'||a.version_id "
            "WHERE d.group_id=%s AND d.work_id=%s AND d.goal_revision=%s",
            (tx.group_id, work.id, work.goal_revision),
        ).fetchone()
        phase = (
            "none"
            if not states
            else "uncertain"
            if counts["unknown"]
            else "rejected"
            if counts["rejected"]
            else "accepted"
            if counts["accepted"] == len(states)
            else "partially_accepted"
            if counts["accepted"]
            else "in_progress"
            if counts["dispatching"]
            else "pending"
        )
        detail = work_status_detail(work)
        if work.state not in {WorkState.PAUSED, WorkState.FAILED, WorkState.WAITING_INPUT}:
            messages = {
                "queued": "任务已在后台排队。",
                "running": "任务正在后台执行。",
                "waiting_external": "任务正在等待外部结果。",
                "validating": "候选结果正在验证。",
                "delivering": "任务结果正在交付，接受情况以原生回执为准。",
                "completed": "任务已完成。",
                "cancelled": "任务已取消。",
            }
            detail = {
                "code": work.state.value,
                "phase": "delivery" if work.state == WorkState.DELIVERING else "execution",
                "message": messages[work.state.value],
            }
        return {
            **{
                key: getattr(work, key)
                for key in (
                    "id",
                    "group_id",
                    "requester_id",
                    "request_event_id",
                    "goal",
                    "revision",
                    "goal_revision",
                    "policy_revision",
                    "pause_reason",
                )
            },
            "state": work.state.value,
            "created_at": work.created_at.isoformat(),
            "updated_at": work.updated_at.isoformat(),
            "status_detail": detail,
            "presentation": {
                "phase": detail["phase"],
                "background_processing": automatic_recovery_pending(work),
            },
            "activity": {
                "recent_tools": activity,
                "complete": len(events) <= 4 and len(activity) == len(events),
            },
            "outputs": dict(outputs or {"candidate_count": 0, "validated_count": 0}),
            "delivery": {
                "total": len(states),
                **{
                    state: counts[state]
                    for state in ("accepted", "pending", "dispatching", "unknown", "rejected")
                },
                "phase": phase,
                **dict(accepted_outputs or {"artifact_accepted": 0, "image_accepted": 0}),
            },
        }

    @staticmethod
    def _snapshot(
        tx: UnitOfWork, scope: dict[str, Any], rows: list[dict[str, Any]], complete: bool
    ) -> dict[str, Any]:
        facts = [WorkFacts._fact(tx, row) for row in rows]
        clock = tx.conn.execute("SELECT clock_timestamp() AS observed_at").fetchone()
        assert clock is not None
        return {
            "observed_at": clock["observed_at"].isoformat(),
            "complete": complete,
            "query_scope": scope,
            "facts": facts,
        }

    def lookup_in_tx(
        self,
        tx: UnitOfWork,
        *,
        ids: Sequence[str] = (),
        exclude_ids: Sequence[str] = (),
        title: str | None = None,
        states: Sequence[str] = (),
        requester_id: str | None = None,
        limit: int = 20,
        offset: int = 0,
        through_sequence: int | None = None,
    ) -> dict[str, Any]:
        if type(limit) is not int or not 1 <= limit <= 100:
            raise DomainError("invalid_input", "Fact lookup pages contain at most 100 works")
        rows, count = _visible_rows(
            tx,
            ids=ids,
            exclude_ids=exclude_ids,
            title=title,
            states=states,
            requester_id=requester_id,
            limit=limit,
            offset=offset,
            through_sequence=through_sequence,
        )
        complete = offset == 0 and len(rows) == count
        scope = {
            "kind": "lookup",
            "group_id": tx.group_id,
            "visibility": "public",
            "through_sequence": through_sequence,
            "ids": list(ids),
            "exclude_ids": list(exclude_ids),
            "title": title,
            "states": list(states),
            "requester_id": requester_id,
            "limit": limit,
            "offset": offset,
            "matched_count": count,
            "returned_count": len(rows),
            "has_more": offset + len(rows) < count,
        }
        snapshot = self._snapshot(tx, scope, rows, complete)
        requested_ids = set(ids) - set(exclude_ids)
        snapshot["status"] = (
            "unavailable"
            if requested_ids and count == 0
            else "empty"
            if count == 0
            else "ambiguous"
            if title and count > 1 and not ids
            else "partial"
            if not complete or requested_ids and count < len(requested_ids)
            else "matched"
        )
        return snapshot

    def overview_in_tx(
        self,
        tx: UnitOfWork,
        *,
        active_limit: int = 8,
        recent_limit: int = 5,
        requester_id: str | None = None,
        through_sequence: int | None = None,
    ) -> dict[str, Any]:
        if any(
            type(value) is not int or not 1 <= value <= 20 for value in (active_limit, recent_limit)
        ):
            raise DomainError(
                "invalid_input", "Work overviews require one to twenty works per bucket"
            )
        common: dict[str, Any] = {
            "requester_id": requester_id,
            "through_sequence": through_sequence,
        }
        live, live_count = _visible_rows(
            tx, states=_LIVE, limit=active_limit, active_first=True, **common
        )
        paused, paused_count = _visible_rows(
            tx, states=("paused",), recovery_only=True, limit=1000, **common
        )
        recovering = [
            row
            for row in paused
            if automatic_recovery_pending(
                WorkOrder.model_validate(
                    {key: row[key] for key in WorkOrder.model_fields if key in row}
                )
            )
        ]
        active = sorted(
            [*live, *recovering],
            key=lambda row: (
                3
                if row["state"] in {"running", "validating", "delivering"}
                else 2
                if row["state"] == "queued"
                else 1
                if row["state"] == "paused"
                else 0,
                row["updated_at"],
                row["id"],
            ),
            reverse=True,
        )[:active_limit]
        recent, recent_count = _visible_rows(tx, states=_CLOSED, limit=recent_limit, **common)
        active_count = live_count + len(recovering)
        active_complete = len(paused) == paused_count and len(active) == active_count
        recent_complete = len(recent) == recent_count
        scope = {
            "kind": "overview",
            "group_id": tx.group_id,
            "visibility": "public",
            **common,
            "matched_count": active_count + recent_count,
            "returned_count": len(active) + len(recent),
            "has_more": not (active_complete and recent_complete),
            "active": {
                "limit": active_limit,
                "matched_count": active_count,
                "returned_count": len(active),
                "complete": active_complete,
                "count_is_lower_bound": len(paused) < paused_count,
                "includes_automatic_recovery_paused": True,
            },
            "recent": {
                "limit": recent_limit,
                "matched_count": recent_count,
                "returned_count": len(recent),
                "complete": recent_complete,
                "states": list(_CLOSED),
            },
            "other_states_require_lookup": ["paused"],
        }
        return self._snapshot(tx, scope, [*active, *recent], active_complete and recent_complete)
