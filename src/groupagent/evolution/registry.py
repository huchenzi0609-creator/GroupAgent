"""Durable group-scoped proposal, evaluation and activation transactions."""

from __future__ import annotations

from typing import Any, Literal
from uuid import uuid4

from psycopg.types.json import Jsonb

from groupagent.domain import ActorContext, DomainError, ExecutionLease, Role
from groupagent.memory.causality import causal_sources
from groupagent.memory.common import memory_lock, source_rows
from groupagent.persistence import PostgresStore, UnitOfWork
from groupagent.tools import ToolContext

from .models import (
    CodeBundle,
    CodeEvaluationReport,
    CodeProposal,
    EvaluationReport,
    Kind,
    StrategyProposal,
    Version,
    canonical,
    digest,
)

_VERSION_SELECT = (
    "SELECT v.*,e.passed,a.version_id AS active_id FROM ga_evolution_versions v "
    "LEFT JOIN ga_evolution_evaluations e ON (e.group_id,e.version_id)=(v.group_id,v.id) "
    "LEFT JOIN ga_evolution_active a ON (a.group_id,a.kind,a.name)=(v.group_id,v.kind,v.name) "
)


def _version(row: dict[str, Any]) -> Version:
    state: Literal["proposed", "rejected", "evaluated", "active"] = "proposed"
    if row.get("passed") is False:
        state = "rejected"
    elif row.get("active_id") == row["id"]:
        state = "active"
    elif row.get("passed") is True:
        state = "evaluated"
    return Version(
        id=row["id"],
        kind=row["kind"],
        name=row["body"]["name"],
        version=row["version"],
        body_sha256=row["body_sha256"],
        state=state,
    )


class EvolutionRegistry:
    def __init__(self, store: PostgresStore) -> None:
        self.store = store

    @staticmethod
    def _work(tx: UnitOfWork, ctx: ToolContext, lease: ExecutionLease) -> dict[str, Any]:
        if ctx.cancelled.is_set():
            raise DomainError("cancelled", "Execution cancelled")
        if (ctx.group_id, ctx.work_id) != (lease.group_id, lease.work_id):
            raise DomainError("scope_violation", "Evolution requires the exact leased work scope")
        memory_lock(tx)
        tx.assert_lease(lease)
        work = tx._work_row(lease.work_id, lock=False)
        tx._check_work_source(work)
        if (
            work["requester_id"] != tx.actor.member_id
            or work["scope"].get("derived_depth", 0) != 0
            or work["scope"].get("origin_schedule_id")
            or work["scope"].get("origin_trigger_rule_id")
        ):
            raise DomainError("scope_violation", "Evolution requires direct requester work")
        return work

    @staticmethod
    def _sources(tx: UnitOfWork, work: dict[str, Any]) -> dict[str, str]:
        scope = work["scope"]
        recorded = dict(scope.get("source_fingerprints", {}))
        recorded.update(scope.get("human_request_fingerprints", {}))
        identifiers = {work["request_event_id"], *recorded}
        for key in (
            "human_request_event_ids",
            "source_event_ids",
            "attachment_event_ids",
            "context_source_ids",
            "input_event_ids",
            "retained_fact_source_ids",
        ):
            identifiers.update(scope.get(key, []))
        identifiers = causal_sources(tx, identifiers)
        source_rows(tx, sorted(identifiers), shared=True, require_complete=True)
        result = {identifier: tx.event_fingerprint(identifier) for identifier in identifiers}
        if any(result[identifier] != expected for identifier, expected in recorded.items()):
            raise DomainError("source_unavailable", "Proposal work sources have changed")
        return result

    @staticmethod
    def _available(tx: UnitOfWork, row: dict[str, Any]) -> None:
        fingerprints = row["source_fingerprints"]
        if row["revoked"]:
            raise DomainError("source_unavailable", "Proposal was revoked")
        source_rows(tx, sorted(fingerprints), shared=True, require_complete=True)
        if any(tx.event_fingerprint(key) != expected for key, expected in fingerprints.items()):
            raise DomainError("source_unavailable", "Proposal sources have changed")
        tx._check_work_source(tx._work_row(row["work_id"], lock=False))

    @staticmethod
    def _find(tx: UnitOfWork, version_id: str, kind: Kind) -> dict[str, Any]:
        row = tx.conn.execute(
            _VERSION_SELECT + "WHERE v.group_id=%s AND v.id=%s AND v.kind=%s AND NOT v.revoked",
            (tx.group_id, version_id, kind),
        ).fetchone()
        if row is None:
            raise DomainError("not_found", "Version was not found in this group")
        return row

    def propose(
        self,
        ctx: ToolContext,
        lease: ExecutionLease,
        content: StrategyProposal | CodeProposal,
    ) -> Version:
        if ctx.actor.role != Role.OWNER:
            raise DomainError("permission_denied", "Only a group owner can sponsor proposals")
        kind: Kind = "strategy" if isinstance(content, StrategyProposal) else "code"
        body = content.model_dump(mode="json")
        body_sha = digest(canonical(body))
        name_key = digest(content.name.encode())
        with self.store.transaction(ctx.actor, trace_id=ctx.scope.trace_id) as tx:
            work = self._work(tx, ctx, lease)
            sources = self._sources(tx, work)
            existing = tx.conn.execute(
                _VERSION_SELECT + "WHERE v.group_id=%s AND v.work_id=%s AND v.idempotency_key=%s",
                (tx.group_id, lease.work_id, ctx.idempotency_key),
            ).fetchone()
            if existing is not None:
                if (
                    existing["body_sha256"] != body_sha
                    or existing["kind"] != kind
                    or existing["goal_revision"] != lease.goal_revision
                ):
                    raise DomainError(
                        "idempotency_conflict", "Call is bound to a different proposal"
                    )
                self._available(tx, existing)
                return _version(existing)
            row = tx.conn.execute(
                "SELECT COALESCE(MAX(version),0)+1 AS next FROM ga_evolution_versions "
                "WHERE group_id=%s AND kind=%s AND name=%s",
                (tx.group_id, kind, name_key),
            ).fetchone()
            assert row is not None
            version_id = uuid4().hex
            saved = tx.conn.execute(
                "INSERT INTO ga_evolution_versions (group_id,id,kind,name,version,work_id,"
                "goal_revision,proposed_by,idempotency_key,source_fingerprints,body,body_sha256) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING *",
                (
                    tx.group_id,
                    version_id,
                    kind,
                    name_key,
                    row["next"],
                    lease.work_id,
                    lease.goal_revision,
                    ctx.actor.member_id,
                    ctx.idempotency_key,
                    Jsonb(sources),
                    Jsonb(body),
                    body_sha,
                ),
            ).fetchone()
            assert saved is not None
            tx._audit("evolution_proposed", version_id, kind=kind, sha256=body_sha)
            return _version(saved)

    def active_strategies(
        self, ctx: ToolContext, lease: ExecutionLease, name: str | None
    ) -> tuple[Version, ...]:
        with self.store.transaction(ctx.actor, trace_id=ctx.scope.trace_id) as tx:
            self._work(tx, ctx, lease)
            rows = tx.conn.execute(
                _VERSION_SELECT + "WHERE v.group_id=%s AND v.kind='strategy' AND NOT v.revoked "
                "AND a.version_id=v.id AND (%s::text IS NULL OR v.name=%s) "
                "ORDER BY v.body->>'name' LIMIT 30",
                (tx.group_id, name, digest(name.encode()) if name is not None else None),
            ).fetchall()
            available = []
            for row in rows:
                try:
                    self._available(tx, row)
                except DomainError as exc:
                    if exc.code not in {"source_unavailable", "scope_violation", "not_found"}:
                        raise
                    continue
                available.append(_version(row))
            return tuple(available)

    def strategy(
        self, ctx: ToolContext, lease: ExecutionLease, version_id: str, *, trial: bool = False
    ) -> tuple[Version, StrategyProposal]:
        with self.store.transaction(ctx.actor, trace_id=ctx.scope.trace_id) as tx:
            work = self._work(tx, ctx, lease)
            row = self._find(tx, version_id, "strategy")
            self._available(tx, row)
            if trial:
                if (
                    ctx.actor.role != Role.OWNER
                    or row["work_id"] != ctx.work_id
                    or row["goal_revision"] != lease.goal_revision
                    or row["passed"] is False
                ):
                    raise DomainError(
                        "permission_denied", "Trial requires the proposing work generation"
                    )
            elif row["active_id"] != version_id or row["passed"] is not True:
                raise DomainError("invalid_transition", "Strategy version is not currently active")
            scope = dict(work["scope"])
            sources = dict(scope.get("source_fingerprints", {}))
            sources.update(row["source_fingerprints"])
            if len(sources) > 500:
                raise DomainError("scope_violation", "Strategy exceeds the work source limit")
            scope.update(source_fingerprints=sources, source_event_ids=sorted(sources))
            tx.conn.execute(
                "UPDATE ga_work_orders SET scope=%s WHERE group_id=%s AND id=%s",
                (Jsonb(scope), tx.group_id, ctx.work_id),
            )
            tx._audit(
                "evolution_trial" if trial else "evolution_used", version_id, work_id=ctx.work_id
            )
            return _version(row), StrategyProposal.model_validate(row["body"])

    def code_status(self, ctx: ToolContext, lease: ExecutionLease, version_id: str) -> Version:
        if ctx.actor.role != Role.OWNER:
            raise DomainError("permission_denied", "Only a group owner can inspect code proposals")
        with self.store.transaction(ctx.actor, trace_id=ctx.scope.trace_id) as tx:
            self._work(tx, ctx, lease)
            row = self._find(tx, version_id, "code")
            self._available(tx, row)
            return _version(row)

    @staticmethod
    def _operator(actor: ActorContext) -> None:
        if actor.role != Role.MAINTAINER:
            raise DomainError("permission_denied", "A trusted development operator is required")

    def export_strategy(
        self, actor: ActorContext, version_id: str
    ) -> tuple[Version, StrategyProposal]:
        self._operator(actor)
        with self.store.transaction(actor) as tx:
            memory_lock(tx)
            row = self._find(tx, version_id, "strategy")
            self._available(tx, row)
            tx._audit("evolution_strategy_reviewed", version_id, sha256=row["body_sha256"])
            return _version(row), StrategyProposal.model_validate(row["body"])

    def export_code(self, actor: ActorContext, version_id: str) -> CodeBundle:
        self._operator(actor)
        with self.store.transaction(actor) as tx:
            memory_lock(tx)
            row = self._find(tx, version_id, "code")
            self._available(tx, row)
            tx._audit("evolution_code_exported", version_id, sha256=row["body_sha256"])
            return CodeBundle(
                version_id=version_id,
                body_sha256=row["body_sha256"],
                proposal=CodeProposal.model_validate(row["body"]),
            )

    def record_evaluation(self, actor: ActorContext, report: EvaluationReport) -> Version:
        self._operator(actor)
        kind: Kind = "code" if isinstance(report, CodeEvaluationReport) else "strategy"
        with self.store.transaction(actor) as tx:
            memory_lock(tx)
            row = self._find(tx, report.version_id, kind)
            self._available(tx, row)
            if actor.member_id == row["proposed_by"]:
                raise DomainError(
                    "permission_denied", "Evaluation requires an independent operator"
                )
            if row["body_sha256"] != report.body_sha256 or (
                isinstance(report, CodeEvaluationReport)
                and row["body"]["baseline_sha256"] != report.baseline_sha256
            ):
                raise DomainError("version_conflict", "Evaluation targets a different proposal")
            if kind == "strategy" and not {"scope", "behavior"}.issubset(report.checks):
                raise DomainError(
                    "invalid_input", "Strategy review requires scope and behavior checks"
                )
            report_body = report.model_dump(mode="json")
            report_sha = digest(canonical(report_body))
            saved = tx.conn.execute(
                "INSERT INTO ga_evolution_evaluations "
                "(group_id,version_id,reviewed_by,report_sha256,report,passed) VALUES(%s,%s,%s,%s,%s,%s) "
                "ON CONFLICT (group_id,version_id) DO NOTHING",
                (
                    tx.group_id,
                    row["id"],
                    actor.member_id,
                    report_sha,
                    Jsonb(report_body),
                    report.passed,
                ),
            )
            if not saved.rowcount:
                existing = tx.conn.execute(
                    "SELECT report_sha256 FROM ga_evolution_evaluations WHERE group_id=%s AND version_id=%s",
                    (tx.group_id, row["id"]),
                ).fetchone()
                if not existing or existing["report_sha256"] != report_sha:
                    raise DomainError(
                        "version_conflict", "Version already has an immutable evaluation"
                    )
            else:
                tx._audit("evolution_evaluated", row["id"], sha256=report_sha, passed=report.passed)
            return _version({**row, "passed": report.passed})

    def promote(
        self,
        actor: ActorContext,
        version_id: str,
        *,
        kind: Kind,
        expected_active: str | None,
        code_evidence: CodeEvaluationReport | None = None,
    ) -> Version:
        """A trusted operator activates an evaluated version or rolls back by CAS."""
        self._operator(actor)
        with self.store.transaction(actor) as tx:
            memory_lock(tx)
            row = self._find(tx, version_id, kind)
            self._available(tx, row)
            if row["proposed_by"] == actor.member_id or row["passed"] is not True:
                raise DomainError(
                    "permission_denied", "Promotion requires independent passing evidence"
                )
            if kind == "code":
                stored = tx.conn.execute(
                    "SELECT report_sha256 FROM ga_evolution_evaluations WHERE group_id=%s AND version_id=%s",
                    (tx.group_id, row["id"]),
                ).fetchone()
                if (
                    code_evidence is None
                    or not stored
                    or stored["report_sha256"]
                    != digest(canonical(code_evidence.model_dump(mode="json")))
                ):
                    raise DomainError(
                        "version_conflict", "Development rollout requires the evaluated bundle"
                    )
            if row["active_id"] != expected_active:
                raise DomainError("version_conflict", "The active version changed")
            tx.conn.execute(
                "INSERT INTO ga_evolution_active (group_id,kind,name,version_id) VALUES (%s,%s,%s,%s) "
                "ON CONFLICT (group_id,kind,name) DO UPDATE SET version_id=EXCLUDED.version_id",
                (tx.group_id, kind, row["name"], version_id),
            )
            tx._audit(
                "evolution_promoted", version_id, previous_version_id=expected_active, kind=kind
            )
            return _version({**row, "active_id": version_id})

    def deactivate(self, actor: ActorContext, *, kind: Kind, expected_active: str) -> None:
        self._operator(actor)
        with self.store.transaction(actor) as tx:
            memory_lock(tx)
            removed = tx.conn.execute(
                "DELETE FROM ga_evolution_active WHERE group_id=%s AND kind=%s AND version_id=%s",
                (tx.group_id, kind, expected_active),
            )
            if not removed.rowcount:
                raise DomainError("version_conflict", "The active version changed")
            tx._audit("evolution_deactivated", expected_active, kind=kind)
