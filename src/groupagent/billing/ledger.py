from __future__ import annotations

import json
from datetime import UTC, date, datetime
from decimal import Decimal
from hashlib import sha256
from typing import Any, Literal, Self
from uuid import uuid4
from zoneinfo import ZoneInfo

from psycopg.types.json import Jsonb
from pydantic import (
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    ValidationError,
    field_validator,
    model_validator,
)

from groupagent.domain import ActorContext, DomainError, Role
from groupagent.memory.common import memory_lock
from groupagent.persistence import PostgresStore

from .models import QUANTUM, PriceSnapshot, Reservation

_RECONCILIATION_ACTION = "budget_billing_reconciled"


def _money(value: Decimal) -> Decimal:
    if (
        not value.is_finite()
        or value < 0
        or value >= Decimal("1e18")
        or value.quantize(QUANTUM) != value
    ):
        raise ValueError("Amounts require finite nonnegative six-decimal monetary values")
    return value.quantize(QUANTUM)


class _BillingReconciliationBase(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    key: str = Field(min_length=1, max_length=200, strict=True)
    source: Literal["user_reported_provider_console"] = "user_reported_provider_console"
    operator_role: Literal["owner"] = "owner"
    as_of: AwareDatetime
    recorded_at: AwareDatetime
    currency: Literal["CNY", "USD"]
    reported_total: Decimal
    prefix_count: int = Field(ge=1, strict=True)
    prefix_gross: Decimal
    prefix_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    prefix_by_day: dict[date, Decimal] = Field(min_length=1)
    evidence_ref: str = Field(min_length=1, max_length=2000, strict=True)
    evidence_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    previous_id: str | None = Field(default=None, pattern=r"^[1-9][0-9]*$")
    previous_digest: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    integrity_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")

    @field_validator("reported_total", "prefix_gross")
    @classmethod
    def money(cls, value: Decimal) -> Decimal:
        return _money(value)

    @field_validator("prefix_by_day")
    @classmethod
    def daily_money(cls, value: dict[date, Decimal]) -> dict[date, Decimal]:
        return {day: _money(amount) for day, amount in value.items()}

    @field_validator("as_of", "recorded_at")
    @classmethod
    def utc(cls, value: datetime) -> datetime:
        return value.astimezone(UTC)

    @field_validator("key", "evidence_ref")
    @classmethod
    def nonblank(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("A nonblank reconciliation key and evidence reference are required")
        return value

    @model_validator(mode="after")
    def consistent(self) -> Self:
        if (
            self.as_of > self.recorded_at
            or sum(self.prefix_by_day.values(), Decimal(0)) != self.prefix_gross
            or (self.previous_id is None) != (self.previous_digest is None)
        ):
            raise ValueError("Invalid reconciliation time, daily totals or predecessor")
        return self


class _BillingReconciliation(_BillingReconciliationBase):
    # Keep the exact v1 fields/digest readable; old audits are never rewritten.
    schema_version: Literal[1] = 1


class _HeldUnknownBillingReconciliation(_BillingReconciliationBase):
    schema_version: Literal[2] = 2
    unknown_prefix_policy: Literal["retain_reserved_amounts"] = "retain_reserved_amounts"
    prefix_unknown_count: int = Field(default=0, ge=0, strict=True)
    prefix_unknown_held: Decimal = Decimal(0)

    @field_validator("prefix_unknown_held")
    @classmethod
    def held_money(cls, value: Decimal) -> Decimal:
        return _money(value)

    @model_validator(mode="after")
    def unknown_totals_consistent(self) -> Self:
        if self.prefix_unknown_count > self.prefix_count or (
            self.prefix_unknown_count == 0 and self.prefix_unknown_held != 0
        ):
            raise ValueError("Unknown prefix counts and holds must describe existing records")
        return self


_BillingRecord = _BillingReconciliation | _HeldUnknownBillingReconciliation


def _digest(value: Any) -> str:
    def canonical(item: Any) -> str:
        if isinstance(item, datetime):
            return item.astimezone(UTC).isoformat(timespec="microseconds")
        return str(item)

    return sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":"), default=canonical).encode()
    ).hexdigest()


def _reconciliation_digest(
    group_id: str, actor_id: str, policy_revision: int, body: _BillingRecord
) -> str:
    return _digest(
        {
            "group_id": group_id,
            "actor_id": actor_id,
            "policy_revision": policy_revision,
            "resource_ref": body.key,
            "details": body.model_dump(mode="json", exclude={"integrity_sha256"}),
        }
    )


def _units(value: dict[str, Decimal]) -> Jsonb:
    return Jsonb({key: str(amount) for key, amount in value.items()})


def _limits_enabled(policy: dict[str, Any] | None) -> bool:
    # Only an existing policy with both limits explicitly NULL is unlimited.
    return policy is None or any(
        policy[field] is not None for field in ("daily_limit", "total_limit")
    )


def _reservation(
    row: dict[str, Any], *, is_new: bool = False, limits_enabled: bool = True
) -> Reservation:
    return Reservation.model_validate(
        {
            **{key: row[key] for key in Reservation.model_fields if key in row},
            "is_new": is_new,
            "limits_enabled": limits_enabled,
        }
    )


class BudgetLedger:
    def __init__(self, store: PostgresStore, *, allow_test_prices: bool = False) -> None:
        self.store, self.allow_test_prices = store, allow_test_prices
        self.fixture_only = allow_test_prices

    def configure(
        self,
        actor: ActorContext,
        *,
        currency: str,
        daily_limit: Decimal | None,
        total_limit: Decimal | None = None,
        time_zone: str = "Asia/Shanghai",
        expected_revision: int | None = None,
    ) -> int:
        if actor.role != Role.OWNER:
            raise DomainError("permission_denied", "Only the group owner sets monetary limits")
        if currency not in {"CNY", "USD"}:
            raise DomainError("invalid_input", "Unsupported budget currency")
        ZoneInfo(time_zone)
        limits = [value for value in (daily_limit, total_limit) if value is not None]
        if any(
            not value.is_finite() or value <= 0 or value.quantize(QUANTUM) != value
            for value in limits
        ):
            raise DomainError("invalid_input", "Budget limits require positive finite amounts")
        with self.store.transaction(actor) as tx:
            row = tx.conn.execute(
                "SELECT * FROM ga_budget_policy WHERE group_id=%s FOR UPDATE", (actor.group_id,)
            ).fetchone()
            if row is None:
                if expected_revision not in (None, 0):
                    raise DomainError("version_conflict", "Budget policy has changed")
                row = tx.conn.execute(
                    "INSERT INTO ga_budget_policy(group_id,currency,daily_limit,total_limit,time_zone) VALUES(%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING RETURNING revision",
                    (actor.group_id, currency, daily_limit, total_limit, time_zone),
                ).fetchone()
                if row is None:
                    raise DomainError("version_conflict", "Budget policy was concurrently created")
            else:
                if expected_revision != row["revision"]:
                    raise DomainError(
                        "version_conflict", "Budget update requires the current revision"
                    )
                if currency != row["currency"]:
                    raise DomainError(
                        "currency_mismatch",
                        "Create a separately scoped budget for another currency",
                    )
                row = tx.conn.execute(
                    "UPDATE ga_budget_policy SET daily_limit=%s,total_limit=%s,time_zone=%s,revision=revision+1,updated_at=clock_timestamp() WHERE group_id=%s RETURNING revision",
                    (daily_limit, total_limit, time_zone, actor.group_id),
                ).fetchone()
            assert row is not None
            tx._audit(
                "budget_configured",
                actor.group_id,
                currency=currency,
                daily_limit=str(daily_limit) if daily_limit is not None else None,
                total_limit=str(total_limit) if total_limit is not None else None,
            )
            return int(row["revision"])

    def register_price(self, actor: ActorContext, price: PriceSnapshot) -> None:
        if actor.role not in {Role.OWNER, Role.MAINTAINER}:
            raise DomainError(
                "permission_denied", "Price snapshots are trusted deployment configuration"
            )
        if price.test_only and not self.allow_test_prices:
            raise DomainError("price_missing", "Fixture prices cannot authorize real calls")
        body = price.model_dump(mode="json")
        with self.store.transaction(actor) as tx:
            tx.conn.execute(
                "INSERT INTO ga_price_snapshots(group_id,id,body) VALUES(%s,%s,%s) ON CONFLICT DO NOTHING",
                (actor.group_id, price.id, Jsonb(body)),
            )
            existing = tx.conn.execute(
                "SELECT body FROM ga_price_snapshots WHERE group_id=%s AND id=%s",
                (actor.group_id, price.id),
            ).fetchone()
            if existing is None or existing["body"] != body:
                raise DomainError("version_conflict", "Price snapshots are immutable; use a new ID")

    @staticmethod
    def _billing_prefix(
        tx: Any, as_of: datetime, currency: str, *, preserve_unknown_holds: bool = False
    ) -> dict[str, Any]:
        summary = tx.conn.execute(
            "SELECT count(*) AS count, "
            "count(*) FILTER (WHERE (state NOT IN ('settled','released') "
            "AND NOT (%s AND state='unknown')) "
            "OR (state='settled' AND actual_amount IS NULL) OR currency<>%s) AS invalid, "
            "COALESCE(sum(actual_amount) FILTER (WHERE state='settled'),0) AS gross, "
            "count(*) FILTER (WHERE state='unknown') AS unknown_count, "
            "COALESCE(sum(reserved_amount) FILTER (WHERE state='unknown'),0) AS unknown_held, "
            "max(updated_at) AS last_update FROM ga_budget_reservations "
            "WHERE group_id=%s AND created_at<=%s",
            (preserve_unknown_holds, currency, tx.group_id, as_of),
        ).fetchone()
        assert summary is not None
        if not summary["count"] or summary["invalid"] or summary["last_update"] > as_of:
            raise DomainError(
                "reconciliation_required",
                "Billing prefix must be nonempty and terminal before the reported cutoff",
            )
        rows = tx.conn.execute(
            "SELECT * FROM ga_budget_reservations WHERE group_id=%s AND created_at<=%s "
            "ORDER BY id FOR SHARE",
            (tx.group_id, as_of),
        ).fetchall()
        by_day = tx.conn.execute(
            "SELECT budget_day,COALESCE(sum(actual_amount) FILTER (WHERE state='settled'),0) "
            "AS gross FROM ga_budget_reservations WHERE group_id=%s AND created_at<=%s "
            "GROUP BY budget_day ORDER BY budget_day",
            (tx.group_id, as_of),
        ).fetchall()
        return {
            "count": int(summary["count"]),
            "gross": summary["gross"],
            "unknown_count": int(summary["unknown_count"]),
            "unknown_held": summary["unknown_held"],
            "sha256": _digest(rows),
            "by_day": {row["budget_day"]: row["gross"] for row in by_day},
        }

    @staticmethod
    def _billing_reconciliations(tx: Any, currency: str) -> list[tuple[str, _BillingRecord]]:
        rows = tx.conn.execute(
            "SELECT * FROM ga_audit_events WHERE group_id=%s AND action=%s ORDER BY id",
            (tx.group_id, _RECONCILIATION_ACTION),
        ).fetchall()
        records: list[tuple[str, _BillingRecord]] = []
        keys: set[str] = set()
        try:
            for row in rows:
                if not isinstance(row["details"], dict):
                    raise ValueError("Billing reconciliation payload is not an object")
                version = row["details"].get("schema_version")
                if type(version) is not int or version not in {1, 2}:
                    raise ValueError("Unknown billing reconciliation version")
                body: _BillingRecord = (
                    _BillingReconciliation.model_validate(row["details"])
                    if version == 1
                    else _HeldUnknownBillingReconciliation.model_validate(row["details"])
                )
                prior_id, prior = records[-1] if records else (None, None)
                if (
                    body.model_dump(mode="json") != row["details"]
                    or body.currency != currency
                    or body.key != row["resource_ref"]
                    or body.key in keys
                    or body.recorded_at != row["created_at"]
                    or body.previous_id != prior_id
                    or body.previous_digest != (prior.integrity_sha256 if prior else None)
                    or body.integrity_sha256
                    != _reconciliation_digest(
                        tx.group_id, row["actor_id"], row["policy_revision"], body
                    )
                    or (
                        prior is not None
                        and (
                            body.as_of <= prior.as_of
                            or body.reported_total < prior.reported_total
                            or body.prefix_count < prior.prefix_count
                            or body.prefix_gross < prior.prefix_gross
                        )
                    )
                ):
                    raise ValueError("Billing reconciliation chain does not match its audit")
                records.append((str(row["id"]), body))
                keys.add(body.key)
        except (ValidationError, ValueError, TypeError, KeyError) as error:
            raise DomainError(
                "reconciliation_required", "Billing reconciliation audit failed integrity checks"
            ) from error
        return records

    def _budget_usage(
        self, tx: Any, policy: dict[str, Any], day: date, *, exclude_daily_id: str | None = None
    ) -> dict[str, Any]:
        # Callers hold the policy lock, shared with reserve/begin/settle/reconciliation.
        sums = tx.conn.execute(
            "SELECT COALESCE(sum(amount),0) AS total, "
            "COALESCE(sum(amount) FILTER (WHERE budget_day=%s AND id IS DISTINCT FROM %s),0) "
            "AS daily, COALESCE(sum(reserved_amount) FILTER "
            "(WHERE state IN ('reserved','inflight','unknown')),0) AS held "
            "FROM (SELECT *,CASE WHEN state='settled' THEN actual_amount "
            "WHEN state IN ('reserved','inflight','unknown') THEN reserved_amount ELSE 0 END "
            "AS amount FROM ga_budget_reservations WHERE group_id=%s) AS usage",
            (day, exclude_daily_id, tx.group_id),
        ).fetchone()
        assert sums is not None
        result: dict[str, Any] = {
            "gross_total": sums["total"],
            "gross_settled_estimate": sums["total"] - sums["held"],
            "gross_daily": sums["daily"],
            "held": sums["held"],
            "total": sums["total"],
            "daily": sums["daily"],
            "reconciliation": None,
        }
        records = self._billing_reconciliations(tx, policy["currency"])
        if records:
            identifier, latest = records[-1]
            prefix = self._billing_prefix(
                tx,
                latest.as_of,
                latest.currency,
                preserve_unknown_holds=isinstance(latest, _HeldUnknownBillingReconciliation),
            )
            if (
                prefix["count"] != latest.prefix_count
                or prefix["gross"] != latest.prefix_gross
                or prefix["sha256"] != latest.prefix_sha256
                or prefix["by_day"] != latest.prefix_by_day
                or (
                    isinstance(latest, _HeldUnknownBillingReconciliation)
                    and (
                        prefix["unknown_count"] != latest.prefix_unknown_count
                        or prefix["unknown_held"] != latest.prefix_unknown_held
                    )
                )
            ):
                raise DomainError(
                    "reconciliation_required", "Reconciled billing prefix no longer matches"
                )
            result["total"] += latest.reported_total - latest.prefix_gross
            if day in latest.prefix_by_day:
                # The cumulative bill has no trusted per-day allocation. Charging it
                # entirely to each covered day is a conservative upper bound.
                result["daily"] += latest.reported_total - latest.prefix_by_day[day]
            result["reconciliation"] = {"id": identifier, **latest.model_dump(mode="json")}
        return result

    def reconcile_billing(
        self,
        actor: ActorContext,
        *,
        key: str,
        as_of: datetime,
        currency: str,
        reported_total: Decimal,
        expected_prefix_count: int,
        expected_prefix_gross: Decimal,
        evidence_ref: str,
        evidence_sha256: str,
        expected_previous_id: str | None = None,
        preserve_unknown_holds: bool = False,
    ) -> dict[str, Any]:
        """Trusted local Owner operator API; never register as a model/channel tool.

        Preserve policy, prices and reservations. The existing audit is application-
        append-only, not a database-enforced or independently attested billing log.

        The explicit v2 mode accepts a cumulative Owner report while preserving
        every unknown reservation as an additional conservative hold. It replaces
        only the settled estimates: no unknown usage or effect is resolved, and
        reserved/inflight prefixes are still rejected. This can intentionally
        double-count unknown fees already included in the reported total.
        """
        if actor.role != Role.OWNER:
            raise DomainError("permission_denied", "Only a trusted Owner may reconcile billing")
        if (
            not isinstance(as_of, datetime)
            or as_of.utcoffset() is None
            or not isinstance(reported_total, Decimal)
            or not isinstance(expected_prefix_gross, Decimal)
            or type(preserve_unknown_holds) is not bool
        ):
            raise DomainError(
                "invalid_input", "Reconciliation needs aware time and Decimal amounts"
            )
        with self.store.transaction(actor) as tx:
            memory_lock(tx)
            policy = tx.conn.execute(
                "SELECT * FROM ga_budget_policy WHERE group_id=%s FOR UPDATE", (actor.group_id,)
            ).fetchone()
            if policy is None:
                raise DomainError("budget_unconfigured", "A monetary policy is required")
            if currency != policy["currency"]:
                raise DomainError("currency_mismatch", "Billing evidence currency does not match")
            clock = tx.conn.execute("SELECT clock_timestamp() AS now").fetchone()
            assert clock is not None
            stamp = clock["now"]
            try:
                record_type = (
                    _HeldUnknownBillingReconciliation
                    if preserve_unknown_holds
                    else _BillingReconciliation
                )
                request = record_type(
                    key=key,
                    as_of=as_of,
                    recorded_at=stamp,
                    currency=policy["currency"],
                    reported_total=reported_total,
                    prefix_count=expected_prefix_count,
                    prefix_gross=expected_prefix_gross,
                    prefix_sha256="0" * 64,
                    prefix_by_day={as_of.date(): expected_prefix_gross},
                    evidence_ref=evidence_ref,
                    evidence_sha256=evidence_sha256,
                    previous_id=expected_previous_id,
                    previous_digest="0" * 64 if expected_previous_id is not None else None,
                    integrity_sha256="0" * 64,
                )
            except (ValidationError, ValueError) as error:
                raise DomainError(
                    "invalid_input", "Invalid billing reconciliation evidence"
                ) from error
            records = self._billing_reconciliations(tx, currency)
            # Revalidate any current prefix even on an idempotent retry.
            self._budget_usage(tx, policy, stamp.astimezone(ZoneInfo(policy["time_zone"])).date())
            fields = {
                "schema_version",
                "key",
                "as_of",
                "currency",
                "reported_total",
                "prefix_count",
                "prefix_gross",
                "evidence_ref",
                "evidence_sha256",
                "previous_id",
            }
            for identifier, old in records:
                if old.key == key:
                    if old.model_dump(include=fields) != request.model_dump(include=fields):
                        raise DomainError(
                            "idempotency_conflict", "Reconciliation key has other evidence"
                        )
                    return {"id": identifier, **old.model_dump(mode="json")}
            previous_id, previous = records[-1] if records else (None, None)
            if expected_previous_id != previous_id:
                raise DomainError("version_conflict", "Billing reconciliation predecessor changed")
            if previous and (
                request.as_of <= previous.as_of or request.reported_total < previous.reported_total
            ):
                raise DomainError(
                    "version_conflict", "Cumulative billing must advance without refunds"
                )
            prefix = self._billing_prefix(
                tx, request.as_of, currency, preserve_unknown_holds=preserve_unknown_holds
            )
            if prefix["count"] != expected_prefix_count or prefix["gross"] != expected_prefix_gross:
                raise DomainError(
                    "version_conflict", "Billing prefix count or gross estimate differs"
                )
            payload = {
                **request.model_dump(),
                "prefix_sha256": prefix["sha256"],
                "prefix_by_day": prefix["by_day"],
                "previous_digest": previous.integrity_sha256 if previous else None,
            }
            if preserve_unknown_holds:
                payload.update(
                    prefix_unknown_count=prefix["unknown_count"],
                    prefix_unknown_held=prefix["unknown_held"],
                )
            body = record_type.model_validate(payload)
            body = body.model_copy(
                update={
                    "integrity_sha256": _reconciliation_digest(
                        actor.group_id, actor.member_id, actor.policy_revision, body
                    )
                }
            )
            row = tx.conn.execute(
                "INSERT INTO ga_audit_events(group_id,trace_id,actor_id,policy_revision,action,"
                "resource_ref,details,created_at) VALUES(%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id",
                (
                    actor.group_id,
                    tx.trace_id,
                    actor.member_id,
                    actor.policy_revision,
                    _RECONCILIATION_ACTION,
                    key,
                    Jsonb(body.model_dump(mode="json")),
                    stamp,
                ),
            ).fetchone()
            assert row is not None
            return {"id": str(row["id"]), **body.model_dump(mode="json")}

    def reserve(
        self,
        actor: ActorContext,
        key: str,
        operation: str,
        price_id: str,
        estimated_units: dict[str, Decimal],
        *,
        work_id: str | None = None,
        source_event_id: str | None = None,
        read_only_authorized: bool = False,
        now: datetime | None = None,
    ) -> Reservation:
        if not key.strip() or len(key) > 300:
            raise DomainError("invalid_input", "A bounded stable billing key is required")
        if actor.role not in {Role.MEMBER, Role.OWNER} and not read_only_authorized:
            raise DomainError(
                "permission_denied", "Read-only billing needs current policy authorization"
            )
        now = now or datetime.now(UTC)
        if now.tzinfo is None:
            raise DomainError("invalid_input", "Budget clock must be timezone aware")
        with self.store.transaction(actor) as tx:
            memory_lock(tx)
            self._payment_scope(tx, work_id, source_event_id)
            policy = tx.conn.execute(
                "SELECT * FROM ga_budget_policy WHERE group_id=%s FOR UPDATE", (actor.group_id,)
            ).fetchone()
            if policy is None:
                raise DomainError(
                    "budget_unconfigured", "A monetary budget must be configured before paid calls"
                )
            old = tx.conn.execute(
                "SELECT * FROM ga_budget_reservations WHERE group_id=%s AND key=%s",
                (actor.group_id, key),
            ).fetchone()
            if old:
                previous = _reservation(old, limits_enabled=_limits_enabled(policy))
                if (
                    previous.actor_id,
                    previous.operation,
                    previous.price_id,
                    previous.estimated_units,
                    previous.work_id,
                    previous.source_event_id,
                ) != (
                    actor.member_id,
                    operation,
                    price_id,
                    estimated_units,
                    work_id,
                    source_event_id,
                ):
                    raise DomainError(
                        "idempotency_conflict", "Billing key already has another purpose"
                    )
                return previous
            price_row = tx.conn.execute(
                "SELECT body FROM ga_price_snapshots WHERE group_id=%s AND id=%s",
                (actor.group_id, price_id),
            ).fetchone()
            if price_row is None:
                raise DomainError(
                    "price_missing", "This provider operation has no registered price"
                )
            price = PriceSnapshot.model_validate(price_row["body"])
            if (
                price.operation != operation
                or price.currency != policy["currency"]
                or (price.test_only and not self.allow_test_prices)
            ):
                raise DomainError(
                    "price_missing", "Price operation, currency or environment does not match"
                )
            amount = price.cost(estimated_units)
            day = now.astimezone(ZoneInfo(policy["time_zone"])).date()
            sums = self._budget_usage(tx, policy, day)
            if (
                policy["blocked"]
                or (
                    policy["daily_limit"] is not None
                    and sums["daily"] + amount > policy["daily_limit"]
                )
                or (
                    policy["total_limit"] is not None
                    and sums["total"] + amount > policy["total_limit"]
                )
            ):
                raise DomainError(
                    "budget_exhausted", "The next call exceeds the remaining monetary budget"
                )
            row = tx.conn.execute(
                "INSERT INTO ga_budget_reservations(id,group_id,actor_id,key,operation,price_id,currency,budget_day,reserved_amount,estimated_units,work_id,source_event_id,policy_revision) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING *",
                (
                    uuid4().hex,
                    actor.group_id,
                    actor.member_id,
                    key,
                    operation,
                    price_id,
                    price.currency,
                    day,
                    amount,
                    _units(estimated_units),
                    work_id,
                    source_event_id,
                    actor.policy_revision,
                ),
            ).fetchone()
            assert row is not None
            tx._audit(
                "budget_reserved",
                row["id"],
                operation=operation,
                amount=str(amount),
                currency=price.currency,
            )
            return _reservation(row, is_new=True, limits_enabled=_limits_enabled(policy))

    @staticmethod
    def _payment_scope(tx: Any, work_id: str | None, source_event_id: str | None) -> None:
        source = tx.get_event(source_event_id) if source_event_id else None
        if source is not None:
            if source.actor_id != tx.actor.member_id:
                raise DomainError("permission_denied", "Billing request identity does not match")
            if source.content_state != "active":
                raise DomainError(
                    "source_unavailable", "Deleted request cannot authorize paid calls"
                )
        if work_id is not None:
            row = tx._work_row(work_id)
            if row["requester_id"] != tx.actor.member_id and tx.actor.role != Role.OWNER:
                raise DomainError("permission_denied", "Cannot spend another member's work budget")
            if tx.actor.role not in {Role.OWNER, Role.MEMBER}:
                raise DomainError(
                    "permission_denied", "Read-only identities cannot spend a work budget"
                )
            if row["state"] not in {"queued", "running", "validating"} or row["scope"].get(
                "privacy_redacted"
            ):
                raise DomainError(
                    "invalid_transition", "This work no longer authorizes paid execution"
                )
            tx.assert_task_sources(
                row["scope"].get("source_task_revisions", {}), require_current=False
            )
            request = tx.get_event(row["request_event_id"])
            if request.content_state != "active":
                raise DomainError("source_unavailable", "Work request is no longer available")
            tx._validate_work_sources(
                row["scope"].get("source_fingerprints", {}),
                row["scope"].get("retained_fact_source_ids", []),
                row["input_versions"],
            )

    @staticmethod
    def _owned(tx: Any, identifier: str) -> dict[str, Any]:
        row = tx.conn.execute(
            "SELECT * FROM ga_budget_reservations WHERE group_id=%s AND id=%s FOR UPDATE",
            (tx.group_id, identifier),
        ).fetchone()
        if row is None:
            raise DomainError("not_found", "Budget reservation is outside this group")
        if row["actor_id"] != tx.actor.member_id and tx.actor.role != Role.OWNER:
            raise DomainError("permission_denied", "Budget reservation belongs to another member")
        return row

    def begin(self, actor: ActorContext, identifier: str, *, now: datetime | None = None) -> bool:
        with self.store.transaction(actor) as tx:
            # Source invalidation, work control, policy changes and settlement
            # share this order: memory -> work -> policy -> reservation.
            memory_lock(tx)
            preview = tx.conn.execute(
                "SELECT * FROM ga_budget_reservations WHERE group_id=%s AND id=%s",
                (actor.group_id, identifier),
            ).fetchone()
            if preview is None:
                raise DomainError("not_found", "Budget reservation is outside this group")
            if preview["actor_id"] != actor.member_id:
                raise DomainError(
                    "permission_denied", "Only the original requester may start a paid call"
                )
            if preview["state"] != "reserved":
                return False
            self._payment_scope(tx, preview["work_id"], preview["source_event_id"])
            policy = tx.conn.execute(
                "SELECT *,clock_timestamp() AS now FROM ga_budget_policy WHERE group_id=%s FOR UPDATE",
                (actor.group_id,),
            ).fetchone()
            if policy is None:
                raise DomainError("budget_unconfigured", "A monetary budget is required")
            row = self._owned(tx, identifier)
            if row["state"] != "reserved":
                return False
            if row["policy_revision"] != actor.policy_revision:
                raise DomainError("policy_changed", "Reservation authority has changed")
            stamp = now or policy["now"]
            if stamp.utcoffset() is None:
                raise DomainError("invalid_input", "Budget clock must be timezone aware")
            day = stamp.astimezone(ZoneInfo(policy["time_zone"])).date()
            sums = self._budget_usage(tx, policy, day, exclude_daily_id=identifier)
            if (
                policy["blocked"]
                or (
                    policy["daily_limit"] is not None
                    and sums["daily"] + row["reserved_amount"] > policy["daily_limit"]
                )
                or (policy["total_limit"] is not None and sums["total"] > policy["total_limit"])
            ):
                raise DomainError(
                    "budget_exhausted", "Current monetary limits no longer permit this call"
                )
            price_row = tx.conn.execute(
                "SELECT body FROM ga_price_snapshots WHERE group_id=%s AND id=%s",
                (actor.group_id, row["price_id"]),
            ).fetchone()
            if price_row is None or (
                price_row["body"].get("test_only") and not self.allow_test_prices
            ):
                raise DomainError("price_missing", "Reservation has no authorized production price")
            tx.conn.execute(
                "UPDATE ga_budget_reservations SET state='inflight',budget_day=%s,updated_at=clock_timestamp() WHERE id=%s",
                (day, identifier),
            )
            return True

    def unknown(
        self, actor: ActorContext, identifier: str, reason: str = "provider_outcome_unknown"
    ) -> None:
        with self.store.transaction(actor) as tx:
            row = self._owned(tx, identifier)
            if row["state"] in {"settled", "released"}:
                return
            tx.conn.execute(
                "UPDATE ga_budget_reservations SET state='unknown',reason=%s,updated_at=clock_timestamp() WHERE id=%s",
                (reason[:120], identifier),
            )
            tx._audit("budget_unknown", identifier)

    def release_unstarted(self, actor: ActorContext, identifier: str) -> None:
        with self.store.transaction(actor) as tx:
            row = self._owned(tx, identifier)
            if row["state"] == "released":
                return
            if row["state"] != "reserved":
                raise DomainError(
                    "reconciliation_required", "An attempted call cannot be presumed unbilled"
                )
            tx.conn.execute(
                "UPDATE ga_budget_reservations SET state='released',updated_at=clock_timestamp() WHERE id=%s",
                (identifier,),
            )

    def settle(
        self,
        actor: ActorContext,
        identifier: str,
        actual_units: dict[str, Decimal],
        evidence_ref: str,
    ) -> Reservation:
        if not evidence_ref.strip():
            raise DomainError(
                "invalid_input", "Settlement needs a provider usage evidence reference"
            )
        with self.store.transaction(actor) as tx:
            # Same ordering as reserve: budget policy before reservation.
            policy = tx.conn.execute(
                "SELECT * FROM ga_budget_policy WHERE group_id=%s FOR UPDATE",
                (actor.group_id,),
            ).fetchone()
            limits_enabled = _limits_enabled(policy)
            row = self._owned(tx, identifier)
            if row["state"] == "settled":
                if _reservation(row).actual_units != actual_units:
                    raise DomainError("idempotency_conflict", "Settled usage cannot be overwritten")
                return _reservation(row, limits_enabled=limits_enabled)
            if row["state"] not in {"inflight", "unknown"}:
                raise DomainError("invalid_transition", "Only an attempted call can be settled")
            price_row = tx.conn.execute(
                "SELECT body FROM ga_price_snapshots WHERE group_id=%s AND id=%s",
                (actor.group_id, row["price_id"]),
            ).fetchone()
            assert price_row is not None
            amount = PriceSnapshot.model_validate(price_row["body"]).cost(actual_units)
            overrun = amount > row["reserved_amount"]
            result = tx.conn.execute(
                "UPDATE ga_budget_reservations SET state='settled',actual_amount=%s,actual_units=%s,evidence_ref=%s,overrun=%s,updated_at=clock_timestamp() WHERE id=%s RETURNING *",
                (amount, _units(actual_units), evidence_ref, overrun, identifier),
            ).fetchone()
            if overrun and limits_enabled:
                tx.conn.execute(
                    "UPDATE ga_budget_policy SET blocked=true WHERE group_id=%s", (actor.group_id,)
                )
            assert result is not None
            tx._audit("budget_settled", identifier, amount=str(amount), overrun=overrun)
            return _reservation(result, limits_enabled=limits_enabled)

    def report(self, actor: ActorContext) -> dict[str, Any]:
        with self.store.transaction(actor) as tx:
            memory_lock(tx)
            policy = tx.conn.execute(
                "SELECT *,clock_timestamp() AS now FROM ga_budget_policy WHERE group_id=%s "
                "FOR UPDATE",
                (actor.group_id,),
            ).fetchone()
            usage = None
            if policy is not None:
                usage = self._budget_usage(
                    tx, policy, policy["now"].astimezone(ZoneInfo(policy["time_zone"])).date()
                )
            rows = tx.conn.execute(
                "SELECT state,count(*) AS calls,COALESCE(sum(reserved_amount),0) AS reserved,COALESCE(sum(actual_amount),0) AS actual FROM ga_budget_reservations WHERE group_id=%s GROUP BY state ORDER BY state",
                (actor.group_id,),
            ).fetchall()
            return {
                "group_id": actor.group_id,
                "states": json.loads(json.dumps(rows, default=str)),
                "accounting": json.loads(json.dumps(usage, default=str)),
                "accounting_basis": "owner_reported_prefix_plus_conservative_suffix"
                if usage and usage["reconciliation"]
                else "conservative_estimate",
            }
