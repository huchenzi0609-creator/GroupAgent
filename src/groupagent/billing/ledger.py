from __future__ import annotations

import json
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from uuid import uuid4
from zoneinfo import ZoneInfo

from psycopg.types.json import Jsonb

from groupagent.domain import ActorContext, DomainError, Role
from groupagent.memory.common import memory_lock
from groupagent.persistence import PostgresStore

from .models import QUANTUM, PriceSnapshot, Reservation


def _units(value: dict[str, Decimal]) -> Jsonb:
    return Jsonb({key: str(amount) for key, amount in value.items()})


def _reservation(row: dict[str, Any], *, is_new: bool = False) -> Reservation:
    return Reservation.model_validate(
        {**{key: row[key] for key in Reservation.model_fields if key in row}, "is_new": is_new}
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
        daily_limit: Decimal,
        total_limit: Decimal | None = None,
        time_zone: str = "Asia/Shanghai",
        expected_revision: int | None = None,
    ) -> int:
        if actor.role != Role.OWNER:
            raise DomainError("permission_denied", "Only the group owner sets monetary limits")
        if currency not in {"CNY", "USD"}:
            raise DomainError("invalid_input", "Unsupported budget currency")
        ZoneInfo(time_zone)
        limits = [daily_limit] + ([total_limit] if total_limit is not None else [])
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
            tx._audit("budget_configured", actor.group_id, currency=currency)
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
                previous = _reservation(old)
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
            sums = tx.conn.execute(
                "SELECT COALESCE(sum(CASE WHEN state='settled' THEN actual_amount WHEN state IN ('reserved','inflight','unknown') THEN reserved_amount ELSE 0 END),0) AS total, COALESCE(sum(CASE WHEN budget_day=%s THEN CASE WHEN state='settled' THEN actual_amount WHEN state IN ('reserved','inflight','unknown') THEN reserved_amount ELSE 0 END ELSE 0 END),0) AS daily FROM ga_budget_reservations WHERE group_id=%s",
                (day, actor.group_id),
            ).fetchone()
            assert sums is not None
            if (
                policy["blocked"]
                or sums["daily"] + amount > policy["daily_limit"]
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
            return _reservation(row, is_new=True)

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
            sums = tx.conn.execute(
                "SELECT COALESCE(sum(CASE WHEN state='settled' THEN actual_amount WHEN state IN ('reserved','inflight','unknown') THEN reserved_amount ELSE 0 END),0) AS total, "
                "COALESCE(sum(CASE WHEN budget_day=%s AND id<>%s THEN CASE WHEN state='settled' THEN actual_amount WHEN state IN ('reserved','inflight','unknown') THEN reserved_amount ELSE 0 END ELSE 0 END),0) AS daily "
                "FROM ga_budget_reservations WHERE group_id=%s",
                (day, identifier, actor.group_id),
            ).fetchone()
            assert sums is not None
            if (
                policy["blocked"]
                or sums["daily"] + row["reserved_amount"] > policy["daily_limit"]
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
            tx.conn.execute(
                "SELECT group_id FROM ga_budget_policy WHERE group_id=%s FOR UPDATE",
                (actor.group_id,),
            ).fetchone()
            row = self._owned(tx, identifier)
            if row["state"] == "settled":
                if _reservation(row).actual_units != actual_units:
                    raise DomainError("idempotency_conflict", "Settled usage cannot be overwritten")
                return _reservation(row)
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
            if overrun:
                tx.conn.execute(
                    "UPDATE ga_budget_policy SET blocked=true WHERE group_id=%s", (actor.group_id,)
                )
            assert result is not None
            tx._audit("budget_settled", identifier, amount=str(amount), overrun=overrun)
            return _reservation(result)

    def report(self, actor: ActorContext) -> dict[str, Any]:
        with self.store.transaction(actor) as tx:
            rows = tx.conn.execute(
                "SELECT state,count(*) AS calls,COALESCE(sum(reserved_amount),0) AS reserved,COALESCE(sum(actual_amount),0) AS actual FROM ga_budget_reservations WHERE group_id=%s GROUP BY state ORDER BY state",
                (actor.group_id,),
            ).fetchall()
            return {"group_id": actor.group_id, "states": json.loads(json.dumps(rows, default=str))}
