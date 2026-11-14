from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import uuid4

import pytest

from groupagent.billing import BudgetLedger
from groupagent.domain import DomainError, EventKind, GroupEvent, WorkState
from groupagent.memory import MemoryService

from .test_ledger import reserve, setup

__all__ = ["setup"]
pytestmark = pytest.mark.postgres


@pytest.mark.parametrize("change", ["blocked", "daily", "total", "overrun"])
def test_reserved_call_is_revalidated_before_start_after_budget_change(setup, change):
    store, ledger, actor, _ = setup
    planned = reserve(ledger, actor, "planned", 500)
    if change == "blocked":
        with store.transaction(actor) as tx:
            tx.conn.execute(
                "UPDATE ga_budget_policy SET blocked=true WHERE group_id=%s", (actor.group_id,)
            )
    elif change in {"daily", "total"}:
        ledger.configure(
            actor,
            currency="CNY",
            daily_limit=Decimal("0.2") if change == "daily" else Decimal(1),
            total_limit=Decimal("0.2") if change == "total" else Decimal(2),
            expected_revision=1,
        )
    else:
        spent = reserve(ledger, actor, "spent", 100)
        assert ledger.begin(actor, spent.id)
        ledger.settle(actor, spent.id, {"input_tokens": Decimal(101)}, "synthetic:overrun")
    with pytest.raises(DomainError) as caught:
        ledger.begin(actor, planned.id)
    assert caught.value.code == "budget_exhausted"
    with store.transaction(actor) as tx:
        assert (
            tx.conn.execute(
                "SELECT state FROM ga_budget_reservations WHERE id=%s", (planned.id,)
            ).fetchone()["state"]
            == "reserved"
        )
    ledger.release_unstarted(actor, planned.id)


def test_overnight_reservation_is_charged_against_actual_start_day(setup):
    store, ledger, actor, _ = setup
    before = datetime(2026, 11, 14, 15, 59, tzinfo=UTC)
    after = before + timedelta(minutes=2)
    old = reserve(ledger, actor, "yesterday", 600, now=before)
    today = reserve(ledger, actor, "today", 500, now=after)
    with pytest.raises(DomainError) as caught:
        ledger.begin(actor, old.id, now=after)
    assert caught.value.code == "budget_exhausted"
    ledger.release_unstarted(actor, today.id)
    assert ledger.begin(actor, old.id, now=after)
    with store.transaction(actor) as tx:
        assert (
            str(
                tx.conn.execute(
                    "SELECT budget_day FROM ga_budget_reservations WHERE id=%s", (old.id,)
                ).fetchone()["budget_day"]
            )
            == "2026-11-15"
        )


def source(store, actor):
    now = datetime.now(UTC)
    identity = uuid4().hex
    value = GroupEvent(
        id=identity,
        source_key="billing-bound:" + identity,
        group_id=actor.group_id,
        actor_id=actor.member_id,
        kind=EventKind.TEXT,
        text="synthetic billing request",
        occurred_at=now,
        received_at=now,
    )
    with store.transaction(actor) as tx:
        tx.ingest_event(value, identity, "1", 0)
    return value


@pytest.mark.parametrize(
    "change", ["forgotten_source", "forgotten_dependency", "cancelled", "paused", "completed"]
)
def test_reserved_call_cannot_start_after_source_or_work_authority_is_withdrawn(setup, change):
    store, ledger, actor, _ = setup
    request = source(store, actor)
    dependency = source(store, actor)
    with store.transaction(actor) as tx:
        work = tx.create_work(
            "synthetic budget work",
            request.id,
            scope={"source_fingerprints": {dependency.id: tx.event_fingerprint(dependency.id)}},
        )
    planned = reserve(ledger, actor, "planned", 100, work_id=work.id, source_event_id=request.id)
    if change.startswith("forgotten"):
        MemoryService(store).forget(
            actor, event_ids=[request.id if change == "forgotten_source" else dependency.id]
        )
    elif change == "completed":
        # Fixture isolates the billing boundary; work lifecycle itself is tested elsewhere.
        with store.transaction(actor) as tx:
            tx.conn.execute(
                "UPDATE ga_work_orders SET state='completed' WHERE group_id=%s AND id=%s",
                (actor.group_id, work.id),
            )
    else:
        with store.transaction(actor) as tx:
            tx.transition_work(
                work.id,
                WorkState.CANCELLED if change == "cancelled" else WorkState.PAUSED,
                expected_revision=work.revision,
                reason="synthetic-control",
            )
    with pytest.raises(DomainError) as caught:
        ledger.begin(actor, planned.id)
    assert caught.value.code in {"invalid_transition", "source_unavailable"}
    with pytest.raises(DomainError):
        reserve(ledger, actor, "new-after-change", 100, work_id=work.id, source_event_id=request.id)


def test_strict_ledger_cannot_start_preexisting_fixture_reservation(setup):
    store, ledger, actor, _ = setup
    planned = reserve(ledger, actor)
    with pytest.raises(DomainError) as caught:
        BudgetLedger(store).begin(actor, planned.id)
    assert caught.value.code == "price_missing"


def test_paid_usage_can_still_settle_after_request_forget(setup):
    store, ledger, actor, _ = setup
    request = source(store, actor)
    planned = reserve(ledger, actor, source_event_id=request.id)
    assert ledger.begin(actor, planned.id)
    MemoryService(store).forget(actor, event_ids=[request.id])
    settled = ledger.settle(
        actor, planned.id, {"input_tokens": Decimal(90)}, "synthetic:actual-usage"
    )
    assert settled.state == "settled" and settled.actual_amount == Decimal("0.09")
