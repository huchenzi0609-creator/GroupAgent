"""Trusted cumulative bills never resolve or release unknown reservations."""

from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from psycopg.types.json import Jsonb

from groupagent.domain import DomainError

from .test_reconciliation import accounting, historical, reconcile, reserve, settled, snapshot
from .test_reconciliation import billing as billing

pytestmark = pytest.mark.postgres


def unknown(billing, key, amount, when):
    store, ledger, actor = billing
    item = reserve(ledger, actor, key, amount)
    assert ledger.begin(actor, item.id)
    ledger.unknown(actor, item.id)
    historical(store, actor, item.id, when)
    return item


def test_new_cumulative_report_preserves_v1_audit_unknown_rows_and_original_limit(billing):
    store, ledger, actor = billing
    base = datetime.now(UTC) - timedelta(days=3)
    settled(billing, "original-prefix", "20", base)
    first = reconcile(billing, base + timedelta(hours=1), reported="3")
    held = unknown(billing, "unknown-prefix", "2", base + timedelta(days=1))
    settled(billing, "later-prefix", "10", base + timedelta(days=1))
    before = snapshot(store, actor)
    second = reconcile(
        billing,
        base + timedelta(days=1, hours=1),
        key="new-cumulative",
        reported="4",
        previous=first["id"],
        preserve_unknown_holds=True,
    )
    assert snapshot(store, actor) == before
    assert second["schema_version"] == 2
    assert second["prefix_unknown_count"] == 1
    assert Decimal(second["prefix_unknown_held"]) == Decimal(2)
    usage = accounting(ledger, actor)
    assert Decimal(usage["gross_total"]) == Decimal(32)
    assert Decimal(usage["gross_settled_estimate"]) == Decimal(30)
    assert Decimal(usage["held"]) == Decimal(2)
    assert Decimal(usage["total"]) == Decimal(6)  # Actual total + all unknown holds.
    assert usage["reconciliation"]["id"] == second["id"]
    with store.connect() as conn:
        original = conn.execute(
            "SELECT details FROM ga_audit_events WHERE id=%s", (int(first["id"]),)
        ).fetchone()["details"]
        current = conn.execute(
            "SELECT state,actual_amount FROM ga_budget_reservations WHERE id=%s", (held.id,)
        ).fetchone()
    assert {**original, "id": first["id"]} == first
    assert "prefix_unknown_held" not in original  # The v1 payload/digest is unchanged.
    assert current["state"] == "unknown" and current["actual_amount"] is None
    exact = reserve(ledger, actor, "exact-original-limit", "44")
    assert ledger.begin(actor, exact.id)
    with pytest.raises(DomainError, match="budget"):
        reserve(ledger, actor, "over-original-limit", "0.000001")


@pytest.mark.parametrize("state", ["reserved", "inflight"])
def test_active_prefix_is_still_rejected_by_explicit_cumulative_mode(billing, state):
    store, ledger, actor = billing
    base = datetime.now(UTC) - timedelta(days=2)
    item = reserve(ledger, actor, "unfinished", "2")
    if state == "inflight":
        assert ledger.begin(actor, item.id)
    historical(store, actor, item.id, base)
    before = snapshot(store, actor)
    with pytest.raises(DomainError) as failed:
        reconcile(billing, base + timedelta(hours=1), preserve_unknown_holds=True)
    assert failed.value.code == "reconciliation_required"
    assert snapshot(store, actor) == before


def test_unknown_updated_after_report_cutoff_cannot_be_reconciled(billing):
    store, ledger, actor = billing
    base = datetime.now(UTC) - timedelta(days=2)
    item = unknown(billing, "late-unknown", "2", base)
    ledger.unknown(actor, item.id, "later-financial-information")
    with pytest.raises(DomainError) as failed:
        reconcile(billing, base + timedelta(hours=1), preserve_unknown_holds=True)
    assert failed.value.code == "reconciliation_required"


def test_explicit_mode_is_idempotent_and_cannot_be_downgraded_by_same_key(billing):
    base = datetime.now(UTC) - timedelta(days=2)
    unknown(billing, "unknown", "2", base)
    cutoff = base + timedelta(hours=1)
    record = reconcile(billing, cutoff, reported="1", preserve_unknown_holds=True)
    assert reconcile(billing, cutoff, reported="1", preserve_unknown_holds=True) == record
    with pytest.raises(DomainError) as failed:
        reconcile(billing, cutoff, reported="1")
    assert failed.value.code == "idempotency_conflict"
    assert Decimal(accounting(billing[1], billing[2])["total"]) == Decimal(3)


@pytest.mark.parametrize("tamper", ["unknown_hold", "unknown_state", "audit_count"])
def test_unknown_prefix_or_v2_audit_changes_fail_closed(billing, tamper):
    store, ledger, actor = billing
    base = datetime.now(UTC) - timedelta(days=2)
    item = unknown(billing, "unknown", "2", base)
    record = reconcile(billing, base + timedelta(hours=1), preserve_unknown_holds=True)
    with store.transaction(actor) as tx:
        if tamper == "unknown_hold":
            tx.conn.execute(
                "UPDATE ga_budget_reservations SET reserved_amount=1 WHERE id=%s", (item.id,)
            )
        elif tamper == "unknown_state":
            tx.conn.execute(
                "UPDATE ga_budget_reservations SET state='released' WHERE id=%s", (item.id,)
            )
        else:
            tx.conn.execute(
                "UPDATE ga_audit_events SET details=jsonb_set(details,'{prefix_unknown_count}','0'::jsonb) WHERE id=%s",
                (int(record["id"]),),
            )
    for action in (
        lambda: ledger.report(actor),
        lambda: reserve(ledger, actor, "after-tamper", "1"),
    ):
        with pytest.raises(DomainError) as failed:
            action()
        assert failed.value.code == "reconciliation_required"


def test_later_genuine_unknown_settlement_does_not_silently_reuse_old_cumulative_bill(billing):
    _, ledger, actor = billing
    base = datetime.now(UTC) - timedelta(days=2)
    item = unknown(billing, "later-settled", "2", base)
    reconcile(billing, base + timedelta(hours=1), preserve_unknown_holds=True)
    ledger.settle(actor, item.id, {"units": Decimal(1)}, "fixture://genuine-later-usage")
    with pytest.raises(DomainError) as failed:
        ledger.report(actor)
    assert failed.value.code == "reconciliation_required"


def test_daily_limit_conservatively_includes_report_and_unknown_hold(billing):
    _, ledger, actor = billing
    base = (datetime.now(UTC) - timedelta(days=2)).replace(hour=1)
    settled(billing, "settled", "20", base)
    unknown(billing, "unknown", "2", base)
    reconcile(billing, base + timedelta(hours=1), reported="5", preserve_unknown_holds=True)
    ledger.configure(
        actor, currency="CNY", daily_limit=Decimal(8), total_limit=Decimal(50), expected_revision=1
    )
    exact = reserve(ledger, actor, "daily-exact", "1", now=base)
    assert ledger.begin(actor, exact.id, now=base)
    with pytest.raises(DomainError) as failed:
        reserve(ledger, actor, "daily-overflow", "0.000001", now=base)
    assert failed.value.code == "budget_exhausted"


def test_concurrent_calls_share_the_adjusted_limit_without_releasing_unknowns(billing):
    _, ledger, actor = billing
    base = datetime.now(UTC) - timedelta(days=2)
    settled(billing, "settled", "20", base)
    unknown(billing, "unknown", "2", base)
    reconcile(billing, base + timedelta(hours=1), reported="4", preserve_unknown_holds=True)

    def contender(index):
        try:
            item = reserve(ledger, actor, f"parallel-{index}", "10")
            assert ledger.begin(actor, item.id)
            return item.id
        except DomainError as error:
            assert error.code == "budget_exhausted"
            return None

    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(contender, range(10)))
    assert sum(value is not None for value in results) == 4
    assert Decimal(accounting(ledger, actor)["total"]) == Decimal(46)


@pytest.mark.parametrize("value", [1, "yes", None])
def test_unknown_hold_mode_requires_an_explicit_boolean(billing, value):
    base = datetime.now(UTC) - timedelta(days=2)
    settled(billing, "settled", "20", base)
    with pytest.raises(DomainError) as failed:
        reconcile(billing, base + timedelta(hours=1), preserve_unknown_holds=value)
    assert failed.value.code == "invalid_input"


@pytest.mark.parametrize("payload", [None, [], True, {"schema_version": 99}])
def test_malformed_audit_payload_cannot_escape_integrity_failure(billing, payload):
    store, ledger, actor = billing
    base = datetime.now(UTC) - timedelta(days=2)
    unknown(billing, "unknown", "2", base)
    record = reconcile(billing, base + timedelta(hours=1), preserve_unknown_holds=True)
    with store.transaction(actor) as tx:
        tx.conn.execute(
            "UPDATE ga_audit_events SET details=%s WHERE id=%s",
            (Jsonb(payload), int(record["id"])),
        )
    with pytest.raises(DomainError) as failed:
        ledger.report(actor)
    assert failed.value.code == "reconciliation_required"
