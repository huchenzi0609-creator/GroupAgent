from __future__ import annotations

import os
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from hashlib import sha256
from uuid import uuid4
from zoneinfo import ZoneInfo

import pytest
from psycopg.conninfo import make_conninfo

from groupagent.billing import BudgetLedger, PriceSnapshot
from groupagent.domain import ActorContext, DomainError, Role
from groupagent.persistence import PostgresStore

pytestmark = pytest.mark.postgres
EVIDENCE = sha256(b"synthetic owner-reported billing evidence").hexdigest()


@pytest.fixture
def billing():
    dsn = os.environ.get("GROUPAGENT_TEST_DSN")
    if not dsn:
        pytest.skip("Requires isolated PostgreSQL")
    store = PostgresStore(dsn)
    store.migrate()
    group = "billing-reconciliation-" + uuid4().hex
    store.register_group(group)
    actor = ActorContext(group_id=group, member_id="synthetic-owner", role=Role.OWNER)
    ledger = BudgetLedger(store, allow_test_prices=True)
    ledger.configure(actor, currency="CNY", daily_limit=Decimal(50), total_limit=Decimal(50))
    ledger.register_price(
        actor,
        PriceSnapshot(
            id="synthetic",
            provider="synthetic",
            operation="text",
            currency="CNY",
            rates={"units": Decimal(1)},
            source_url="fixture://synthetic-billing-not-a-provider-invoice",
            captured_at=datetime.now(UTC),
            test_only=True,
        ),
    )
    return store, ledger, actor


def reserve(ledger, actor, key, amount, *, now=None):
    return ledger.reserve(actor, key, "text", "synthetic", {"units": Decimal(amount)}, now=now)


def historical(store, actor, identifier, when, *, keep_updated=False):
    # Synthetic fixture only: retain genuine API state transitions and move their
    # timestamps to a historical billing prefix without sleeping or provider calls.
    with store.transaction(actor) as tx:
        tx.conn.execute(
            "UPDATE ga_budget_reservations SET created_at=%s,budget_day=%s"
            + ("" if keep_updated else ",updated_at=%s")
            + " WHERE group_id=%s AND id=%s",
            (when, when.astimezone(ZoneInfo("Asia/Shanghai")).date())
            + (() if keep_updated else (when,))
            + (actor.group_id, identifier),
        )


def settled(billing, key, amount, when):
    store, ledger, actor = billing
    item = reserve(ledger, actor, key, amount)
    assert ledger.begin(actor, item.id)
    ledger.settle(actor, item.id, {"units": Decimal(amount)}, "fixture://usage/" + key)
    historical(store, actor, item.id, when)
    return item


def reconcile(billing, cutoff, *, key="console-1", reported="5", previous=None, **changes):
    store, ledger, actor = billing
    with store.connect() as conn:
        row = conn.execute(
            "SELECT count(*) AS count,COALESCE(sum(actual_amount) "
            "FILTER (WHERE state='settled'),0) AS gross FROM ga_budget_reservations "
            "WHERE group_id=%s AND created_at<=%s",
            (actor.group_id, cutoff),
        ).fetchone()
    values = {
        "key": key,
        "as_of": cutoff,
        "currency": "CNY",
        "reported_total": Decimal(reported),
        "expected_prefix_count": row["count"],
        "expected_prefix_gross": row["gross"],
        "evidence_ref": "fixture://user-reported-provider-console",
        "evidence_sha256": EVIDENCE,
        "expected_previous_id": previous,
        **changes,
    }
    return ledger.reconcile_billing(actor, **values)


def snapshot(store, actor):
    with store.connect() as conn:
        return {
            table: conn.execute(
                f"SELECT * FROM {table} WHERE group_id=%s ORDER BY 1", (actor.group_id,)
            ).fetchall()
            for table in ("ga_budget_policy", "ga_price_snapshots", "ga_budget_reservations")
        }


def accounting(ledger, actor):
    return ledger.report(actor)["accounting"]


@pytest.mark.parametrize("state", ["reserved", "inflight", "unknown"])
def test_unresolved_prefix_cannot_be_reconciled(billing, state):
    store, ledger, actor = billing
    base = datetime.now(UTC) - timedelta(days=2)
    item = reserve(ledger, actor, "unresolved", "1")
    if state != "reserved":
        assert ledger.begin(actor, item.id)
    if state == "unknown":
        ledger.unknown(actor, item.id)
    historical(store, actor, item.id, base)
    before = snapshot(store, actor)
    with pytest.raises(DomainError) as caught:
        reconcile(billing, base + timedelta(hours=1), reported="0")
    assert caught.value.code == "reconciliation_required"
    assert snapshot(store, actor) == before


def test_settlement_after_cutoff_cannot_be_retroactively_reconciled(billing):
    store, ledger, actor = billing
    base = datetime.now(UTC) - timedelta(days=2)
    item = settled(billing, "late-settlement", "1", base)
    with store.transaction(actor) as tx:
        tx.conn.execute(
            "UPDATE ga_budget_reservations SET updated_at=clock_timestamp() WHERE id=%s", (item.id,)
        )
    with pytest.raises(DomainError) as caught:
        reconcile(billing, base + timedelta(hours=1))
    assert caught.value.code == "reconciliation_required"


def test_reconciliation_preserves_gross_policy_prices_and_suffix_unknown(billing):
    store, ledger, actor = billing
    base = datetime.now(UTC) - timedelta(days=2)
    settled(billing, "prefix", "40", base)
    unknown = reserve(ledger, actor, "unknown-suffix", "0.108410")
    assert ledger.begin(actor, unknown.id)
    ledger.unknown(actor, unknown.id)
    before = snapshot(store, actor)
    record = reconcile(billing, base + timedelta(hours=1))
    assert snapshot(store, actor) == before
    usage = accounting(ledger, actor)
    assert usage["gross_settled_estimate"] == "40.000000"
    assert Decimal(usage["gross_total"]) == Decimal("40.108410")
    assert usage["held"] == "0.108410"
    assert Decimal(usage["total"]) == Decimal("5.108410")
    assert usage["reconciliation"]["id"] == record["id"]
    assert record["source"] == "user_reported_provider_console"
    final = reserve(ledger, actor, "up-to-original-50", "44.891590")
    assert ledger.begin(actor, final.id)
    with pytest.raises(DomainError) as caught:
        reserve(ledger, actor, "over-original-50", "0.000001")
    assert caught.value.code == "budget_exhausted"
    assert accounting(ledger, actor)["held"] == "45.000000"


def test_prefix_digest_is_independent_of_connection_time_zone(billing):
    store, _, actor = billing
    base = datetime.now(UTC) - timedelta(days=2)
    settled(billing, "prefix", "40", base)
    reconcile(billing, base + timedelta(hours=1))
    utc_store = PostgresStore(make_conninfo(store.dsn, options="-c timezone=UTC"))
    utc_ledger = BudgetLedger(utc_store, allow_test_prices=True)
    assert Decimal(accounting(utc_ledger, actor)["total"]) == Decimal(5)


def test_same_key_retry_is_idempotent_under_concurrency(billing):
    store, ledger, actor = billing
    base = datetime.now(UTC) - timedelta(days=2)
    settled(billing, "prefix", "40", base)
    cutoff = base + timedelta(hours=1)
    with ThreadPoolExecutor(max_workers=5) as pool:
        records = list(pool.map(lambda _: reconcile(billing, cutoff), range(5)))
    assert len({record["id"] for record in records}) == 1
    with store.connect() as conn:
        assert (
            conn.execute(
                "SELECT count(*) AS count FROM ga_audit_events WHERE group_id=%s "
                "AND action='budget_billing_reconciled'",
                (actor.group_id,),
            ).fetchone()["count"]
            == 1
        )
    assert Decimal(accounting(ledger, actor)["total"]) == Decimal(5)


@pytest.mark.parametrize(
    "change",
    [
        {"reported_total": Decimal(6)},
        {"expected_prefix_count": 2},
        {"expected_prefix_gross": Decimal(39)},
        {"evidence_sha256": "a" * 64},
        {"evidence_ref": "fixture://different-report"},
        {"expected_previous_id": "1"},
    ],
)
def test_reusing_key_with_different_evidence_fails(billing, change):
    base = datetime.now(UTC) - timedelta(days=2)
    settled(billing, "prefix", "40", base)
    cutoff = base + timedelta(hours=1)
    reconcile(billing, cutoff)
    with pytest.raises(DomainError) as caught:
        reconcile(billing, cutoff, **change)
    assert caught.value.code == "idempotency_conflict"


@pytest.mark.parametrize(
    "change",
    [
        {"expected_prefix_count": 2},
        {"expected_prefix_gross": Decimal("39.99")},
    ],
)
def test_operator_expected_prefix_must_match_sql(billing, change):
    base = datetime.now(UTC) - timedelta(days=2)
    settled(billing, "prefix", "40", base)
    with pytest.raises(DomainError) as caught:
        reconcile(billing, base + timedelta(hours=1), **change)
    assert caught.value.code == "version_conflict"


def test_later_cumulative_report_replaces_prefix_without_double_subtraction(billing):
    _, ledger, actor = billing
    base = datetime.now(UTC) - timedelta(days=3)
    settled(billing, "prefix-1", "20", base)
    first_cutoff = base + timedelta(hours=1)
    first = reconcile(billing, first_cutoff, reported="3")
    settled(billing, "prefix-2", "10", base + timedelta(days=1))
    second = reconcile(
        billing,
        base + timedelta(days=1, hours=1),
        key="console-2",
        reported="4",
        previous=first["id"],
    )
    assert Decimal(accounting(ledger, actor)["gross_total"]) == Decimal(30)
    assert Decimal(accounting(ledger, actor)["total"]) == Decimal(4)
    assert reconcile(billing, first_cutoff, reported="3") == first
    assert accounting(ledger, actor)["reconciliation"]["id"] == second["id"]


@pytest.mark.parametrize("change", ["stale_previous", "same_cutoff", "refund"])
def test_later_report_requires_current_predecessor_and_monotone_cumulative(billing, change):
    base = datetime.now(UTC) - timedelta(days=2)
    settled(billing, "prefix", "40", base)
    cutoff = base + timedelta(hours=1)
    first = reconcile(billing, cutoff)
    with pytest.raises(DomainError) as caught:
        reconcile(
            billing,
            cutoff if change == "same_cutoff" else cutoff + timedelta(hours=1),
            key="console-2",
            reported="4" if change == "refund" else "6",
            previous=None if change == "stale_previous" else first["id"],
        )
    assert caught.value.code == "version_conflict"


@pytest.mark.parametrize("day_offset,headroom", [(0, "2"), (1, "2"), (2, "6")])
def test_daily_budget_uses_entire_report_on_each_covered_day(billing, day_offset, headroom):
    _, ledger, actor = billing
    base = (datetime.now(UTC) - timedelta(days=4)).replace(hour=1)
    settled(billing, "day-1", "20", base)
    settled(billing, "day-2", "10", base + timedelta(days=1))
    reconcile(billing, base + timedelta(days=1, hours=1), reported="4")
    ledger.configure(
        actor, currency="CNY", daily_limit=Decimal(6), total_limit=Decimal(50), expected_revision=1
    )
    day = base + timedelta(days=day_offset)
    exact = reserve(ledger, actor, "exact-daily", headroom, now=day)
    assert ledger.begin(actor, exact.id, now=day)
    with pytest.raises(DomainError) as caught:
        reserve(ledger, actor, "daily-overflow", "0.000001", now=day)
    assert caught.value.code == "budget_exhausted"


def test_reserve_and_begin_revalidate_the_same_adjusted_capacity(billing):
    _, ledger, actor = billing
    base = datetime.now(UTC) - timedelta(days=2)
    settled(billing, "prefix", "40", base)
    first = reconcile(billing, base + timedelta(hours=1))
    planned = reserve(ledger, actor, "planned", "40")
    reconcile(
        billing,
        base + timedelta(hours=2),
        key="higher-cumulative",
        reported="15",
        previous=first["id"],
    )
    for action in [
        lambda: ledger.begin(actor, planned.id),
        lambda: reserve(ledger, actor, "new", "0.000001"),
    ]:
        with pytest.raises(DomainError) as caught:
            action()
        assert caught.value.code == "budget_exhausted"


def test_concurrent_adjusted_reservations_cannot_exceed_original_limit(billing):
    _, ledger, actor = billing
    base = datetime.now(UTC) - timedelta(days=2)
    settled(billing, "prefix", "40", base)
    reconcile(billing, base + timedelta(hours=1))

    def contender(index):
        try:
            item = reserve(ledger, actor, f"parallel-{index}", "10")
            assert ledger.begin(actor, item.id)
            return item.id
        except DomainError as error:
            assert error.code == "budget_exhausted"
            return None

    with ThreadPoolExecutor(max_workers=8) as pool:
        attempts = list(pool.map(contender, range(10)))
    assert sum(item is not None for item in attempts) == 4
    assert Decimal(accounting(ledger, actor)["total"]) == Decimal(45)


@pytest.mark.parametrize(
    "tamper", ["prefix_amount", "prefix_metadata", "audit_payload", "audit_actor"]
)
def test_prefix_or_audit_integrity_failure_closes_report_reserve_and_begin(billing, tamper):
    store, ledger, actor = billing
    base = datetime.now(UTC) - timedelta(days=2)
    old = settled(billing, "prefix", "40", base)
    record = reconcile(billing, base + timedelta(hours=1))
    planned = reserve(ledger, actor, "planned", "1")
    with store.transaction(actor) as tx:
        if tamper.startswith("prefix"):
            tx.conn.execute(
                "UPDATE ga_budget_reservations SET "
                + ("actual_amount=39" if tamper == "prefix_amount" else "reason='tampered'")
                + " WHERE id=%s",
                (old.id,),
            )
        elif tamper == "audit_payload":
            tx.conn.execute(
                "UPDATE ga_audit_events SET details=jsonb_set(details,'{reported_total}',"
                "'\"0.000000\"'::jsonb) WHERE id=%s",
                (int(record["id"]),),
            )
        else:
            tx.conn.execute(
                "UPDATE ga_audit_events SET actor_id='tampered' WHERE id=%s", (int(record["id"]),)
            )
    for action in [
        lambda: ledger.report(actor),
        lambda: ledger.begin(actor, planned.id),
        lambda: reserve(ledger, actor, "new-after-tamper", "1"),
    ]:
        with pytest.raises(DomainError) as caught:
            action()
        assert caught.value.code == "reconciliation_required"


@pytest.mark.parametrize("role", [Role.MEMBER, Role.OBSERVER, Role.MAINTAINER])
def test_only_trusted_owner_operator_can_reconcile(billing, role):
    _, ledger, actor = billing
    denied = actor.model_copy(update={"role": role})
    with pytest.raises(DomainError) as caught:
        ledger.reconcile_billing(
            denied,
            key="unauthorized",
            as_of=datetime.now(UTC),
            currency="CNY",
            reported_total=Decimal(1),
            expected_prefix_count=1,
            expected_prefix_gross=Decimal(1),
            evidence_ref="fixture://not-authority",
            evidence_sha256=EVIDENCE,
        )
    assert caught.value.code == "permission_denied"


@pytest.mark.parametrize(
    "change",
    [
        {"key": " "},
        {"evidence_ref": " "},
        {"evidence_sha256": "not-a-sha256"},
        {"reported_total": Decimal("NaN")},
        {"reported_total": Decimal("-1")},
        {"reported_total": Decimal("0.0000001")},
        {"reported_total": 4.83},
        {"expected_prefix_count": True},
        {"as_of": datetime.now()},
        {"as_of": datetime.now(UTC) + timedelta(days=10)},
    ],
)
def test_invalid_evidence_never_changes_financial_records(billing, change):
    store, _, actor = billing
    base = datetime.now(UTC) - timedelta(days=2)
    settled(billing, "prefix", "40", base)
    before = snapshot(store, actor)
    with pytest.raises(DomainError) as caught:
        reconcile(billing, base + timedelta(hours=1), **change)
    assert caught.value.code == "invalid_input"
    assert snapshot(store, actor) == before
