"""Upgrade 014 to 015 without changing historical prices, holds or freezes."""

from __future__ import annotations

import os
from datetime import UTC, datetime
from decimal import Decimal
from importlib.resources import files
from uuid import uuid4

import psycopg
import pytest
from psycopg import sql
from psycopg.conninfo import make_conninfo

from groupagent.billing import BudgetLedger, PriceSnapshot
from groupagent.domain import ActorContext, Role
from groupagent.persistence import PostgresStore
from groupagent.persistence import postgres as postgres_module

pytestmark = pytest.mark.postgres


def test_unlimited_upgrade_retains_old_checksums_ledger_and_administrative_freeze(
    tmp_path, monkeypatch
):
    dsn = os.environ.get("GROUPAGENT_TEST_DSN")
    if not dsn:
        pytest.skip("Requires isolated PostgreSQL")
    base = PostgresStore(dsn)
    schema = "unlimited_upgrade_" + uuid4().hex
    with base.connect() as conn:
        conn.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
    store = PostgresStore(make_conninfo(dsn, options=f"-c search_path={schema}"))
    old = tmp_path / "migrations"
    old.mkdir()
    for path in files("groupagent.persistence").joinpath("migrations").iterdir():
        if path.name.endswith(".sql") and int(path.name.split("_", 1)[0]) <= 14:
            (old / path.name).write_text(path.read_text(), encoding="utf-8")
    try:
        with monkeypatch.context() as patch:
            patch.setattr(postgres_module, "files", lambda _: tmp_path)
            assert store.migrate() == 14
        store.register_group("synthetic-upgrade")
        actor = ActorContext(
            group_id="synthetic-upgrade", member_id="synthetic-owner", role=Role.OWNER
        )
        ledger = BudgetLedger(store, allow_test_prices=True)
        ledger.configure(actor, currency="CNY", daily_limit=Decimal(1), total_limit=Decimal(2))
        ledger.register_price(
            actor,
            PriceSnapshot(
                id="fixture",
                provider="synthetic",
                operation="text",
                currency="CNY",
                rates={"units": Decimal(1)},
                source_url="fixture://not-a-commercial-price",
                captured_at=datetime.now(UTC),
                test_only=True,
            ),
        )
        held = ledger.reserve(actor, "held", "text", "fixture", {"units": Decimal("0.2")})
        assert ledger.begin(actor, held.id)
        ledger.unknown(actor, held.id)
        spent = ledger.reserve(actor, "spent", "text", "fixture", {"units": Decimal("0.1")})
        assert ledger.begin(actor, spent.id)
        ledger.settle(actor, spent.id, {"units": Decimal("0.3")}, "synthetic:usage")

        def snapshot():
            with store.connect() as conn:
                return {
                    table: conn.execute(f"SELECT * FROM {table} ORDER BY 1").fetchall()
                    for table in (
                        "ga_budget_policy",
                        "ga_budget_reservations",
                        "ga_price_snapshots",
                        "ga_audit_events",
                    )
                }

        before = snapshot()
        with store.connect() as conn:
            checksums = conn.execute(
                "SELECT * FROM ga_schema_migrations ORDER BY version"
            ).fetchall()
        assert store.migrate() == 16 and store.migrate() == 16
        assert snapshot() == before
        with store.connect() as conn:
            assert (
                conn.execute(
                    "SELECT * FROM ga_schema_migrations WHERE version<=14 ORDER BY version"
                ).fetchall()
                == checksums
            )
        assert (
            ledger.configure(
                actor, currency="CNY", daily_limit=None, total_limit=None, expected_revision=1
            )
            == 2
        )
        with store.connect() as conn:
            policy = conn.execute("SELECT * FROM ga_budget_policy").fetchone()
            assert policy["daily_limit"] is None and policy["total_limit"] is None
            assert policy["blocked"]  # Unlimited never removes a pre-existing freeze.
        for invalid in (Decimal(0), Decimal(-1)):
            with pytest.raises(psycopg.errors.CheckViolation), store.connect() as conn:
                conn.execute("UPDATE ga_budget_policy SET daily_limit=%s", (invalid,))
    finally:
        with base.connect() as conn:
            conn.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))
