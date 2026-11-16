"""Explicit unbounded monetary policy survives ordinary runtime restarts."""

import os
from decimal import Decimal
from uuid import uuid4

import pytest

from groupagent.config import AppConfig, GroupConfig, RuntimeConfig, SpendConfig
from groupagent.domain import ActorContext, Role
from groupagent.persistence import PostgresStore
from groupagent.runtime.app import build_app


def config(spend):
    return AppConfig(
        group=GroupConfig(
            internal_id="synthetic-spend",
            external_group_id="synthetic@chatroom",
            owner_ids=["owner"],
        ),
        spend=spend,
    )


def test_unlimited_requires_explicit_authorization_and_still_needs_prices():
    price_only = SpendConfig(pricing_snapshot_id="synthetic-prices")
    assert not price_only.configured_for_paid_calls()
    assert "spend_limit_or_price_snapshot_missing" in config(price_only).production_blockers()
    unbounded = price_only.model_copy(update={"production_requires_explicit_limit": False})
    assert unbounded.monetary_limits_disabled and unbounded.configured_for_paid_calls()
    assert "spend_limit_or_price_snapshot_missing" not in config(unbounded).production_blockers()
    assert not SpendConfig(production_requires_explicit_limit=False).configured_for_paid_calls()


def test_finite_limits_are_not_ignored_when_explicit_requirement_is_off():
    spend = SpendConfig(
        production_requires_explicit_limit=False,
        group_daily_limit=Decimal(2),
        session_total_limit=Decimal(5),
        pricing_snapshot_id="synthetic-prices",
    )
    assert spend.configured_for_paid_calls() and not spend.monetary_limits_disabled
    assert SpendConfig.model_validate_json(spend.model_dump_json()) == spend


@pytest.mark.postgres
async def test_build_and_restart_keep_real_null_limits_without_adding_a_cap(tmp_path, monkeypatch):
    dsn = os.environ.get("GROUPAGENT_TEST_DSN")
    if not dsn:
        pytest.skip("Requires isolated PostgreSQL")

    class NoContainer:
        def __init__(self, *args, **kwargs):
            pass

        async def run(self, *args, **kwargs):
            raise AssertionError("No Docker in this wiring test")

    monkeypatch.setattr("groupagent.runtime.app.DockerSandbox", NoContainer)
    store = PostgresStore(dsn)
    store.migrate()
    group = "unlimited-" + uuid4().hex
    cfg = AppConfig(
        group=GroupConfig(
            enabled=True,
            internal_id=group,
            external_group_id="synthetic@chatroom",
            owner_ids=["owner"],
        ),
        spend=SpendConfig(
            production_requires_explicit_limit=False, pricing_snapshot_id="deepseek-20261114-text"
        ),
        runtime=RuntimeConfig(channel="local", engine="scoped", worker_image="sha256:" + "0" * 64),
    )
    secret = tmp_path / "synthetic-secrets"
    secret.write_text("DEEPSEEK_API_KEY=synthetic-only-fixture\n")
    secret.chmod(0o600)
    owner = ActorContext(group_id=group, member_id="owner", role=Role.OWNER)
    for _ in range(2):
        app = build_app(cfg, store, tmp_path / "app", secret)
        try:
            with store.transaction(owner) as tx:
                row = tx.conn.execute(
                    "SELECT daily_limit,total_limit,blocked,revision FROM ga_budget_policy WHERE group_id=%s",
                    (group,),
                ).fetchone()
                assert row == {
                    "daily_limit": None,
                    "total_limit": None,
                    "blocked": False,
                    "revision": 1,
                }
                assert (
                    tx.conn.execute(
                        "SELECT count(*) AS n FROM ga_budget_reservations WHERE group_id=%s",
                        (group,),
                    ).fetchone()["n"]
                    == 0
                )
        finally:
            await app.close()
