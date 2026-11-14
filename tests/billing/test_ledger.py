from __future__ import annotations

import os
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import uuid4

import pytest

from groupagent.billing import BudgetLedger, PriceSnapshot
from groupagent.billing.metering import MeteredModel
from groupagent.domain import ActorContext, DomainError, Role
from groupagent.execution import ModelReply, ScriptedFakeModel, TokenUsage
from groupagent.execution.models import ModelMessage, ModelRequest
from groupagent.persistence import PostgresStore

pytestmark = pytest.mark.postgres


@pytest.fixture
def setup():
    dsn = os.environ.get("GROUPAGENT_TEST_DSN")
    if not dsn:
        pytest.skip("Requires isolated PostgreSQL")
    store = PostgresStore(dsn)
    store.migrate()
    group = "billing-" + uuid4().hex
    store.register_group(group)
    actor = ActorContext(group_id=group, member_id="synthetic-owner", role=Role.OWNER)
    ledger = BudgetLedger(store, allow_test_prices=True)
    ledger.configure(actor, currency="CNY", daily_limit=Decimal(1), total_limit=Decimal(2))
    price = PriceSnapshot(
        id="fixture",
        provider="synthetic",
        operation="text",
        currency="CNY",
        rates={"input_tokens": Decimal("0.001"), "output_tokens": Decimal("0.002")},
        source_url="fixture://not-a-commercial-price",
        captured_at=datetime.now(UTC),
        test_only=True,
    )
    ledger.register_price(actor, price)
    return store, ledger, actor, price


def reserve(ledger, actor, key="one", tokens=100, **kwargs):
    return ledger.reserve(
        actor, key, "text", "fixture", {"input_tokens": Decimal(tokens)}, **kwargs
    )


def test_atomic_concurrent_reservations_cannot_overspend(setup):
    store, ledger, actor, _ = setup

    def contender(index):
        try:
            return reserve(ledger, actor, str(index), 200).id
        except DomainError as e:
            assert e.code == "budget_exhausted"
            return None

    with ThreadPoolExecutor(max_workers=10) as pool:
        results = list(pool.map(contender, range(12)))
    assert sum(x is not None for x in results) == 5
    with store.connect() as conn:
        row = conn.execute(
            "SELECT sum(reserved_amount) AS amount FROM ga_budget_reservations WHERE group_id=%s",
            (actor.group_id,),
        ).fetchone()
        assert row["amount"] == Decimal(1)


def test_duplicate_key_and_begin_have_one_provider_attempt(setup):
    _, ledger, actor, _ = setup
    one = reserve(ledger, actor)
    replay = reserve(ledger, actor)
    assert one.is_new and not replay.is_new and one.id == replay.id
    assert ledger.begin(actor, one.id)
    assert not ledger.begin(actor, one.id)
    with pytest.raises(DomainError) as e:
        reserve(ledger, actor, tokens=200)
    assert e.value.code == "idempotency_conflict"


def test_unknown_cost_is_held_after_restart_not_released(setup):
    store, ledger, actor, _ = setup
    one = reserve(ledger, actor, tokens=900)
    ledger.begin(actor, one.id)
    ledger.unknown(actor, one.id)
    restarted = BudgetLedger(store, allow_test_prices=True)
    with pytest.raises(DomainError) as e:
        reserve(restarted, actor, "new", 200)
    assert e.value.code == "budget_exhausted"
    with pytest.raises(DomainError) as e:
        restarted.release_unstarted(actor, one.id)
    assert e.value.code == "reconciliation_required"
    settled = restarted.settle(
        actor, one.id, {"input_tokens": Decimal(100)}, "synthetic:usage-receipt"
    )
    assert settled.actual_amount == Decimal("0.1")
    assert reserve(restarted, actor, "after-reconcile", 800).is_new


def test_provider_overrun_is_accounted_and_blocks_next_call(setup):
    _, ledger, actor, _ = setup
    one = reserve(ledger, actor)
    ledger.begin(actor, one.id)
    settled = ledger.settle(actor, one.id, {"input_tokens": Decimal(1500)}, "synthetic:usage")
    assert settled.overrun and settled.actual_amount == Decimal("1.5")
    with pytest.raises(DomainError) as e:
        reserve(ledger, actor, "another", 1)
    assert e.value.code == "budget_exhausted"
    assert (
        ledger.settle(actor, one.id, {"input_tokens": Decimal(1500)}, "synthetic:replay").id
        == one.id
    )
    with pytest.raises(DomainError):
        ledger.settle(actor, one.id, {"input_tokens": Decimal(1)}, "synthetic:rewrite")


def test_daily_window_and_total_limit_are_both_enforced(setup):
    _, ledger, actor, _ = setup
    before = datetime(2026, 11, 14, 15, 59, tzinfo=UTC)
    after = before + timedelta(minutes=2)
    reserve(ledger, actor, "day1", 1000, now=before)
    reserve(ledger, actor, "day2", 1000, now=after)
    with pytest.raises(DomainError) as e:
        reserve(ledger, actor, "day3", 1, now=after + timedelta(days=1))
    assert e.value.code == "budget_exhausted"


def test_scope_role_price_and_policy_revocation(setup):
    store, ledger, actor, price = setup
    member = actor.model_copy(update={"member_id": "member", "role": Role.MEMBER})
    observer = actor.model_copy(update={"member_id": "reader", "role": Role.OBSERVER})
    with pytest.raises(DomainError):
        ledger.configure(member, currency="CNY", daily_limit=Decimal(100))
    with pytest.raises(DomainError):
        ledger.register_price(member, price)
    with pytest.raises(DomainError):
        reserve(ledger, observer)
    grant = reserve(ledger, observer, "readonly", read_only_authorized=True)
    with pytest.raises(DomainError):
        ledger.begin(member, grant.id)
    one = reserve(ledger, actor)
    store.register_group(actor.group_id, 2)
    with pytest.raises(DomainError) as e:
        ledger.begin(actor, one.id)
    assert e.value.code == "policy_changed"
    fresh = actor.model_copy(update={"policy_revision": 2})
    with pytest.raises(DomainError) as e:
        ledger.begin(fresh, one.id)
    assert e.value.code == "policy_changed"


def test_fixture_prices_and_changed_prices_are_rejected(setup):
    store, ledger, actor, price = setup
    strict = BudgetLedger(store)
    with pytest.raises(DomainError):
        strict.register_price(actor, price)
    with pytest.raises(DomainError):
        reserve(strict, actor)
    with pytest.raises(DomainError) as e:
        ledger.register_price(
            actor, price.model_copy(update={"rates": {"input_tokens": Decimal(0)}})
        )
    assert e.value.code == "version_conflict"


async def test_model_is_not_called_when_budget_is_insufficient(setup):
    _, ledger, actor, _ = setup
    model = ScriptedFakeModel(
        [ModelReply(text="unused", usage=TokenUsage(input_tokens=1, output_tokens=1))]
    )
    metered = MeteredModel(model, ledger, actor, price_id="fixture")
    request = ModelRequest(
        messages=(ModelMessage(role="user", content="x" * 2000),), tools=(), max_output_tokens=100
    )
    with pytest.raises(DomainError) as e:
        await metered.complete(request)
    assert e.value.code == "budget_exhausted" and not model.requests
