"""Explicit unlimited policies retain accounting and every nonmonetary guard."""

from decimal import Decimal
from types import SimpleNamespace
from uuid import uuid4

import pytest

from groupagent.billing import BudgetLedger
from groupagent.billing.metering import LedgerModelBudget, MeteredModel
from groupagent.domain import DomainError, Role
from groupagent.execution import ModelReply, ScriptedFakeModel, TokenUsage
from groupagent.execution.models import ModelMessage, ModelRequest
from groupagent.memory import MemoryService

from .test_begin_revalidation import source
from .test_ledger import reserve
from .test_ledger import setup as setup

pytestmark = pytest.mark.postgres


def configure_unlimited(ledger, actor, *, expected_revision=1):
    return ledger.configure(
        actor,
        currency="CNY",
        daily_limit=None,
        total_limit=None,
        expected_revision=expected_revision,
    )


def reservations(store, actor):
    with store.transaction(actor) as tx:
        return tx.conn.execute(
            "SELECT * FROM ga_budget_reservations WHERE group_id=%s ORDER BY id",
            (actor.group_id,),
        ).fetchall()


def test_unlimited_preserves_all_charge_states_and_admits_beyond_exhausted_old_limits(setup):
    store, ledger, actor, _ = setup
    spent = reserve(ledger, actor, "spent", 600)
    assert ledger.begin(actor, spent.id)
    ledger.settle(actor, spent.id, {"input_tokens": Decimal(600)}, "synthetic:usage")
    held = reserve(ledger, actor, "unknown", 200)
    assert ledger.begin(actor, held.id)
    ledger.unknown(actor, held.id)
    released = reserve(ledger, actor, "released", 100)
    ledger.release_unstarted(actor, released.id)
    planned = reserve(ledger, actor, "planned", 100)
    inflight = reserve(ledger, actor, "inflight", 100)
    assert ledger.begin(actor, inflight.id)
    with pytest.raises(DomainError) as caught:
        reserve(ledger, actor, "old-budget-exhausted", 1)
    assert caught.value.code == "budget_exhausted"
    before = reservations(store, actor)
    assert {row["state"] for row in before} == {
        "settled",
        "unknown",
        "released",
        "reserved",
        "inflight",
    }

    assert configure_unlimited(ledger, actor) == 2
    assert reservations(store, actor) == before
    restarted = BudgetLedger(store, allow_test_prices=True)
    assert restarted.begin(actor, planned.id)
    large = reserve(restarted, actor, "beyond-old-limits", 100_000_000)
    assert not large.limits_enabled and restarted.begin(actor, large.id)
    replay = reserve(restarted, actor, "beyond-old-limits", 100_000_000)
    assert replay.id == large.id and not replay.is_new and not replay.limits_enabled
    assert not restarted.begin(actor, large.id)
    with pytest.raises(DomainError) as caught:
        reserve(restarted, actor, "beyond-old-limits", 1)
    assert caught.value.code == "idempotency_conflict"
    with pytest.raises(DomainError) as caught:
        restarted.release_unstarted(actor, held.id)
    assert caught.value.code == "reconciliation_required"
    usage = restarted.report(actor)["accounting"]
    assert Decimal(usage["gross_settled_estimate"]) == Decimal("0.6")
    assert Decimal(usage["held"]) == Decimal("100000.4")


@pytest.mark.parametrize("unlimited_at_settlement", [False, True])
def test_overrun_records_actual_cost_and_obeys_current_policy(setup, unlimited_at_settlement):
    store, ledger, actor, _ = setup
    if not unlimited_at_settlement:
        configure_unlimited(ledger, actor)
    item = reserve(ledger, actor)
    assert ledger.begin(actor, item.id)
    if unlimited_at_settlement:
        configure_unlimited(ledger, actor)
    else:
        ledger.configure(actor, currency="CNY", daily_limit=Decimal(1), expected_revision=2)
    actual = {"input_tokens": Decimal(1500)}
    settled = ledger.settle(actor, item.id, actual, "synthetic:overrun")
    assert settled.overrun and settled.actual_amount == Decimal("1.5")
    assert settled.limits_enabled is not unlimited_at_settlement
    assert ledger.settle(actor, item.id, actual, "synthetic:replay") == settled
    with store.transaction(actor) as tx:
        policy = tx.conn.execute(
            "SELECT blocked FROM ga_budget_policy WHERE group_id=%s", (actor.group_id,)
        ).fetchone()
        assert policy["blocked"] is not unlimited_at_settlement
    if unlimited_at_settlement:
        assert reserve(ledger, actor, "next", 2000).is_new
    else:
        with pytest.raises(DomainError) as caught:
            reserve(ledger, actor, "next", 1)
        assert caught.value.code == "budget_exhausted"
    with pytest.raises(DomainError) as caught:
        ledger.settle(actor, item.id, {"input_tokens": Decimal(1)}, "synthetic:rewrite")
    assert caught.value.code == "idempotency_conflict"


@pytest.mark.parametrize("unlimited", [False, True])
@pytest.mark.parametrize("boundary", ["model", "engine"])
async def test_metering_overrun_stops_only_when_limits_enabled(setup, unlimited, boundary):
    store, ledger, actor, _ = setup
    if unlimited:
        configure_unlimited(ledger, actor)
    usage = TokenUsage(input_tokens=1500, output_tokens=0)
    if boundary == "model":
        model = ScriptedFakeModel([ModelReply(text="synthetic result", usage=usage)])
        metered = MeteredModel(model, ledger, actor, price_id="fixture")
        request = ModelRequest(
            messages=(ModelMessage(role="user", content="synthetic request"),),
            tools=(),
            max_output_tokens=10,
        )
        operation = metered.complete(request)
    else:
        request_event = source(store, actor)
        with store.transaction(actor) as tx:
            work = tx.create_work("synthetic metering work", request_event.id)
        budget = LedgerModelBudget(ledger, price_id="fixture")
        identifier = await budget.reserve(
            SimpleNamespace(actor=actor, work_id=work.id, attempt_id="synthetic-attempt"),
            round_number=0,
            input_tokens=10,
            output_tokens=10,
        )
        operation = budget.settle(identifier, usage)
    if unlimited:
        await operation
        assert reserve(ledger, actor, "following-call", 2000).is_new
    else:
        with pytest.raises(DomainError) as caught:
            await operation
        assert caught.value.code == "budget_exhausted"
    settled = [row for row in reservations(store, actor) if row["state"] == "settled"]
    assert len(settled) == 1 and settled[0]["overrun"]
    assert settled[0]["actual_amount"] == Decimal("1.5")


@pytest.mark.parametrize("window", ["daily", "total"])
def test_each_remaining_finite_window_still_denies_reserve_and_begin(setup, window):
    _, ledger, actor, _ = setup
    ledger.configure(
        actor,
        currency="CNY",
        daily_limit=Decimal("0.1") if window == "daily" else None,
        total_limit=Decimal("0.1") if window == "total" else None,
        expected_revision=1,
    )
    planned = reserve(ledger, actor, "planned", 100)
    assert planned.limits_enabled
    with pytest.raises(DomainError) as caught:
        reserve(ledger, actor, "over-finite-window", 1)
    assert caught.value.code == "budget_exhausted"
    ledger.configure(
        actor,
        currency="CNY",
        daily_limit=Decimal("0.05") if window == "daily" else None,
        total_limit=Decimal("0.05") if window == "total" else None,
        expected_revision=2,
    )
    with pytest.raises(DomainError) as caught:
        ledger.begin(actor, planned.id)
    assert caught.value.code == "budget_exhausted"


def test_unlimited_requires_configured_owner_policy_and_current_revision(setup):
    store, ledger, actor, price = setup
    member = actor.model_copy(update={"member_id": "member", "role": Role.MEMBER})
    with pytest.raises(DomainError) as caught:
        configure_unlimited(ledger, member)
    assert caught.value.code == "permission_denied"
    with pytest.raises(DomainError) as caught:
        configure_unlimited(ledger, actor, expected_revision=0)
    assert caught.value.code == "version_conflict"
    configure_unlimited(ledger, actor)
    with pytest.raises(DomainError) as caught:
        ledger.configure(actor, currency="USD", daily_limit=None, expected_revision=2)
    assert caught.value.code == "currency_mismatch"
    new_group = "billing-unconfigured-" + uuid4().hex
    store.register_group(new_group)
    unconfigured = actor.model_copy(update={"group_id": new_group})
    ledger.register_price(unconfigured, price)
    with pytest.raises(DomainError) as caught:
        reserve(ledger, unconfigured)
    assert caught.value.code == "budget_unconfigured"
    assert ledger.configure(unconfigured, currency="CNY", daily_limit=None) == 1
    assert reserve(ledger, unconfigured).is_new


def test_switching_to_unlimited_does_not_clear_administrative_freeze(setup):
    store, ledger, actor, _ = setup
    planned = reserve(ledger, actor)
    with store.transaction(actor) as tx:
        tx.conn.execute(
            "UPDATE ga_budget_policy SET blocked=true WHERE group_id=%s", (actor.group_id,)
        )
    configure_unlimited(ledger, actor)
    for action in (
        lambda: reserve(ledger, actor, "new"),
        lambda: ledger.begin(actor, planned.id),
    ):
        with pytest.raises(DomainError) as caught:
            action()
        assert caught.value.code == "budget_exhausted"


def test_unlimited_preserves_source_identity_prices_and_revocation(setup):
    store, ledger, actor, _ = setup
    configure_unlimited(ledger, actor)
    request = source(store, actor)
    member = actor.model_copy(update={"member_id": "other", "role": Role.MEMBER})
    observer = actor.model_copy(update={"member_id": "reader", "role": Role.OBSERVER})
    for action in (
        lambda: reserve(ledger, observer),
        lambda: reserve(ledger, member, source_event_id=request.id),
        lambda: reserve(BudgetLedger(store), actor),
    ):
        with pytest.raises(DomainError) as caught:
            action()
        assert caught.value.code in {"permission_denied", "price_missing"}
    planned = reserve(ledger, actor, source_event_id=request.id)
    with pytest.raises(DomainError) as caught:
        ledger.begin(member, planned.id)
    assert caught.value.code == "permission_denied"
    MemoryService(store).forget(actor, event_ids=[request.id])
    with pytest.raises(DomainError) as caught:
        ledger.begin(actor, planned.id)
    assert caught.value.code == "source_unavailable"


@pytest.mark.parametrize("value", [Decimal(0), Decimal(-1), Decimal("NaN"), Decimal("Infinity")])
def test_unlimited_other_window_does_not_weaken_finite_limit_validation(setup, value):
    _, ledger, actor, _ = setup
    with pytest.raises(DomainError) as caught:
        ledger.configure(
            actor, currency="CNY", daily_limit=None, total_limit=value, expected_revision=1
        )
    assert caught.value.code == "invalid_input"
