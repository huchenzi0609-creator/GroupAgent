"""Restart settlement uses durable work ownership, never a process-local actor map."""

from decimal import Decimal

import pytest

from groupagent.billing import BudgetLedger
from groupagent.billing.metering import LedgerModelBudget
from groupagent.domain import DomainError
from groupagent.execution import ExecutionRequest, TokenUsage

from .test_begin_revalidation import source
from .test_ledger import setup as setup

pytestmark = pytest.mark.postgres


def work_request(store, actor, tmp_path):
    event = source(store, actor)
    with store.transaction(actor) as tx:
        work = tx.create_work("Synthetic accounting recovery", event.id)
    return ExecutionRequest(
        work_id=work.id,
        attempt_id="original-model-attempt",
        actor=actor,
        objective=work.goal,
        workspace=tmp_path,
        trace_id=event.id,
    )


@pytest.mark.parametrize("usage", [None, TokenUsage(input_tokens=4, output_tokens=3)])
async def test_restart_records_pending_usage_without_new_provider_attempt(setup, tmp_path, usage):
    store, ledger, actor, _ = setup
    request = work_request(store, actor, tmp_path)
    first = LedgerModelBudget(ledger, price_id="fixture")
    identifier = await first.reserve(request, round_number=1, input_tokens=20, output_tokens=10)
    restarted = LedgerModelBudget(BudgetLedger(store, allow_test_prices=True), price_id="fixture")
    await restarted.recover(request, identifier, usage)
    await restarted.recover(request, identifier, usage)
    with store.transaction(actor) as tx:
        rows = tx.conn.execute(
            "SELECT state,actual_units FROM ga_budget_reservations WHERE group_id=%s",
            (tx.group_id,),
        ).fetchall()
    assert len(rows) == 1
    assert rows[0]["state"] == ("unknown" if usage is None else "settled")


async def test_restart_cannot_attach_another_work_reservation(setup, tmp_path):
    store, ledger, actor, _ = setup
    first_request = work_request(store, actor, tmp_path)
    other_request = work_request(store, actor, tmp_path)
    first = LedgerModelBudget(ledger, price_id="fixture")
    identifier = await first.reserve(
        first_request, round_number=1, input_tokens=20, output_tokens=10
    )
    restarted = LedgerModelBudget(ledger, price_id="fixture")
    with pytest.raises(DomainError, match="does not match"):
        await restarted.recover(
            other_request, identifier, TokenUsage(input_tokens=4, output_tokens=3)
        )
    with store.transaction(actor) as tx:
        assert (
            tx.conn.execute(
                "SELECT state FROM ga_budget_reservations WHERE group_id=%s AND id=%s",
                (tx.group_id, identifier),
            ).fetchone()["state"]
            == "inflight"
        )


async def test_restart_records_confirmed_rejection_for_exact_attempt_only(setup, tmp_path):
    store, ledger, actor, _ = setup
    request = work_request(store, actor, tmp_path)
    other = work_request(store, actor, tmp_path)
    budget = LedgerModelBudget(ledger, price_id="fixture")
    identifier = await budget.reserve(request, round_number=1, input_tokens=20, output_tokens=10)
    restarted = LedgerModelBudget(ledger, price_id="fixture")
    with pytest.raises(DomainError, match="does not match"):
        await restarted.recover_rejection(other, identifier)
    await restarted.recover_rejection(request, identifier)
    await restarted.recover_rejection(request, identifier)
    with store.transaction(actor) as tx:
        row = tx.conn.execute(
            "SELECT state,reason,actual_amount FROM ga_budget_reservations WHERE id=%s",
            (identifier,),
        ).fetchone()
    assert row == {
        "state": "released",
        "reason": "provider_rejected_rate_limit",
        "actual_amount": None,
    }


async def test_persisted_key_before_reserve_does_not_create_a_charge_on_recovery(setup, tmp_path):
    store, ledger, actor, _ = setup
    request = work_request(store, actor, tmp_path)
    budget = LedgerModelBudget(ledger, price_id="fixture")
    key = budget.recovery_key(request, round_number=1)
    await budget.recover(request, key, None)
    with store.transaction(actor) as tx:
        assert not tx.conn.execute(
            "SELECT 1 FROM ga_budget_reservations WHERE group_id=%s", (tx.group_id,)
        ).fetchone()


async def test_crash_between_reserve_and_begin_releases_only_proven_unstarted_charge(
    setup, tmp_path
):
    store, ledger, actor, _ = setup
    request = work_request(store, actor, tmp_path)
    budget = LedgerModelBudget(ledger, price_id="fixture")
    key = budget.recovery_key(request, round_number=1)
    reserved = ledger.reserve(
        actor, key, "text", "fixture", {"input_tokens": Decimal(10)}, work_id=request.work_id
    )
    restarted = LedgerModelBudget(ledger, price_id="fixture")
    await restarted.recover(request, key, None)
    await restarted.recover(request, key, None)
    with store.transaction(actor) as tx:
        assert (
            tx.conn.execute(
                "SELECT state FROM ga_budget_reservations WHERE group_id=%s AND id=%s",
                (tx.group_id, reserved.id),
            ).fetchone()["state"]
            == "released"
        )
