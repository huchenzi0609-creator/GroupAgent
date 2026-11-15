"""Pure host retry boundaries and actual PG accounting; never a live provider."""

import asyncio
import os
from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace
from uuid import uuid4

import httpx
import pytest

from groupagent.billing import BudgetLedger, PriceSnapshot
from groupagent.billing.metering import MeteredModel
from groupagent.domain import ActorContext, DomainError, Role
from groupagent.execution import ModelReply, TokenUsage
from groupagent.execution.deepseek import DeepSeekHTTPModel
from groupagent.execution.models import ModelMessage, ModelRequest
from groupagent.persistence import PostgresStore


@pytest.fixture
def actor():
    return ActorContext(group_id="synthetic-host-retry", member_id="owner", role=Role.OWNER)


@pytest.fixture
def model_request():
    return ModelRequest(
        messages=(ModelMessage(role="user", content="Synthetic bounded model_request"),),
        tools=(),
        max_output_tokens=16,
    )


@pytest.fixture
def delays(monkeypatch):
    values = []

    async def immediate(delay):
        values.append(delay)

    monkeypatch.setattr("groupagent.billing.metering.asyncio.sleep", immediate)
    return values


def reply():
    return ModelReply(text="Synthetic reply", usage=TokenUsage(input_tokens=10, output_tokens=5))


def failure(code="provider_unavailable", *, retryable=False):
    return DomainError(code, "Synthetic provider error", retryable=retryable)


class ExplicitModel:
    name = "synthetic-host-retry-model"

    def __init__(self, outcomes):
        self.outcomes = list(outcomes)
        self.calls = 0
        self.active = 0
        self.max_active = 0

    async def complete(self, model_request):
        self.calls += 1
        self.active += 1
        self.max_active = max(self.max_active, self.active)
        try:
            outcome = self.outcomes.pop(0)
            if isinstance(outcome, BaseException):
                raise outcome
            return outcome
        finally:
            self.active -= 1


class ExplicitLedger:
    def __init__(self, broken=None):
        self.broken = broken
        self.keys = []
        self.states = {}
        self.estimates = {}

    def check(self, stage):
        if stage == self.broken:
            # An accounting failure deliberately shares a retryable model code.
            raise failure("provider_unavailable")

    def reserve(self, actor, key, operation, price, units, **kwargs):
        self.check("reserve")
        self.keys.append(key)
        identifier = str(len(self.keys))
        self.states[identifier] = "reserved"
        self.estimates[identifier] = units
        return SimpleNamespace(id=identifier, is_new=True)

    def begin(self, actor, identifier):
        self.check("begin")
        self.states[identifier] = "inflight"
        return True

    def settle(self, actor, identifier, units, evidence):
        self.check("settle")
        self.states[identifier] = "settled"
        return SimpleNamespace(overrun=False)

    def unknown(self, actor, identifier):
        self.check("unknown")
        self.states[identifier] = "unknown"


@pytest.mark.parametrize("code", ["provider_unavailable", "invalid_model_response"])
async def test_each_inference_retry_keeps_its_own_unknown_charge(
    actor, model_request, delays, code
):
    model = ExplicitModel([failure(code), reply()])
    ledger = ExplicitLedger()
    metered = MeteredModel(model, ledger, actor, price_id="fixture", additional_retry_limit=2)
    assert await metered.complete(model_request) == reply()
    assert model.calls == 2 and model.max_active == 1 and model.active == 0
    assert ledger.states == {"1": "unknown", "2": "settled"}
    assert len(set(ledger.keys)) == 2 and all(ledger.estimates.values())
    assert delays == [1]


@pytest.mark.parametrize("code", ["provider_unavailable", "invalid_model_response"])
async def test_exhaustion_stops_and_cannot_trigger_an_outer_retry(
    actor, model_request, delays, code
):
    errors = [failure(code, retryable=True) for _ in range(3)]
    model, ledger = ExplicitModel(errors), ExplicitLedger()
    metered = MeteredModel(model, ledger, actor, price_id="fixture", additional_retry_limit=2)
    with pytest.raises(DomainError) as caught:
        await metered.complete(model_request)
    assert caught.value.code == code and caught.value.retryable is False
    assert model.calls == 3 and ledger.states == {"1": "unknown", "2": "unknown", "3": "unknown"}
    assert delays == [1, 2]


async def test_default_zero_preserves_the_original_error_and_one_attempt(
    actor, model_request, delays
):
    original = failure(retryable=True)
    model, ledger = ExplicitModel([original, reply()]), ExplicitLedger()
    with pytest.raises(DomainError) as caught:
        await MeteredModel(model, ledger, actor, price_id="fixture").complete(model_request)
    assert caught.value is original and caught.value.retryable
    assert model.calls == 1 and ledger.states == {"1": "unknown"} and not delays


@pytest.mark.parametrize(
    "code", ["auth_required", "permission_denied", "budget_exhausted", "rate_limited"]
)
async def test_other_provider_errors_never_retry(actor, model_request, delays, code):
    model, ledger = ExplicitModel([failure(code, retryable=True), reply()]), ExplicitLedger()
    with pytest.raises(DomainError) as caught:
        await MeteredModel(
            model, ledger, actor, price_id="fixture", additional_retry_limit=2
        ).complete(model_request)
    assert caught.value.code == code and model.calls == 1 and not delays


@pytest.mark.parametrize(
    "stage,calls", [("reserve", 0), ("begin", 0), ("settle", 1), ("unknown", 1)]
)
async def test_accounting_errors_are_outside_the_model_retry_boundary(
    actor, model_request, delays, stage, calls
):
    model = ExplicitModel([failure(), reply()] if stage == "unknown" else [reply(), reply()])
    ledger = ExplicitLedger(stage)
    with pytest.raises(DomainError) as caught:
        await MeteredModel(
            model, ledger, actor, price_id="fixture", additional_retry_limit=2
        ).complete(model_request)
    assert caught.value.code == "provider_unavailable" and model.calls == calls
    assert len(ledger.keys) <= 1 and not delays


async def test_cancel_during_backoff_keeps_first_unknown_and_starts_no_next_call(
    actor, model_request, monkeypatch
):
    sleeping = asyncio.Event()

    async def blocked_delay(delay):
        sleeping.set()
        await asyncio.Event().wait()

    monkeypatch.setattr("groupagent.billing.metering.asyncio.sleep", blocked_delay)
    model, ledger = ExplicitModel([failure(), reply()]), ExplicitLedger()
    metered = MeteredModel(model, ledger, actor, price_id="fixture", additional_retry_limit=2)
    task = asyncio.create_task(metered.complete(model_request))
    await asyncio.wait_for(sleeping.wait(), 1)
    assert model.active == 0
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert model.calls == 1 and ledger.states == {"1": "unknown"}


async def test_next_admission_waits_until_previous_model_cleanup_finishes(
    actor, model_request, delays
):
    cleaning, released = asyncio.Event(), asyncio.Event()

    class CleaningModel(ExplicitModel):
        async def complete(self, model_request):
            try:
                return await super().complete(model_request)
            finally:
                if self.calls == 1:
                    cleaning.set()
                    await released.wait()

    model, ledger = CleaningModel([failure(), reply()]), ExplicitLedger()
    task = asyncio.create_task(
        MeteredModel(model, ledger, actor, price_id="fixture", additional_retry_limit=2).complete(
            model_request
        )
    )
    await asyncio.wait_for(cleaning.wait(), 1)
    assert model.calls == len(ledger.keys) == 1
    released.set()
    assert await task == reply()
    assert model.calls == 2 and model.max_active == 1


async def test_cancellation_unwinds_the_model_and_keeps_the_charge(actor, model_request, delays):
    entered, stopped = asyncio.Event(), asyncio.Event()

    class CancelableModel:
        name = "synthetic-cancelable-host-model"
        calls = 0

        async def complete(self, model_request):
            self.calls += 1
            entered.set()
            try:
                await asyncio.Event().wait()
            finally:
                stopped.set()

    model, ledger = CancelableModel(), ExplicitLedger()
    metered = MeteredModel(model, ledger, actor, price_id="fixture", additional_retry_limit=2)
    task = asyncio.create_task(metered.complete(model_request))
    await asyncio.wait_for(entered.wait(), 1)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert stopped.is_set() and model.calls == 1
    assert ledger.states == {"1": "unknown"} and not delays


@pytest.mark.parametrize("limit", [-1, 6, True, 1.5, "2"])
def test_retry_limit_must_match_the_bounded_configuration_contract(actor, limit):
    with pytest.raises(ValueError):
        MeteredModel(
            ExplicitModel([]),
            ExplicitLedger(),
            actor,
            price_id="fixture",
            additional_retry_limit=limit,
        )


@pytest.fixture
def pg_ledger():
    dsn = os.environ.get("GROUPAGENT_TEST_DSN")
    if not dsn:
        pytest.skip("Requires isolated PostgreSQL")
    store = PostgresStore(dsn)
    store.migrate()
    actor = ActorContext(group_id="host-retry-" + uuid4().hex, member_id="owner", role=Role.OWNER)
    store.register_group(actor.group_id)
    ledger = BudgetLedger(store, allow_test_prices=True)
    ledger.configure(actor, currency="CNY", daily_limit=Decimal(1), total_limit=Decimal(2))
    ledger.register_price(
        actor,
        PriceSnapshot(
            id="fixture",
            provider="synthetic-only",
            operation="text",
            currency="CNY",
            rates={"input_tokens": Decimal("0.000001"), "output_tokens": Decimal("0.000002")},
            source_url="fixture://not-a-commercial-price",
            captured_at=datetime.now(UTC),
            test_only=True,
        ),
    )
    return store, ledger, actor


@pytest.mark.postgres
@pytest.mark.parametrize("code", ["provider_unavailable", "invalid_model_response"])
async def test_actual_pg_and_mock_http_preserve_failed_attempt_then_settle_success(
    pg_ledger, model_request, delays, code
):
    store, ledger, actor = pg_ledger
    calls = []

    def respond(value):
        calls.append(value)
        if len(calls) == 1:
            return (
                httpx.Response(503)
                if code == "provider_unavailable"
                else httpx.Response(200, json={"usage": {}})
            )
        return httpx.Response(
            200,
            json={
                "choices": [
                    {
                        "message": {"role": "assistant", "content": "Synthetic reply"},
                        "finish_reason": "stop",
                    }
                ],
                "usage": {"prompt_tokens": 10, "completion_tokens": 5, "total_tokens": 15},
            },
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as http:
        model = DeepSeekHTTPModel("synthetic-only-fixture", client=http)
        actual = await MeteredModel(
            model, ledger, actor, price_id="fixture", additional_retry_limit=1
        ).complete(model_request)
    assert actual == reply() and len(calls) == 2
    with store.connect() as conn:
        rows = conn.execute(
            "SELECT id,key,state,reserved_amount,actual_amount FROM ga_budget_reservations WHERE group_id=%s ORDER BY created_at",
            (actor.group_id,),
        ).fetchall()
    assert len(rows) == 2 and len({r["id"] for r in rows}) == len({r["key"] for r in rows}) == 2
    assert rows[0]["state"] == "unknown" and rows[0]["actual_amount"] is None
    assert rows[1]["state"] == "settled" and rows[1]["actual_amount"] == Decimal("0.000020")
    report = ledger.report(actor)
    assert Decimal(report["accounting"]["held"]) == rows[0]["reserved_amount"]
    assert (
        Decimal(report["accounting"]["total"])
        == rows[0]["reserved_amount"] + rows[1]["actual_amount"]
    )


@pytest.mark.postgres
async def test_actual_pg_exhaustion_retains_all_three_unknown_reservations(
    pg_ledger, model_request, delays
):
    store, ledger, actor = pg_ledger
    model = ExplicitModel([failure()] * 3)
    with pytest.raises(DomainError):
        await MeteredModel(
            model, ledger, actor, price_id="fixture", additional_retry_limit=2
        ).complete(model_request)
    with store.connect() as conn:
        rows = conn.execute(
            "SELECT state,reserved_amount FROM ga_budget_reservations WHERE group_id=%s",
            (actor.group_id,),
        ).fetchall()
    assert model.calls == 3 and len(rows) == 3 and all(r["state"] == "unknown" for r in rows)
    assert Decimal(ledger.report(actor)["accounting"]["held"]) == sum(
        r["reserved_amount"] for r in rows
    )


@pytest.mark.postgres
@pytest.mark.parametrize("limit", [0, 2])
async def test_runtime_router_and_summarizer_receive_the_configured_limit(
    pg_ledger, tmp_path, monkeypatch, limit
):
    from groupagent.config import (
        AppConfig,
        ExecutionConfig,
        GroupConfig,
        RuntimeConfig,
        SpendConfig,
    )
    from groupagent.runtime.app import build_app

    class NoContainerFixture:
        def __init__(self, *args, **kwargs):
            pass

        async def run(self, *args, **kwargs):
            raise AssertionError("This wiring test must not launch Docker")

    monkeypatch.setattr("groupagent.runtime.app.DockerSandbox", NoContainerFixture)
    store, _, actor = pg_ledger
    config = AppConfig(
        group=GroupConfig(
            enabled=True,
            internal_id=actor.group_id,
            external_group_id="synthetic-local@chatroom",
            owner_ids=[actor.member_id],
        ),
        execution=ExecutionConfig(additional_retry_limit=limit),
        spend=SpendConfig(
            group_daily_limit=Decimal(1),
            session_total_limit=Decimal(2),
            pricing_snapshot_id="deepseek-20261114-text",
        ),
        runtime=RuntimeConfig(channel="local", engine="scoped", worker_image="sha256:" + "0" * 64),
    )
    secrets = tmp_path / "synthetic-secrets"
    secrets.write_text("DEEPSEEK_API_KEY=synthetic-only-fixture\n")
    secrets.chmod(0o600)
    app = build_app(config, store, tmp_path / "app", secrets)
    try:
        router = app.coordinator.provider_factory(actor, SimpleNamespace(id="synthetic-event"))
        assert router.model.additional_retry_limit == limit
        assert app.coordinator.memory.summarizer.model.additional_retry_limit == limit
        with store.connect() as conn:
            assert (
                conn.execute(
                    "SELECT count(*) AS n FROM ga_budget_reservations WHERE group_id=%s",
                    (actor.group_id,),
                ).fetchone()["n"]
                == 0
            )
    finally:
        await app.close()
