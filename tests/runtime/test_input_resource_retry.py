"""Real input/PG/ledger contracts; worker and provider transports are offline fixtures."""

from __future__ import annotations

import asyncio
from decimal import Decimal

import pytest

from groupagent.domain import DomainError
from groupagent.memory import MemoryService
from groupagent.resources import ResourceDeferred
from groupagent.runtime.inputs import InputProcessor
from groupagent.runtime.state import source_fingerprint

from .test_inputs import InputFixtureWorker, attached, fixture_setup, rows

pytestmark = pytest.mark.postgres


class DeferredAudioWorker(InputFixtureWorker):
    def __init__(self):
        super().__init__()
        self.low = False
        self.error = ResourceDeferred
        self.validations = 0

    async def run(self, context, operation, inputs, **kwargs):
        if operation == "audio_validate":
            self.validations += 1
            if self.low and ":attempt-" in context.idempotency_key:
                raise self.error()
        return await super().run(context, operation, inputs, **kwargs)


async def test_repeated_unstarted_audio_deferrals_preserve_cache_and_all_paid_attempts(runtime_env):
    env, worker = runtime_env, DeferredAudioWorker()
    setup = fixture_setup(env, worker=worker)
    event = attached(env)
    worker.low = True
    try:
        cached = None
        for _ in range(4):
            with pytest.raises(DomainError) as error:
                await setup.processor.preprocess_audio(event)
            assert (
                error.value.code == "resource_deferred"
                and error.value.effect_state == "not_started"
            )
            body = rows(env, "ga_media_inputs")[0]["body"]
            assert body["attempts"] == 0 and body["error_code"] == "resource_deferred"
            assert body["claim_until"] == 0
            cached = cached or body["reference"]
            assert body["reference"] == cached
            assert not setup.calls and not rows(env, "ga_budget_reservations")
            assert not rows(env, "ga_decisions") and len(rows(env, "ga_events")) == 1
        # A new controller process can reuse the validated source after waiting;
        # its first actual provider call still has attempt number one.
        restarted = InputProcessor(
            env.store,
            env.artifacts,
            worker,
            setup.processor.providers,
            setup.processor.routes,
            setup.ledger,
            env.channel,
            env.root / "inputs",
            lambda _: env.actor,
        )
        worker.low = False
        result = await restarted.preprocess_audio(event)
        assert result.reply_to == event.id and len(setup.calls) == 1
        body = rows(env, "ga_media_inputs")[0]["body"]
        assert body["attempts"] == 1 and body["state"] == "complete"
        reservations = rows(env, "ga_budget_reservations")
        assert len(reservations) == 1 and reservations[0]["state"] == "settled"
        assert ":attempt-1:asr-0" in reservations[0]["key"]
        assert len(rows(env, "ga_decisions")) == 1
    finally:
        await setup.http.aclose()


async def test_later_unpaid_deferral_does_not_refund_an_earlier_unknown_attempt(runtime_env):
    import httpx

    env, worker, requests = runtime_env, DeferredAudioWorker(), []

    def response(request):
        requests.append(request)
        if len(requests) == 1:
            return httpx.Response(503)
        return httpx.Response(
            200, json={"request_id": "resumed", "text": "合成转写", "duration_ms": 400}
        )

    setup = fixture_setup(env, response, worker=worker)
    event = attached(env)
    try:
        with pytest.raises(DomainError):
            await setup.processor.preprocess_audio(event)
        original = rows(env, "ga_budget_reservations")[0]
        assert original["state"] == "unknown"
        worker.low = True
        for _ in range(3):
            with pytest.raises(DomainError) as error:
                await setup.processor.preprocess_audio(event)
            assert error.value.code == "resource_deferred"
            assert rows(env, "ga_media_inputs")[0]["body"]["attempts"] == 1
            assert rows(env, "ga_budget_reservations") == [original]
        worker.low = False
        result = await setup.processor.preprocess_audio(event)
        assert result.reply_to == event.id and len(requests) == 2
        assert rows(env, "ga_media_inputs")[0]["body"]["attempts"] == 2
        reservations = rows(env, "ga_budget_reservations")
        assert {row["state"] for row in reservations} == {"unknown", "settled"}
        assert next(row for row in reservations if row["id"] == original["id"]) == original
    finally:
        await setup.http.aclose()


@pytest.mark.parametrize("effect_state", [None, "unknown", "confirmed"])
async def test_only_not_started_resource_errors_qualify_for_an_attempt_refund(
    runtime_env, effect_state
):
    env, worker = runtime_env, DeferredAudioWorker()
    setup = fixture_setup(env, worker=worker)
    event = attached(env)
    worker.low = True
    worker.error = lambda: DomainError(
        "resource_deferred", "Synthetic uncertain phase", effect_state=effect_state
    )
    try:
        for attempt in range(1, 6):
            with pytest.raises(DomainError) as error:
                await setup.processor.preprocess_audio(event)
            assert error.value.effect_state == effect_state
            assert rows(env, "ga_media_inputs")[0]["body"]["attempts"] == attempt
        assert not setup.calls and not rows(env, "ga_budget_reservations")
    finally:
        await setup.http.aclose()


@pytest.mark.parametrize("state", ["reserved", "inflight", "unknown", "settled", "released"])
async def test_any_current_attempt_reservation_is_preserved_before_distinct_pure_inference_retry(
    runtime_env, state
):
    env, worker = runtime_env, DeferredAudioWorker()
    setup = fixture_setup(env, worker=worker)
    event = attached(env)
    original_run, original_reservation = worker.run, []

    async def reserve_then_defer(context, operation, inputs, **kwargs):
        if operation == "audio_validate" and ":attempt-" in context.idempotency_key:
            # Persist independently of _InputBudget's local reservation set. A
            # lost reserve response must still prevent a supposedly free retry.
            reservation = setup.ledger.reserve(
                env.actor,
                context.idempotency_key + ":asr-0",
                "media.asr",
                "fixture-price",
                {"audio_seconds": Decimal(1)},
                source_event_id=event.id,
            )
            if state in {"inflight", "unknown", "settled"}:
                setup.ledger.begin(env.actor, reservation.id)
            if state == "unknown":
                setup.ledger.unknown(env.actor, reservation.id, "synthetic existing unknown")
            elif state == "settled":
                setup.ledger.settle(
                    env.actor, reservation.id, {"audio_seconds": Decimal(1)}, "fixture:paid"
                )
            elif state == "released":
                setup.ledger.release_unstarted(env.actor, reservation.id)
            original_reservation.extend(rows(env, "ga_budget_reservations"))
            raise ResourceDeferred()
        return await original_run(context, operation, inputs, **kwargs)

    worker.run = reserve_then_defer
    try:
        with pytest.raises(DomainError) as error:
            await setup.processor.preprocess_audio(event)
        assert error.value.code == "input_inference_retry" and error.value.retryable is True
        assert error.value.resource_ref == original_reservation[0]["id"]
        body = rows(env, "ga_media_inputs")[0]["body"]
        assert body["attempts"] == 1 and body["error_code"] == "input_inference_retry"
        assert body["last_inference_reservation_id"] == original_reservation[0]["id"]
        assert rows(env, "ga_budget_reservations") == original_reservation
        for _ in range(3):
            with pytest.raises(DomainError) as blocked:
                await setup.processor.preprocess_audio(event)
            assert blocked.value.code == "input_inference_retry"
        assert rows(env, "ga_media_inputs")[0]["body"]["attempts"] == 4
        reservations = rows(env, "ga_budget_reservations")
        assert len(reservations) == 4 and len({row["key"] for row in reservations}) == 4
        assert (
            next(row for row in reservations if row["id"] == original_reservation[0]["id"])
            == original_reservation[0]
        )
        assert not setup.calls and not rows(env, "ga_decisions")
    finally:
        await setup.http.aclose()


@pytest.mark.parametrize("change", ["claim_replaced", "source_forgotten"])
async def test_late_resource_error_cannot_refund_a_new_claim_or_restore_forgotten_input(
    runtime_env, change
):
    env, worker = runtime_env, DeferredAudioWorker()
    setup = fixture_setup(env, worker=worker)
    event = attached(env)
    original_run = worker.run
    entered, release = asyncio.Event(), asyncio.Event()

    async def waiting_worker(context, operation, inputs, **kwargs):
        if operation == "audio_validate" and ":attempt-" in context.idempotency_key:
            entered.set()
            await release.wait()
            raise ResourceDeferred()
        return await original_run(context, operation, inputs, **kwargs)

    worker.run = waiting_worker
    task = asyncio.create_task(setup.processor.preprocess_audio(event))
    try:
        await asyncio.wait_for(entered.wait(), timeout=5)
        if change == "source_forgotten":
            await asyncio.to_thread(
                MemoryService(env.store).forget, env.actor, event_ids=[event.id]
            )
        else:
            with env.store.transaction(env.actor) as tx:
                tx.conn.execute(
                    "UPDATE ga_media_inputs SET body=jsonb_set(body,'{claim_until}','0') WHERE group_id=%s AND event_id=%s",
                    (env.actor.group_id, event.id),
                )
            cached = rows(env, "ga_media_inputs")[0]
            await asyncio.to_thread(
                setup.processor._claim,
                env.actor,
                event,
                cached["attachment_id"],
                source_fingerprint(event),
                "newer-claim",
            )
        before = rows(env, "ga_media_inputs")[0]
        release.set()
        with pytest.raises(DomainError) as error:
            await task
        assert error.value.code == "resource_deferred"
        assert rows(env, "ga_media_inputs")[0] == before
        assert not rows(env, "ga_budget_reservations") and not setup.calls
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)
        await setup.http.aclose()
