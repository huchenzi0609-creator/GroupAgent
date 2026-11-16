"""Real local PG, synthetic pixels, fake engine and native queued receipts only."""

from __future__ import annotations

import io
from datetime import UTC, datetime
from types import SimpleNamespace

import pytest
from PIL import Image

from groupagent.artifacts import ArtifactScope, ValidationRecord
from groupagent.channels import ChannelReceipt
from groupagent.domain import DeliveryState, DomainError, WorkState
from groupagent.execution import (
    ExecutionRequest,
    ModelReply,
    ScopedToolEngine,
    ScriptedFakeModel,
    TokenUsage,
)
from groupagent.media.validation import inspect_image
from groupagent.runtime.contracts import DeliveryDraft, PreparedExecution, ValidatedCandidate
from groupagent.runtime.dispatch import Dispatcher
from groupagent.runtime.executor import WorkSupervisor
from groupagent.tools import ToolGateway

from .conftest import source

pytestmark = [pytest.mark.postgres, pytest.mark.public_image_workflow]


class NativeQueueFixture:
    def __init__(self, state="queued", *, has_job=True):
        self.state, self.has_job = state, has_job
        self.receipts = {}
        self.submissions = []

    async def lookup_delivery(self, group_id, key):
        receipt = self.receipts.get(key)
        assert receipt is None or receipt.group_id == group_id
        return receipt

    async def send_media(self, group_id, name, content, *, idempotency_key, kind, **kwargs):
        assert kind == "image" and idempotency_key not in self.receipts
        inspect_image(content)
        self.submissions.append(idempotency_key)
        receipt = ChannelReceipt(
            account_id="synthetic-account",
            group_id=group_id,
            idempotency_key=idempotency_key,
            wire_idempotency_key=idempotency_key,
            state=self.state,
            job_id="native-job:" + idempotency_key if self.has_job else None,
            observed_at=datetime.now(UTC),
        )
        self.receipts[idempotency_key] = receipt
        return receipt

    def accept(self, key):
        self.receipts[key] = self.receipts[key].model_copy(
            update={"state": "accepted", "platform_message_id": "native-message:" + key}
        )


def prepared_supervisor(env, channel, *, image_count=5):
    event = source(env, f"Deliver {image_count} synthetic images")
    with env.store.transaction(env.actor) as tx:
        work = tx.create_work("Deliver synthetic images", event.id)
    calls = SimpleNamespace(factory=0, validation=0)

    async def factory(work, lease, actor, workspace):
        calls.factory += 1
        return PreparedExecution(
            ScopedToolEngine(
                ScriptedFakeModel(
                    [
                        ModelReply(
                            text="Synthetic image results",
                            usage=TokenUsage(input_tokens=1, output_tokens=1),
                        )
                    ]
                ),
                ToolGateway(),
            ),
            ExecutionRequest(
                work_id=work.id,
                attempt_id=lease.attempt_id,
                actor=actor,
                objective=work.goal,
                workspace=workspace,
                revision=work.goal_revision,
                trace_id=work.request_event_id,
            ),
        )

    class Validator:
        async def validate(self, work, lease, actor, snapshot, workspace):
            calls.validation += 1
            scope = ArtifactScope(actor.group_id, work.id, actor.member_id)
            drafts = []
            for index in range(image_count):
                stream = io.BytesIO()
                Image.new("RGB", (24 + index, 16), (index * 30, 50, 80)).save(stream, "PNG")
                content = stream.getvalue()
                info, _ = inspect_image(content)
                version = env.artifacts.create(
                    scope,
                    f"synthetic-{index}.png",
                    content,
                    provenance={"media_metadata": info.model_dump(mode="json")},
                )
                validation = ValidationRecord(
                    file_id=version.file_id,
                    version_id=version.version_id,
                    sha256=version.sha256,
                    format="png",
                    structural_passed=True,
                    content_passed=True,
                    media_decoded=True,
                    checks=("synthetic_fixture_actual_pixel_decode",),
                )
                env.artifacts.record_validation(
                    scope, version.file_id, version.version_id, validation
                )
                env.registry.register(actor, lease, version, validation=validation.to_dict())
                drafts.append(
                    DeliveryDraft(
                        "artifact:" + version.version_id,
                        version.sha256,
                        f"synthetic-image:{work.id}:{index}",
                    )
                )
            return ValidatedCandidate({"result": "synthetic-pixel-fixture"}, tuple(drafts))

    supervisor = WorkSupervisor(
        env.store,
        env.state,
        env.service,
        lambda _: env.actor,
        factory,
        Validator(),
        env.root / "workspaces",
    )
    dispatcher = Dispatcher(env.state, env.service, lambda _: env.actor, channel, env.registry)
    return work, supervisor, dispatcher, calls


def current_work(env, work):
    with env.store.transaction(env.actor) as tx:
        return tx.get_work(work.id)


def attempt_count(env, work):
    with env.store.transaction(env.actor) as tx:
        return tx.conn.execute(
            "SELECT count(*) AS n FROM ga_execution_attempts WHERE group_id=%s AND work_id=%s",
            (env.actor.group_id, work.id),
        ).fetchone()["n"]


@pytest.mark.parametrize("native_state", ["queued", "submitting", "running"])
async def test_normal_async_image_bundle_stays_delivering_until_every_native_acceptance(
    runtime_env, native_state
):
    env, channel = runtime_env, NativeQueueFixture(native_state)
    work, supervisor, dispatcher, calls = prepared_supervisor(env, channel)
    initial = await supervisor.run_once()
    assert initial.state == WorkState.DELIVERING, initial.pause_reason
    delivering_revision = current_work(env, work).revision
    for index in range(5):
        assert await dispatcher.tick(limit=1) == 0
        assert len(channel.submissions) == index + 1
        key = channel.submissions[-1]
        with env.store.transaction(env.actor) as tx:
            parent = next(row for row in tx.list_delivery() if row.state == DeliveryState.UNKNOWN)
            part = tx.conn.execute(
                "SELECT state,platform_job_id FROM ga_delivery_parts WHERE group_id=%s AND intent_id=%s",
                (env.actor.group_id, parent.id),
            ).fetchone()
            assert part == {"state": "submitted", "platform_job_id": "native-job:" + key}
        states = supervisor._delivery_states(env.actor, current_work(env, work))
        assert states == ("accepted",) * index + ("dispatching",) + ("pending",) * (4 - index)
        for _ in range(2):
            await supervisor.tick()
            assert current_work(env, work).state == WorkState.DELIVERING
            assert current_work(env, work).revision == delivering_revision
            assert await dispatcher.tick(limit=1) == 0
            assert len(channel.submissions) == index + 1
        channel.accept(key)
        assert await dispatcher.tick(limit=1) == 1
        await supervisor._poll_deliveries()
    assert current_work(env, work).state == WorkState.COMPLETED
    assert calls.factory == calls.validation == 1
    assert len(channel.submissions) == len(set(channel.submissions)) == 5
    with env.store.transaction(env.actor) as tx:
        assert all(row.state == DeliveryState.ACCEPTED for row in tx.list_delivery())
        assert (
            tx.conn.execute(
                "SELECT count(*) AS n FROM ga_audit_events WHERE group_id=%s AND resource_ref=%s "
                "AND action='work_paused'",
                (env.actor.group_id, work.id),
            ).fetchone()["n"]
            == 0
        )
    await supervisor.close()


@pytest.mark.parametrize("uncertainty", ["missing_native_job", "needs_review", "lost_response"])
async def test_real_uncertainty_still_pauses_without_revalidation_or_resending(
    runtime_env, monkeypatch, uncertainty
):
    env = runtime_env
    channel = NativeQueueFixture(has_job=uncertainty != "missing_native_job")
    if uncertainty == "lost_response":
        original = channel.send_media

        async def response_lost(*args, **kwargs):
            await original(*args, **kwargs)
            raise RuntimeError("synthetic response lost after submission")

        monkeypatch.setattr(channel, "send_media", response_lost)
    work, supervisor, dispatcher, calls = prepared_supervisor(env, channel, image_count=1)
    initial = await supervisor.run_once()
    assert initial.state == WorkState.DELIVERING, initial.pause_reason
    if uncertainty == "lost_response":
        with pytest.raises(RuntimeError, match="synthetic response lost"):
            await dispatcher.tick(limit=1)
        # A real lookup with a native job may later make this safe to wait;
        # this branch intentionally has no such response available yet.
        channel.receipts.clear()
    else:
        assert await dispatcher.tick(limit=1) == 0
        if uncertainty == "needs_review":
            key = channel.submissions[0]
            channel.receipts[key] = channel.receipts[key].model_copy(
                update={"state": "needs_review"}
            )
            assert await dispatcher.tick(limit=1) == 0
    await supervisor._poll_deliveries()
    current = current_work(env, work)
    assert current.state == WorkState.PAUSED and current.pause_reason == "delivery_uncertain"
    assert supervisor._delivery_states(env.actor, current) == ("unknown",)
    for _ in range(2):
        await supervisor.tick()
        assert await dispatcher.tick(limit=1) == 0
        assert current_work(env, work).state == WorkState.PAUSED
    assert calls.factory == calls.validation == len(channel.submissions) == 1
    await supervisor.close()


def restart_supervisor(env, previous):
    # Simulate process loss, without graceful close pausing its owned work.
    previous._delivering.clear()
    return WorkSupervisor(
        env.store,
        env.state,
        env.service,
        lambda _: env.actor,
        previous.factory,
        previous.validator,
        env.root / "workspaces",
    )


def expire_lease(env, work):
    with env.store.transaction(env.actor) as tx:
        tx.conn.execute(
            "UPDATE ga_work_orders SET lease_expires_at=clock_timestamp()-interval '1 second' "
            "WHERE group_id=%s AND id=%s",
            (env.actor.group_id, work.id),
        )
        tx.conn.execute(
            "UPDATE ga_execution_attempts SET expires_at=clock_timestamp()-interval '1 second' "
            "WHERE group_id=%s AND work_id=%s AND state='active'",
            (env.actor.group_id, work.id),
        )


async def test_restart_takes_fenced_delivery_supervision_only_and_tracks_original_native_job(
    runtime_env,
):
    env, channel = runtime_env, NativeQueueFixture("running")
    work, supervisor, dispatcher, calls = prepared_supervisor(env, channel, image_count=1)
    assert (await supervisor.run_once()).state == WorkState.DELIVERING
    assert await dispatcher.tick(limit=1) == 0
    key = channel.submissions[0]
    original_checkpoint = env.state.load_checkpoint(env.actor, work.id)
    for _ in range(2):
        old_lease = supervisor._delivering[work.id].lease
        expire_lease(env, work)
        supervisor = restart_supervisor(env, supervisor)
        recovered = await supervisor.run_once()
        assert recovered.state == WorkState.DELIVERING and recovered.pause_reason is None
        new_lease = supervisor._delivering[work.id].lease
        assert new_lease.fencing_token > old_lease.fencing_token
        assert new_lease.attempt_id != old_lease.attempt_id
        assert supervisor._delivering[work.id].engine is None
        with pytest.raises(DomainError, match="lease"):
            with env.store.transaction(env.actor) as tx:
                tx.assert_lease(old_lease)
        assert env.state.load_checkpoint(env.actor, work.id) == original_checkpoint
        assert await dispatcher.tick(limit=1) == 0
        assert channel.submissions == [key]
        assert calls.factory == calls.validation == 1
    channel.accept(key)
    assert await dispatcher.tick(limit=1) == 1
    await supervisor._poll_deliveries()
    assert current_work(env, work).state == WorkState.COMPLETED
    assert channel.submissions == [key] and calls.factory == calls.validation == 1
    await supervisor.close()


async def test_restart_with_needs_review_part_does_not_claim_a_supervision_lease(runtime_env):
    env, channel = runtime_env, NativeQueueFixture()
    work, supervisor, dispatcher, calls = prepared_supervisor(env, channel, image_count=1)
    await supervisor.run_once()
    await dispatcher.tick(limit=1)
    key = channel.submissions[0]
    channel.receipts[key] = channel.receipts[key].model_copy(update={"state": "needs_review"})
    await dispatcher.tick(limit=1)
    assert current_work(env, work).state == WorkState.DELIVERING
    old_attempt = attempt_count(env, work)
    expire_lease(env, work)
    supervisor = restart_supervisor(env, supervisor)
    recovered = await supervisor.run_once()
    assert recovered.state == WorkState.PAUSED and recovered.pause_reason == "delivery_uncertain"
    assert attempt_count(env, work) == old_attempt
    assert not supervisor.delivering_work_ids
    assert calls.factory == calls.validation == len(channel.submissions) == 1
    await supervisor.close()


@pytest.mark.parametrize("missing_evidence", ["validation", "bundle", "safe_candidate"])
async def test_restart_refuses_incomplete_durable_candidate_evidence(runtime_env, missing_evidence):
    env, channel = runtime_env, NativeQueueFixture()
    work, supervisor, dispatcher, calls = prepared_supervisor(env, channel, image_count=1)
    await supervisor.run_once()
    await dispatcher.tick(limit=1)
    old_attempt = attempt_count(env, work)
    if missing_evidence == "safe_candidate":
        checkpoint = env.state.load_checkpoint(env.actor, work.id)
        env.state.save_checkpoint(
            env.actor,
            supervisor._delivering[work.id].lease,
            checkpoint.model_copy(
                update={"safe_to_resume": False, "sequence": checkpoint.sequence + 1}
            ),
        )
    else:
        with env.store.transaction(env.actor) as tx:
            if missing_evidence == "validation":
                tx.conn.execute(
                    "DELETE FROM ga_work_validations WHERE group_id=%s AND work_id=%s",
                    (env.actor.group_id, work.id),
                )
            else:
                tx.conn.execute(
                    "DELETE FROM ga_delivery_bundles WHERE group_id=%s AND bundle_id=%s",
                    (env.actor.group_id, f"result:{work.id}:{work.goal_revision}"),
                )
    expire_lease(env, work)
    supervisor = restart_supervisor(env, supervisor)
    recovered = await supervisor.run_once()
    assert recovered.state == WorkState.PAUSED
    assert attempt_count(env, work) == old_attempt and not supervisor.delivering_work_ids
    assert calls.factory == calls.validation == len(channel.submissions) == 1
    await supervisor.close()


async def test_live_finalizer_cannot_be_replaced_by_delivery_supervision(runtime_env):
    env, channel = runtime_env, NativeQueueFixture()
    work, supervisor, dispatcher, calls = prepared_supervisor(env, channel, image_count=1)
    await supervisor.run_once()
    await dispatcher.tick(limit=1)
    current = current_work(env, work)
    with pytest.raises(DomainError) as failed:
        env.state.recover_phase(
            env.actor,
            work.id,
            expected_revision=current.revision,
            evidence_ref="synthetic-live-owner-test",
            worker_id="synthetic-contender",
        )
    assert failed.value.code == "lease_active"
    assert current_work(env, work) == current
    assert calls.factory == calls.validation == len(channel.submissions) == 1
    channel.accept(channel.submissions[0])
    await dispatcher.tick(limit=1)
    await supervisor._poll_deliveries()
    await supervisor.close()


async def test_accepted_parts_before_parent_finish_recover_without_revalidation_or_resend(
    runtime_env, monkeypatch
):
    env, channel = runtime_env, NativeQueueFixture()
    work, supervisor, dispatcher, calls = prepared_supervisor(env, channel, image_count=1)
    await supervisor.run_once()
    await dispatcher.tick(limit=1)
    key = channel.submissions[0]
    channel.accept(key)
    original_finish = dispatcher._finish

    def interrupted_finish(*args, **kwargs):
        raise RuntimeError("synthetic crash before parent finish")

    monkeypatch.setattr(dispatcher, "_finish", interrupted_finish)
    with pytest.raises(RuntimeError, match="before parent finish"):
        await dispatcher.tick(limit=1)
    with env.store.transaction(env.actor) as tx:
        assert tx.list_delivery()[0].state == DeliveryState.UNKNOWN
        part = tx.conn.execute(
            "SELECT state,platform_message_id FROM ga_delivery_parts WHERE group_id=%s",
            (env.actor.group_id,),
        ).fetchone()
        assert part == {"state": "accepted", "platform_message_id": "native-message:" + key}
    assert supervisor._delivery_states(env.actor, current_work(env, work)) == ("dispatching",)
    await supervisor._poll_deliveries()
    assert current_work(env, work).state == WorkState.DELIVERING
    expire_lease(env, work)
    supervisor = restart_supervisor(env, supervisor)
    assert (await supervisor.run_once()).state == WorkState.DELIVERING
    with env.store.transaction(env.actor) as tx:
        assert tx.list_delivery()[0].state == DeliveryState.UNKNOWN
    monkeypatch.setattr(dispatcher, "_finish", original_finish)
    assert await dispatcher.tick(limit=1) == 1
    await supervisor._poll_deliveries()
    assert current_work(env, work).state == WorkState.COMPLETED
    assert calls.factory == calls.validation == len(channel.submissions) == 1
    await supervisor.close()


async def test_restart_rechecks_secondary_source_fingerprint_inside_supervision_transaction(
    runtime_env, monkeypatch
):
    env, channel = runtime_env, NativeQueueFixture()
    work, supervisor, dispatcher, calls = prepared_supervisor(env, channel, image_count=1)
    await supervisor.run_once()
    await dispatcher.tick(limit=1)
    secondary = source(env, "Synthetic secondary candidate evidence")
    checkpoint = env.state.load_checkpoint(env.actor, work.id)
    env.state.save_checkpoint(
        env.actor,
        supervisor._delivering[work.id].lease,
        checkpoint.model_copy(update={"sequence": checkpoint.sequence + 1}),
        source_event_ids=(secondary.id,),
    )
    original_load, changed = env.state.load_checkpoint, False

    def source_changes_after_preliminary_check(actor, work_id):
        nonlocal changed
        loaded = original_load(actor, work_id)
        if not changed:
            changed = True
            with env.store.transaction(env.actor) as tx:
                tx.conn.execute(
                    "UPDATE ga_events SET body=jsonb_set(body,'{text}',to_jsonb(%s::text)) "
                    "WHERE group_id=%s AND id=%s",
                    ("Changed synthetic secondary evidence", env.actor.group_id, secondary.id),
                )
        return loaded

    monkeypatch.setattr(env.state, "load_checkpoint", source_changes_after_preliminary_check)
    old_attempts = attempt_count(env, work)
    expire_lease(env, work)
    supervisor = restart_supervisor(env, supervisor)
    recovered = await supervisor.run_once()
    assert recovered.state == WorkState.PAUSED and recovered.pause_reason == "source_unavailable"
    assert attempt_count(env, work) == old_attempts and not supervisor.delivering_work_ids
    assert calls.factory == calls.validation == len(channel.submissions) == 1
    await supervisor.close()
