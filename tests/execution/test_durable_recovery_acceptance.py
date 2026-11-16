"""Real PG and LangGraph recovery; models/workers are explicitly offline fakes."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from psycopg.types.json import Jsonb
from test_supervisor import (
    Factory,
    Validator,
    accept_pending,
    deliveries,
    new_work,
    supervisor,
    work_state,
)
from test_supervisor import (
    env as supervisor_env,
)

from groupagent.billing import BudgetLedger, PriceSnapshot
from groupagent.billing.metering import LedgerModelBudget
from groupagent.domain import WorkState
from groupagent.execution import (
    EngineCheckpoint,
    ExecutionRequest,
    LangGraphEngine,
    ModelMessage,
    ModelReply,
    ModelToolCall,
    RunState,
    ScriptedFakeModel,
    TokenUsage,
)
from groupagent.execution.models import UsageTotals
from groupagent.runtime.contracts import PreparedExecution
from groupagent.runtime.recovery import (
    RECONCILIATION_SCOPE_KEY,
    RECOVERY_SCOPE_KEY,
    RecoveryEvidence,
    checkpoint_digest,
    reconcile_unpublished_compute_checkpoint,
)
from groupagent.tools import (
    EffectState,
    PolicyDecision,
    ToolContext,
    ToolGateway,
    ToolReceipt,
    ToolScope,
    register_workspace_tools,
)
from groupagent.tools.compute import register_compute_tools
from groupagent.tools.gateway import tool_call_fingerprint
from groupagent.tools.models import ToolLedgerEntry

pytestmark = pytest.mark.postgres


@pytest.fixture
def env(tmp_path):
    return supervisor_env.__wrapped__(tmp_path)


def due_now(env, identifier, key=RECOVERY_SCOPE_KEY):
    with env.store.transaction(env.actor) as tx:
        current = tx.get_work(identifier)
        metadata = dict(current.scope[key])
        field = "next_check_at" if key == RECONCILIATION_SCOPE_KEY else "next_retry_at"
        metadata[field] = (datetime.now(UTC) - timedelta(seconds=1)).isoformat()
        tx.conn.execute(
            "UPDATE ga_work_orders SET scope=jsonb_set(scope,%s,%s) WHERE group_id=%s AND id=%s",
            ([key], Jsonb(metadata), env.actor.group_id, identifier),
        )


class SlicedFactory:
    def __init__(self):
        self.calls = 0
        self.models = []

    async def __call__(self, work, lease, actor, workspace):
        gateway = ToolGateway()
        register_workspace_tools(gateway)
        self.calls += 1
        model = ScriptedFakeModel(
            [
                ModelReply(
                    tool_calls=(
                        ModelToolCall(
                            id="write-once",
                            name="workspace_write_text",
                            arguments={"path": "result.md", "text": "Synthetic durable artifact"},
                        ),
                    ),
                    finish_reason="tool_calls",
                    usage=TokenUsage(input_tokens=10, output_tokens=5),
                )
            ]
            if self.calls == 1
            else [
                ModelReply(
                    text="Verified synthetic answer",
                    usage=TokenUsage(input_tokens=12, output_tokens=6),
                )
            ]
        )
        self.models.append(model)
        return PreparedExecution(
            LangGraphEngine(
                model, gateway, activation_max_tool_calls=1 if self.calls == 1 else 256
            ),
            ExecutionRequest(
                work_id=work.id,
                attempt_id=lease.attempt_id,
                actor=actor,
                objective=work.goal,
                workspace=workspace,
                trace_id=work.request_event_id,
                revision=work.goal_revision,
                allowed_tools=gateway.registered_names,
            ),
        )


async def test_safe_activation_yield_is_queued_durably_and_restart_never_repeats_completed_tool(
    env,
):
    work = new_work(env)
    factory = SlicedFactory()
    first = supervisor(env, factory, execution_backend="langgraph")
    waiting = await first.run_once()
    assert waiting.state == WorkState.QUEUED and waiting.pause_reason is None
    assert waiting.scope[RECOVERY_SCOPE_KEY]["error_code"] == "activation_yielded"
    checkpoint = env.state.load_checkpoint(env.actor, work.id)
    assert checkpoint.safe_to_resume and len(checkpoint.receipts) == 1
    assert checkpoint.receipts[0].effect_state == EffectState.SUCCEEDED
    content = (checkpoint.request.workspace / "result.md").read_bytes()
    await first.close()
    restarted = supervisor(env, factory, execution_backend="langgraph")
    try:
        assert await restarted.run_once() is None and factory.calls == 1
        due_now(env, work.id)
        delivered = await restarted.run_once()
        assert delivered.state == WorkState.DELIVERING and factory.calls == 2
        restored = env.state.load_checkpoint(env.actor, work.id)
        assert restored.usage.tool_calls == 1 and restored.usage.rounds == 2
        assert len(restored.receipts) == 1
        assert (restored.request.workspace / "result.md").read_bytes() == content
        assert len(deliveries(env, work.id)) == 1
        accept_pending(env, work.id)
        await restarted.tick()
        assert work_state(env, work.id).state == WorkState.COMPLETED
    finally:
        await restarted.close()


async def test_repeated_validation_faults_have_no_total_attempt_cutoff_and_eventually_deliver_once(
    env,
):
    work = new_work(env)
    factory, validator = Factory(), Validator(fail=5)
    service = supervisor(env, factory, validator, validation_repair_limit=0)
    try:
        for index in range(5):
            waiting = await service.run_once()
            assert waiting.state == WorkState.QUEUED and waiting.pause_reason is None
            plan = waiting.scope[RECOVERY_SCOPE_KEY]
            assert plan["consecutive_failures"] == index + 1
            assert not deliveries(env, work.id)
            assert await service.run_once() is None
            due_now(env, work.id)
        delivered = await service.run_once()
        assert delivered.state == WorkState.DELIVERING and len(factory.models) == 6
        checkpoint = env.state.load_checkpoint(env.actor, work.id)
        assert checkpoint.usage.rounds == 6 and checkpoint.backend_state["validation_repairs"] == 5
        assert len(deliveries(env, work.id)) == 1
        accept_pending(env, work.id)
        await service.tick()
        assert work_state(env, work.id).state == WorkState.COMPLETED
    finally:
        await service.close()


class NeverRun:
    allowed_programs = frozenset({"python3", "ls", "cat", "wc", "head", "tail", "sort"})

    async def run(self, spec, context):
        raise AssertionError("Legacy reconciliation cannot replay the original worker")


def legacy_unknown_work(env, *, external=False):
    work = new_work(env)
    service = supervisor(env)
    workspace = service._workspace(work)
    with env.store.transaction(env.actor) as tx:
        lease = tx.claim_work(work.id, "synthetic-stopped-controller")
    name = "native_send" if external else "python_compute"
    arguments = {"code": "print('synthetic prior probe')", "output_paths": ["missing.txt"]}
    receipt = ToolReceipt(
        call_id="legacy-failed",
        tool_name=name,
        idempotency_key=f"{work.id}:legacy-failed",
        success=False,
        effect_state=EffectState.UNKNOWN,
        error_code="worker_failed",
    )
    checkpoint = EngineCheckpoint(
        request=ExecutionRequest(
            work_id=work.id,
            attempt_id=lease.attempt_id,
            actor=env.actor,
            objective=work.goal,
            workspace=workspace,
            trace_id=work.request_event_id,
            allowed_tools=frozenset({name}),
        ),
        revision=work.goal_revision,
        state=RunState.PAUSED,
        sequence=5,
        usage=UsageTotals(rounds=2, tokens=100, tool_calls=1, active_seconds=3),
        safe_to_resume=False,
        error_code="effect_unknown",
        unresolved_tools=frozenset({name}),
        receipts=(receipt,),
        ledger=(
            ToolLedgerEntry(fingerprint=tool_call_fingerprint(name, arguments), receipt=receipt),
        ),
        messages=(
            ModelMessage(
                role="assistant",
                tool_calls=(ModelToolCall(id=receipt.call_id, name=name, arguments=arguments),),
            ),
            ModelMessage(
                role="tool", tool_call_id=receipt.call_id, content=receipt.model_dump_json()
            ),
        ),
    )
    env.state.save_checkpoint(env.actor, lease, checkpoint)
    with env.store.transaction(env.actor) as tx:
        current = tx.get_work(work.id)
        tx.transition_work(
            work.id,
            WorkState.PAUSED,
            expected_revision=current.revision,
            lease=lease,
            reason="effect_unknown",
        )
    return work, checkpoint


async def test_legacy_compute_recovery_archives_original_proof_and_resumes_same_work(env):
    work, original = legacy_unknown_work(env)
    gateway = ToolGateway()
    register_compute_tools(gateway, NeverRun())
    calls = []

    async def reconcile(current, checkpoint, actor):
        calls.append(checkpoint_digest(checkpoint))
        # This synthetic fixture has no output catalog, publication audit or
        # native sends at this call's boundary. Runtime already fenced/quiesced
        # the old controller before invoking this host-only callback.
        return reconcile_unpublished_compute_checkpoint(
            checkpoint,
            gateway=gateway,
            evidence=RecoveryEvidence(
                checkpoint_digest(checkpoint),
                True,
                True,
                True,
                True,
                "synthetic-host-history-proof",
            ),
        )

    service = supervisor(env, reconcile_checkpoint=reconcile)
    try:
        assert await service.run_once() is None
        waiting = work_state(env, work.id)
        assert waiting.state == WorkState.QUEUED and len(calls) == 1
        reconciled = env.state.load_checkpoint(env.actor, work.id)
        assert reconciled.receipts[0].effect_state == EffectState.NO_EFFECT
        assert reconciled.usage == original.usage
        with env.store.transaction(env.actor) as tx:
            audit = tx.conn.execute(
                "SELECT details FROM ga_audit_events WHERE group_id=%s AND resource_ref=%s AND action='work_effect_reconciled'",
                (env.actor.group_id, work.id),
            ).fetchone()
        assert audit["details"]["checkpoint_digest"] == checkpoint_digest(original)
        assert env.state._decode_checkpoint(audit["details"]["prior_checkpoint"]) == original
        due_now(env, work.id)
        delivered = await service.run_once()
        assert delivered.state == WorkState.DELIVERING and delivered.id == work.id
        assert len(deliveries(env, work.id)) == 1
    finally:
        await service.close()


async def test_real_unknown_effect_keeps_durable_inspection_and_is_never_replayed(env):
    work, original = legacy_unknown_work(env, external=True)
    factory = Factory()
    checks = []

    async def reconcile(current, checkpoint, actor):
        checks.append(checkpoint_digest(checkpoint))
        return None  # Native receipt service still cannot establish the result.

    service = supervisor(env, factory, reconcile_checkpoint=reconcile)
    try:
        assert await service.run_once() is None and len(checks) == 1
        first = work_state(env, work.id)
        plan = first.scope[RECONCILIATION_SCOPE_KEY]
        assert first.state == WorkState.PAUSED and plan["checks"] == 1
        assert await service.run_once() is None and len(checks) == 1
        due_now(env, work.id, RECONCILIATION_SCOPE_KEY)
        assert await service.run_once() is None and len(checks) == 2
        held = work_state(env, work.id)
        assert held.scope[RECONCILIATION_SCOPE_KEY]["checks"] == 2
        assert not factory.models and not deliveries(env, work.id)
        assert env.state.load_checkpoint(env.actor, work.id) == original
    finally:
        await service.close()


async def test_slow_external_receipt_inspection_does_not_starve_other_authorized_work(env):
    uncertain, original = legacy_unknown_work(env, external=True)
    ready = new_work(env, "Produce another verified synthetic answer")
    stopped = asyncio.Event()

    async def unavailable_receipt(current, checkpoint, actor):
        try:
            await asyncio.sleep(30)
        finally:
            stopped.set()

    service = supervisor(env, reconcile_checkpoint=unavailable_receipt)
    try:
        result = await asyncio.wait_for(service.run_once(), timeout=2)
        assert result.id == ready.id and result.state == WorkState.DELIVERING
        assert stopped.is_set()
        held = work_state(env, uncertain.id)
        assert held.state == WorkState.PAUSED and RECONCILIATION_SCOPE_KEY in held.scope
        assert env.state.load_checkpoint(env.actor, uncertain.id) == original
        assert not deliveries(env, uncertain.id)
    finally:
        await service.close()


@pytest.mark.parametrize("interruption", ["expired_running", "legacy_paused"])
async def test_expired_pure_inference_is_fenced_and_late_old_response_cannot_write(
    env, interruption
):
    work = new_work(env)
    ledger = BudgetLedger(env.store, allow_test_prices=True)
    ledger.configure(env.actor, currency="CNY", daily_limit=None, total_limit=None)
    ledger.register_price(
        env.actor,
        PriceSnapshot(
            id="synthetic-fenced-inference-price",
            provider="synthetic",
            operation="text",
            currency="CNY",
            rates={"input_tokens": Decimal("0.001"), "output_tokens": Decimal("0.001")},
            source_url="fixture://synthetic-fenced-inference-price",
            captured_at=datetime.now(UTC),
            test_only=True,
        ),
    )
    layout = supervisor(env)
    workspace = layout._workspace(work)
    with env.store.transaction(env.actor) as tx:
        old_lease = tx.claim_work(work.id, "synthetic-old-controller", ttl_seconds=30)

    async def old_policy(scope):
        with env.store.transaction(env.actor) as tx:
            tx.assert_lease(old_lease)
        return PolicyDecision(actor=env.actor, allowed_tools=scope.allowed_tools)

    old_gateway = ToolGateway(old_policy)
    register_workspace_tools(old_gateway)

    class LateOldModel:
        name = "synthetic-late-inference/offline"

        def __init__(self):
            self.started, self.release = asyncio.Event(), asyncio.Event()

        async def complete(self, request):
            self.started.set()
            await self.release.wait()
            return ModelReply(
                tool_calls=(
                    ModelToolCall(
                        id="late-old-write",
                        name="workspace_write_text",
                        arguments={
                            "path": "late-old.txt",
                            "text": "This old controller no longer has authority",
                        },
                    ),
                ),
                finish_reason="tool_calls",
                usage=TokenUsage(input_tokens=10, output_tokens=5),
            )

    old_model = LateOldModel()
    old_request = ExecutionRequest(
        work_id=work.id,
        attempt_id=old_lease.attempt_id,
        actor=env.actor,
        objective=work.goal,
        workspace=workspace,
        trace_id=work.request_event_id,
        allowed_tools=old_gateway.registered_names,
    )

    async def old_events(event):
        env.state.append_event(env.actor, old_lease, event)

    async def old_checkpoints(checkpoint):
        env.state.save_checkpoint(env.actor, old_lease, checkpoint)

    old_engine = LangGraphEngine(
        old_model,
        old_gateway,
        model_budget=LedgerModelBudget(ledger, price_id="synthetic-fenced-inference-price"),
        event_sink=old_events,
        checkpoint_sink=old_checkpoints,
    )
    old_run = await old_engine.start(old_request)
    await asyncio.wait_for(old_model.started.wait(), timeout=2)
    original = env.state.load_checkpoint(env.actor, work.id)
    assert original.backend_state["inflight_model_reservation"]
    assert not original.safe_to_resume and not original.unresolved_tools
    with env.store.transaction(env.actor) as tx:
        assert not tx.conn.execute(
            "SELECT 1 FROM ga_audit_events WHERE group_id=%s AND resource_ref=%s AND action='execution_stopped'",
            (env.actor.group_id, work.id),
        ).fetchone()
        tx.conn.execute(
            "UPDATE ga_work_orders SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE group_id=%s AND id=%s",
            (env.actor.group_id, work.id),
        )
        if interruption == "legacy_paused":
            current = tx.get_work(work.id)
            tx.transition_work(
                work.id,
                WorkState.PAUSED,
                expected_revision=current.revision,
                reason="provider_reconciliation_required",
            )

    class FreshFactory:
        async def __call__(self, current, lease, actor, directory):
            async def policy(scope):
                with env.store.transaction(actor) as tx:
                    tx.assert_lease(lease)
                return PolicyDecision(actor=actor, allowed_tools=scope.allowed_tools)

            gateway = ToolGateway(policy)
            register_workspace_tools(gateway)
            model = ScriptedFakeModel(
                [
                    ModelReply(
                        text="Verified synthetic answer after inference fencing",
                        usage=TokenUsage(input_tokens=12, output_tokens=6),
                    )
                ]
            )
            return PreparedExecution(
                LangGraphEngine(
                    model,
                    gateway,
                    model_budget=LedgerModelBudget(
                        ledger, price_id="synthetic-fenced-inference-price"
                    ),
                ),
                ExecutionRequest(
                    work_id=current.id,
                    attempt_id=lease.attempt_id,
                    actor=actor,
                    objective=current.goal,
                    workspace=directory,
                    trace_id=current.request_event_id,
                    revision=current.goal_revision,
                    allowed_tools=gateway.registered_names,
                ),
            )

    service = supervisor(env, FreshFactory(), execution_backend="langgraph")
    try:
        if interruption == "legacy_paused":
            assert await service.run_once() is None
            scheduled = work_state(env, work.id)
            assert scheduled.state == WorkState.QUEUED
            assert env.state.load_checkpoint(env.actor, work.id) == original
            due_now(env, work.id)
        result = await asyncio.wait_for(service.run_once(), timeout=3)
        assert result.state == WorkState.DELIVERING and result.id == work.id
        with env.store.transaction(env.actor) as tx:
            charges = tx.conn.execute(
                "SELECT id,state,key FROM ga_budget_reservations WHERE group_id=%s AND work_id=%s ORDER BY created_at",
                (env.actor.group_id, work.id),
            ).fetchall()
            fence = tx.conn.execute(
                "SELECT details FROM ga_audit_events WHERE group_id=%s AND resource_ref=%s AND action='work_inference_fenced'",
                (env.actor.group_id, work.id),
            ).fetchone()["details"]
            old_stored = tx.conn.execute(
                "SELECT body FROM ga_runtime_checkpoints WHERE group_id=%s AND work_id=%s AND attempt_id=%s",
                (env.actor.group_id, work.id, old_lease.attempt_id),
            ).fetchone()["body"]
        assert len(charges) == 2 and {row["state"] for row in charges} == {"unknown", "settled"}
        assert len({row["key"] for row in charges}) == 2
        assert fence["fencing_token"] > fence["prior_fencing_token"]
        assert fence["evidence"] == "fresh_database_lease_fences_old_inference_controller"
        assert env.state._decode_checkpoint(old_stored) == original
        old_model.release.set()
        await asyncio.wait_for(old_engine.wait(old_run), timeout=2)
        scope = ToolScope(
            env.actor.group_id,
            work.id,
            env.actor,
            workspace,
            old_gateway.registered_names,
            work.request_event_id,
        )
        stale = await old_gateway.invoke(
            "workspace_write_text",
            {"path": "late-direct.txt", "text": "stale"},
            ToolContext(scope, "late-direct", f"{work.id}:late-direct", asyncio.Event()),
        )
        assert stale.error_code == "stale_lease" and stale.effect_state == EffectState.NOT_STARTED
        assert (
            not (workspace / "late-old.txt").exists()
            and not (workspace / "late-direct.txt").exists()
        )
        assert len(deliveries(env, work.id)) == 1
        accept_pending(env, work.id)
        await service.tick()
        assert work_state(env, work.id).state == WorkState.COMPLETED
    finally:
        old_model.release.set()
        await asyncio.wait_for(old_engine.wait(old_run), timeout=2)
        await service.close()
