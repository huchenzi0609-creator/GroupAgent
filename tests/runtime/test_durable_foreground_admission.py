"""Real PG admission: committed actions awaiting prose and recoverable event waits."""

from datetime import UTC, datetime

import pytest
from psycopg.types.json import Jsonb

from groupagent.coordinator import CoordinationResult, Decision
from groupagent.domain import DomainError

from .conftest import source
from .test_app import app_for

pytestmark = pytest.mark.postgres


async def test_committed_action_pending_response_remains_admissible_after_restart(runtime_env):
    env = runtime_env
    request = source(env, "Pot，请建立工作并回复结果。")
    result = CoordinationResult(
        event_id=request.id,
        decision=Decision(kind="reply", response="工作已建立。", reason_code="synthetic_commit"),
    )
    record = {
        "kind": "reply",
        "result": result.model_dump(mode="json"),
        "agent_pending_response": True,
        "action_receipts": [],
    }
    with env.store.transaction(env.actor) as tx:
        tx.record_decision(request.id, record)
    restarted, _ = app_for(env, {})
    with env.store.transaction(env.actor) as tx:
        assert request.id in {item.id for item in tx.list_pending_events()}
    assert request.id in {item.id for item in restarted._ready_events()}
    with env.store.transaction(env.actor) as tx:
        record["agent_pending_response"] = False
        tx.conn.execute(
            "UPDATE ga_decisions SET body=%s WHERE group_id=%s AND event_id=%s",
            (Jsonb(record), tx.group_id, request.id),
        )
        assert request.id not in {item.id for item in tx.list_pending_events()}
    assert request.id not in {item.id for item in restarted._ready_events()}
    await restarted.close()


async def test_recoverable_event_faults_back_off_without_a_total_retry_budget(runtime_env):
    env = runtime_env
    request = source(env, "Pot，请回答这条消息。")
    app, _ = app_for(
        env,
        {request.id: Decision(kind="reply", response="我在。", reason_code="synthetic_answer")},
    )
    process = app.coordinator.process
    failures = 0

    async def flaky(event):
        nonlocal failures
        if failures < 7:
            failures += 1
            raise DomainError(
                "network_unavailable", "Synthetic transient provider fault", retryable=True
            )
        return await process(event)

    app.coordinator.process = flaky
    for _ in range(7):
        await app._process(request)
    with env.store.transaction(env.actor) as tx:
        retry = tx.conn.execute(
            "SELECT attempts,blocked,next_attempt_at FROM ga_event_retry "
            "WHERE group_id=%s AND event_id=%s",
            (tx.group_id, request.id),
        ).fetchone()
        assert retry["attempts"] == 7 and not retry["blocked"]
        assert retry["next_attempt_at"] > datetime.now(UTC)
        assert tx.get_decision(request.id) is None
        assert not tx.list_delivery()
    assert request.id not in {item.id for item in app._ready_events()}
    with env.store.transaction(env.actor) as tx:
        tx.conn.execute(
            "UPDATE ga_event_retry SET next_attempt_at=clock_timestamp() "
            "WHERE group_id=%s AND event_id=%s",
            (tx.group_id, request.id),
        )
    assert await app.process_pending(batched=False) == 1
    with env.store.transaction(env.actor) as tx:
        assert tx.get_decision(request.id) is not None
        assert len(tx.list_delivery()) == 1
        assert not tx.conn.execute(
            "SELECT 1 FROM ga_event_retry WHERE group_id=%s AND event_id=%s",
            (tx.group_id, request.id),
        ).fetchone()
    await app.close()
