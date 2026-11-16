"""Real durable state for continued work, with synthetic data and no paid requests."""

import pytest

from groupagent.domain import WorkState
from groupagent.execution import EngineCheckpoint, ExecutionRequest, ModelMessage, RunState
from groupagent.execution.models import UsageTotals
from groupagent.runtime.rollback import park_for_legacy_rollback

from .conftest import source

pytestmark = pytest.mark.postgres


def test_large_checkpoint_roundtrip_and_same_sequence_replay(runtime_env):
    env = runtime_env
    request = source(env)
    with env.store.transaction(env.actor) as tx:
        work = tx.create_work("Synthetic durable long work", request.id)
        lease = tx.claim_work(work.id, "synthetic-state-worker", ttl_seconds=300)
    checkpoint = EngineCheckpoint(
        request=ExecutionRequest(
            work_id=work.id,
            attempt_id=lease.attempt_id,
            actor=env.actor,
            objective=work.goal,
            workspace=env.root,
            trace_id=request.id,
        ),
        revision=work.goal_revision,
        state=RunState.PAUSED,
        sequence=1,
        messages=(ModelMessage(role="user", content="x" * (9 * 1024**2)),),
        usage=UsageTotals(rounds=501, tool_calls=1001, tokens=1_000_001, active_seconds=10001),
        safe_to_resume=True,
    )
    env.state.save_checkpoint(env.actor, lease, checkpoint)
    assert env.state.load_checkpoint(env.actor, work.id) == checkpoint
    env.state.save_checkpoint(env.actor, lease, checkpoint)
    with env.store.transaction(env.actor) as tx:
        row = tx.conn.execute(
            "SELECT body ? 'checkpoint_codec' AS compressed FROM ga_runtime_checkpoints "
            "WHERE group_id=%s AND work_id=%s",
            (env.actor.group_id, work.id),
        ).fetchone()
    assert row["compressed"] is True


def test_legacy_rollback_parks_work_and_preserves_explicit_user_control(runtime_env):
    env = runtime_env
    first, second, third = [source(env) for _ in range(3)]
    with env.store.transaction(env.actor) as tx:
        queued = tx.create_work("Synthetic queued work", first.id)
        paused = tx.create_work("Synthetic user pause", second.id)
        tx.transition_work(
            paused.id, WorkState.PAUSED, expected_revision=paused.revision, reason="user_request"
        )
        cancelled = tx.create_work("Synthetic cancellation", third.id)
        tx.transition_work(
            cancelled.id,
            WorkState.CANCELLED,
            expected_revision=cancelled.revision,
            reason="cancelled",
        )
    assert park_for_legacy_rollback(env.store, env.actor) == 1
    assert park_for_legacy_rollback(env.store, env.actor) == 0
    with env.store.transaction(env.actor) as tx:
        result = tx.get_work(queued.id)
        assert result.pause_reason == "legacy_release_rollback"
        assert result.scope["_runtime_release_rollback"]["state"] == "queued"
        assert tx.get_work(paused.id).pause_reason == "user_request"
        assert tx.get_work(cancelled.id).state == WorkState.CANCELLED
