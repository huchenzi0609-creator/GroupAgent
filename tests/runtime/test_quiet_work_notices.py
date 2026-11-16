"""Routine work is silent; this does not disable concrete missing-input notices."""

import pytest

from groupagent.domain import WorkState

from .test_app import app_for, event
from .test_early_ack import intake

pytestmark = pytest.mark.postgres


async def test_running_and_validation_never_emit_timer_boilerplate(runtime_env):
    env = runtime_env
    app, _ = app_for(env, {})
    request = event(env, "请制作测试文档。")
    try:
        await intake(app, env, request)
        with env.store.transaction(env.actor) as tx:
            work = tx.create_work("制作测试文档", request.id)
            lease = tx.claim_work(work.id, "synthetic-worker", ttl_seconds=300)
            tx.conn.execute(
                "UPDATE ga_work_orders SET updated_at=clock_timestamp()-interval '10 minutes' WHERE group_id=%s AND id=%s",
                (env.actor.group_id, work.id),
            )
        assert app.notices.tick() == 0
        with env.store.transaction(env.actor) as tx:
            current = tx.get_work(work.id)
            tx.transition_work(
                work.id, WorkState.VALIDATING, expected_revision=current.revision, lease=lease
            )
            tx.conn.execute(
                "UPDATE ga_work_orders SET updated_at=clock_timestamp()-interval '10 minutes' WHERE group_id=%s AND id=%s",
                (env.actor.group_id, work.id),
            )
        assert app.notices.tick() == 0
        with env.store.transaction(env.actor) as tx:
            assert tx.list_delivery() == []
    finally:
        await app.close()
