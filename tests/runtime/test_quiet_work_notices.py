"""Routine work is silent; this does not disable concrete missing-input notices."""

import hashlib

import pytest
from psycopg.types.json import Jsonb

from groupagent.domain import DomainError, WorkState
from groupagent.runtime.notices import check_notice
from groupagent.runtime.recovery import plan_recovery

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


async def test_automatic_recovery_has_no_notice_and_is_presented_as_background_work(runtime_env):
    env = runtime_env
    app, _ = app_for(env, {})
    request = event(env, "请整理资料。")
    try:
        await intake(app, env, request)
        with env.store.transaction(env.actor) as tx:
            work = tx.create_work("整理资料", request.id)
            work = tx.transition_work(
                work.id,
                WorkState.PAUSED,
                expected_revision=work.revision,
                reason="provider_unavailable",
            )
            metadata = plan_recovery(work, work.pause_reason, failure_key="synthetic-attempt")
            tx.conn.execute(
                "UPDATE ga_work_orders SET scope=jsonb_set(scope,%s,%s) WHERE group_id=%s AND id=%s",
                (["_runtime_recovery"], Jsonb(metadata), env.actor.group_id, work.id),
            )
        assert app.notices.tick() == 0
        # A new goal must not inherit an unrelated waiting job as its agenda.
        assert app.coordinator._context(env.actor, request)["works"] == []
        status = event(env, "这项工作现在怎么样？", reply_to=request.id)
        await intake(app, env, status)
        context = app.coordinator._context(env.actor, status)
        assert context["works"][0]["state"] == "queued"
        assert context["works"][0]["status_detail"]["code"] == "background_processing"
        with env.store.transaction(env.actor) as tx:
            assert not tx.list_delivery()
    finally:
        await app.close()


async def test_pending_legacy_recovery_notice_cannot_be_sent(runtime_env):
    env = runtime_env
    app, _ = app_for(env, {})
    request = event(env, "请整理资料。")
    try:
        await intake(app, env, request)
        with env.store.transaction(env.actor) as tx:
            work = tx.create_work("整理资料", request.id)
            tx.transition_work(
                work.id,
                WorkState.PAUSED,
                expected_revision=work.revision,
                reason="provider_timeout",
            )
            text = "text:Synthetic routine retry notice"
            intent = tx.enqueue_delivery(
                None, text, hashlib.sha256(text.encode()).hexdigest(), "synthetic-recovery-notice"
            )
            tx.conn.execute(
                "INSERT INTO ga_progress_notices(group_id,work_id,notice_key,intent_id) VALUES(%s,%s,%s,%s)",
                (env.actor.group_id, work.id, "paused:1", intent.id),
            )
        with pytest.raises(DomainError, match="Automatic recovery notices are silent"):
            with env.store.transaction(env.actor) as tx:
                check_notice(tx, intent.id)
        assert await app.dispatcher.tick() == 0
        assert not list((env.channel.root / "outgoing").iterdir())
    finally:
        await app.close()
