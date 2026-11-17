from __future__ import annotations

import asyncio
import threading
import time
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from psycopg.errors import DeadlockDetected
from psycopg.pq import TransactionStatus

from groupagent.coordinator import Decision
from groupagent.memory import MemoryService
from groupagent.memory.models import SummaryDraft
from groupagent.ops import CleanupRunner
from groupagent.persistence.postgres import bounded_owner_lock_waits
from groupagent.runtime.cleanup import RuntimeCleanupHandler

from .test_app import app_for, event

pytestmark = pytest.mark.postgres


async def wait_until(predicate, *, timeout=5):
    async with asyncio.timeout(timeout):
        while not predicate():
            await asyncio.sleep(0.01)


def expired_source(env):
    old = datetime.now(UTC) - timedelta(days=40)
    value = event(env, "synthetic expired source", occurred_at=old, received_at=old)
    with env.store.transaction(env.actor) as tx:
        tx.ingest_event(value, value.id, "1", 0)
    MemoryService(env.store).expire(env.service, before=datetime.now(UTC) - timedelta(days=30))
    return value


def install_cleanup(app, env, *, hold=None, entered=None, seen=None, quiesce=None, channel=None):
    owner = threading.get_ident()
    loop = asyncio.get_running_loop()

    async def owner_quiesce(works, events):
        assert threading.get_ident() == owner and asyncio.get_running_loop() is loop
        if quiesce is not None:
            await quiesce(works, events)
        else:
            await app.supervisor.quiesce(works)

    def owner_busy(works, events):
        assert threading.get_ident() == owner and asyncio.get_running_loop() is loop
        if seen is not None:
            seen.add("busy_owner")
        return False

    class SlowSynchronousCleanup(RuntimeCleanupHandler):
        def _remove_payloads(self, actor, job, plan):
            assert threading.get_ident() != owner
            with self.store.connect() as conn:
                assert conn.execute("SHOW lock_timeout").fetchone()["lock_timeout"] == "0"
            if seen is not None:
                seen.add("worker")
            if entered is not None:
                entered.set()
            if hold is not None:
                assert hold.wait(45), "Synthetic synchronous cleanup release was not supplied"
            super()._remove_payloads(actor, job, plan)

    handler = SlowSynchronousCleanup(
        env.store,
        env.artifacts,
        env.root,
        technical=env.service,
        channel=channel or env.channel,
        quiesce=owner_quiesce,
        is_busy=owner_busy,
    )
    app.cleanup = CleanupRunner(app.coordinator.memory, handler)


async def test_real_slow_locked_cleanup_keeps_heartbeat_intake_and_shutdown_lease(
    runtime_env, record_testsuite_property
):
    """Run longer than the actual thirty-second lease, with the real memory lock held."""
    env = runtime_env
    expired_source(env)
    app, _ = app_for(env, {})
    app.config = app.config.model_copy(
        update={
            "runtime": app.config.runtime.model_copy(update={"poll_seconds": 0.02}),
            "resources": app.config.resources.model_copy(update={"retry_seconds": 1}),
        }
    )
    app.coordinator.config = app.config
    entered, release = threading.Event(), threading.Event()
    seen = set()
    install_cleanup(app, env, hold=release, entered=entered, seen=seen)
    renewals = []
    heartbeat = app.state.heartbeat_process

    def renew(*args, **kwargs):
        result = heartbeat(*args, **kwargs)
        renewals.append(time.monotonic())
        return result

    app.state.heartbeat_process = renew
    polls = []
    consume = env.channel.consume

    async def consume_with_evidence(*args, **kwargs):
        polls.append(time.monotonic())
        return await consume(*args, **kwargs)

    env.channel.consume = consume_with_evidence
    closed = asyncio.Event()

    class Resource:
        async def aclose(self):
            assert not app._maintenance_lane.busy
            closed.set()

    app.resources = (Resource(),)
    incoming = event(env, "请解释一个合成术语")
    running = asyncio.create_task(app.run())
    replacement = None
    try:
        await wait_until(entered.is_set)
        began = time.monotonic()
        before_polls = len(polls)
        env.channel.append_incoming(incoming)
        await wait_until(lambda: len(polls) >= before_polls + 3, timeout=4)
        assert len(polls) >= before_polls + 3, (
            "Intake escalated resource waits behind cleanup's lock"
        )
        waiting_polls = polls[before_polls:]
        assert max(b - a for a, b in zip(waiting_polls, waiting_polls[1:], strict=False)) < 1.5
        assert not running.done() and app._maintenance_lane.busy
        # Stop during the same real blocking transaction. Renewal must outlive
        # stop.set() until the thread has actually stopped, not just its waiter.
        app.stop.set()
        await asyncio.sleep(0.1)
        running.cancel()
        await asyncio.sleep(0.05)
        running.cancel()
        assert not running.done() and not closed.is_set()
        await asyncio.sleep(max(0, 31.2 - (time.monotonic() - began)))
        assert len(renewals) >= 4 and not running.done()
        assert (
            env.state.claim_process(
                env.service, "application", "synthetic-too-early-replacement", ttl_seconds=30
            )
            is None
        ), "A new controller entered while the old cleanup writer was still alive"
        release.set()
        outcome = await asyncio.wait_for(asyncio.gather(running, return_exceptions=True), 5)
        assert isinstance(outcome[0], asyncio.CancelledError)
        assert closed.is_set() and seen >= {"worker", "busy_owner"}
        record_testsuite_property("maintenance_actual_lease_seconds", 30)
        record_testsuite_property("maintenance_actual_heartbeat_seconds", 8)
        record_testsuite_property("maintenance_real_block_seconds", time.monotonic() - began)
        record_testsuite_property("maintenance_heartbeat_renewals", len(renewals))
        record_testsuite_property("maintenance_intake_polls_while_held", len(polls) - before_polls)
        record_testsuite_property("maintenance_replacement_denied_before_join", True)
        replacement = env.state.claim_process(
            env.service, "application", "synthetic-joined-replacement", ttl_seconds=30
        )
        assert replacement is not None
        env.state.release_process(env.service, replacement)
        replacement = None
        restarted, _ = app_for(env, {})
        try:
            assert await restarted.poll_intake() == 1
            assert await restarted.poll_intake() == 0
            with env.store.transaction(env.actor) as tx:
                assert tx.get_event(incoming.id).text == incoming.text
        finally:
            await restarted.close()
    finally:
        app.stop.set()
        release.set()
        running.cancel()
        await asyncio.gather(running, return_exceptions=True)
        if replacement is not None:
            env.state.release_process(env.service, replacement)
        await app.close()


async def test_callback_affinity_and_compaction_use_owner_context(runtime_env):
    env = runtime_env
    expired_source(env)
    owner = threading.get_ident()
    loop = asyncio.get_running_loop()
    seen = set()
    app, _ = app_for(env, {})
    with env.store.transaction(env.actor) as tx:
        for _ in range(40):
            value = event(env, "synthetic context " * 40)
            tx.ingest_event(value, value.id, "1", 0)

    def assert_owner():
        assert threading.get_ident() == owner and asyncio.get_running_loop() is loop
        with env.store.connect() as conn:
            assert conn.execute("SHOW lock_timeout").fetchone()["lock_timeout"] == "25ms"

    class AffineSummarizer:
        async def summarize(self, request):
            assert_owner()
            seen.add("summarizer_owner")
            return SummaryDraft(
                text="Synthetic bounded summary",
                covered_event_ids=[value["id"] for value in request.events],
                provider="synthetic-affinity-fixture",
                model="no-model-call",
                usage={},
            )

    class AffineChannel:
        async def purge_event(self, *args, **kwargs):
            assert_owner()
            seen.add("channel_owner")
            return await env.channel.purge_event(*args, **kwargs)

    app.coordinator.memory.summarizer = AffineSummarizer()
    install_cleanup(app, env, seen=seen, channel=AffineChannel())
    try:
        with bounded_owner_lock_waits(lambda: app._maintenance_lane.busy):
            await app.maintenance(force=True)
        assert seen >= {"worker", "busy_owner", "summarizer_owner", "channel_owner"}
    finally:
        await app.close()


async def test_cancelled_maintenance_joins_owner_callback_and_never_overlaps(runtime_env):
    env = runtime_env
    value = expired_source(env)
    app, _ = app_for(env, {})
    entered, release = asyncio.Event(), asyncio.Event()
    seen = set()

    async def quiesce(works, events):
        entered.set()
        await release.wait()

    install_cleanup(app, env, seen=seen, quiesce=quiesce)
    maintenance = asyncio.create_task(app.maintenance(force=True))
    closing = None
    try:
        await asyncio.wait_for(entered.wait(), 3)
        maintenance.cancel()
        await asyncio.sleep(0.02)
        maintenance.cancel()
        await app.maintenance(force=True)
        closing = asyncio.create_task(app.close())
        await asyncio.sleep(0.05)
        closing.cancel()
        await asyncio.sleep(0.05)
        assert app._maintenance_lane.busy and not maintenance.done() and not closing.done()
        assert "worker" not in seen
        release.set()
        await asyncio.wait_for(asyncio.gather(maintenance, closing, return_exceptions=True), 3)
        with env.store.transaction(env.actor) as tx:
            assert (
                tx.conn.execute(
                    "SELECT count(*) AS n FROM ga_audit_events WHERE group_id=%s AND action='runtime_local_cleanup_verified'",
                    (env.actor.group_id,),
                ).fetchone()["n"]
                == 0
            )
            assert tx.get_event(value.id).content_state == "expired"
        assert not app._maintenance_lane.busy
    finally:
        release.set()
        await asyncio.gather(maintenance, *([closing] if closing else []), return_exceptions=True)
        await app.close()


async def test_real_lock_contention_is_non_escalating_resource_wait(runtime_env):
    env = runtime_env
    request = event(env, "@组长 请解释合成上下文")
    app, _ = app_for(
        env, {request.id: Decision(kind="observe", reason_code="synthetic_observation")}
    )
    with env.store.transaction(env.actor) as tx:
        tx.ingest_event(request, request.id, "1", 0)
    hold = env.store.connect()
    hold.execute(
        "SELECT pg_advisory_xact_lock(hashtextextended(%s,0))", ("memory:" + env.actor.group_id,)
    )
    try:
        with bounded_owner_lock_waits(lambda: True):
            with env.store.connect() as fresh:
                assert fresh.info.transaction_status == TransactionStatus.IDLE

            # to_thread deliberately copies ContextVars; its connection must
            # nevertheless retain the original worker lock policy.
            def worker_timeout():
                with env.store.connect() as conn:
                    return conn.execute("SHOW lock_timeout").fetchone()["lock_timeout"]

            assert await asyncio.to_thread(worker_timeout) == "0"
            for _ in range(2):
                started = time.monotonic()
                await app._process(request)
                assert time.monotonic() - started < 0.5
            with env.store.transaction(env.actor) as tx:
                retry = tx.conn.execute(
                    "SELECT attempts,blocked,code,next_attempt_at-clock_timestamp() AS delay FROM ga_event_retry WHERE group_id=%s AND event_id=%s",
                    (env.actor.group_id, request.id),
                ).fetchone()
                assert retry["attempts"] == 0 and retry["blocked"] is False
                assert retry["code"] == "resource_deferred"
                assert retry["delay"].total_seconds() <= app.config.resources.retry_seconds
                assert tx.get_decision(request.id) is None and not tx.list_delivery()
            with pytest.raises(DeadlockDetected):
                with env.store.transaction(env.actor) as tx:
                    tx.conn.execute("DO $$ BEGIN RAISE SQLSTATE '40P01'; END $$")
    finally:
        hold.rollback()
        hold.close()
    try:
        await app._process(request)
        with env.store.transaction(env.actor) as tx:
            assert tx.get_decision(request.id) is not None
            assert (
                tx.conn.execute(
                    "SELECT 1 FROM ga_event_retry WHERE group_id=%s AND event_id=%s",
                    (env.actor.group_id, request.id),
                ).fetchone()
                is None
            )
    finally:
        await app.close()


async def test_uncommitted_intake_interrupts_cleanup_before_next_job_and_retries(runtime_env):
    env = runtime_env
    expired_source(env)
    expired_source(env)
    app, _ = app_for(env, {})
    app.config = app.config.model_copy(
        update={
            "runtime": app.config.runtime.model_copy(update={"poll_seconds": 0.02}),
            "resources": app.config.resources.model_copy(update={"retry_seconds": 1}),
        }
    )
    app.coordinator.config = app.config
    entered, release = threading.Event(), threading.Event()
    install_cleanup(app, env, entered=entered, hold=release)
    incoming = event(env, "请解释一个新的合成术语")
    running = asyncio.create_task(app.run())
    try:
        await wait_until(entered.is_set)
        env.channel.append_incoming(incoming)
        await wait_until(app._maintenance_lane._cancel_requested.is_set)
        assert not running.done() and app._maintenance_lane.busy
        with env.store.transaction(env.actor) as tx:
            assert (
                tx.conn.execute(
                    "SELECT 1 FROM ga_events WHERE group_id=%s AND id=%s",
                    (env.actor.group_id, incoming.id),
                ).fetchone()
                is None
            )
        release.set()

        def committed():
            with env.store.transaction(env.actor) as tx:
                return (
                    tx.conn.execute(
                        "SELECT 1 FROM ga_events WHERE group_id=%s AND id=%s",
                        (env.actor.group_id, incoming.id),
                    ).fetchone()
                    is not None
                )

        await wait_until(committed)
        assert not running.done(), "Foreground arrival must not kill the maintenance loop"
        with env.store.transaction(env.actor) as tx:
            assert (
                tx.conn.execute(
                    "SELECT count(*) AS n FROM ga_audit_events WHERE group_id=%s AND action='runtime_local_cleanup_verified'",
                    (env.actor.group_id,),
                ).fetchone()["n"]
                == 1
            ), "A second cleanup job started after the arrival stop request"
    finally:
        release.set()
        app.stop.set()
        await asyncio.gather(running, return_exceptions=True)
        await app.close()


async def test_cancel_sync_backup_branch_finishes_current_job_but_not_the_next(runtime_env):
    env = runtime_env
    app, _ = app_for(env, {})
    entered, release = threading.Event(), threading.Event()
    with env.store.transaction(env.actor) as tx:
        for _ in range(2):
            tx.conn.execute(
                "INSERT INTO ga_memory_cleanup(id,group_id,resource_kind) VALUES(%s,%s,'backup_tombstone')",
                (uuid4().hex, env.actor.group_id),
            )

    class ExplicitSyntheticEmptyBackupHandler:
        calls = 0

        def expire(self, actor, *, now=None):
            self.calls += 1
            entered.set()
            assert release.wait(5)
            return []  # The actual scoped ga_ops_backups query also proves it is empty.

    backups = ExplicitSyntheticEmptyBackupHandler()
    app.cleanup = CleanupRunner(app.coordinator.memory, lambda job: "unused", backups=backups)
    maintenance = asyncio.create_task(app.maintenance(force=True))
    try:
        await wait_until(entered.is_set)
        maintenance.cancel()
        await asyncio.sleep(0.02)
        assert app._maintenance_lane.busy and not maintenance.done()
        release.set()
        await asyncio.wait_for(asyncio.gather(maintenance, return_exceptions=True), 3)
        assert backups.calls == 1
        with env.store.transaction(env.actor) as tx:
            states = tx.conn.execute(
                "SELECT state,count(*) AS n FROM ga_memory_cleanup WHERE group_id=%s GROUP BY state",
                (env.actor.group_id,),
            ).fetchall()
            assert {row["state"]: row["n"] for row in states} == {"pending": 1, "complete": 1}
    finally:
        release.set()
        await asyncio.gather(maintenance, return_exceptions=True)
        await app.close()
