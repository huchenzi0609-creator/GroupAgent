"""Adversarial index interleavings against PostgreSQL's actual unique indexes.

The rolled-back primary-key placeholder is a synchronization device. It holds
one insertion after its arbiter precheck while another insertion commits the
same logical delivery. No production constraint or index is removed or weakened.
"""

from __future__ import annotations

import hashlib
import os
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from queue import Queue
from threading import Barrier, current_thread
from uuid import uuid4

import pytest

from groupagent.domain import ActorContext, DeliveryIntent, DomainError, EventKind, GroupEvent, Role
from groupagent.persistence import PostgresStore, UnitOfWork
from groupagent.persistence import postgres as postgres_module

pytestmark = pytest.mark.postgres


@pytest.fixture(scope="module")
def store() -> PostgresStore:
    dsn = os.environ.get("GROUPAGENT_TEST_DSN")
    if not dsn:
        pytest.skip("Set GROUPAGENT_TEST_DSN for real PostgreSQL outbox concurrency tests")
    result = PostgresStore(dsn)
    assert result.migrate() == 13
    return result


@pytest.fixture
def actor(store: PostgresStore) -> ActorContext:
    group_id = "outbox-race-" + uuid4().hex
    store.register_group(group_id)
    return ActorContext(group_id=group_id, member_id="synthetic-member", role=Role.MEMBER)


def _source(store: PostgresStore, actor: ActorContext) -> GroupEvent:
    stamp = datetime.now(UTC)
    event_id = uuid4().hex
    event = GroupEvent(
        id=event_id,
        source_key="outbox-race:" + event_id,
        group_id=actor.group_id,
        actor_id=actor.member_id,
        kind=EventKind.TEXT,
        text="Synthetic request",
        occurred_at=stamp,
        received_at=stamp,
    )
    with store.transaction(actor) as tx:
        tx.ingest_event(event, event_id, event_id, 0)
    return event


def _enqueue(tx: UnitOfWork, *, reply: bool, source_id: str, text: str) -> DeliveryIntent:
    if reply:
        return tx.enqueue_reply(source_id, text, read_only_authorized=True)
    return tx.enqueue_delivery(
        None, "text:" + text, hashlib.sha256(text.encode()).hexdigest(), "generic:" + source_id
    )


def _wait_until_blocked(store: PostgresStore, waiting_pid: int, blocker_pid: int) -> None:
    """Observe the database lock, rather than guessing timing with a fixed sleep."""
    deadline = time.monotonic() + 10
    with store.connect() as monitor:
        while time.monotonic() < deadline:
            row = monitor.execute(
                "SELECT %s::integer = ANY(pg_blocking_pids(%s)) AS blocked",
                (blocker_pid, waiting_pid),
            ).fetchone()
            if row is not None and row["blocked"]:
                return
            time.sleep(0.01)
    pytest.fail("Contending insertion never reached the intended database lock")


@pytest.mark.parametrize(
    "reply", [pytest.param(True, id="reply"), pytest.param(False, id="generic")]
)
@pytest.mark.parametrize(
    "same_body", [pytest.param(True, id="same-body"), pytest.param(False, id="different-body")]
)
def test_index_interleaving_resolves_canonical_delivery(
    store: PostgresStore,
    actor: ActorContext,
    monkeypatch: pytest.MonkeyPatch,
    reply: bool,
    same_body: bool,
) -> None:
    if reply:
        actor = actor.model_copy(update={"role": Role.OBSERVER})
    source = _source(store, actor)
    blocked_id = uuid4().hex
    original_id = postgres_module._id
    thread_prefix = "outbox-index-contender"
    monkeypatch.setattr(
        postgres_module,
        "_id",
        lambda: blocked_id if current_thread().name.startswith(thread_prefix) else original_id(),
    )
    waiting_pid: Queue[int] = Queue()

    def contender() -> DeliveryIntent:
        with store.transaction(actor) as tx:
            tx.conn.execute("SET LOCAL statement_timeout = '20s'")
            waiting_pid.put(tx.conn.info.backend_pid)
            return _enqueue(tx, reply=reply, source_id=source.id, text="contending body")

    with store.connect() as blocker:
        blocker.execute(
            "INSERT INTO ga_deliveries (id, group_id, created_by, policy_revision, payload_ref, "
            "payload_hash, idempotency_key) VALUES (%s,%s,%s,%s,%s,%s,%s)",
            (
                blocked_id,
                actor.group_id,
                actor.member_id,
                actor.policy_revision,
                "text:temporary placeholder",
                "a" * 64,
                "placeholder:" + blocked_id,
            ),
        )
        with ThreadPoolExecutor(max_workers=1, thread_name_prefix=thread_prefix) as executor:
            future = executor.submit(contender)
            try:
                _wait_until_blocked(store, waiting_pid.get(timeout=10), blocker.info.backend_pid)
                # The old targeted ON CONFLICT already passed its idempotency
                # precheck and now waits in the primary index. Commit its rival
                # before releasing that wait: ga_one_reply_per_request then
                # exposes the previously unhandled unique-index conflict.
                with store.transaction(actor) as tx:
                    winner = _enqueue(
                        tx,
                        reply=reply,
                        source_id=source.id,
                        text="contending body" if same_body else "winning different body",
                    )
            finally:
                blocker.rollback()
            if same_body:
                assert future.result(timeout=10).id == winner.id
            else:
                with pytest.raises(DomainError) as caught:
                    future.result(timeout=10)
                assert caught.value.code == "idempotency_conflict"
    with store.transaction(actor) as tx:
        deliveries = tx.list_delivery()
        assert len(deliveries) == 1
        assert deliveries[0].id == winner.id
        assert deliveries[0].payload_ref == winner.payload_ref


@pytest.mark.parametrize(
    "reply", [pytest.param(True, id="reply"), pytest.param(False, id="generic")]
)
def test_unrelated_id_collision_never_returns_an_unrelated_intent(
    store: PostgresStore, actor: ActorContext, monkeypatch: pytest.MonkeyPatch, reply: bool
) -> None:
    source = _source(store, actor)
    with store.transaction(actor) as tx:
        original = tx.enqueue_delivery(None, "text:original", "a" * 64, "original-purpose")
    monkeypatch.setattr(postgres_module, "_id", lambda: original.id)
    with pytest.raises(DomainError) as caught:
        with store.transaction(actor) as tx:
            _enqueue(tx, reply=reply, source_id=source.id, text="new purpose")
    assert caught.value.code == "idempotency_conflict"
    with store.transaction(actor) as tx:
        assert [item.id for item in tx.list_delivery()] == [original.id]


@pytest.mark.parametrize(
    "reply", [pytest.param(True, id="reply"), pytest.param(False, id="generic")]
)
def test_repeated_synchronized_outbox_contention(
    store: PostgresStore, actor: ActorContext, monkeypatch: pytest.MonkeyPatch, reply: bool
) -> None:
    if reply:
        actor = actor.model_copy(update={"role": Role.OBSERVER})
    original_insert = UnitOfWork._insert_delivery
    barrier = Barrier(4)

    def synchronized_insert(self, *args, **kwargs):
        barrier.wait(timeout=10)
        return original_insert(self, *args, **kwargs)

    monkeypatch.setattr(UnitOfWork, "_insert_delivery", synchronized_insert)
    with ThreadPoolExecutor(max_workers=4) as executor:
        for _round in range(16):
            source = _source(store, actor)

            def contender(_index: int, source_id: str = source.id) -> str:
                with store.transaction(actor) as tx:
                    return _enqueue(
                        tx, reply=reply, source_id=source_id, text="same logical delivery"
                    ).id

            identifiers = list(executor.map(contender, range(4)))
            assert len(set(identifiers)) == 1
    with store.transaction(actor) as tx:
        assert len(tx.list_delivery()) == 16
    with store.connect() as conn:
        row = conn.execute(
            "SELECT count(*) AS n FROM ga_audit_events "
            "WHERE group_id = %s AND action = 'delivery_enqueued'",
            (actor.group_id,),
        ).fetchone()
        assert row is not None and row["n"] == 16
