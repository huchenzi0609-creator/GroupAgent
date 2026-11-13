"""Real PostgreSQL integration tests with synthetic, uniquely scoped groups.

Set GROUPAGENT_TEST_DSN to an isolated PostgreSQL 16+ database. No schema or
shared tables are dropped. Direct SQL expiry is deliberate crash injection,
not the application's clock or a mock storage implementation.
"""

from __future__ import annotations

import hashlib
import os
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from pathlib import Path
from threading import Barrier, Event
from uuid import uuid4

import psycopg
import pytest
from psycopg import sql
from psycopg.conninfo import make_conninfo

from groupagent.domain import (
    ActorContext,
    DeliveryState,
    DomainError,
    EventKind,
    ExecutionLease,
    GroupEvent,
    Role,
    TeamTaskState,
    WorkOrder,
    WorkState,
)
from groupagent.persistence import PostgresStore
from groupagent.persistence import postgres as postgres_module

pytestmark = pytest.mark.postgres
HASH = hashlib.sha256(b"synthetic validated payload").hexdigest()


@pytest.fixture(scope="module")
def store() -> PostgresStore:
    dsn = os.environ.get("GROUPAGENT_TEST_DSN")
    if not dsn:
        pytest.skip("Set GROUPAGENT_TEST_DSN for real PostgreSQL integration tests")
    result = PostgresStore(dsn)
    assert result.migrate() == 3
    return result


@pytest.fixture
def actor(store: PostgresStore) -> ActorContext:
    group_id = f"test-{uuid4()}"
    store.register_group(group_id)
    return ActorContext(group_id=group_id, member_id="member-a", role=Role.MEMBER)


def event(actor: ActorContext, *, suffix: str = "one") -> GroupEvent:
    now = datetime.now(UTC)
    return GroupEvent(
        id=f"event-{actor.group_id}-{suffix}",
        source_key=f"account:{actor.group_id}:{suffix}",
        group_id=actor.group_id,
        actor_id=actor.member_id,
        kind=EventKind.TEXT,
        text="Please prepare a synthetic report",
        occurred_at=now,
        received_at=now,
    )


def create(store: PostgresStore, actor: ActorContext) -> WorkOrder:
    source = event(actor)
    with store.transaction(actor) as tx:
        tx.ingest_event(source, "stream", "cursor-1", 0)
        return tx.create_work("Prepare report", source.id)


def preparing_delivery(
    store: PostgresStore, actor: ActorContext
) -> tuple[WorkOrder, ExecutionLease]:
    work = create(store, actor)
    with store.transaction(actor) as tx:
        lease = tx.claim_work(work.id, "worker")
        running = tx.get_work(work.id)
        validating = tx.transition_work(
            work.id, WorkState.VALIDATING, expected_revision=running.revision, lease=lease
        )
        tx.record_validation(work.id, lease, "validation:synthetic-report")
        delivering = tx.transition_work(
            work.id, WorkState.DELIVERING, expected_revision=validating.revision, lease=lease
        )
    return delivering, lease


def test_migration_is_idempotent(store: PostgresStore) -> None:
    assert store.migrate() == 3
    assert store.migrate() == 3


def test_migration_checksum_and_failure_rollback_in_disposable_schema(
    store: PostgresStore, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    schema = f"migration_test_{uuid4().hex}"
    with store.connect() as conn:
        conn.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
    scoped = PostgresStore(make_conninfo(store.dsn, options=f"-c search_path={schema}"))
    try:
        assert scoped.migrate() == 3
        with scoped.connect() as conn:
            conn.execute("UPDATE ga_schema_migrations SET sha256 = 'changed'")
        with pytest.raises(DomainError) as caught:
            scoped.migrate()
        assert caught.value.code == "migration_changed"
    finally:
        with store.connect() as conn:
            conn.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))

    failure_schema = f"migration_failure_{uuid4().hex}"
    with store.connect() as conn:
        conn.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(failure_schema)))
    failing = PostgresStore(make_conninfo(store.dsn, options=f"-c search_path={failure_schema}"))
    (tmp_path / "migrations").mkdir()
    (tmp_path / "migrations" / "001_failure.sql").write_text(
        "CREATE TABLE partial_change (id integer); SELECT 1 / 0;", encoding="utf-8"
    )
    monkeypatch.setattr(postgres_module, "files", lambda _: tmp_path)
    try:
        with pytest.raises(psycopg.errors.DivisionByZero):
            failing.migrate()
        with failing.connect() as conn:
            row = conn.execute(
                "SELECT count(*) AS n FROM information_schema.tables WHERE table_schema = %s",
                (failure_schema,),
            ).fetchone()
            assert row is not None and row["n"] == 0
    finally:
        with store.connect() as conn:
            conn.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(failure_schema)))


def test_event_work_outbox_cursor_rollback_together(
    store: PostgresStore, actor: ActorContext
) -> None:
    source = event(actor)
    with pytest.raises(RuntimeError, match="simulated crash"):
        with store.transaction(actor) as tx:
            tx.ingest_event(source, "stream", "cursor-1", 0)
            tx.create_work("Prepare report", source.id)
            tx.record_decision(source.id, {"kind": "start_work"})
            tx.enqueue_delivery(None, "text:accepted", HASH, "ack:one")
            raise RuntimeError("simulated crash before commit")
    with store.transaction(actor) as tx:
        assert tx.get_cursor("stream") is None
        assert tx.list_work() == []
        assert tx.list_delivery() == []
        assert tx.list_pending_events() == []
    with store.connect() as conn:
        row = conn.execute(
            "SELECT count(*) AS n FROM ga_audit_events WHERE group_id = %s", (actor.group_id,)
        ).fetchone()
        assert row is not None and row["n"] == 0


def test_duplicate_old_event_never_moves_cursor(store: PostgresStore, actor: ActorContext) -> None:
    first, second = event(actor), event(actor, suffix="two")
    with store.transaction(actor) as tx:
        tx.ingest_event(first, "stream", "cursor-1", 0)
        tx.ingest_event(second, "stream", "cursor-2", 1)
    replay = first.model_copy(update={"id": str(uuid4()), "received_at": datetime.now(UTC)})
    with store.transaction(actor) as tx:
        for replay_cursor in ("cursor-0", "cursor-99"):
            result = tx.ingest_event(replay, "stream", replay_cursor, 0)
            assert not result.inserted
            assert result.event.id == first.id
            assert result.cursor_revision == 2
            current = tx.get_cursor("stream")
            assert current is not None and current.cursor == "cursor-2"
            assert current.revision == 2
    with pytest.raises(DomainError) as caught:
        with store.transaction(actor) as tx:
            tx.ingest_event(first.model_copy(update={"text": "different"}), "stream", "cursor-3", 2)
    assert caught.value.code == "idempotency_conflict"


def test_cursor_conflict_rolls_back_new_event(store: PostgresStore, actor: ActorContext) -> None:
    first, second = event(actor), event(actor, suffix="two")
    with store.transaction(actor) as tx:
        tx.ingest_event(first, "stream", "cursor-1", 0)
    with pytest.raises(DomainError) as caught:
        with store.transaction(actor) as tx:
            tx.ingest_event(second, "stream", "cursor-2", 0)
    assert caught.value.code == "version_conflict"
    with store.transaction(actor) as tx:
        assert [item.id for item in tx.list_pending_events()] == [first.id]


def test_concurrent_replay_creates_exactly_one_work(
    store: PostgresStore, actor: ActorContext
) -> None:
    source = event(actor)
    barrier = Barrier(2)

    def replay() -> tuple[bool, str]:
        barrier.wait()
        with store.transaction(actor) as tx:
            result = tx.ingest_event(source, "stream", "cursor-1", 0)
            work = tx.create_work("Prepare report", result.event.id)
            return result.inserted, work.id

    with ThreadPoolExecutor(max_workers=2) as executor:
        outcomes = list(executor.map(lambda _: replay(), range(2)))
    assert sum(inserted for inserted, _ in outcomes) == 1
    assert len({work_id for _, work_id in outcomes}) == 1
    with store.transaction(actor) as tx:
        assert len(tx.list_work()) == 1


def test_decision_recovery_and_concurrent_candidates_are_atomic(
    store: PostgresStore, actor: ActorContext
) -> None:
    source = event(actor)
    with store.transaction(actor) as tx:
        tx.ingest_event(source, "stream", "cursor-1", 0)
    # Crash after ingestion, before model response: event remains recoverable.
    with store.transaction(actor) as tx:
        assert [item.id for item in tx.list_pending_events()] == [source.id]
    barrier = Barrier(2)

    def decide(kind: str) -> str:
        barrier.wait()
        with store.transaction(actor) as tx:
            existing = tx.get_decision(source.id)
            if existing:
                return str(existing["kind"])
            if kind == "start_work":
                tx.create_work("Prepare report", source.id)
            else:
                tx.enqueue_delivery(None, "text:answer", HASH, f"reply:{source.id}")
            decision = tx.record_decision(source.id, {"kind": kind})
            return str(decision["kind"])

    with ThreadPoolExecutor(max_workers=2) as executor:
        outcomes = list(executor.map(decide, ["start_work", "reply"]))
    assert len(set(outcomes)) == 1
    with store.transaction(actor) as tx:
        assert tx.list_pending_events() == []
        assert len(tx.list_work()) + len(tx.list_delivery()) == 1
        assert tx.record_decision(source.id, {"kind": "ignore"})["kind"] == outcomes[0]


def test_only_one_concurrent_goal_revision_wins(store: PostgresStore, actor: ActorContext) -> None:
    work = create(store, actor)
    barrier = Barrier(2)

    def revise(goal: str) -> str:
        barrier.wait()
        try:
            with store.transaction(actor) as tx:
                tx.revise_work(work.id, goal, expected_revision=1)
            return "updated"
        except DomainError as error:
            return error.code

    with ThreadPoolExecutor(max_workers=2) as executor:
        outcomes = list(executor.map(revise, ["One target", "Another target"]))
    assert sorted(outcomes) == ["updated", "version_conflict"]
    with store.transaction(actor) as tx:
        current = tx.get_work(work.id)
        assert current.revision == 2 and current.goal_revision == 2


def test_work_context_can_prioritize_active_and_recent_jobs(
    store: PostgresStore, actor: ActorContext
) -> None:
    first = create(store, actor)
    second_event = event(actor, suffix="second-job")
    with store.transaction(actor) as tx:
        tx.ingest_event(second_event, "stream", "cursor-2", 1)
        second = tx.create_work("Second job", second_event.id)
        tx.transition_work(second.id, WorkState.FAILED, expected_revision=1, reason="configuration")
        assert [work.id for work in tx.list_work()] == [first.id, second.id]
        assert [work.id for work in tx.list_work(newest_first=True)] == [second.id, first.id]
        assert [work.id for work in tx.list_work(active_first=True, newest_first=True)] == [
            first.id,
            second.id,
        ]


def test_lease_claim_is_exclusive_and_old_fence_is_rejected(
    store: PostgresStore, actor: ActorContext
) -> None:
    work = create(store, actor)
    barrier = Barrier(2)

    def claim(worker_id: str) -> ExecutionLease | str:
        barrier.wait()
        try:
            with store.transaction(actor) as tx:
                return tx.claim_work(work.id, worker_id)
        except DomainError as error:
            return error.code

    with ThreadPoolExecutor(max_workers=2) as executor:
        outcomes = list(executor.map(claim, ["worker-a", "worker-b"]))
    leases = [outcome for outcome in outcomes if isinstance(outcome, ExecutionLease)]
    assert len(leases) == 1 and "lease_busy" in outcomes
    old = leases[0]
    with store.connect() as conn:
        conn.execute(
            "UPDATE ga_work_orders SET lease_expires_at = clock_timestamp() - interval '1 second' "
            "WHERE group_id = %s AND id = %s",
            (actor.group_id, work.id),
        )
    with pytest.raises(DomainError) as caught:
        with store.transaction(actor) as tx:
            tx.claim_work(work.id, "worker-restart")
    assert caught.value.code == "reconciliation_required"
    with store.transaction(actor) as tx:
        current = tx.claim_work(work.id, "worker-restart", recovery_evidence_ref="effects:checked")
        assert current.fencing_token > old.fencing_token
        assert current.attempt_no == old.attempt_no + 1
        tx.assert_lease(current)
    with pytest.raises(DomainError) as caught:
        with store.transaction(actor) as tx:
            tx.heartbeat_lease(old)
    assert caught.value.code == "stale_lease"
    with pytest.raises(DomainError) as caught:
        with store.transaction(actor) as tx:
            latest = tx.get_work(work.id)
            tx.transition_work(
                work.id, WorkState.VALIDATING, expected_revision=latest.revision, lease=old
            )
    assert caught.value.code == "stale_lease"


def test_lease_time_is_checked_after_a_contended_row_lock(
    store: PostgresStore, actor: ActorContext
) -> None:
    work = create(store, actor)
    with store.transaction(actor) as tx:
        lease = tx.claim_work(work.id, "worker", ttl_seconds=1)
    started = Event()

    def stale_submit() -> str:
        started.set()
        try:
            with store.transaction(actor) as tx:
                tx.assert_lease(lease)
            return "incorrectly_accepted"
        except DomainError as error:
            return error.code

    with ThreadPoolExecutor(max_workers=1) as executor:
        with store.connect() as blocker:
            blocker.execute(
                "SELECT id FROM ga_work_orders WHERE group_id = %s AND id = %s FOR UPDATE",
                (actor.group_id, work.id),
            )
            future = executor.submit(stale_submit)
            assert started.wait(2)
            blocker.execute("SELECT pg_sleep(1.1)")
        assert future.result(timeout=5) == "stale_lease"


def test_revise_and_cancel_revoke_execution_authority(
    store: PostgresStore, actor: ActorContext
) -> None:
    work = create(store, actor)
    with store.transaction(actor) as tx:
        lease = tx.claim_work(work.id, "worker")
        running = tx.get_work(work.id)
        revised = tx.revise_work(work.id, "Changed goal", expected_revision=running.revision)
        assert revised.goal_revision == 2 and revised.state == WorkState.QUEUED
    with pytest.raises(DomainError) as caught:
        with store.transaction(actor) as tx:
            tx.assert_lease(lease)
    assert caught.value.code == "stale_lease"
    with store.transaction(actor) as tx:
        lease2 = tx.claim_work(work.id, "worker-new")
        running = tx.get_work(work.id)
        cancelled = tx.transition_work(
            work.id, WorkState.CANCELLED, expected_revision=running.revision
        )
        assert cancelled.state == WorkState.CANCELLED
    with pytest.raises(DomainError) as caught:
        with store.transaction(actor) as tx:
            tx.assert_lease(lease2)
    assert caught.value.code == "stale_lease"


def test_validation_and_accepted_receipt_gate_completion(
    store: PostgresStore, actor: ActorContext
) -> None:
    work = create(store, actor)
    with store.transaction(actor) as tx:
        lease = tx.claim_work(work.id, "worker")
        running = tx.get_work(work.id)
        validating = tx.transition_work(
            work.id, WorkState.VALIDATING, expected_revision=running.revision, lease=lease
        )
    with pytest.raises(DomainError) as caught:
        with store.transaction(actor) as tx:
            tx.transition_work(
                work.id, WorkState.DELIVERING, expected_revision=validating.revision, lease=lease
            )
    assert caught.value.code == "validation_failed"
    with store.transaction(actor) as tx:
        tx.record_validation(work.id, lease, "validation:result")
        delivering = tx.transition_work(
            work.id, WorkState.DELIVERING, expected_revision=validating.revision, lease=lease
        )
        delivery = tx.enqueue_delivery(work.id, "artifact:report", HASH, "report:v1", lease=lease)
    with pytest.raises(DomainError) as caught:
        with store.transaction(actor) as tx:
            tx.transition_work(
                work.id, WorkState.COMPLETED, expected_revision=delivering.revision, lease=lease
            )
    assert caught.value.code == "delivery_incomplete"
    with store.transaction(actor) as tx:
        claimed = tx.claim_delivery("dispatcher")
        assert claimed is not None and claimed.id == delivery.id
    with store.transaction(actor) as tx:
        accepted = tx.finish_delivery(
            delivery.id,
            claimed.claim_token,
            DeliveryState.ACCEPTED,
            platform_message_id="synthetic-message-id",
        )
        assert accepted.accepted_at is not None
        assert (
            tx.finish_delivery(
                delivery.id,
                claimed.claim_token,
                DeliveryState.ACCEPTED,
                platform_message_id="synthetic-message-id",
            ).revision
            == accepted.revision
        )
        completed = tx.transition_work(
            work.id, WorkState.COMPLETED, expected_revision=delivering.revision, lease=lease
        )
        assert completed.state == WorkState.COMPLETED


def test_concurrent_dispatchers_claim_only_once(store: PostgresStore, actor: ActorContext) -> None:
    with store.transaction(actor) as tx:
        intent = tx.enqueue_delivery(None, "text:answer", HASH, "reply:v1")
    barrier = Barrier(2)

    def dispatch(worker: str) -> str | None:
        barrier.wait()
        with store.transaction(actor) as tx:
            claimed = tx.claim_delivery(worker)
            return claimed.id if claimed else None

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(dispatch, ["dispatcher-a", "dispatcher-b"]))
    assert results.count(intent.id) == 1 and results.count(None) == 1


def test_dispatcher_crash_becomes_unknown_without_resend(
    store: PostgresStore, actor: ActorContext
) -> None:
    with store.transaction(actor) as tx:
        intent = tx.enqueue_delivery(None, "text:answer", HASH, "reply:v1")
        claimed = tx.claim_delivery("dispatcher")
        assert claimed is not None
    with store.connect() as conn:
        conn.execute(
            "UPDATE ga_deliveries SET claim_expires_at = clock_timestamp() - interval '1 second' "
            "WHERE group_id = %s AND id = %s",
            (actor.group_id, intent.id),
        )
    with store.transaction(actor) as tx:
        recovered = tx.recover_expired_deliveries()
        assert len(recovered) == 1 and recovered[0].state == DeliveryState.UNKNOWN
        assert tx.claim_delivery("dispatcher-restart") is None
        duplicate = tx.enqueue_delivery(None, "text:answer", HASH, "reply:v1")
        assert duplicate.id == intent.id and duplicate.state == DeliveryState.UNKNOWN
    with pytest.raises(DomainError) as caught:
        with store.transaction(actor) as tx:
            tx.finish_delivery(
                intent.id,
                claimed.claim_token,
                DeliveryState.ACCEPTED,
                platform_message_id="late-response",
            )
    assert caught.value.code == "invalid_transition"
    with store.transaction(actor) as tx:
        accepted = tx.reconcile_delivery(
            intent.id,
            DeliveryState.ACCEPTED,
            evidence_ref="lookup:original-key",
            platform_message_id="proved-platform-message",
        )
        assert accepted.idempotency_key == "reply:v1"
        assert tx.claim_delivery("dispatcher-restart") is None


def test_unknown_delivery_pauses_work_and_blocks_resume(
    store: PostgresStore, actor: ActorContext
) -> None:
    work, lease = preparing_delivery(store, actor)
    with store.transaction(actor) as tx:
        intent = tx.enqueue_delivery(work.id, "artifact:report", HASH, "report:v1", lease=lease)
        claimed = tx.claim_delivery("dispatcher")
        assert claimed is not None
        tx.finish_delivery(intent.id, claimed.claim_token, DeliveryState.UNKNOWN, reason="timeout")
        paused = tx.get_work(work.id)
        assert paused.state == WorkState.PAUSED
        assert paused.pause_reason == "delivery_uncertain"
    with pytest.raises(DomainError) as caught:
        with store.transaction(actor) as tx:
            tx.transition_work(work.id, WorkState.QUEUED, expected_revision=paused.revision)
    assert caught.value.code == "delivery_unknown"
    with pytest.raises(DomainError) as caught:
        with store.transaction(actor) as tx:
            tx.enqueue_delivery(work.id, "artifact:report", HASH, "new-key", lease=lease)
    assert caught.value.code == "stale_lease"


def test_delivery_key_and_receipt_cannot_change_payload(
    store: PostgresStore, actor: ActorContext
) -> None:
    with store.transaction(actor) as tx:
        intent = tx.enqueue_delivery(None, "text:answer", HASH, "reply:v1")
    with pytest.raises(DomainError) as caught:
        with store.transaction(actor) as tx:
            tx.enqueue_delivery(
                None, "text:other", hashlib.sha256(b"other").hexdigest(), "reply:v1"
            )
    assert caught.value.code == "idempotency_conflict"
    with store.transaction(actor) as tx:
        claimed = tx.claim_delivery("dispatcher")
        assert claimed is not None
        tx.finish_delivery(
            intent.id, claimed.claim_token, DeliveryState.ACCEPTED, platform_message_id="original"
        )
    with pytest.raises(DomainError) as caught:
        with store.transaction(actor) as tx:
            tx.finish_delivery(
                intent.id, claimed.claim_token, DeliveryState.ACCEPTED, platform_message_id="other"
            )
    assert caught.value.code == "idempotency_conflict"


def test_bot_echo_requires_same_group_accepted_platform_receipt(
    store: PostgresStore, actor: ActorContext
) -> None:
    with store.transaction(actor) as tx:
        intent = tx.enqueue_delivery(None, "text:[bot] same text", HASH, "reply:v1")
        assert not tx.is_bot_message("human-same-account")
        claim = tx.claim_delivery("dispatcher")
        assert claim is not None
        tx.finish_delivery(
            intent.id,
            claim.claim_token,
            DeliveryState.ACCEPTED,
            platform_message_id="confirmed-bot-message",
        )
        assert tx.is_bot_message("confirmed-bot-message")
        assert not tx.is_bot_message("human-same-account")
    other = actor.model_copy(update={"group_id": f"other-{uuid4()}"})
    store.register_group(other.group_id)
    with store.transaction(other) as tx:
        assert not tx.is_bot_message("confirmed-bot-message")


def test_group_member_and_role_isolation(store: PostgresStore, actor: ActorContext) -> None:
    work = create(store, actor)
    other_member = actor.model_copy(update={"member_id": "member-b"})
    outsider = actor.model_copy(update={"group_id": f"other-{uuid4()}"})
    store.register_group(outsider.group_id)
    with pytest.raises(DomainError) as caught:
        with store.transaction(outsider) as tx:
            tx.get_work(work.id)
    assert caught.value.code == "not_found"
    with pytest.raises(DomainError) as caught:
        with store.transaction(other_member) as tx:
            tx.revise_work(work.id, "Overwrite somebody else", expected_revision=work.revision)
    assert caught.value.code == "permission_denied"
    for role in (Role.OBSERVER, Role.MAINTAINER):
        with pytest.raises(DomainError) as caught:
            with store.transaction(actor.model_copy(update={"role": role})) as tx:
                tx.create_work("Not authorized", work.request_event_id)
        assert caught.value.code == "permission_denied"
    with store.transaction(outsider) as tx:
        assert tx.list_work() == [] and tx.list_delivery() == []


def test_policy_revision_revokes_stale_context_and_work(
    store: PostgresStore, actor: ActorContext
) -> None:
    work = create(store, actor)
    store.register_group(actor.group_id, 2)
    with pytest.raises(DomainError) as caught:
        with store.transaction(actor):
            pytest.fail("Stale context entered a transaction")
    assert caught.value.code == "policy_changed"
    updated_actor = actor.model_copy(update={"policy_revision": 2})
    with pytest.raises(DomainError) as caught:
        with store.transaction(updated_actor) as tx:
            tx.claim_work(work.id, "worker")
    assert caught.value.code == "policy_changed"
    with store.transaction(updated_actor) as tx:
        tx.revise_work(work.id, "Reauthorized goal", expected_revision=work.revision)
        tx.claim_work(work.id, "worker")


def test_team_task_uses_member_ids_and_preserves_assignment_evidence(
    store: PostgresStore, actor: ActorContext
) -> None:
    with store.transaction(actor) as tx:
        task = tx.create_team_task("Prepare slides", due_at=datetime.now(UTC) + timedelta(days=1))
        assert task.owner_id == actor.member_id and task.assignment_kind == "self_claim"
    other = actor.model_copy(update={"member_id": "member-b"})
    with pytest.raises(DomainError) as caught:
        with store.transaction(other) as tx:
            tx.update_team_task(task.id, expected_revision=1, status=TeamTaskState.COMPLETED)
    assert caught.value.code == "permission_denied"
    with pytest.raises(DomainError) as caught:
        with store.transaction(actor) as tx:
            tx.create_team_task("Assign someone else", owner_id=other.member_id)
    assert caught.value.code == "permission_denied"
    owner = actor.model_copy(update={"member_id": "owner", "role": Role.OWNER})
    with store.transaction(owner) as tx:
        assigned = tx.update_team_task(task.id, expected_revision=1, owner_id=other.member_id)
        assert assigned.assignment_kind == "owner_assigned" and assigned.revision == 2
    with store.transaction(other) as tx:
        completed = tx.update_team_task(
            task.id,
            expected_revision=2,
            status=TeamTaskState.COMPLETED,
            evidence_refs=["artifact:slides"],
            progress="Self-reported complete",
        )
        assert completed.status == TeamTaskState.COMPLETED
    with store.transaction(actor) as tx:
        assert tx.list_work() == []  # A human task is not an execution work order.
