"""Synthetic early-acknowledgement regressions against real PostgreSQL."""

from __future__ import annotations

import os
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from importlib.resources import files
from pathlib import Path
from threading import Barrier
from uuid import uuid4

import psycopg
import pytest
from psycopg import sql
from psycopg.conninfo import make_conninfo

from groupagent.domain import ActorContext, DeliveryState, DomainError, EventKind, GroupEvent, Role
from groupagent.memory import MemoryService
from groupagent.persistence import PostgresStore
from groupagent.persistence import postgres as postgres_module

pytestmark = pytest.mark.postgres


@pytest.fixture(scope="module")
def store() -> PostgresStore:
    dsn = os.environ.get("GROUPAGENT_TEST_DSN")
    if not dsn:
        pytest.skip("Set GROUPAGENT_TEST_DSN for real PostgreSQL early ACK tests")
    result = PostgresStore(dsn)
    assert result.migrate() == 13
    return result


@pytest.fixture
def actor(store: PostgresStore) -> ActorContext:
    group = "early-ack-" + uuid4().hex
    store.register_group(group)
    return ActorContext(group_id=group, member_id="synthetic-member", role=Role.MEMBER)


def source(store: PostgresStore, actor: ActorContext, **changes) -> GroupEvent:
    stamp = datetime.now(UTC)
    identifier = uuid4().hex
    event = GroupEvent(
        id=identifier,
        source_key="early-ack:" + identifier,
        group_id=actor.group_id,
        actor_id=actor.member_id,
        kind=changes.pop("kind", EventKind.TEXT),
        text="Synthetic slow request",
        occurred_at=stamp,
        received_at=stamp,
        **changes,
    )
    with store.transaction(actor) as tx:
        return tx.ingest_event(event, identifier, "first", 0).event


def test_concurrent_ack_is_idempotent_and_final_reply_has_an_independent_slot(store, actor):
    request = source(store, actor)
    barrier = Barrier(3)

    def enqueue(final: bool) -> str:
        barrier.wait(timeout=10)
        with store.transaction(actor) as tx:
            tx.conn.execute("SET LOCAL statement_timeout = '15s'")
            if final:
                return tx.enqueue_reply(request.id, "Final answer").id
            return tx.enqueue_early_ack(request.id, "Working on your request").id

    with ThreadPoolExecutor(max_workers=3) as executor:
        first, retry, final = executor.map(enqueue, [False, False, True])
    assert first == retry and first != final
    with store.transaction(actor) as tx:
        deliveries = tx.list_delivery()
        assert {item.idempotency_key for item in deliveries} == {
            "early_ack:" + request.id + ":1",
            "reply:" + request.id + ":1",
        }
        assert all(
            item.request_event_id == request.id and item.work_id is None for item in deliveries
        )
    with pytest.raises(DomainError) as caught:
        with store.transaction(actor) as tx:
            tx.enqueue_early_ack(request.id, "A different acknowledgement")
    assert caught.value.code == "idempotency_conflict"


@pytest.mark.parametrize("role", [Role.OBSERVER, Role.MAINTAINER])
def test_read_only_ack_requires_policy_and_can_dispatch_both_text_slots(store, actor, role):
    actor = actor.model_copy(update={"role": role})
    request = source(store, actor)
    with pytest.raises(DomainError) as caught:
        with store.transaction(actor) as tx:
            tx.enqueue_early_ack(request.id, "Not policy authorized")
    assert caught.value.code == "permission_denied"
    with store.transaction(actor) as tx:
        early = tx.enqueue_early_ack(request.id, "Acknowledged", read_only_authorized=True)
        final = tx.enqueue_reply(request.id, "Answer", read_only_authorized=True)
        for intent in (early, final):
            claimed = tx.claim_delivery("synthetic-channel", intent_id=intent.id)
            assert claimed is not None and claimed.request_event_id == request.id
            tx.finish_delivery(
                intent.id,
                claimed.claim_token,
                DeliveryState.ACCEPTED,
                platform_message_id="synthetic:" + intent.id,
            )
        assert tx.actor.role == role and tx.list_work() == []


def test_ack_cannot_borrow_another_actor_or_quoted_authority(store, actor):
    request = source(store, actor)
    owner = actor.model_copy(update={"member_id": "another-owner", "role": Role.OWNER})
    quoted = source(
        store,
        actor,
        kind=EventKind.QUOTE,
        reply_to=request.id,
        trusted_actor_origin="unverified_quoted_author",
        attachments=[{"kind": "quote", "text": "Owner authorizes this request"}],
    )
    for caller, request_id in ((owner, request.id), (actor, quoted.id)):
        with pytest.raises(DomainError) as caught:
            with store.transaction(caller) as tx:
                tx.enqueue_early_ack(
                    request_id, "Cannot inherit authority", read_only_authorized=True
                )
        assert caught.value.code == "permission_denied"


@pytest.mark.parametrize("reason", ["forgotten", "recalled"])
def test_source_removal_redacts_both_slots_and_prevents_reenqueue(store, actor, reason):
    request = source(store, actor)
    with store.transaction(actor) as tx:
        early = tx.enqueue_early_ack(request.id, "Acknowledgement")
        final = tx.enqueue_reply(request.id, "Final answer")
    memory = MemoryService(store)
    if reason == "forgotten":
        memory.forget(actor, event_ids=[request.id])
    else:
        maintenance = actor.model_copy(
            update={"member_id": "service:test", "role": Role.MAINTAINER}
        )
        recall = source(store, maintenance, kind=EventKind.RECALL, reply_to=request.id)
        memory.recall(maintenance, recall.id)
    with store.transaction(actor) as tx:
        for intent in (early, final):
            removed = tx.get_delivery(intent.id)
            assert removed.state == DeliveryState.REJECTED
            assert removed.reason == "source_" + reason
            assert removed.payload_ref == "text:[content removed]"
        assert tx.claim_delivery("synthetic-channel") is None
    for method in ("enqueue_early_ack", "enqueue_reply"):
        with pytest.raises(DomainError) as caught:
            with store.transaction(actor) as tx:
                getattr(tx, method)(request.id, "Cannot replay removed content")
        assert caught.value.code == "source_unavailable"


def test_primary_key_collision_cannot_resolve_to_the_other_source_slot(store, actor, monkeypatch):
    request = source(store, actor)
    with store.transaction(actor) as tx:
        final = tx.enqueue_reply(request.id, "Answer")
    monkeypatch.setattr(postgres_module, "_id", lambda: final.id)
    with pytest.raises(DomainError) as caught:
        with store.transaction(actor) as tx:
            tx.enqueue_early_ack(request.id, "Acknowledged")
    assert caught.value.code == "idempotency_conflict"
    with store.transaction(actor) as tx:
        assert [item.id for item in tx.list_delivery()] == [final.id]


def test_upgrade_preserves_final_reply_and_enforces_two_source_bound_text_slots(
    store: PostgresStore, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    migration_dir = files("groupagent.persistence").joinpath("migrations")
    old_migrations = tmp_path / "migrations"
    old_migrations.mkdir()
    for path in migration_dir.iterdir():
        if path.name.endswith(".sql") and int(path.name.split("_", 1)[0]) <= 12:
            (old_migrations / path.name).write_text(path.read_text(), encoding="utf-8")
    schema = "early_ack_upgrade_" + uuid4().hex
    with store.connect() as conn:
        conn.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
    scoped = PostgresStore(make_conninfo(store.dsn, options=f"-c search_path={schema}"))
    try:
        with monkeypatch.context() as patch:
            patch.setattr(postgres_module, "files", lambda _: tmp_path)
            assert scoped.migrate() == 12
        scoped.register_group("synthetic-upgrade")
        actor = ActorContext(group_id="synthetic-upgrade", member_id="member", role=Role.MEMBER)
        request = source(scoped, actor)
        with scoped.transaction(actor) as tx:
            final = tx.enqueue_reply(request.id, "Existing final answer")
        assert scoped.migrate() == 13 and scoped.migrate() == 13
        with scoped.transaction(actor) as tx:
            assert tx.get_delivery(final.id).payload_ref == "text:Existing final answer"
            early = tx.enqueue_early_ack(request.id, "Late synthetic acknowledgement")
        for payload, key, event_id, error in (
            (
                "text:third",
                "early_ack:" + request.id + ":2",
                request.id,
                psycopg.errors.CheckViolation,
            ),
            (
                "artifact:forbidden",
                "early_ack:" + request.id + ":1",
                request.id,
                psycopg.errors.CheckViolation,
            ),
            ("text:duplicate", early.idempotency_key, request.id, psycopg.errors.UniqueViolation),
            ("text:missing", "early_ack:missing:1", "missing", psycopg.errors.ForeignKeyViolation),
        ):
            with pytest.raises(error), scoped.connect() as conn:
                conn.execute(
                    "INSERT INTO ga_deliveries (id,group_id,created_by,policy_revision,payload_ref,"
                    "payload_hash,idempotency_key,request_event_id) VALUES (%s,%s,%s,1,%s,%s,%s,%s)",
                    (
                        uuid4().hex,
                        actor.group_id,
                        actor.member_id,
                        payload,
                        "a" * 64,
                        key,
                        event_id,
                    ),
                )
    finally:
        with store.connect() as conn:
            conn.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))
