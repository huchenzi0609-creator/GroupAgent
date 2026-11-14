from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from threading import Event

import pytest

from groupagent.domain import DomainError

pytestmark = pytest.mark.postgres


def test_snapshot_sources_and_epoch_are_atomic_and_new_messages_do_not_invalidate(
    store, memory, actor, emit
):
    source = emit(actor, "current context")
    snapshot = memory.query_context(actor, source_event_ids=[source.id])
    assert snapshot["source_fingerprints"][source.id]
    emit(actor, "new discussion")
    with store.transaction(actor) as tx:
        memory.prepare_in_tx(tx)
        memory.assert_snapshot_in_tx(tx, snapshot)


@pytest.mark.parametrize("mutation", ["forget", "correct", "invalidate", "topic"])
def test_provider_snapshot_is_rejected_after_memory_changes(store, memory, actor, emit, mutation):
    source = emit(actor, "statement")
    fact = memory.record_fact(actor, "state", 1, source_event_ids=[source.id], explicit=True)
    snapshot = memory.query_context(actor, source_event_ids=[source.id])
    if mutation == "forget":
        memory.forget(actor, event_ids=[source.id])
    elif mutation == "correct":
        memory.correct_fact(
            actor, fact.id, 2, expected_revision=1, source_event_ids=[source.id], explicit=True
        )
    elif mutation == "invalidate":
        memory.invalidate_fact(actor, fact.id, expected_revision=1)
    else:
        topic = memory.create_topic(actor, "restricted", visibility="participants")
        memory.link_event(actor, source.id, topic.id, evidence_event_ids=[source.id])
    with pytest.raises(DomainError) as caught:
        with store.transaction(actor) as tx:
            memory.assert_snapshot_in_tx(tx, snapshot)
    assert caught.value.code == "memory_changed"


def test_name_correction_invalidates_old_snapshot(store, memory, members, owner, actor, emit):
    source = emit(actor, "self reference")
    snapshot = memory.query_context(actor, source_event_ids=[source.id])
    members.confirm_name(owner, actor.member_id, "新名字", expected_revision=1)
    with pytest.raises(DomainError) as caught:
        with store.transaction(actor) as tx:
            memory.assert_snapshot_in_tx(tx, snapshot)
    assert caught.value.code == "memory_changed"


def test_snapshot_is_bound_to_trusted_member_context(store, memory, actor, emit):
    source = emit(actor, "own context")
    snapshot = memory.query_context(actor, source_event_ids=[source.id])
    other = actor.model_copy(update={"member_id": "member-b"})
    with pytest.raises(DomainError) as caught:
        with store.transaction(other) as tx:
            memory.assert_snapshot_in_tx(tx, snapshot)
    assert caught.value.code == "scope_violation"


def test_snapshot_holds_privacy_lock_until_hits_and_fingerprints_are_read(
    store, memory, actor, emit
):
    source = emit(actor, "concurrent private source")
    started = Event()

    def erase():
        started.set()
        memory.forget(actor, event_ids=[source.id])

    with ThreadPoolExecutor(max_workers=1) as executor:
        with store.transaction(actor) as tx:
            memory.prepare_in_tx(tx)
            future = executor.submit(erase)
            assert started.wait(5)
            snapshot = memory.snapshot_in_tx(tx, source_event_ids=[source.id])
            assert source.id in snapshot["source_fingerprints"]
            assert any(item["id"] == source.id for item in snapshot["items"])
        future.result(timeout=10)
    with pytest.raises(DomainError):
        with store.transaction(actor) as tx:
            memory.assert_snapshot_in_tx(tx, snapshot)
