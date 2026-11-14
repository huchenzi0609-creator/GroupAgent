from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from groupagent.domain import DomainError
from groupagent.memory import MemoryService
from groupagent.ops import DeletionJournal

from .test_cleanup import source

pytestmark = pytest.mark.postgres


@pytest.mark.parametrize("in_tx", [False, True])
def test_external_journal_failure_rolls_back_privacy_and_bound_business_transaction(
    store, actor, member, in_tx
):
    event = source(store, member)

    class FailingSink:
        def append_tombstones(self, markers):
            assert markers and markers[0]["event_id"] == event.id
            assert "text" not in markers[0] and "synthetic delete marker" not in str(markers)
            raise OSError("synthetic journal failure")

    memory = MemoryService(store, deletion_sink=FailingSink())
    with pytest.raises(DomainError) as caught:
        if in_tx:
            with store.transaction(member) as tx:
                memory.prepare_in_tx(tx)
                tx.create_team_task("must also rollback", source_event_id=event.id)
                memory.in_tx(tx).forget(member, event_ids=[event.id])
        else:
            memory.forget(member, event_ids=[event.id])
    assert caught.value.code == "deletion_journal_failed"
    with store.transaction(member) as tx:
        assert tx.get_event(event.id).text == event.text
        assert (
            tx.conn.execute("SELECT count(*) AS n FROM ga_source_tombstones").fetchone()["n"] == 0
        )
        assert not tx.list_team_tasks()


def test_real_journal_is_durable_before_database_privacy_commit(store, member, tmp_path):
    event = source(store, member)
    journal = DeletionJournal(tmp_path / "external")
    memory = MemoryService(store, deletion_sink=journal)
    with store.transaction(member) as tx:
        memory.prepare_in_tx(tx)
        memory.in_tx(tx).forget(member, event_ids=[event.id])
        marker = DeletionJournal(journal.root).load(member.group_id)[0]
        assert marker.event_id == event.id and marker.reason == "forgotten"
        # The source transaction is deliberately still uncommitted here.
        with store.connect() as other:
            row = other.execute(
                "SELECT memory_state FROM ga_events WHERE group_id=%s AND id=%s",
                (member.group_id, event.id),
            ).fetchone()
            assert row["memory_state"] == "active"
    with store.transaction(member) as tx:
        assert tx.get_event(event.id).content_state == "forgotten"


def test_transaction_failure_after_fsync_keeps_conservative_tombstone(store, member, tmp_path):
    event = source(store, member)
    journal = DeletionJournal(tmp_path / "external")
    memory = MemoryService(store, deletion_sink=journal)
    with pytest.raises(RuntimeError):
        with store.transaction(member) as tx:
            memory.prepare_in_tx(tx)
            memory.in_tx(tx).forget(member, event_ids=[event.id])
            raise RuntimeError("synthetic commit-side failure")
    assert journal.load(member.group_id)[0].reason == "forgotten"
    with store.transaction(member) as tx:
        assert tx.get_event(event.id).content_state == "active"
    # Recovery is conservative: a persisted erasure marker is never undone by rollback.


def test_expiry_then_forget_upgrades_external_marker(store, actor, member, tmp_path):
    event = source(store, member)
    journal = DeletionJournal(tmp_path / "external")
    memory = MemoryService(store, deletion_sink=journal)
    with store.transaction(actor) as tx:
        tx.conn.execute(
            "UPDATE ga_events SET occurred_at=clock_timestamp()-interval '40 days' WHERE group_id=%s AND id=%s",
            (actor.group_id, event.id),
        )
    memory.expire(actor, before=datetime.now(UTC) - timedelta(days=30))
    assert journal.load(member.group_id)[0].reason == "expired"
    memory.forget(member, event_ids=[event.id])
    assert journal.load(member.group_id)[0].reason == "forgotten"
