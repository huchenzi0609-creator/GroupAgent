"""Business revisions invalidate their consumers; privacy remains group-wide."""

import asyncio
from uuid import uuid4

import pytest
from psycopg.types.json import Jsonb

from groupagent.domain import DomainError
from groupagent.memory import MemoryService, SummaryDraft

pytestmark = pytest.mark.postgres


class BlockingProvider:
    def __init__(self):
        self.started, self.release = asyncio.Event(), asyncio.Event()

    async def summarize(self, request):
        self.started.set()
        await self.release.wait()
        return SummaryDraft(
            text="synthetic raw discussion summary",
            covered_event_ids=[row["id"] for row in request.events],
            provider="fixture",
            model="fixture",
        )


def assert_valid(store, memory, actor, snapshot):
    with store.transaction(actor) as tx:
        memory.assert_snapshot_in_tx(tx, snapshot)


def task_list_snapshot(store, memory, actor, current, query):
    with store.transaction(actor) as tx:
        snapshot = memory.snapshot_in_tx(
            tx, "unrelated-current-message", source_event_ids=[current.id]
        )
        _, witness = memory.task_query_in_tx(tx, query, public_only=True)
        snapshot["task_queries"] = [witness]
        return snapshot


def test_actual_task_progress_does_not_invalidate_unrelated_chat_or_goal(
    store, memory, actor, emit
):
    original = emit(actor, "human task source")
    current = emit(actor, "unrelated current question")
    with store.transaction(actor) as tx:
        task = tx.create_team_task("separate duty", source_event_id=original.id)
    snapshot = memory.query_context(
        actor, "unrelated current question", source_event_ids=[current.id]
    )
    update = emit(actor, "progress on another duty")
    with store.transaction(actor) as tx:
        tx.update_team_task(
            task.id, expected_revision=1, progress="in progress", source_event_id=update.id
        )
    assert snapshot["source_task_revisions"] == {} and snapshot["task_queries"] == []
    assert_valid(store, memory, actor, snapshot)
    with store.transaction(actor) as tx:
        value = tx.create_work(
            "unrelated new goal",
            current.id,
            scope={
                "source_fingerprints": snapshot["source_fingerprints"],
                "source_task_revisions": snapshot["source_task_revisions"],
            },
        )
        assert value.scope["source_task_revisions"] == {}


def test_selected_task_revision_rejects_stale_snapshot_without_privacy_epoch_change(
    store, memory, actor, emit
):
    original = emit(actor, "human task source")
    with store.transaction(actor) as tx:
        task = tx.create_team_task("watched task", source_event_id=original.id)
    snapshot = memory.query_context(actor, "watched")
    update = emit(actor, "watched task progress")
    with store.transaction(actor) as tx:
        tx.update_team_task(
            task.id, expected_revision=1, progress="changed", source_event_id=update.id
        )
        epoch = tx.conn.execute(
            "SELECT epoch FROM ga_memory_epochs WHERE group_id=%s", (tx.group_id,)
        ).fetchone()["epoch"]
    assert epoch == snapshot["privacy_epoch"]
    with pytest.raises(DomainError) as caught:
        assert_valid(store, memory, actor, snapshot)
    assert caught.value.code == "memory_changed"


def test_claim_revision_does_not_invalidate_unrelated_snapshot(store, memory, actor, emit):
    identifier = uuid4().hex
    current = emit(actor, "unrelated current question")
    with store.transaction(actor) as tx:
        tx.conn.execute(
            "INSERT INTO ga_team_tasks(id,group_id,title,owner_id,assignment_kind) VALUES(%s,%s,'unassigned duty',NULL,'unassigned')",
            (identifier, tx.group_id),
        )
    unrelated = memory.query_context(
        actor, "unrelated current question", source_event_ids=[current.id]
    )
    selected = memory.query_context(actor, "unassigned")
    claim = emit(actor, "I claim the unassigned duty")
    with store.transaction(actor) as tx:
        tx.claim_team_task(identifier, expected_revision=1, source_event_id=claim.id)
    assert_valid(store, memory, actor, unrelated)
    with pytest.raises(DomainError) as caught:
        assert_valid(store, memory, actor, selected)
    assert caught.value.code == "memory_changed"


@pytest.mark.parametrize("change", ["new", "now_matching"])
def test_consumed_task_query_detects_new_membership(store, memory, actor, emit, change):
    current = emit(actor, "current list request")
    original = emit(actor, "other duty source")
    with store.transaction(actor) as tx:
        other = tx.create_team_task("other duty", source_event_id=original.id)
    snapshot = task_list_snapshot(store, memory, actor, current, "watched")
    update = emit(actor, "new relevant duty")
    with store.transaction(actor) as tx:
        if change == "new":
            tx.create_team_task("watched new duty", source_event_id=update.id)
        else:
            tx.update_team_task(
                other.id, expected_revision=1, progress="watched result", source_event_id=update.id
            )
    with pytest.raises(DomainError) as caught:
        assert_valid(store, memory, actor, snapshot)
    assert caught.value.code == "memory_changed"


def test_task_query_ignores_unmatched_and_private_task_progress(store, memory, actor, emit):
    current = emit(actor, "current list request")
    public_source = emit(actor, "unrelated duty")
    private_source = emit(actor, "private duty")
    private = memory.create_topic(actor, "private task context", visibility="participants")
    memory.link_event(actor, private_source.id, private.id, evidence_event_ids=[private_source.id])
    with store.transaction(actor) as tx:
        unrelated = tx.create_team_task("other duty", source_event_id=public_source.id)
        hidden = tx.create_team_task("watched private duty", source_event_id=private_source.id)
    snapshot = task_list_snapshot(store, memory, actor, current, "watched")
    update = emit(actor, "progress update")
    with store.transaction(actor) as tx:
        tx.update_team_task(
            unrelated.id, expected_revision=1, progress="changed", source_event_id=update.id
        )
        tx.update_team_task(
            hidden.id,
            expected_revision=1,
            progress="private change",
            source_event_id=private_source.id,
        )
    assert_valid(store, memory, actor, snapshot)


def test_privacy_transition_still_invalidates_unrelated_snapshot(store, memory, actor, emit):
    original = emit(actor, "source later made private")
    current = emit(actor, "unrelated current question")
    snapshot = memory.query_context(
        actor, "unrelated current question", source_event_ids=[current.id]
    )
    private = memory.create_topic(actor, "restricted context", visibility="participants")
    memory.link_event(actor, original.id, private.id, evidence_event_ids=[original.id])
    with pytest.raises(DomainError) as caught:
        assert_valid(store, memory, actor, snapshot)
    assert caught.value.code == "memory_changed"


async def test_raw_event_compaction_survives_concurrent_task_progress(store, actor, emit):
    original = emit(actor, "original discussion")
    with store.transaction(actor) as tx:
        task = tx.create_team_task("separate duty", source_event_id=original.id)
    provider = BlockingProvider()
    memory = MemoryService(store, provider)
    pending = asyncio.create_task(memory.compact(actor, force=True))
    await provider.started.wait()
    update = emit(actor, "new progress message after selected window")
    with store.transaction(actor) as tx:
        tx.update_team_task(
            task.id, expected_revision=1, progress="changed", source_event_id=update.id
        )
    provider.release.set()
    summary = await pending
    assert summary is not None and summary.source_event_ids == [original.id]
    assert update.id not in summary.source_event_ids


async def test_compaction_rechecks_selected_source_bytes_without_epoch_change(store, actor, emit):
    source = emit(actor, "selected source")
    provider = BlockingProvider()
    memory = MemoryService(store, provider)
    pending = asyncio.create_task(memory.compact(actor, force=True))
    await provider.started.wait()
    with store.transaction(actor) as tx:
        tx.conn.execute(
            "UPDATE ga_events SET body=%s WHERE group_id=%s AND id=%s",
            (
                Jsonb(
                    source.model_copy(update={"text": "changed selected source"}).model_dump(
                        mode="json"
                    )
                ),
                tx.group_id,
                source.id,
            ),
        )
    provider.release.set()
    with pytest.raises(DomainError) as caught:
        await pending
    assert caught.value.code == "memory_changed"
    assert memory.checkpoint(actor)["through_sequence"] == 0
