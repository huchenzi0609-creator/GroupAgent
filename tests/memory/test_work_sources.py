from __future__ import annotations

from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest

from groupagent.domain import DomainError

pytestmark = pytest.mark.postgres


def test_goal_revision_atomically_saves_new_source_fingerprints(store, memory, actor, emit):
    first = emit(actor, "initial request")
    followup = emit(actor, "revised request")
    with store.transaction(actor) as tx:
        work = tx.create_work("initial", first.id)
        fingerprint = tx.event_fingerprint(followup.id)
        revised = tx.revise_work(
            work.id,
            "revised",
            expected_revision=1,
            source_fingerprints={followup.id: fingerprint},
            retained_fact_source_ids=[],
        )
        assert revised.scope["source_fingerprints"] == {followup.id: fingerprint}
        assert revised.scope["source_event_ids"] == [followup.id]
    with store.connect() as conn:
        history = conn.execute(
            "SELECT body FROM ga_work_revisions WHERE group_id=%s AND work_id=%s AND goal_revision=2",
            (actor.group_id, work.id),
        ).fetchone()["body"]
        assert history["scope"] == revised.scope


@pytest.mark.parametrize("failure", ["wrong_hash", "foreign_group", "forgotten"])
def test_goal_revision_rejects_unavailable_or_changed_source(store, memory, actor, emit, failure):
    request = emit(actor, "request")
    source = emit(actor, "source")
    target = source
    if failure == "foreign_group":
        outside = actor.model_copy(update={"group_id": "foreign-" + uuid4().hex})
        store.register_group(outside.group_id)
        target = emit(outside, "outside")
    if failure == "forgotten":
        memory.forget(actor, event_ids=[source.id])
    with store.transaction(actor) as tx:
        work = tx.create_work("original", request.id)
    with pytest.raises(DomainError):
        with store.transaction(actor) as tx:
            digest = (
                "a" * 64
                if failure in {"wrong_hash", "foreign_group"}
                else tx.event_fingerprint(source.id)
            )
            tx.revise_work(
                work.id,
                "must not persist",
                expected_revision=1,
                source_fingerprints={target.id: digest},
            )
    with store.transaction(actor) as tx:
        assert tx.get_work(work.id).goal_revision == 1


@pytest.mark.parametrize("saved_fact", [False, True])
def test_expired_source_requires_real_retained_fact_authority(
    store, memory, maintenance, actor, emit, saved_fact
):
    now = datetime.now(UTC)
    old = emit(actor, "explicit archived project statement", at=now - timedelta(days=40))
    if saved_fact:
        memory.record_fact(
            actor, "project_decision", "approved", source_event_ids=[old.id], explicit=True
        )
    memory.expire(maintenance, before=now - timedelta(days=30), now=now)
    request = emit(actor, "fresh authorized request")
    with store.transaction(actor) as tx:
        work = tx.create_work("initial", request.id)
        digest = tx.event_fingerprint(old.id)
    if saved_fact:
        with store.transaction(actor) as tx:
            revised = tx.revise_work(
                work.id,
                "use the explicit retained fact",
                expected_revision=1,
                source_fingerprints={old.id: digest},
                retained_fact_source_ids=[old.id],
            )
            assert revised.scope["retained_fact_source_ids"] == [old.id]
    else:
        with pytest.raises(DomainError) as caught:
            with store.transaction(actor) as tx:
                tx.revise_work(
                    work.id,
                    "pretend a retained fact exists",
                    expected_revision=1,
                    source_fingerprints={old.id: digest},
                    retained_fact_source_ids=[old.id],
                )
        assert caught.value.code == "source_unavailable"


def test_task_due_date_distinguishes_omitted_clear_and_aware_value(store, actor):
    initial = datetime(2030, 1, 1, tzinfo=UTC)
    revised = datetime(2030, 2, 1, tzinfo=UTC)
    with store.transaction(actor) as tx:
        task = tx.create_team_task("human responsibility", due_at=initial)
        progress = tx.update_team_task(task.id, expected_revision=1, progress="started")
        assert progress.due_at == initial
        changed = tx.update_team_task(task.id, expected_revision=2, due_at=revised)
        assert changed.due_at == revised
        cleared = tx.update_team_task(task.id, expected_revision=3, due_at=None)
        assert cleared.due_at is None
    with pytest.raises(DomainError) as caught:
        with store.transaction(actor) as tx:
            tx.update_team_task(task.id, expected_revision=4, due_at=datetime(2030, 3, 1))
    assert caught.value.code == "invalid_input"
