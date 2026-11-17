"""Human request provenance supports deletion without becoming live input authority."""

from datetime import UTC, datetime, timedelta

import pytest

from groupagent.domain import EventKind, WorkState

pytestmark = pytest.mark.postgres


def independent_human_revision(store, actor, emit):
    request = emit(actor, "Prepare the authorized report")
    change = emit(actor, "private revised subject", at=datetime.now(UTC) - timedelta(days=2))
    with store.transaction(actor) as tx:
        work = tx.create_work(
            request_event_id=request.id,
            goal="Report about the private revised subject",
            acceptance_criteria=["Include the private revised subject"],
            scope={
                "source_fingerprints": {request.id: tx.event_fingerprint(request.id)},
                "human_request_event_ids": [request.id, change.id],
                "human_request_history_complete": True,
                "human_request_fingerprints": {
                    request.id: tx.event_fingerprint(request.id),
                    change.id: tx.event_fingerprint(change.id),
                },
            },
        )
    return work, change


@pytest.mark.parametrize("operation", ["forget", "recall"])
def test_explicit_erasure_follows_human_revision_outside_input_sources(
    store, memory, actor, maintenance, emit, operation
):
    work, change = independent_human_revision(store, actor, emit)
    with store.transaction(actor) as tx:
        assert change.id not in tx.get_work(work.id).scope["source_fingerprints"]
    if operation == "forget":
        result = memory.forget(actor, event_ids=[change.id])
    else:
        recall = emit(actor, kind=EventKind.RECALL, reply_to=change.id)
        result = memory.recall(maintenance, recall.id)
    assert change.id in result.event_ids
    with store.transaction(actor) as tx:
        current = tx.get_work(work.id)
        assert current.state == WorkState.CANCELLED
        assert current.scope["privacy_redacted"] is True
        assert "private revised subject" not in current.goal
        revisions = tx.conn.execute(
            "SELECT body FROM ga_work_revisions WHERE group_id=%s AND work_id=%s",
            (actor.group_id, work.id),
        ).fetchall()
        assert revisions and all(
            "private revised subject" not in str(row["body"]) for row in revisions
        )


def test_ordinary_ttl_of_human_revision_does_not_cancel_retained_work(
    store, memory, actor, maintenance, emit
):
    work, change = independent_human_revision(store, actor, emit)
    memory.expire(maintenance, before=datetime.now(UTC) - timedelta(days=1))
    with store.transaction(actor) as tx:
        current = tx.get_work(work.id)
        assert current.state == WorkState.QUEUED and current.goal == work.goal
        assert current.scope["human_request_event_ids"] == work.scope["human_request_event_ids"]
        assert change.id not in current.scope["source_fingerprints"]
        assert (
            tx.conn.execute(
                "SELECT memory_state FROM ga_events WHERE group_id=%s AND id=%s",
                (actor.group_id, change.id),
            ).fetchone()["memory_state"]
            == "expired"
        )
