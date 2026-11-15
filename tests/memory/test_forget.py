from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest

from groupagent.domain import DeliveryState, DomainError, EventKind, GroupEvent, Role, WorkState

pytestmark = pytest.mark.postgres


async def test_forget_redacts_events_summaries_facts_decisions_and_pending_reply(
    store, memory, actor, emit
):
    secret = "synthetic-private-marker-" + uuid4().hex
    source = emit(
        actor,
        secret,
        attachments=[{"file_id": "synthetic-file", "version_id": "v1", "filename": secret}],
    )
    fact = memory.record_fact(
        actor, "private fact", secret, source_event_ids=[source.id], explicit=True
    )
    await memory.compact(actor, force=True)
    with store.transaction(actor) as tx:
        reply = tx.enqueue_reply(source.id, secret)
        tx.record_decision(source.id, {"kind": "reply", "result": {"response": secret}})
    result = memory.forget(actor, event_ids=[source.id])
    assert fact.id in result.fact_ids and result.invalidated_summaries >= 1
    assert memory.retrieve(actor, secret, include_tasks=False) == []
    with store.transaction(actor) as tx:
        canonical = tx.get_event(source.id)
        assert (
            canonical.content_state == "forgotten"
            and canonical.text == ""
            and canonical.attachments == []
        )
        assert secret not in json.dumps(tx.get_decision(source.id))
        delivery = tx.get_delivery(reply.id)
        assert delivery.state == DeliveryState.REJECTED and secret not in delivery.payload_ref
        assert tx.list_pending_events() == []
    with store.connect() as conn:
        for table, column in (
            ("ga_events", "body"),
            ("ga_memory_summaries", "text"),
            ("ga_fact_revisions", "value"),
            ("ga_audit_events", "details"),
            ("ga_memory_cleanup", "resource_refs"),
        ):
            rows = conn.execute(
                "SELECT " + column + " AS value FROM " + table + " WHERE group_id=%s",
                (actor.group_id,),
            ).fetchall()
            assert secret not in json.dumps(rows, default=str)


@pytest.mark.parametrize(
    "role,own,allowed",
    [
        (Role.MEMBER, True, True),
        (Role.OBSERVER, True, True),
        (Role.MAINTAINER, True, True),
        (Role.OWNER, False, True),
        (Role.MEMBER, False, False),
        (Role.OBSERVER, False, False),
        (Role.MAINTAINER, False, False),
    ],
)
def test_forget_authority_is_source_scoped(memory, actor, emit, role, own, allowed):
    source = emit(actor, "owned private source")
    editor = actor.model_copy(
        update={"role": role, "member_id": actor.member_id if own else "other"}
    )
    if allowed:
        memory.forget(editor, event_ids=[source.id])
        assert not memory.source_available(actor, source.id)
    else:
        with pytest.raises(DomainError) as caught:
            memory.forget(editor, event_ids=[source.id])
        assert caught.value.code == "permission_denied"
        assert memory.source_available(actor, source.id)


@pytest.mark.parametrize("operation", ["work", "reply", "fact"])
def test_replay_cannot_revive_forgotten_source_or_authorize_actions(
    store, memory, actor, emit, operation
):
    source = emit(actor, "deleted secret")
    memory.forget(actor, event_ids=[source.id])
    with store.transaction(actor) as tx:
        replay = tx.ingest_event(source, "replay-stream", "opaque-new", 0)
        assert (
            not replay.inserted
            and replay.event.content_state == "forgotten"
            and replay.event.text == ""
        )
    with pytest.raises(DomainError) as caught:
        if operation == "fact":
            memory.record_fact(
                actor,
                "revive",
                "deleted secret",
                source_event_ids=[source.id],
                explicit=True,
                request_key="new-key",
            )
        else:
            with store.transaction(actor) as tx:
                if operation == "work":
                    tx.create_work("revive", source.id)
                else:
                    tx.enqueue_reply(source.id, "revive")
    assert caught.value.code == "source_unavailable"


def test_same_platform_source_under_new_key_stays_tombstoned(store, memory, actor, emit):
    source = emit(
        actor,
        "private platform source",
        platform_message_id="platform-1",
        channel_account_id="synthetic-account",
    )
    memory.forget(actor, event_ids=[source.id])
    alias = source.model_copy(
        update={"id": uuid4().hex, "source_key": "changed-epoch:" + uuid4().hex}
    )
    with store.transaction(actor) as tx:
        replay = tx.ingest_event(alias, "new-epoch", "cursor", 0)
        assert not replay.inserted and replay.event.content_state == "forgotten"
        assert tx.get_event(alias.id).text == ""
    assert memory.retrieve(actor, "private platform source", include_tasks=False) == []


def test_existing_platform_alias_is_also_redacted(store, memory, actor, emit):
    source = emit(actor, "aliased secret", platform_message_id="p1", channel_account_id="account")
    alias = source.model_copy(update={"id": uuid4().hex, "source_key": "second-key:" + uuid4().hex})
    with store.transaction(actor) as tx:
        tx.ingest_event(alias, "alias-stream", "cursor", 0)
    result = memory.forget(actor, event_ids=[source.id])
    assert set(result.event_ids) == {source.id, alias.id}
    assert memory.retrieve(actor, "aliased secret", include_tasks=False) == []


@pytest.mark.parametrize("reason", ["expired", "recalled"])
def test_expiry_and_recall_also_prevent_replay(store, memory, maintenance, actor, emit, reason):
    now = datetime.now(UTC)
    source = emit(actor, "removed marker", at=now - timedelta(days=40))
    if reason == "expired":
        memory.expire(maintenance, before=now - timedelta(days=30), now=now)
    else:
        recall = emit(
            maintenance,
            kind=EventKind.RECALL,
            reply_to=source.id,
            trusted_actor_origin="agentwechat_authenticated_recall_metadata",
        )
        memory.recall(maintenance, recall.id)
    with store.transaction(actor) as tx:
        replay = tx.ingest_event(source, "replay", "cursor", 0)
        assert not replay.inserted and replay.event.content_state == reason


def test_recall_before_original_message_creates_durable_tombstone(
    store, memory, maintenance, actor, emit
):
    identity = uuid4().hex
    original_key = "future-platform:" + identity
    recall = emit(
        maintenance,
        kind=EventKind.RECALL,
        reply_to=identity,
        platform_message_id="future-platform",
        channel_account_id="account",
        trusted_actor_origin="agentwechat_authenticated_recall_metadata",
    )
    memory.recall(maintenance, recall.id)
    now = datetime.now(UTC)
    original = GroupEvent(
        id=identity,
        source_key=original_key,
        group_id=actor.group_id,
        actor_id=actor.member_id,
        kind=EventKind.TEXT,
        text="must never be persisted",
        occurred_at=now,
        received_at=now,
        platform_message_id="future-platform",
        channel_account_id="account",
    )
    with store.transaction(actor) as tx:
        result = tx.ingest_event(original, "original", "cursor", 0)
        assert not result.inserted and result.event.content_state == "recalled"
        assert tx.get_event(identity).text == ""
    assert memory.retrieve(actor, "must never be persisted", include_tasks=False) == []


def test_forgetting_fact_erases_all_revisions_and_owned_sources(store, memory, actor, emit):
    first = emit(actor, "old fact secret")
    fact = memory.record_fact(actor, "secret", "old", source_event_ids=[first.id], explicit=True)
    second = emit(actor, "new fact secret")
    memory.correct_fact(
        actor, fact.id, "new", expected_revision=1, source_event_ids=[second.id], explicit=True
    )
    result = memory.forget(actor, fact_ids=[fact.id])
    assert set(result.event_ids) == {first.id, second.id}
    assert memory.retrieve(actor, "secret", include_tasks=False) == []
    with pytest.raises(DomainError):
        memory.record_fact(
            actor,
            "secret",
            "old",
            source_event_ids=[first.id],
            explicit=True,
            request_key="different-key",
        )


def test_cross_group_forget_cannot_touch_other_group(store, memory, actor, emit):
    source = emit(actor, "same group only")
    outsider = actor.model_copy(update={"group_id": "outside-" + uuid4().hex, "role": Role.OWNER})
    store.register_group(outsider.group_id)
    with pytest.raises(DomainError) as caught:
        memory.forget(outsider, event_ids=[source.id])
    assert caught.value.code == "not_found" and memory.source_available(actor, source.id)


def test_accepted_receipt_metadata_survives_content_forget(store, memory, actor, emit):
    source = emit(actor, "sent private message")
    with store.transaction(actor) as tx:
        reply = tx.enqueue_reply(source.id, "sent private answer")
        claimed = tx.claim_delivery("synthetic-channel")
        tx.finish_delivery(
            reply.id,
            claimed.claim_token,
            DeliveryState.ACCEPTED,
            platform_message_id="synthetic-accepted",
        )
    memory.forget(actor, event_ids=[source.id])
    with store.transaction(actor) as tx:
        receipt = tx.get_delivery(reply.id)
        assert (
            receipt.state == DeliveryState.ACCEPTED
            and receipt.platform_message_id == "synthetic-accepted"
        )
        assert receipt.payload_ref == "text:[content removed]"


def test_cleanup_queue_has_scoped_refs_and_backup_deadline(store, memory, maintenance, actor, emit):
    now = datetime.now(UTC)
    source = emit(actor, "source", attachments=[{"file_id": "f1", "version_id": "v1"}])
    with store.transaction(actor) as tx:
        work = tx.create_work("synthetic artifact work", source.id)
    result = memory.forget(actor, event_ids=[source.id], now=now)
    jobs = memory.pending_cleanup(maintenance, now=now)
    assert {job.resource_kind for job in jobs} == {"event_attachments", "work_artifacts"}
    attachment = next(job for job in jobs if job.resource_kind == "event_attachments")
    assert attachment.resource_refs == ["artifact:f1:v1"] and attachment.event_id == source.id
    assert next(job for job in jobs if job.resource_kind == "work_artifacts").work_id == work.id
    done = memory.complete_cleanup(
        maintenance, attachment.id, expected_revision=1, evidence_ref="synthetic:purge-receipt"
    )
    assert done.state == "complete"
    later = memory.pending_cleanup(maintenance, now=result.backups_delete_after)
    assert any(job.resource_kind == "backup_tombstone" for job in later)


def test_cleanup_completion_is_permission_and_revision_checked(memory, maintenance, actor, emit):
    source = emit(actor, "cleanup source")
    memory.forget(actor, event_ids=[source.id])
    job = memory.pending_cleanup(maintenance)[0]
    with pytest.raises(DomainError) as caught:
        memory.complete_cleanup(actor, job.id, expected_revision=1, evidence_ref="not maintenance")
    assert caught.value.code == "permission_denied"
    memory.complete_cleanup(
        maintenance, job.id, expected_revision=1, evidence_ref="synthetic-proof"
    )
    with pytest.raises(DomainError) as caught:
        memory.complete_cleanup(maintenance, job.id, expected_revision=1, evidence_ref="stale")
    assert caught.value.code == "version_conflict"


def test_forget_in_coordinator_transaction_rolls_back_with_decision(store, memory, actor, emit):
    source = emit(actor, "not yet forgotten")
    with pytest.raises(RuntimeError):
        with store.transaction(actor) as tx:
            memory.prepare_in_tx(tx)
            memory.in_tx(tx).forget(actor, event_ids=[source.id])
            raise RuntimeError("coordinator crash")
    assert memory.source_available(actor, source.id)


@pytest.mark.parametrize("depth", [1, 2])
def test_quote_snapshots_and_media_handles_are_redacted_without_erasing_quoters_body(
    store, memory, maintenance, actor, emit, depth
):
    original = emit(actor, "original private marker")
    other = actor.model_copy(update={"member_id": "member-b"})
    parent = original
    quoted = []
    for index in range(depth):
        parent = emit(
            other,
            f"my own reply {index}",
            kind=EventKind.QUOTE,
            reply_to=parent.id,
            attachments=[
                {"kind": "quote", "text": "original private marker", "state": "embedded"},
                {"kind": "image", "handle": {"asset_id": "private-image-handle"}},
            ],
        )
        quoted.append(parent)
    fact = memory.record_fact(
        other,
        "quoted private detail",
        "original private marker",
        source_event_ids=[parent.id],
        explicit=True,
    )
    with store.transaction(other) as tx:
        fingerprint = memory._fingerprint(tx, parent.id)
        work = tx.create_work("analyze quoted image", parent.id)
    result = memory.forget(actor, event_ids=[original.id])
    assert set(result.redacted_quote_ids) == {item.id for item in quoted}
    for quote in quoted:
        with store.transaction(other) as tx:
            canonical = tx.get_event(quote.id)
            assert canonical.text == quote.text and canonical.content_state == "active"
            assert len(canonical.attachments) == 1 and canonical.attachments[0]["privacy_redacted"]
            assert canonical.attachments[0]["media_availability"] == "unavailable"
            assert "original private marker" not in canonical.model_dump_json()
            assert "private-image-handle" not in canonical.model_dump_json()
            replay = tx.ingest_event(quote, "quote-replay:" + quote.id, "cursor", 0)
            assert not replay.inserted and replay.event.attachments == canonical.attachments
        assert not memory.source_available(other, quote.id)
    with store.transaction(other) as tx:
        assert memory._fingerprint(tx, parent.id) != fingerprint
        assert tx.get_work(work.id).state.value == "cancelled"
        with pytest.raises(DomainError):
            tx.create_work("do not reload quote image", parent.id)
    with pytest.raises(DomainError):
        memory.get_fact(other, fact.id)
    with pytest.raises(DomainError):
        memory.record_fact(
            other,
            "replayed quote",
            "original private marker",
            source_event_ids=[parent.id],
            explicit=True,
            request_key="new-but-stale-quote",
        )
    jobs = memory.pending_cleanup(maintenance)
    assert {quote.id for quote in quoted}.issubset({job.event_id for job in jobs})
    assert any(job.work_id == work.id for job in jobs)


def test_new_quote_cannot_import_removed_snapshot(store, memory, actor, emit):
    original = emit(actor, "old private marker")
    memory.forget(actor, event_ids=[original.id])
    quote = emit(
        actor,
        "new independent reply",
        kind=EventKind.QUOTE,
        reply_to=original.id,
        attachments=[
            {"kind": "quote", "text": "old private marker", "state": "embedded"},
            {"kind": "image", "handle": {"asset_id": "old-image"}},
        ],
    )
    with store.transaction(actor) as tx:
        canonical = tx.get_event(quote.id)
        assert canonical.text == "new independent reply"
        assert "old private marker" not in canonical.model_dump_json()
        assert "old-image" not in canonical.model_dump_json()
    with store.connect() as conn:
        body = conn.execute(
            "SELECT body FROM ga_events WHERE group_id=%s AND id=%s", (actor.group_id, quote.id)
        ).fetchone()["body"]
        assert "old private marker" not in json.dumps(body)


@pytest.mark.parametrize("failed", [False, True])
def test_forget_redacts_rag_derived_work_history_and_blocks_retry(
    store, memory, actor, emit, failed
):
    secret = emit(actor, "RAG private marker")
    request = emit(actor, "please use the authorized context")
    with store.transaction(actor) as tx:
        work = tx.create_work(
            "RAG private marker",
            request.id,
            acceptance_criteria=["RAG private marker"],
            scope={"source_fingerprints": {secret.id: memory._fingerprint(tx, secret.id)}},
        )
        if failed:
            tx.transition_work(work.id, WorkState.FAILED, expected_revision=1)
    result = memory.forget(actor, event_ids=[secret.id])
    with store.transaction(actor) as tx:
        redacted = tx.get_work(work.id)
        assert "RAG private marker" not in redacted.model_dump_json()
        assert redacted.scope["privacy_redacted"] is True
        if failed:
            with pytest.raises(DomainError) as caught:
                tx.transition_work(
                    work.id, WorkState.QUEUED, expected_revision=redacted.revision, reason="retry"
                )
            assert caught.value.code == "source_unavailable"
        else:
            assert redacted.state.value == "cancelled"
    with store.connect() as conn:
        history = conn.execute(
            "SELECT body FROM ga_work_revisions WHERE group_id=%s AND work_id=%s",
            (actor.group_id, work.id),
        ).fetchall()
        assert "RAG private marker" not in json.dumps(history)
        job = conn.execute(
            "SELECT 1 FROM ga_memory_cleanup WHERE group_id=%s AND work_id=%s AND id=ANY(%s)",
            (actor.group_id, work.id, result.cleanup_ids),
        ).fetchone()
        assert job is not None
