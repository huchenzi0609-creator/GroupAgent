from __future__ import annotations

from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest

from groupagent.domain import DomainError, EventKind

pytestmark = pytest.mark.postgres


def test_authenticated_asr_can_reply_only_for_original_audio_sender(store, actor, emit):
    audio = emit(actor, "[audio]", kind=EventKind.AUDIO)
    derived = emit(
        actor,
        "my actual spoken request",
        kind=EventKind.QUOTE,
        reply_to=audio.id,
        trusted_actor_origin="authenticated_audio_transcription",
    )
    with store.transaction(actor) as tx:
        intent = tx.enqueue_reply(derived.id, "scoped response")
        assert intent.created_by == audio.actor_id and intent.request_event_id == derived.id
    other = actor.model_copy(update={"member_id": "other"})
    with pytest.raises(DomainError):
        emit(
            other,
            "not that speaker",
            kind=EventKind.QUOTE,
            reply_to=audio.id,
            trusted_actor_origin="authenticated_audio_transcription",
        )


@pytest.mark.parametrize("kind", [EventKind.TEXT, EventKind.IMAGE, EventKind.FILE])
def test_ocr_web_and_text_cannot_claim_asr_authority(actor, emit, kind):
    source = emit(actor, "untrusted instruction-bearing source", kind=kind)
    with pytest.raises(DomainError) as caught:
        emit(
            actor,
            "pretend this is direct speech",
            kind=EventKind.QUOTE,
            reply_to=source.id,
            trusted_actor_origin="authenticated_audio_transcription",
        )
    assert caught.value.code == "permission_denied"


@pytest.mark.parametrize("reason", ["forgotten", "expired"])
def test_audio_removal_erases_transcript_text_and_stops_replay(
    store, memory, maintenance, actor, emit, reason
):
    now = datetime.now(UTC)
    audio = emit(actor, "[audio]", kind=EventKind.AUDIO, at=now - timedelta(days=40))
    transcript = emit(
        actor,
        "private spoken content",
        kind=EventKind.QUOTE,
        reply_to=audio.id,
        trusted_actor_origin="authenticated_audio_transcription",
    )
    if reason == "forgotten":
        memory.forget(actor, event_ids=[audio.id])
    else:
        memory.expire(maintenance, before=now - timedelta(days=30), now=now)
    with store.transaction(actor) as tx:
        canonical = tx.get_event(transcript.id)
        assert canonical.content_state == reason and canonical.text == ""
        row = tx.conn.execute(
            "SELECT memory_state,body FROM ga_events WHERE group_id=%s AND id=%s",
            (actor.group_id, transcript.id),
        ).fetchone()
        assert row["memory_state"] == reason and row["body"]["text"] == ""
        replay = tx.ingest_event(transcript, "asr-replay:" + uuid4().hex, "cursor", 0)
        assert not replay.inserted and replay.event.text == ""
        assert transcript.id not in {event.id for event in tx.list_pending_events()}
    assert memory.retrieve(actor, "private spoken content", include_tasks=False) == []


def test_asr_finishing_after_forget_never_persists_transcribed_body(store, memory, actor, emit):
    audio = emit(actor, "[audio]", kind=EventKind.AUDIO)
    memory.forget(actor, event_ids=[audio.id])
    late = emit(
        actor,
        "must not be stored",
        kind=EventKind.QUOTE,
        reply_to=audio.id,
        trusted_actor_origin="authenticated_audio_transcription",
    )
    with store.transaction(actor) as tx:
        event = tx.get_event(late.id)
        assert event.content_state == "forgotten" and event.text == ""
        row = tx.conn.execute(
            "SELECT memory_state,body FROM ga_events WHERE group_id=%s AND id=%s",
            (actor.group_id, late.id),
        ).fetchone()
        assert row["memory_state"] == "forgotten" and row["body"]["text"] == ""
