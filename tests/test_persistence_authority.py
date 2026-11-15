"""Real PostgreSQL regressions for scoped reply and transport capabilities."""

from __future__ import annotations

import hashlib
import json
import os
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from threading import Barrier
from uuid import uuid4

import pytest

from groupagent.domain import ActorContext, DeliveryState, DomainError, EventKind, GroupEvent, Role
from groupagent.persistence import PostgresStore

pytestmark = pytest.mark.postgres


@pytest.fixture(scope="module")
def store() -> PostgresStore:
    dsn = os.environ.get("GROUPAGENT_TEST_DSN")
    if not dsn:
        pytest.skip("Set GROUPAGENT_TEST_DSN for real PostgreSQL capability tests")
    result = PostgresStore(dsn)
    assert result.migrate() == 13
    return result


@pytest.fixture
def actor(store: PostgresStore) -> ActorContext:
    group_id = "authority-test-" + uuid4().hex
    store.register_group(group_id)
    return ActorContext(group_id=group_id, member_id="synthetic-member", role=Role.MEMBER)


def seed(
    store: PostgresStore,
    actor: ActorContext,
    *,
    kind: EventKind = EventKind.TEXT,
    origin: str = "verified_channel_metadata",
    stream_id: str | None = None,
) -> GroupEvent:
    stamp = datetime.now(UTC)
    event_id = str(uuid4())
    source = GroupEvent(
        id=event_id,
        source_key="authority-source:" + event_id,
        group_id=actor.group_id,
        actor_id=actor.member_id,
        kind=kind,
        text="A synthetic read-only question",
        occurred_at=stamp,
        received_at=stamp,
        trusted_actor_origin=origin,
    )
    with store.transaction(actor) as tx:
        tx.ingest_event(source, stream_id or event_id, "first", 0)
    return source


@pytest.mark.parametrize("role", [Role.OBSERVER, Role.MAINTAINER])
@pytest.mark.parametrize(
    "origin",
    [
        "verified_channel_metadata",
        "agentwechat_authenticated_message_metadata",
    ],
)
def test_read_only_reply_roundtrip_preserves_role_and_identity(
    store: PostgresStore, actor: ActorContext, role: Role, origin: str
) -> None:
    actor = actor.model_copy(update={"role": role})
    source = seed(store, actor, origin=origin)
    text = "A scoped read-only answer"
    with store.transaction(actor) as tx:
        intent = tx.enqueue_reply(source.id, text, read_only_authorized=True)
        assert tx.actor.role == role
        assert intent.created_by == actor.member_id
        assert intent.request_event_id == source.id and intent.work_id is None
        assert intent.group_id == actor.group_id and intent.payload_ref == "text:" + text
        assert intent.payload_hash == hashlib.sha256(text.encode()).hexdigest()
        assert intent.idempotency_key == "reply:" + source.id + ":1"
        assert tx.enqueue_reply(source.id, text, read_only_authorized=True).id == intent.id
        claimed = tx.claim_delivery("synthetic-channel", intent_id=intent.id)
        assert claimed is not None
    with store.transaction(actor) as tx:
        accepted = tx.finish_delivery(
            intent.id,
            claimed.claim_token,
            DeliveryState.ACCEPTED,
            platform_message_id="synthetic-reply:" + intent.id,
        )
        assert accepted.state == DeliveryState.ACCEPTED and tx.actor.role == role
        assert tx.list_work() == []
        assert tx.claim_delivery("synthetic-channel") is None


@pytest.mark.parametrize("role", [Role.OBSERVER, Role.MAINTAINER])
def test_read_only_reply_requires_explicit_policy_and_grants_no_business_writes(
    store: PostgresStore, actor: ActorContext, role: Role
) -> None:
    actor = actor.model_copy(update={"role": role})
    source = seed(store, actor)
    with pytest.raises(DomainError) as caught:
        with store.transaction(actor) as tx:
            tx.enqueue_reply(source.id, "Not authorized by policy")
    assert caught.value.code == "permission_denied"
    with pytest.raises(DomainError) as caught:
        with store.transaction(actor) as tx:
            tx.enqueue_delivery(None, "artifact:private-version", "a" * 64, "arbitrary-key")
    assert caught.value.code == "permission_denied"
    with pytest.raises(DomainError) as caught:
        with store.transaction(actor) as tx:
            tx.create_work("Cannot execute tools", source.id)
    assert caught.value.code == "permission_denied"
    with store.transaction(actor) as tx:
        literal = tx.enqueue_reply(source.id, "artifact:private-version", read_only_authorized=True)
        assert literal.payload_ref == "text:artifact:private-version"
        assert literal.work_id is None  # Literal text cannot become an artifact delivery.


@pytest.mark.parametrize("role", [Role.OBSERVER, Role.MAINTAINER, Role.OWNER])
def test_reply_actor_must_match_even_for_group_owner(
    store: PostgresStore, actor: ActorContext, role: Role
) -> None:
    source = seed(store, actor)
    other = actor.model_copy(update={"member_id": "another-member", "role": role})
    with pytest.raises(DomainError) as caught:
        with store.transaction(other) as tx:
            tx.enqueue_reply(source.id, "Impersonated response", read_only_authorized=True)
    assert caught.value.code == "permission_denied"


def test_reply_needs_persisted_same_group_request(
    store: PostgresStore, actor: ActorContext
) -> None:
    source = seed(store, actor)
    outsider = actor.model_copy(
        update={"group_id": "outside-" + uuid4().hex, "role": Role.OBSERVER}
    )
    store.register_group(outsider.group_id)
    for target, event_id in ((actor, "missing-event"), (outsider, source.id)):
        with pytest.raises(DomainError) as caught:
            with store.transaction(target) as tx:
                tx.enqueue_reply(event_id, "No cross-group reply", read_only_authorized=True)
        assert caught.value.code == "not_found"


@pytest.mark.parametrize(
    "kind, origin",
    [
        (EventKind.SYSTEM, "verified_channel_metadata"),
        (EventKind.MEMBERSHIP, "verified_channel_metadata"),
        (EventKind.RECALL, "verified_channel_metadata"),
        (EventKind.TEXT, "unverified_forwarded_author"),
    ],
)
def test_reply_cannot_use_synthetic_or_unverified_actor_sources(
    store: PostgresStore, actor: ActorContext, kind: EventKind, origin: str
) -> None:
    actor = actor.model_copy(update={"role": Role.OBSERVER})
    source = seed(store, actor, kind=kind, origin=origin)
    with pytest.raises(DomainError) as caught:
        with store.transaction(actor) as tx:
            tx.enqueue_reply(source.id, "No authority from metadata", read_only_authorized=True)
    assert caught.value.code == "permission_denied"


@pytest.mark.parametrize("role", [Role.OBSERVER, Role.MAINTAINER])
def test_reply_grant_cannot_claim_or_finish_unbound_artifact_intents(
    store: PostgresStore, actor: ActorContext, role: Role
) -> None:
    with store.transaction(actor) as tx:
        artifact = tx.enqueue_delivery(None, "artifact:synthetic", "a" * 64, "member:artifact")
    restricted = actor.model_copy(update={"role": role})
    source = seed(store, restricted)
    with store.transaction(restricted) as tx:
        reply = tx.enqueue_reply(source.id, "Authorized text only", read_only_authorized=True)
        assert tx.claim_delivery("channel", intent_id=artifact.id) is None
        claimed = tx.claim_delivery("channel")
        assert claimed is not None and claimed.id == reply.id
    with pytest.raises(DomainError) as caught:
        with store.transaction(restricted) as tx:
            tx.finish_delivery(artifact.id, 0, DeliveryState.ACCEPTED, platform_message_id="fake")
    assert caught.value.code == "permission_denied"


def test_concurrent_reply_and_unknown_recovery_keep_one_fixed_key(
    store: PostgresStore, actor: ActorContext
) -> None:
    actor = actor.model_copy(update={"role": Role.OBSERVER})
    source = seed(store, actor)
    barrier = Barrier(2)

    def enqueue() -> str:
        barrier.wait()
        with store.transaction(actor) as tx:
            return tx.enqueue_reply(source.id, "Same answer", read_only_authorized=True).id

    with ThreadPoolExecutor(max_workers=2) as executor:
        identifiers = list(executor.map(lambda _: enqueue(), range(2)))
    assert identifiers[0] == identifiers[1]
    with pytest.raises(DomainError) as caught:
        with store.transaction(actor) as tx:
            tx.enqueue_reply(source.id, "A changed payload", read_only_authorized=True)
    assert caught.value.code == "idempotency_conflict"
    with store.transaction(actor) as tx:
        claimed = tx.claim_delivery("channel")
        assert claimed is not None
    with store.connect() as conn:
        conn.execute(
            "UPDATE ga_deliveries SET claim_expires_at = clock_timestamp() - interval '1 second' "
            "WHERE group_id = %s AND id = %s",
            (actor.group_id, claimed.id),
        )
    with store.transaction(actor) as tx:
        recovered = tx.recover_expired_deliveries()
        assert len(recovered) == 1 and recovered[0].state == DeliveryState.UNKNOWN
        assert tx.claim_delivery("channel-restart") is None
        assert (
            tx.enqueue_reply(source.id, "Same answer", read_only_authorized=True).id == claimed.id
        )
        tx.reconcile_delivery(
            claimed.id,
            DeliveryState.ACCEPTED,
            evidence_ref="synthetic:receipt-query",
            platform_message_id="synthetic:known-reply",
        )


def test_reply_policy_revision_is_rechecked_before_dispatch(
    store: PostgresStore, actor: ActorContext
) -> None:
    actor = actor.model_copy(update={"role": Role.OBSERVER})
    source = seed(store, actor)
    with store.transaction(actor) as tx:
        tx.enqueue_reply(source.id, "Previously authorized", read_only_authorized=True)
    store.register_group(actor.group_id, 2)
    with store.transaction(actor.model_copy(update={"policy_revision": 2})) as tx:
        assert tx.claim_delivery("channel") is None


@pytest.mark.parametrize("role", [Role.OWNER, Role.MAINTAINER])
def test_transport_cursor_initial_and_repeated_checkpoint_are_event_free(
    store: PostgresStore, actor: ActorContext, role: Role
) -> None:
    actor = actor.model_copy(update={"role": role})
    opaque = "opaque::not-for-audit"
    with store.transaction(actor) as tx:
        first = tx.advance_cursor(
            "channel", opaque, expected_cursor_revision=0, reason="channel_batch_complete"
        )
        again = tx.advance_cursor(
            "channel", opaque, expected_cursor_revision=0, reason="channel_batch_complete"
        )
        assert first.revision == 1 and again == first
        assert tx.list_pending_events() == []
    with store.connect() as conn:
        rows = conn.execute(
            "SELECT details FROM ga_audit_events WHERE group_id = %s AND action = 'cursor_advanced'",
            (actor.group_id,),
        ).fetchall()
        assert len(rows) == 1 and opaque not in json.dumps(rows)
        assert rows[0]["details"] == {
            "reason": "channel_batch_complete",
            "previous_revision": 0,
            "revision": 1,
        }


@pytest.mark.parametrize("role", [Role.MEMBER, Role.OBSERVER])
def test_business_and_read_only_members_cannot_advance_transport_cursor(
    store: PostgresStore, actor: ActorContext, role: Role
) -> None:
    actor = actor.model_copy(update={"role": role})
    with pytest.raises(DomainError) as caught:
        with store.transaction(actor) as tx:
            tx.advance_cursor(
                "channel", "opaque", expected_cursor_revision=0, reason="channel_empty_batch"
            )
    assert caught.value.code == "permission_denied"
    with store.transaction(actor) as tx:
        assert tx.get_cursor("channel") is None


def test_competing_transport_checkpoints_use_compare_and_swap(
    store: PostgresStore, actor: ActorContext
) -> None:
    actor = actor.model_copy(update={"role": Role.MAINTAINER})
    barrier = Barrier(2)

    def advance(cursor: str) -> str:
        barrier.wait()
        try:
            with store.transaction(actor) as tx:
                tx.advance_cursor(
                    "channel", cursor, expected_cursor_revision=0, reason="channel_filtered_batch"
                )
            return "advanced"
        except DomainError as error:
            return error.code

    with ThreadPoolExecutor(max_workers=2) as executor:
        outcomes = list(executor.map(advance, ["opaque-a", "opaque-b"]))
    assert sorted(outcomes) == ["advanced", "version_conflict"]
    with store.transaction(actor) as tx:
        checkpoint = tx.get_cursor("channel")
        assert checkpoint is not None and checkpoint.revision == 1


def test_transport_checkpoint_does_not_change_duplicate_event_semantics(
    store: PostgresStore, actor: ActorContext
) -> None:
    source = seed(store, actor, stream_id="channel")
    transport = actor.model_copy(update={"role": Role.MAINTAINER})
    with store.transaction(transport) as tx:
        checkpoint = tx.advance_cursor(
            "channel",
            "after-foreign-events",
            expected_cursor_revision=1,
            reason="channel_filtered_batch",
        )
        assert checkpoint.revision == 2
    with store.transaction(actor) as tx:
        replay = tx.ingest_event(source, "channel", "first", 0)
        assert not replay.inserted and replay.cursor_revision == 2
        current = tx.get_cursor("channel")
        assert current is not None and current.cursor == "after-foreign-events"
    with pytest.raises(DomainError) as caught:
        with store.transaction(transport) as tx:
            tx.advance_cursor(
                "channel",
                "stale-other-checkpoint",
                expected_cursor_revision=1,
                reason="channel_batch_complete",
            )
    assert caught.value.code == "version_conflict"


def test_transport_checkpoint_rolls_back_and_has_group_scope(
    store: PostgresStore, actor: ActorContext
) -> None:
    actor = actor.model_copy(update={"role": Role.MAINTAINER})
    with pytest.raises(RuntimeError, match="before ack"):
        with store.transaction(actor) as tx:
            tx.advance_cursor(
                "channel", "opaque-a", expected_cursor_revision=0, reason="channel_empty_batch"
            )
            raise RuntimeError("crash before ack")
    with store.transaction(actor) as tx:
        assert tx.get_cursor("channel") is None
        tx.advance_cursor(
            "channel", "opaque-a", expected_cursor_revision=0, reason="channel_empty_batch"
        )
    other = actor.model_copy(update={"group_id": "outside-" + uuid4().hex})
    store.register_group(other.group_id)
    with store.transaction(other) as tx:
        assert tx.get_cursor("channel") is None
        tx.advance_cursor(
            "channel", "opaque-b", expected_cursor_revision=0, reason="channel_empty_batch"
        )
    with store.transaction(actor) as tx:
        current = tx.get_cursor("channel")
        assert current is not None and current.cursor == "opaque-a"


def test_transport_reason_rejects_message_content_and_stale_policy(
    store: PostgresStore, actor: ActorContext
) -> None:
    actor = actor.model_copy(update={"role": Role.MAINTAINER})
    with pytest.raises(DomainError) as caught:
        with store.transaction(actor) as tx:
            tx.advance_cursor(
                "channel",
                "opaque",
                expected_cursor_revision=0,
                reason="actual private message content",
            )
    assert caught.value.code == "invalid_input"
    store.register_group(actor.group_id, 2)
    with pytest.raises(DomainError) as caught:
        with store.transaction(actor) as tx:
            tx.advance_cursor(
                "channel", "opaque", expected_cursor_revision=0, reason="channel_batch_complete"
            )
    assert caught.value.code == "policy_changed"
