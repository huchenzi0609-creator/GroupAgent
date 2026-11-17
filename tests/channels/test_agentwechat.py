from __future__ import annotations

import hashlib
import json

import httpx
import pytest
from pydantic import ValidationError

from groupagent.channels import (
    AgentWechatAdapter,
    AgentWechatConfig,
    AttachmentHandle,
    ChannelCursor,
)
from groupagent.domain import DeliveryState, DomainError, EventKind

from .conftest import ACCOUNT, GROUP, ROOM, TOKEN, asset, message


@pytest.mark.parametrize(
    "url",
    [
        "http://remote.invalid",
        "https://secret@host.invalid",
        "https://host.invalid?token=secret",
        "https://host.invalid/v1",
    ],
)
def test_endpoint_config_rejects_insecure_or_ambiguous_urls(config, url):
    with pytest.raises(ValidationError):
        AgentWechatConfig.model_validate({**config.model_dump(), "base_url": url})


def test_explicit_group_and_credential_boundaries(config):
    with pytest.raises(ValidationError):
        AgentWechatConfig.model_validate({**config.model_dump(), "external_group_id": "filehelper"})
    for token in ("", "secret\nheader"):
        with pytest.raises(DomainError) as error:
            AgentWechatAdapter(config, token)
        assert token not in str(error.value) if token else True


async def test_runtime_capabilities_pin_version_and_hide_host_fields(adapter, daemon):
    result = await adapter.capabilities()
    assert result.reported_version == "2.2.1" and not result.video_processing
    assert not result.live_acceptance_verified and not result.recipient_read_receipts
    assert "video" not in result.metadata_media_kinds
    assert result.file_send_max_bytes == 25 * 1024 * 1024
    assert TOKEN not in repr(adapter) and TOKEN not in result.model_dump_json()
    assert "do-not-expose" not in result.model_dump_json()
    daemon.version = "2.3.0"
    with pytest.raises(DomainError) as error:
        await adapter.capabilities()
    assert error.value.code == "channel_version_mismatch"


async def test_stable_platform_id_ignores_local_profile_id_and_payload_role(adapter, daemon):
    daemon.messages["local-1"]["role"] = "owner"
    daemon.messages["local-1"]["senderName"] = "Administrator"
    first = await adapter.read_message("local-1")
    daemon.messages["local-2"] = message("local-2", role="maintainer")
    second = await adapter.read_message("local-2")
    assert first.id == second.id and first.source_key == second.source_key
    assert first.platform_message_id == "18446744073709551000"
    assert first.actor_id == "synthetic-member"
    assert "role" not in first.model_dump()
    assert first.trusted_actor_origin == "agentwechat_authenticated_message_metadata"


async def test_normalized_mentions_use_native_ids_without_parsing_names_or_quoted_text(
    adapter, daemon
):
    daemon.messages["local-1"].update(
        {
            "body": "我看到了Pot的说明。",
            "mentions": {"state": "known", "userIds": ["synthetic-peer"], "self": False},
        }
    )
    native = await adapter.read_message("local-1")
    assert native.mentioned_member_ids == ["synthetic-peer"]
    daemon.messages["local-1"]["mentions"]["state"] = "unknown"
    assert (await adapter.read_message("local-1")).mentioned_member_ids == []


async def test_local_id_fallback_changes_with_source_epoch(config, daemon):
    daemon.messages["local-1"]["serverId"] = None
    async with httpx.AsyncClient(transport=httpx.MockTransport(daemon)) as client:
        first = await AgentWechatAdapter(config, TOKEN, client=client).read_message("local-1")
        second = await AgentWechatAdapter(
            config.model_copy(update={"source_epoch": "epoch-2"}), TOKEN, client=client
        ).read_message("local-1")
    assert first.source_key != second.source_key
    assert first.platform_message_id is None


async def test_consume_filters_foreign_threads_and_requires_explicit_ack(adapter, daemon):
    daemon.events = [
        {
            "cursor": "1",
            "kind": "message.committed",
            "data": {"threadId": "other@chatroom", "messageId": "foreign"},
            "createdAt": "2026-11-13T08:00:00Z",
        },
        {
            "cursor": "2",
            "kind": "message.committed",
            "data": {"threadId": ROOM, "messageId": "local-1"},
            "createdAt": "2026-11-13T08:00:01Z",
        },
    ]
    daemon.next_cursor = "2"
    batch = await adapter.consume()
    assert len(batch.events) == 1 and batch.entries[0].cursor.value == "2"
    assert batch.skipped[0].reason == "outside_group"
    assert not any(
        "foreign" in request.url.path or "/ack" in request.url.path for request in daemon.requests
    )
    cursor = ChannelCursor.model_validate_json(batch.next_cursor.serialize())
    await adapter.ack_committed(cursor)
    assert json.loads(daemon.requests[-1].content) == {"cursor": "2"}
    assert "/read-state" not in daemon.requests[-1].url.path


@pytest.mark.parametrize(
    "kind",
    [
        "sync.received",
        "account.push",
        "account.control_observed",
        "projection.blocked",
        "media.auto-download",
        "thread.read-state",
        "credentials.projected",
        "metadata.changed",
        "sync.committed",
        "profile.recovery-pending",
    ],
)
async def test_observed_native_controls_allow_target_text_and_voice_without_payload_leak(
    adapter, daemon, kind
):
    daemon.messages["voice"] = message("voice", kind=34, body=None, serverId="18446744073709551002")
    daemon.events = [
        {
            "cursor": "1",
            "kind": kind,
            "data": {"accountId": ACCOUNT, "commands": ["SYNTHETIC_CONTROL_ONLY"]},
            "createdAt": "2026-11-13T08:00:00Z",
        },
        {
            "cursor": "2",
            "kind": "message.committed",
            "data": {"threadId": "other@chatroom", "messageId": "foreign"},
            "createdAt": "2026-11-13T08:00:01Z",
        },
        {
            "cursor": "3",
            "kind": "message.committed",
            "data": {"threadId": ROOM, "messageId": "local-1"},
            "createdAt": "2026-11-13T08:00:02Z",
        },
        {
            "cursor": "4",
            "kind": "message.committed",
            "data": {"threadId": ROOM, "messageId": "voice"},
            "createdAt": "2026-11-13T08:00:03Z",
        },
    ]
    daemon.next_cursor = "4"
    batch = await adapter.consume()
    assert [event.kind for event in batch.events] == [EventKind.TEXT, EventKind.AUDIO]
    assert [item.reason for item in batch.skipped] == ["control_event", "outside_group"]
    assert batch.skipped[0].kind == kind and batch.next_cursor.value == "4"
    assert "SYNTHETIC_CONTROL_ONLY" not in batch.model_dump_json()
    assert not any(
        request.method == "POST" or "foreign" in request.url.path or "/media" in request.url.path
        for request in daemon.requests
    )
    await adapter.ack_committed(batch.next_cursor)
    assert json.loads(daemon.requests[-1].content) == {"cursor": "4"}


@pytest.mark.parametrize(
    "kind",
    [
        "sync.received",
        "account.push",
        "account.control_observed",
        "projection.blocked",
        "media.auto-download",
        "thread.read-state",
        "credentials.projected",
        "metadata.changed",
        "sync.committed",
        "profile.recovery-pending",
    ],
)
async def test_observed_native_controls_still_reject_another_account(adapter, daemon, kind):
    daemon.events = [
        {
            "cursor": "1",
            "kind": kind,
            "data": {"accountId": "another-account"},
            "createdAt": "2026-11-13T08:00:00Z",
        }
    ]
    daemon.next_cursor = "1"
    with pytest.raises(DomainError) as error:
        await adapter.consume()
    assert error.value.code == "scope_denied"
    assert not any(request.method == "POST" for request in daemon.requests)


async def test_unknown_control_kind_in_a_foreign_chat_is_not_silently_skipped(adapter, daemon):
    daemon.events = [
        {
            "cursor": "1",
            "kind": "projection.unknown",
            "data": {"threadId": "other@chatroom"},
            "createdAt": "2026-11-13T08:00:00Z",
        }
    ]
    daemon.next_cursor = "1"
    with pytest.raises(DomainError) as error:
        await adapter.consume()
    assert error.value.code == "unsupported_event"
    assert not any(
        request.method == "POST" or "/messages/" in request.url.path for request in daemon.requests
    )


async def test_wrong_scope_or_epoch_cursor_fails_before_http(adapter, daemon):
    cursor = ChannelCursor(
        account_id=ACCOUNT,
        group_id=GROUP,
        external_group_id=ROOM,
        source_epoch="wrong-epoch",
        daemon_server_id="synthetic-daemon-storage",
        value="0",
    )
    with pytest.raises(DomainError) as error:
        await adapter.consume(cursor)
    assert error.value.code == "source_epoch_mismatch" and daemon.requests == []
    with pytest.raises(DomainError) as error:
        await adapter.consume(cursor.model_copy(update={"group_id": "other"}))
    assert error.value.code == "scope_denied" and daemon.requests == []


async def test_unknown_events_nonmonotonic_cursors_and_expiry_do_not_ack(adapter, daemon):
    daemon.events = [
        {"cursor": "1", "kind": "mystery.event", "data": {}, "createdAt": "2026-11-13T08:00:00Z"}
    ]
    daemon.next_cursor = "1"
    with pytest.raises(DomainError) as error:
        await adapter.consume()
    assert error.value.code == "unsupported_event"
    daemon.events[0]["kind"] = "thread.updated"
    daemon.events.append(dict(daemon.events[0]))
    with pytest.raises(DomainError) as error:
        await adapter.consume()
    assert error.value.code == "channel_gap"
    daemon.failure = lambda request: (
        httpx.Response(409, json={"error": {"code": "event_cursor_expired"}})
        if request.url.path.endswith("/events")
        else None
    )
    with pytest.raises(DomainError) as error:
        await adapter.consume()
    assert error.value.code == "channel_gap"
    assert not any("/ack" in request.url.path for request in daemon.requests)


@pytest.mark.parametrize(
    "changes,code",
    [
        ({"threadId": "other@chatroom"}, "scope_denied"),
        ({"accountId": "other"}, "scope_denied"),
        ({"senderId": None}, "invalid_channel_response"),
        ({"serverId": 18446744073709551000}, "invalid_channel_response"),
        ({"createdAt": "2026-11-13T08:00:00"}, "timestamp_unavailable"),
        ({"type": 999, "senderId": None}, "invalid_channel_response"),
    ],
)
async def test_invalid_inbound_identity_and_protocol_fields_fail(adapter, daemon, changes, code):
    daemon.messages["local-1"].update(changes)
    with pytest.raises(DomainError) as error:
        await adapter.read_message("local-1")
    assert error.value.code == code


@pytest.mark.parametrize(
    "kind,app_type,asset_kind,event_kind",
    [
        (3, None, "image", EventKind.IMAGE),
        (34, None, "voice", EventKind.AUDIO),
        (49, 6, "file", EventKind.FILE),
    ],
)
async def test_images_audio_files_have_scoped_metadata_handles(
    adapter, daemon, kind, app_type, asset_kind, event_kind
):
    daemon.messages["local-1"] = message(kind=kind, appMessageType=app_type)
    daemon.assets["local-1"] = [asset(kind=asset_kind)]
    event = await adapter.read_message("local-1")
    assert event.kind == event_kind
    handle = AttachmentHandle.model_validate(event.attachments[0]["handle"])
    assert handle.group_id == GROUP and handle.account_id == ACCOUNT
    assert handle.availability == "unresolved" and handle.asset_id is None
    assert "untrusted.invalid" not in event.model_dump_json()
    assert not any(request.url.path.endswith("/content") for request in daemon.requests)


async def test_media_availability_changes_do_not_mutate_replayed_event(adapter, daemon):
    daemon.messages["local-1"] = message(kind=3)
    daemon.assets["local-1"] = [
        asset(state="pending", sha256=None, byteLength=None, contentType=None)
    ]
    before = await adapter.read_message("local-1")
    daemon.assets["local-1"] = [asset(content=daemon.content)]
    after = await adapter.read_message("local-1")
    assert before.model_dump(exclude={"received_at"}) == after.model_dump(exclude={"received_at"})
    handles = await adapter.attachment_handles("local-1")
    assert handles[0].availability == "available" and handles[0].sha256 is not None


async def test_recall_event_without_local_message_row_keeps_stable_reference(adapter, daemon):
    daemon.events = [
        {
            "cursor": "1",
            "kind": "message.recalled",
            "createdAt": "2026-11-13T08:02:00Z",
            "data": {
                "threadId": ROOM,
                "serverId": "18446744073709551000",
                "observedAt": "2026-11-13T08:02:00Z",
            },
        }
    ]
    daemon.next_cursor = "1"
    batch = await adapter.consume()
    assert batch.events[0].kind == EventKind.RECALL and batch.events[0].reply_to
    assert not any("/messages/" in request.url.path for request in daemon.requests)


async def test_uncached_unknown_size_is_not_downloaded(adapter, daemon):
    daemon.messages["local-1"] = message(kind=34)
    daemon.assets["local-1"] = [
        asset(kind="voice", state="pending", byteLength=None, sha256=None, contentType=None)
    ]
    event = await adapter.read_message("local-1")
    handle = AttachmentHandle.model_validate(event.attachments[0]["handle"])
    with pytest.raises(DomainError) as error:
        await adapter.request_attachment_download(handle)
    assert error.value.code == "attachment_size_unknown"
    assert not any(request.method == "POST" for request in daemon.requests)


async def test_need_review_receipt_is_unknown_not_rejected_or_retried(adapter, daemon):
    daemon.failure = lambda request: (
        httpx.Response(202, json=daemon.receipt(state="needs_review"))
        if request.url.path.endswith("/sends")
        else None
    )
    receipt = await adapter.send_text(GROUP, "Hello", "uncertain-intent")
    assert receipt.delivery_state == DeliveryState.UNKNOWN and not receipt.accepted
    assert len([request for request in daemon.requests if request.url.path.endswith("/sends")]) == 1


async def test_404_without_api_not_found_code_does_not_prove_send_absent(adapter, daemon):
    daemon.failure = lambda request: (
        httpx.Response(404, content=b"Reverse proxy missing route")
        if request.url.path.endswith("/by-idempotency-key")
        else None
    )
    with pytest.raises(DomainError) as error:
        await adapter.lookup_delivery(GROUP, "intent")
    assert error.value.code == "channel_http_error"


async def test_video_and_video_quotes_never_request_media(adapter, daemon):
    daemon.messages["local-1"] = message(kind=43)
    video = await adapter.read_message("local-1")
    assert video.kind == EventKind.UNSUPPORTED_VIDEO
    daemon.messages["quote"] = message(
        "quote",
        kind=49,
        appMessageType=57,
        serverId="999",
        quote={
            "serverId": "998",
            "type": 43,
            "state": "embedded",
            "embeddedText": "Video caption",
            "embeddedImage": True,
        },
    )
    quoted = await adapter.read_message("quote")
    assert quoted.kind == EventKind.QUOTE and quoted.reply_to
    assert quoted.attachments[0]["media_availability"] == "unsupported_video"
    assert not any("/media" in request.url.path for request in daemon.requests)


async def test_quote_uses_embedded_excerpt_without_fetching_original(adapter, daemon):
    daemon.messages["local-1"] = message(
        kind=49,
        appMessageType=57,
        body="My comment",
        quote={
            "serverId": "17",
            "type": 1,
            "state": "embedded",
            "senderId": "quoted-member",
            "embeddedText": "Only the quoted excerpt",
        },
    )
    event = await adapter.read_message("local-1")
    assert event.text == "My comment"
    assert event.attachments[0]["text"] == "Only the quoted excerpt"
    assert len([r for r in daemon.requests if "/messages/" in r.url.path]) == 1


async def test_recall_has_new_event_identity_and_references_original(adapter, daemon):
    original = await adapter.read_message("local-1")
    daemon.messages["local-1"].update(
        recall={"observedAt": "2026-11-13T08:02:00Z"}, body="[Message recalled]"
    )
    recall = await adapter.read_message("local-1")
    assert (
        recall.kind == EventKind.RECALL
        and recall.id != original.id
        and recall.reply_to == original.id
    )
    assert recall.occurred_at.minute == 2


async def test_fetch_rechecks_scope_and_hash_without_trusting_content_url(adapter, daemon):
    daemon.messages["local-1"] = message(kind=3)
    daemon.assets["local-1"] = [asset(content=daemon.content)]
    event = await adapter.read_message("local-1")
    handle = AttachmentHandle.model_validate(event.attachments[0]["handle"])
    result = await adapter.fetch_attachment(handle)
    assert (
        result.content == daemon.content
        and result.sha256 == hashlib.sha256(daemon.content).hexdigest()
    )
    assert all(request.url.host == "synthetic.invalid" for request in daemon.requests)
    assert daemon.requests[-1].url.path.endswith("/media/asset-1/content")
    count = len(daemon.requests)
    with pytest.raises(DomainError) as error:
        await adapter.fetch_attachment(handle.model_copy(update={"group_id": "other"}))
    assert error.value.code == "scope_denied" and len(daemon.requests) == count
    daemon.content = b"tampered"
    with pytest.raises(DomainError) as error:
        await adapter.fetch_attachment(handle)
    assert error.value.code == "media_integrity_failed"


@pytest.mark.parametrize(
    "changes",
    [
        {"state": "unknown"},
        {"kind": "mystery"},
        {"variant": "mystery"},
        {"filename": "../../secret"},
    ],
)
async def test_unknown_or_unsafe_attachment_metadata_fails(adapter, daemon, changes):
    daemon.messages["local-1"] = message(kind=3)
    daemon.assets["local-1"] = [asset(**changes)]
    with pytest.raises(DomainError):
        await adapter.attachment_handles("local-1")
    assert not any(request.url.path.endswith("/content") for request in daemon.requests)


async def test_pending_download_is_explicit_and_never_sends_read_state(adapter, daemon):
    daemon.messages["local-1"] = message(kind=34)
    daemon.assets["local-1"] = [
        asset(
            kind="voice",
            state="pending",
            byteLength=None,
            sha256=None,
            contentType=None,
            advertisedByteLength=1024,
        )
    ]
    event = await adapter.read_message("local-1")
    handle = AttachmentHandle.model_validate(event.attachments[0]["handle"])
    with pytest.raises(DomainError) as error:
        await adapter.fetch_attachment(handle)
    assert error.value.code == "media_pending"
    transfer = await adapter.request_attachment_download(handle)
    assert transfer.state == "queued" and transfer.job_id == "transfer-1"
    assert not any("/read-state" in request.url.path for request in daemon.requests)


async def test_member_names_use_only_room_display_name_and_absence_is_unconfirmed(adapter, daemon):
    daemon.roster = [
        {
            "id": "one",
            "username": "one",
            "roomDisplayName": "Group Name",
            "displayName": "Private Remark",
            "nickname": "Private Nickname",
            "role": "owner",
        },
        {
            "id": "two",
            "username": "two",
            "roomDisplayName": None,
            "displayName": "Private Remark Two",
        },
    ]
    page = await adapter.members()
    assert page.members[0].room_display_name == "Group Name"
    assert page.members[1].room_display_name is None and page.partial
    assert "Private" not in page.model_dump_json() and '"role"' not in page.model_dump_json()
    daemon.member_state = "unconfirmed"
    result = await adapter.resolve_member("missing")
    assert result.membership_state == "unconfirmed" and result.room_display_name is None


async def test_stable_send_key_lookup_and_not_read_semantics(adapter, daemon):
    queued = await adapter.send_text(GROUP, "Hello", "logical-intent-1")
    assert queued.delivery_state == DeliveryState.PENDING and not queued.accepted
    await adapter.send_text(GROUP, "Hello", "logical-intent-1")
    assert len(daemon.sent) == 1
    daemon.sent[queued.wire_idempotency_key] = daemon.receipt()
    accepted = await adapter.lookup_delivery(GROUP, "logical-intent-1")
    assert accepted.accepted and accepted.delivery_state == DeliveryState.ACCEPTED
    assert accepted.recipient_read is None
    assert accepted.platform_message_id == "18446744073709551001"
    with pytest.raises(DomainError) as error:
        await adapter.send_text(GROUP, "Changed payload", "logical-intent-1")
    assert error.value.code == "idempotency_conflict"


async def test_timeout_is_unknown_and_does_not_retry_or_leak_token(adapter, daemon):
    def fail(request):
        if request.url.path.endswith("/sends"):
            raise httpx.ReadTimeout("Synthetic transport timeout", request=request)
        return None

    daemon.failure = fail
    with pytest.raises(DomainError) as error:
        await adapter.send_text(GROUP, "Hello", "stable-1")
    assert error.value.code == "delivery_unknown" and error.value.effect_state == "unknown"
    assert not error.value.retryable and TOKEN not in str(error.value)
    assert len([request for request in daemon.requests if request.url.path.endswith("/sends")]) == 1


async def test_redirect_never_forwards_credentials(adapter, daemon):
    daemon.failure = lambda request: httpx.Response(
        302, headers={"Location": "https://evil.invalid/steal"}
    )
    with pytest.raises(DomainError) as error:
        await adapter.capabilities()
    assert error.value.code == "redirect_rejected"
    assert len(daemon.requests) == 1 and daemon.requests[0].url.host == "synthetic.invalid"


async def test_foreign_send_scope_rejected_before_any_http(adapter, daemon):
    with pytest.raises(DomainError) as error:
        await adapter.send_text("other", "Hello", "intent")
    assert error.value.code == "scope_denied" and daemon.requests == []


@pytest.mark.parametrize(
    "receipt_changes",
    [
        {"state": "succeeded"},
        {"state": "accepted", "serverId": None},
        {"destination": "other@chatroom"},
    ],
)
async def test_unknown_or_foreign_receipts_never_report_accepted(adapter, daemon, receipt_changes):
    daemon.failure = lambda request: (
        httpx.Response(202, json=daemon.receipt(**receipt_changes))
        if request.url.path.endswith("/sends")
        else None
    )
    with pytest.raises(DomainError) as error:
        await adapter.send_text(GROUP, "Hello", "intent")
    assert error.value.code in {"invalid_receipt", "scope_denied"}
    assert error.value.effect_state == "unknown"


async def test_file_send_verifies_staging_and_reconciles_restart(adapter, daemon, config):
    first = await adapter.send_file(GROUP, daemon.upload_name, daemon.upload_content, "file-intent")
    assert first.job_id == "job-1" and not first.accepted
    second = await adapter.send_file(
        GROUP, daemon.upload_name, daemon.upload_content, "file-intent"
    )
    assert second.job_id == first.job_id and daemon.upload_calls == 1
    with pytest.raises(DomainError) as error:
        await adapter.send_file(GROUP, daemon.upload_name, b"different", "file-intent")
    assert error.value.code == "idempotency_conflict"
    async with httpx.AsyncClient(transport=httpx.MockTransport(daemon)) as client:
        restarted = AgentWechatAdapter(config, TOKEN, client=client)
        found = await restarted.lookup_delivery(GROUP, "file-intent")
        assert found.job_id == "job-1"
        with pytest.raises(DomainError) as error:
            await restarted.send_file(
                GROUP, daemon.upload_name, daemon.upload_content, "file-intent"
            )
        assert error.value.code == "delivery_requires_reconciliation"
    assert daemon.upload_calls == 1


async def test_video_send_and_paths_rejected_before_http(adapter, daemon):
    for name, code in (("video.mp4", "unsupported_video"), ("../report.md", "invalid_attachment")):
        with pytest.raises(DomainError) as error:
            await adapter.send_file(GROUP, name, b"synthetic", "intent")
        assert error.value.code == code
    assert daemon.requests == []


async def test_persisted_daemon_identity_blocks_reset_after_restart(config, daemon):
    pinned = config.model_copy(update={"expected_daemon_server_id": "original-storage"})
    async with httpx.AsyncClient(transport=httpx.MockTransport(daemon)) as client:
        adapter = AgentWechatAdapter(pinned, TOKEN, client=client)
        with pytest.raises(DomainError) as error:
            await adapter.send_text(GROUP, "Hello", "intent")
    assert error.value.code == "source_epoch_mismatch"
    assert not any(request.method == "POST" for request in daemon.requests)


async def test_daemon_reset_between_intake_and_ack_is_rejected(adapter, daemon):
    batch = await adapter.consume()
    daemon.server_id = "replacement-storage"
    with pytest.raises(DomainError) as error:
        await adapter.ack_committed(batch.next_cursor)
    assert error.value.code == "source_epoch_mismatch"
    assert not any(request.url.path.endswith("/ack") for request in daemon.requests)


async def test_attachment_message_rebinding_and_changed_hash_are_rejected(adapter, daemon):
    daemon.messages["local-1"] = message(kind=3)
    daemon.assets["local-1"] = [asset(content=daemon.content)]
    handle = (await adapter.attachment_handles("local-1"))[0]
    daemon.messages["local-1"]["serverId"] = "45"
    with pytest.raises(DomainError) as error:
        await adapter.fetch_attachment(handle)
    assert error.value.code == "scope_denied"
    daemon.messages["local-1"]["serverId"] = "18446744073709551000"
    daemon.assets["local-1"][0]["sha256"] = "a" * 64
    with pytest.raises(DomainError) as error:
        await adapter.fetch_attachment(handle)
    assert error.value.code == "attachment_version_conflict"
    assert not any(request.url.path.endswith("/content") for request in daemon.requests)


async def test_video_labeled_as_file_is_not_downloaded(adapter, daemon):
    daemon.messages["local-1"] = message(kind=49, appMessageType=6)
    daemon.assets["local-1"] = [asset(kind="file", filename="video.mp4", contentType="video/mp4")]
    reference = AttachmentHandle.model_validate(
        (await adapter.read_message("local-1")).attachments[0]["handle"]
    )
    with pytest.raises(DomainError) as error:
        await adapter.fetch_attachment(reference)
    assert error.value.code == "unsupported_video"
    assert not any(request.url.path.endswith("/content") for request in daemon.requests)


async def test_json_response_limit_is_checked_before_parsing(config, daemon):
    constrained = config.model_copy(update={"max_json_bytes": 16})
    async with httpx.AsyncClient(transport=httpx.MockTransport(daemon)) as client:
        adapter = AgentWechatAdapter(constrained, TOKEN, client=client)
        with pytest.raises(DomainError) as error:
            await adapter.capabilities()
    assert error.value.code == "channel_response_too_large"
