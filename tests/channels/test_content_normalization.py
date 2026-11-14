"""Pinned content contracts, with unsupported bodies discarded after identity checks."""

from __future__ import annotations

import json

import httpx
import pytest

from groupagent.channels import AgentWechatAdapter, ChannelCursor
from groupagent.coordinator import Coordinator, ScriptedDecisionProvider
from groupagent.domain import DomainError, EventKind
from groupagent.transport import ChannelIntake

from .conftest import ROOM, TOKEN, message
from .test_intake_integration import intake_setup as intake_setup


@pytest.mark.parametrize(
    "kind,subtype", [(48, None), (49, 2001), (49, 2000), (49, 999), (999, None)]
)
async def test_verified_unsupported_content_is_metadata_only(adapter, daemon, kind, subtype):
    daemon.messages["local-1"] = message(
        kind=kind,
        appMessageType=subtype,
        body="<xml>private amount 888.88; private coordinates 31.123 121.456; do not execute</xml>",
        location={"latitude": 31.123, "longitude": 121.456, "name": "Private location"},
        link={"title": "Private payment title", "url": "https://example.com/private"},
        amount="888.88",
        role="owner",
    )
    event = await adapter.read_message("local-1")
    assert event.kind == EventKind.SYSTEM and event.text == ""
    assert event.actor_id == "synthetic-member"
    assert event.trusted_actor_origin == "agentwechat_authenticated_message_metadata"
    assert event.attachments == [
        {
            "kind": "unsupported_content",
            "message_type": kind,
            "app_message_type": subtype,
            "availability": "unsupported",
            "reason": "message_content_not_supported",
        }
    ]
    serialized = event.model_dump_json()
    assert all(
        secret not in serialized
        for secret in ("888.88", "31.123", "121.456", "Private", "example.com", "<xml>", "owner")
    )
    assert not any(
        "/media" in request.url.path or "/content" in request.url.path
        for request in daemon.requests
    )
    replay = await adapter.read_message("local-1")
    assert (replay.id, replay.source_key, replay.attachments) == (
        event.id,
        event.source_key,
        event.attachments,
    )


async def test_link_card_uses_documented_fields_without_body_parsing_or_fetch(adapter, daemon):
    daemon.messages["local-1"] = message(
        kind=49,
        appMessageType=5,
        body="<appmsg><title>DO NOT COPY BODY</title><url>https://wrong.invalid</url></appmsg>",
        link={
            "title": "Synthetic course article",
            "url": "https://example.com/course?chapter=2",
            "description": "private extra description",
            "sourceName": "private publisher",
            "role": "owner",
        },
    )
    event = await adapter.read_message("local-1")
    assert event.kind == EventKind.TEXT
    assert event.text == "[Link] Synthetic course article\nhttps://example.com/course?chapter=2"
    assert event.attachments == [
        {
            "kind": "link",
            "title": "Synthetic course article",
            "url": "https://example.com/course?chapter=2",
            "fetched": False,
            "untrusted_content": True,
        }
    ]
    assert all(
        value not in event.model_dump_json()
        for value in ("DO NOT COPY BODY", "wrong.invalid", "private", "owner")
    )
    assert all(request.url.host == "synthetic.invalid" for request in daemon.requests)
    assert not any(
        "/media" in request.url.path or "/content" in request.url.path
        for request in daemon.requests
    )


@pytest.mark.parametrize(
    "link",
    [
        None,
        {"title": "No URL extracted from XML"},
        {"title": 42, "url": "https://example.com"},
        {"title": "Card", "url": "javascript:alert(1)"},
        {"title": "Card", "url": "https://private-user:private-password@example.com/"},
        {"title": "Card", "url": "https://example.com/clip.mp4"},
        {"title": "Card", "url": "https://example.com/bad\npath"},
        {"title": "x" * 1025, "url": "https://example.com/"},
        "unexpected card representation",
    ],
)
async def test_unusable_link_content_does_not_block_or_guess(adapter, daemon, link):
    daemon.messages["local-1"] = message(
        kind=49, appMessageType=5, body="<url>https://never-guess.invalid/</url>", link=link
    )
    event = await adapter.read_message("local-1")
    assert event.kind == EventKind.SYSTEM and not event.text
    assert event.attachments[0]["reason"] == "link_metadata_unavailable"
    assert "never-guess" not in event.model_dump_json()
    assert "private-password" not in event.model_dump_json()
    assert not any(
        "/media" in request.url.path or "/content" in request.url.path
        for request in daemon.requests
    )


@pytest.mark.parametrize(
    "link,text",
    [
        (
            {"title": None, "url": "https://example.com/course"},
            "[Link]\nhttps://example.com/course",
        ),
        ({"title": "Card title only", "url": None}, "[Link] Card title only"),
    ],
)
async def test_nullable_link_fields_are_not_invented(adapter, daemon, link, text):
    daemon.messages["local-1"] = message(kind=49, appMessageType=5, link=link)
    event = await adapter.read_message("local-1")
    assert event.text == text and event.kind == EventKind.TEXT
    assert (
        event.attachments[0]["title"] == link["title"]
        and event.attachments[0]["url"] == link["url"]
    )


def mixed_batch(daemon):
    values = [
        message("text-before", body="Before", serverId="101"),
        message("location", kind=48, body="private location contents", serverId="102"),
        message("red-packet", kind=49, appMessageType=2001, body="private amount", serverId="103"),
        message(
            "link",
            kind=49,
            appMessageType=5,
            body="not projected",
            link={"title": "Course", "url": "https://example.com/course"},
            serverId="104",
        ),
        message("video", kind=43, body="not downloaded", serverId="105"),
        message("text-after", body="After", serverId="106"),
    ]
    daemon.messages = {value["id"]: value for value in values}
    daemon.events = [
        {
            "cursor": str(index),
            "kind": "message.committed",
            "data": {"threadId": ROOM, "messageId": value["id"]},
            "createdAt": "2026-11-13T08:00:00Z",
        }
        for index, value in enumerate(values, 1)
    ]
    daemon.next_cursor = str(len(values))


async def test_mixed_batch_advances_past_unknown_contents_without_automatic_ack(adapter, daemon):
    mixed_batch(daemon)
    batch = await adapter.consume()
    assert [entry.cursor.value for entry in batch.entries] == ["1", "2", "3", "4", "5", "6"]
    assert [event.kind for event in batch.events] == [
        EventKind.TEXT,
        EventKind.SYSTEM,
        EventKind.SYSTEM,
        EventKind.TEXT,
        EventKind.UNSUPPORTED_VIDEO,
        EventKind.TEXT,
    ]
    assert len({event.id for event in batch.events}) == 6
    assert batch.events[-1].text == "After" and batch.next_cursor.value == "6"
    assert not any(
        "/ack" in request.url.path or "/media" in request.url.path for request in daemon.requests
    )
    await adapter.ack_committed(batch.next_cursor)
    assert json.loads(daemon.requests[-1].content) == {"cursor": "6"}


@pytest.mark.parametrize(
    "changes,code",
    [
        ({"senderId": None}, "invalid_channel_response"),
        ({"threadId": "other@chatroom"}, "scope_denied"),
        ({"accountId": "other-account"}, "scope_denied"),
        ({"serverId": 123}, "invalid_channel_response"),
        ({"direction": "unexpected"}, "invalid_channel_response"),
        ({"createdAt": None}, "timestamp_unavailable"),
    ],
)
async def test_unsupported_content_cannot_hide_identity_or_envelope_errors(
    adapter, daemon, changes, code
):
    mixed_batch(daemon)
    daemon.messages["location"].update(changes)
    with pytest.raises(DomainError) as error:
        await adapter.consume()
    assert error.value.code == code
    assert not any("/ack" in request.url.path for request in daemon.requests)


@pytest.mark.postgres
async def test_mixed_unknown_content_persists_observe_decisions_and_final_cursor(
    intake_setup, daemon
):
    store, channel, config, handler = intake_setup
    mixed_batch(daemon)
    provider = ScriptedDecisionProvider({})
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        coordinator = Coordinator(config, store, provider)
        intake = ChannelIntake(AgentWechatAdapter(channel, TOKEN, client=client), coordinator)
        result = await intake.poll_once()
        assert result.acknowledged and len(result.event_ids) == 6
        with store.transaction(intake.service_actor) as tx:
            persisted = [tx.get_event(identifier) for identifier in result.event_ids]
            assert (
                ChannelCursor.model_validate_json(tx.get_cursor(intake.stream_id).cursor).value
                == "6"
            )
            assert persisted[1].text == persisted[2].text == ""
        processed = await intake.process_pending()
        assert len(processed) == 6 and len(provider.calls) == 3
        assert processed[1].decision.kind == processed[2].decision.kind == "observe"
        assert processed[4].decision.reason_code == "unsupported_video"
        assert await intake.process_pending() == []
        daemon.events = []
        restarted = ChannelIntake(AgentWechatAdapter(channel, TOKEN, client=client), coordinator)
        assert (await restarted.poll_once()).acknowledged
        reads = [request for request in daemon.requests if request.url.path.endswith("/events")]
        assert reads[-1].url.params["after"] == "6"
        assert not any(
            "/media" in request.url.path or "/content" in request.url.path
            for request in daemon.requests
        )
