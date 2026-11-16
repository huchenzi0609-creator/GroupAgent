"""Real PostgreSQL intake with synthetic HTTP transport; no live account."""

from __future__ import annotations

import json
import os
from uuid import uuid4

import httpx
import pytest

from groupagent.channels import AgentWechatAdapter, ChannelCursor
from groupagent.config import AppConfig, GroupConfig
from groupagent.coordinator import Coordinator, Decision, ScriptedDecisionProvider
from groupagent.domain import DomainError
from groupagent.persistence import PostgresStore
from groupagent.transport import ChannelIntake

from .conftest import ROOM, TOKEN, message

pytestmark = pytest.mark.postgres


@pytest.fixture
def intake_setup(config, daemon):
    dsn = os.environ.get("GROUPAGENT_TEST_DSN")
    if not dsn:
        pytest.skip("Requires isolated PostgreSQL")
    store = PostgresStore(dsn)
    store.migrate()
    suffix = uuid4().hex
    group_id = "intake-" + suffix
    store.register_group(group_id)
    # Both upstream identity and our group are unique: source keys cannot be
    # re-bound to another group even in different synthetic test cases.
    config = config.model_copy(update={"internal_group_id": group_id, "account_id": "a-" + suffix})
    app = AppConfig(
        deployment_mode="test",
        group=GroupConfig(
            enabled=True,
            internal_id=group_id,
            external_group_id=ROOM,
            owner_ids=["owner"],
        ),
    )

    # FakeDaemon pins its fixture account; translate only in this synthetic wrapper.
    def handler(request):
        from .conftest import ACCOUNT

        original_path = request.url.path
        mapped_url = request.url.copy_with(path=original_path.replace(config.account_id, ACCOUNT))
        mapped = httpx.Request(
            request.method, mapped_url, headers=request.headers, content=request.content
        )
        response = daemon(mapped)
        try:
            body = response.json()
        except ValueError:
            return response
        encoded = json.dumps(body).replace(ACCOUNT, config.account_id)
        return httpx.Response(
            response.status_code, content=encoded, headers={"content-type": "application/json"}
        )

    return store, config, app, handler


def channel_event(cursor, *, foreign=False, kind="message.committed"):
    return {
        "cursor": str(cursor),
        "kind": kind,
        "data": {"threadId": "other@chatroom" if foreign else ROOM, "messageId": "local-1"},
        "createdAt": "2026-11-13T08:00:00Z",
    }


async def test_committed_intake_precedes_model_and_ack(intake_setup, daemon):
    store, channel, config, handler = intake_setup
    controls = ["sync.received", "account.push", "account.control_observed", "projection.blocked"]
    daemon.events = [
        channel_event(1),
        channel_event(2, foreign=True),
        *[
            {
                "cursor": str(index),
                "kind": kind,
                "data": {"commands": ["SYNTHETIC_CONTROL_ONLY"]},
                "createdAt": "2026-11-13T08:00:00Z",
            }
            for index, kind in enumerate(controls, 3)
        ],
    ]
    daemon.next_cursor = "6"

    class BrokenModel:
        async def decide(self, *args):
            raise DomainError("provider_unavailable", "Synthetic outage")

    coordinator = Coordinator(config, store, BrokenModel())

    def committed_before_ack(request):
        if request.url.path.endswith("/ack"):
            # This separate connection cannot see an uncommitted intake cursor.
            with store.transaction(intake.service_actor) as tx:
                assert len(tx.list_pending_events()) == 1
                saved = ChannelCursor.model_validate_json(tx.get_cursor(intake.stream_id).cursor)
                assert saved.value == "6"
        return handler(request)

    async with httpx.AsyncClient(transport=httpx.MockTransport(committed_before_ack)) as client:
        adapter = AgentWechatAdapter(channel, TOKEN, client=client)
        intake = ChannelIntake(adapter, coordinator)
        result = await intake.poll_once()
        assert result.acknowledged and len(result.event_ids) == 1 and result.skipped_count == 5
        assert daemon.requests[-1].url.path.endswith("/ack")
        with store.transaction(intake.service_actor) as tx:
            assert len(tx.list_pending_events()) == 1 and not tx.list_work()
            cursor = ChannelCursor.model_validate_json(tx.get_cursor(intake.stream_id).cursor)
            assert cursor.value == "6"
        with pytest.raises(DomainError, match="Synthetic outage"):
            await intake.process_pending()
        with store.transaction(intake.service_actor) as tx:
            source = tx.get_event(result.event_ids[0])
        provider = ScriptedDecisionProvider(
            {
                source.source_key: Decision(
                    kind="start_work", reason_code="fixture", goal="Prepare a report"
                )
            }
        )
        intake.coordinator = Coordinator(config, store, provider)
        recovered = await intake.process_pending()
        assert recovered[0].work_id
        assert await intake.process_pending() == []


async def test_all_skipped_batch_advances_without_foreign_body(intake_setup, daemon):
    store, channel, config, handler = intake_setup
    daemon.events = [channel_event(1, foreign=True), channel_event(2, kind="thread.updated")]
    daemon.next_cursor = "2"
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        intake = ChannelIntake(
            AgentWechatAdapter(channel, TOKEN, client=client),
            Coordinator(config, store, ScriptedDecisionProvider({})),
        )
        result = await intake.poll_once()
        assert not result.event_ids and result.skipped_count == 2
        with store.transaction(intake.service_actor) as tx:
            assert not tx.list_pending_events()
            assert (
                ChannelCursor.model_validate_json(tx.get_cursor(intake.stream_id).cursor).value
                == "2"
            )
        assert not any("/messages/" in request.url.path for request in daemon.requests)


async def test_authenticated_channel_request_can_receive_a_bound_reply(intake_setup, daemon):
    store, channel, config, handler = intake_setup
    daemon.events, daemon.next_cursor = [channel_event(1)], "1"
    provider = ScriptedDecisionProvider({})
    coordinator = Coordinator(config, store, provider)
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        intake = ChannelIntake(AgentWechatAdapter(channel, TOKEN, client=client), coordinator)
        received = await intake.poll_once()
        with store.transaction(intake.service_actor) as tx:
            source = tx.get_event(received.event_ids[0])
        assert source.trusted_actor_origin == "agentwechat_authenticated_message_metadata"
        provider.decisions[source.source_key] = Decision(
            kind="reply", reason_code="synthetic_question", response="已收到，我来帮你整理。"
        )
        results = await intake.process_pending()
        assert len(results) == 1 and results[0].delivery_id
        with store.transaction(coordinator.actor(source)) as tx:
            delivery = tx.get_delivery(results[0].delivery_id)
            assert delivery.request_event_id == source.id
            assert delivery.payload_ref == "text:已收到，我来帮你整理。"
        assert await intake.process_pending() == []


async def test_ack_failure_restarts_at_durable_cursor_and_epoch_reset_is_not_silent(
    intake_setup, daemon
):
    store, channel, config, handler = intake_setup
    daemon.messages["local-1"] = message()
    daemon.events, daemon.next_cursor = [channel_event(1)], "1"
    daemon.failure = lambda request: (
        httpx.Response(503) if request.url.path.endswith("/ack") else None
    )
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        coordinator = Coordinator(config, store, ScriptedDecisionProvider({}))
        intake = ChannelIntake(AgentWechatAdapter(channel, TOKEN, client=client), coordinator)
        with pytest.raises(DomainError):
            await intake.poll_once()
        with store.transaction(intake.service_actor) as tx:
            assert len(tx.list_pending_events()) == 1
            assert (
                ChannelCursor.model_validate_json(tx.get_cursor(intake.stream_id).cursor).value
                == "1"
            )
        daemon.failure, daemon.events = None, []
        restarted = ChannelIntake(AgentWechatAdapter(channel, TOKEN, client=client), coordinator)
        assert (await restarted.poll_once()).acknowledged
        event_reads = [
            request for request in daemon.requests if request.url.path.endswith("/events")
        ]
        assert event_reads[-1].url.params["after"] == "1"
        wrong = ChannelIntake(
            AgentWechatAdapter(
                channel.model_copy(update={"source_epoch": "changed"}), TOKEN, client=client
            ),
            coordinator,
        )
        before = len(daemon.requests)
        with pytest.raises(DomainError) as error:
            await wrong.poll_once()
        assert error.value.code == "source_epoch_mismatch" and len(daemon.requests) == before
