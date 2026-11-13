from __future__ import annotations

import asyncio
import hashlib
import os
from datetime import UTC, datetime
from uuid import uuid4

import httpx
import pytest

from groupagent.config import AppConfig, GroupConfig
from groupagent.coordinator import (
    Coordinator,
    Decision,
    DeepSeekDecisionProvider,
    ScriptedDecisionProvider,
)
from groupagent.domain import ActorContext, DeliveryState, DomainError, EventKind, GroupEvent, Role
from groupagent.persistence import PostgresStore


@pytest.fixture
def setup():
    dsn = os.environ.get("GROUPAGENT_TEST_DSN")
    if not dsn:
        pytest.skip("Requires isolated PostgreSQL")
    store = PostgresStore(dsn)
    store.migrate()
    group = "coordinator-" + uuid4().hex
    store.register_group(group)
    config = AppConfig(
        deployment_mode="test",
        group=GroupConfig(
            enabled=True,
            internal_id=group,
            external_group_id="synthetic",
            owner_ids=["owner"],
        ),
    )
    return store, config


def event(config, text="Please prepare a report", **updates):
    identifier = uuid4().hex
    now = datetime.now(UTC)
    return GroupEvent(
        id=identifier,
        source_key="synthetic:" + identifier,
        group_id=config.group.internal_id,
        actor_id=updates.pop("actor_id", "member"),
        kind=updates.pop("kind", EventKind.TEXT),
        text=text,
        occurred_at=now,
        received_at=now,
        **updates,
    )


@pytest.mark.postgres
async def test_duplicate_crash_recovery_and_no_prefix(setup):
    store, config = setup
    source = event(config)

    class Broken:
        async def decide(self, *args):
            raise DomainError("provider_unavailable", "Synthetic outage")

    coordinator = Coordinator(config, store, Broken())
    with pytest.raises(DomainError):
        await coordinator.receive(source, stream_id="s", cursor="1", verified_channel=True)
    actor = coordinator.actor(source)
    with store.transaction(actor) as tx:
        assert [e.id for e in tx.list_pending_events()] == [source.id]
        assert not tx.list_work()
    provider = ScriptedDecisionProvider(
        {source.source_key: Decision(kind="start_work", reason_code="test", goal="Prepare report")}
    )
    coordinator = Coordinator(config, store, provider)
    first = await coordinator.receive(source, stream_id="s", cursor="1", verified_channel=True)
    replay = await coordinator.receive(source, stream_id="s", cursor="1", verified_channel=True)
    assert first.work_id == replay.work_id
    assert replay.duplicate and len(provider.calls) == 1
    with store.transaction(actor) as tx:
        assert len(tx.list_work()) == 1
        assert tx.list_pending_events() == []


@pytest.mark.postgres
async def test_video_never_reaches_model_and_unverified_input_rejected(setup):
    store, config = setup
    source = event(config, kind=EventKind.UNSUPPORTED_VIDEO)
    provider = ScriptedDecisionProvider({})
    coordinator = Coordinator(config, store, provider)
    with pytest.raises(DomainError, match="Unverified"):
        await coordinator.receive(source, stream_id="s", cursor="1")
    outcome = await coordinator.receive(source, stream_id="s", cursor="1", verified_channel=True)
    assert outcome.decision.reason_code == "unsupported_video"
    assert outcome.work_id is None and outcome.delivery_id is None
    assert provider.calls == []


@pytest.mark.postgres
async def test_long_reply_not_truncated_and_actor_cannot_be_spoofed(setup):
    store, config = setup
    source = event(config)
    long_reply = "详细回答" * 100
    provider = ScriptedDecisionProvider(
        {source.source_key: Decision(kind="reply", reason_code="test", response=long_reply)}
    )
    coordinator = Coordinator(config, store, provider)
    outcome = await coordinator.receive(source, stream_id="s", cursor="1", verified_channel=True)
    assert outcome.delivery_id
    with store.transaction(coordinator.actor(source)) as tx:
        assert tx.get_delivery(outcome.delivery_id).payload_ref == "text:" + long_reply
    spoof = source.model_copy(update={"actor_id": "owner"})
    with pytest.raises(DomainError, match="identity"):
        await coordinator.process(spoof)


@pytest.mark.postgres
async def test_verified_send_echo_is_ignored_but_same_account_human_is_not(setup):
    store, config = setup
    actor = ActorContext(group_id=config.group.internal_id, member_id="owner", role=Role.OWNER)
    with store.transaction(actor) as tx:
        intent = tx.enqueue_delivery(
            None, "text:hello", hashlib.sha256(b"hello").hexdigest(), "fixture-echo"
        )
        claim = tx.claim_delivery("fixture")
        tx.finish_delivery(
            intent.id,
            claim.claim_token,
            DeliveryState.ACCEPTED,
            platform_message_id="sent-platform-id",
        )
    echo = event(config, "hello", actor_id="owner", platform_message_id="sent-platform-id")
    human = event(config, "hello", actor_id="owner", platform_message_id="human-platform-id")
    provider = ScriptedDecisionProvider(
        {human.source_key: Decision(kind="reply", reason_code="human", response="你好")}
    )
    coordinator = Coordinator(config, store, provider)
    one = await coordinator.receive(echo, stream_id="s", cursor="1", verified_channel=True)
    two = await coordinator.receive(human, stream_id="s", cursor="2", verified_channel=True)
    assert one.decision.kind == "ignore" and one.delivery_id is None
    assert two.delivery_id and provider.calls == [human.source_key]


@pytest.mark.postgres
async def test_competing_decisions_create_only_one_work(setup):
    store, config = setup
    source = event(config)

    class Racing:
        calls = 0
        ready = asyncio.Event()

        async def decide(self, *args):
            self.calls += 1
            number = self.calls
            if self.calls == 2:
                self.ready.set()
            await self.ready.wait()
            return Decision(kind="start_work", reason_code="race", goal=f"Goal {number}")

    coordinator = Coordinator(config, store, Racing())
    results = await asyncio.gather(
        *[
            coordinator.receive(source, stream_id="s", cursor="1", verified_channel=True)
            for _ in range(2)
        ]
    )
    assert results[0].work_id == results[1].work_id
    with store.transaction(coordinator.actor(source)) as tx:
        assert len(tx.list_work()) == 1


@pytest.mark.postgres
@pytest.mark.parametrize("role", ["observer", "maintainer"])
async def test_read_only_reply_needs_policy_and_cannot_create_work(setup, role):
    store, config = setup
    group = config.group.model_copy(update={role + "_ids": ["reader"]})
    config = config.model_copy(update={"group": group})
    source = event(config, actor_id="reader")
    provider = ScriptedDecisionProvider(
        {source.source_key: Decision(kind="start_work", reason_code="fixture", goal="Draft file")}
    )
    coordinator = Coordinator(config, store, provider)
    denied = await coordinator.receive(source, stream_id="s", cursor="1", verified_channel=True)
    assert denied.decision.kind == "observe" and not provider.calls
    config = config.model_copy(
        update={"group": group.model_copy(update={"read_only_qa_roles": [role]})}
    )
    source = event(config, actor_id="reader")
    provider.decisions[source.source_key] = Decision(
        kind="start_work", reason_code="fixture", goal="Draft file"
    )
    coordinator = Coordinator(config, store, provider)
    result = await coordinator.receive(source, stream_id="s", cursor="2", verified_channel=True)
    assert result.decision.kind == "reply" and result.delivery_id and not result.work_id
    with store.transaction(coordinator.actor(source)) as tx:
        assert not tx.list_work()
        assert tx.get_delivery(result.delivery_id).request_event_id == source.id


async def test_http_router_keeps_model_role_outside_authority():
    async def handler(request):
        return httpx.Response(
            200,
            json={
                "choices": [
                    {
                        "finish_reason": "stop",
                        "message": {
                            "content": '{"kind":"start_work","reason_code":"request","goal":"x","role":"owner"}'
                        },
                    }
                ]
            },
        )

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    router = DeepSeekDecisionProvider("synthetic-fixture-token", client=client)
    config = AppConfig(
        group=GroupConfig(internal_id="g", external_group_id="synthetic", owner_ids=["owner"])
    )
    with pytest.raises(DomainError, match="valid decision"):
        await router.decide(event(config), {})
    await client.aclose()


@pytest.mark.parametrize("finish_reason", ["length", "content_filter", "tool_calls", None])
async def test_http_router_requires_complete_structured_output(finish_reason):
    async def handler(request):
        return httpx.Response(
            200,
            json={
                "choices": [
                    {
                        "finish_reason": finish_reason,
                        "message": {"content": '{"kind":"observe","reason_code":"x"}'},
                    }
                ]
            },
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        router = DeepSeekDecisionProvider("synthetic-fixture-token", client=client)
        config = AppConfig(
            group=GroupConfig(internal_id="g", external_group_id="synthetic", owner_ids=["owner"])
        )
        with pytest.raises(DomainError):
            await router.decide(event(config), {})


async def test_http_router_never_follows_redirect_even_with_injected_client():
    hosts = []

    async def handler(request):
        hosts.append(request.url.host)
        return httpx.Response(307, headers={"Location": "https://untrusted.example/collect"})

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler), follow_redirects=True
    ) as client:
        router = DeepSeekDecisionProvider("synthetic-fixture-token", client=client)
        config = AppConfig(
            group=GroupConfig(internal_id="g", external_group_id="synthetic", owner_ids=["owner"])
        )
        with pytest.raises(DomainError):
            await router.decide(event(config), {})
    assert hosts == ["api.deepseek.com"]
