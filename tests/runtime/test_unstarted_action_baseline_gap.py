"""Behavioural regression intentionally compatible with the pre-fix source API."""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

import pytest
from conversation.turn_model_fixture import finish_fixture_reply

from groupagent.config import AppConfig, GroupConfig
from groupagent.conversation.provider import ModelDecisionProvider
from groupagent.conversation.service import ConversationCoordinator
from groupagent.domain import EventKind, GroupEvent
from groupagent.execution.models import ModelReply, ModelToolCall, TokenUsage
from groupagent.memory import MemberService, MemoryService
from groupagent.scheduling import Scheduler

pytestmark = pytest.mark.postgres
USAGE = TokenUsage(input_tokens=1, output_tokens=1)


class PromiseThenAction:
    name = "synthetic-independent-action-obligation"

    def __init__(self):
        self.rounds = 0

    async def complete(self, request):
        self.rounds += 1
        if self.rounds == 1:
            return ModelReply(text="我现在去搜，查完告诉你。", usage=USAGE)
        if self.rounds == 2:
            return ModelReply(
                tool_calls=(
                    ModelToolCall(
                        id="baseline-gap-action",
                        name="start_work",
                        arguments={
                            "goal": "公开核实Polaris Studio官网下载和安装步骤。",
                            "work_capabilities": ["public_research"],
                            "outputs": [{"format": "text"}],
                        },
                    ),
                ),
                finish_reason="tool_calls",
                usage=USAGE,
            )
        return finish_fixture_reply(
            request, ModelReply(text="核实后给你官方地址和安装步骤。", usage=USAGE)
        )


async def test_explicit_search_must_commit_in_same_turn_instead_of_promise_only(runtime_env):
    env = runtime_env
    model = PromiseThenAction()
    config = AppConfig(
        group=GroupConfig(
            enabled=True,
            internal_id=env.actor.group_id,
            external_group_id=env.config.external_group_id,
            owner_ids=[env.actor.member_id],
        )
    )
    coordinator = ConversationCoordinator(
        config,
        env.store,
        lambda *_: ModelDecisionProvider(model),
        memory=MemoryService(env.store),
        members=MemberService(env.store),
        scheduler=Scheduler(env.store),
        artifacts=env.registry,
    )
    stamp = datetime.now(UTC)
    event = GroupEvent(
        id=uuid4().hex,
        source_key=uuid4().hex,
        group_id=env.actor.group_id,
        actor_id=env.actor.member_id,
        kind=EventKind.TEXT,
        text="你去网上搜一下不就知道了，搜完告诉我怎么安装，并给出官方下载链接。",
        occurred_at=stamp,
        received_at=stamp,
    )
    with env.store.transaction(env.actor) as tx:
        tx.ingest_event(event, event.id, "1", 0)
    result = await coordinator.process(event)
    with env.store.transaction(env.actor) as tx:
        works = tx.list_work()
        assert len(works) == 1, "The accepted search request ended as prose with no actual work"
        assert works[0].request_event_id == event.id
        assert result.work_id == works[0].id
        assert not any(d.payload_ref == "text:我现在去搜，查完告诉你。" for d in tx.list_delivery())
