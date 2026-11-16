"""Isolated durable foreground fixtures; no provider or channel network calls."""

import os
from types import SimpleNamespace
from uuid import uuid4

import pytest

from groupagent.config import AppConfig, GroupConfig
from groupagent.conversation.provider import ModelDecisionProvider
from groupagent.conversation.service import ConversationCoordinator
from groupagent.domain import ActorContext, Role
from groupagent.memory import DeterministicFixtureSummarizer, MemberService, MemoryService
from groupagent.persistence import PostgresStore
from groupagent.scheduling import Scheduler


@pytest.fixture
def conversation_env():
    dsn = os.environ.get("GROUPAGENT_TEST_DSN")
    if not dsn:
        pytest.skip("Requires isolated PostgreSQL")
    store = PostgresStore(dsn)
    store.migrate()
    group = "foreground-" + uuid4().hex
    store.register_group(group)
    actor = ActorContext(group_id=group, member_id="synthetic-owner", role=Role.OWNER)
    config = AppConfig(
        group=GroupConfig(
            enabled=True,
            internal_id=group,
            external_group_id="synthetic@chatroom",
            owner_ids=[actor.member_id],
        )
    )
    env = SimpleNamespace(store=store, actor=actor, models={})
    env.coordinator = ConversationCoordinator(
        config,
        store,
        lambda actor, event: ModelDecisionProvider(env.models[event.id]),
        memory=MemoryService(store, DeterministicFixtureSummarizer()),
        members=MemberService(store),
        scheduler=Scheduler(store),
    )
    return env
