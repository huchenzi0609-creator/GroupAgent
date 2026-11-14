from __future__ import annotations

import os
from datetime import UTC, datetime
from uuid import uuid4

import pytest

from groupagent.domain import ActorContext, EventKind, GroupEvent, Role
from groupagent.memory import (
    DeterministicFixtureSummarizer,
    MemberService,
    MemoryService,
    RosterSnapshot,
)
from groupagent.persistence import PostgresStore


@pytest.fixture(scope="session")
def store() -> PostgresStore:
    dsn = os.environ.get("GROUPAGENT_TEST_DSN")
    if not dsn:
        pytest.skip("Set GROUPAGENT_TEST_DSN for real PostgreSQL memory tests")
    store = PostgresStore(dsn)
    store.migrate()
    with store.connect() as conn:
        assert conn.execute("SELECT 1 FROM ga_schema_migrations WHERE version=4").fetchone()
    return store


@pytest.fixture
def actor(store: PostgresStore) -> ActorContext:
    group = "memory-test-" + uuid4().hex
    store.register_group(group)
    return ActorContext(group_id=group, member_id="member-a", role=Role.MEMBER)


@pytest.fixture
def owner(actor: ActorContext) -> ActorContext:
    return actor.model_copy(update={"member_id": "owner", "role": Role.OWNER})


@pytest.fixture
def maintenance(actor: ActorContext) -> ActorContext:
    return actor.model_copy(update={"member_id": "service:runtime", "role": Role.MAINTAINER})


@pytest.fixture
def members(store: PostgresStore) -> MemberService:
    return MemberService(store)


@pytest.fixture
def memory(store: PostgresStore) -> MemoryService:
    return MemoryService(store, DeterministicFixtureSummarizer())


@pytest.fixture
def emit(store: PostgresStore):
    def emit(
        actor: ActorContext,
        text: str = "synthetic source",
        *,
        kind: EventKind = EventKind.TEXT,
        at: datetime | None = None,
        **extra,
    ) -> GroupEvent:
        stamp = at or datetime.now(UTC)
        identity = uuid4().hex
        event = GroupEvent(
            id=identity,
            source_key="memory-source:" + identity,
            group_id=actor.group_id,
            actor_id=actor.member_id,
            kind=kind,
            text=text,
            occurred_at=stamp,
            received_at=stamp,
            **extra,
        )
        with store.transaction(actor) as tx:
            tx.ingest_event(event, identity, identity, 0)
        return event

    return emit


class FixtureRoster:
    def __init__(self, snapshot: RosterSnapshot | None = None, *, error: Exception | None = None):
        self.snapshot = snapshot
        self.error = error
        self.calls = 0

    async def fetch(self, group_id: str) -> RosterSnapshot:
        self.calls += 1
        if self.error:
            raise self.error
        assert self.snapshot is not None
        return self.snapshot
