from __future__ import annotations

import os
from datetime import UTC, datetime
from types import SimpleNamespace
from uuid import uuid4

import pytest

from groupagent.artifacts import ArtifactStore
from groupagent.channels import AgentWechatConfig
from groupagent.channels.local import LocalChannel
from groupagent.domain import ActorContext, EventKind, GroupEvent, Role
from groupagent.persistence import PostgresStore
from groupagent.runtime import RuntimeState
from groupagent.runtime.artifacts import ArtifactRegistry


@pytest.fixture
def runtime_env(tmp_path):
    dsn = os.environ.get("GROUPAGENT_TEST_DSN")
    if not dsn:
        pytest.skip("Requires isolated PostgreSQL")
    store = PostgresStore(dsn)
    store.migrate()
    group = "runtime-" + uuid4().hex
    store.register_group(group)
    actor = ActorContext(group_id=group, member_id="synthetic-owner", role=Role.OWNER)
    service = ActorContext(group_id=group, member_id="service:runtime", role=Role.MAINTAINER)
    config = AgentWechatConfig(
        base_url="http://127.0.0.1",
        account_id="local-account",
        external_group_id="local-only@chatroom",
        internal_group_id=group,
        source_epoch="local-test-1",
    )
    artifacts = ArtifactStore(tmp_path / "artifacts")
    return SimpleNamespace(
        store=store,
        state=RuntimeState(store),
        actor=actor,
        service=service,
        config=config,
        channel=LocalChannel(tmp_path / "channel", config),
        artifacts=artifacts,
        registry=ArtifactRegistry(store, artifacts),
        root=tmp_path,
    )


def source(env, text="A synthetic request"):
    now = datetime.now(UTC)
    value = GroupEvent(
        id=uuid4().hex,
        source_key="source:" + uuid4().hex,
        group_id=env.actor.group_id,
        actor_id=env.actor.member_id,
        kind=EventKind.TEXT,
        text=text,
        occurred_at=now,
        received_at=now,
    )
    with env.store.transaction(env.actor) as tx:
        tx.ingest_event(value, "local-test:" + value.id, "1", 0)
    return value
