from __future__ import annotations

import pytest

from groupagent.domain import DomainError
from groupagent.memory import MemoryService
from groupagent.memory.common import source_rows

from .conftest import source

pytestmark = pytest.mark.postgres


def test_membership_lookup_avoids_generic_plan_without_disabling_other_preparation(runtime_env):
    env = runtime_env
    event = source(env)
    memory = MemoryService(env.store)
    topic = memory.create_topic(env.actor, "Synthetic planning test")
    memory.link_event(env.actor, event.id, topic.id, evidence_event_ids=[event.id])
    with env.store.transaction(env.actor) as tx:
        assert tx.conn.prepare_threshold == 5
        for _ in range(20):
            rows = source_rows(tx, [event.id], shared=True)
            assert len(rows) == 1 and rows[0]["id"] == event.id
        statements = [
            row["statement"]
            for row in tx.conn.execute("SELECT statement FROM pg_prepared_statements").fetchall()
        ]
        assert any("SELECT body,memory_state FROM ga_events" in text for text in statements)
        assert not any(
            "SELECT t.* FROM ga_memory_topics t JOIN ga_topic_events e" in text
            for text in statements
        )
        assert tx.conn.prepare_threshold == 5
    memory.forget(env.actor, event_ids=[event.id])
    with pytest.raises(DomainError) as caught:
        with env.store.transaction(env.actor) as tx:
            source_rows(tx, [event.id], shared=True)
    assert caught.value.code == "source_unavailable"


def test_membership_planning_does_not_change_group_scope(runtime_env):
    env = runtime_env
    event = source(env)
    group = env.actor.group_id + "-foreign"
    env.store.register_group(group)
    foreign = env.actor.model_copy(update={"group_id": group})
    with pytest.raises(DomainError) as caught:
        with env.store.transaction(foreign) as tx:
            source_rows(tx, [event.id], shared=True)
    assert caught.value.code == "not_found"
