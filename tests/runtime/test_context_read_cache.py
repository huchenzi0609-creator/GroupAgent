from __future__ import annotations

import json
from collections import Counter
from contextlib import contextmanager

import psycopg
import pytest
from psycopg.types.json import Jsonb

from groupagent.domain import DomainError
from groupagent.memory import MemoryService
from groupagent.persistence import UnitOfWork

from .test_context_privacy import emit, realistic_history

pytestmark = pytest.mark.postgres
EVENT_SELECT = "SELECT body,memory_state FROM ga_events WHERE group_id = %s AND id = %s"


def test_context_cache_deduplicates_sql_without_changing_payload_or_hashes(
    runtime_env, monkeypatch
):
    env = runtime_env
    h = realistic_history(env, task_count=0)
    request = emit(env, "请新建独立工作，制作一份合成调研报告，整理资料并交付Markdown。")
    original_execute = psycopg.Connection.execute
    original_cache = UnitOfWork.context_event_reads
    original_get_event = UnitOfWork.get_event
    calls = []
    active_phases, phases = {}, []

    def counted(conn, query, params=None, **kwargs):
        if query == EVENT_SELECT:
            calls.append(params[1])
            if id(conn) in active_phases:
                active_phases[id(conn)]["selects"].append(params[1])
        return original_execute(conn, query, params, **kwargs)

    def observed_get_event(tx, identifier):
        if id(tx.conn) in active_phases:
            active_phases[id(tx.conn)]["reads"].append(identifier)
        return original_get_event(tx, identifier)

    @contextmanager
    def uncached(tx):
        yield

    @contextmanager
    def observed_cache(tx):
        with original_cache(tx):
            phase = {"reads": [], "selects": []}
            active_phases[id(tx.conn)] = phase
            phases.append(phase)
            try:
                yield
            finally:
                active_phases.pop(id(tx.conn))

    monkeypatch.setattr(psycopg.Connection, "execute", counted)
    monkeypatch.setattr(UnitOfWork, "get_event", observed_get_event)
    monkeypatch.setattr(UnitOfWork, "context_event_reads", uncached)
    before = h.coordinator._context(env.actor, request)
    before_count = len(calls)
    calls.clear()
    monkeypatch.setattr(UnitOfWork, "context_event_reads", observed_cache)
    after = h.coordinator._context(env.actor, request)
    assert len(calls) < before_count
    assert phases and any(Counter(phase["reads"])[request.id] > 1 for phase in phases)
    for phase in phases:
        # Each raw event row is loaded once in its own phase; canonical reads,
        # quote sanitization and fingerprints may still inspect it repeatedly.
        assert all(count == 1 for count in Counter(phase["selects"]).values())
        assert set(phase["selects"]) == set(phase["reads"])
    assert before["_snapshot"] == after["_snapshot"]
    before.pop("now", None)
    after.pop("now", None)
    assert json.dumps(before, sort_keys=True, default=str) == json.dumps(
        after, sort_keys=True, default=str
    )


@pytest.mark.parametrize("mutation", ["body", "privacy", "delete"])
def test_same_phase_source_mutation_fails_before_context_can_escape(runtime_env, mutation):
    env = runtime_env
    source = emit(env, "original synthetic source")
    with pytest.raises(DomainError) as caught:
        with env.store.transaction(env.actor) as tx:
            MemoryService.prepare_in_tx(tx)
            with tx.context_event_reads():
                assert tx.get_event(source.id).text == source.text
                if mutation == "body":
                    body = source.model_copy(update={"text": "changed"}).model_dump(mode="json")
                    tx.conn.execute(
                        "UPDATE ga_events SET body=%s WHERE group_id=%s AND id=%s",
                        (Jsonb(body), env.actor.group_id, source.id),
                    )
                elif mutation == "privacy":
                    tx.conn.execute(
                        "UPDATE ga_events SET memory_state='forgotten' WHERE group_id=%s AND id=%s",
                        (env.actor.group_id, source.id),
                    )
                else:
                    tx.conn.execute(
                        "DELETE FROM ga_events WHERE group_id=%s AND id=%s",
                        (env.actor.group_id, source.id),
                    )
    assert caught.value.code == "memory_changed"
    with env.store.transaction(env.actor) as tx:
        assert tx.get_event(source.id) == source
        assert tx._context_event_rows is None


def test_context_cache_never_shares_mutable_models_or_survives_actor_change(runtime_env):
    env = runtime_env
    source = emit(env, "synthetic", attachments=[{"kind": "data", "nested": {"items": [1, 2]}}])
    with env.store.transaction(env.actor) as tx:
        MemoryService.prepare_in_tx(tx)
        with tx.context_event_reads():
            changed = tx.get_event(source.id)
            changed.attachments[0]["nested"]["items"].append(3)
            changed.attachments.append({"kind": "untrusted-mutation"})
            assert tx.get_event(source.id).attachments == source.attachments
        assert tx._context_event_rows is None
    with pytest.raises(DomainError) as caught:
        with env.store.transaction(env.actor) as tx:
            MemoryService.prepare_in_tx(tx)
            with tx.context_event_reads():
                tx.get_event(source.id)
                tx.actor = env.actor.model_copy(update={"group_id": "another-group"})
                tx.get_event(source.id)
    assert caught.value.code == "scope_violation"


def test_context_cache_does_not_survive_transaction_or_mask_later_privacy_change(runtime_env):
    env = runtime_env
    source = emit(env, "synthetic")
    memory = MemoryService(env.store)
    with env.store.transaction(env.actor) as tx:
        MemoryService.prepare_in_tx(tx)
        with tx.context_event_reads():
            snapshot = memory.snapshot_in_tx(tx, source_event_ids=[source.id])
            assert tx.get_event(source.id).text == source.text
        assert tx._context_event_rows is None
    memory.forget(env.actor, event_ids=[source.id])
    with pytest.raises(DomainError) as caught:
        with env.store.transaction(env.actor) as tx:
            assert tx._context_event_rows is None
            memory.assert_snapshot_in_tx(tx, snapshot)
    assert caught.value.code in {"memory_changed", "source_unavailable"}
