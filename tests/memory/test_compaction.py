from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta

import pytest

from groupagent.domain import DomainError, EventKind
from groupagent.memory import DeterministicFixtureSummarizer, MemoryService, SummaryDraft
from groupagent.persistence import UnitOfWork

pytestmark = pytest.mark.postgres


async def test_token_trigger_is_not_fixed_message_count(memory, actor, emit):
    emit(actor, "short")
    assert await memory.compact(actor, token_budget=4000) is None
    emit(actor, "large-source " * 800)
    summary = await memory.compact(actor, token_budget=5000, trigger_ratio=0.5)
    assert summary is not None and len(summary.source_event_ids) == 2
    assert summary.incomplete and summary.provider == "deterministic-fixture"


async def test_summary_and_cursor_survive_process_restart(store, memory, actor, emit):
    early = emit(actor, "early proposal is use solar energy")
    later = emit(actor, "unresolved: project cost")
    summary = await memory.compact(actor, force=True)
    assert summary is not None and summary.source_event_ids == [early.id, later.id]
    restarted = MemoryService(store, DeterministicFixtureSummarizer())
    hits = restarted.retrieve(actor, "solar", include_tasks=False)
    assert any(hit.kind == "summary" and hit.incomplete for hit in hits)
    assert restarted.checkpoint(actor)["through_sequence"] == summary.last_sequence
    assert await restarted.compact(actor, force=True) is None


@pytest.mark.parametrize("failure", ["provider", "wrong_coverage", "blank"])
async def test_failed_summary_never_advances_cursor(store, actor, emit, failure):
    source = emit(actor, "retain this full source")

    class Provider:
        async def summarize(self, request):
            if failure == "provider":
                raise DomainError("provider_unavailable", "synthetic provider failure")
            return SummaryDraft(
                text="   " if failure == "blank" else "candidate",
                covered_event_ids=["foreign-id"] if failure == "wrong_coverage" else [source.id],
                provider="fixture",
                model="fixture",
            )

    memory = MemoryService(store, Provider())
    with pytest.raises(DomainError):
        await memory.compact(actor, force=True)
    assert memory.checkpoint(actor)["through_sequence"] == 0
    assert [hit.id for hit in memory.retrieve(actor, include_tasks=False)] == [source.id]


async def test_summary_commit_and_cursor_are_atomic(store, memory, actor, emit, monkeypatch):
    source = emit(actor, "source survives checkpoint crash")
    original = UnitOfWork._audit

    def crash(self, action, resource_ref, **details):
        if action == "memory_compacted":
            raise RuntimeError("crash before commit")
        return original(self, action, resource_ref, **details)

    with monkeypatch.context() as patch:
        patch.setattr(UnitOfWork, "_audit", crash)
        with pytest.raises(RuntimeError):
            await memory.compact(actor, force=True)
    assert memory.checkpoint(actor)["through_sequence"] == 0
    assert [hit.id for hit in memory.retrieve(actor, include_tasks=False)] == [source.id]


class BlockingProvider:
    def __init__(self):
        self.started = asyncio.Event()
        self.release = asyncio.Event()
        self.calls = 0

    async def summarize(self, request):
        self.calls += 1
        self.started.set()
        await self.release.wait()
        return SummaryDraft(
            text="candidate secret summary",
            covered_event_ids=[event["id"] for event in request.events],
            provider="fixture",
            model="fixture",
        )


async def test_concurrent_compactors_have_one_effective_provider(store, actor, emit):
    emit(actor, "sample")
    provider = BlockingProvider()
    memory = MemoryService(store, provider)
    first = asyncio.create_task(memory.compact(actor, force=True))
    await provider.started.wait()
    with pytest.raises(DomainError) as caught:
        await MemoryService(store, provider).compact(actor, force=True)
    assert caught.value.code == "lease_busy" and provider.calls == 1
    provider.release.set()
    assert await first is not None


@pytest.mark.parametrize("change", ["forget", "policy", "departure", "lease_expiry"])
async def test_provider_result_cannot_commit_after_revocation(
    store, members, maintenance, actor, emit, change
):
    source = emit(actor, "secret source")
    provider = BlockingProvider()
    memory = MemoryService(store, provider)
    pending = asyncio.create_task(memory.compact(actor, force=True))
    await provider.started.wait()
    current = actor
    if change == "forget":
        memory.forget(actor, event_ids=[source.id])
    elif change == "policy":
        store.register_group(actor.group_id, 2)
        current = actor.model_copy(update={"policy_revision": 2})
    elif change == "departure":
        evidence = emit(maintenance, kind=EventKind.MEMBERSHIP)
        members.apply_membership(
            maintenance, actor.member_id, False, evidence.id, verified_channel=True
        )
        current = maintenance
    else:
        with store.connect() as conn:
            conn.execute(
                "UPDATE ga_memory_checkpoints SET lease_until=clock_timestamp()-interval '1 second' WHERE group_id=%s",
                (actor.group_id,),
            )
    provider.release.set()
    with pytest.raises(DomainError):
        await pending
    assert memory.checkpoint(current)["through_sequence"] == 0
    with store.connect() as conn:
        assert (
            conn.execute(
                "SELECT count(*) AS n FROM ga_memory_summaries WHERE group_id=%s", (actor.group_id,)
            ).fetchone()["n"]
            == 0
        )


async def test_cancelled_provider_releases_lease_without_acknowledging_sources(store, actor, emit):
    emit(actor, "cancelled source")
    provider = BlockingProvider()
    memory = MemoryService(store, provider)
    pending = asyncio.create_task(memory.compact(actor, force=True))
    await provider.started.wait()
    pending.cancel()
    with pytest.raises(asyncio.CancelledError):
        await pending
    assert memory.checkpoint(actor)["through_sequence"] == 0
    restarted = MemoryService(store, DeterministicFixtureSummarizer())
    assert await restarted.compact(actor, force=True) is not None


async def test_raw_expiration_invalidates_summary_and_rewinds_rebuild(
    store, memory, maintenance, actor, emit
):
    now = datetime.now(UTC)
    old = emit(actor, "expired chat marker", at=now - timedelta(days=40))
    fresh = emit(actor, "fresh source")
    await memory.compact(actor, force=True)
    memory.expire(maintenance, before=now - timedelta(days=30), now=now)
    assert memory.retrieve(actor, "expired chat marker", include_tasks=False) == []
    rebuilt = await memory.compact(actor, force=True)
    assert rebuilt is not None and rebuilt.source_event_ids == [fresh.id]
    assert old.id not in rebuilt.source_event_ids


async def test_bound_memory_service_rejects_provider_inside_outer_transaction(
    store, memory, actor, emit
):
    emit(actor, "source")
    with store.transaction(actor) as tx:
        with pytest.raises(DomainError) as caught:
            await memory.in_tx(tx).compact(actor, force=True)
        assert caught.value.code == "invalid_transaction"


async def test_oversized_single_source_fails_without_truncation(memory, actor, emit):
    emit(actor, "中文" * 3000)
    with pytest.raises(DomainError) as caught:
        await memory.compact(actor, force=True, token_budget=100)
    assert caught.value.code == "context_too_large"
    assert memory.checkpoint(actor)["through_sequence"] == 0 and memory.summarizer.calls == []
