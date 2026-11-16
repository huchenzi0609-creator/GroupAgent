from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime, timedelta

import pytest

from groupagent.domain import DomainError, EventKind
from groupagent.execution.models import ModelReply, TokenUsage
from groupagent.memory import (
    DeterministicFixtureSummarizer,
    MemoryService,
    ModelPortSummarizer,
    SummaryDraft,
)
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


class InvalidSummaryProvider:
    configuration_key = "synthetic-invalid-summary-v1"

    def __init__(self):
        self.calls = 0

    async def summarize(self, request):
        self.calls += 1
        raise DomainError("invalid_summary", "Synthetic failure; do not persist this message")


async def test_invalid_window_cools_down_across_restart_without_advancing_or_losing_sources(
    store, actor, emit
):
    source = emit(actor, "synthetic private source requiring preservation " * 100)
    with store.transaction(actor) as tx:
        task = tx.create_team_task("Keep this long-term responsibility", source_event_id=source.id)
    provider = InvalidSummaryProvider()
    memory = MemoryService(store, provider)
    with pytest.raises(DomainError):
        await memory.compact(actor, token_budget=4000, trigger_ratio=0.1)
    restarted = MemoryService(store, provider)
    assert await restarted.compact(actor, token_budget=4000, trigger_ratio=0.1) is None
    assert await restarted.compact(actor, force=True) is None
    assert provider.calls == 1
    assert restarted.checkpoint(actor)["through_sequence"] == 0
    assert [hit.id for hit in restarted.retrieve(actor, include_tasks=False)] == [source.id]
    with store.transaction(actor) as tx:
        assert tx.get_team_task(task.id) == task
    with store.connect() as conn:
        failures = conn.execute(
            "SELECT details FROM ga_audit_events WHERE group_id=%s "
            "AND action='memory_compaction_failed'",
            (actor.group_id,),
        ).fetchall()
        assert len(failures) == 1
        assert failures[0]["details"]["retry_after_seconds"] == 900
        assert failures[0]["details"]["consecutive_failures"] == 1
        assert source.text not in str(failures)
        assert source.id not in str(failures)
        assert "do not persist" not in str(failures)
        assert (
            conn.execute(
                "SELECT count(*) AS n FROM ga_memory_summaries WHERE group_id=%s", (actor.group_id,)
            ).fetchone()["n"]
            == 0
        )


async def test_same_invalid_window_uses_bounded_exponential_backoff(store, actor, emit):
    emit(actor, "synthetic source")
    provider = InvalidSummaryProvider()
    memory = MemoryService(store, provider)
    for expected_seconds in (900, 1800, 3600, 3600):
        with pytest.raises(DomainError):
            await memory.compact(actor, force=True)
        with store.connect() as conn:
            failure = conn.execute(
                "SELECT id,details FROM ga_audit_events WHERE group_id=%s "
                "AND action='memory_compaction_failed' ORDER BY id DESC LIMIT 1",
                (actor.group_id,),
            ).fetchone()
            assert failure["details"]["retry_after_seconds"] == expected_seconds
            conn.execute(
                "UPDATE ga_audit_events SET created_at=clock_timestamp()-interval '61 minutes' "
                "WHERE id=%s",
                (failure["id"],),
            )
    assert provider.calls == 4 and memory.checkpoint(actor)["through_sequence"] == 0


async def test_changed_window_or_provider_configuration_can_recover_immediately(store, actor, emit):
    emit(actor, "synthetic first source")
    provider = InvalidSummaryProvider()
    memory = MemoryService(store, provider)
    with pytest.raises(DomainError):
        await memory.compact(actor, force=True)
    emit(actor, "synthetic newly available evidence")
    with pytest.raises(DomainError):
        await memory.compact(actor, force=True)
    provider.configuration_key = "synthetic-fixed-summary-v2"
    with pytest.raises(DomainError):
        await memory.compact(actor, force=True)
    assert provider.calls == 3
    recovered = await MemoryService(store, DeterministicFixtureSummarizer()).compact(
        actor, force=True
    )
    assert recovered is not None and len(recovered.source_event_ids) == 2


async def test_output_capacity_chunks_sources_and_preserves_exact_checkpoint_mapping(
    store, actor, emit
):
    class AliasEchoModel:
        name = "synthetic-alias-echo"

        def __init__(self):
            self.calls = []

        async def complete(self, query):
            sources = json.loads(query.messages[1].content)["events"]
            self.calls.append(sources)
            return ModelReply(
                text=json.dumps(
                    {"text": "Synthetic summary", "covered_event_ids": [s["id"] for s in sources]}
                ),
                usage=TokenUsage(input_tokens=100, output_tokens=50),
            )

    model = AliasEchoModel()
    summarizer = ModelPortSummarizer(model, max_output_tokens=128)
    sources = [emit(actor, f"synthetic source {index}") for index in range(8)]
    memory = MemoryService(store, summarizer)
    first = await memory.compact(actor, force=True)
    assert first is not None
    assert first.source_event_ids == [
        source.id for source in sources[: summarizer.max_source_events]
    ]
    assert memory.checkpoint(actor)["through_sequence"] == first.last_sequence
    second = await memory.compact(actor, force=True)
    assert second is not None
    assert first.source_event_ids + second.source_event_ids == [source.id for source in sources]
    assert memory.checkpoint(actor)["through_sequence"] == second.last_sequence
    assert len(model.calls) == 2
    assert all(source["id"].startswith("e") for window in model.calls for source in window)


async def test_invalid_summary_diagnostics_keep_finish_usage_without_private_model_text(
    store, actor, emit
):
    source = emit(actor, "synthetic private-source-marker")

    class TruncatedModel:
        name = "synthetic-truncated-summary"

        async def complete(self, query):
            return ModelReply(
                text='{"text":"synthetic private-reply-marker"',
                finish_reason="length",
                usage=TokenUsage(input_tokens=200, output_tokens=1500),
            )

    memory = MemoryService(store, ModelPortSummarizer(TruncatedModel()))
    with pytest.raises(DomainError):
        await memory.compact(actor, force=True)
    with store.connect() as conn:
        details = conn.execute(
            "SELECT details FROM ga_audit_events WHERE group_id=%s "
            "AND action='memory_compaction_failed'",
            (actor.group_id,),
        ).fetchone()["details"]
    assert details["reason"] == "incomplete_response" and details["finish_reason"] == "length"
    assert details["usage"] == {"input_tokens": 200, "output_tokens": 1500}
    assert details["max_output_tokens"] == 1500
    assert details["source_count"] == 1
    assert "private-source-marker" not in str(details)
    assert "private-reply-marker" not in str(details)
    assert source.id not in str(details)
