from __future__ import annotations

import pytest

from groupagent.domain import EventKind

pytestmark = pytest.mark.postgres


@pytest.mark.parametrize("kind", [EventKind.SYSTEM, EventKind.RECALL, EventKind.MEMBERSHIP])
async def test_control_metadata_never_enters_raw_rag_or_compaction(
    store, memory, actor, emit, kind
):
    internal = emit(actor, "INTERNAL BILLING METADATA MUST NOT BE CONVERSATION", kind=kind)
    conversation = emit(actor, "real conversation source")
    hits = memory.retrieve(actor, include_tasks=False)
    assert [hit.id for hit in hits] == [conversation.id]
    summary = await memory.compact(actor, force=True)
    assert summary is not None and summary.source_event_ids == [conversation.id]
    assert "INTERNAL BILLING" not in summary.text
    assert memory.summarizer.calls[0].events[0]["id"] == conversation.id
    with store.transaction(actor) as tx:
        assert tx.get_event(internal.id).text.startswith("INTERNAL")
