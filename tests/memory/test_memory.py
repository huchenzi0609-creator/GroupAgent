from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from threading import Barrier
from uuid import uuid4

import pytest

from groupagent.domain import DomainError, EventKind, Role

pytestmark = pytest.mark.postgres


@pytest.mark.parametrize(
    "kind",
    [
        EventKind.TEXT,
        EventKind.IMAGE,
        EventKind.AUDIO,
        EventKind.FILE,
        EventKind.QUOTE,
        EventKind.FORWARDED,
        EventKind.UNSUPPORTED_VIDEO,
    ],
)
def test_source_retrieval_preserves_author_kind_and_scope(memory, actor, emit, kind):
    source = emit(actor, "项目乙 参考资料", kind=kind)
    hits = memory.retrieve(actor, "项目乙", include_tasks=False)
    assert len(hits) == 1 and hits[0].kind == "event" and hits[0].id == source.id
    assert hits[0].source_refs[0].actor_id == actor.member_id
    assert hits[0].source_refs[0].state == "active"


def test_group_isolation_and_source_guard(store, memory, actor, emit):
    source = emit(actor, "private-group-marker")
    outsider = actor.model_copy(update={"group_id": "outside-" + uuid4().hex})
    store.register_group(outsider.group_id)
    assert memory.retrieve(outsider, "private-group-marker") == []
    assert not memory.source_available(outsider, source.id)


def test_topics_use_native_quote_evidence_and_keep_two_threads_separate(memory, actor, emit):
    alpha = emit(actor, "alpha proposal")
    beta = emit(actor, "beta proposal")
    a = memory.create_topic(actor, "alpha")
    b = memory.create_topic(actor, "beta")
    memory.link_event(actor, alpha.id, a.id, evidence_event_ids=[alpha.id])
    memory.link_event(actor, beta.id, b.id, evidence_event_ids=[beta.id])
    follow = emit(actor, "shorten my proposal", reply_to=alpha.id)
    memory.link_event(actor, follow.id, a.id, evidence_event_ids=[alpha.id])
    assert {hit.id for hit in memory.retrieve(actor, topic_id=a.id, include_tasks=False)} == {
        alpha.id,
        follow.id,
    }
    assert [topic.id for topic in memory.topics_for_event(actor, follow.id)] == [a.id]
    with pytest.raises(DomainError) as caught:
        memory.link_event(actor, beta.id, a.id, evidence_event_ids=[alpha.id])
    assert caught.value.code == "ambiguous_reference"


@pytest.mark.parametrize("role", [Role.MEMBER, Role.OBSERVER, Role.MAINTAINER])
async def test_restricted_topic_is_hidden_from_other_members(memory, actor, emit, role):
    source = emit(actor, "restricted secret")
    topic = memory.create_topic(actor, "restricted", visibility="participants")
    memory.link_event(actor, source.id, topic.id, evidence_event_ids=[source.id])
    await memory.compact(actor, topic_id=topic.id, force=True)
    other = actor.model_copy(update={"member_id": "member-b", "role": role})
    assert memory.retrieve(other, "restricted secret", include_tasks=False) == []
    with pytest.raises(DomainError) as caught:
        memory.retrieve(other, topic_id=topic.id)
    assert caught.value.code == "permission_denied"


async def test_shared_summary_cannot_absorb_private_topic(memory, actor, owner, emit):
    source = emit(actor, "private-private")
    topic = memory.create_topic(actor, "restricted", visibility="participants")
    memory.link_event(actor, source.id, topic.id, evidence_event_ids=[source.id])
    assert await memory.compact(owner, force=True) is None
    assert memory.summarizer.calls == []


def test_retrieval_matches_literal_sql_wildcards(memory, actor, emit):
    wanted = emit(actor, "progress 50%_done")
    emit(actor, "progress unrelated")
    assert [hit.id for hit in memory.retrieve(actor, "%_", include_tasks=False)] == [wanted.id]


def test_recent_context_uses_event_time_while_keyword_search_reaches_old_sources(
    memory, actor, emit
):
    now = datetime.now(UTC)
    old = emit(actor, "old-search-marker", at=now - timedelta(days=1))
    recent = emit(actor, "newer discussion", at=now)
    late_backfill = emit(actor, "backfilled ancient discussion", at=now - timedelta(days=2))
    assert [hit.id for hit in memory.retrieve(actor, limit=1, include_tasks=False)] == [recent.id]
    assert [hit.id for hit in memory.retrieve(actor, "old-search-marker", include_tasks=False)] == [
        old.id
    ]
    assert late_backfill.id != recent.id


def test_self_facts_never_merge_two_members(memory, actor, emit):
    other = actor.model_copy(update={"member_id": "member-b"})
    first, second = emit(actor, "我负责PPT"), emit(other, "我负责文献")
    a = memory.record_fact(
        actor, "responsibility", "PPT", source_event_ids=[first.id], explicit=True
    )
    b = memory.record_fact(
        other, "responsibility", "literature", source_event_ids=[second.id], explicit=True
    )
    assert a.subject_id == actor.member_id and b.subject_id == other.member_id
    assert a.id != b.id
    with pytest.raises(DomainError) as caught:
        memory.record_fact(
            actor,
            "responsibility",
            "override",
            subject_id=other.member_id,
            source_event_ids=[first.id],
            explicit=True,
        )
    assert caught.value.code == "permission_denied"


@pytest.mark.parametrize("explicit", [False, True])
def test_only_explicit_statements_become_long_term_decisions(memory, actor, emit, explicit):
    source = emit(actor, "We might use proposal A")
    if explicit:
        fact = memory.record_fact(
            actor,
            "chosen_proposal",
            "A",
            kind="decision",
            source_event_ids=[source.id],
            explicit=True,
        )
        assert fact.kind == "decision" and fact.subject_id is None
    else:
        with pytest.raises(DomainError) as caught:
            memory.record_fact(
                actor,
                "chosen_proposal",
                "A",
                kind="decision",
                source_event_ids=[source.id],
                explicit=False,
            )
        assert caught.value.code == "invalid_input"


async def test_fact_correction_invalidates_summary_and_keeps_audit_history(
    store, memory, actor, emit
):
    original = emit(actor, "Deadline is Friday")
    fact = memory.record_fact(
        actor, "deadline", "Friday", source_event_ids=[original.id], explicit=True
    )
    summary = await memory.compact(actor, force=True)
    assert summary is not None
    correction = emit(actor, "Correction: deadline is Monday")
    current = memory.correct_fact(
        actor,
        fact.id,
        "Monday",
        expected_revision=1,
        source_event_ids=[correction.id],
        explicit=True,
    )
    assert current.revision == 2 and current.value == "Monday"
    assert not any(
        hit.kind == "summary" for hit in memory.retrieve(actor, "Friday", include_tasks=False)
    )
    with store.connect() as conn:
        rows = conn.execute(
            "SELECT revision,state FROM ga_fact_revisions WHERE group_id=%s AND fact_id=%s ORDER BY revision",
            (actor.group_id, fact.id),
        ).fetchall()
        assert rows == [{"revision": 1, "state": "superseded"}, {"revision": 2, "state": "active"}]


@pytest.mark.parametrize(
    "role,own,allowed",
    [
        (Role.MEMBER, True, True),
        (Role.MEMBER, False, False),
        (Role.OWNER, False, True),
        (Role.OBSERVER, True, False),
        (Role.MAINTAINER, True, False),
    ],
)
def test_fact_correction_permission_boundary(memory, actor, emit, role, own, allowed):
    source = emit(actor, "fact source")
    fact = memory.record_fact(actor, "answer", 1, source_event_ids=[source.id], explicit=True)
    editor = actor.model_copy(
        update={"role": role, "member_id": actor.member_id if own else "other"}
    )
    if allowed:
        assert (
            memory.correct_fact(
                editor, fact.id, 2, expected_revision=1, source_event_ids=[source.id], explicit=True
            ).value
            == 2
        )
    else:
        with pytest.raises(DomainError) as caught:
            memory.correct_fact(
                editor, fact.id, 2, expected_revision=1, source_event_ids=[source.id], explicit=True
            )
        assert caught.value.code == "permission_denied"


def test_concurrent_fact_correction_has_one_revision_winner(memory, actor, emit):
    source = emit(actor, "baseline")
    fact = memory.record_fact(actor, "result", 0, source_event_ids=[source.id], explicit=True)
    barrier = Barrier(2)

    def change(value):
        barrier.wait()
        try:
            return memory.correct_fact(
                actor,
                fact.id,
                value,
                expected_revision=1,
                source_event_ids=[source.id],
                explicit=True,
            ).revision
        except DomainError as error:
            return error.code

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(change, [1, 2]))
    assert results.count(2) == 1 and results.count("version_conflict") == 1


def test_fact_replay_cannot_overwrite_or_revive_invalidated_fact(memory, actor, emit):
    source = emit(actor, "stable statement")
    fact = memory.record_fact(
        actor, "answer", 1, source_event_ids=[source.id], explicit=True, request_key="event:fact"
    )
    assert (
        memory.record_fact(
            actor,
            "answer",
            1,
            source_event_ids=[source.id],
            explicit=True,
            request_key="event:fact",
        ).id
        == fact.id
    )
    with pytest.raises(DomainError) as caught:
        memory.record_fact(
            actor,
            "answer",
            2,
            source_event_ids=[source.id],
            explicit=True,
            request_key="event:fact",
        )
    assert caught.value.code == "idempotency_conflict"
    memory.invalidate_fact(actor, fact.id, expected_revision=1)
    with pytest.raises(DomainError) as caught:
        memory.record_fact(
            actor,
            "answer",
            1,
            source_event_ids=[source.id],
            explicit=True,
            request_key="event:fact",
        )
    assert caught.value.code == "source_unavailable"


def test_memory_business_mutation_and_decision_share_one_transaction(store, memory, actor, emit):
    source = emit(actor, "explicit shared fact")
    with pytest.raises(RuntimeError):
        with store.transaction(actor) as tx:
            memory.prepare_in_tx(tx)
            assert tx.get_decision(source.id) is None
            memory.in_tx(tx).record_fact(
                actor, "answer", 42, source_event_ids=[source.id], explicit=True
            )
            tx.record_decision(source.id, {"kind": "observe"})
            raise RuntimeError("crash before atomic decision commit")
    assert not any(hit.kind == "fact" for hit in memory.retrieve(actor))
    with store.transaction(actor) as tx:
        assert tx.get_decision(source.id) is None


def test_tasks_survive_raw_retention_and_remain_human_tasks(
    store, memory, maintenance, actor, emit
):
    now = datetime.now(UTC)
    source = emit(actor, "我负责PPT，不是请Agent制作", at=now - timedelta(days=40))
    with store.transaction(actor) as tx:
        task = tx.create_team_task("PPT", source_event_id=source.id)
    memory.expire(maintenance, before=now - timedelta(days=30), now=now)
    with store.transaction(actor) as tx:
        assert tx.list_team_tasks(owner_id=actor.member_id)[0].id == task.id
        assert tx.find_team_tasks("PPT")[0].owner_id == actor.member_id
        assert tx.list_work() == []
    hits = memory.retrieve(actor, "PPT")
    assert (
        len(hits) == 1 and hits[0].kind == "team_task" and hits[0].source_refs[0].state == "expired"
    )


def test_explicit_fact_retains_minimal_evidence_after_raw_expiry(memory, maintenance, actor, emit):
    now = datetime.now(UTC)
    source = emit(actor, "raw chat exact phrase", at=now - timedelta(days=40))
    fact = memory.record_fact(
        actor, "project_deadline", "2030-01-01", source_event_ids=[source.id], explicit=True
    )
    memory.expire(maintenance, before=now - timedelta(days=30), now=now)
    assert memory.get_fact(actor, fact.id).value == "2030-01-01"
    assert memory.retrieve(actor, "raw chat exact phrase", include_tasks=False) == []
    hit = memory.retrieve(actor, "project_deadline", include_tasks=False)[0]
    assert hit.source_refs[0].event_id == source.id and hit.source_refs[0].state == "expired"
