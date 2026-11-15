from __future__ import annotations

import json
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest

from groupagent.domain import DomainError, TeamTaskState
from groupagent.memory import MemoryService
from groupagent.ops import DeletionJournal

pytestmark = pytest.mark.postgres


def test_task_update_and_appended_progress_keep_all_current_field_sources(
    store, memory, actor, emit
):
    original = emit(actor, "I own this task")
    first = emit(actor, "first contribution")
    second = emit(actor, "second contribution")
    with store.transaction(actor) as tx:
        task = tx.create_team_task("independent title", source_event_id=original.id)
        task = tx.update_team_task(
            task.id, expected_revision=1, progress="first contribution", source_event_id=first.id
        )
        task = tx.update_team_task(
            task.id,
            expected_revision=2,
            progress="first contribution; second contribution",
            source_event_id=second.id,
            append_source_fields={"progress"},
        )
    assert set(task.field_sources["progress"]) == {first.id, second.id}
    hit = next(hit for hit in memory.retrieve(actor, "independent") if hit.kind == "team_task")
    assert {ref.event_id for ref in hit.source_refs} == {original.id, first.id, second.id}
    assert hit.source_task_revisions == {task.id: 3} and hit.provenance_kind == "source_bound"
    memory.forget(actor, event_ids=[first.id])
    with store.transaction(actor) as tx:
        current = tx.get_team_task(task.id)
        assert (
            current.content_state == "active"
            and current.title == "independent title"
            and current.progress == ""
        )
        assert (
            first.id not in current.source_event_ids and second.id not in current.source_event_ids
        )
    assert "contribution" not in " ".join(
        hit.text for hit in memory.retrieve(actor) if hit.kind == "team_task"
    )


@pytest.mark.parametrize(
    "field", ["title", "progress", "evidence_refs", "due_at", "status", "owner_id"]
)
def test_forget_update_source_redacts_only_affected_task_fields(
    store, memory, actor, owner, emit, field
):
    initial = emit(actor, "original independent assignment")
    manager = owner if field == "owner_id" else actor
    change = emit(manager, "private update content")
    values = {
        "title": "secret title",
        "progress": "secret progress",
        "evidence_refs": ["secret:proof"],
        "due_at": datetime(2030, 1, 1, tzinfo=UTC),
        "status": TeamTaskState.COMPLETED,
        "owner_id": "synthetic-assignee",
    }
    with store.transaction(actor) as tx:
        task = tx.create_team_task("original title", source_event_id=initial.id)
    with store.transaction(manager) as tx:
        updated = tx.update_team_task(
            task.id,
            expected_revision=task.revision,
            source_event_id=change.id,
            **{field: values[field]},
        )
    assert change.id in updated.field_sources[field]
    result = memory.forget(manager, event_ids=[change.id])
    assert task.id in result.task_ids
    with store.transaction(actor) as tx:
        current = tx.get_team_task(task.id)
        if field == "title":
            assert current.content_state == "forgotten" and not tx.find_team_tasks("secret")
        else:
            assert current.content_state == "active" and current.title == "original title"
            expected = {
                "progress": "",
                "evidence_refs": [],
                "due_at": None,
                "status": TeamTaskState.OPEN,
                "owner_id": None,
            }
            assert getattr(current, field) == expected[field]
        assert change.id not in current.source_event_ids
        assert "private update content" not in current.model_dump_json()


def test_task_source_author_and_availability_are_rechecked(store, memory, actor, owner, emit):
    original = emit(actor, "own assignment")
    other = emit(owner, "someone else source")
    erased = emit(actor, "withdrawn update")
    with store.transaction(actor) as tx:
        task = tx.create_team_task("own task", source_event_id=original.id)
    memory.forget(actor, event_ids=[erased.id])
    for source, code in ((other, "permission_denied"), (erased, "source_unavailable")):
        with pytest.raises(DomainError) as caught:
            with store.transaction(actor) as tx:
                tx.update_team_task(
                    task.id,
                    expected_revision=1,
                    progress="must not save",
                    source_event_id=source.id,
                )
        assert caught.value.code == code


def test_raw_retention_preserves_task_fields_and_all_minimal_expired_sources(
    store, memory, actor, maintenance, emit
):
    old = datetime.now(UTC) - timedelta(days=40)
    creation = emit(actor, "raw creating text", at=old)
    update = emit(actor, "raw progress text", at=old)
    with store.transaction(actor) as tx:
        task = tx.create_team_task("durable human task", source_event_id=creation.id)
        task = tx.update_team_task(
            task.id,
            expected_revision=1,
            progress="durable explicit progress",
            source_event_id=update.id,
        )
    memory.expire(maintenance, before=datetime.now(UTC) - timedelta(days=30))
    hit = next(hit for hit in memory.retrieve(actor, "durable") if hit.kind == "team_task")
    assert {ref.state for ref in hit.source_refs} == {"expired"} and len(hit.source_refs) == 2
    current = emit(actor, "summarize retained duties")
    with store.transaction(actor) as tx:
        work = tx.create_work(
            "summarize duties",
            current.id,
            scope={
                "source_fingerprints": {
                    identifier: tx.event_fingerprint(identifier)
                    for identifier in task.source_event_ids
                },
                "retained_fact_source_ids": task.source_event_ids,
                "source_task_revisions": {task.id: task.revision},
            },
        )
        assert work.scope["source_task_revisions"] == {task.id: task.revision}


def test_independent_task_delete_has_own_journal_tombstone_and_cascades_reply_work_catalog(
    store, actor, maintenance, emit, tmp_path
):
    journal = DeletionJournal(tmp_path / "deletions")
    memory = MemoryService(store, deletion_sink=journal)
    question = emit(actor, "summarize my independent task")
    work_request = emit(actor, "process this independent record")
    with store.transaction(actor) as tx:
        task = tx.create_team_task("legacy-private-title")
        work = tx.create_work(
            "derived legacy-private-title",
            work_request.id,
            scope={"source_task_revisions": {task.id: task.revision}},
        )
        tx.record_decision(
            question.id,
            {
                "kind": "reply",
                "source_task_revisions": {task.id: task.revision},
                "result": {"response": "legacy-private-title"},
            },
        )
        reply = tx.enqueue_reply(question.id, "legacy-private-title")
        tx.conn.execute(
            "INSERT INTO ga_artifact_catalog(group_id,version_id,file_id,work_id,created_by,name,format,sha256,byte_length,body,validation) VALUES(%s,'fixture-version','fixture-file',%s,%s,'private.md','md',%s,1,'{}','{\"passed\":true}')",
            (actor.group_id, work.id, actor.member_id, "a" * 64),
        )
    result = memory.forget(actor, task_ids=[task.id])
    assert result.task_ids == [task.id]
    marker = journal.load(actor.group_id)[0]
    assert marker.task_id == task.id and marker.event_id is None
    assert any(
        job.resource_kind == "work_artifacts" and job.work_id == work.id
        for job in memory.pending_cleanup(maintenance)
    )
    with store.transaction(actor) as tx:
        assert tx.get_team_task(task.id).content_state == "forgotten"
        assert tx.get_decision(question.id)["privacy_redacted"]
        assert tx.get_delivery(reply.id).payload_ref == "text:[content removed]"
        assert tx.get_work(work.id).scope["privacy_redacted"]
        assert not tx.conn.execute(
            "SELECT available FROM ga_artifact_catalog WHERE group_id=%s", (actor.group_id,)
        ).fetchone()["available"]
        assert "legacy-private-title" not in json.dumps(tx.get_decision(question.id))
        assert tx.list_team_tasks() == []


def test_null_source_legacy_record_has_no_fabricated_author_or_event_and_requires_owner_delete(
    store, memory, actor, owner
):
    identifier = uuid4().hex
    with store.transaction(owner) as tx:
        tx.conn.execute(
            "INSERT INTO ga_team_tasks(id,group_id,title,owner_id,assignment_kind) VALUES(%s,%s,'synthetic legacy duty',%s,'legacy_import')",
            (identifier, actor.group_id, actor.member_id),
        )
    hit = next(hit for hit in memory.retrieve(actor, "legacy") if hit.kind == "team_task")
    assert hit.provenance_kind == "legacy_unattributed" and hit.source_refs == []
    assert hit.source_task_revisions == {identifier: 1}
    with pytest.raises(DomainError) as caught:
        memory.forget(actor, task_ids=[identifier])
    assert caught.value.code == "permission_denied"
    assert memory.forget(owner, task_ids=[identifier]).task_ids == [identifier]


def test_task_update_invalidates_inflight_snapshot_and_stale_work_admission(
    store, memory, actor, emit
):
    initial = emit(actor, "task source")
    update = emit(actor, "changed progress")
    with store.transaction(actor) as tx:
        task = tx.create_team_task("stable task", source_event_id=initial.id)
    snapshot = memory.query_context(actor, "stable")
    with store.transaction(actor) as tx:
        tx.update_team_task(task.id, expected_revision=1, progress="new", source_event_id=update.id)
    with pytest.raises(DomainError) as caught:
        with store.transaction(actor) as tx:
            memory.assert_snapshot_in_tx(tx, snapshot)
    assert caught.value.code == "memory_changed"
    with pytest.raises(DomainError) as caught:
        with store.transaction(actor) as tx:
            tx.create_work(
                "stale summary", update.id, scope={"source_task_revisions": {task.id: 1}}
            )
    assert caught.value.code == "memory_changed"


def test_task_update_concurrency_keeps_only_winning_source_and_revision(store, actor, emit):
    first = emit(actor, "creation")
    changes = [emit(actor, "candidate " + str(index)) for index in range(4)]
    with store.transaction(actor) as tx:
        task = tx.create_team_task("concurrent", source_event_id=first.id)

    def update(source):
        try:
            with store.transaction(actor) as tx:
                result = tx.update_team_task(
                    task.id, expected_revision=1, progress=source.text, source_event_id=source.id
                )
                return source.id, result.revision
        except DomainError as error:
            assert error.code == "version_conflict"
            return None

    with ThreadPoolExecutor(max_workers=4) as pool:
        winners = [result for result in pool.map(update, changes) if result is not None]
    assert len(winners) == 1 and winners[0][1] == 2
    with store.transaction(actor) as tx:
        final = tx.get_team_task(task.id)
        assert set(final.field_sources["progress"]) == {winners[0][0]}


def test_task_provenance_mutation_rolls_back_with_coordinator_transaction(
    store, memory, actor, emit
):
    start = emit(actor, "create")
    update = emit(actor, "sensitive new progress")
    with store.transaction(actor) as tx:
        task = tx.create_team_task("atomic task", source_event_id=start.id)
    with pytest.raises(RuntimeError):
        with store.transaction(actor) as tx:
            memory.prepare_in_tx(tx)
            tx.update_team_task(
                task.id, expected_revision=1, progress=update.text, source_event_id=update.id
            )
            tx.record_decision(update.id, {"kind": "reply"})
            raise RuntimeError("synthetic crash")
    with store.transaction(actor) as tx:
        assert tx.get_team_task(task.id).revision == 1 and tx.get_team_task(task.id).progress == ""
        assert update.id not in tx.get_team_task(task.id).source_event_ids
        assert tx.get_decision(update.id) is None


def test_work_admission_serializes_with_task_forget_even_without_event_fingerprints(
    store, memory, actor, emit
):
    from concurrent.futures import TimeoutError
    from threading import Event

    request = emit(actor, "use an independent task")
    with store.transaction(actor) as tx:
        task = tx.create_team_task("independent sensitive business value")
    entered = Event()

    def admit():
        entered.set()
        try:
            with store.transaction(actor) as tx:
                return tx.create_work(
                    "must not retain forgotten content",
                    request.id,
                    scope={"source_task_revisions": {task.id: 1}},
                )
        except DomainError as error:
            return error.code

    with ThreadPoolExecutor(max_workers=1) as pool:
        with store.transaction(actor) as tx:
            memory.prepare_in_tx(tx)
            memory.in_tx(tx).forget(actor, task_ids=[task.id])
            future = pool.submit(admit)
            assert entered.wait(timeout=1)
            with pytest.raises(TimeoutError):
                future.result(timeout=0.05)
        assert future.result(timeout=2) == "source_unavailable"
    with store.transaction(actor) as tx:
        assert tx.list_work() == []


def test_revised_task_sources_are_saved_with_goal_revision_history(store, actor, emit):
    source = emit(actor, "current task")
    change = emit(actor, "updated task")
    with store.transaction(actor) as tx:
        task = tx.create_team_task("a task", source_event_id=source.id)
        work = tx.create_work(
            "first summary", change.id, scope={"source_task_revisions": {task.id: 1}}
        )
        tx.update_team_task(
            task.id, expected_revision=1, progress="new progress", source_event_id=change.id
        )
        revised = tx.revise_work(
            work.id, "current summary", expected_revision=1, source_task_revisions={task.id: 2}
        )
        history = tx.conn.execute(
            "SELECT body FROM ga_work_revisions WHERE group_id=%s AND work_id=%s AND goal_revision=2",
            (actor.group_id, work.id),
        ).fetchone()["body"]
        assert (
            revised.scope["source_task_revisions"]
            == history["scope"]["source_task_revisions"]
            == {task.id: 2}
        )


def test_forgetting_own_claim_does_not_delete_unattributed_legacy_title(store, memory, actor, emit):
    identity = uuid4().hex
    source = emit(actor, "I claim the unassigned task")
    with store.transaction(actor) as tx:
        tx.conn.execute(
            "INSERT INTO ga_team_tasks(id,group_id,title,owner_id,assignment_kind) VALUES(%s,%s,'independent legacy assignment',NULL,'unassigned')",
            (identity, actor.group_id),
        )
        claimed = tx.claim_team_task(identity, expected_revision=1, source_event_id=source.id)
        assert claimed.field_sources["owner_id"] == [source.id]
    memory.forget(actor, event_ids=[source.id])
    with store.transaction(actor) as tx:
        task = tx.get_team_task(identity)
        assert task.title == "independent legacy assignment" and task.content_state == "active"
        assert task.owner_id is None and task.status == TeamTaskState.OPEN
        assert task.source_event_ids == []
    with pytest.raises(DomainError) as caught:
        memory.forget(actor, task_ids=[identity])
    assert caught.value.code == "permission_denied"


def test_independent_field_replacement_survives_forget_of_old_contribution(
    store, memory, actor, emit
):
    original = emit(actor, "task created")
    first = emit(actor, "old progress")
    second = emit(actor, "completely independent replacement")
    with store.transaction(actor) as tx:
        task = tx.create_team_task("stable task title", source_event_id=original.id)
        tx.update_team_task(
            task.id, expected_revision=1, progress="old progress", source_event_id=first.id
        )
        task = tx.update_team_task(
            task.id,
            expected_revision=2,
            progress="independent replacement",
            source_event_id=second.id,
        )
    assert task.field_sources["progress"] == [second.id]
    memory.forget(actor, event_ids=[first.id])
    with store.transaction(actor) as tx:
        current = tx.get_team_task(task.id)
        assert current.progress == "independent replacement" and current.revision == 3


def test_future_progress_forget_preserves_unrelated_old_task_artifact_but_revokes_actual_consumer(
    store, memory, actor, emit, tmp_path
):
    from groupagent.artifacts import ArtifactScope, ArtifactStore
    from groupagent.runtime.artifacts import ArtifactRegistry

    artifacts = ArtifactStore(tmp_path / "artifacts")
    registry = ArtifactRegistry(store, artifacts)
    before = emit(actor, "produce output from original independent task")
    with store.transaction(actor) as tx:
        task = tx.create_team_task("stable independent title")
        old_work = tx.create_work(
            "old task summary", before.id, scope={"source_task_revisions": {task.id: 1}}
        )
    version = artifacts.create(
        ArtifactScope(actor.group_id, old_work.id, actor.member_id),
        "before.md",
        b"fixed historical result",
    )
    with store.transaction(actor) as tx:
        from psycopg.types.json import Jsonb

        tx.conn.execute(
            "INSERT INTO ga_artifact_catalog(group_id,version_id,file_id,work_id,created_by,name,format,sha256,byte_length,body,validation,source_event_ids,source_fingerprints) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            (
                actor.group_id,
                version.version_id,
                version.file_id,
                old_work.id,
                actor.member_id,
                version.name,
                version.format,
                version.sha256,
                version.size_bytes,
                Jsonb(version.to_dict()),
                Jsonb({"passed": True, "sha256": version.sha256}),
                [before.id],
                Jsonb({before.id: tx.event_fingerprint(before.id)}),
            ),
        )
    private = emit(actor, "future private progress that was never in the old document")
    after = emit(actor, "summarize the latest task progress")
    copy_request = emit(actor, "reuse the fixed earlier document without current task progress")
    with store.transaction(actor) as tx:
        task = tx.update_team_task(
            task.id,
            expected_revision=1,
            progress="future private progress",
            source_event_id=private.id,
        )
        new_work = tx.create_work(
            "future private progress summary",
            after.id,
            scope={"source_task_revisions": {task.id: 2}},
        )
        assert private.id in new_work.scope["source_fingerprints"]
        historical_copy = tx.create_work(
            "fixed older document copy",
            copy_request.id,
            input_versions=[version.version_id],
            scope={
                "source_task_revisions": {task.id: 2},
                "source_fingerprints": {before.id: tx.event_fingerprint(before.id)},
            },
        )
        assert private.id not in historical_copy.scope["source_fingerprints"]
    memory.forget(actor, event_ids=[private.id])
    assert (
        registry.read(actor, version.version_id, require_validated=True)[1]
        == b"fixed historical result"
    )
    with store.transaction(actor) as tx:
        assert tx.get_work(old_work.id).scope.get("privacy_redacted") is not True
        assert tx.get_work(new_work.id).scope["privacy_redacted"] is True
        assert tx.get_work(historical_copy.id).scope.get("privacy_redacted") is not True
    memory.forget(actor, task_ids=[task.id])
    with pytest.raises(DomainError) as caught:
        registry.read(actor, version.version_id)
    assert caught.value.code == "source_unavailable"


def test_repeating_own_claim_cannot_replace_owner_assignment_authority(
    store, memory, actor, owner, emit
):
    assignment = emit(owner, "owner assigns this duty to the member")
    acknowledgement = emit(actor, "acknowledge my existing duty")
    with store.transaction(owner) as tx:
        task = tx.create_team_task(
            "owner assigned duty", owner_id=actor.member_id, source_event_id=assignment.id
        )
    with store.transaction(actor) as tx:
        unchanged = tx.claim_team_task(
            task.id, expected_revision=1, source_event_id=acknowledgement.id
        )
        assert unchanged.assignment_kind == "owner_assigned" and unchanged.revision == 1
        assert unchanged.field_sources["owner_id"] == [assignment.id]
    with pytest.raises(DomainError) as caught:
        with store.transaction(actor) as tx:
            tx.update_team_task(
                task.id,
                expected_revision=1,
                owner_id=actor.member_id,
                source_event_id=acknowledgement.id,
            )
    assert caught.value.code == "permission_denied"
    memory.forget(actor, event_ids=[acknowledgement.id])
    with store.transaction(actor) as tx:
        assert tx.get_team_task(task.id).owner_id == actor.member_id
        assert tx.get_team_task(task.id).assignment_kind == "owner_assigned"


def test_owner_assignment_uses_owner_request_not_another_members_message(store, actor, owner, emit):
    member_message = emit(actor, "ordinary member context")
    with pytest.raises(DomainError) as caught:
        with store.transaction(owner) as tx:
            tx.create_team_task(
                "must use actual owner request",
                owner_id=actor.member_id,
                source_event_id=member_message.id,
            )
    assert caught.value.code == "permission_denied"
