from __future__ import annotations

from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest

from groupagent.domain import EventKind, GroupEvent
from groupagent.memory import MemoryService

pytestmark = pytest.mark.postgres


def event(store, member, text, *, at=None):
    stamp = at or datetime.now(UTC)
    identity = uuid4().hex
    result = GroupEvent(
        id=identity,
        source_key="revision:" + identity,
        group_id=member.group_id,
        actor_id=member.member_id,
        kind=EventKind.TEXT,
        text=text,
        occurred_at=stamp,
        received_at=stamp,
    )
    with store.transaction(member) as tx:
        tx.ingest_event(result, identity, "1", 0)
    return result


def test_output_requirement_revision_updates_acceptance_without_expanding_scope_or_budget(
    store, member
):
    source = event(store, member, "original request")
    original_scope = {
        "allowed_group_ids": [member.group_id],
        "public_web_read": False,
        "outputs": [{"format": "pptx", "pages": 8}],
        "delivery_artifacts": True,
    }
    budget = {"max_model_rounds": 4, "max_total_tokens": 1000}
    with store.transaction(member) as tx:
        work = tx.create_work("eight slides", source.id, scope=original_scope, budget=budget)
        updated = tx.revise_work(
            work.id,
            "ten-page PDF",
            expected_revision=1,
            output_requirements=[{"format": "pdf", "pages": 10}],
            required_checks=["result", "render"],
            delivery_artifacts=True,
            speech_requested=False,
        )
        assert updated.scope["outputs"] == [{"format": "pdf", "pages": 10}]
        assert updated.scope["public_web_read"] is False
        assert updated.scope["allowed_group_ids"] == [member.group_id]
        assert updated.budget == budget and updated.required_checks == ["result", "render"]
        revision = tx.conn.execute(
            "SELECT body FROM ga_work_revisions WHERE group_id=%s AND work_id=%s AND goal_revision=2",
            (member.group_id, work.id),
        ).fetchone()["body"]
        assert (
            revision["scope"] == updated.scope
            and revision["required_checks"] == updated.required_checks
        )


def test_independent_human_task_authorizes_minimal_expired_source_provenance(store, actor, member):
    now = datetime.now(UTC)
    old = event(store, member, "human task source", at=now - timedelta(days=40))
    with store.transaction(member) as tx:
        tx.create_team_task("retained human responsibility", source_event_id=old.id)
    MemoryService(store).expire(actor, before=now - timedelta(days=30), now=now)
    current = event(store, member, "summarize my existing duties")
    with store.transaction(member) as tx:
        work = tx.create_work("initial", current.id)
        updated = tx.revise_work(
            work.id,
            "summarize retained duties",
            expected_revision=1,
            source_fingerprints={old.id: tx.event_fingerprint(old.id)},
            retained_fact_source_ids=[old.id],
        )
        assert updated.scope["retained_fact_source_ids"] == [old.id]


@pytest.mark.parametrize(
    "evidence",
    [
        "valid",
        "invalid_validation",
        "unavailable",
        "expired",
        "foreign_actor",
        "not_selected",
        "forgotten",
    ],
)
def test_retained_document_evidence_never_reopens_deleted_raw_source(
    store, actor, member, evidence
):
    from psycopg.types.json import Jsonb

    from groupagent.domain import DomainError

    now = datetime.now(UTC)
    old = event(store, member, "raw original", at=now - timedelta(days=40))
    with store.transaction(member) as tx:
        original = tx.create_work("original output job", old.id)
        tx.conn.execute(
            "INSERT INTO ga_artifact_catalog(group_id,version_id,file_id,work_id,created_by,name,format,sha256,byte_length,body,validation,source_event_ids,available,expires_at) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            (
                member.group_id,
                "synthetic-version",
                "synthetic-file",
                original.id,
                "other-member" if evidence == "foreign_actor" else member.member_id,
                "retained.md",
                "md",
                "a" * 64,
                7,
                Jsonb({"fixture_only": True}),
                Jsonb({"passed": evidence != "invalid_validation"}),
                [old.id],
                evidence != "unavailable",
                now + timedelta(days=-1 if evidence == "expired" else 90),
            ),
        )
    memory = MemoryService(store)
    if evidence == "forgotten":
        memory.forget(member, event_ids=[old.id])
    else:
        memory.expire(actor, before=now - timedelta(days=30), now=now)
    current = event(store, member, "reuse this validated document")
    with store.transaction(member) as tx:
        work = tx.create_work("initial", current.id)
        fingerprint = tx.event_fingerprint(old.id)

    def revise():
        with store.transaction(member) as tx:
            return tx.revise_work(
                work.id,
                "summarize retained output",
                expected_revision=1,
                input_versions=[] if evidence == "not_selected" else ["synthetic-version"],
                source_fingerprints={old.id: fingerprint},
                retained_fact_source_ids=[old.id],
            )

    if evidence == "valid":
        revised = revise()
        assert revised.scope["retained_fact_source_ids"] == [old.id]
        with store.transaction(member) as tx:
            assert tx.get_event(old.id).text == ""
    else:
        with pytest.raises(DomainError) as caught:
            revise()
        assert caught.value.code == "source_unavailable"
