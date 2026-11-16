from __future__ import annotations

import hashlib
from types import SimpleNamespace

import pytest

from groupagent.artifacts import ArtifactScope, DocumentService, Report, Section
from groupagent.domain import DeliveryState, DomainError, WorkState
from groupagent.runtime.contracts import DeliveryDraft
from groupagent.runtime.dispatch import Dispatcher
from groupagent.runtime.executor import WorkSupervisor

from .conftest import source

pytestmark = pytest.mark.postgres


def staged_candidate(env):
    event = source(env, "Produce a synthetic report")
    with env.store.transaction(env.actor) as tx:
        work = tx.create_work("Produce a synthetic report", event.id)
        lease = tx.claim_work(work.id, "synthetic-worker", ttl_seconds=300)
    scope = ArtifactScope(env.actor.group_id, work.id, env.actor.member_id)
    docs = DocumentService(env.artifacts)
    versions = []
    for name, content in (("old.md", "Old synthetic content"), ("new.md", "Repaired content")):
        version = docs.generate(scope, name, Report("Result", (Section("Evidence", (content,)),)))
        validation = docs.validate(
            scope, version.file_id, version.version_id, expected_text=(content,)
        )
        env.registry.register(env.actor, lease, version, validation=validation.to_dict())
        versions.append(version)
    answer = "文件已准备好。"
    answer_draft = DeliveryDraft(
        "text:" + answer,
        hashlib.sha256(answer.encode()).hexdigest(),
        f"answer:{work.id}:{lease.goal_revision}",
    )
    files = tuple(
        DeliveryDraft(
            "artifact:" + version.version_id,
            version.sha256,
            f"artifact:{work.id}:{lease.goal_revision}:{version.version_id}",
        )
        for version in versions
    )
    with env.store.transaction(env.actor) as tx:
        current = tx.get_work(work.id)
        current = tx.transition_work(
            work.id, WorkState.VALIDATING, expected_revision=current.revision, lease=lease
        )
        tx.record_validation(work.id, lease, "synthetic:initial-validation")
        tx.transition_work(
            work.id, WorkState.DELIVERING, expected_revision=current.revision, lease=lease
        )
        original = env.state.enqueue_bundle_in_tx(tx, lease, (answer_draft, files[0]))
    return SimpleNamespace(
        work=work,
        lease=lease,
        scope=scope,
        versions=versions,
        original=original,
        replacement=(answer_draft, files[1]),
    )


def renewed_validation(env, candidate):
    with env.store.transaction(env.actor) as tx:
        current = tx.get_work(candidate.work.id)
        current = tx.transition_work(
            current.id, WorkState.PAUSED, expected_revision=current.revision, reason="worker_lost"
        )
        current = tx.transition_work(
            current.id, WorkState.QUEUED, expected_revision=current.revision, reason="recovery"
        )
        lease = tx.claim_work(current.id, "synthetic-repair-worker", ttl_seconds=300)
        current = tx.get_work(current.id)
        current = tx.transition_work(
            current.id, WorkState.VALIDATING, expected_revision=current.revision, lease=lease
        )
        tx.record_validation(current.id, lease, "synthetic:repaired-validation")
        tx.transition_work(
            current.id, WorkState.DELIVERING, expected_revision=current.revision, lease=lease
        )
    return lease


async def test_repaired_candidate_reuses_accepted_text_and_replaces_only_unsubmitted_file(
    runtime_env, monkeypatch
):
    env = runtime_env
    candidate = staged_candidate(env)
    sent = []
    original_text, original_file = env.channel.send_text, env.channel.send_file

    async def send_text(*args):
        sent.append(("text", args[1]))
        return await original_text(*args)

    async def send_file(*args):
        sent.append(("file", args[1]))
        return await original_file(*args)

    monkeypatch.setattr(env.channel, "send_text", send_text)
    monkeypatch.setattr(env.channel, "send_file", send_file)
    dispatcher = Dispatcher(env.state, env.service, lambda _: env.actor, env.channel, env.registry)
    assert await dispatcher.tick(limit=1) == 1
    with env.store.transaction(env.actor) as tx:
        accepted_answer = tx.get_delivery(candidate.original[0].id)
        assert accepted_answer.state == DeliveryState.ACCEPTED
    # Preparing wire parts alone is not a native submission and remains safe to replace.
    dispatcher._prepare(
        env.actor,
        candidate.original[1],
        dispatcher._payloads(env.actor, candidate.original[1]),
    )
    lease = renewed_validation(env, candidate)
    with env.store.transaction(env.actor) as tx:
        replacement = env.state.enqueue_bundle_in_tx(tx, lease, candidate.replacement)
        assert replacement[0] == accepted_answer
        assert replacement[1].id != candidate.original[1].id
        assert replacement[1].payload_ref == "artifact:" + candidate.versions[1].version_id
        old = tx.get_delivery(candidate.original[1].id)
        assert old.state == DeliveryState.REJECTED
        assert old.reason == "candidate_superseded_before_dispatch"
        assert old.payload_ref == candidate.original[1].payload_ref and old.claim_token == 0
        binding = tx.conn.execute(
            "SELECT intent_id,position FROM ga_delivery_bundles WHERE group_id=%s "
            "AND bundle_id=%s ORDER BY position",
            (env.actor.group_id, f"result:{candidate.work.id}:{lease.goal_revision}"),
        ).fetchall()
        assert binding == [
            {"intent_id": replacement[0].id, "position": 0},
            {"intent_id": replacement[1].id, "position": 1},
        ]
        audit = tx.conn.execute(
            "SELECT details FROM ga_audit_events WHERE group_id=%s AND resource_ref=%s "
            "AND action='delivery_candidate_superseded'",
            (env.actor.group_id, old.id),
        ).fetchone()["details"]
        assert audit["payload_ref"] == old.payload_ref
        assert audit["replacement_payload_ref"] == replacement[1].payload_ref
        assert env.state.enqueue_bundle_in_tx(tx, lease, candidate.replacement) == replacement
    assert await dispatcher.tick() == 1
    assert [kind for kind, _ in sent] == ["text", "file"]
    assert sent[1] == ("file", "new.md")
    payloads = list((env.root / "channel/outgoing").glob("*.payload"))
    assert len(payloads) == 1
    assert payloads[0].read_bytes() == env.artifacts.read(
        candidate.scope, candidate.versions[1].file_id
    )
    with env.store.transaction(env.actor) as tx:
        assert tx.get_delivery(accepted_answer.id) == accepted_answer
        current = tx.get_work(candidate.work.id)
        tx.transition_work(
            current.id, WorkState.COMPLETED, expected_revision=current.revision, lease=lease
        )
    states = WorkSupervisor._delivery_states(
        SimpleNamespace(store=env.store), env.actor, candidate.work
    )
    assert states == ("accepted", "accepted")
    assert await dispatcher.tick() == 0


@pytest.mark.parametrize(
    "native_state",
    ["accepted", "unknown", "dispatching", "claimed", "submitted", "part_accepted", "part_unknown"],
)
def test_candidate_replacement_preserves_any_submitted_or_uncertain_file(runtime_env, native_state):
    env = runtime_env
    candidate = staged_candidate(env)
    old = candidate.original[1]
    with env.store.transaction(env.actor) as tx:
        if native_state in {"accepted", "unknown", "dispatching"}:
            tx.conn.execute(
                "UPDATE ga_deliveries SET state=%s,claim_token=1,worker_id='synthetic-sender',"
                "accepted_at=CASE WHEN %s='accepted' THEN clock_timestamp() ELSE NULL END,"
                "platform_message_id=CASE WHEN %s='accepted' THEN 'synthetic-receipt' ELSE NULL END "
                "WHERE group_id=%s AND id=%s",
                (native_state, native_state, native_state, env.actor.group_id, old.id),
            )
        elif native_state == "claimed":
            tx.conn.execute(
                "UPDATE ga_deliveries SET claim_token=1 WHERE group_id=%s AND id=%s",
                (env.actor.group_id, old.id),
            )
        else:
            part_state = native_state.removeprefix("part_")
            tx.conn.execute(
                "INSERT INTO ga_delivery_parts(group_id,intent_id,position,idempotency_key,"
                "payload_hash,state) VALUES(%s,%s,0,%s,%s,%s)",
                (
                    env.actor.group_id,
                    old.id,
                    "synthetic-part:" + old.id,
                    old.payload_hash,
                    part_state,
                ),
            )
        before = tx.get_delivery(old.id)
    with pytest.raises(DomainError) as caught, env.store.transaction(env.actor) as tx:
        env.state.enqueue_bundle_in_tx(tx, candidate.lease, candidate.replacement)
    assert caught.value.code in {"delivery_unknown", "idempotency_conflict"}
    with env.store.transaction(env.actor) as tx:
        assert tx.get_delivery(old.id) == before
        assert [row.id for row in tx.list_delivery()] == [row.id for row in candidate.original]
        assert tx.conn.execute(
            "SELECT intent_id FROM ga_delivery_bundles WHERE group_id=%s AND bundle_id=%s "
            "ORDER BY position",
            (env.actor.group_id, f"result:{candidate.work.id}:{candidate.lease.goal_revision}"),
        ).fetchall() == [{"intent_id": row.id} for row in candidate.original]
        assert (
            tx.conn.execute(
                "SELECT count(*) AS n FROM ga_audit_events WHERE group_id=%s "
                "AND action='delivery_candidate_superseded'",
                (env.actor.group_id,),
            ).fetchone()["n"]
            == 0
        )


@pytest.mark.parametrize("reason", [None, "cancelled_before_dispatch", "work_stopped"])
def test_other_rejections_still_block_completion(runtime_env, reason):
    env = runtime_env
    candidate = staged_candidate(env)
    with env.store.transaction(env.actor) as tx:
        claimed = tx.claim_delivery("synthetic-sender", intent_id=candidate.original[0].id)
        tx.finish_delivery(
            claimed.id,
            claimed.claim_token,
            DeliveryState.ACCEPTED,
            platform_message_id="synthetic-answer-receipt",
        )
        tx.conn.execute(
            "UPDATE ga_deliveries SET state='rejected',reason=%s WHERE group_id=%s AND id=%s",
            (reason, env.actor.group_id, candidate.original[1].id),
        )
    states = WorkSupervisor._delivery_states(
        SimpleNamespace(store=env.store), env.actor, candidate.work
    )
    assert "rejected" in states
    with pytest.raises(DomainError) as caught, env.store.transaction(env.actor) as tx:
        current = tx.get_work(candidate.work.id)
        tx.transition_work(
            current.id,
            WorkState.COMPLETED,
            expected_revision=current.revision,
            lease=candidate.lease,
        )
    assert caught.value.code == "delivery_incomplete"
