"""Pure instruction history is transient; raw text cannot re-enter durable resumes."""

import hashlib
import json
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest

from groupagent.domain import DomainError, EventKind, GroupEvent, WorkState
from groupagent.execution import ModelReply, TokenUsage
from groupagent.memory import MemoryService
from groupagent.runtime.executor import WorkSupervisor
from groupagent.runtime.toolkit import _unindexed_human_history

from .conftest import source
from .test_toolkit_contract import candidate
from .test_toolkit_contract import toolkit_setup as toolkit_setup

pytestmark = pytest.mark.postgres
RAW = "SYNTHETIC_PRIVATE_HISTORICAL_INSTRUCTION"
REASON = "Synthetic correction cites " + RAW


def create_work(setup, *, indexed=False):
    env = setup.env
    original = source(env, "Give the authorized short explanation")
    old = datetime.now(UTC) - timedelta(days=31)
    history = GroupEvent(
        id=uuid4().hex,
        source_key=uuid4().hex,
        group_id=env.actor.group_id,
        actor_id=env.actor.member_id,
        kind=EventKind.TEXT,
        text=RAW,
        occurred_at=old,
        received_at=datetime.now(UTC),
    )
    with env.store.transaction(env.actor) as tx:
        tx.ingest_event(history, history.source_key, "1", 0)
        fingerprints = {original.id: tx.event_fingerprint(original.id)}
        if indexed:
            fingerprints[history.id] = tx.event_fingerprint(history.id)
        work = tx.create_work(
            "Produce the currently authorized derived explanation",
            original.id,
            scope={
                "source_fingerprints": fingerprints,
                "human_request_event_ids": [original.id, history.id],
                "human_request_fingerprints": {
                    original.id: tx.event_fingerprint(original.id),
                    history.id: tx.event_fingerprint(history.id),
                },
                "human_request_history_complete": True,
            },
        )
    return work, history


@pytest.mark.parametrize("indexed", [False, True])
async def test_only_unindexed_history_withholds_free_diagnostics(
    toolkit_setup, monkeypatch, indexed
):
    setup, env = toolkit_setup, toolkit_setup.env
    work, _ = create_work(setup, indexed=indexed)
    with env.store.transaction(env.actor) as tx:
        lease = tx.claim_work(work.id, "synthetic-history-validator", ttl_seconds=300)
        work = tx.get_work(work.id)
    workspace = env.root / work.id
    workspace.mkdir()
    prepared = await setup.toolkit.prepare(work, lease, env.actor, workspace)
    assert RAW not in prepared.request.objective
    assert "human_requirements" not in json.loads(prepared.request.objective)
    assert json.loads(prepared.request.objective)["human_requirement_refs"]["references"]

    async def reviewer(request):
        assert RAW in request.messages[1].content  # Transient, source-checked critic input only.
        return ModelReply(
            text=json.dumps(
                {
                    "content_passed": True,
                    "group_reply_passed": False,
                    "reason": REASON,
                    "reason_withheld": False,
                }
            ),
            usage=TokenUsage(input_tokens=1, output_tokens=1),
        )

    monkeypatch.setattr(setup.model, "complete", reviewer)
    run = type("Run", (), {"prepared": prepared, "work": work, "lease": lease})()
    with pytest.raises(DomainError) as caught:
        await setup.toolkit.validate(work, lease, env.actor, candidate(run), workspace)
    assert (RAW in caught.value.message) is indexed
    with env.store.transaction(env.actor) as tx:
        record = tx.get_work(work.id).scope["_runtime_semantic_reviews"][-1]
        assert (RAW in record["reason"]) is indexed
        assert record["reason_sha256"] == hashlib.sha256(REASON.encode()).hexdigest()
        assert record["reason_withheld"] is not indexed
    assert _unindexed_human_history(work) is not indexed


async def test_failure_feedback_checkpoint_and_ttl_resume_never_copy_pure_history(
    toolkit_setup, monkeypatch
):
    setup, env = toolkit_setup, toolkit_setup.env
    work, history = create_work(setup)
    reviews, generations = [], []

    async def model(request):
        if request.messages[0].content.startswith("你是独立结果检查器"):
            payload = json.loads(request.messages[1].content)
            reviews.append(payload)
            return ModelReply(
                text=json.dumps(
                    {
                        "content_passed": True,
                        "group_reply_passed": len(reviews) > 1,
                        "reason": REASON,
                    }
                ),
                usage=TokenUsage(input_tokens=1, output_tokens=1),
            )
        generations.append(request)
        assert RAW not in request.model_dump_json()
        return ModelReply(
            text="The authorized concise synthetic explanation.",
            usage=TokenUsage(input_tokens=1, output_tokens=1),
        )

    monkeypatch.setattr(setup.model, "complete", model)
    supervisor = WorkSupervisor(
        env.store,
        env.state,
        env.service,
        lambda _: env.actor,
        setup.toolkit.prepare,
        setup.toolkit,
        env.root / "history-workspaces",
        lease_seconds=3,
        heartbeat_seconds=0.1,
        poll_seconds=0.02,
        release_attempt=setup.toolkit.release_attempt,
    )
    try:
        assert (await supervisor.run_once()).state == WorkState.QUEUED
        checkpoint = env.state.load_checkpoint(env.actor, work.id)
        assert RAW not in checkpoint.model_dump_json()
        assert RAW in json.dumps(reviews[0])
        with env.store.transaction(env.actor) as tx:
            row = tx.conn.execute(
                "SELECT source_event_ids,body FROM ga_runtime_checkpoints WHERE work_id=%s",
                (work.id,),
            ).fetchone()
            assert history.id not in row["source_event_ids"]
            assert RAW not in json.dumps(row["body"])
            assert RAW not in json.dumps(tx.get_work(work.id).scope["_runtime_semantic_reviews"])

        MemoryService(env.store).expire(env.service, before=datetime.now(UTC) - timedelta(days=30))
        with env.store.transaction(env.actor) as tx:
            assert tx.get_event(history.id).content_state == "expired"
            assert tx.get_event(history.id).text == ""
            current = tx.get_work(work.id)
            assert current.state == WorkState.QUEUED and current.goal == work.goal
        assert (await supervisor.run_once()).state == WorkState.DELIVERING
        restored = env.state.load_checkpoint(env.actor, work.id)
        assert RAW not in restored.model_dump_json()
        assert len(generations) == len(reviews) == 2
        assert RAW not in json.dumps(reviews[-1])
        assert reviews[-1]["human_requirements"]["history_complete"] is False
        assert reviews[-1]["human_requirements"]["retained_authorized_context"]["goal"] == work.goal
        with env.store.transaction(env.actor) as tx:
            assert tx.get_work(work.id).goal == work.goal
            assert all(RAW not in delivery.payload_ref for delivery in tx.list_delivery())
    finally:
        await supervisor.close()
