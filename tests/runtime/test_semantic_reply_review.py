"""Dual verdicts and real-PG candidate validation, using synthetic reviewers only."""

import json

import pytest
from psycopg.types.json import Jsonb

from groupagent.domain import DomainError
from groupagent.execution import ModelReply, TokenUsage
from groupagent.runtime.review import SemanticReviewFailure, parse_review

from .test_research_group_delivery import fetch_reference
from .test_toolkit_contract import candidate, prepare
from .test_toolkit_contract import toolkit_setup as toolkit_setup


@pytest.mark.parametrize(
    "value",
    [
        {"passed": True},
        {"content_passed": True},
        {"content_passed": "true", "group_reply_passed": True},
        {"content_passed": True, "group_reply_passed": "false"},
        {"content_passed": 1, "group_reply_passed": True},
        {"content_passed": True, "group_reply_passed": None},
        [],
    ],
)
def test_missing_legacy_or_coerced_verdicts_are_invalid(value):
    assert parse_review(json.dumps(value)) is None


@pytest.mark.parametrize(
    "content,reply", [(True, True), (True, False), (False, True), (False, False)]
)
def test_only_host_conjunction_decides_acceptance(content, reply):
    assessment = parse_review(
        json.dumps(
            {
                "content_passed": content,
                "group_reply_passed": reply,
                "passed": True,
                "repair_scope": "reply_only",
            }
        )
    )
    assert assessment.passed is (content and reply)


@pytest.mark.postgres
@pytest.mark.parametrize("content,reply", [(True, False), (False, True), (False, False)])
async def test_review_rejection_records_both_verdicts_and_never_enqueues(
    toolkit_setup, monkeypatch, content, reply
):
    setup = toolkit_setup
    run = await prepare(setup)

    async def reviewer(_):
        return ModelReply(
            text=json.dumps(
                {
                    "content_passed": content,
                    "group_reply_passed": reply,
                    "passed": True,
                    "repair_scope": "reply_only",
                    "reason": "Synthetic independent verdicts",
                }
            ),
            usage=TokenUsage(input_tokens=1, output_tokens=1),
        )

    monkeypatch.setattr(setup.model, "complete", reviewer)
    with pytest.raises(SemanticReviewFailure) as caught:
        await setup.toolkit.validate(
            run.work, run.lease, setup.env.actor, candidate(run), run.workspace
        )
    assert caught.value.assessment.content_passed is content
    assert caught.value.assessment.group_reply_passed is reply
    with setup.env.store.transaction(setup.env.actor) as tx:
        assert not tx.list_delivery()
        record = tx.get_work(run.work.id).scope["_runtime_semantic_reviews"][-1]
        assert record["content_passed"] is content and record["group_reply_passed"] is reply
        assert record["accepted"] is False and record["passed"] is False


@pytest.mark.postgres
async def test_legacy_single_verdict_cannot_reuse_an_accepted_cache(toolkit_setup):
    setup = toolkit_setup
    run = await prepare(setup)
    await setup.toolkit.validate(
        run.work, run.lease, setup.env.actor, candidate(run), run.workspace
    )
    with setup.env.store.transaction(setup.env.actor) as tx:
        work = tx.get_work(run.work.id)
        scope = dict(work.scope)
        record = scope["_runtime_semantic_reviews"][-1]
        record.pop("content_passed")
        record.pop("group_reply_passed")
        assert record["accepted"] is True
        tx.conn.execute(
            "UPDATE ga_work_orders SET scope=%s WHERE group_id=%s AND id=%s",
            (Jsonb(scope), setup.env.actor.group_id, work.id),
        )
    await setup.toolkit.validate(
        run.work, run.lease, setup.env.actor, candidate(run), run.workspace
    )
    assert len(setup.model.requests) == 2
    # A changed candidate also requires its own complete review, never the old pass.
    await setup.toolkit.validate(
        run.work,
        run.lease,
        setup.env.actor,
        candidate(run, "Corrected synthetic text"),
        run.workspace,
    )
    assert len(setup.model.requests) == 3


@pytest.mark.postgres
async def test_new_reply_reenters_citation_validation_before_critic(toolkit_setup, monkeypatch):
    setup = toolkit_setup
    run = await prepare(setup, scope={"research_required": True})
    fetched = await fetch_reference(setup, monkeypatch, run)

    async def style_reject(_):
        return ModelReply(
            text='{"content_passed":true,"group_reply_passed":false,"reason":"Unneeded branches and closing offer"}',
            usage=TokenUsage(input_tokens=1, output_tokens=1),
        )

    monkeypatch.setattr(setup.model, "complete", style_reject)
    with pytest.raises(SemanticReviewFailure):
        await setup.toolkit.validate(
            run.work,
            run.lease,
            setup.env.actor,
            candidate(run, "Known result", (fetched,)),
            run.workspace,
        )
    with pytest.raises(DomainError) as caught:
        await setup.toolkit.validate(
            run.work,
            run.lease,
            setup.env.actor,
            candidate(run, "Known result https://invented.example.test/source", (fetched,)),
            run.workspace,
        )
    assert caught.value.code == "validation_failed"
    assert not isinstance(caught.value, SemanticReviewFailure)
