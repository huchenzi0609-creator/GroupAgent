"""Review completed evidence once; retries still prove source and artifact authority."""

import json

import pytest

from groupagent.domain import DomainError
from groupagent.tools.models import ToolReceipt

from .test_toolkit_contract import call, candidate, create_document, prepare
from .test_toolkit_contract import toolkit_setup as toolkit_setup

pytestmark = pytest.mark.postgres


async def prepared_candidate(setup):
    run = await prepare(setup, scope={"outputs": [{"format": "md"}]})
    generated = await create_document(run)
    selected = await call(
        run, "artifact_deliver", {"version_ids": [generated.output["version_id"]]}
    )
    return run, (generated, selected)


async def test_same_evidence_after_jsonb_ordering_reuses_accepted_review(toolkit_setup):
    setup = toolkit_setup
    run, receipts = await prepared_candidate(setup)
    first = candidate(run, receipts=receipts)
    result = await setup.toolkit.validate(
        run.work, run.lease, setup.env.actor, first, run.workspace
    )
    assert result.checks["result"] and len(setup.model.requests) == 1
    reordered = tuple(
        ToolReceipt.model_validate(
            json.loads(json.dumps(receipt.model_dump(mode="json"), sort_keys=True))
        )
        for receipt in receipts
    )
    setup.toolkit._services.clear()  # Same immutable bytes, a new service instance/view.
    await setup.toolkit.validate(
        run.work, run.lease, setup.env.actor, candidate(run, receipts=reordered), run.workspace
    )
    assert len(setup.model.requests) == 1
    with setup.env.store.transaction(setup.env.actor) as tx:
        count = tx.conn.execute(
            "SELECT count(*) AS n FROM ga_audit_events WHERE group_id=%s AND resource_ref=%s "
            "AND action='semantic_review_reused'",
            (setup.env.actor.group_id, run.work.id),
        ).fetchone()["n"]
    assert count == 1


async def test_changed_candidate_and_checker_each_require_new_review(toolkit_setup):
    setup = toolkit_setup
    run, receipts = await prepared_candidate(setup)
    for text in ("合成报告已经准备好。", "合成报告与简短说明已经准备好。"):
        await setup.toolkit.validate(
            run.work, run.lease, setup.env.actor, candidate(run, text, receipts), run.workspace
        )
    assert len(setup.model.requests) == 2
    setup.model.name = "explicit-offline-review-fixture/revision-2"
    await setup.toolkit.validate(
        run.work,
        run.lease,
        setup.env.actor,
        candidate(run, "合成报告与简短说明已经准备好。", receipts),
        run.workspace,
    )
    assert len(setup.model.requests) == 3


async def test_rejected_review_is_not_a_cache_and_forgotten_sources_never_reuse(toolkit_setup):
    setup = toolkit_setup
    run, receipts = await prepared_candidate(setup)
    setup.model.passed = False
    with pytest.raises(DomainError, match="Independent review"):
        await setup.toolkit.validate(
            run.work, run.lease, setup.env.actor, candidate(run, receipts=receipts), run.workspace
        )
    setup.model.passed = True
    await setup.toolkit.validate(
        run.work, run.lease, setup.env.actor, candidate(run, receipts=receipts), run.workspace
    )
    assert len(setup.model.requests) == 2
    setup.toolkit.memory.forget(setup.env.actor, event_ids=[run.event.id])
    with pytest.raises(DomainError):
        await setup.toolkit.validate(
            run.work, run.lease, setup.env.actor, candidate(run, receipts=receipts), run.workspace
        )
    assert len(setup.model.requests) == 2


async def test_web_task_objective_contains_only_selected_capability_guidance(toolkit_setup):
    setup = toolkit_setup
    run = await prepare(
        setup,
        goal="检索公开Python文档，用一句话和链接解释列表。",
        scope={
            "research_required": True,
            "research_message_only": True,
            "outputs": [{"format": "text"}],
        },
    )
    objective = json.loads(run.prepared.request.objective)
    instructions = "\n".join(objective["instructions"])
    assert "web_read" in run.prepared.request.allowed_tools
    assert "PPT" not in instructions and "PNG" not in instructions and "ASR" not in instructions
    assert len(instructions.encode()) < 4500


async def test_capability_view_can_load_needed_tools_without_changing_its_grant(toolkit_setup):
    setup = toolkit_setup
    run = await prepare(
        setup,
        goal="根据之前的公开资料整理说明",
        scope={
            "work_capabilities": ["public_research"],
        },
    )
    request = run.prepared.request
    assert request.visible_tools and "tool_catalog" in request.visible_tools
    assert "document_generate" in request.allowed_tools
    assert "document_generate" not in request.visible_tools
    before = run.scope.allowed_tools
    loaded = await call(run, "tool_catalog", {"capabilities": ["documents"]})
    assert loaded.success and "document_generate" in loaded.loaded_tools
    assert loaded.loaded_tools.issubset(before)
    assert run.scope.allowed_tools == before
