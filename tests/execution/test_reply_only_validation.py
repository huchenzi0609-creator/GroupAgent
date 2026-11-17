"""The existing graph repairs wording without redoing accepted tool observations."""

import hashlib

import pytest
from test_supervisor import Validator, deliveries, new_work, supervisor
from test_supervisor import env as env

from groupagent.domain import DomainError, WorkState
from groupagent.execution import (
    ExecutionRequest,
    ModelReply,
    ModelToolCall,
    ScopedToolEngine,
    ScriptedFakeModel,
    TokenUsage,
)
from groupagent.execution.engine import REPLY_ONLY_VALIDATION_KEY
from groupagent.runtime.contracts import PreparedExecution
from groupagent.runtime.review import ReviewAssessment, SemanticReviewFailure
from groupagent.tools import IdempotencyMode, ToolData, ToolDefinition, ToolGateway


def binding(work_id, revision, text="Synthetic candidate"):
    return {
        "work_id": work_id,
        "goal_revision": revision,
        "candidate_sha256": hashlib.sha256(text.encode()).hexdigest(),
        "request_sha256": "b" * 64,
        "content_passed": True,
        "group_reply_passed": False,
    }


def rejected(work, snapshot, *, content=True, reply=False):
    return SemanticReviewFailure(
        ReviewAssessment(content, reply, "Synthetic review rejection"),
        work_id=work.id,
        goal_revision=work.goal_revision,
        candidate_sha256=hashlib.sha256(snapshot.candidate_text.strip().encode()).hexdigest(),
        request_sha256="b" * 64,
    )


async def test_restriction_survives_cached_tools_and_blocks_direct_dispatch(
    execution_request, gateway
):
    engine = ScopedToolEngine(ScriptedFakeModel([]), gateway)
    run = engine._create(execution_request)
    run.backend_state["active_tools"] = list(execution_request.allowed_tools)
    run.backend_state[REPLY_ONLY_VALIDATION_KEY] = binding(run.request.work_id, run.revision)
    assert engine._visible_tools(run) == frozenset()
    assert run.scope.allowed_tools == execution_request.allowed_tools
    with pytest.raises(DomainError, match="Only candidate reply text"):
        await engine._dispatch_tool(
            run,
            ModelToolCall(id="forbidden", name="workspace_write_text", arguments={}),
            parallel=False,
        )
    assert not run.receipts
    run.revision += 1
    assert engine._visible_tools(run) == execution_request.allowed_tools
    assert REPLY_ONLY_VALIDATION_KEY not in run.backend_state


class Page(ToolData):
    text: str


class EvidenceFactory:
    def __init__(self, texts):
        self.texts, self.models, self.reads = texts, [], []

    async def __call__(self, work, lease, actor, workspace):
        gateway = ToolGateway()

        async def fetch(args, context):
            self.reads.append("synthetic saved page")
            return Page(text="Synthetic factual evidence")

        gateway.register(
            ToolDefinition(
                name="web_fetch",
                description="Synthetic read fixture, no network",
                input_model=ToolData,
                output_model=Page,
                handler=fetch,
                risk_class="read_only",
                idempotency_mode=IdempotencyMode.READ_ONLY,
            )
        )
        replies = [
            ModelReply(
                text=self.texts[len(self.models)], usage=TokenUsage(input_tokens=1, output_tokens=1)
            )
        ]
        if not self.models:
            replies.insert(
                0,
                ModelReply(
                    tool_calls=(ModelToolCall(id="saved-fetch", name="web_fetch", arguments={}),),
                    finish_reason="tool_calls",
                    usage=TokenUsage(input_tokens=1, output_tokens=1),
                ),
            )
        model = ScriptedFakeModel(replies)
        self.models.append(model)
        return PreparedExecution(
            ScopedToolEngine(model, gateway),
            ExecutionRequest(
                work_id=work.id,
                attempt_id=lease.attempt_id,
                actor=actor,
                objective=work.goal,
                workspace=workspace,
                revision=work.goal_revision,
                trace_id=work.request_event_id,
                allowed_tools=frozenset({"web_fetch"}),
                required_tools=frozenset({"web_fetch"}),
            ),
        )


@pytest.mark.postgres
async def test_same_work_reply_revision_retains_receipts_and_never_researches_again(env):
    work = new_work(env)
    factory = EvidenceFactory(["Complete facts, unnecessary branches", "Only necessary facts"])

    class Reviewer(Validator):
        async def validate(self, work, lease, actor, snapshot, workspace):
            if self.calls == 0:
                self.calls += 1
                raise rejected(work, snapshot)
            return await super().validate(work, lease, actor, snapshot, workspace)

    service = supervisor(env, factory, Reviewer())
    try:
        assert (await service.run_once()).state == WorkState.QUEUED
        before = env.state.load_checkpoint(env.actor, work.id)
        assert before.backend_state[REPLY_ONLY_VALIDATION_KEY]["work_id"] == work.id
        assert before.usage.tool_calls == 1 and len(before.receipts) == 1
        assert not deliveries(env, work.id)
        assert (await service.run_once()).state == WorkState.DELIVERING
        after = env.state.load_checkpoint(env.actor, work.id)
        assert after.revision == before.revision == work.goal_revision
        assert after.request.allowed_tools == before.request.allowed_tools
        assert after.request.required_tools == before.request.required_tools
        assert after.receipts == before.receipts
        assert after.ledger == before.ledger and after.usage.tool_calls == 1
        assert len(factory.reads) == 1 and factory.models[1].requests[0].tools == ()
        assert deliveries(env, work.id)[0].payload_ref == "text:Only necessary facts"
    finally:
        await service.close()


@pytest.mark.postgres
@pytest.mark.parametrize("failure", ["content", "url"])
async def test_substantive_revalidation_failure_releases_reply_only_tools(env, failure):
    work = new_work(env)
    factory = EvidenceFactory(["Long result", "Incorrect short result", "Corrected result"])

    class Reviewer(Validator):
        async def validate(self, work, lease, actor, snapshot, workspace):
            self.calls += 1
            if self.calls == 1:
                raise rejected(work, snapshot)
            if self.calls == 2:
                if failure == "content":
                    raise rejected(work, snapshot, content=False, reply=True)
                error = DomainError("validation_failed", "A cited URL lacks actual provenance")
                # Model-like diagnostic fields cannot grant text-only authority.
                error.details = {"content_passed": True, "group_reply_passed": False}
                raise error
            return await super().validate(work, lease, actor, snapshot, workspace)

    service = supervisor(env, factory, Reviewer())
    try:
        assert (await service.run_once()).state == WorkState.QUEUED
        assert (await service.run_once()).state == WorkState.QUEUED
        checkpoint = env.state.load_checkpoint(env.actor, work.id)
        assert REPLY_ONLY_VALIDATION_KEY not in checkpoint.backend_state
        assert factory.models[1].requests[0].tools == ()
        assert (await service.run_once()).state == WorkState.DELIVERING
        assert factory.models[2].requests[0].tools
    finally:
        await service.close()
