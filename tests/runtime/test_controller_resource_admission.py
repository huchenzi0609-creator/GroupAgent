"""Controller headroom and actual worker admission are separate physical needs.

Providers, models and process launch are explicit offline fixtures. PostgreSQL,
when enabled, only exercises durable scheduling; no production service is used.
"""

import asyncio
import hashlib
import json
from types import SimpleNamespace

import httpx
import pytest

from groupagent.artifacts import ArtifactScope, ArtifactStore
from groupagent.artifacts.isolated import IsolatedDocumentService
from groupagent.domain import ActorContext, Role, WorkState
from groupagent.execution import (
    ExecutionRequest,
    ModelReply,
    ModelToolCall,
    ScopedToolEngine,
    TokenUsage,
)
from groupagent.media.isolation import IsolatedMediaWorker
from groupagent.research import DeepSeekResearchClient, register_research_tools
from groupagent.resources import MemorySnapshot, ResourceDeferred, ResourceGovernor, ResourcePolicy
from groupagent.runtime.app import RuntimeApp
from groupagent.runtime.contracts import DeliveryDraft, PreparedExecution, ValidatedCandidate
from groupagent.runtime.executor import WorkSupervisor
from groupagent.runtime.resource_profile import controller_resources_ready, worker_limits
from groupagent.sandbox import DockerSandbox
from groupagent.tools import ToolContext, ToolGateway, ToolScope

from .conftest import source
from .test_resource_profile import small_config

MIB = 1024 * 1024


def governor(available):
    return ResourceGovernor(
        ResourcePolicy(enabled=True, reserve_memory_mb=96, transfer_memory_mb=128),
        sampler=lambda: MemorySnapshot(available[0] * MIB),
    )


@pytest.mark.parametrize(
    "available,controller,worker",
    [(287, False, False), (288, True, False), (681, True, False), (736, True, True)],
)
def test_controller_headroom_does_not_require_a_full_unstarted_child(available, controller, worker):
    admission = governor([available])
    assert controller_resources_ready(admission) is controller
    assert admission.ready(512) is worker
    assert not admission.busy  # A probe does not occupy the actual worker lane.


def test_controller_still_honors_worker_exclusivity_poison_and_unknown_capacity():
    admission = governor([736])
    reservation = admission.reserve(512)
    assert not controller_resources_ready(admission)
    reservation.release()
    assert controller_resources_ready(admission)
    admission.poison()
    assert not controller_resources_ready(admission)
    unknown = ResourceGovernor(ResourcePolicy(enabled=True), sampler=lambda: MemorySnapshot(None))
    assert not controller_resources_ready(unknown)


async def test_maintenance_uses_controller_headroom_when_foreground_is_idle():
    calls = []

    async def compact(actor):
        calls.append("compact")

    app = object.__new__(RuntimeApp)
    app.config = small_config()
    app._fence_application = lambda: None
    app._processing = set()
    app._last_maintenance = 0
    app.supervisor = SimpleNamespace(active_work_id=None)
    app.worker_governor = governor([681])
    app.owner = app.technical = ActorContext(group_id="fixture", member_id="owner", role=Role.OWNER)
    app.coordinator = SimpleNamespace(
        current_mode=lambda: "active",
        memory=SimpleNamespace(
            compact=compact, expire=lambda *args, **kwargs: calls.append("expire")
        ),
    )
    app.cleanup = None
    await app.maintenance(force=True)
    assert calls == ["compact", "expire"]
    calls.clear()
    app._processing.add("foreground-request")
    await app.maintenance(force=True)
    assert calls == []


async def test_direct_document_validation_still_reserves_full_worker_before_render(tmp_path):
    admission = governor([681])
    config = small_config()
    sandbox = DockerSandbox(
        "sha256:" + "a" * 64,
        docker_path="never-launched-fixture",
        limits=worker_limits(config),
        resource_governor=admission,
    )
    actor = ActorContext(group_id="fixture", member_id="owner", role=Role.OWNER)
    context = ToolContext(
        ToolScope(actor.group_id, "work", actor, tmp_path.resolve(), frozenset(), "trace"),
        "validate",
        "validate",
        asyncio.Event(),
    )
    with ArtifactStore(tmp_path / "artifacts") as store:
        version = store.create(
            ArtifactScope(actor.group_id, "work", actor.member_id),
            "fixture.docx",
            b"never parsed outside an admitted worker",
        )
        documents = IsolatedDocumentService(store, IsolatedMediaWorker(sandbox))
        assert controller_resources_ready(admission)
        with pytest.raises(ResourceDeferred):
            await documents.validate(
                context,
                version.file_id,
                version.version_id,
                expected_text=("fixture",),
                render=True,
            )
    assert not admission.busy and not DockerSandbox.scope_active(actor.group_id, "work")


@pytest.mark.postgres
async def test_queued_research_reaches_candidate_and_validation_below_worker_threshold(runtime_env):
    env, requests, models = runtime_env, [], []
    admission = governor([681])
    event = source(env, "Find a public source and answer concisely")
    with env.store.transaction(env.actor) as tx:
        work = tx.create_work(event.text, event.id, scope={"research_required": True})

    async def public(host):
        return ("93.184.216.34",)

    def respond(request):
        requests.append(request)
        return httpx.Response(
            200, json={"title": "Synthetic source", "content": "Actual offline transport evidence"}
        )

    class ModelFixture:
        name = "offline-controller-admission-fixture"
        calls = 0

        async def complete(self, request):
            self.calls += 1
            if self.calls == 1:
                return ModelReply(
                    tool_calls=(
                        ModelToolCall(
                            id="fetch",
                            name="web_fetch",
                            arguments={"url": "https://example.com/source"},
                        ),
                    ),
                    finish_reason="tool_calls",
                    usage=TokenUsage(input_tokens=10, output_tokens=5),
                )
            body = json.loads(request.messages[-1].content)["output"]["source"]
            return ModelReply(
                text="Supported offline answer. " + body["citation"],
                usage=TokenUsage(input_tokens=10, output_tokens=5),
            )

    class ValidatorFixture:
        async def validate(self, work, lease, actor, snapshot, workspace):
            assert snapshot.receipts[0].success
            assert snapshot.receipts[0].output["source"]["citation"] in snapshot.candidate_text
            text = snapshot.candidate_text
            return ValidatedCandidate(
                {"result": "offline-verified-source"},
                (
                    DeliveryDraft(
                        "text:" + text,
                        hashlib.sha256(text.encode()).hexdigest(),
                        "answer:" + work.id,
                    ),
                ),
            )

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as http:
        client = DeepSeekResearchClient("synthetic-key", client=http, resolver=public)

        async def factory(work, lease, actor, workspace):
            gateway, model = ToolGateway(), ModelFixture()
            register_research_tools(gateway, client)
            models.append(model)
            return PreparedExecution(
                ScopedToolEngine(model, gateway),
                ExecutionRequest(
                    work_id=work.id,
                    attempt_id=lease.attempt_id,
                    actor=actor,
                    objective=work.goal,
                    workspace=workspace,
                    allowed_tools=frozenset({"web_fetch"}),
                    required_tools=frozenset({"web_fetch"}),
                    trace_id=work.request_event_id,
                ),
            )

        supervisor = WorkSupervisor(
            env.store,
            env.state,
            env.service,
            lambda _: env.actor,
            factory,
            ValidatorFixture(),
            env.root / "workspaces",
            resource_ready=lambda: controller_resources_ready(admission),
        )
        assert not admission.ready(512)
        assert await supervisor.tick() == 1
        await asyncio.wait_for(supervisor._task, 3)
    with env.store.transaction(env.actor) as tx:
        assert tx.get_work(work.id).state == WorkState.DELIVERING
    assert len(requests) == 1 and models[0].calls == 2 and not admission.busy
