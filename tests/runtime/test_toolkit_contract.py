"""Real PostgreSQL composition contracts; providers and parsers are explicit fixtures.

These tests do not launch Docker or call a paid provider. The separate real
worker suites establish decoding, rendering and OS isolation.
"""

from __future__ import annotations

import asyncio
import io
import json
import tempfile
from dataclasses import asdict
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest
from PIL import Image

from groupagent.artifacts import ArtifactScope, ArtifactStore, DocumentService, Report
from groupagent.artifacts.formats import build_markdown, extract
from groupagent.artifacts.worker_operations import unpack_document
from groupagent.billing import BudgetLedger, PriceSnapshot
from groupagent.config import AppConfig, GroupConfig, RuntimeConfig, SpendConfig
from groupagent.domain import DomainError
from groupagent.execution.models import (
    EngineCheckpoint,
    ModelReply,
    RunSnapshot,
    RunState,
    TokenUsage,
    UsageTotals,
)
from groupagent.media.validation import inspect_image
from groupagent.memory import MemoryService
from groupagent.research import DeepSeekResearchClient
from groupagent.runtime.inputs import InputProcessor
from groupagent.runtime.state import source_fingerprint
from groupagent.runtime.toolkit import WorkToolkit
from groupagent.tools import ToolContext, ToolScope

from .conftest import source

pytestmark = pytest.mark.postgres


class NoProcessFixtureSandbox:
    allowed_programs = frozenset({"python3", "sh", "ls", "cat", "wc", "head", "tail", "sort"})

    def __init__(self, *args, **kwargs):
        self.policy_guard = kwargs.get("policy_guard")

    async def run(self, *args, **kwargs):
        raise AssertionError("This contract fixture must not launch any process")


class SyntheticFixtureWorker:
    def __init__(self, sandbox):
        self.sandbox = sandbox
        self.calls = []

    async def run(self, context, operation, inputs, *, parameters=None, outputs=(), **kwargs):
        self.calls.append(operation)
        if self.sandbox.policy_guard:
            await self.sandbox.policy_guard(context)
        params = parameters or {}
        if operation == "image_validate":
            metadata, preview = inspect_image(inputs[0])
            return metadata.model_dump(mode="json"), (preview,)
        assert params.get("format") == "md", "Synthetic host fixture never parses Office/PDF"
        if operation == "document_generate":
            document = unpack_document(params["document"], lambda i: inputs[i])
            assert isinstance(document, Report)
            return {"format": "md"}, (build_markdown(document),)
        if operation == "document_inspect":
            return {"unit_count": len(extract(inputs[0], "md"))}, ()
        if operation == "document_read":
            units = extract(inputs[0], "md")
            start, count = params.get("start_unit", 0), params.get("max_units", 200)
            return {
                "units": [asdict(unit) for unit in units[start : start + count]],
                "total_units": len(units),
                "complete": start == 0 and count >= len(units),
            }, ()
        if operation == "document_validate":
            with tempfile.TemporaryDirectory(prefix="toolkit-md-fixture-") as directory:
                with ArtifactStore(Path(directory)) as store:
                    scope = ArtifactScope("fixture", "fixture", "fixture")
                    version = store.create(scope, "fixture.md", inputs[0])
                    validation = DocumentService(store).validate(
                        scope,
                        version.file_id,
                        expected_text=tuple(params["expected_text"]),
                        render=False,
                    )
                    return validation.to_dict(), ()
        raise AssertionError("Unexpected fixture operation: " + operation)


class FixtureSemanticReviewer:
    name = "explicit-offline-review-fixture"

    def __init__(self):
        self.requests = []
        self.passed = True

    async def complete(self, request):
        self.requests.append(request)
        assert not request.tools
        return ModelReply(
            text=json.dumps({"passed": self.passed, "reason": "offline reviewer fixture"}),
            usage=TokenUsage(input_tokens=10, output_tokens=5),
        )


@pytest.fixture
async def toolkit_setup(runtime_env, monkeypatch):
    env = runtime_env
    monkeypatch.setattr("groupagent.runtime.toolkit.DockerSandbox", NoProcessFixtureSandbox)
    monkeypatch.setattr("groupagent.runtime.toolkit.IsolatedMediaWorker", SyntheticFixtureWorker)
    ledger = BudgetLedger(env.store, allow_test_prices=True)
    ledger.configure(env.actor, currency="CNY", daily_limit=Decimal(10), total_limit=Decimal(10))
    for name, op, units in (
        (
            "fixture-text",
            "text",
            {"input_tokens": Decimal("0.000001"), "output_tokens": Decimal("0.000001")},
        ),
        ("deepseek-20261114-web.search", "web.search", {"calls": Decimal("0.001")}),
        ("deepseek-20261114-web.fetch", "web.fetch", {"calls": Decimal("0.001")}),
    ):
        ledger.register_price(
            env.actor,
            PriceSnapshot(
                id=name,
                provider="synthetic-only",
                operation=op,
                currency="CNY",
                rates=units,
                source_url="fixture://not-a-commercial-price",
                captured_at=datetime.now(UTC),
                test_only=True,
            ),
        )
    model = FixtureSemanticReviewer()
    requested = []

    def respond(request):
        requested.append(request)
        return httpx.Response(
            200,
            json={
                "title": "Synthetic public page",
                "content": "A real mock-transport observation for contract testing.",
            },
        )

    async def resolve(host):
        return ("93.184.216.34",)

    http = httpx.AsyncClient(transport=httpx.MockTransport(respond))
    research = DeepSeekResearchClient("synthetic-only-key", client=http, resolver=resolve)
    config = AppConfig(
        group=GroupConfig(
            enabled=True,
            internal_id=env.actor.group_id,
            external_group_id=env.config.external_group_id,
            owner_ids=[env.actor.member_id],
        ),
        spend=SpendConfig(pricing_snapshot_id="fixture-text"),
        runtime=RuntimeConfig(
            engine="scoped", worker_image="sha256:" + "0" * 64, semantic_validation=True
        ),
    )
    inputs = InputProcessor(
        env.store,
        env.artifacts,
        SyntheticFixtureWorker(NoProcessFixtureSandbox()),
        {},
        {},
        ledger,
        env.channel,
        env.root / "inputs",
        lambda _: env.actor,
    )
    toolkit = WorkToolkit(
        config,
        env.store,
        env.artifacts,
        env.registry,
        MemoryService(env.store),
        model,
        ledger,
        media_providers={},
        routes={},
        research=research,
        inputs=inputs,
    )
    yield SimpleNamespace(
        env=env, toolkit=toolkit, model=model, http_calls=requested, inputs=inputs
    )
    await http.aclose()


async def prepare(setup, *, scope=None, goal="Prepare a synthetic report"):
    env = setup.env
    event = source(env, goal)
    with env.store.transaction(env.actor) as tx:
        work = tx.create_work(
            goal,
            event.id,
            scope={"source_fingerprints": {event.id: source_fingerprint(event)}, **(scope or {})},
        )
        lease = tx.claim_work(work.id, "toolkit-fixture", ttl_seconds=300)
        work = tx.get_work(work.id)
    workspace = env.root / work.id
    workspace.mkdir()
    prepared = await setup.toolkit.prepare(work, lease, env.actor, workspace)
    gateway = prepared.engine.gateway
    tool_scope = ToolScope(
        env.actor.group_id, work.id, env.actor, workspace, prepared.request.allowed_tools, event.id
    )
    gateway.bind_scope(tool_scope)
    return SimpleNamespace(
        work=work,
        lease=lease,
        workspace=workspace,
        prepared=prepared,
        gateway=gateway,
        scope=tool_scope,
        event=event,
    )


async def call(run, name, args, key=None):
    key = key or name
    return await run.gateway.invoke(name, args, ToolContext(run.scope, key, key, asyncio.Event()))


def candidate(run, text="A verified synthetic result.", receipts=()):
    checkpoint = EngineCheckpoint(
        request=run.prepared.request,
        revision=run.work.goal_revision,
        state=RunState.CANDIDATE,
        sequence=1,
        messages=(),
        usage=UsageTotals(),
        receipts=tuple(receipts),
        safe_to_resume=True,
    )
    return RunSnapshot(
        run_id="fixture-run",
        work_id=run.work.id,
        attempt_id=run.lease.attempt_id,
        group_id=run.work.group_id,
        revision=run.work.goal_revision,
        state=RunState.CANDIDATE,
        usage=UsageTotals(),
        candidate_text=text,
        receipts=tuple(receipts),
        last_sequence=1,
        checkpoint=checkpoint,
    )


async def create_document(run):
    generated = await call(
        run,
        "document_generate",
        {
            "name": "report.md",
            "kind": "report",
            "title": "Synthetic Report",
            "sections": [{"title": "Findings", "paragraphs": ["The synthetic result is 87."]}],
        },
    )
    assert generated.success, generated
    return generated


async def test_real_toolkit_catalog_contains_file_research_media_compute_and_control(toolkit_setup):
    run = await prepare(toolkit_setup)
    expected = {
        "request_input",
        "document_generate",
        "document_read",
        "document_search",
        "document_edit",
        "document_validate",
        "pdf_combine",
        "document_extract_image",
        "chart_render",
        "image_analyze",
        "media_ocr",
        "provider_ocr",
        "audio_transcribe",
        "speech_synthesize",
        "python_compute",
        "process_run",
        "shell_command",
        "artifact_from_workspace",
        "artifact_search",
        "artifact_import",
        "artifact_deliver",
        "memory_search",
        "web_search",
        "web_fetch",
    }
    assert expected <= run.prepared.request.allowed_tools
    assert run.prepared.request.allowed_tools == run.gateway.registered_names
    assert "image_generate" not in run.prepared.request.allowed_tools
    assert not any("video" in name for name in run.prepared.request.allowed_tools)
    objective = json.loads(run.prepared.request.objective)
    assert objective["goal"] == run.work.goal and objective["input_files"] == []
    assert not toolkit_setup.model.requests and not toolkit_setup.http_calls


async def test_generated_document_is_catalogued_then_selected_and_independently_validated(
    toolkit_setup,
):
    setup = toolkit_setup
    run = await prepare(setup, scope={"outputs": [{"format": "md", "expected_text": ["87"]}]})
    generated = await create_document(run)
    outputs = setup.env.registry.outputs(setup.env.actor, run.work.id, run.work.goal_revision)
    assert len(outputs) == 1 and outputs[0]["purpose"] == "candidate"
    with pytest.raises(DomainError, match="Missing requested formats"):
        await setup.toolkit.validate(
            run.work,
            run.lease,
            setup.env.actor,
            candidate(run, receipts=(generated,)),
            run.workspace,
        )
    selected = await call(
        run, "artifact_deliver", {"version_ids": [generated.output["version_id"]]}
    )
    assert selected.success
    result = await setup.toolkit.validate(
        run.work,
        run.lease,
        setup.env.actor,
        candidate(run, receipts=(generated, selected)),
        run.workspace,
    )
    assert "artifact:md" in result.checks and len(result.deliveries) == 2
    assert any(
        item.payload_ref == "artifact:" + generated.output["version_id"]
        for item in result.deliveries
    )
    assert len(setup.model.requests) == 1
    with setup.env.store.transaction(setup.env.actor) as tx:
        assert not tx.conn.execute(
            "SELECT 1 FROM ga_deliveries WHERE group_id=%s", (setup.env.actor.group_id,)
        ).fetchone(), "Validation drafts do not send by themselves"


async def test_search_import_same_work_and_other_work_create_scoped_immutable_copies(toolkit_setup):
    setup = toolkit_setup
    first = await prepare(setup)
    generated = await create_document(first)
    version_id = generated.output["version_id"]
    listed = await call(first, "artifact_search", {"query": "report"})
    assert listed.success and any(row["version_id"] == version_id for row in listed.output["files"])
    same = await call(first, "artifact_import", {"version_id": version_id})
    assert same.success, same
    same_copy = next(row for row in same.output["files"] if row.get("from_version") == version_id)
    assert same_copy["version_id"] != version_id
    repeated = await call(first, "artifact_import", {"version_id": version_id}, "import-again")
    assert repeated.success and repeated.output["files"] == same.output["files"]
    second = await prepare(setup)
    copied = await call(second, "artifact_import", {"version_id": version_id})
    assert copied.success, copied
    result = copied.output["files"][0]
    original, original_bytes = setup.env.registry.read(setup.env.actor, version_id)
    copy, copy_bytes = setup.env.registry.read(setup.env.actor, result["version_id"])
    assert original.work_id != copy.work_id and original_bytes == copy_bytes
    with setup.env.store.transaction(setup.env.actor) as tx:
        current = tx.get_work(second.work.id)
    assert first.event.id in current.scope["source_fingerprints"]
    selected = await call(
        second, "artifact_deliver", {"version_ids": [version_id]}, "foreign-publish"
    )
    assert not selected.success and selected.error_code == "scope_violation"


async def test_withdrawn_source_blocks_search_import_and_stale_work_tools(toolkit_setup):
    setup = toolkit_setup
    first = await prepare(setup)
    generated = await create_document(first)
    second = await prepare(setup)
    MemoryService(setup.env.store).forget(setup.env.actor, event_ids=[first.event.id])
    listed = await call(second, "artifact_search", {"query": "report"})
    assert listed.success and listed.output["files"] == []
    denied = await call(second, "artifact_import", {"version_id": generated.output["version_id"]})
    assert not denied.success and denied.error_code == "source_unavailable"
    stale = await call(
        first,
        "document_generate",
        {
            "name": "late.md",
            "kind": "report",
            "title": "Late",
            "sections": [{"title": "Late", "paragraphs": ["No source"]}],
        },
        "late",
    )
    assert not stale.success and stale.error_code in {"source_unavailable", "stale_lease"}


async def test_research_claim_without_actual_receipt_cannot_pass(toolkit_setup):
    setup = toolkit_setup
    run = await prepare(setup, scope={"research_required": True}, goal="Search public sources")
    with pytest.raises(DomainError, match="actual research tool evidence"):
        await setup.toolkit.validate(
            run.work,
            run.lease,
            setup.env.actor,
            candidate(run, "I searched and verified https://example.com/source"),
            run.workspace,
        )
    assert not setup.model.requests
    fetched = await call(run, "web_fetch", {"url": "https://example.com/source"})
    assert fetched.success and len(setup.http_calls) == 1
    accepted = await setup.toolkit.validate(
        run.work,
        run.lease,
        setup.env.actor,
        candidate(
            run, "The synthetic observation is documented at https://example.com/source", (fetched,)
        ),
        run.workspace,
    )
    assert accepted.checks["result"] and len(setup.model.requests) == 1
    with pytest.raises(DomainError, match="actual research tool evidence"):
        await setup.toolkit.validate(
            run.work,
            run.lease,
            setup.env.actor,
            candidate(run, "Unrelated source https://example.com/invented", (fetched,)),
            run.workspace,
        )


async def test_real_media_decode_evidence_is_required_even_when_reviewer_would_approve(
    toolkit_setup,
):
    setup = toolkit_setup
    run = await prepare(setup, scope={"outputs": [{"format": "png"}]})
    image = Image.new("RGB", (640, 480), "white")
    stream = io.BytesIO()
    image.save(stream, "PNG")
    version = setup.env.artifacts.create(
        ArtifactScope(setup.env.actor.group_id, run.work.id, setup.env.actor.member_id),
        "unverified.png",
        stream.getvalue(),
    )
    setup.env.registry.register(setup.env.actor, run.lease, version, purpose="candidate")
    selected = await call(run, "artifact_deliver", {"version_ids": [version.version_id]})
    assert selected.success
    with pytest.raises(DomainError, match="verified format/decode"):
        await setup.toolkit.validate(
            run.work,
            run.lease,
            setup.env.actor,
            candidate(run, receipts=(selected,)),
            run.workspace,
        )
    assert not setup.model.requests
    (run.workspace / "validated.png").write_bytes(stream.getvalue())
    adopted = await call(
        run, "artifact_from_workspace", {"path": "validated.png", "name": "validated.png"}
    )
    assert adopted.success
    chosen = await call(
        run, "artifact_deliver", {"version_ids": [adopted.output["version_id"]]}, "select-decoded"
    )
    result = await setup.toolkit.validate(
        run.work,
        run.lease,
        setup.env.actor,
        candidate(run, receipts=(adopted, chosen)),
        run.workspace,
    )
    assert "artifact:png" in result.checks


async def test_semantic_rejection_and_no_send_policy_are_not_overridden_by_format_success(
    toolkit_setup,
):
    setup = toolkit_setup
    run = await prepare(setup, scope={"outputs": [{"format": "md"}], "delivery_artifacts": False})
    generated = await create_document(run)
    selected = await call(
        run, "artifact_deliver", {"version_ids": [generated.output["version_id"]]}
    )
    setup.model.passed = False
    with pytest.raises(DomainError, match="Independent review"):
        await setup.toolkit.validate(
            run.work,
            run.lease,
            setup.env.actor,
            candidate(run, receipts=(generated, selected)),
            run.workspace,
        )
    setup.model.passed = True
    result = await setup.toolkit.validate(
        run.work,
        run.lease,
        setup.env.actor,
        candidate(run, receipts=(generated, selected)),
        run.workspace,
    )
    assert len(result.deliveries) == 1 and result.deliveries[0].payload_ref.startswith("text:")
    assert "暂不发送" in result.deliveries[0].payload_ref


async def test_missing_input_processor_does_not_silently_drop_explicit_versions(toolkit_setup):
    setup = toolkit_setup
    old = await prepare(setup)
    generated = await create_document(old)
    event = source(setup.env)
    with setup.env.store.transaction(setup.env.actor) as tx:
        work = tx.create_work(
            "Use supplied document", event.id, input_versions=[generated.output["version_id"]]
        )
        lease = tx.claim_work(work.id, "fixture-worker", ttl_seconds=300)
    setup.toolkit.inputs = None
    workspace = setup.env.root / work.id
    workspace.mkdir()
    with pytest.raises(DomainError) as error:
        await setup.toolkit.prepare(work, lease, setup.env.actor, workspace)
    assert error.value.code == "dependency_unavailable"


async def test_memory_tool_adds_only_public_actual_sources_to_work_fingerprint_scope(toolkit_setup):
    setup = toolkit_setup
    public = source(setup.env, "Distinct public fixture evidence")
    run = await prepare(setup)
    result = await call(run, "memory_search", {"query": "Distinct public fixture evidence"})
    assert result.success and result.output["items"]
    with setup.env.store.transaction(setup.env.actor) as tx:
        current = tx.get_work(run.work.id)
    assert current.scope["source_fingerprints"][public.id] == source_fingerprint(public)
    MemoryService(setup.env.store).forget(setup.env.actor, event_ids=[public.id])
    denied = await call(run, "artifact_search", {"query": ""})
    assert not denied.success
