"""Real PostgreSQL composition contracts; providers and parsers are explicit fixtures.

These tests do not launch Docker or call a paid provider. The separate real
worker suites establish decoding, rendering and OS isolation.
"""

from __future__ import annotations

import asyncio
import hashlib
import io
import json
import tempfile
from dataclasses import asdict
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest
from PIL import Image
from psycopg.errors import QueryCanceled

from groupagent.artifacts import (
    ArtifactScope,
    ArtifactStore,
    DocumentService,
    Report,
    ValidationRecord,
)
from groupagent.artifacts.formats import build_markdown, extract
from groupagent.artifacts.worker_operations import unpack_document
from groupagent.billing import BudgetLedger, PriceSnapshot
from groupagent.config import AppConfig, GroupConfig, RuntimeConfig, SpendConfig
from groupagent.domain import DomainError, EventKind, WorkState
from groupagent.execution import LangGraphEngine, ScriptedFakeModel
from groupagent.execution.models import (
    EngineCheckpoint,
    ModelReply,
    ModelToolCall,
    RunSnapshot,
    RunState,
    TokenUsage,
    UsageTotals,
)
from groupagent.media.validation import inspect_image
from groupagent.memory import MemoryService
from groupagent.research import DeepSeekResearchClient
from groupagent.research.citations import UnverifiedCitation
from groupagent.runtime.dispatch import Dispatcher
from groupagent.runtime.inputs import InputProcessor
from groupagent.runtime.state import source_fingerprint
from groupagent.runtime.toolkit import WorkToolkit
from groupagent.scheduling import EveryTiming, Scheduler
from groupagent.tools import EffectState, ToolContext, ToolScope
from groupagent.tools.models import ToolFailureDisposition, failure_disposition

from .conftest import source
from .test_app import event
from .test_inputs import attached

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
        if operation == "ocr_image":
            return {
                "pages": [
                    {"page_number": 1, "text": "值班：测试乙，展架 L-187", "method": "tesseract"}
                ]
            }, ()
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
            text=json.dumps(
                {
                    "content_passed": self.passed,
                    "group_reply_passed": True,
                    "reason": "offline reviewer fixture",
                }
            ),
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


async def prepare(setup, *, scope=None, goal="Prepare a synthetic report", request_text=None):
    env = setup.env
    event = source(env, goal if request_text is None else request_text)
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
    assert run.gateway.registered_names - run.prepared.request.allowed_tools == {
        "speech_synthesize"
    }
    assert "image_generate" not in run.prepared.request.allowed_tools
    assert "speech_synthesize" not in run.prepared.request.allowed_tools
    assert not any("video" in name for name in run.prepared.request.allowed_tools)
    objective = json.loads(run.prepared.request.objective)
    assert objective["goal"] == run.work.goal and objective["input_files"] == []
    assert objective["agent_name"] == "Pot" and objective["bot_badge"] == "【Pot】"
    assert "Pot" in objective["agent_aliases"]
    assert any("你的名字是Pot" in item for item in objective["instructions"])


@pytest.mark.parametrize("required", [[], ["python_compute"]])
async def test_trusted_required_tools_are_explicit_in_native_objective_without_changing_goal(
    toolkit_setup, required
):
    setup = toolkit_setup
    goal = "请计算17*23。"
    run = await prepare(setup, goal=goal, scope={"required_tools": required})
    objective = json.loads(run.prepared.request.objective)
    assert objective["goal"] == objective["original_request"]["text"] == goal
    assert objective["required_tools"] == sorted(run.prepared.request.required_tools) == required
    assert all(name in run.prepared.request.allowed_tools for name in required)
    assert any(
        "Required tools" in item and "receipts" in item for item in objective["instructions"]
    )
    # Scope and successful-required-tool enforcement are executable contracts,
    # not the presence of a historical sentence in every prompt.
    # UUIDs and clock microseconds may contain these digits by chance. Check
    # request/constraint content, where a leaked expected answer would matter.
    assert objective["input_files"] == []
    semantic_content = {
        "goal": objective["goal"],
        "original_request_text": objective["original_request"]["text"],
        "acceptance_criteria": objective["acceptance_criteria"],
        "required_outputs": objective["required_outputs"],
        "instructions": objective["instructions"],
    }
    assert "391" not in json.dumps(semantic_content, ensure_ascii=False)
    assert not setup.model.requests and not setup.http_calls


async def test_required_but_disabled_tool_is_rejected_without_reactivating_or_calling_provider(
    toolkit_setup,
):
    setup = toolkit_setup
    setup.toolkit.config = setup.toolkit.config.model_copy(
        update={
            "features": setup.toolkit.config.features.model_copy(update={"web_research": False})
        }
    )
    with pytest.raises(DomainError) as caught:
        await prepare(
            setup, goal="Read a synthetic official page", scope={"required_tools": ["web_fetch"]}
        )
    assert caught.value.code == "tool_not_registered"
    assert setup.toolkit.config.features.web_research is False
    assert not setup.model.requests and not setup.http_calls
    with setup.env.store.connect() as conn:
        row = conn.execute(
            "SELECT count(*) AS n FROM ga_budget_reservations WHERE group_id=%s",
            (setup.env.actor.group_id,),
        ).fetchone()
    assert row["n"] == 0
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
    review_request = setup.model.requests[0]
    selected_content = json.loads(review_request.messages[1].content)["artifacts"][0]
    assert selected_content["version_id"] == generated.output["version_id"]
    assert selected_content["sha256"] == generated.output["sha256"]
    assert selected_content["verified_selected_artifact"]["content_passed"] is True
    assert selected_content["verified_selected_artifact"]["checked_literals"] == ["87"]
    assert "不能要求在群回复重复文件内容" in review_request.messages[0].content
    with setup.env.store.transaction(setup.env.actor) as tx:
        assert not tx.conn.execute(
            "SELECT 1 FROM ga_deliveries WHERE group_id=%s", (setup.env.actor.group_id,)
        ).fetchone(), "Validation drafts do not send by themselves"


async def test_explicit_final_artifact_formats_leave_unrequested_images_reusable(toolkit_setup):
    setup = toolkit_setup
    # Previously stored Markdown work can carry a model's inapplicable page count.
    run = await prepare(setup, scope={"outputs": [{"format": "md", "min_units": 1}]})
    report = await create_document(run)
    report_id = report.output["version_id"]
    image_path = run.workspace / "auxiliary.png"
    Image.new("RGB", (64, 48), "navy").save(image_path)
    image = await call(
        run, "artifact_from_workspace", {"path": image_path.name, "name": image_path.name}
    )
    assert image.success
    image_id = image.output["version_id"]
    selection = await call(run, "artifact_deliver", {"version_ids": [report_id]}, "report-only")
    assert selection.success

    mixed = await call(
        run, "artifact_deliver", {"version_ids": [report_id, image_id]}, "report-plus-auxiliary"
    )
    assert mixed.error_code == "unexpected_final_artifact_format"
    assert mixed.effect_state == EffectState.NOT_STARTED
    assert failure_disposition(mixed) == ToolFailureDisposition.REPAIR
    assert mixed.message and image_id in mixed.message and "(png)" in mixed.message
    assert "(md)" in mixed.message
    inventory = setup.env.registry.outputs(setup.env.actor, run.work.id, run.work.goal_revision)
    assert {item["version_id"]: item["purpose"] for item in inventory} == {
        report_id: "result",
        image_id: "candidate",
    }
    version, content = setup.env.registry.read(setup.env.actor, image_id)
    assert version.format == "png" and content == image_path.read_bytes()

    # An existing selection may have been recorded before the publication check existed.
    with setup.env.store.transaction(setup.env.actor) as tx:
        tx.conn.execute(
            "UPDATE ga_work_outputs SET purpose='result' WHERE group_id=%s AND work_id=%s "
            "AND goal_revision=%s AND version_id=%s",
            (setup.env.actor.group_id, run.work.id, run.work.goal_revision, image_id),
        )
    snapshot = candidate(run, "The report is ready.", receipts=(report, image, selection))
    with pytest.raises(DomainError, match="only the requested final formats") as rejected:
        await setup.toolkit.validate(run.work, run.lease, setup.env.actor, snapshot, run.workspace)
    assert rejected.value.code == "validation_failed"
    assert not setup.model.requests
    repaired = await call(run, "artifact_deliver", {"version_ids": [report_id]}, "repair-selection")
    assert repaired.success
    validated = await setup.toolkit.validate(
        run.work, run.lease, setup.env.actor, snapshot, run.workspace
    )
    assert [
        draft.payload_ref
        for draft in validated.deliveries
        if draft.payload_ref.startswith("artifact:")
    ] == ["artifact:" + report_id]


@pytest.mark.parametrize("formats", [[], ["text"], ["md", "jpeg"]])
async def test_artifact_deliver_preserves_permitted_document_and_jpeg_pair(toolkit_setup, formats):
    setup = toolkit_setup
    run = await prepare(setup, scope={"outputs": [{"format": name} for name in formats]})
    report = await create_document(run)
    stream = io.BytesIO()
    Image.new("RGB", (64, 48), "navy").save(stream, "JPEG")
    image_bytes = stream.getvalue()
    scope = ArtifactScope(setup.env.actor.group_id, run.work.id, setup.env.actor.member_id)
    image = setup.env.artifacts.create(scope, "previous-filename.jpg", image_bytes)
    decoded, _ = inspect_image(image_bytes)
    assert decoded.source_sha256 == image.sha256 and decoded.format == "jpeg"
    setup.env.artifacts.record_validation(
        scope,
        image.file_id,
        image.version_id,
        ValidationRecord(
            image.file_id,
            image.version_id,
            image.sha256,
            image.format,
            structural_passed=True,
            content_passed=True,
            media_decoded=True,
        ),
    )
    setup.env.registry.register(setup.env.actor, run.lease, image, purpose="candidate")
    versions = [report.output["version_id"], image.version_id]
    selection = await call(run, "artifact_deliver", {"version_ids": versions})
    assert selection.success
    validated = await setup.toolkit.validate(
        run.work,
        run.lease,
        setup.env.actor,
        candidate(run, receipts=(report, selection)),
        run.workspace,
    )
    assert {
        draft.payload_ref
        for draft in validated.deliveries
        if draft.payload_ref.startswith("artifact:")
    } == {"artifact:" + identifier for identifier in versions}
    assert "artifact:jpg" in validated.checks
    assert ("artifact:jpeg" in validated.checks) == ("jpeg" in formats)


async def test_review_accepts_exact_citation_and_targets_the_unverified_artifact_locator(
    toolkit_setup,
):
    setup = toolkit_setup
    run = await prepare(
        setup,
        goal="Verify a public reference and deliver the synthetic report as Markdown",
        scope={
            "outputs": [{"format": "md"}],
            "research_required": True,
            "required_tools": ["web_fetch"],
        },
    )
    url = "https://example.com/team's-guide"
    fetched = await call(run, "web_fetch", {"url": url})
    assert fetched.success and fetched.output["source"]["url"] == url

    async def choose(citation, name):
        document = await call(
            run,
            "document_generate",
            {
                "name": name + ".md",
                "kind": "report",
                "title": "Fixture findings",
                "sections": [{"title": "Reference", "paragraphs": ["Evidence link " + citation]}],
            },
            key="generate-" + name,
        )
        assert document.success, document
        selection = await call(
            run,
            "artifact_deliver",
            {"version_ids": [document.output["version_id"]]},
            key="deliver-" + name,
        )
        assert selection.success
        return document

    good = await choose(url, "verified")
    answer = candidate(run, "The report is ready.", receipts=(fetched,))
    validated = await setup.toolkit.validate(
        run.work, run.lease, setup.env.actor, answer, run.workspace
    )
    assert any(
        draft.payload_ref == "artifact:" + good.output["version_id"]
        for draft in validated.deliveries
    )

    wrong = "https://example.com/team's-guess"
    invalid = await choose(wrong, "unverified")
    with pytest.raises(UnverifiedCitation) as rejected:
        await setup.toolkit.validate(run.work, run.lease, setup.env.actor, answer, run.workspace)
    details = rejected.value.details
    assert details["surface"] == invalid.output["version_id"]
    assert details["unverified_urls"] == [wrong]
    assert details["verified_sources"] == [
        {"source_id": fetched.output["source"]["source_id"], "url": url}
    ]
    assert len(setup.model.requests) == 1  # Wrong citation never reaches semantic review.


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


async def test_wrong_artifact_reference_returns_model_to_existing_copy_and_delivery(toolkit_setup):
    setup = toolkit_setup
    source_run = await prepare(setup)
    source_version = (await create_document(source_run)).output["version_id"]
    run = await prepare(setup, scope={"outputs": [{"format": "md", "expected_text": ["87"]}]})
    copies = []

    def propose(tool_name, key, **arguments):
        return ModelReply(
            tool_calls=(ModelToolCall(id=key, name=tool_name, arguments=arguments),),
            finish_reason="tool_calls",
            usage=TokenUsage(input_tokens=2, output_tokens=1),
        )

    class ArtifactRepairFixture:
        name = "offline-artifact-repair"

        async def complete(self, request):
            observations = [
                json.loads(row.content) for row in request.messages if row.role == "tool"
            ]
            if not observations:
                return propose("artifact_import", "known-version", version_id=source_version)
            seen = observations[-1]
            if len(observations) == 1:
                copies.append(seen["output"]["files"][0])
                return propose("artifact_import", "wrong-reference", version_id="nonexistent")
            if len(observations) == 2:
                assert seen["error_code"] == "not_found" and seen["effect_state"] == "not_started"
                assert seen["failure"]["disposition"] == "repair"
                return propose(
                    "document_read",
                    "read-known-copy",
                    file_id=copies[-1]["file_id"],
                    version_id=copies[-1]["version_id"],
                )
            if len(observations) == 3:
                assert "87" in json.dumps(seen["output"])
                return propose(
                    "document_generate",
                    "write-result",
                    name="result.md",
                    kind="report",
                    title="Result",
                    sections=[{"title": "Findings", "paragraphs": ["Original value: 87."]}],
                )
            if len(observations) == 4:
                return propose(
                    "artifact_deliver", "select-result", version_ids=[seen["output"]["version_id"]]
                )
            return ModelReply(
                text="The verified report contains the original value 87.",
                usage=TokenUsage(input_tokens=2, output_tokens=1),
            )

    engine = LangGraphEngine(ArtifactRepairFixture(), run.gateway)
    result = await engine.wait(await engine.start(run.prepared.request))
    assert result.state == RunState.CANDIDATE, (
        result.error_code,
        [(row.tool_name, row.error_code, row.effect_state) for row in result.receipts],
    )
    assert [receipt.tool_name for receipt in result.receipts] == [
        "artifact_import",
        "artifact_import",
        "document_read",
        "document_generate",
        "artifact_deliver",
    ]
    validated = await setup.toolkit.validate(
        run.work, run.lease, setup.env.actor, result, run.workspace
    )
    assert "artifact:md" in validated.checks
    assert "artifact:" + result.receipts[-2].output["version_id"] in {
        delivery.payload_ref for delivery in validated.deliveries
    }
    with setup.env.store.transaction(setup.env.actor) as tx:
        assert tx.get_work(run.work.id).input_versions == [source_version]


async def test_quoted_screenshot_prepared_for_ocr_when_appearance_identity_is_denied(toolkit_setup):
    env = toolkit_setup.env
    image = attached(env, kind="image", name="quoted-notice.png")
    goal = (
        f"读取附件截图 {image.id} 中的公告，识别值班人的可见姓名和展架编号。"
        "若图中另有一名人物，判断是否与值班人为同一人。"
    )
    request = event(env, goal, kind=EventKind.QUOTE, reply_to=image.id)
    with env.store.transaction(env.actor) as tx:
        tx.ingest_event(request, "fixture:" + request.id, "1", 0)
        work = tx.create_work(
            goal,
            request.id,
            scope={
                "source_fingerprints": {
                    item.id: tx.event_fingerprint(item.id) for item in (request, image)
                },
                "attachment_event_ids": [image.id],
                "work_capabilities": ["image_analysis"],
            },
        )
        lease = tx.claim_work(work.id, "toolkit-fixture", ttl_seconds=300)
        work = tx.get_work(work.id)
    workspace = env.root / work.id
    workspace.mkdir()
    prepared = await toolkit_setup.toolkit.prepare(work, lease, env.actor, workspace)
    objective = json.loads(prepared.request.objective)
    (reference,) = objective["input_files"]
    assert reference["status"] == "ready" and reference["source_event_id"] == image.id
    gateway = prepared.engine.gateway
    scope = ToolScope(
        env.actor.group_id,
        work.id,
        env.actor,
        workspace,
        prepared.request.allowed_tools,
        request.id,
    )
    gateway.bind_scope(scope)
    run = SimpleNamespace(gateway=gateway, scope=scope)
    handles = {key: reference[key] for key in ("file_id", "version_id")}

    denied = await call(
        run,
        "image_analyze",
        {
            "images": [handles],
            "prompt": "请确认这位男士的真实姓名。",
            "route_id": "unneeded-for-refused-operation",
        },
    )
    assert not denied.success and denied.error_code == "face_identification_unavailable"
    assert denied.effect_state == EffectState.NO_EFFECT
    observed = await call(run, "media_ocr", handles)
    assert observed.success, observed
    assert observed.output["source"]["source_event_id"] == image.id
    assert observed.output["pages"][0]["text"] == "值班：测试乙，展架 L-187"


async def test_worker_reuses_read_attachment_instead_of_waiting_for_another_upload(toolkit_setup):
    setup = toolkit_setup
    source_run = await prepare(setup)
    source_version = (await create_document(source_run)).output["version_id"]
    run = await prepare(
        setup,
        goal="Revise the supplied report into a new Markdown file",
        scope={
            "outputs": [{"format": "md", "expected_text": ["87"]}],
            "required_tools": ["workspace_read_text"],
        },
    )
    round_number = 0
    usage = TokenUsage(input_tokens=2, output_tokens=1)

    def propose(tool_name, **arguments):
        return ModelReply(
            tool_calls=(ModelToolCall(id=str(round_number), name=tool_name, arguments=arguments),),
            finish_reason="tool_calls",
            usage=usage,
        )

    class WorkerFixture:
        name = "offline-prepared-attachment"

        async def complete(self, request):
            nonlocal round_number
            round_number += 1
            if round_number == 1:
                return propose("artifact_import", version_id=source_version)
            latest = request.messages[-1]
            data = json.loads(latest.content)
            if round_number == 2:
                imported = data["output"]["files"][0]
                assert imported["from_version"] == source_version
                return propose(
                    "artifact_export",
                    file_id=imported["file_id"],
                    version_id=imported["version_id"],
                    path="source.md",
                )
            if round_number == 3:
                assert data["output"]["path"] == "source.md"
                return propose("workspace_read_text", path="source.md")
            if round_number == 4:
                assert "87" in data["output"]["text"]
                return ModelReply(text="Please upload the original.", needs_input=True, usage=usage)
            if round_number == 5:
                assert data["code"] == "input_request_requires_evidence"
                return propose(
                    "request_input",
                    question="Please upload the original again.",
                    missing=[{"kind": "attachment", "reference": "source.md"}],
                )
            if round_number == 6:
                assert data["error_code"] == "input_already_available"
                return propose(
                    "document_generate",
                    name="revised.md",
                    kind="report",
                    title="Revised report",
                    sections=[{"title": "Findings", "paragraphs": ["The original value was 87."]}],
                )
            if round_number == 7:
                return propose("artifact_deliver", version_ids=[data["output"]["version_id"]])
            assert data["success"]
            return ModelReply(text="The revised report preserves the value 87.", usage=usage)

    engine = LangGraphEngine(WorkerFixture(), run.gateway)
    result = await engine.wait(await engine.start(run.prepared.request))
    assert result.state == RunState.CANDIDATE and result.checkpoint.input_request is None, (
        result.error_code,
        [(row.tool_name, row.error_code) for row in result.receipts],
    )
    assert round_number == 8
    rejected = next(receipt for receipt in result.receipts if receipt.tool_name == "request_input")
    assert (
        rejected.control is None and failure_disposition(rejected) == ToolFailureDisposition.REPAIR
    )
    validated = await setup.toolkit.validate(
        run.work, run.lease, setup.env.actor, result, run.workspace
    )
    assert "artifact:md" in validated.checks


@pytest.mark.parametrize("outcome", ["answered", "private", "malformed", "timeout", "withdrawn"])
async def test_request_input_requires_current_necessary_missing_evidence(toolkit_setup, outcome):
    setup = toolkit_setup
    page = "https://example.com/workshop-program"
    instructions = (
        "Prepare a public workshop brief. Link to the program's official page for registration."
        if outcome != "private"
        else "Reconcile the registrations, using my private payment ledger as the authority."
    )
    source(setup.env, "Unrelated neighboring discussion")
    run = await prepare(setup, goal="Prepare the requested report", request_text=instructions)
    fetched = await call(run, "web_fetch", {"url": page})
    assert fetched.success
    usage = TokenUsage(input_tokens=20, output_tokens=10)

    class InputReviewer:
        name = "fixture-input-necessity-check"
        requests = []

        async def complete(self, request):
            self.requests.append(request)
            assert not request.tools
            assert request.max_output_tokens == run.prepared.request.budget.max_output_tokens
            payload = json.loads(request.messages[-1].content)
            provided = payload["human_requirements"]["requests"]
            assert [item["source_event_id"] for item in provided] == [run.event.id]
            assert provided[0]["text"] == instructions
            assert "Unrelated neighboring discussion" not in request.messages[-1].content
            assert payload["ready_input_files"] == []
            assert "web_fetch" in payload["authorized_tools"]
            assert page in payload["recent_successful_tools"][0]["output"]
            assert payload["proposed_request"]["missing"] == missing
            if outcome == "timeout":
                raise TimeoutError()
            if outcome == "withdrawn":
                setup.toolkit.memory.forget(setup.env.actor, event_ids=[run.event.id])
            verdict = {"needs_requester": outcome in {"private", "withdrawn"}}
            if outcome == "malformed":
                verdict["needs_requester"] = "true"
            verdict["reason"] = (
                "The private ledger has not been received."
                if outcome in {"private", "withdrawn"}
                else "Use the official program page already confirmed by web_fetch."
            )
            return ModelReply(text=json.dumps(verdict), usage=usage)

    reviewer = InputReviewer()
    setup.toolkit.model = reviewer
    missing = (
        [{"kind": "attachment", "reference": "payment-ledger.xlsx"}]
        if outcome == "private"
        else [{"kind": "information", "field": "registration link type"}]
    )
    worker = ScriptedFakeModel(
        [
            ModelReply(
                tool_calls=(
                    ModelToolCall(
                        id="typed-input-proposal",
                        name="request_input",
                        arguments={
                            "question": "Please confirm the required input.",
                            "missing": missing,
                        },
                    ),
                ),
                finish_reason="tool_calls",
                usage=usage,
            ),
            ModelReply(
                text="Prepared the workshop brief with the official registration link.", usage=usage
            ),
        ]
    )
    engine = LangGraphEngine(worker, run.gateway)
    result = await engine.wait(await engine.start(run.prepared.request))
    assert len(reviewer.requests) == 1
    checked = next(item for item in result.receipts if item.tool_name == "request_input")
    if outcome == "private":
        assert checked.control and result.state == RunState.WAITING_INPUT
        assert result.checkpoint.input_request.required_inputs == ("payment-ledger.xlsx",)
        assert len(worker.requests) == 1
    elif outcome == "withdrawn":
        assert checked.control is None and result.checkpoint.input_request is None
        assert checked.error_code in {"stale_lease", "source_unavailable"}
        assert failure_disposition(checked) == ToolFailureDisposition.FENCE
        assert len(worker.requests) == 1
    else:
        assert checked.control is None and result.checkpoint.input_request is None
        assert result.state == RunState.CANDIDATE and len(worker.requests) == 2
        assert failure_disposition(checked) == ToolFailureDisposition.REPAIR
        if outcome == "answered":
            assert checked.error_code == "input_not_required"
            assert "official program page" in worker.requests[1].messages[-1].content
        else:
            assert checked.error_code == "input_check_unavailable"
    with setup.env.store.connect() as conn:
        charge = conn.execute(
            "SELECT state FROM ga_budget_reservations WHERE group_id=%s AND work_id=%s "
            "AND operation='text'",
            (setup.env.actor.group_id, run.work.id),
        ).fetchone()
    assert charge["state"] == ("unknown" if outcome == "timeout" else "settled")


@pytest.mark.parametrize("phase", ["lookup_timeout", "after_scope_commit"])
async def test_artifact_import_keeps_ambiguous_failures_for_reconciliation(
    toolkit_setup, monkeypatch, phase
):
    setup = toolkit_setup
    source_run = await prepare(setup)
    source_version = (await create_document(source_run)).output["version_id"]
    run = await prepare(setup)

    if phase == "lookup_timeout":

        def interrupted_lookup(*_args):
            raise QueryCanceled("Synthetic database statement timeout")

        monkeypatch.setattr(setup.env.registry, "consume_in_tx", interrupted_lookup)
    else:

        async def failed_copy(*_args):
            raise DomainError("not_found", "Synthetic failure after the scope UPDATE committed")

        monkeypatch.setattr(setup.inputs, "import_for_work", failed_copy)

    receipt = await call(run, "artifact_import", {"version_id": source_version})
    assert receipt.effect_state == EffectState.UNKNOWN
    assert failure_disposition(receipt) == ToolFailureDisposition.RECONCILE
    with setup.env.store.transaction(setup.env.actor) as tx:
        expected = [source_version] if phase == "after_scope_commit" else []
        assert tx.get_work(run.work.id).input_versions == expected


@pytest.mark.parametrize("missing_path", ["reports/absent.md", "absent-folder/result.md"])
async def test_missing_workspace_output_repairs_in_same_run(toolkit_setup, missing_path):
    setup = toolkit_setup
    run = await prepare(setup, scope={"outputs": [{"format": "md"}]})
    report = "# Notice\n\nThe synthetic workshop opens at 09:30.\n"
    (run.workspace / "reports").mkdir()
    good_path = "reports/result.md"
    (run.workspace / good_path).write_text(report)
    targets = {"path": missing_path, "name": "result.md"}
    usage = TokenUsage(input_tokens=10, output_tokens=5)

    def proposal(name, call_id, arguments):
        return ModelReply(
            tool_calls=(ModelToolCall(name=name, id=call_id, arguments=arguments),),
            finish_reason="tool_calls",
            usage=usage,
        )

    class Worker:
        name = "offline-workspace-path-repair"
        calls = 0

        async def complete(self, request):
            self.calls += 1
            if self.calls == 1:
                return proposal("artifact_from_workspace", "missing-output", targets)
            observed = json.loads(request.messages[-1].content)
            if self.calls == 2:
                assert observed["error_code"] == "invalid_path"
                assert observed["effect_state"] == "not_started"
                assert "relative to this work's workspace" in observed["message"]
                assert not setup.env.registry.outputs(
                    setup.env.actor, run.work.id, run.work.goal_revision
                )
                return proposal(
                    "artifact_from_workspace", "actual-output", {**targets, "path": good_path}
                )
            if self.calls == 3:
                assert observed["success"]
                return proposal(
                    "artifact_deliver",
                    "select-output",
                    {"version_ids": [observed["output"]["version_id"]]},
                )
            assert observed["success"]
            return ModelReply(text="The workshop notice is ready.", usage=usage)

    worker = Worker()
    engine = LangGraphEngine(worker, run.gateway)
    request = run.prepared.request.model_copy(
        update={"visible_tools": frozenset({"artifact_from_workspace", "artifact_deliver"})}
    )
    result = await engine.wait(await engine.start(request))
    assert result.state == RunState.CANDIDATE and result.checkpoint.safe_to_resume
    assert worker.calls == 4
    rejected, adopted, selected = result.receipts
    assert failure_disposition(rejected) == ToolFailureDisposition.REPAIR
    assert adopted.success and selected.success
    delayed = run.workspace / missing_path
    delayed.parent.mkdir(exist_ok=True)
    delayed.write_text(report)
    replay = await run.gateway.invoke(
        "artifact_from_workspace",
        targets,
        ToolContext(run.scope, "after-path-fix", rejected.idempotency_key, asyncio.Event()),
    )
    assert replay.replayed and replay.effect_state == EffectState.NOT_STARTED
    assert replay.error_code == "invalid_path"
    outputs = setup.env.registry.outputs(setup.env.actor, run.work.id, run.work.goal_revision)
    (output,) = outputs
    assert output["version_id"] == adopted.output["version_id"] and output["purpose"] == "result"
    validated = await setup.toolkit.validate(
        run.work, run.lease, setup.env.actor, result, run.workspace
    )
    assert "artifact:md" in validated.checks


async def test_workspace_adoption_preserves_uncertainty_after_catalog_commit(
    toolkit_setup, monkeypatch
):
    setup = toolkit_setup
    run = await prepare(setup)
    path = run.workspace / "result.md"
    path.write_text("# Synthetic fixture\n")
    register = setup.env.registry.register
    committed = []

    def interrupted_register(actor, lease, version, **kwargs):
        register(actor, lease, version, **kwargs)
        committed.append(version.version_id)
        raise DomainError("invalid_path", "Synthetic failure after catalog commit")

    monkeypatch.setattr(setup.env.registry, "register", interrupted_register)
    arguments = {"path": path.name, "name": path.name}
    receipt = await call(run, "artifact_from_workspace", arguments)
    assert receipt.error_code == "invalid_path" and receipt.effect_state == EffectState.UNKNOWN
    assert failure_disposition(receipt) == ToolFailureDisposition.RECONCILE
    replay = await call(run, "artifact_from_workspace", arguments)
    assert replay.replayed and replay.effect_state == EffectState.UNKNOWN
    outputs = setup.env.registry.outputs(setup.env.actor, run.work.id, run.work.goal_revision)
    assert len(committed) == 1 and [item["version_id"] for item in outputs] == committed


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
    assert denied.effect_state == EffectState.NOT_STARTED
    assert failure_disposition(denied) == ToolFailureDisposition.FENCE
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
            run,
            "The source is https://example.com/source",
            (fetched,),
        ),
        run.workspace,
    )
    assert accepted.checks["result"] and len(setup.model.requests) == 1
    with pytest.raises(DomainError, match="no successful research provenance"):
        await setup.toolkit.validate(
            run.work,
            run.lease,
            setup.env.actor,
            candidate(run, "Unrelated source https://example.com/invented", (fetched,)),
            run.workspace,
        )


@pytest.mark.parametrize("requested", [False, True])
async def test_concise_research_does_not_publish_unsolicited_code_examples(
    toolkit_setup, requested
):
    setup = toolkit_setup
    text = "请检索公开资料，简短说明两个操作并给链接和访问时间。"
    if requested:
        text += "请给一个代码示例。"
    run = await prepare(setup, scope={"research_required": True}, goal=text, request_text=text)
    fetched = await call(run, "web_fetch", {"url": "https://example.com/source"})
    assert fetched.success
    answer = (
        "操作说明。\n```python\nitems = [3, 1, 2]\nprint(sorted(items))  # [1, 2, 3]\n```\n"
        + fetched.output["source"]["citation"]
    )
    if not requested:
        with pytest.raises(DomainError, match="not extra examples"):
            await setup.toolkit.validate(
                run.work,
                run.lease,
                setup.env.actor,
                candidate(run, answer, (fetched,)),
                run.workspace,
            )
        assert not setup.model.requests
    else:
        result = await setup.toolkit.validate(
            run.work, run.lease, setup.env.actor, candidate(run, answer, (fetched,)), run.workspace
        )
        assert result.checks["result"] and len(setup.model.requests) == 1
        assert "逐项核对数量、元素、数值及输出" in setup.model.requests[0].messages[0].content


@pytest.mark.parametrize("configured,effective", [(0, 0), (1, 1), (5, 2)])
async def test_execution_retry_policy_cannot_exceed_original_work_grant(
    toolkit_setup, configured, effective
):
    setup = toolkit_setup
    setup.toolkit.config = setup.toolkit.config.model_copy(
        update={
            "execution": setup.toolkit.config.execution.model_copy(
                update={"additional_retry_limit": configured}
            )
        }
    )
    # The work's original default grant allows two additional attempts. A later
    # service setting can reduce it (including zero), never enlarge that grant.
    run = await prepare(setup)
    assert run.prepared.request.budget.additional_retry_limit == effective


async def test_current_unlimited_policy_overrides_legacy_cumulative_caps(toolkit_setup):
    setup = toolkit_setup
    env = setup.env
    event = source(env)
    with env.store.transaction(env.actor) as tx:
        work = tx.create_work(
            "Synthetic continued work",
            event.id,
            budget={"max_model_rounds": 24, "max_tool_calls": 60, "max_active_seconds": 1200},
        )
        lease = tx.claim_work(work.id, "synthetic-continued-work", ttl_seconds=300)
        work = tx.get_work(work.id)
    workspace = env.root / work.id
    workspace.mkdir()
    prepared = await setup.toolkit.prepare(work, lease, env.actor, workspace)
    assert prepared.request.budget.max_rounds is None
    assert prepared.request.budget.max_tool_calls is None
    assert prepared.request.budget.max_active_seconds is None
    assert prepared.request.budget.max_tokens is None
    assert work.budget["max_model_rounds"] == 24  # Historical grant stays auditable.


@pytest.mark.parametrize("configured,failures,expected_calls", [(0, 1, 1), (5, 2, 3)])
async def test_semantic_model_retries_respect_original_grant_and_keep_unknown_charges(
    toolkit_setup, configured, failures, expected_calls
):
    setup = toolkit_setup
    setup.toolkit.config = setup.toolkit.config.model_copy(
        update={
            "execution": setup.toolkit.config.execution.model_copy(
                update={"additional_retry_limit": configured}
            )
        }
    )
    run = await prepare(setup, goal="给出一项简短的合成解释")
    calls = []

    async def transient(request):
        calls.append(request)
        if len(calls) <= failures:
            raise DomainError("provider_unavailable", "Synthetic model transport failure")
        return ModelReply(
            text='{"content_passed":true,"group_reply_passed":true,"reason":"Synthetic content verified"}',
            usage=TokenUsage(input_tokens=10, output_tokens=5),
        )

    setup.model.complete = transient
    if configured == 0:
        with pytest.raises(DomainError, match="Synthetic model transport failure"):
            await setup.toolkit.validate(
                run.work, run.lease, setup.env.actor, candidate(run), run.workspace
            )
    else:
        result = await setup.toolkit.validate(
            run.work, run.lease, setup.env.actor, candidate(run), run.workspace
        )
        assert result.checks["result"]
    assert len(calls) == expected_calls
    with setup.env.store.transaction(setup.env.actor) as tx:
        states = tx.conn.execute(
            "SELECT state FROM ga_budget_reservations WHERE group_id=%s AND work_id=%s",
            (setup.env.actor.group_id, run.work.id),
        ).fetchall()
    assert sum(row["state"] == "unknown" for row in states) == failures
    assert sum(row["state"] == "settled" for row in states) == (expected_calls - failures)


@pytest.mark.parametrize("columns", [False, True])
async def test_actual_generation_normalizes_default_numeric_vector_but_keeps_explicit_columns(
    toolkit_setup, columns
):
    setup = toolkit_setup
    run = await prepare(setup, scope={"outputs": [{"format": "md"}]})
    table = {"headers": ["Theta", "Sigma", "Kappa"], "rows": [["1.20", "-0.25", "+2.00"]]}
    if columns:
        table["layout"] = "columns"
    generated = await call(
        run,
        "document_generate",
        {
            "name": "records.md",
            "kind": "report",
            "title": "Synthetic measurements",
            "sections": [{"title": "Data", "paragraphs": [], "tables": [table]}],
        },
    )
    assert generated.success
    content = setup.env.artifacts.read(
        ArtifactScope(setup.env.actor.group_id, run.work.id, setup.env.actor.member_id),
        generated.output["file_id"],
        generated.output["version_id"],
    ).decode()
    rows = [
        tuple(cell.strip() for cell in line.strip().strip("|").split("|"))
        for line in content.splitlines()
        if line.lstrip().startswith("|")
    ]
    if columns:
        assert rows[0] == ("Theta", "Sigma", "Kappa")
        assert rows[2] == ("1.20", "-0.25", "+2.00")
    else:
        assert rows[2:] == [("Theta", "1.20"), ("Sigma", "-0.25"), ("Kappa", "+2.00")]
    assert not setup.http_calls and not setup.model.requests


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
            candidate(run, "草稿已准备好，按你的要求暂不发送文件。", (generated, selected)),
            run.workspace,
        )
    setup.model.passed = True
    result = await setup.toolkit.validate(
        run.work,
        run.lease,
        setup.env.actor,
        candidate(run, "草稿已准备好，按你的要求暂不发送文件。", (generated, selected)),
        run.workspace,
    )
    assert len(result.deliveries) == 1 and result.deliveries[0].payload_ref.startswith("text:")
    assert "暂不发送" in result.deliveries[0].payload_ref


async def test_semantic_review_evidence_is_version_bound_redacted_and_privacy_removable(
    toolkit_setup,
):
    setup = toolkit_setup
    run = await prepare(setup, scope={"outputs": [{"format": "md"}], "delivery_artifacts": False})
    generated = await create_document(run)
    selected = await call(
        run, "artifact_deliver", {"version_ids": [generated.output["version_id"]]}
    )
    secret = "sk-" + "synthetic_only_" * 3
    reply = ModelReply(
        text=json.dumps(
            {
                "content_passed": False,
                "group_reply_passed": True,
                "reason": "Missing footer; " + secret,
            }
        ),
        usage=TokenUsage(input_tokens=10, output_tokens=5),
    )

    async def reviewer(request):
        return reply

    setup.model.complete = reviewer
    with pytest.raises(DomainError, match="Missing footer") as caught:
        await setup.toolkit.validate(
            run.work,
            run.lease,
            setup.env.actor,
            candidate(run, receipts=(generated, selected)),
            run.workspace,
        )
    assert secret not in caught.value.message
    with setup.env.store.transaction(setup.env.actor) as tx:
        evidence = tx.get_work(run.work.id).scope["_runtime_semantic_reviews"][-1]
        audits = tx.conn.execute(
            "SELECT details FROM ga_audit_events WHERE group_id=%s AND resource_ref=%s AND action='semantic_review_completed'",
            (setup.env.actor.group_id, run.work.id),
        ).fetchall()
    assert evidence["passed"] is False and evidence["accepted"] is False
    assert evidence["finish_reason"] == "stop"
    assert evidence["goal_revision"] == run.work.goal_revision
    assert evidence["version_ids"] == [generated.output["version_id"]]
    assert evidence["attempt_id"] == run.lease.attempt_id
    assert (
        evidence["response_sha256"] == hashlib.sha256(reply.model_dump_json().encode()).hexdigest()
    )
    assert secret not in json.dumps(evidence)
    assert len(audits) == 1 and "reason" not in audits[0]["details"]
    reply = ModelReply(
        text=json.dumps(
            {
                "content_passed": True,
                "group_reply_passed": True,
                "reason": "Corrected final candidate",
            }
        ),
        usage=TokenUsage(input_tokens=10, output_tokens=5),
    )
    await setup.toolkit.validate(
        run.work,
        run.lease,
        setup.env.actor,
        candidate(run, receipts=(generated, selected)),
        run.workspace,
    )
    with setup.env.store.transaction(setup.env.actor) as tx:
        evidence = tx.get_work(run.work.id).scope["_runtime_semantic_reviews"]
    assert [item["passed"] for item in evidence] == [False, True]
    setup.toolkit.memory.forget(setup.env.actor, event_ids=[run.event.id])
    with setup.env.store.transaction(setup.env.actor) as tx:
        assert "_runtime_semantic_reviews" not in tx.get_work(run.work.id).scope


async def test_preupgrade_reminder_work_keeps_recipient_scope_and_literal_validation(toolkit_setup):
    setup, env = toolkit_setup, toolkit_setup.env
    authorization = source(env, "一小时后只提醒我检查作业，不需要代我检查")
    now = datetime.now(UTC)
    scheduler = Scheduler(env.store)
    with env.store.transaction(env.actor) as tx:
        fingerprints = {authorization.id: tx.event_fingerprint(authorization.id)}
    rule = scheduler.create(
        env.actor,
        "Synthetic reminder",
        "检查作业",
        EveryTiming(interval_seconds=3600),
        now=now,
        scope={
            "reminder_only": True,
            "reminder_body_literal": True,
            "reminder_body_source_event_id": authorization.id,
            "source_fingerprints": fingerprints,
        },
    )
    with env.store.transaction(env.actor) as tx:
        # Represent an already-admitted WorkOrder from the previous scheduler.
        # A fresh wakeup would enqueue this verified literal directly.
        row = scheduler._row(tx, rule.id)
        due_at = now + timedelta(hours=1)
        occurrence = scheduler._occurrence(tx, row, due_at)
        assert occurrence is not None
        work_id = scheduler._admit_goal(tx, row, occurrence, due_at, fingerprints)
        tx.conn.execute(
            "UPDATE ga_schedule_occurrences SET state='enqueued',work_id=%s "
            "WHERE group_id=%s AND id=%s",
            (work_id, tx.group_id, occurrence["id"]),
        )
        lease = tx.claim_work(work_id, "reminder-fixture", ttl_seconds=300)
        work = tx.get_work(work_id)
    workspace = env.root / work.id
    workspace.mkdir()
    prepared = await setup.toolkit.prepare(work, lease, env.actor, workspace)
    objective = json.loads(prepared.request.objective)
    assert objective["execution_intent"] == "reminder" and objective["goal"] == "检查作业"
    assert objective["reminder_body"] == "检查作业"
    assert objective["agent_name"] == "Pot" and objective["bot_badge"] == "【Pot】"
    assert any("你的名字是Pot" in item for item in objective["instructions"])
    assert "发送提醒本身不需要" in objective["instructions"][0]
    assert "不要为了被提醒的事项调用 request_input" in objective["instructions"][0]
    assert not objective["input_files"] and prepared.request.allowed_tools == frozenset()
    reminder_scope = ToolScope(
        env.actor.group_id, work.id, env.actor, workspace, frozenset(), authorization.id
    )
    prepared.engine.gateway.bind_scope(reminder_scope)
    forbidden_input = await prepared.engine.gateway.invoke(
        "request_input",
        {"question": "请提供清单文件", "required_inputs": ["清单"]},
        ToolContext(reminder_scope, "forbidden-input", "forbidden-input", asyncio.Event()),
    )
    assert not forbidden_input.success and forbidden_input.control is None
    assert forbidden_input.error_code == "permission_denied"
    run = SimpleNamespace(work=work, lease=lease, workspace=workspace, prepared=prepared)
    with pytest.raises(DomainError, match="preserve reminder_body verbatim"):
        await setup.toolkit.validate(
            work, lease, env.actor, candidate(run, text="提醒：该检查一下作业了。"), workspace
        )
    assert not setup.model.requests
    result = await setup.toolkit.validate(
        work, lease, env.actor, candidate(run, text="提醒：该检查作业了。"), workspace
    )
    review_request = setup.model.requests[-1]
    assert json.loads(review_request.messages[1].content)["execution_intent"] == "reminder"
    assert (
        "收件人" in review_request.messages[0].content
        and "提醒" in review_request.messages[0].content
    )
    assert (
        len(result.deliveries) == 1
        and result.deliveries[0].payload_ref == "text:提醒：该检查作业了。"
    )
    with env.store.transaction(env.actor) as tx:
        current = tx.get_work(work.id)
        current = tx.transition_work(
            work.id, WorkState.VALIDATING, expected_revision=current.revision, lease=lease
        )
        tx.record_validation(work.id, lease, "synthetic-reminder-validation")
        tx.transition_work(
            work.id, WorkState.DELIVERING, expected_revision=current.revision, lease=lease
        )
        env.state.enqueue_bundle_in_tx(tx, lease, result.deliveries)
    dispatcher = Dispatcher(env.state, env.service, lambda _: env.actor, env.channel, env.registry)
    assert await dispatcher.tick() == 1
    sent = [json.loads(path.read_text()) for path in (env.root / "channel/outgoing").glob("*.json")]
    assert len(sent) == 1 and sent[0]["payload"]["text"] == "【Pot】提醒：该检查作业了。"
    assert not setup.http_calls


@pytest.mark.parametrize(
    "scope",
    [
        {},
        {"reminder_only": True},
        {"reminder_only": False, "origin_schedule_id": "synthetic", "derived_depth": 1},
        {"reminder_only": "true", "origin_schedule_id": "synthetic", "derived_depth": 1},
        {"reminder_only": True, "origin_schedule_id": "", "derived_depth": 1},
        {"reminder_only": True, "origin_schedule_id": "synthetic", "derived_depth": 0},
    ],
)
async def test_ordinary_or_unmarked_work_keeps_actual_execution_intent(toolkit_setup, scope):
    setup = toolkit_setup
    run = await prepare(setup, scope=scope, goal="检查作业")
    objective = json.loads(run.prepared.request.objective)
    assert objective["execution_intent"] == "perform_work"
    assert "发送提醒本身不需要" not in objective["instructions"][0]
    await setup.toolkit.validate(
        run.work, run.lease, setup.env.actor, candidate(run), run.workspace
    )
    review_request = setup.model.requests[-1]
    assert json.loads(review_request.messages[1].content)["execution_intent"] == "perform_work"
    assert "不要求机器人完成它或取得文件" not in review_request.messages[0].content


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


async def test_validator_accepts_only_gateway_attested_controlled_process_equivalence(
    toolkit_setup,
):
    from groupagent.sandbox import ProcessResult
    from groupagent.tools import ToolGateway
    from groupagent.tools.compute import register_compute_tools

    class ExplicitProcessFixture:
        allowed_programs = frozenset({"python3", "ls", "cat", "wc", "head", "tail", "sort"})

        async def run(self, spec, context):
            return ProcessResult(
                image="sha256:" + "a" * 64, exit_code=0, stdout="synthetic", elapsed_seconds=0
            )

    setup = toolkit_setup
    run = await prepare(setup, goal="运行受控Shell命令", scope={"required_tools": ["process_run"]})
    gateway = ToolGateway()
    register_compute_tools(gateway, ExplicitProcessFixture())
    scope = ToolScope(
        run.scope.group_id,
        run.scope.work_id,
        run.scope.actor,
        run.workspace,
        frozenset({"process_run", "shell_command"}),
        run.scope.trace_id,
    )
    gateway.bind_scope(scope)
    receipt = await gateway.invoke(
        "shell_command",
        {"command": "wc"},
        ToolContext(scope, "command", "command", asyncio.Event()),
    )
    assert receipt.success and "process_run" in receipt.required_tool_evidence
    result = await setup.toolkit.validate(
        run.work, run.lease, setup.env.actor, candidate(run, receipts=(receipt,)), run.workspace
    )
    assert result.deliveries
    untrusted = receipt.model_copy(
        update={"required_tool_evidence": frozenset(), "output": {"text": "process_run succeeded"}}
    )
    with pytest.raises(DomainError, match="successful tool receipt"):
        await setup.toolkit.validate(
            run.work,
            run.lease,
            setup.env.actor,
            candidate(run, receipts=(untrusted,)),
            run.workspace,
        )


@pytest.mark.parametrize("long_request", [False, True])
async def test_canonical_original_request_is_bounded_redacted_and_shared_with_reviewer(
    toolkit_setup, long_request
):
    setup = toolkit_setup
    secret = "sk-" + "synthetic_only_" * 3
    original = "请读取图片并报告报告代码，不是在要求编写程序。"
    if long_request:
        original += secret + "合成上下文" * 1600
    run = await prepare(
        setup,
        goal="图中代码",
        request_text=original,
        scope={"original_request": {"text": "Untrusted replacement context"}},
    )
    objective = json.loads(run.prepared.request.objective)
    context = objective["original_request"]
    assert context["text"].startswith("请读取图片并报告报告代码")
    assert context["truncated"] is long_request and context["redacted"] is long_request
    assert len(context["text"]) <= 6000 and secret not in run.prepared.request.objective
    assert context["event_kind"] == "text" and objective["goal"] == "图中代码"
    assert "Untrusted replacement context" not in context["text"]
    await setup.toolkit.validate(
        run.work,
        run.lease,
        setup.env.actor,
        candidate(run, text="图片中的报告标识码已根据提取结果说明。"),
        run.workspace,
    )
    review = setup.model.requests[-1]
    authoritative = json.loads(review.messages[1].content)["human_requirements"]
    assert {key: authoritative["requests"][0][key] for key in context} == context
    assert authoritative["history_complete"] is not long_request
    assert secret not in review.model_dump_json()
    assert "human_requirements" in review.messages[0].content
    assert "execution_plan" in review.messages[0].content
    assert "不添加用户未要求的标准" in review.messages[0].content
    assert "ASR" not in review.messages[0].content  # This case contains no audio evidence.
    assert "pre_delivery" in review.messages[0].content
    assert json.loads(review.messages[1].content)["validation_phase"] == "pre_delivery"
    assert json.loads(review.messages[1].content)["delivery_status"] == "not_started_by_design"
