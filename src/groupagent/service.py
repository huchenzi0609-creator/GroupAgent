"""An explicitly synthetic, end-to-end vertical slice over real storage and files.

The model and channel are replay fixtures. PostgreSQL, scoped tools, artifact
creation, validation, outbox, and completion gates are the actual implementations.
No credentials, live chat messages or production databases are accessed.
"""

from __future__ import annotations

import asyncio
import json
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal
from uuid import uuid4

from pydantic import Field

from groupagent.artifacts import (
    ArtifactScope,
    ArtifactStore,
    Deck,
    DocumentRenderer,
    DocumentService,
    Report,
    Section,
    Slide,
)
from groupagent.config import AppConfig, GroupConfig
from groupagent.coordinator import Coordinator, Decision, ScriptedDecisionProvider
from groupagent.domain import ActorContext, DeliveryState, EventKind, GroupEvent, Role, WorkState
from groupagent.execution import (
    ExecutionRequest,
    ModelReply,
    ModelToolCall,
    RunState,
    ScopedToolEngine,
    ScriptedFakeModel,
    TokenUsage,
)
from groupagent.persistence import PostgresStore
from groupagent.tools import PolicyDecision, ToolContext, ToolData, ToolDefinition, ToolGateway


class DocumentInput(ToolData):
    title: str = Field(min_length=1, max_length=200)
    paragraphs: list[str] = Field(min_length=1, max_length=20)
    format: Literal["md", "docx", "pdf", "pptx"] = "md"


class DocumentOutput(ToolData):
    file_id: str
    version_id: str
    name: str
    sha256: str


async def run_demo(
    dsn: str,
    root: Path,
    *,
    document_format: Literal["md", "docx", "pdf", "pptx"] = "md",
    renderer: DocumentRenderer | None = None,
    language: Literal["en", "zh"] = "en",
    pdf_font_path: Path | None = None,
    office_font_family: str = "Arial",
) -> dict:
    root = root.resolve()
    root.mkdir(parents=True, exist_ok=True)
    store = PostgresStore(dsn)
    store.migrate()
    group_id = "synthetic-" + uuid4().hex
    member_id = "synthetic-member"
    source_key = group_id + ":request"
    store.register_group(group_id)
    config = AppConfig(
        deployment_mode="test",
        group=GroupConfig(
            enabled=True,
            internal_id=group_id,
            external_group_id="synthetic-only",
            owner_ids=[member_id],
        ),
    )
    provider = ScriptedDecisionProvider(
        {
            source_key: Decision(
                kind="start_work",
                reason_code="synthetic_document_fixture",
                goal="Produce a synthetic course report and verify its output",
                acceptance_criteria=[
                    "File opens",
                    "Required title and body are present",
                    "Validation passes",
                ],
            )
        }
    )
    coordinator = Coordinator(config, store, provider)
    stamp = datetime.now(UTC)
    event = GroupEvent(
        id=uuid4().hex,
        source_key=source_key,
        group_id=group_id,
        actor_id=member_id,
        kind=EventKind.TEXT,
        text="请根据这份合成资料生成课程报告。",
        occurred_at=stamp,
        received_at=stamp,
    )
    outcome = await coordinator.receive(
        event, stream_id="synthetic", cursor="1", verified_channel=True
    )
    duplicate = await coordinator.receive(
        event, stream_id="synthetic", cursor="1", verified_channel=True
    )
    assert outcome.work_id and outcome.work_id == duplicate.work_id and duplicate.duplicate
    actor = ActorContext(group_id=group_id, member_id=member_id, role=Role.OWNER)
    with store.transaction(actor) as tx:
        lease = tx.claim_work(outcome.work_id, "synthetic-worker", ttl_seconds=300)
        work = tx.get_work(outcome.work_id)
    workspace = root / "workspaces" / work.id
    workspace.mkdir(parents=True, exist_ok=True)
    artifacts = ArtifactStore(root / "artifacts")
    documents = DocumentService(
        artifacts,
        renderer=renderer,
        pdf_font_path=pdf_font_path,
        office_font_family=office_font_family,
    )
    generated = []
    scope = ArtifactScope(group_id=group_id, work_id=work.id, actor_id=member_id)

    async def policy(tool_scope):
        with store.transaction(actor) as tx:
            tx.assert_lease(lease)
        return PolicyDecision(actor=actor, allowed_tools=tool_scope.allowed_tools)

    async def create_document(args: DocumentInput, context: ToolContext) -> DocumentOutput:
        # The worker receives no channel credential or production source material.
        if context.group_id != scope.group_id or context.work_id != scope.work_id:
            raise ValueError("Mismatched artifact scope")
        content: Report | Deck
        if args.format == "pptx":
            content = Deck(args.title, (Slide(args.title, tuple(args.paragraphs)),))
        else:
            content = Report(args.title, (Section("Overview", tuple(args.paragraphs)),))
        artifact = await asyncio.to_thread(
            documents.generate, scope, "course-report." + args.format, content
        )
        generated.append(artifact)
        return DocumentOutput(
            file_id=artifact.file_id,
            version_id=artifact.version_id,
            name=artifact.name,
            sha256=artifact.sha256,
        )

    gateway = ToolGateway(policy)
    gateway.register(
        ToolDefinition(
            name="artifact_create_document",
            description="Create a scoped synthetic course document",
            input_model=DocumentInput,
            output_model=DocumentOutput,
            handler=create_document,
            risk_class="local_draft",
        )
    )
    title = "Synthetic course report" if language == "en" else "课程协作验证报告"
    text = (
        "This synthetic report demonstrates durable work, scoped tools and verified artifacts."
        if language == "en"
        else "这是一份合成测试报告，用于验证持久任务、权限边界、中文渲染与产物交付。"
    )
    model = ScriptedFakeModel(
        [
            ModelReply(
                tool_calls=(
                    ModelToolCall(
                        id="call-1",
                        name="artifact_create_document",
                        arguments={"title": title, "paragraphs": [text], "format": document_format},
                    ),
                ),
                finish_reason="tool_calls",
                usage=TokenUsage(input_tokens=100, output_tokens=30),
            ),
            ModelReply(
                text="Candidate document created; validate it before delivery.",
                usage=TokenUsage(input_tokens=150, output_tokens=20),
            ),
        ]
    )
    evidence_dir = root / "evidence" / work.id
    evidence_dir.mkdir(parents=True, exist_ok=True)

    async def checkpoint_sink(checkpoint):
        target = evidence_dir / "checkpoint.json"
        temp = target.with_suffix(".pending")
        temp.write_text(checkpoint.model_dump_json(), encoding="utf-8")
        temp.replace(target)

    engine = ScopedToolEngine(model, gateway, checkpoint_sink=checkpoint_sink)
    run_id = await engine.start(
        ExecutionRequest(
            work_id=work.id,
            attempt_id=lease.attempt_id,
            actor=actor,
            objective=work.goal,
            workspace=workspace.resolve(),
            allowed_tools=frozenset({"artifact_create_document"}),
            required_tools=frozenset({"artifact_create_document"}),
            trace_id=event.id,
            revision=work.goal_revision,
        )
    )
    snapshot = await engine.wait(run_id)
    if snapshot.state != RunState.CANDIDATE or len(generated) != 1:
        raise RuntimeError("Synthetic execution did not produce the required artifact candidate")
    with store.transaction(actor) as tx:
        current = tx.get_work(work.id)
        current = tx.transition_work(
            work.id, WorkState.VALIDATING, expected_revision=current.revision, lease=lease
        )
    artifact = generated[0]
    validation = await asyncio.to_thread(
        documents.validate,
        scope,
        artifact.file_id,
        artifact.version_id,
        expected_text=(title, text),
        render=document_format != "md",
    )
    validation_path = evidence_dir / "validation.json"
    validation_path.write_text(
        json.dumps(validation.to_dict(), ensure_ascii=False, indent=2), encoding="utf-8"
    )
    if not validation.passed:
        with store.transaction(actor) as tx:
            current = tx.get_work(work.id)
            tx.transition_work(
                work.id,
                WorkState.FAILED,
                expected_revision=current.revision,
                lease=lease,
                reason="validation_failed",
            )
        raise RuntimeError("Artifact validation failed; inspect the synthetic validation record")
    with store.transaction(actor) as tx:
        tx.record_validation(work.id, lease, str(validation_path), check_key="result", passed=True)
        current = tx.get_work(work.id)
        current = tx.transition_work(
            work.id, WorkState.DELIVERING, expected_revision=current.revision, lease=lease
        )
        intent = tx.enqueue_delivery(
            work.id,
            payload_ref="artifact:" + artifact.version_id,
            payload_hash=artifact.sha256,
            idempotency_key="result:" + work.id + ":1:" + artifact.version_id,
            lease=lease,
        )
    # Synthetic channel deliberately marked in its receipt; no real WeChat send.
    with store.transaction(actor) as tx:
        claimed = tx.claim_delivery("synthetic-channel", ttl_seconds=30)
        assert claimed and claimed.id == intent.id
        receipt = tx.finish_delivery(
            claimed.id,
            claimed.claim_token,
            DeliveryState.ACCEPTED,
            platform_message_id="synthetic-receipt-" + intent.id,
        )
        current = tx.get_work(work.id)
        current = tx.transition_work(
            work.id, WorkState.COMPLETED, expected_revision=current.revision, lease=lease
        )
    result = {
        "mode": "synthetic-replay",
        "live_model": False,
        "live_channel": False,
        "group_id": group_id,
        "work_id": work.id,
        "state": current.state.value,
        "artifact": asdict(artifact),
        "validation": validation.to_dict(),
        "delivery_state": receipt.state.value,
        "evidence_dir": str(evidence_dir),
        "request_replay_deduplicated": True,
    }
    (evidence_dir / "result.json").write_text(
        json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    return result
