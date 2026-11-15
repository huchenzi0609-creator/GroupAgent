"""Real PostgreSQL scope/import contracts with synthetic Office parser fixtures.

No model, Docker worker, external provider, or monetary policy is created here.
"""

import asyncio
import io
from datetime import UTC, datetime
from uuid import uuid4

import pytest
from pptx import Presentation
from pptx.util import Inches

from groupagent.artifacts import ArtifactScope
from groupagent.artifacts.isolated import IsolatedDocumentService
from groupagent.billing import BudgetLedger
from groupagent.config import AppConfig, GroupConfig
from groupagent.conversation.service import ConversationCoordinator
from groupagent.coordinator import Decision
from groupagent.domain import ActorContext, DomainError, EventKind, GroupEvent, Role
from groupagent.media import MediaService, MediaSource
from groupagent.memory import MemoryService
from groupagent.runtime.inputs import InputProcessor
from groupagent.tools import ToolContext, ToolScope

from .test_inputs import InputFixtureWorker

pytestmark = pytest.mark.postgres


def event(env, *, kind=EventKind.TEXT, attachments=(), actor=None, reply_to=None):
    actor = actor or env.actor
    now = datetime.now(UTC)
    value = GroupEvent(
        id=uuid4().hex,
        source_key="artifact-source-input:" + uuid4().hex,
        group_id=actor.group_id,
        actor_id=actor.member_id,
        kind=kind,
        text="Synthetic source request",
        attachments=list(attachments),
        occurred_at=now,
        received_at=now,
        reply_to=reply_to,
    )
    with env.store.transaction(actor) as tx:
        return tx.ingest_event(value, "fixture:" + value.id, "1", 0).event


def registered_pptx(env, origin, *, related=(), actor=None):
    actor = actor or env.actor
    with env.store.transaction(actor) as tx:
        work = tx.create_work(
            "Create a synthetic presentation",
            origin.id,
            scope={
                "source_fingerprints": {
                    item.id: tx.event_fingerprint(item.id) for item in [origin, *related]
                }
            },
        )
        lease = tx.claim_work(work.id, "fixture-producer", ttl_seconds=300)
    presentation = Presentation()
    slide = presentation.slides.add_slide(presentation.slide_layouts[6])
    slide.shapes.add_textbox(
        Inches(1), Inches(1), Inches(6), Inches(1)
    ).text = "Synthetic immutable source"
    payload = io.BytesIO()
    presentation.save(payload)
    provenance = MediaSource(
        group_id=actor.group_id,
        work_id=work.id,
        source_event_id=origin.id,
        source_message_id=origin.id,
        member_id=actor.member_id,
        attachment_id="generated:" + work.id,
        declared_kind="file",
        origin_kind="generated",
    ).model_dump(mode="json")
    version = env.artifacts.create(
        ArtifactScope(actor.group_id, work.id, actor.member_id),
        "source.pptx",
        payload.getvalue(),
        provenance=provenance,
    )
    env.registry.register(actor, lease, version, purpose="result")
    return version, payload.getvalue()


def admitted_work(env, selected, versions):
    request = event(env)
    # Only _inputs is exercised: the public model/router is not instantiated.
    coordinator = object.__new__(ConversationCoordinator)
    coordinator.config = AppConfig(
        group=GroupConfig(
            internal_id=env.actor.group_id,
            external_group_id=env.config.external_group_id,
            owner_ids=[env.actor.member_id],
        )
    )
    coordinator.artifacts = env.registry
    decision = Decision(
        kind="start_work",
        reason_code="synthetic-source-selection",
        goal="Edit the selected immutable presentation",
        input_event_ids=[item.id for item in selected],
        input_versions=[item.version_id for item in versions],
    )
    with env.store.transaction(env.actor) as tx:
        snapshot = {
            "source_fingerprints": {
                item.id: tx.event_fingerprint(item.id) for item in [request, *selected]
            }
        }
        context = {
            "_snapshot": snapshot,
            "files": [{"version_id": item.version_id} for item in versions],
        }
        channel_sources = coordinator._inputs(tx, request, decision, context)
        work = tx.create_work(
            decision.goal,
            request.id,
            input_versions=decision.input_versions,
            scope={
                "source_fingerprints": snapshot["source_fingerprints"],
                "attachment_event_ids": channel_sources,
                "retained_fact_source_ids": snapshot.get("artifact_retained_source_ids", []),
            },
        )
        lease = tx.claim_work(work.id, "fixture-importer", ttl_seconds=300)
        work = tx.get_work(work.id)
    workspace = env.root / work.id
    workspace.mkdir()
    tool_context = ToolContext(
        ToolScope(env.actor.group_id, work.id, env.actor, workspace, frozenset(), request.id),
        "fixture-import",
        "fixture-import:" + work.id,
        asyncio.Event(),
    )
    return work, lease, tool_context


def importer(env):
    worker = InputFixtureWorker()
    processor = InputProcessor(
        env.store,
        env.artifacts,
        worker,
        {},
        {},
        BudgetLedger(env.store),
        env.channel,
        env.root / "inputs",
        lambda _: env.actor,
    )
    return (
        processor,
        worker,
        IsolatedDocumentService(env.artifacts, worker),
        MediaService(env.artifacts, worker),
    )


@pytest.mark.parametrize("kind", [EventKind.TEXT, EventKind.QUOTE])
async def test_primary_empty_request_is_provenance_and_version_imports_immutable_copy(
    runtime_env, kind
):
    env = runtime_env
    origin = event(env, kind=kind)
    original, payload = registered_pptx(env, origin)
    work, lease, context = admitted_work(env, [origin], [original])
    assert work.scope["attachment_event_ids"] == []
    assert origin.id in work.scope["source_fingerprints"]
    assert work.input_versions == [original.version_id]
    processor, _, documents, media = importer(env)
    imported = await processor.import_for_work(work, lease, context, documents, media)
    assert len(imported) == 1 and imported[0]["from_version"] == original.version_id
    assert imported[0]["version_id"] != original.version_id
    copy, copied_bytes = env.registry.read(env.actor, imported[0]["version_id"])
    assert copied_bytes == payload and copy.work_id == work.id
    assert copy.provenance["source_event_id"] == origin.id
    assert copy.provenance["input_version_id"] == original.version_id
    assert env.registry.read(env.actor, original.version_id)[1] == payload
    assert await processor.import_for_work(work, lease, context, documents, media) == imported
    with env.store.transaction(env.actor) as tx:
        assert tx.get_work(work.id).scope["source_fingerprints"][origin.id] == (
            tx.event_fingerprint(origin.id)
        )
        assert (
            tx.conn.execute(
                "SELECT count(*) AS n FROM ga_budget_policy WHERE group_id=%s",
                (env.actor.group_id,),
            ).fetchone()["n"]
            == 0
        )
    MemoryService(env.store).forget(env.actor, event_ids=[origin.id])
    with pytest.raises(DomainError) as withdrawn:
        env.registry.read(env.actor, copy.version_id)
    assert withdrawn.value.code == "source_unavailable"


@pytest.mark.parametrize("case", ["no_version", "different_primary"])
async def test_empty_text_without_matching_primary_artifact_still_fails_closed(runtime_env, case):
    env = runtime_env
    selected = event(env)
    versions = []
    if case == "different_primary":
        primary = event(env)
        original, _ = registered_pptx(env, primary, related=[selected])
        versions.append(original)
    work, lease, context = admitted_work(env, [selected], versions)
    # The selected text can even be a registered transitive dependency; only
    # the host version's exact primary provenance can suppress media import.
    assert work.scope["attachment_event_ids"] == [selected.id]
    processor, worker, documents, media = importer(env)
    with pytest.raises(DomainError) as failure:
        await processor.import_for_work(work, lease, context, documents, media)
    assert failure.value.code == "invalid_attachment" and not worker.calls


@pytest.mark.parametrize(
    "kind,attachments",
    [
        (EventKind.FILE, []),
        (EventKind.IMAGE, []),
        (EventKind.AUDIO, []),
        (EventKind.TEXT, [{"kind": "file"}]),
        (EventKind.QUOTE, [{"kind": "file", "handle": {}}]),
    ],
)
async def test_real_media_types_or_malformed_handles_are_never_pruned(
    runtime_env, kind, attachments
):
    env = runtime_env
    origin = event(env, kind=kind, attachments=attachments)
    original, _ = registered_pptx(env, origin)
    work, lease, context = admitted_work(env, [origin], [original])
    assert work.scope["attachment_event_ids"] == [origin.id]
    processor, worker, documents, media = importer(env)
    with pytest.raises(DomainError) as failure:
        await processor.import_for_work(work, lease, context, documents, media)
    assert failure.value.code == "invalid_attachment" and not worker.calls


@pytest.mark.parametrize("withdrawal", ["forget", "recall"])
def test_withdrawn_primary_source_cannot_be_normalized_or_reauthorized(runtime_env, withdrawal):
    env = runtime_env
    origin = event(env)
    original, _ = registered_pptx(env, origin)
    memory = MemoryService(env.store)
    if withdrawal == "forget":
        memory.forget(env.actor, event_ids=[origin.id])
    else:
        recall = event(env, kind=EventKind.RECALL, reply_to=origin.id)
        memory.recall(env.service, recall.id)
    with pytest.raises(DomainError) as failure:
        admitted_work(env, [origin], [original])
    assert failure.value.code == "source_unavailable"


def test_private_foreign_draft_cannot_supply_primary_source_authority(runtime_env):
    env = runtime_env
    other = ActorContext(group_id=env.actor.group_id, member_id="other-author", role=Role.MEMBER)
    origin = event(env, actor=other)
    private, _ = registered_pptx(env, origin, actor=other)
    with pytest.raises(DomainError) as failure:
        admitted_work(env, [origin], [private])
    assert failure.value.code == "permission_denied"
