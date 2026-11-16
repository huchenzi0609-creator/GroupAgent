"""Cross-work source citations use real scoped PostgreSQL and synthetic image bytes.

No HTTP request, model, paid provider, or isolated worker claim is involved.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
from dataclasses import replace

import pytest
from media.conftest import png_fixture
from psycopg.types.json import Jsonb

from groupagent.domain import DomainError
from groupagent.media import MediaService, MediaSource
from groupagent.memory import MemoryService
from groupagent.research.image_transforms import ImageTransformService, resolve_public_image
from groupagent.runtime.inputs import InputProcessor
from groupagent.tools import ToolContext, ToolScope

from .test_artifact_provenance_inputs import event, importer
from .test_inputs import InputFixtureWorker

pytestmark = [pytest.mark.postgres, pytest.mark.public_image_workflow]


def work_context(env, work, *, name):
    workspace = env.root / work.id
    workspace.mkdir(exist_ok=True)
    return ToolContext(
        ToolScope(
            env.actor.group_id, work.id, env.actor, workspace, frozenset(), work.request_event_id
        ),
        name,
        name + ":" + work.id,
        asyncio.Event(),
    )


async def researched_images(env, *, converted=False):
    origin = event(env)
    with env.store.transaction(env.actor) as tx:
        work = tx.create_work(
            "Find two synthetic public images",
            origin.id,
            scope={"source_fingerprints": {origin.id: tx.event_fingerprint(origin.id)}},
        )
        lease = tx.claim_work(work.id, "synthetic-public-image-producer", ttl_seconds=300)
    context = work_context(env, work, name="synthetic-public-image")
    if converted:
        from research.test_image_transforms import InlineTransformFixtureWorker

        media = MediaService(env.artifacts, InlineTransformFixtureWorker())
    else:
        media = MediaService(env.artifacts, InputFixtureWorker())
    versions, payloads = [], []
    for index in range(2):
        payload = png_fixture("SYNTHETIC PUBLIC IMAGE " + str(index))
        image_url = f"https://images.example.com/image-{index}.png"
        source = MediaSource(
            group_id=env.actor.group_id,
            work_id=work.id,
            source_event_id=origin.id,
            source_message_id=origin.id,
            member_id=env.actor.member_id,
            attachment_id="public-image:" + str(index),
            declared_kind="image",
            origin_kind="research",
            source_url=image_url,
            sequence=index,
        )
        reference = await media.ingest_image(
            context,
            source,
            payload,
            research_provenance={
                "requested_url": image_url,
                "page_url": f"https://source.example.com/page-{index}",
                "page_final_url": f"https://source.example.com/page-{index}",
                "page_sha256": hashlib.sha256(f"synthetic page {index}".encode()).hexdigest(),
                "page_retrieved_at": "2026-11-16T08:00:00+08:00",
                "retrieved_at": f"2026-11-16T08:00:0{index + 1}+08:00",
                "content_type": "image/png",
                "discovery": "img_src",
            },
        )
        version = env.artifacts.get(media.scope(context), reference.file_id, reference.version_id)
        env.registry.register(env.actor, lease, version, purpose="result")
        if converted:
            ref = await ImageTransformService(media).convert(
                context, version.file_id, version.version_id, "webp"
            )
            version = env.artifacts.get(media.scope(context), ref.file_id, ref.version_id)
            env.registry.register(env.actor, lease, version, purpose="result")
            payload = env.artifacts.read(media.scope(context), ref.file_id, ref.version_id)
        versions.append(version)
        payloads.append(payload)
    return origin, versions, payloads


def document_work(env, origin, versions):
    request = event(env)
    with env.store.transaction(env.actor) as tx:
        work = tx.create_work(
            "Put the two selected images in Word with their sources",
            request.id,
            input_versions=[version.version_id for version in versions],
            scope={
                "source_fingerprints": {
                    value.id: tx.event_fingerprint(value.id) for value in (request, origin)
                },
                "attachment_event_ids": [],
            },
        )
        lease = tx.claim_work(work.id, "synthetic-word-importer", ttl_seconds=300)
    return work, lease, work_context(env, work, name="synthetic-word-input")


def replace_cached_input(env, work, lease, original, value):
    with env.store.transaction(env.actor) as tx:
        row = tx.conn.execute(
            "SELECT body FROM ga_media_inputs WHERE group_id=%s AND event_id=%s AND attachment_id=%s",
            (env.actor.group_id, work.request_event_id, "artifact:" + original.version_id),
        ).fetchone()
        body = dict(row["body"])
        body["imports"][work.id + ":" + str(lease.goal_revision)] = value
        tx.conn.execute(
            "UPDATE ga_media_inputs SET body=%s WHERE group_id=%s AND event_id=%s AND attachment_id=%s",
            (
                Jsonb(body),
                env.actor.group_id,
                work.request_event_id,
                "artifact:" + original.version_id,
            ),
        )


def assert_sources(item, original, index):
    assert item["from_version"] == original.version_id
    assert item["sha256"] == original.sha256
    assert item["public_image"] == original.provenance["public_image"]
    assert item["page_citation"] == (
        f"https://source.example.com/page-{index}（页面访问时间 UTC：2026-11-16T00:00:00Z）"
    )
    assert item["image_citation"] == (
        f"https://images.example.com/image-{index}.png（图片访问时间 UTC：2026-11-16T00:00:0{index + 1}Z）"
    )


async def test_word_input_preserves_both_public_image_sources_bytes_and_cached_retry(runtime_env):
    env = runtime_env
    origin, originals, payloads = await researched_images(env)
    work, lease, context = document_work(env, origin, originals)
    processor, worker, documents, media = importer(env)

    imported = await processor.import_for_work(work, lease, context, documents, media)

    assert len(imported) == 2 and worker.calls == ["image_validate", "image_validate"]
    for index, (item, original, payload) in enumerate(
        zip(imported, originals, payloads, strict=True)
    ):
        assert_sources(item, original, index)
        copied, content = env.registry.read(env.actor, item["version_id"])
        assert copied.work_id == work.id and copied.version_id != original.version_id
        assert content == payload and hashlib.sha256(content).hexdigest() == original.sha256
        assert copied.provenance["origin_kind"] == "research"
        assert copied.provenance["public_image"] == original.provenance["public_image"]
    assert await processor.import_for_work(work, lease, context, documents, media) == imported
    assert worker.calls == ["image_validate", "image_validate"]

    MemoryService(env.store).forget(env.actor, event_ids=[origin.id])
    with pytest.raises(DomainError) as withdrawn:
        await processor.import_for_work(work, lease, context, documents, media)
    assert withdrawn.value.code in {"source_unavailable", "stale_lease"}
    assert worker.calls == ["image_validate", "image_validate"]


async def test_legacy_cached_image_gets_a_new_attested_copy_without_rewriting_history(runtime_env):
    env = runtime_env
    origin, originals, payloads = await researched_images(env)
    work, lease, context = document_work(env, origin, originals)
    processor, worker, documents, media = importer(env)
    imported = await processor.import_for_work(work, lease, context, documents, media)
    legacy_ids = []
    for item, original, payload in zip(imported, originals, payloads, strict=True):
        # Model a copy made before provenance preservation was implemented.
        source = media.load_ref(context, item["file_id"], item["version_id"]).source
        legacy = await media.ingest_image(context, source, payload)
        version = env.artifacts.get(media.scope(context), legacy.file_id, legacy.version_id)
        assert "public_image" not in version.provenance
        env.registry.register(
            env.actor, lease, version, source_event_ids=(origin.id,), purpose="input"
        )
        legacy_ids.append(legacy.version_id)
        replace_cached_input(
            env,
            work,
            lease,
            original,
            {
                **item,
                "file_id": legacy.file_id,
                "version_id": legacy.version_id,
                "from_version": "forged-original",
                "public_image": {"final_url": "https://forged.example.com/image.png"},
                "page_citation": "forged page citation",
                "image_citation": "forged image citation",
            },
        )
    (context.workspace / "inputs.json").write_text(
        json.dumps({"public_image": {"final_url": "https://workspace.example.com/forged.png"}})
    )
    before = list(worker.calls)

    recovered = await processor.import_for_work(work, lease, context, documents, media)

    assert not set(item["version_id"] for item in recovered) & set(legacy_ids)
    for index, (item, original) in enumerate(zip(recovered, originals, strict=True)):
        assert_sources(item, original, index)
        cached, cached_bytes = env.registry.read(env.actor, item["version_id"])
        assert "public_image" in cached.provenance
        assert hashlib.sha256(cached_bytes).hexdigest() == original.sha256
        legacy, _ = env.registry.read(env.actor, legacy_ids[index])
        assert "public_image" not in legacy.provenance
    assert worker.calls == before + ["image_validate", "image_validate"]
    assert await processor.import_for_work(work, lease, context, documents, media) == recovered
    assert worker.calls == before + ["image_validate", "image_validate"]


@pytest.mark.parametrize("forgery", ["different_bytes", "different_work"])
async def test_cached_input_cannot_attach_original_citations_to_another_artifact(
    runtime_env, forgery
):
    env = runtime_env
    origin, originals, _ = await researched_images(env)
    work, lease, context = document_work(env, origin, originals)
    processor, worker, documents, media = importer(env)
    imported = await processor.import_for_work(work, lease, context, documents, media)
    other = imported[1] if forgery == "different_bytes" else originals[0].to_dict()
    replace_cached_input(env, work, lease, originals[0], {**imported[0], **other})
    before = list(worker.calls)

    with pytest.raises(DomainError) as invalid:
        await processor.import_for_work(work, lease, context, documents, media)

    assert invalid.value.code == "validation_failed"
    assert worker.calls == before


@pytest.mark.parametrize("changes", [{"sha256": "0" * 64}, {"page_final_url": "x" * 8193}])
async def test_public_image_manifest_rejects_unbound_or_unbounded_provenance(runtime_env, changes):
    _, originals, _ = await researched_images(runtime_env)
    original = originals[0]
    altered = replace(
        original,
        provenance={
            **original.provenance,
            "public_image": {**original.provenance["public_image"], **changes},
        },
    )
    with pytest.raises(DomainError) as invalid:
        InputProcessor._public_image_input(altered, runtime_env.artifacts)
    assert invalid.value.code == "public_image_evidence_invalid"


async def test_converted_public_images_keep_root_sources_across_authorized_work_copy(runtime_env):
    env = runtime_env
    origin, originals, payloads = await researched_images(env, converted=True)
    assert all(version.format == "webp" for version in originals)
    work, lease, context = document_work(env, origin, originals)
    processor, worker, documents, media = importer(env)
    imported = await processor.import_for_work(work, lease, context, documents, media)
    for item, original, payload in zip(imported, originals, payloads, strict=True):
        copied, content = env.registry.read(env.actor, item["version_id"])
        assert content == payload and copied.sha256 == original.sha256
        lineage = resolve_public_image(env.artifacts, media.scope(context), copied)
        assert lineage is not None
        assert lineage.source_sha256 == original.provenance["public_image"]["sha256"]
        assert item["public_image"]["sha256"] == lineage.source_sha256
    before = list(worker.calls)
    assert await processor.import_for_work(work, lease, context, documents, media) == imported
    assert worker.calls == before
    MemoryService(env.store).forget(env.actor, event_ids=[origin.id])
    with pytest.raises(DomainError):
        await processor.import_for_work(work, lease, context, documents, media)
