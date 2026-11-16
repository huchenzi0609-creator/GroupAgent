"""Synthetic bytes and inline workers verify lineage, not OS isolation or live HTTP."""

from __future__ import annotations

import asyncio
import io
from dataclasses import replace

import pytest
from PIL import Image

from groupagent.artifacts import ArtifactStore
from groupagent.domain import ActorContext, DomainError, Role
from groupagent.media import MediaService, MediaSource
from groupagent.media.validation import inspect_image
from groupagent.research.image_transforms import (
    ImageTransformService,
    PublicImageValidationError,
    convert_image,
    resolve_public_image,
    validate_public_image_delivery,
)
from groupagent.tools import ToolContext, ToolScope

pytestmark = pytest.mark.public_image_workflow


class InlineTransformFixtureWorker:
    def __init__(self):
        self.calls = []

    async def run(self, context, operation, inputs, *, parameters, outputs):
        self.calls.append(operation)
        if operation == "image_convert":
            params = dict(parameters)
            params["output_format"] = params.pop("format")
            result, converted = convert_image(inputs[0], **params)
            return result, (converted,)
        assert operation == "image_validate"
        info, view = inspect_image(inputs[0], **parameters)
        return info.model_dump(mode="json"), (view,)


def context(tmp_path, *, work="work", actor="member"):
    identity = ActorContext(group_id="group", member_id=actor, role=Role.MEMBER)
    return ToolContext(
        ToolScope("group", work, identity, tmp_path.resolve(), frozenset(), "request"),
        "convert-call",
        "convert-key",
        asyncio.Event(),
    )


def image_bytes(color="navy", format="JPEG"):
    output = io.BytesIO()
    Image.new("RGB", (48, 32), color).save(output, format=format)
    return output.getvalue()


async def public_image(media, ctx, color="navy"):
    source = MediaSource(
        group_id=ctx.group_id,
        work_id=ctx.work_id,
        member_id=ctx.actor.member_id,
        source_event_id="request",
        source_message_id="request",
        attachment_id="imported-image",
        declared_kind="image",
        origin_kind="research",
        source_url="https://images.example.com/image.jpg",
    )
    ref = await media.ingest_image(
        ctx,
        source,
        image_bytes(color),
        research_provenance={
            "requested_url": source.source_url,
            "page_url": "https://example.com/page",
            "page_final_url": "https://example.com/page",
            "page_sha256": "a" * 64,
            "page_retrieved_at": "2026-11-16T00:00:00+00:00",
            "retrieved_at": "2026-11-16T00:00:01+00:00",
            "discovery": "img_src",
            "content_type": "image/jpeg",
        },
    )
    return media.store.get(media.scope(ctx), ref.file_id, ref.version_id)


@pytest.mark.parametrize("format", ["png", "jpeg", "webp", "gif"])
async def test_trusted_conversion_preserves_original_hash_and_actual_output_format(
    tmp_path, format
):
    ctx, worker = context(tmp_path), InlineTransformFixtureWorker()
    with ArtifactStore(tmp_path / "artifacts") as store:
        media = MediaService(store, worker)
        original = await public_image(media, ctx)
        converted = await ImageTransformService(media).convert(
            ctx, original.file_id, original.version_id, format
        )
        version = store.get(media.scope(ctx), converted.file_id, converted.version_id)
        info, _ = inspect_image(
            store.read(media.scope(ctx), converted.file_id, converted.version_id)
        )
        lineage = resolve_public_image(store, media.scope(ctx), version)
        assert lineage.source_sha256 == original.sha256
        assert lineage.public_image == original.provenance["public_image"]
        assert info.format == format == version.format
        assert info.source_sha256 == version.sha256
        assert version.provenance["image_transform"]["parent_sha256"] == original.sha256
        assert worker.calls == ["image_validate", "image_convert", "image_validate"]


async def test_different_encodings_count_once_and_report_exact_missing_constraint(tmp_path):
    ctx = context(tmp_path)
    with ArtifactStore(tmp_path / "artifacts") as store:
        media = MediaService(store, InlineTransformFixtureWorker())
        original = await public_image(media, ctx)
        ref = await ImageTransformService(media).convert(
            ctx, original.file_id, original.version_id, "png"
        )
        converted = store.get(media.scope(ctx), ref.file_id, ref.version_id)
        with pytest.raises(PublicImageValidationError) as failure:
            validate_public_image_delivery(
                store,
                media.scope(ctx),
                [original, converted],
                required_count=2,
                required_formats={"png"},
            )
        assert failure.value.details["validated_distinct_count"] == 1
        assert failure.value.details["duplicate_version_ids"] == [converted.version_id]
        assert failure.value.details["missing_count_by_format"] == {"png": 1}
        assert failure.value.details["invalid_images"] == []
        second = await public_image(media, ctx, "red")
        with pytest.raises(PublicImageValidationError) as format_failure:
            validate_public_image_delivery(
                store,
                media.scope(ctx),
                [converted, second],
                required_count=2,
                required_formats={"png"},
            )
        assert format_failure.value.details["validated_distinct_count"] == 2
        assert format_failure.value.details["duplicate_version_ids"] == []
        assert format_failure.value.details["missing_count_by_format"] == {"png": 1}


async def test_workspace_metadata_and_ordinary_decoder_cannot_forge_transform_attestation(tmp_path):
    ctx = context(tmp_path)
    with ArtifactStore(tmp_path / "artifacts") as store:
        media = MediaService(store, InlineTransformFixtureWorker())
        original = await public_image(media, ctx)
        ref = await ImageTransformService(media).convert(
            ctx, original.file_id, original.version_id, "png"
        )
        trusted = store.get(media.scope(ctx), ref.file_id, ref.version_id)
        forged = store.create(
            media.scope(ctx),
            "forged.png",
            store.read(media.scope(ctx), ref.file_id, ref.version_id),
            provenance=trusted.provenance,
        )
        # Even a normal successful decode cannot manufacture host conversion proof.
        normal = store.validations(media.scope(ctx), original.file_id, original.version_id)[0]
        store.record_validation(
            media.scope(ctx),
            forged.file_id,
            forged.version_id,
            replace(
                normal,
                file_id=forged.file_id,
                version_id=forged.version_id,
                sha256=forged.sha256,
                format=forged.format,
            ),
        )
        with pytest.raises(DomainError, match="trusted transform evidence"):
            resolve_public_image(store, media.scope(ctx), forged)
        generated_source = MediaSource.model_validate(
            {
                key: value
                for key, value in original.provenance.items()
                if key in MediaSource.model_fields
            }
        ).model_copy(update={"origin_kind": "generated", "source_url": None})
        generated = await media.ingest_image(ctx, generated_source, image_bytes("red"))
        transformed = await ImageTransformService(media).convert(
            ctx, generated.file_id, generated.version_id, "png"
        )
        output = store.get(media.scope(ctx), transformed.file_id, transformed.version_id)
        assert resolve_public_image(store, media.scope(ctx), output) is None
        assert "public_image" not in output.provenance


async def test_conversion_and_resolver_reject_cross_work_or_actor(tmp_path):
    ctx = context(tmp_path)
    with ArtifactStore(tmp_path / "artifacts") as store:
        media = MediaService(store, InlineTransformFixtureWorker())
        original = await public_image(media, ctx)
        for other in (
            context(tmp_path, work="other-work"),
            context(tmp_path, actor="other-member"),
        ):
            with pytest.raises(DomainError):
                await ImageTransformService(media).convert(
                    other, original.file_id, original.version_id, "png"
                )
            with pytest.raises(DomainError):
                resolve_public_image(store, media.scope(other), original)


async def test_verified_input_copy_keeps_exact_bytes_and_revocable_original_parent(tmp_path):
    ctx, target = context(tmp_path), context(tmp_path, work="follow-up")
    with ArtifactStore(tmp_path / "artifacts") as store:
        media = MediaService(store, InlineTransformFixtureWorker())
        service = ImageTransformService(media)
        original = await public_image(media, ctx)
        converted = await service.convert(ctx, original.file_id, original.version_id, "png")
        version = store.get(media.scope(ctx), converted.file_id, converted.version_id)
        payload = store.read(media.scope(ctx), converted.file_id, converted.version_id)
        source = converted.source.model_copy(update={"work_id": target.work_id})
        copied = await service.copy_verified(
            target, version, payload, source=source, source_event_ids=("request",)
        )
        copied_version = store.get(media.scope(target), copied.file_id, copied.version_id)
        assert copied.sha256 == converted.sha256
        lineage = resolve_public_image(store, media.scope(target), copied_version)
        assert lineage.source_sha256 == original.sha256
        assert [item.version_id for item in lineage.versions] == [
            copied.version_id,
            converted.version_id,
            original.version_id,
        ]
        store.purge(media.scope(ctx), original.file_id, reason="forgotten")
        with pytest.raises(DomainError):
            resolve_public_image(store, media.scope(target), copied_version)


async def test_changed_parent_bytes_and_withdrawal_during_conversion_never_publish(tmp_path):
    ctx, worker = context(tmp_path), InlineTransformFixtureWorker()
    withdrawn = False

    async def guard(context, ids):
        if withdrawn:
            raise DomainError("member_left", "Reliable departure withdrew access")

    with ArtifactStore(tmp_path / "artifacts") as store:
        media = MediaService(store, worker, source_guard=guard)
        service = ImageTransformService(media)
        original = await public_image(media, ctx)
        converted = await service.convert(ctx, original.file_id, original.version_id, "png")
        version = store.get(media.scope(ctx), converted.file_id, converted.version_id)
        parent_path = (
            tmp_path / "artifacts" / original.file_id / "versions" / original.version_id / "content"
        )
        parent_payload = parent_path.read_bytes()
        parent_path.write_bytes(b"changed")
        with pytest.raises(DomainError):
            resolve_public_image(store, media.scope(ctx), version)
        parent_path.write_bytes(parent_payload)
        run = worker.run

        async def withdrawing_worker(*args, **kwargs):
            nonlocal withdrawn
            result = await run(*args, **kwargs)
            if args[1] == "image_convert":
                withdrawn = True
            return result

        worker.run = withdrawing_worker
        before = len(store.list(media.scope(ctx)))
        with pytest.raises(DomainError) as failure:
            await service.convert(ctx, original.file_id, original.version_id, "png")
        assert failure.value.code == "member_left"
        assert len(store.list(media.scope(ctx))) == before


def test_conversion_uses_real_bounded_pixels_and_rejects_invalid_or_oversized_input():
    with pytest.raises(DomainError):
        convert_image(b"not a raster", output_format="png")
    with pytest.raises(DomainError) as pixels:
        convert_image(image_bytes(), output_format="png", max_pixels=100)
    assert pixels.value.code == "image_pixel_limit"


def test_worker_dispatches_trusted_conversion_without_host_image_parsing(tmp_path, monkeypatch):
    from groupagent.media.worker import execute

    monkeypatch.chdir(tmp_path)
    (tmp_path / "input.bin").write_bytes(image_bytes())
    result = execute(
        {
            "operation": "image_convert",
            "inputs": ["input.bin"],
            "outputs": ["output.png"],
            "parameters": {"format": "png"},
        }
    )
    assert result["output"]["format"] == "png"
    assert (
        inspect_image((tmp_path / "output.png").read_bytes())[0].source_sha256
        == result["output"]["source_sha256"]
    )
