"""Real chart/PDF bytes and optional 512 MiB Docker acceptance; no provider calls."""

from __future__ import annotations

import asyncio
import hashlib
import io
import os
from dataclasses import replace

import pytest
from docx import Document
from PIL import Image
from pypdf import PdfReader

from groupagent.artifacts import (
    ArtifactError,
    ArtifactScope,
    ArtifactStore,
    IsolatedDocumentService,
)
from groupagent.artifacts.models import ValidationRecord
from groupagent.artifacts.tools import DocumentCreateInput, register_document_tools
from groupagent.artifacts.visuals import chart_png
from groupagent.artifacts.worker_operations import _chart, document_operation
from groupagent.domain import ActorContext, Role
from groupagent.media import IsolatedMediaWorker
from groupagent.media.validation import inspect_image
from groupagent.sandbox import DockerSandbox, SandboxLimits
from groupagent.tools import EffectState, ToolContext, ToolGateway, ToolScope


class InProcessByteBuilder:
    """Actual format bytes, with no claim of rendering or process isolation."""

    def __init__(self, directory):
        self.directory = directory
        self.calls = []

    async def run(self, context, operation, inputs, *, parameters, outputs=()):
        self.calls.append(operation)
        if operation == "chart_render":
            pixels = chart_png(_chart(parameters["chart"]))
            metadata, _ = inspect_image(pixels)
            return metadata.model_dump(mode="json"), (pixels,)
        metadata = document_operation(
            operation,
            {"parameters": parameters, "outputs": outputs},
            lambda request, index=0: inputs[index],
            lambda name: self.directory / name,
        )
        return metadata, tuple((self.directory / name).read_bytes() for name in outputs)


def runtime(tmp_path, worker):
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    store = ArtifactStore(tmp_path / "artifacts")
    service = IsolatedDocumentService(store, worker)
    gateway = ToolGateway()

    async def provenance(context):
        return {"work_id": context.work_id, "source_event_ids": [context.call_id]}

    register_document_tools(gateway, service, provenance_factory=provenance)
    scope = ToolScope(
        "managed-image-group",
        "managed-image-work",
        ActorContext(group_id="managed-image-group", member_id="author", role=Role.OWNER),
        workspace,
        gateway.registered_names,
        "managed-image-trace",
    )
    gateway.bind_scope(scope)
    return store, service, gateway, ToolContext(scope, "create", "create", asyncio.Event())


def report(images):
    return {
        "name": "平方和核验.pdf",
        "kind": "report",
        "title": "平方和核验：338350",
        "sections": [
            {
                "title": "结果与推导",
                "paragraphs": [
                    "逐项计算从1到100的平方并相加，结果为338350。",
                    "平方和公式为 n(n+1)(2n+1)/6；代入100，得到100*101*201/6=338350。",
                ],
            },
            {
                "title": "图表核验",
                "paragraphs": [
                    "图1展示累计平方和的增长；图2按每十个数分组，各组相加同样得到338350。"
                ],
                "images": [
                    {
                        "file_id": item["file_id"],
                        "version_id": item["version_id"],
                        "alt_text": caption,
                        "width_inches": 4.5,
                    }
                    for item, caption in zip(
                        images, ("图1 累计平方和", "图2 每十个数的平方和"), strict=True
                    )
                ],
            },
        ],
    }


async def charts(gateway, context):
    values = tuple(sum(i * i for i in range(1, n + 1)) for n in range(10, 101, 10))
    grouped = tuple(sum(i * i for i in range(n - 9, n + 1)) for n in range(10, 101, 10))
    assert values[-1] == sum(grouped) == 338350
    images = []
    for index, (kind, data, title) in enumerate(
        (("line", values, "累计平方和"), ("bar", grouped, "分组平方和"))
    ):
        result = await gateway.invoke(
            "chart_render",
            {
                "name": f"figure-{index + 1}.png",
                "chart": {
                    "kind": kind,
                    "title": title,
                    "categories": [str(n) for n in range(10, 101, 10)],
                    "series": [{"name": "平方和", "values": data}],
                },
            },
            replace(context, call_id=f"chart-{index}", idempotency_key=f"chart-{index}"),
        )
        assert result.success, result
        images.append(result.output)
    return images


def pixel_digest(image):
    return hashlib.sha256(image.convert("RGB").tobytes()).hexdigest()


def assert_pdf(store, context, generated, images):
    scope = ArtifactScope(context.group_id, context.work_id, context.actor.member_id)
    version = store.get(scope, generated["file_id"], generated["version_id"])
    raw = store.read(scope, version.file_id, version.version_id)
    parsed = PdfReader(io.BytesIO(raw))
    assert 1 <= len(parsed.pages) <= 2
    assert "338350" in "".join(page.extract_text() for page in parsed.pages)
    assert "平方和核验" in parsed.metadata.title
    actual_images = [picture.image for page in parsed.pages for picture in page.images]
    assert len(actual_images) == 2
    expected_hashes = []
    for reference in images:
        source = store.read(scope, reference["file_id"], reference["version_id"])
        assert hashlib.sha256(source).hexdigest() == reference["sha256"]
        with Image.open(io.BytesIO(source)) as pixels:
            expected_hashes.append(pixel_digest(pixels))
    assert sorted(pixel_digest(image) for image in actual_images) == sorted(expected_hashes)
    assert version.provenance["source_image_refs"] == [
        {
            "location": f"sections[1].images[{index}]",
            "file_id": reference["file_id"],
            "version_id": reference["version_id"],
            "sha256": reference["sha256"],
        }
        for index, reference in enumerate(images)
    ]
    assert version.provenance["input_versions"] == [image["version_id"] for image in images]
    assert version.provenance["source_event_ids"] == ["chart-0", "chart-1", "create"]
    return version


async def test_top_level_images_error_explains_how_to_keep_existing_references(tmp_path):
    worker = InProcessByteBuilder(tmp_path)
    store, _, gateway, context = runtime(tmp_path, worker)
    try:
        invalid = {
            "name": "figures.pdf",
            "kind": "report",
            "title": "Synthetic report",
            "sections": [{"title": "Figures", "paragraphs": ["Result 338350"]}],
            "images": [{"file_id": "a" * 32, "version_id": "b" * 32}],
        }
        result = await gateway.invoke("document_generate", invalid, context)
        assert not result.success and result.effect_state == EffectState.NOT_STARTED
        issue = next(item for item in result.input_issues if item.field == ("images",))
        assert "sections[].images" in issue.hint and "instead of dropping" in issue.hint
        assert "file_id/version_id" in issue.hint and not worker.calls
        schema = DocumentCreateInput.model_json_schema()
        assert (
            "Embed real scoped image bytes"
            in schema["$defs"]["SectionInput"]["properties"]["images"]["description"]
        )
    finally:
        store.close()


async def test_managed_pdf_embeds_exact_image_versions_and_preserves_source_revocation(tmp_path):
    store, _, gateway, context = runtime(tmp_path, InProcessByteBuilder(tmp_path))
    try:
        images = await charts(gateway, context)
        scope = ArtifactScope(context.group_id, context.work_id, context.actor.member_id)
        # A changed current pointer cannot silently replace an explicitly selected version.
        store.revise(
            scope,
            images[0]["file_id"],
            store.read(scope, images[1]["file_id"], images[1]["version_id"]),
            expected_version_id=images[0]["version_id"],
        )
        result = await gateway.invoke("document_generate", report(images), context)
        assert result.success, result
        version = assert_pdf(store, context, result.output, images)
        assert "chart-0" in version.provenance["source_event_ids"]
        store.purge_event(context.group_id, "chart-0", context.actor.member_id)
        with pytest.raises(ArtifactError, match="unavailable|deleted|purged"):
            store.read(scope, version.file_id, version.version_id)
    finally:
        store.close()


async def test_failed_validation_is_a_repairable_failure_with_full_record_and_unchanged_bytes(
    tmp_path,
):
    store, service, gateway, context = runtime(tmp_path, InProcessByteBuilder(tmp_path))
    try:
        images = await charts(gateway, context)
        result = await gateway.invoke("document_generate", report(images), context)
        assert result.success, result
        scope = ArtifactScope(context.group_id, context.work_id, context.actor.member_id)
        reference = result.output
        before = store.read(scope, reference["file_id"], reference["version_id"])

        class SyntheticFailedValidation:
            async def run(self, context, operation, inputs, **kwargs):
                assert operation == "document_validate" and inputs == (before,)
                return ValidationRecord(
                    "worker",
                    "worker",
                    hashlib.sha256(inputs[0]).hexdigest(),
                    "pdf",
                    True,
                    False,
                    checks=("structure_open", "expected_text_assertions"),
                    issues=("missing_content:0",),
                    failed_expectations=(
                        {
                            "index": 0,
                            "expected": "Required missing text",
                            "stage": "extracted_text",
                        },
                    ),
                ).to_dict(), ()

        service.worker = SyntheticFailedValidation()
        failed = await gateway.invoke(
            "document_validate",
            {
                **{key: reference[key] for key in ("file_id", "version_id")},
                "expected_text": ["Required missing text"],
            },
            replace(context, call_id="negative-validation", idempotency_key="negative-validation"),
        )
        assert not failed.success and failed.effect_state == EffectState.NO_EFFECT
        assert failed.error_code == "document_validation_failed"
        assert failed.failure.disposition.value == "repair"
        assert "Required missing text" in failed.message
        data = failed.output["data"]
        assert data["file_id"] == reference["file_id"]
        assert data["version_id"] == reference["version_id"]
        assert not data["passed"] and data["issues"] == ["missing_content:0"]
        assert data["failed_expectations"][0]["expected"] == "Required missing text"
        assert store.read(scope, reference["file_id"], reference["version_id"]) == before
        assert (
            store.validations(scope, reference["file_id"], reference["version_id"])[0].validation_id
            == data["validation_id"]
        )
    finally:
        store.close()


@pytest.mark.sandbox
async def test_real_512mib_managed_pdf_with_two_chart_images_renders_and_validates(tmp_path):
    image = os.environ.get("GROUPAGENT_WORKER_IMAGE")
    if not image:
        pytest.skip("Requires an immutable Docker worker; no provider calls")
    sandbox = DockerSandbox(
        image,
        docker_host=os.environ.get("GROUPAGENT_DOCKER_HOST"),
        limits=SandboxLimits(memory_mb=512, workspace_mb=128, temp_mb=128, pids=128),
    )
    store, _, gateway, context = runtime(tmp_path, IsolatedMediaWorker(sandbox))
    try:
        images = await charts(gateway, context)
        result = await gateway.invoke("document_generate", report(images), context)
        assert result.success, result
        version = assert_pdf(store, context, result.output, images)
        validation = await gateway.invoke(
            "document_validate",
            {
                "file_id": version.file_id,
                "version_id": version.version_id,
                "expected_text": ["平方和核验", "338350", "图1 累计平方和", "图2 每十个数的平方和"],
            },
            replace(context, call_id="validate", idempotency_key="validate"),
        )
        assert validation.success, validation
        data = validation.output["data"]
        assert data["passed"] and data["rendered"], data
        assert data["page_count"] <= 2 and len(data["preview_sha256"]) == data["page_count"]
        scope = ArtifactScope(context.group_id, context.work_id, context.actor.member_id)
        for index, digest in enumerate(data["preview_sha256"]):
            preview = store.read_preview(
                scope, version.file_id, version.version_id, data["validation_id"], page_index=index
            )
            assert hashlib.sha256(preview).hexdigest() == digest
            with Image.open(io.BytesIO(preview)) as rendered:
                rendered.load()
                assert rendered.width > 500 and rendered.height > 500
    finally:
        store.close()


@pytest.mark.parametrize(
    "with_urls,isolated",
    [(False, False), (True, False), pytest.param(True, True, marks=pytest.mark.sandbox)],
)
async def test_docx_caption_follows_each_image_and_preserves_requested_source_url(
    tmp_path, with_urls, isolated
):
    if isolated:
        image = os.environ.get("GROUPAGENT_WORKER_IMAGE")
        if not image:
            pytest.skip("Requires immutable Docker worker; no provider calls")
        worker = IsolatedMediaWorker(
            DockerSandbox(
                image,
                docker_host=os.environ.get("GROUPAGENT_DOCKER_HOST"),
                limits=SandboxLimits(memory_mb=512, workspace_mb=128, temp_mb=128, pids=128),
            )
        )
    else:
        worker = InProcessByteBuilder(tmp_path)
    store, _, gateway, context = runtime(tmp_path, worker)
    try:
        images = await charts(gateway, context)
        captions = ["图1 累计平方和", "图2 分组平方和"]
        if with_urls:
            captions = [
                f"{caption}；来源：https://example.org/figure-{index + 1}"
                for index, caption in enumerate(captions)
            ]
        result = await gateway.invoke(
            "document_generate",
            {
                "name": "caption-order.docx",
                "kind": "report",
                "title": "图文来源顺序",
                "sections": [
                    {
                        "title": "两幅图表",
                        "paragraphs": ["说明文字在图表之前。"],
                        "images": [
                            {
                                "file_id": item["file_id"],
                                "version_id": item["version_id"],
                                "alt_text": caption,
                                "width_inches": 4.5,
                            }
                            for item, caption in zip(images, captions, strict=True)
                        ],
                    }
                ],
            },
            context,
        )
        assert result.success, result
        scope = ArtifactScope(context.group_id, context.work_id, context.actor.member_id)
        version = store.get(scope, result.output["file_id"], result.output["version_id"])
        raw = store.read(scope, version.file_id, version.version_id)
        document = Document(io.BytesIO(raw))
        paragraphs = document.paragraphs
        image_positions = [
            index
            for index, paragraph in enumerate(paragraphs)
            if paragraph._p.xpath(".//w:drawing")
        ]
        assert len(image_positions) == len(document.inline_shapes) == 2
        assert paragraphs[image_positions[0] - 1].text == "说明文字在图表之前。"
        for index, (position, caption, reference) in enumerate(
            zip(image_positions, captions, images, strict=True)
        ):
            # Check the native OOXML body order, not flattened extracted text.
            assert paragraphs[position + 1].text == caption
            assert paragraphs[position + 1].style.name == "Caption"
            picture = document.inline_shapes[index]
            assert picture._inline.docPr.get("descr") == caption
            relationship = picture._inline.xpath(".//a:blip")[0].embed
            with Image.open(io.BytesIO(document.part.related_parts[relationship].blob)) as actual:
                with Image.open(
                    io.BytesIO(store.read(scope, reference["file_id"], reference["version_id"]))
                ) as source:
                    assert pixel_digest(actual) == pixel_digest(source)
            if with_urls:
                url = f"https://example.org/figure-{index + 1}"
                assert url in paragraphs[position + 1].text
                assert not any(url in paragraph.text for paragraph in paragraphs[: position + 1])
        assert version.provenance["input_versions"] == [item["version_id"] for item in images]
        if isolated:
            validation = await gateway.invoke(
                "document_validate",
                {
                    "file_id": version.file_id,
                    "version_id": version.version_id,
                    "expected_text": ["图文来源顺序", *captions],
                },
                replace(context, call_id="validate-captions", idempotency_key="validate-captions"),
            )
            assert validation.success, validation
            assert validation.output["data"]["passed"] and validation.output["data"]["rendered"]
        assert store.read(scope, version.file_id, version.version_id) == raw
    finally:
        store.close()
