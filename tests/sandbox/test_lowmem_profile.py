"""Real worker limits; Mac admission is a fixture, not a 2GB host measurement."""

from __future__ import annotations

import asyncio
import io
import json
import os
from pathlib import Path

import pytest
from PIL import Image

from groupagent.artifacts import Deck, Report, Section, Slide
from groupagent.artifacts.worker_operations import pack_document
from groupagent.config import AppConfig
from groupagent.domain import ActorContext, DomainError, Role
from groupagent.media import IsolatedMediaWorker
from groupagent.resources import MemorySnapshot, ResourceGovernor, ResourcePolicy
from groupagent.runtime.resource_profile import worker_limits
from groupagent.sandbox import DockerSandbox
from groupagent.tools import ToolContext, ToolScope

pytestmark = pytest.mark.sandbox


@pytest.fixture
def measured_worker(tmp_path):
    image = os.environ.get("GROUPAGENT_WORKER_IMAGE")
    if not image:
        pytest.skip("Requires the reviewed immutable Docker worker image")
    config = AppConfig.model_validate(
        {
            "group": {
                "internal_id": "synthetic",
                "external_group_id": "synthetic",
                "owner_ids": ["owner"],
            },
            "resources": {"profile": "server_2gb"},
        }
    )
    governor = ResourceGovernor(
        ResourcePolicy(enabled=True, reserve_memory_mb=128, transfer_memory_mb=128),
        sampler=lambda: MemorySnapshot(2048 * 1024**2),
    )
    measurements = []

    class Measured(DockerSandbox):
        async def run(self, spec, context):
            result = await super().run(spec, context)
            measurements.append(
                {
                    "operation": context.call_id,
                    "elapsed_seconds": result.elapsed_seconds,
                    **result.resource_usage,
                }
            )
            return result

    sandbox = Measured(
        image,
        docker_host=os.environ.get("GROUPAGENT_DOCKER_HOST"),
        limits=worker_limits(config),
        resource_governor=governor,
    )
    actor = ActorContext(group_id="synthetic", member_id="owner", role=Role.OWNER)
    context = ToolContext(
        ToolScope(
            "synthetic", "lowmem", actor, tmp_path.resolve(), frozenset(), "synthetic-lowmem"
        ),
        "lowmem",
        "lowmem",
        asyncio.Event(),
    )
    yield IsolatedMediaWorker(sandbox), context
    assert not governor.busy and not governor.poisoned
    output = os.environ.get("GROUPAGENT_RESOURCE_EVIDENCE")
    if output:
        destination = Path(output)
        destination.mkdir(parents=True, exist_ok=True)
        (destination / (tmp_path.name + ".json")).write_text(json.dumps(measurements, indent=2))


@pytest.mark.parametrize("format_name", ["docx", "pdf", "pptx"])
async def test_generate_and_render_one_office_document_at_512mb(measured_worker, format_name):
    worker, context = measured_worker
    document = (
        Deck("Pot resource fixture", (Slide("Course result", ("Synthetic score 87",)),))
        if format_name == "pptx"
        else Report("Pot resource fixture", (Section("Course result", ("Synthetic score 87",)),))
    )
    packed, images = pack_document(document)
    _, files = await worker.run(
        context,
        "document_generate",
        images,
        parameters={"format": format_name, "document": packed},
        outputs=("result." + format_name,),
    )
    assert files[0]
    validation, previews = await worker.run(
        context,
        "document_validate",
        (files[0],),
        parameters={"format": format_name, "render": True, "expected_text": ["Synthetic score 87"]},
        outputs=("preview.png",),
    )
    assert validation["passed"] and previews[0].startswith(b"\x89PNG")


async def test_offline_chromium_at_512mb(measured_worker):
    worker, context = measured_worker
    result, files = await worker.run(
        context,
        "browser_snapshot",
        (b"<html><title>Pot fixture</title><body>Course result 87</body></html>",),
        outputs=("screenshot.png",),
    )
    assert "Course result 87" in result["text"] and files[0].startswith(b"\x89PNG")


async def test_real_worker_rejects_oversized_pixels_before_decoding(measured_worker):
    worker, context = measured_worker
    output = io.BytesIO()
    Image.new("RGB", (4000, 3500), "white").save(output, "PNG")
    with pytest.raises(DomainError) as error:
        await worker.run(
            context,
            "image_validate",
            (output.getvalue(),),
            parameters={"max_bytes": 8 * 1024**2, "max_pixels": 12_000_000},
            outputs=("view.png",),
        )
    assert error.value.code == "image_pixel_limit"
