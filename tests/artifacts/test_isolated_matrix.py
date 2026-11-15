from __future__ import annotations

import asyncio
import os

import pytest

from groupagent.artifacts import ArtifactScope, ArtifactStore, IsolatedDocumentService
from groupagent.domain import ActorContext, Role
from groupagent.media import IsolatedMediaWorker
from groupagent.sandbox import DockerSandbox, SandboxLimits
from groupagent.tools import ToolContext, ToolScope

from .test_extended_documents import matrix_document
from .test_native_locators import budget_report


@pytest.fixture
def document_runtime(tmp_path):
    image = os.environ.get("GROUPAGENT_WORKER_IMAGE")
    if not image:
        pytest.skip("Requires immutable media Docker worker")
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    context = ToolContext(
        ToolScope(
            "matrix-group",
            "matrix-work",
            ActorContext(group_id="matrix-group", member_id="matrix-author", role=Role.OWNER),
            workspace.resolve(),
            frozenset(),
            "matrix-trace",
        ),
        "matrix-call",
        "matrix-key",
        asyncio.Event(),
    )
    worker = IsolatedMediaWorker(
        DockerSandbox(
            image,
            docker_host=os.environ.get("GROUPAGENT_DOCKER_HOST"),
            limits=SandboxLimits(memory_mb=1024, workspace_mb=256, temp_mb=256, pids=128),
        )
    )
    with ArtifactStore(tmp_path / "store") as store:
        yield IsolatedDocumentService(store, worker), context


@pytest.mark.parametrize("extension", ["md", "docx", "pdf", "pptx"])
@pytest.mark.parametrize("case", range(10))
async def test_real_isolated_ten_case_matrix(document_runtime, extension, case):
    service, context = document_runtime
    version = await service.generate(
        context, f"matrix-{case}.{extension}", matrix_document(case, extension)
    )
    validation = await service.validate(
        context, version.file_id, expected_text=("87",), render=extension != "md"
    )
    assert validation.passed, (extension, case, validation.issues)
    if extension != "md":
        assert validation.rendered and validation.preview_sha256
    if extension == "pptx" and case == 9:
        assert validation.page_count == 10


async def test_real_native_docx_read_search_edit_uses_returned_locator(document_runtime):
    service, context = document_runtime
    scope = ArtifactScope(context.group_id, context.work_id, context.actor.member_id)
    original = await service.generate(context, "native-budget.docx", budget_report())
    before = service.store.read(scope, original.file_id, original.version_id)
    read = await service.read(context, original.file_id)
    found = await service.search(context, original.file_id, "预算100元")
    assert found["match_count"] == 1
    target = found["units"][0]
    assert target["editable"] and target["locator"].startswith("paragraph:")
    assert target["locator"] in {unit.locator for unit in read.units if unit.editable}
    revised = await service.edit_target(
        context,
        original.file_id,
        expected_version_id=original.version_id,
        target=target["locator"],
        expected_text="预算100元",
        replacement="预算150元",
    )
    after = await service.read(context, revised.file_id, revised.version_id)
    assert [(unit.locator, unit.text) for unit in after.units] == [
        (
            unit.locator,
            unit.text.replace("预算100元", "预算150元")
            if unit.locator == target["locator"]
            else unit.text,
        )
        for unit in read.units
    ]
    assert revised.version_id != original.version_id
    assert service.store.read(scope, original.file_id, original.version_id) == before


@pytest.mark.parametrize("extension", ["docx", "pptx"])
async def test_real_ordinary_template_without_private_marker_is_validated(
    document_runtime, extension
):
    import io

    from docx import Document
    from pptx import Presentation

    service, context = document_runtime
    if extension == "docx":
        template = Document()
        template.add_heading("{{COURSE}}", 0)
        template.add_paragraph("Result 87 from a synthetic course exercise.")
        template.sections[0].header.paragraphs[0].text = "Public template header"
    else:
        from pptx.util import Inches, Pt

        template = Presentation()
        slide = template.slides.add_slide(template.slide_layouts[6])
        title = slide.shapes.add_textbox(Inches(0.5), Inches(0.5), Inches(8), Inches(1))
        title.text = "{{COURSE}}"
        title.text_frame.paragraphs[0].font.size = Pt(28)
        body = slide.shapes.add_textbox(Inches(0.5), Inches(2), Inches(8), Inches(2))
        body.text = "Result 87 from a synthetic course exercise."
        body.text_frame.paragraphs[0].font.size = Pt(22)
    template.core_properties.keywords = ""
    buffer = io.BytesIO()
    template.save(buffer)
    version = await service.import_document(context, "template." + extension, buffer.getvalue())
    filled = await service.fill_template(
        context,
        version.file_id,
        expected_version_id=version.version_id,
        replacements={"{{COURSE}}": "Synthetic Course"},
    )
    validation = await service.validate(
        context, filled.file_id, expected_text=("Synthetic Course", "87")
    )
    assert validation.passed and validation.rendered, validation.issues
    if extension == "docx":
        assert "Public template header" in (await service.read(context, filled.file_id)).text


async def test_real_render_rejects_text_hidden_by_later_opaque_shape(document_runtime):
    import io

    from pptx import Presentation
    from pptx.dml.color import RGBColor
    from pptx.enum.shapes import MSO_SHAPE
    from pptx.util import Inches, Pt

    service, context = document_runtime
    presentation = Presentation()
    slide = presentation.slides.add_slide(presentation.slide_layouts[6])
    title = slide.shapes.add_textbox(Inches(0.5), Inches(0.5), Inches(8), Inches(1))
    title.text = "Visible Title"
    body = slide.shapes.add_textbox(Inches(0.5), Inches(2), Inches(7), Inches(1))
    body.text = "Hidden result 87"
    body.text_frame.paragraphs[0].font.size = Pt(24)
    cover = slide.shapes.add_shape(
        MSO_SHAPE.RECTANGLE, Inches(0.4), Inches(1.9), Inches(7.5), Inches(1.2)
    )
    cover.fill.solid()
    cover.fill.fore_color.rgb = RGBColor(255, 255, 255)
    cover.line.fill.background()
    output = io.BytesIO()
    presentation.save(output)
    version = await service.import_document(context, "covered.pptx", output.getvalue())
    validation = await service.validate(
        context, version.file_id, expected_text=("Visible Title", "87")
    )
    assert validation.rendered and not validation.passed
    assert any(item.startswith("text_occluded:") for item in validation.issues), validation.issues
