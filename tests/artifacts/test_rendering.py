from __future__ import annotations

import os
from io import BytesIO
from pathlib import Path

import pytest
from pptx import Presentation
from pptx.util import Inches

from groupagent.artifacts import (
    ArtifactError,
    ArtifactScope,
    Deck,
    DocumentRenderer,
    DocumentService,
    Report,
    Section,
    Slide,
    TableData,
)

pytestmark = [
    pytest.mark.render,
    pytest.mark.skipif(
        os.environ.get("GROUPAGENT_RENDER_TESTS") != "1",
        reason="Set GROUPAGENT_RENDER_TESTS=1 with LibreOffice and Poppler installed",
    ),
]


@pytest.mark.parametrize("extension", ["docx", "pdf", "pptx"])
def test_real_render_content_layout_previews_and_scoping(store, scope, report, deck, extension):
    renderer = DocumentRenderer(pdftotext=os.environ.get("GROUPAGENT_PDFTOTEXT"))
    service = DocumentService(store, renderer=renderer)
    version = service.generate(
        scope, f"course.{extension}", deck if extension == "pptx" else report
    )
    validation = service.validate(
        scope, version.file_id, expected_text=("Findings", "87 percent", "Next Steps"), render=True
    )
    assert validation.structural_passed and validation.content_passed
    assert validation.rendered and validation.layout_passed, validation.issues
    assert validation.passed and validation.status == "validated"
    assert not validation.visual_reviewed  # Automated checks are recorded honestly.
    assert validation.page_count == (3 if extension == "pptx" else 1)
    for page_index in range(validation.page_count):
        preview = store.read_preview(
            scope,
            version.file_id,
            version.version_id,
            validation.validation_id,
            page_index=page_index,
        )
        assert preview.startswith(b"\x89PNG\r\n\x1a\n")
    with pytest.raises(ArtifactError) as error:
        store.read_preview(
            ArtifactScope("other-group", scope.work_id, scope.actor_id),
            version.file_id,
            version.version_id,
            validation.validation_id,
        )
    assert error.value.code == "scope_denied"


def test_rendered_edited_report_retains_table(store, scope, report):
    service = DocumentService(
        store, renderer=DocumentRenderer(pdftotext=os.environ.get("GROUPAGENT_PDFTOTEXT"))
    )
    original = service.generate(scope, "course.docx", report)
    edited = service.edit_section(
        scope,
        original.file_id,
        expected_version_id=original.version_id,
        section_index=1,
        paragraphs=("The study included 24 students.",),
    )
    validation = service.validate(
        scope,
        edited.file_id,
        edited.version_id,
        expected_text=("24 students", "87 percent", "Next Steps"),
        render=True,
    )
    assert validation.passed, validation.issues


@pytest.mark.parametrize("extension", ["docx", "pdf", "pptx"])
def test_chinese_render_with_explicit_fonts(store, scope, extension):
    font_dirs = os.environ.get("GROUPAGENT_FONT_DIRS")
    pdf_font = os.environ.get("GROUPAGENT_PDF_FONT")
    office_font = os.environ.get("GROUPAGENT_OFFICE_FONT")
    if not font_dirs or not pdf_font or not office_font:
        pytest.skip("Configure trusted CJK font directories, PDF font and Office font family")
    service = DocumentService(
        store,
        renderer=DocumentRenderer(
            pdftotext=os.environ.get("GROUPAGENT_PDFTOTEXT"),
            font_directories=tuple(Path(path) for path in font_dirs.split(os.pathsep)),
        ),
        pdf_font_path=Path(pdf_font),
        office_font_family=office_font,
    )
    table = TableData(("指标", "结果"), (("样本人数", "24人"), ("正确率", "87%")))
    report = Report(
        "课程项目报告",
        (
            Section("研究结果", ("本次课堂练习共邀请24名学生，比较两种学习方法。",), (table,)),
            Section("下一步", ("下学期扩大样本并重复实验。",)),
        ),
    )
    deck = Deck(
        "课程项目",
        (
            Slide("研究结果", ("本次课堂练习共邀请24名学生，比较两种学习方法。",), table),
            Slide("下一步", ("下学期扩大样本并重复实验。",)),
        ),
    )
    original = service.generate(
        scope, "课程报告." + extension, deck if extension == "pptx" else report
    )
    validation = service.validate(
        scope,
        original.file_id,
        expected_text=("研究结果", "24名学生", "87%", "下一步"),
        render=True,
    )
    assert validation.passed, validation.issues


def test_overflowing_text_frame_fails_actual_render_validation(store, scope, deck):
    service = DocumentService(
        store, renderer=DocumentRenderer(pdftotext=os.environ.get("GROUPAGENT_PDFTOTEXT"))
    )
    original = service.generate(scope, "course.pptx", deck)
    presentation = Presentation(BytesIO(store.read(scope, original.file_id)))
    body = next(shape for shape in presentation.slides[0].shapes if shape.name == "groupagent.body")
    body.height = Inches(0.2)
    output = BytesIO()
    presentation.save(output)
    broken = store.revise(
        scope, original.file_id, output.getvalue(), expected_version_id=original.version_id
    )
    validation = service.validate(
        scope, broken.file_id, expected_text=("Study Design",), render=True
    )
    assert validation.rendered and not validation.passed
    assert any(issue.startswith("text_frame_overflow:") for issue in validation.issues)
