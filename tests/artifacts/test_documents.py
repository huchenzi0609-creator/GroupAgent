from __future__ import annotations

import io
import zipfile

import pytest
from docx import Document
from pptx import Presentation
from pypdf import PdfReader, PdfWriter

from groupagent.artifacts import ArtifactError, ArtifactScope, Deck, Report, Section, Slide


@pytest.mark.parametrize("extension", ["md", "docx", "pdf", "pptx"])
def test_actual_formats_generate_read_search_and_structural_validation(
    service,
    store,
    scope,
    report,
    deck,
    extension,
):
    version = service.generate(
        scope, f"course.{extension}", deck if extension == "pptx" else report
    )
    read = service.read(scope, version.file_id)
    assert "Findings" in read.text and "87 percent" in read.text
    assert service.search(scope, version.file_id, "87 percent")
    validation = service.validate(scope, version.file_id, expected_text=("Findings", "87 percent"))
    assert validation.structural_passed and validation.content_passed
    assert not validation.rendered
    assert validation.passed == (extension == "md")
    assert store.validations(scope, version.file_id) == (validation,)
    if extension == "pdf":
        assert read.units[0].locator == "page:1"


@pytest.mark.parametrize("extension", ["md", "docx"])
def test_second_section_shortening_preserves_table_and_other_sections(
    service,
    store,
    scope,
    report,
    extension,
):
    original = service.generate(scope, f"course.{extension}", report)
    original_bytes = store.read(scope, original.file_id)
    revised = service.edit_section(
        scope,
        original.file_id,
        expected_version_id=original.version_id,
        section_index=1,
        paragraphs=("The study included 24 students.",),
    )
    read = service.read(scope, revised.file_id)
    assert "The study included 24 students." in read.text
    assert "Students completed the same quiz" not in read.text
    assert "87 percent" in read.text and "Next Steps" in read.text and "Purpose" in read.text
    assert store.read(scope, original.file_id, original.version_id) == original_bytes
    if extension == "docx":
        first_table = Document(io.BytesIO(original_bytes)).tables[0]._tbl.xml
        second_table = Document(io.BytesIO(store.read(scope, revised.file_id))).tables[0]._tbl.xml
        assert first_table == second_table
    with pytest.raises(ArtifactError) as error:
        service.edit_section(
            scope,
            original.file_id,
            expected_version_id=original.version_id,
            section_index=1,
            paragraphs=("Stale edit",),
        )
    assert error.value.code == "version_conflict"


def test_slide_edit_preserves_table_and_other_slides(service, store, scope, deck):
    original = service.generate(scope, "course.pptx", deck)
    before = Presentation(io.BytesIO(store.read(scope, original.file_id)))
    revised = service.edit_slide(
        scope,
        original.file_id,
        expected_version_id=original.version_id,
        slide_index=1,
        title="Key Findings",
        bullets=("24 students took part.",),
    )
    after = Presentation(io.BytesIO(store.read(scope, revised.file_id)))
    assert before.slides[0]._element.xml == after.slides[0]._element.xml
    assert before.slides[2]._element.xml == after.slides[2]._element.xml
    before_table = next(s for s in before.slides[1].shapes if s.has_table)
    after_table = next(s for s in after.slides[1].shapes if s.has_table)
    assert before_table._element.xml == after_table._element.xml
    assert "Key Findings" in service.read(scope, revised.file_id).text


def test_pdf_edit_is_explicit_rebuild_retaining_source(service, store, scope, report):
    original = service.generate(scope, "course.pdf", report)
    source = store.read(scope, original.file_id)
    rebuilt = service.rebuild_pdf(
        scope,
        original.file_id,
        Report("Updated Course Report", (Section("Result", ("24 students.",)),)),
        expected_version_id=original.version_id,
    )
    assert "Updated Course Report" in service.read(scope, rebuilt.file_id).text
    assert store.read(scope, original.file_id, original.version_id) == source


def test_content_assertions_and_missing_renderer_never_claim_validated(service, scope, report):
    version = service.generate(scope, "course.docx", report)
    assert not service.validate(scope, version.file_id).passed
    missing = service.validate(scope, version.file_id, expected_text=("Missing conclusion",))
    assert missing.status == "failed" and "missing_content:0" in missing.issues
    service.renderer.pdftoppm = None
    unavailable = service.validate(
        scope, version.file_id, expected_text=("Course Project",), render=True
    )
    assert unavailable.structural_passed and not unavailable.rendered and not unavailable.passed
    assert "renderer_unavailable" in unavailable.issues


def test_cross_scope_edit_is_denied_before_format_work(service, store, scope, report):
    version = service.generate(scope, "course.docx", report)
    with pytest.raises(ArtifactError) as error:
        service.edit_section(
            ArtifactScope("other", scope.work_id, scope.actor_id),
            version.file_id,
            expected_version_id=version.version_id,
            section_index=1,
            paragraphs=("Changed",),
        )
    assert error.value.code == "scope_denied"
    assert store.get(scope, version.file_id) == version


@pytest.mark.parametrize("name", ["broken.docx", "broken.pptx", "broken.pdf", "broken.md"])
def test_corrupt_files_fail_diagnostically(service, store, scope, name):
    version = store.create(scope, name, b"\x00\xffbroken")
    with pytest.raises(ArtifactError) as error:
        service.read(scope, version.file_id)
    assert error.value.code in {"invalid_document", "format_mismatch"}
    assert not service.validate(scope, version.file_id, expected_text=("Something",)).passed


def test_encrypted_pdf_and_macro_office_refused(service, store, scope, report):
    original = service.generate(scope, "course.pdf", report)
    writer = PdfWriter()
    writer.append(PdfReader(io.BytesIO(store.read(scope, original.file_id))))
    writer.encrypt("synthetic-password")
    output = io.BytesIO()
    writer.write(output)
    encrypted = store.create(scope, "encrypted.pdf", output.getvalue())
    with pytest.raises(ArtifactError) as error:
        service.read(scope, encrypted.file_id)
    assert error.value.code == "encrypted_document"
    docx = service.generate(scope, "course.docx", report)
    output = io.BytesIO(store.read(scope, docx.file_id))
    with zipfile.ZipFile(output, "a") as archive:
        archive.writestr("word/vbaProject.bin", b"synthetic macro marker")
    macro = store.create(scope, "macro.docx", output.getvalue())
    with pytest.raises(ArtifactError) as error:
        service.read(scope, macro.file_id)
    assert error.value.code == "unsupported_feature"


def test_zip_path_escape_refused_without_extraction(service, store, scope, report, tmp_path):
    docx = service.generate(scope, "course.docx", report)
    output = io.BytesIO(store.read(scope, docx.file_id))
    with zipfile.ZipFile(output, "a") as archive:
        archive.writestr("../../escape", b"attack")
    unsafe = store.create(scope, "unsafe.docx", output.getvalue())
    with pytest.raises(ArtifactError) as error:
        service.read(scope, unsafe.file_id)
    assert error.value.code == "unsafe_archive"
    assert not (tmp_path / "escape").exists()


def test_embedded_video_is_rejected_before_office_processing(service, store, scope, deck):
    original = service.generate(scope, "course.pptx", deck)
    content = io.BytesIO(store.read(scope, original.file_id))
    with zipfile.ZipFile(content, "a") as archive:
        archive.writestr("ppt/media/video.mp4", b"synthetic unsupported video")
    embedded = store.create(scope, "embedded.pptx", content.getvalue())
    with pytest.raises(ArtifactError) as error:
        service.read(scope, embedded.file_id)
    assert error.value.code == "unsupported_video"


def test_unsupported_edits_and_video_do_not_create_files(service, store, scope, report):
    with pytest.raises(ArtifactError) as error:
        service.generate(scope, "course.mp4", report)
    assert error.value.code == "unsupported_video"
    assert store.list(scope) == ()
    with pytest.raises(ArtifactError) as error:
        service.generate(scope, "oversized.pptx", Deck("Title", (Slide("Title", ("x" * 181,)),)))
    assert error.value.code == "invalid_document"
    version = store.create(scope, "report.md", b"# Title\n\n## Section\n\n![chart](chart.png)\n")
    with pytest.raises(ArtifactError) as error:
        service.edit_section(
            scope,
            version.file_id,
            expected_version_id=version.version_id,
            section_index=0,
            paragraphs=("Replacement",),
        )
    assert error.value.code == "unsupported_feature"
