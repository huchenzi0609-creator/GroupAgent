"""Scoped document operations suitable for a synthetic course report/deck slice."""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

from groupagent.domain import DomainError
from groupagent.security import redact_credentials

from .formats import (
    build_docx,
    build_markdown,
    build_pdf,
    build_pptx,
    edit_deck_slide,
    edit_report_section,
    extract,
)
from .models import (
    ArtifactError,
    ArtifactScope,
    ArtifactVersion,
    Deck,
    DocumentRead,
    DocumentUnit,
    Report,
    ValidationRecord,
)
from .rendering import DocumentRenderer, normalized_text
from .store import ArtifactStore, document_format


class DocumentService:
    def __init__(
        self,
        store: ArtifactStore,
        *,
        renderer: DocumentRenderer | None = None,
        pdf_font_path: Path | None = None,
        office_font_family: str = "Arial",
    ):
        self.store = store
        self.renderer = renderer or DocumentRenderer()
        self.pdf_font_path = pdf_font_path
        self.office_font_family = office_font_family

    def generate(self, scope: ArtifactScope, name: str, document: Report | Deck) -> ArtifactVersion:
        if not isinstance(scope, ArtifactScope):
            raise ArtifactError("invalid_scope", "A trusted ArtifactScope is required")
        format_name = document_format(name)
        if format_name == "pptx":
            if not isinstance(document, Deck):
                raise ArtifactError("invalid_document", "PPTX generation requires a Deck")
            content = build_pptx(document, font_family=self.office_font_family)
        else:
            if not isinstance(document, Report):
                raise ArtifactError("invalid_document", "Report generation requires a Report")
            if format_name == "pdf":
                content = build_pdf(document, font_path=self.pdf_font_path)
            elif format_name == "docx":
                content = build_docx(document, font_family=self.office_font_family)
            else:
                content = build_markdown(document)
        extract(content, format_name)
        return self.store.create(scope, name, content)

    def read(
        self, scope: ArtifactScope, file_id: str, version_id: str | None = None
    ) -> DocumentRead:
        version = self.store.get(scope, file_id, version_id)
        units = extract(self.store.read(scope, file_id, version.version_id), version.format)
        warnings: tuple[str, ...] = ()
        if version.format == "pdf" and any(not unit.text.strip() for unit in units):
            warnings = ("PDF pages without extractable text require OCR; OCR is not implemented",)
        return DocumentRead(file_id, version.version_id, version.format, units, warnings)

    def search(
        self, scope: ArtifactScope, file_id: str, query: str, version_id: str | None = None
    ) -> tuple[DocumentUnit, ...]:
        if not query.strip():
            raise ArtifactError("invalid_query", "Search text is required")
        return tuple(
            unit
            for unit in self.read(scope, file_id, version_id).units
            if query.casefold() in unit.text.casefold()
        )

    def _editable(
        self, scope: ArtifactScope, file_id: str, expected_version_id: str
    ) -> ArtifactVersion:
        current = self.store.get(scope, file_id)
        if current.version_id != expected_version_id:
            raise ArtifactError("version_conflict", "Current artifact version has changed")
        return current

    def edit_section(
        self,
        scope: ArtifactScope,
        file_id: str,
        *,
        expected_version_id: str,
        section_index: int,
        paragraphs: tuple[str, ...],
    ) -> ArtifactVersion:
        current = self._editable(scope, file_id, expected_version_id)
        content = self.store.read(scope, file_id, current.version_id)
        revised = edit_report_section(content, current.format, section_index, paragraphs)
        extract(revised, current.format)
        return self.store.revise(scope, file_id, revised, expected_version_id=expected_version_id)

    def edit_slide(
        self,
        scope: ArtifactScope,
        file_id: str,
        *,
        expected_version_id: str,
        slide_index: int,
        title: str | None = None,
        bullets: tuple[str, ...] | None = None,
    ) -> ArtifactVersion:
        current = self._editable(scope, file_id, expected_version_id)
        if current.format != "pptx":
            raise ArtifactError("unsupported_edit", "Slide editing requires PPTX")
        content = self.store.read(scope, file_id, current.version_id)
        revised = edit_deck_slide(content, slide_index, title=title, bullets=bullets)
        extract(revised, current.format)
        return self.store.revise(scope, file_id, revised, expected_version_id=expected_version_id)

    def rebuild_pdf(
        self, scope: ArtifactScope, file_id: str, report: Report, *, expected_version_id: str
    ) -> ArtifactVersion:
        current = self._editable(scope, file_id, expected_version_id)
        if current.format != "pdf":
            raise ArtifactError("unsupported_edit", "PDF rebuilding requires a PDF source")
        return self.store.revise(
            scope,
            file_id,
            build_pdf(report, font_path=self.pdf_font_path),
            expected_version_id=expected_version_id,
        )

    def validate(
        self,
        scope: ArtifactScope,
        file_id: str,
        version_id: str | None = None,
        *,
        expected_text: tuple[str, ...] = (),
        render: bool = False,
        ocr_fallback: bool = False,
    ) -> ValidationRecord:
        version = self.store.get(scope, file_id, version_id)
        if len(expected_text) > 100 or any(
            not isinstance(text, str) or not text.strip() or len(text) > 1000
            for text in expected_text
        ):
            raise ArtifactError(
                "invalid_assertion", "Expected text assertions must be nonempty strings"
            )
        content = self.store.read(scope, file_id, version.version_id)
        checks = ["sha256_integrity"]
        issues: list[str] = []
        failed_expectations: list[dict] = []
        structural = False
        content_passed = False
        previews: tuple[bytes, ...] = ()
        render_fields: dict = {}
        ocr_text: str | None = None
        try:
            units = extract(content, version.format)
            if version.format == "pdf" and ocr_fallback:
                from groupagent.media.validation import ocr_pdf

                recognized = ocr_pdf(content)
                if any(page.method == "tesseract" for page in recognized.pages):
                    units = tuple(
                        DocumentUnit(f"page:{page.page_number}", page.text)
                        for page in recognized.pages
                    )
                    ocr_text = "\n".join(page.text for page in recognized.pages)
                    checks.append("scanned_pdf_ocr")
            structural = bool(units)
            checks.append("structure_open")
            if not structural:
                issues.append("empty_document")
            actual = normalized_text("\n".join(unit.text for unit in units))
            if expected_text and all(isinstance(t, str) and t.strip() for t in expected_text):
                missing = [
                    str(i)
                    for i, text in enumerate(expected_text)
                    if normalized_text(text) not in actual
                ]
                checks.append("expected_text_assertions")
                issues.extend(f"missing_content:{index}" for index in missing)
                failed_expectations.extend(
                    {
                        "index": int(index),
                        "expected": redact_credentials(expected_text[int(index)]),
                        "stage": "extracted_text",
                    }
                    for index in missing
                )
                content_passed = not missing
            if render and version.format != "md":
                result = self.renderer.render(
                    content, version.format, expected_text=expected_text, ocr_text=ocr_text
                )
                checks.extend(result.checks)
                issues.extend(result.issues)
                for issue in result.issues:
                    if issue.startswith("rendered_missing_content:"):
                        index = int(issue.rsplit(":", 1)[1])
                        failed_expectations.append(
                            {
                                "index": index,
                                "expected": redact_credentials(expected_text[index]),
                                "stage": "rendered_text",
                            }
                        )
                previews = result.previews
                render_fields = {
                    "rendered": True,
                    "layout_passed": not result.issues and result.layout_supported,
                    "page_count": result.page_count,
                    "renderer": result.renderer,
                    "preview_sha256": result.preview_sha256,
                }
        except (ArtifactError, DomainError) as exc:
            issues.append(exc.code)
        record = ValidationRecord(
            file_id=file_id,
            version_id=version.version_id,
            sha256=version.sha256,
            format=version.format,
            structural_passed=structural,
            content_passed=content_passed,
            checks=tuple(checks),
            issues=tuple(issues),
            failed_expectations=tuple(failed_expectations),
            **render_fields,
            limitations=(
                "Expected-text assertions establish only the caller-supplied completion criteria",
                "Automatic layout checks cover the bounded report/template profile; no arbitrary fidelity",
                "OCR is an extraction aid; no semantic image review, font-glyph equivalence or accessibility certification",
                "Renderer time/profile bounds are not themselves evidence of OS isolation; deployment supplies the sandbox",
            ),
        )
        return self.store.record_validation(
            scope, file_id, version.version_id, record, previews=previews
        )

    def attest_visual_review(
        self,
        scope: ArtifactScope,
        file_id: str,
        version_id: str,
        *,
        validation_id: str,
        review_note: str,
    ) -> ValidationRecord:
        """Trusted reviewer attestation; deliberately not registered as a model tool."""
        if not review_note.strip():
            raise ArtifactError("invalid_review", "A review note is required")
        records = self.store.validations(scope, file_id, version_id)
        previous = next(
            (record for record in records if record.validation_id == validation_id), None
        )
        if previous is None or not previous.rendered or not previous.preview_sha256:
            raise ArtifactError("invalid_review", "A rendered validation with previews is required")
        if previous.parent_validation_id:
            raise ArtifactError("invalid_review", "Attest against the original rendered validation")
        reviewed = replace(
            previous,
            visual_reviewed=True,
            reviewed_by=scope.actor_id,
            review_note=review_note,
            parent_validation_id=validation_id,
            validation_id=uuid4().hex,
            created_at=datetime.now(UTC).isoformat(),
        )
        return self.store.record_validation(scope, file_id, version_id, reviewed)
