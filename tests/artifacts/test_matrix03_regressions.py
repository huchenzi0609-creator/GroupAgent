"""Regression reproductions of synthetic matrix03 failures, without paid calls."""

from __future__ import annotations

import io

import pytest
from pypdf import PdfReader

from groupagent.artifacts import Deck, Report, Section, Slide, TableData, ValidationRecord
from groupagent.artifacts.advanced import edit_text_target
from groupagent.artifacts.formats import build_pdf, check_report
from groupagent.domain import DomainError


def test_blank_comment_paragraphs_and_blank_table_cells_are_valid_form_content(
    service, store, scope
):
    # Same structure as the failing synthetic scoring-form request. The empty
    # paragraphs were the failure; empty table cells were already supported.
    report = Report(
        "Scoring Form",
        (
            Section(
                "Scores",
                ("Synthetic form, without names or actual scores.",),
                tables=(
                    TableData(
                        ("Item", "Points", "Comment"),
                        (
                            ("Content", "40", ""),
                            ("Clarity", "35", ""),
                            ("Timing", "25", ""),
                            ("Total", "100", ""),
                        ),
                    ),
                ),
            ),
            Section("Comments", ("", "", "")),
        ),
    )
    check_report(report)
    for extension in ("md", "docx", "pdf"):
        version = service.generate(scope, "form." + extension, report)
        assert "Comments" in service.read(scope, version.file_id).text
        assert store.read(scope, version.file_id)
    assert len(PdfReader(io.BytesIO(build_pdf(report))).pages) == 1


def test_blank_pdf_paragraphs_create_real_writing_space_not_zero_height():
    def position(paragraphs):
        data = build_pdf(Report("Form", (Section("Comments", paragraphs),)))
        positions = []

        def visitor(text, cm, tm, font, size):
            if "After marker" in text:
                positions.append(cm[1] * tm[4] + cm[3] * tm[5] + cm[5])

        PdfReader(io.BytesIO(data)).pages[0].extract_text(visitor_text=visitor)
        assert len(positions) == 1
        return positions[0]

    normal = position(("Before marker", "After marker"))
    blank = position(("Before marker", "", "", "", "After marker"))
    assert normal - blank >= 40


def test_missing_literal_diagnostic_identifies_exact_phrase_without_semantic_guess(
    service, store, scope
):
    version = service.generate(
        scope,
        "budget.md",
        Report("合成预算", (Section("数据来源", ("所有数字来自合成请求，未进行联网调查。",)),)),
    )
    result = service.validate(scope, version.file_id, expected_text=("合成请求", "没有联网调查"))
    assert not result.passed and result.issues == ("missing_content:1",)
    assert result.failed_expectations == (
        {"index": 1, "expected": "没有联网调查", "stage": "extracted_text"},
    )
    assert "expected_text[1]" in result.failure_summary and "没有联网调查" in result.failure_summary
    assert "literal" in result.failure_summary
    assert ValidationRecord.from_dict(result.to_dict()) == result
    assert (
        store.validations(scope, version.file_id, version.version_id)[-1].failed_expectations
        == result.failed_expectations
    )
    corrected = service.validate(
        scope, version.file_id, expected_text=("合成请求", "未进行联网调查")
    )
    assert corrected.passed


def test_native_table_values_do_not_silently_satisfy_an_explicit_equals_literal(service, scope):
    deck = Deck(
        "Synthetic values",
        (
            Slide(
                "Values",
                (),
                TableData(("Name", "Value"), (("Alpha", "12"), ("Beta", "24"), ("Gamma", "36"))),
            ),
        ),
    )
    version = service.generate(scope, "values.pptx", deck)
    literal = service.validate(scope, version.file_id, expected_text=("Alpha=12",), render=False)
    assert not literal.content_passed
    assert literal.failed_expectations[0]["expected"] == "Alpha=12"
    cells = service.validate(
        scope,
        version.file_id,
        expected_text=("Alpha", "12", "Beta", "24", "Gamma", "36"),
        render=False,
    )
    assert cells.content_passed and not cells.passed  # Structure is not render evidence.


def test_markdown_native_text_target_reports_supported_edit_methods():
    with pytest.raises(DomainError) as error:
        edit_text_target(
            b"# Budget\n\n## Source\n\nSynthetic data\n",
            "md",
            "## Source",
            "## Source",
            "Total 1000\n\n## Source",
        )
    assert error.value.code == "unsupported_text_target_format"


def test_diagnostic_does_not_persist_secret_like_expected_text(service, scope):
    version = service.generate(
        scope, "note.md", Report("Note", (Section("Text", ("Synthetic note body",)),))
    )
    secret = "sk-synthetic_private_credential_1234567890"
    record = service.validate(scope, version.file_id, expected_text=(secret,))
    assert secret not in str(record.to_dict())
    assert "凭据已隐藏" in record.failure_summary
