"""Deliberately limited report/deck formats; never execute attachment content."""

from __future__ import annotations

import base64
import hashlib
import io
import re
import zipfile
from pathlib import Path, PurePosixPath
from typing import Any
from xml.sax.saxutils import escape

from docx import Document
from docx.enum.table import WD_CELL_VERTICAL_ALIGNMENT
from docx.oxml import OxmlElement
from docx.oxml.ns import qn
from docx.shared import Inches as WordInches
from docx.shared import Pt as WordPt
from docx.shared import RGBColor as WordColor
from docx.table import Table
from docx.text.paragraph import Paragraph as WordParagraph
from pptx import Presentation
from pptx.dml.color import RGBColor
from pptx.enum.text import MSO_ANCHOR, MSO_AUTO_SIZE
from pptx.util import Inches, Pt
from pypdf import PdfReader
from pypdf.generic import ArrayObject, FloatObject, NullObject, NumberObject
from reportlab.lib import colors
from reportlab.lib.enums import TA_LEFT
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.cidfonts import UnicodeCIDFont
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.platypus import Image as PDFImage
from reportlab.platypus import KeepTogether, Paragraph, SimpleDocTemplate, Spacer, TableStyle
from reportlab.platypus import Table as PDFTable

from .models import ArtifactError, Deck, DocumentUnit, Report, TableData
from .store import VIDEO_SUFFIXES
from .visuals import add_slide_visuals, chart_png, chart_summary, check_chart, validated_image

REPORT_MARKER = "groupagent-report-v1"
DECK_MARKER = "groupagent-deck-v1"
MAX_PAGES = 200
MAX_SLIDES = 100


def _text(value: str, *, maximum: int = 5000, allow_empty: bool = False) -> str:
    if (
        not isinstance(value, str)
        or (not value.strip() and not allow_empty)
        or len(value) > maximum
        or any(ord(c) < 32 and c not in "\t\n" for c in value)
    ):
        raise ArtifactError(
            "invalid_document", "Document text is empty, too long or contains controls"
        )
    return value


def _table(table: TableData) -> None:
    if not 1 <= len(table.headers) <= 6 or len(table.rows) > 50:
        raise ArtifactError("unsupported_layout", "Tables support 1-6 columns and at most 50 rows")
    for row in (table.headers, *table.rows):
        if len(row) != len(table.headers):
            raise ArtifactError("invalid_document", "Table rows must match the header width")
        for value in row:
            _text(value, maximum=250, allow_empty=True)


def check_report(report: Report) -> None:
    _text(report.title, maximum=180)
    _text(report.subtitle, maximum=500, allow_empty=True)
    if not 1 <= len(report.sections) <= 50:
        raise ArtifactError("invalid_document", "A report needs 1-50 sections")
    for section in report.sections:
        _text(section.title, maximum=180)
        if len(section.paragraphs) > 50 or len(section.tables) > 5:
            raise ArtifactError("unsupported_layout", "Section exceeds the supported layout limits")
        for paragraph in section.paragraphs:
            # Explicit empty paragraphs are form-writing space, not missing data.
            _text(paragraph, allow_empty=True)
        for table in section.tables:
            _table(table)
        if len(section.images) > 3 or len(section.charts) > 3:
            raise ArtifactError(
                "document_limit", "A section supports at most three images and charts"
            )
        for image in section.images:
            validated_image(image)
        for chart in section.charts:
            check_chart(chart)
    if sum(len(section.images) for section in report.sections) > 10:
        raise ArtifactError("document_limit", "Report supports at most ten source images")
    character_count = (
        len(report.title)
        + len(report.subtitle)
        + sum(
            len(section.title)
            + sum(map(len, section.paragraphs))
            + sum(
                len(text)
                for table in section.tables
                for row in (table.headers, *table.rows)
                for text in row
            )
            for section in report.sections
        )
    )
    if character_count > 200_000:
        raise ArtifactError("document_limit", "Report text exceeds 200000 characters")


def check_deck(deck: Deck) -> None:
    _text(deck.title, maximum=120)
    if not 1 <= len(deck.slides) <= MAX_SLIDES:
        raise ArtifactError("invalid_document", "A deck needs 1-100 slides")
    for slide in deck.slides:
        _text(slide.title, maximum=90)
        if len(slide.bullets) > (2 if slide.image or slide.chart else 3 if slide.table else 6):
            raise ArtifactError(
                "unsupported_layout", "Slide has too many paragraphs for this layout"
            )
        for bullet in slide.bullets:
            _text(bullet, maximum=180)
        if slide.image:
            validated_image(slide.image)
        if slide.chart:
            check_chart(slide.chart)
        if slide.table:
            _table(slide.table)
            if len(slide.table.rows) > 5 or len(slide.table.headers) > 4:
                raise ArtifactError(
                    "unsupported_layout", "Slide table supports up to 5 rows and 4 columns"
                )
            if any(len(t) > 55 for row in (slide.table.headers, *slide.table.rows) for t in row):
                raise ArtifactError("unsupported_layout", "Slide table cell text is too long")


def _md_escape(value: str) -> str:
    return value.replace("\\", "\\\\").replace("|", "\\|").replace("\n", " ")


def build_markdown(report: Report) -> bytes:
    check_report(report)
    # A Report contains plain paragraphs; embedded Markdown blocks would make the
    # subsequent section edit ambiguous, so reject rather than silently drop them.
    for section in report.sections:
        for paragraph in section.paragraphs:
            if re.search(r"^(?:#{1,6} |```|~~~|>|[-*] |\d+\. |\|)|!\[|<", paragraph, re.MULTILINE):
                raise ArtifactError(
                    "unsupported_markdown_block", "Report Markdown paragraphs must be plain text"
                )
    parts = [f"# {report.title}", report.subtitle] if report.subtitle else [f"# {report.title}"]
    for section in report.sections:
        parts.extend((f"## {section.title}", *section.paragraphs))
        for table in section.tables:
            rows = [
                "| " + " | ".join(_md_escape(c) for c in row) + " |"
                for row in (table.headers, *table.rows)
            ]
            rows.insert(1, "| " + " | ".join("---" for _ in table.headers) + " |")
            parts.append("\n".join(rows))
        for image in section.images:
            content, _, _ = validated_image(image)
            alt = (
                image.alt_text.replace("\\", "\\\\")
                .replace("[", "\\[")
                .replace("]", "\\]")
                .replace("\n", " ")
            )
            parts.append(f"![{alt}](data:image/png;base64,{base64.b64encode(content).decode()})")
        for chart in section.charts:
            parts.append(chart_summary(chart))
    return ("\n\n".join(parts) + "\n").encode("utf-8")


def build_docx(report: Report, *, font_family: str = "Arial") -> bytes:
    check_report(report)
    document = Document()
    document.core_properties.title = report.title
    document.core_properties.author = "GroupAgent"
    document.core_properties.keywords = REPORT_MARKER
    page = document.sections[0]
    page.top_margin = page.bottom_margin = WordInches(0.8)
    page.left_margin = page.right_margin = WordInches(0.85)
    for name, size in (("Normal", 11), ("Title", 26), ("Heading 1", 16)):
        style = document.styles[name]
        style.font.name = font_family
        fonts = style.element.get_or_add_rPr().rFonts
        for attribute in ("asciiTheme", "hAnsiTheme", "eastAsiaTheme", "cstheme"):
            fonts.attrib.pop(qn(f"w:{attribute}"), None)
        fonts.set(qn("w:eastAsia"), font_family)
        fonts.set(qn("w:cs"), font_family)
        style.font.size = WordPt(size)
        style.font.color.rgb = WordColor(0, 0, 0)
        style.paragraph_format.space_after = WordPt(8)
        style.paragraph_format.line_spacing = 1.15
    for border in document.styles.element.xpath(".//w:pBdr"):
        border.getparent().remove(border)
    document.add_heading(report.title, 0)
    if report.subtitle:
        document.add_paragraph(report.subtitle)
    for section in report.sections:
        document.add_heading(section.title, 1)
        for text in section.paragraphs:
            document.add_paragraph(text)
        for table_data in section.tables:
            table = document.add_table(rows=1, cols=len(table_data.headers))
            table.style = "Table Grid"
            borders = OxmlElement("w:tblBorders")
            for edge in ("top", "left", "bottom", "right", "insideH", "insideV"):
                border = OxmlElement(f"w:{edge}")
                for key, value in (("val", "single"), ("sz", "4"), ("color", "D9D9D9")):
                    border.set(qn(f"w:{key}"), value)
                borders.append(border)
            table._tbl.tblPr.append(borders)
            for index, text in enumerate(table_data.headers):
                cell = table.rows[0].cells[index]
                cell.text = text
                shading = OxmlElement("w:shd")
                shading.set(qn("w:fill"), "E8EFF5")
                cell._tc.get_or_add_tcPr().append(shading)
                for run in cell.paragraphs[0].runs:
                    run.bold = True
            repeat = OxmlElement("w:tblHeader")
            table.rows[0]._tr.get_or_add_trPr().append(repeat)
            for row in table_data.rows:
                cells = table.add_row().cells
                for index, text in enumerate(row):
                    cells[index].text = text
            for row in table.rows:
                for cell in row.cells:
                    cell.vertical_alignment = WD_CELL_VERTICAL_ALIGNMENT.CENTER
                    margins = OxmlElement("w:tcMar")
                    for edge in ("top", "bottom", "left", "right"):
                        margin = OxmlElement(f"w:{edge}")
                        margin.set(qn("w:w"), "100")
                        margin.set(qn("w:type"), "dxa")
                        margins.append(margin)
                    cell._tc.get_or_add_tcPr().append(margins)
                    for paragraph in cell.paragraphs:
                        paragraph.paragraph_format.space_after = WordPt(0)
            document.add_paragraph()
        for image in section.images:
            content, _, _ = validated_image(image)
            paragraph = document.add_paragraph()
            paragraph.paragraph_format.keep_with_next = True
            picture = paragraph.add_run().add_picture(
                io.BytesIO(content), width=WordInches(image.width_inches)
            )
            picture._inline.docPr.set("descr", image.alt_text)
            document.add_paragraph(image.alt_text, style="Caption")
        for chart in section.charts:
            paragraph = document.add_paragraph()
            paragraph.paragraph_format.keep_with_next = True
            paragraph.add_run().add_picture(io.BytesIO(chart_png(chart)), width=WordInches(5.8))
            document.add_paragraph(chart_summary(chart), style="Caption")
    output = io.BytesIO()
    document.save(output)
    return output.getvalue()


def _pdf_font(report: Report, font_path: Path | None) -> str:
    if font_path is not None:
        if not font_path.is_file() or font_path.stat().st_size > 64 * 1024 * 1024:
            raise ArtifactError("font_unavailable", "Configured PDF font is missing or too large")
        name = "GroupAgentFont-" + hashlib.sha256(str(font_path).encode()).hexdigest()[:16]
        if name not in pdfmetrics.getRegisteredFontNames():
            try:
                pdfmetrics.registerFont(TTFont(name, str(font_path)))
            except Exception as exc:
                raise ArtifactError(
                    "font_unavailable", "Configured PDF font cannot be loaded"
                ) from exc
        return name
    # ReportLab's built-in CJK font has a Unicode cmap; no font download occurs.
    if any(ord(c) > 255 for c in repr(report)):
        if "STSong-Light" not in pdfmetrics.getRegisteredFontNames():
            pdfmetrics.registerFont(UnicodeCIDFont("STSong-Light"))
        return "STSong-Light"
    return "Helvetica"


def build_pdf(report: Report, *, font_path: Path | None = None) -> bytes:
    check_report(report)
    output = io.BytesIO()
    font = _pdf_font(report, font_path)
    styles = getSampleStyleSheet()
    body = ParagraphStyle(
        "ReportBody",
        parent=styles["BodyText"],
        fontName=font,
        fontSize=11,
        leading=16,
        spaceAfter=9,
        wordWrap="CJK",
    )
    heading = ParagraphStyle(
        "ReportHeading",
        parent=body,
        fontSize=16,
        leading=21,
        spaceBefore=12,
        spaceAfter=9,
        keepWithNext=True,
    )
    title = ParagraphStyle(
        "ReportTitle", parent=heading, fontSize=25, leading=31, spaceBefore=0, spaceAfter=15
    )
    cell_style = ParagraphStyle(
        "Cell", parent=body, fontSize=9, leading=13, spaceAfter=0, alignment=TA_LEFT
    )
    document = SimpleDocTemplate(
        output,
        pagesize=A4,
        leftMargin=54,
        rightMargin=54,
        topMargin=50,
        bottomMargin=50,
        title=report.title,
        author="GroupAgent",
        subject=REPORT_MARKER,
    )
    story: list[Any] = [Paragraph(escape(report.title), title)]
    if report.subtitle:
        story.append(Paragraph(escape(report.subtitle), body))
    for section in report.sections:
        story.append(Paragraph(escape(section.title), heading))
        for text in section.paragraphs:
            if text.strip():
                story.append(Paragraph(escape(text).replace("\n", "<br/>"), body))
            else:
                story.append(Spacer(1, body.leading))
        for data in section.tables:
            rows = [
                [Paragraph(escape(text), cell_style) for text in row]
                for row in (data.headers, *data.rows)
            ]
            table = PDFTable(
                rows,
                colWidths=[(A4[0] - 108) / len(data.headers)] * len(data.headers),
                repeatRows=1,
                hAlign="LEFT",
            )
            table.setStyle(
                TableStyle(
                    [
                        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#E8EFF5")),
                        ("GRID", (0, 0), (-1, -1), 0.5, colors.HexColor("#D9D9D9")),
                        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
                        ("LEFTPADDING", (0, 0), (-1, -1), 8),
                        ("RIGHTPADDING", (0, 0), (-1, -1), 8),
                        ("TOPPADDING", (0, 0), (-1, -1), 7),
                        ("BOTTOMPADDING", (0, 0), (-1, -1), 7),
                    ]
                )
            )
            story.extend((table, Spacer(1, 10)))
        for image in section.images:
            content, width, height = validated_image(image)
            scaled_width = min(image.width_inches * 72, 400 * width / height)
            story.append(
                KeepTogether(
                    [
                        PDFImage(
                            io.BytesIO(content),
                            width=scaled_width,
                            height=scaled_width * height / width,
                        ),
                        Paragraph(escape(image.alt_text), body),
                        Spacer(1, 10),
                    ]
                )
            )
        for chart in section.charts:
            story.append(
                KeepTogether(
                    [
                        PDFImage(io.BytesIO(chart_png(chart)), width=480, height=288),
                        Paragraph(escape(chart_summary(chart)), body),
                        Spacer(1, 10),
                    ]
                )
            )
    document.build(story)
    return output.getvalue()


def _slide_text(
    shape: Any,
    texts: tuple[str, ...],
    size: int,
    *,
    bold: bool = False,
    font_family: str | None = None,
) -> None:
    frame = shape.text_frame
    font_family = font_family or frame.paragraphs[0].font.name or "Arial"
    frame.clear()
    frame.word_wrap = True
    frame.auto_size = MSO_AUTO_SIZE.NONE
    frame.margin_left = frame.margin_right = Inches(0.08)
    frame.margin_top = frame.margin_bottom = Inches(0.06)
    frame.vertical_anchor = MSO_ANCHOR.TOP
    for index, text in enumerate(texts):
        paragraph = frame.paragraphs[0] if index == 0 else frame.add_paragraph()
        paragraph.text = text
        paragraph.font.name = font_family
        paragraph.font.size = Pt(size)
        paragraph.font.bold = bold
        paragraph.font.color.rgb = RGBColor(20, 33, 46)
        paragraph.space_after = Pt(14)


def build_pptx(deck: Deck, *, font_family: str = "Arial") -> bytes:
    check_deck(deck)
    presentation = Presentation()
    presentation.slide_width = Inches(13.333)
    presentation.slide_height = Inches(7.5)
    presentation.core_properties.title = deck.title
    presentation.core_properties.author = "GroupAgent"
    presentation.core_properties.keywords = DECK_MARKER
    for index, data in enumerate(deck.slides):
        slide = presentation.slides.add_slide(presentation.slide_layouts[6])
        title = slide.shapes.add_textbox(Inches(0.7), Inches(0.5), Inches(11.9), Inches(1.0))
        title.name = "groupagent.title"
        _slide_text(title, (data.title,), 30, bold=True, font_family=font_family)
        body = slide.shapes.add_textbox(
            Inches(0.7),
            Inches(1.75),
            Inches(11.9),
            Inches(1.4 if data.image or data.chart else 1.8 if data.table else 4.7),
        )
        body.name = "groupagent.body"
        _slide_text(body, data.bullets, 22, font_family=font_family)
        if data.image or data.chart:
            add_slide_visuals(slide, data, font_family)
        elif data.table:
            rows = (data.table.headers, *data.table.rows)
            shape = slide.shapes.add_table(
                len(rows), len(rows[0]), Inches(0.8), Inches(3.8), Inches(11.7), Inches(2.6)
            )
            shape.name = "groupagent.table"
            for row_index, row in enumerate(rows):
                for col_index, text in enumerate(row):
                    cell = shape.table.cell(row_index, col_index)
                    cell.text = text
                    cell.margin_left = cell.margin_right = Inches(0.1)
                    cell.vertical_anchor = MSO_ANCHOR.MIDDLE
                    cell.fill.solid()
                    cell.fill.fore_color.rgb = RGBColor(
                        *((232, 239, 245) if row_index == 0 else (255, 255, 255))
                    )
                    for paragraph in cell.text_frame.paragraphs:
                        paragraph.font.name = font_family
                        paragraph.font.size = Pt(16)
                        paragraph.font.bold = row_index == 0
                        paragraph.font.color.rgb = RGBColor(20, 33, 46)
        footer = slide.shapes.add_textbox(Inches(0.7), Inches(7.0), Inches(11.9), Inches(0.3))
        footer.name = "groupagent.footer"
        _slide_text(footer, (f"{deck.title}   |   {index + 1}",), 10, font_family=font_family)
    output = io.BytesIO()
    presentation.save(output)
    return output.getvalue()


def inspect_office(
    content: bytes, format_name: str, *, for_edit: bool = False, for_render: bool = False
) -> None:
    """Reject dangerous/unsupported packages before handing bytes to a parser."""
    if content.startswith(bytes.fromhex("d0cf11e0")):
        raise ArtifactError(
            "encrypted_document", "Encrypted or legacy Office containers are unsupported"
        )
    try:
        with zipfile.ZipFile(io.BytesIO(content)) as archive:
            infos = archive.infolist()
            if len(infos) > 4096 or sum(i.file_size for i in infos) > 120 * 1024 * 1024:
                raise ArtifactError("archive_limit", "Office package exceeds expansion limits")
            names = [i.filename for i in infos]
            if len(set(names)) != len(names):
                raise ArtifactError("invalid_document", "Office package has duplicate members")
            required = "word/document.xml" if format_name == "docx" else "ppt/presentation.xml"
            if required not in names or "[Content_Types].xml" not in names:
                raise ArtifactError(
                    "format_mismatch", "Office package does not match its extension"
                )
            for info in infos:
                path = PurePosixPath(info.filename)
                if path.is_absolute() or ".." in path.parts or "\\" in info.filename:
                    raise ArtifactError("unsafe_archive", "Office package contains unsafe paths")
                if info.flag_bits & 1:
                    raise ArtifactError("encrypted_document", "Encrypted packages are unsupported")
                if info.file_size > max(1, info.compress_size) * 200:
                    raise ArtifactError(
                        "archive_limit", "Office package expansion ratio is too high"
                    )
                lower = info.filename.lower()
                if PurePosixPath(lower).suffix in VIDEO_SUFFIXES:
                    raise ArtifactError(
                        "unsupported_video", "Embedded video is outside this worker"
                    )
                benign_printer_settings = re.fullmatch(
                    r"(?:word|ppt)/printersettings/printersettings\d+\.bin", lower
                )
                chart_workbook = format_name == "pptx" and re.fullmatch(
                    r"ppt/embeddings/[^/]+\.xlsx", lower
                )
                if chart_workbook:
                    with zipfile.ZipFile(io.BytesIO(archive.read(info))) as workbook:
                        if (
                            "xl/workbook.xml" not in workbook.namelist()
                            or sum(item.file_size for item in workbook.infolist()) > 5 * 1024 * 1024
                        ):
                            raise ArtifactError(
                                "unsafe_archive", "Chart workbook is invalid or too large"
                            )
                        for entry in workbook.namelist():
                            if (
                                "vbaproject" in entry.lower()
                                or entry.endswith(".bin")
                                or ".." in PurePosixPath(entry).parts
                            ):
                                raise ArtifactError(
                                    "unsupported_feature",
                                    "Chart workbook contains active or unsafe content",
                                )
                            if entry.endswith(
                                ".rels"
                            ) and b'TargetMode="External"' in workbook.read(entry):
                                raise ArtifactError(
                                    "external_reference", "External chart workbooks are unsupported"
                                )
                if (
                    "vbaproject" in lower
                    or ("/embeddings/" in lower and not chart_workbook)
                    or (lower.endswith(".bin") and not benign_printer_settings)
                ):
                    raise ArtifactError(
                        "unsupported_feature", "Macros and embedded objects are unsupported"
                    )
                if info.filename.endswith((".xml", ".rels")):
                    data = archive.read(info)
                    if b"<!DOCTYPE" in data.upper() or b"<!ENTITY" in data.upper():
                        raise ArtifactError("unsafe_archive", "Custom XML entities are unsupported")
                    if (
                        for_render
                        and info.filename.endswith(".rels")
                        and re.search(
                            rb"TargetMode\s*=\s*[\"']External[\"']", data, flags=re.IGNORECASE
                        )
                    ):
                        raise ArtifactError(
                            "external_reference", "External links are not rendered automatically"
                        )
                    if for_edit and re.search(
                        rb"<(?:\w+:)?(?:ins|del|object|altChunk|sdt|timing|oleObj)\b", data
                    ):
                        raise ArtifactError(
                            "unsupported_feature", "This edit cannot preserve advanced content"
                        )
    except (zipfile.BadZipFile, OSError, RuntimeError) as exc:
        raise ArtifactError("invalid_document", "Office package cannot be opened") from exc


def _passive_open_destination(value: Any) -> bool:
    if value is None:
        return True
    if hasattr(value, "get_object"):
        value = value.get_object()
    if not isinstance(value, ArrayObject) or not 2 <= len(value) <= 6:
        return False
    page = value[0].get_object() if hasattr(value[0], "get_object") else value[0]
    return (
        isinstance(page, dict)
        and page.get("/Type") == "/Page"
        and value[1] in {"/XYZ", "/Fit", "/FitH", "/FitV", "/FitR", "/FitB", "/FitBH", "/FitBV"}
        and all(isinstance(part, (NumberObject, FloatObject, NullObject)) for part in value[2:])
    )


def open_pdf(content: bytes) -> PdfReader:
    if not content.startswith(b"%PDF-"):
        raise ArtifactError("format_mismatch", "PDF signature is missing")
    try:
        reader = PdfReader(io.BytesIO(content), strict=True)
        if reader.is_encrypted:
            raise ArtifactError("encrypted_document", "Encrypted PDF is unsupported")
        root: Any = reader.trailer["/Root"]
        names: Any = root.get("/Names", {})
        if hasattr(names, "get_object"):
            names = names.get_object()
        if (
            root.get("/AA")
            or not _passive_open_destination(root.get("/OpenAction"))
            or names.get("/JavaScript")
            or names.get("/EmbeddedFiles")
        ):
            raise ArtifactError(
                "unsupported_feature", "Active PDF actions and embedded files are unsupported"
            )
        if not 1 <= len(reader.pages) <= MAX_PAGES:
            raise ArtifactError("page_limit", "PDF must contain 1-200 pages")
        for page in reader.pages:
            for reference in page.get("/Annots", ()):
                annotation: Any = reference.get_object()
                if annotation.get("/Subtype") in {"/Movie", "/RichMedia", "/Screen"}:
                    raise ArtifactError("unsupported_video", "Embedded PDF video is unsupported")
                action: Any = annotation.get("/A", {})
                if hasattr(action, "get_object"):
                    action = action.get_object()
                if (
                    annotation.get("/AA")
                    or action.get("/Next")
                    or action.get("/S")
                    in {
                        "/JavaScript",
                        "/Launch",
                    }
                ):
                    raise ArtifactError(
                        "unsupported_feature", "Active PDF annotations are unsupported"
                    )
        return reader
    except ArtifactError:
        raise
    except Exception as exc:
        raise ArtifactError("invalid_document", "PDF cannot be opened") from exc


def extract(
    content: bytes, format_name: str, *, native_targets: bool = False
) -> tuple[DocumentUnit, ...]:
    try:
        if format_name == "md":
            text = content.decode("utf-8")
            if "\x00" in text:
                raise ArtifactError("invalid_document", "Markdown contains binary data")
            text = re.sub(
                r"data:image/[a-z]+;base64,[A-Za-z0-9+/=]+", "[embedded image bytes]", text
            )
            return tuple(
                DocumentUnit(f"block:{index + 1}", block)
                for index, block in enumerate(re.split(r"\n\s*\n", text))
                if block.strip()
            )
        if format_name == "pdf":
            reader = open_pdf(content)
            return tuple(
                DocumentUnit(f"page:{index + 1}", page.extract_text() or "")
                for index, page in enumerate(reader.pages)
            )
        inspect_office(content, format_name, for_edit=True)
        if format_name == "docx":
            document = Document(io.BytesIO(content))
            units = []
            paragraph_index, table_index = 0, 0
            for index, block in enumerate(document.iter_inner_content()):
                if native_targets:
                    if isinstance(block, WordParagraph):
                        units.append(
                            DocumentUnit(f"paragraph:{paragraph_index}", block.text, editable=True)
                        )
                        paragraph_index += 1
                    else:
                        seen_cells = set()
                        for row_index, row in enumerate(block.rows):
                            for column_index, cell in enumerate(row.cells):
                                if cell._tc in seen_cells:
                                    continue  # A merged cell has one canonical native target.
                                seen_cells.add(cell._tc)
                                units.append(
                                    DocumentUnit(
                                        f"table:{table_index}:row:{row_index}:col:{column_index}",
                                        cell.text,
                                        editable=True,
                                    )
                                )
                        table_index += 1
                    continue
                text = (
                    block.text
                    if isinstance(block, WordParagraph)
                    else "\n".join(
                        " | ".join(cell.text for cell in row.cells) for row in block.rows
                    )
                )
                units.append(DocumentUnit(f"block:{index + 1}", text))
            for section_index, section in enumerate(document.sections):
                for label, story in (("header", section.header), ("footer", section.footer)):
                    text = "\n".join(
                        paragraph.text for paragraph in story.paragraphs if paragraph.text
                    )
                    if text:
                        units.append(DocumentUnit(f"section:{section_index + 1}:{label}", text))
            return tuple(units)
        if format_name == "pptx":
            presentation = Presentation(io.BytesIO(content))
            if not 1 <= len(presentation.slides) <= MAX_SLIDES:
                raise ArtifactError("page_limit", "PPTX must contain 1-100 slides")

            def shape_text(shape: Any) -> str:
                if shape.has_text_frame:
                    return str(shape.text)
                if shape.has_table:
                    return "\n".join(
                        " | ".join(cell.text for cell in row.cells) for row in shape.table.rows
                    )
                if shape.has_chart:
                    chart = shape.chart
                    categories = tuple(category.label for category in chart.plots[0].categories)
                    title = chart.chart_title.text_frame.text if chart.has_title else "Chart"
                    return (
                        title
                        + "\n"
                        + "\n".join(
                            series.name
                            + ": "
                            + ", ".join(
                                f"{label}={value:g}"
                                for label, value in zip(categories, series.values, strict=True)
                            )
                            for series in chart.series
                        )
                    )
                nodes = shape._element.xpath(".//p:cNvPr")
                return (
                    "Image description: " + nodes[0].get("descr", "")
                    if nodes and nodes[0].get("descr")
                    else ""
                )

            if native_targets:
                native_units = []
                for index, slide in enumerate(presentation.slides):
                    names = [shape.name for shape in slide.shapes]
                    for shape_index, shape in enumerate(slide.shapes):
                        unique = names.count(shape.name) == 1
                        native_units.append(
                            DocumentUnit(
                                f"slide:{index}:shape:{shape.name}"
                                if unique
                                else f"slide:{index}:shape_index:{shape_index}",
                                shape_text(shape),
                                editable=unique and bool(shape.has_text_frame),
                            )
                        )
                return tuple(native_units)
            return tuple(
                DocumentUnit(
                    f"slide:{index + 1}",
                    "\n".join(shape_text(shape) for shape in slide.shapes),
                )
                for index, slide in enumerate(presentation.slides)
            )
        raise ArtifactError("unsupported_format", "Unsupported document format")
    except ArtifactError:
        raise
    except Exception as exc:
        raise ArtifactError("invalid_document", f"Cannot parse {format_name} document") from exc


def edit_report_section(
    content: bytes, format_name: str, index: int, paragraphs: tuple[str, ...]
) -> bytes:
    if index < 0 or not paragraphs or len(paragraphs) > 50:
        raise ArtifactError(
            "invalid_edit", "A nonnegative section index and replacement text are required"
        )
    for text in paragraphs:
        _text(text, allow_empty=True)
    if format_name == "md":
        text = content.decode("utf-8")
        markdown_headings = list(re.finditer(r"^## [^\n]+$", text, re.MULTILINE))
        if index >= len(markdown_headings):
            raise ArtifactError("section_not_found", "Section index is outside the report")
        start = markdown_headings[index].end()
        end = (
            markdown_headings[index + 1].start()
            if index + 1 < len(markdown_headings)
            else len(text)
        )
        section = text[start:end]
        markdown_blocks = [block for block in re.split(r"\n\s*\n", section) if block.strip()]
        # Only simple paragraphs plus pipe tables are a declared editing capability.
        preserved = []
        for block in markdown_blocks:
            if block.lstrip().startswith("|") or re.fullmatch(
                r"!\[[^\n]*\]\(data:image/png;base64,[A-Za-z0-9+/=]+\)", block.strip()
            ):
                preserved.append(block)
            elif re.search(r"^(?:#{1,6} |```|~~~|>|[-*] |\d+\. )|!\[|<", block, re.MULTILINE):
                raise ArtifactError(
                    "unsupported_feature", "Section has unsupported Markdown structure"
                )
        if any(
            re.search(r"^(?:#{1,6} |```|~~~|>|[-*] |\d+\. |\|)|!\[|<", value, re.MULTILINE)
            for value in paragraphs
        ):
            raise ArtifactError("invalid_edit", "Replacement paragraphs must be plain text")
        replacement = "\n\n" + "\n\n".join((*paragraphs, *preserved)) + "\n\n"
        return (text[:start] + replacement + text[end:]).encode("utf-8")
    if format_name != "docx":
        raise ArtifactError("unsupported_edit", "Section editing supports Markdown and DOCX")
    inspect_office(content, format_name, for_edit=True)
    document = Document(io.BytesIO(content))
    blocks = list(document.iter_inner_content())
    headings = [
        i
        for i, block in enumerate(blocks)
        if isinstance(block, WordParagraph)
        and block.style is not None
        and block.style.name == "Heading 1"
    ]
    if index >= len(headings):
        raise ArtifactError("section_not_found", "Section index is outside the report")
    start = headings[index]
    end = headings[index + 1] if index + 1 < len(headings) else len(blocks)
    for block in blocks[start + 1 : end]:
        if isinstance(block, Table):
            continue
        if block._p.xpath(".//w:drawing") and not block.text.strip():
            continue
        if block.style is not None and block.style.name == "Caption":
            continue
        if block._p.xpath(".//w:drawing|.//w:pict|.//w:fldChar|.//w:hyperlink"):
            raise ArtifactError("unsupported_feature", "Edited paragraphs contain nontext content")
        if block.style is not None and block.style.name.startswith("Heading"):
            raise ArtifactError(
                "unsupported_feature", "Nested headings require a more precise edit"
            )
    for block in blocks[start + 1 : end]:
        if (
            isinstance(block, WordParagraph)
            and not block._p.xpath(".//w:drawing")
            and not (block.style is not None and block.style.name == "Caption")
        ):
            block._p.getparent().remove(block._p)
    heading = blocks[start]
    assert isinstance(heading, WordParagraph)
    previous = heading._p
    for text in paragraphs:
        new = OxmlElement("w:p")
        previous.addnext(new)
        paragraph = WordParagraph(new, document._body)
        paragraph.style = "Normal"
        paragraph.add_run(text)
        previous = new
    output = io.BytesIO()
    document.save(output)
    return output.getvalue()


def edit_deck_slide(
    content: bytes, index: int, *, title: str | None, bullets: tuple[str, ...] | None
) -> bytes:
    inspect_office(content, "pptx", for_edit=True)
    presentation = Presentation(io.BytesIO(content))
    if presentation.core_properties.keywords != DECK_MARKER:
        raise ArtifactError("unsupported_edit", "Slide editing currently supports generated decks")
    if not 0 <= index < len(presentation.slides):
        raise ArtifactError("slide_not_found", "Slide index is outside the deck")
    shapes = {shape.name: shape for shape in presentation.slides[index].shapes}
    if "groupagent.title" not in shapes or "groupagent.body" not in shapes:
        raise ArtifactError("unsupported_edit", "Expected slide text boxes are missing")
    if title is None and bullets is None:
        raise ArtifactError("invalid_edit", "Supply a title or replacement paragraphs")
    if title is not None:
        _text(title, maximum=90)
        _slide_text(shapes["groupagent.title"], (title,), 30, bold=True)
    if bullets is not None:
        if len(bullets) > (3 if "groupagent.table" in shapes else 6):
            raise ArtifactError("unsupported_layout", "Too many slide paragraphs")
        for bullet in bullets:
            _text(bullet, maximum=180)
        _slide_text(shapes["groupagent.body"], bullets, 22)
    output = io.BytesIO()
    presentation.save(output)
    return output.getvalue()
