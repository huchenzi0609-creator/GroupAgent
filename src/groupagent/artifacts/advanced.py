"""Declared template/point edits and PDF page operations; isolated-worker code."""

from __future__ import annotations

import io
import re
from typing import Any

from docx import Document
from docx.text.paragraph import Paragraph as WordParagraph
from pptx import Presentation
from pptx.chart.data import CategoryChartData
from pptx.enum.chart import XL_CHART_TYPE
from pypdf import PdfWriter
from pypdf.annotations import Highlight, Text
from pypdf.generic import ArrayObject, FloatObject

from .formats import inspect_office, open_pdf
from .models import ArtifactError, ChartData, ImageData
from .visuals import check_chart, validated_image


def _pdf_editable(reader: Any) -> None:
    if reader.trailer["/Root"].get("/AcroForm"):
        raise ArtifactError(
            "unsupported_feature",
            "Page edits of interactive or signed PDFs require a separate workflow",
        )


def embedded_image(content: bytes, format_name: str, image_index: int) -> tuple[bytes, str]:
    inspect_office(content, format_name)
    if image_index < 0:
        raise ArtifactError("invalid_target", "Image index must be nonnegative")
    if format_name == "docx":
        document = Document(io.BytesIO(content))
        try:
            shape = document.inline_shapes[image_index]
            blip = shape._inline.graphic.graphicData.pic.blipFill.blip
            part = document.part.related_parts[blip.embed]
            return part.blob, f"inline_image:{image_index + 1}"
        except (IndexError, AttributeError, KeyError):
            raise ArtifactError(
                "edit_target_missing", "Supported inline image does not exist"
            ) from None
    if format_name == "pptx":
        presentation = Presentation(io.BytesIO(content))
        pictures = [
            (index, shape)
            for index, slide in enumerate(presentation.slides)
            for shape in slide.shapes
            if hasattr(shape, "image")
        ]
        try:
            slide_index, picture = pictures[image_index]
            return picture.image.blob, f"slide:{slide_index + 1}:shape:{picture.name}"
        except IndexError:
            raise ArtifactError("edit_target_missing", "Picture does not exist") from None
    raise ArtifactError("unsupported_format", "Embedded image reads support DOCX/PPTX")


def replace_embedded_image(
    content: bytes, format_name: str, image_index: int, image: ImageData
) -> bytes:
    inspect_office(content, format_name, for_edit=True)
    pixels, width, height = validated_image(image)
    if image_index < 0:
        raise ArtifactError("invalid_target", "Image index must be nonnegative")
    document: Any
    if format_name == "docx":
        document = Document(io.BytesIO(content))
        try:
            picture = document.inline_shapes[image_index]
            relation, _ = document.part.get_or_add_image(io.BytesIO(pixels))
            picture._inline.graphic.graphicData.pic.blipFill.blip.embed = relation
            fitted_width = min(picture.width, int(picture.height * width / height))
            picture.width, picture.height = fitted_width, int(fitted_width * height / width)
            picture._inline.docPr.set("descr", image.alt_text)
        except (IndexError, AttributeError):
            raise ArtifactError(
                "edit_target_missing", "Inline image target does not exist"
            ) from None
    elif format_name == "pptx":
        document = Presentation(io.BytesIO(content))
        pictures = [
            (slide, shape)
            for slide in document.slides
            for shape in slide.shapes
            if hasattr(shape, "image")
        ]
        try:
            slide, picture = pictures[image_index]
            _, relation = slide.part.get_or_add_image_part(io.BytesIO(pixels))
            picture._element.xpath(".//a:blip")[0].set(
                "{http://schemas.openxmlformats.org/officeDocument/2006/relationships}embed",
                relation,
            )
            fitted_width = min(picture.width, int(picture.height * width / height))
            picture.width, picture.height = fitted_width, int(fitted_width * height / width)
            picture.crop_left = picture.crop_right = picture.crop_top = picture.crop_bottom = 0
            picture._element.xpath(".//p:cNvPr")[0].set("descr", image.alt_text)
        except (IndexError, AttributeError):
            raise ArtifactError("edit_target_missing", "Picture target does not exist") from None
    else:
        raise ArtifactError("unsupported_format", "Image replacement supports DOCX/PPTX")
    output = io.BytesIO()
    document.save(output)
    return output.getvalue()


def _replace_runs(paragraph: Any, old: str, new: str) -> int:
    """Replace literal text across runs while retaining all unaffected run formatting."""
    if not old or len(new) > 10000:
        raise ArtifactError("invalid_edit", "Replacement needs bounded literal text")
    text = "".join(run.text for run in paragraph.runs)
    positions = [match.start() for match in re.finditer(re.escape(old), text)]
    for start in reversed(positions):
        end = start + len(old)
        offset = 0
        inserted = False
        for run in paragraph.runs:
            before, after = offset, offset + len(run.text)
            offset = after
            if after <= start or before >= end:
                continue
            left, right = max(0, start - before), min(len(run.text), end - before)
            run.text = run.text[:left] + (new if not inserted else "") + run.text[right:]
            inserted = True
    return len(positions)


def _docx_paragraphs(document: Any):
    yield from document.paragraphs
    for table in document.tables:
        for row in table.rows:
            for cell in row.cells:
                yield from cell.paragraphs
    for section in document.sections:
        yield from section.header.paragraphs
        yield from section.footer.paragraphs


def fill_template(content: bytes, format_name: str, replacements: dict[str, str]) -> bytes:
    if (
        not replacements
        or len(replacements) > 100
        or any(not key or len(key) > 100 for key in replacements)
    ):
        raise ArtifactError("invalid_edit", "Template needs 1-100 named literal placeholders")
    if format_name == "md":
        text = content.decode("utf-8")
        fragments = re.split(r"(data:image/[a-z]+;base64,[A-Za-z0-9+/=]+)", text)
        for old, new in replacements.items():
            if not any(
                old in fragment for index, fragment in enumerate(fragments) if index % 2 == 0
            ):
                raise ArtifactError(
                    "template_target_missing", "Template placeholder does not exist"
                )
            fragments = [
                fragment.replace(old, new) if index % 2 == 0 else fragment
                for index, fragment in enumerate(fragments)
            ]
        return "".join(fragments).encode()
    if format_name not in {"docx", "pptx"}:
        raise ArtifactError("unsupported_template", "Template filling supports MD, DOCX and PPTX")
    inspect_office(content, format_name, for_edit=True)
    counts = {key: 0 for key in replacements}
    document: Any = (
        Document(io.BytesIO(content))
        if format_name == "docx"
        else Presentation(io.BytesIO(content))
    )
    if format_name == "docx":
        paragraphs = list(_docx_paragraphs(document))
    else:
        paragraphs = [
            paragraph
            for slide in document.slides
            for shape in slide.shapes
            for frame in (
                [shape.text_frame]
                if shape.has_text_frame
                else [cell.text_frame for row in shape.table.rows for cell in row.cells]
                if shape.has_table
                else []
            )
            for paragraph in frame.paragraphs
        ]
    for paragraph in paragraphs:
        for old, new in replacements.items():
            counts[old] += _replace_runs(paragraph, old, new)
    if any(count == 0 for count in counts.values()):
        raise ArtifactError(
            "template_target_missing", "A requested template placeholder was not found"
        )
    output = io.BytesIO()
    document.save(output)
    return output.getvalue()


def edit_text_target(
    content: bytes, format_name: str, target: str, expected_text: str, replacement: str
) -> bytes:
    if format_name not in {"docx", "pptx"}:
        raise ArtifactError(
            "unsupported_text_target_format",
            "Native text targets require DOCX/PPTX; Markdown uses section or template replacement",
        )
    inspect_office(content, format_name, for_edit=True)
    paragraphs = []
    document: Any
    if format_name == "docx":
        document = Document(io.BytesIO(content))
        paragraph_target = re.fullmatch(r"paragraph:(\d+)", target)
        cell_target = re.fullmatch(r"table:(\d+):row:(\d+):col:(\d+)", target)
        block_target = re.fullmatch(r"block:([1-9]\d*)", target)
        try:
            if paragraph_target:
                paragraphs = [document.paragraphs[int(paragraph_target[1])]]
            elif cell_target:
                table, row, column = map(int, cell_target.groups())
                native_table = document.tables[table]
                if row >= len(native_table.rows) or column >= len(native_table.columns):
                    raise IndexError
                paragraphs = list(native_table.cell(row, column).paragraphs)
            elif block_target:
                # Legacy read locators were one-based body blocks, not paragraph
                # indices: tables occupy a block too. Resolve the actual OOXML
                # order and retain the exactly-one expected-text match below.
                block = list(document.iter_inner_content())[int(block_target[1]) - 1]
                if isinstance(block, WordParagraph):
                    paragraphs = [block]
                else:
                    seen_cells = set()
                    for native_row in block.rows:
                        for cell in native_row.cells:
                            if cell._tc not in seen_cells:
                                seen_cells.add(cell._tc)
                                paragraphs.extend(cell.paragraphs)
            else:
                raise ArtifactError("invalid_target", "Unknown DOCX edit locator")
        except IndexError:
            raise ArtifactError("edit_target_missing", "DOCX edit target does not exist") from None
    elif format_name == "pptx":
        document = Presentation(io.BytesIO(content))
        shape_target = re.fullmatch(r"slide:(\d+):shape:(.+)", target)
        if not shape_target:
            raise ArtifactError(
                "invalid_target", "PPTX edits require an exact slide and shape name"
            )
        try:
            shapes = [
                shape
                for shape in document.slides[int(shape_target[1])].shapes
                if shape.name == shape_target[2]
            ]
        except IndexError:
            raise ArtifactError("edit_target_missing", "Slide does not exist") from None
        if len(shapes) != 1 or not shapes[0].has_text_frame:
            raise ArtifactError("ambiguous_target", "An exact unique text shape is required")
        paragraphs = list(shapes[0].text_frame.paragraphs)
    else:
        raise ArtifactError("unsupported_edit", "Point text editing supports DOCX and PPTX")
    found = sum(_replace_runs(paragraph, expected_text, replacement) for paragraph in paragraphs)
    if found != 1:
        raise ArtifactError(
            "edit_target_mismatch", "Point edit requires exactly one matching expected text"
        )
    output = io.BytesIO()
    document.save(output)
    return output.getvalue()


def replace_chart(content: bytes, slide_index: int, shape_name: str, data: ChartData) -> bytes:
    check_chart(data)
    inspect_office(content, "pptx", for_edit=True)
    document = Presentation(io.BytesIO(content))
    if not 0 <= slide_index < len(document.slides):
        raise ArtifactError("edit_target_missing", "Chart slide does not exist")
    shapes = [
        shape
        for shape in document.slides[slide_index].shapes
        if shape.name == shape_name and shape.has_chart
    ]
    if len(shapes) != 1:
        raise ArtifactError("ambiguous_target", "A unique native chart shape is required")
    chart_data = CategoryChartData()
    chart_data.categories = data.categories
    for series in data.series:
        chart_data.add_series(series.name, series.values)
    chart = shapes[0].chart
    expected_type = {
        "bar": XL_CHART_TYPE.COLUMN_CLUSTERED,
        "line": XL_CHART_TYPE.LINE,
        "pie": XL_CHART_TYPE.PIE,
    }[data.kind]
    if chart.chart_type != expected_type:
        raise ArtifactError(
            "unsupported_chart_type_change",
            "Point edits preserve chart kind; create a new chart to change its type",
        )
    chart.replace_data(chart_data)
    if chart.has_title:
        chart.chart_title.text_frame.text = data.title
    output = io.BytesIO()
    document.save(output)
    return output.getvalue()


def pdf_pages(
    content: bytes, indices: tuple[int, ...], rotations: dict[int, int] | None = None
) -> bytes:
    reader = open_pdf(content)
    _pdf_editable(reader)
    if not 1 <= len(indices) <= 200 or any(
        type(index) is not int or not 0 <= index < len(reader.pages) for index in indices
    ):
        raise ArtifactError("invalid_page_selection", "PDF page selection is outside its bounds")
    rotations = rotations or {}
    if any(
        index not in indices or degrees not in {0, 90, 180, 270}
        for index, degrees in rotations.items()
    ):
        raise ArtifactError(
            "invalid_rotation", "PDF rotations must identify selected pages and quarter turns"
        )
    writer = PdfWriter()
    for index in indices:
        page = writer.add_page(reader.pages[index])
        if rotations.get(index):
            page.rotate(rotations[index])
    writer.add_metadata({"/Subject": "groupagent-report-v1"})
    output = io.BytesIO()
    writer.write(output)
    return output.getvalue()


def combine_pdfs(contents: tuple[bytes, ...]) -> bytes:
    if not 1 <= len(contents) <= 20 or sum(map(len, contents)) > 100 * 1024 * 1024:
        raise ArtifactError("document_limit", "PDF composition exceeds the work input limit")
    writer = PdfWriter()
    for content in contents:
        reader = open_pdf(content)
        _pdf_editable(reader)
        if len(writer.pages) + len(reader.pages) > 200:
            raise ArtifactError("page_limit", "Composed PDF exceeds 200 pages")
        for page in reader.pages:
            writer.add_page(page)
    writer.add_metadata({"/Subject": "groupagent-report-v1"})
    output = io.BytesIO()
    writer.write(output)
    return output.getvalue()


def annotate_pdf(content: bytes, annotations: tuple[dict[str, Any], ...]) -> bytes:
    reader = open_pdf(content)
    _pdf_editable(reader)
    if not 1 <= len(annotations) <= 100:
        raise ArtifactError("annotation_limit", "A PDF edit supports 1-100 annotations")
    writer = PdfWriter()
    writer.clone_document_from_reader(reader)
    for item in annotations:
        if set(item) - {"page_index", "rect", "text", "kind"}:
            raise ArtifactError("invalid_annotation", "Unknown annotation property")
        page_index = item.get("page_index")
        if type(page_index) is not int or not 0 <= page_index < len(writer.pages):
            raise ArtifactError("invalid_annotation", "Annotation page is outside the document")
        rect, text, kind = item.get("rect"), item.get("text", ""), item.get("kind", "note")
        if (
            not isinstance(rect, (tuple, list))
            or len(rect) != 4
            or not isinstance(text, str)
            or len(text) > 500
        ):
            raise ArtifactError("invalid_annotation", "Annotation rectangle/text is invalid")
        page = writer.pages[page_index]
        if tuple(page.mediabox.lower_left) != (0, 0) or tuple(page.cropbox) != tuple(page.mediabox):
            raise ArtifactError(
                "unsupported_page_geometry",
                "Point annotations currently require an uncropped zero-origin page",
            )
        x0, y0, x1, y1 = map(float, rect)
        if not 0 <= x0 < x1 <= float(page.mediabox.width) or not 0 <= y0 < y1 <= float(
            page.mediabox.height
        ):
            raise ArtifactError("invalid_annotation", "Annotation lies outside the page")
        annotation: Any
        if kind == "highlight":
            annotation = Highlight(
                rect=(x0, y0, x1, y1),
                quad_points=ArrayObject(
                    [FloatObject(value) for value in (x0, y1, x1, y1, x0, y0, x1, y0)]
                ),
                highlight_color="ffdd55",
            )
        elif kind == "note":
            annotation = Text(rect=(x0, y0, x1, y1), text=text, open=False)
        else:
            raise ArtifactError(
                "unsupported_annotation", "Only highlight and text-note annotations are supported"
            )
        writer.add_annotation(page_number=page_index, annotation=annotation)
    writer.add_metadata({"/Subject": "groupagent-report-v1"})
    output = io.BytesIO()
    writer.write(output)
    return output.getvalue()
