"""Document operation dispatch used only inside the isolated worker process."""

from __future__ import annotations

import io
import tempfile
from dataclasses import asdict
from pathlib import Path
from typing import Any

from groupagent.security import redact_credentials

from .advanced import (
    annotate_pdf,
    combine_pdfs,
    edit_text_target,
    embedded_image,
    fill_template,
    pdf_pages,
    replace_chart,
    replace_embedded_image,
)
from .documents import DocumentService
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
    ChartData,
    ChartSeries,
    Deck,
    ImageData,
    Report,
    Section,
    Slide,
    TableData,
)
from .store import ArtifactStore


def _table(value: dict[str, Any]) -> TableData:
    return TableData(tuple(value["headers"]), tuple(tuple(row) for row in value["rows"]))


def _chart(value: dict[str, Any]) -> ChartData:
    return ChartData(
        value["kind"],
        value["title"],
        tuple(value["categories"]),
        tuple(ChartSeries(series["name"], tuple(series["values"])) for series in value["series"]),
    )


def unpack_document(value: dict[str, Any], read_image) -> Report | Deck:
    def image(data: dict[str, Any]) -> ImageData:
        return ImageData(
            read_image(data["input_index"]), data["alt_text"], data.get("width_inches", 5.5)
        )

    if value["kind"] == "report":
        return Report(
            value["title"],
            tuple(
                Section(
                    section["title"],
                    tuple(section["paragraphs"]),
                    tuple(_table(table) for table in section.get("tables", ())),
                    tuple(image(data) for data in section.get("images", ())),
                    tuple(_chart(chart) for chart in section.get("charts", ())),
                )
                for section in value["sections"]
            ),
            value.get("subtitle", ""),
        )
    if value["kind"] == "deck":
        return Deck(
            value["title"],
            tuple(
                Slide(
                    slide["title"],
                    tuple(slide["bullets"]),
                    _table(slide["table"]) if slide.get("table") else None,
                    image(slide["image"]) if slide.get("image") else None,
                    _chart(slide["chart"]) if slide.get("chart") else None,
                )
                for slide in value["slides"]
            ),
        )
    raise ArtifactError("invalid_document", "Unknown document kind")


def pack_document(document: Report | Deck) -> tuple[dict[str, Any], tuple[bytes, ...]]:
    inputs: list[bytes] = []

    def image(data: ImageData) -> dict[str, Any]:
        index = len(inputs)
        inputs.append(data.content)
        return {"input_index": index, "alt_text": data.alt_text, "width_inches": data.width_inches}

    if isinstance(document, Report):
        value = {
            "kind": "report",
            "title": document.title,
            "subtitle": document.subtitle,
            "sections": [
                {
                    "title": section.title,
                    "paragraphs": section.paragraphs,
                    "tables": [asdict(table) for table in section.tables],
                    "images": [image(data) for data in section.images],
                    "charts": [asdict(chart) for chart in section.charts],
                }
                for section in document.sections
            ],
        }
    else:
        value = {
            "kind": "deck",
            "title": document.title,
            "slides": [
                {
                    "title": slide.title,
                    "bullets": slide.bullets,
                    "table": asdict(slide.table) if slide.table else None,
                    "image": image(slide.image) if slide.image else None,
                    "chart": asdict(slide.chart) if slide.chart else None,
                }
                for slide in document.slides
            ],
        }
    if len(inputs) > 20 or sum(map(len, inputs)) > 100 * 1024 * 1024:
        raise ArtifactError("document_limit", "Document images exceed the work input limit")
    return value, tuple(inputs)


def document_operation(
    operation: str, request: dict[str, Any], read_input, local_path
) -> dict[str, Any]:
    params, outputs = request.get("parameters", {}), request["outputs"]
    format_name = params.get("format", "pdf")
    if format_name not in {"md", "docx", "pdf", "pptx"}:
        raise ArtifactError("unsupported_format", "Document format is unsupported")
    candidate = Path("/usr/share/fonts/truetype/wqy/wqy-zenhei.ttc")
    font_path = candidate if candidate.is_file() else None
    font_family = "WenQuanYi Zen Hei" if font_path else "DejaVu Sans"
    if operation == "document_generate":
        document = unpack_document(params["document"], lambda index: read_input(request, index))
        if isinstance(document, Deck) and format_name == "pptx":
            content = build_pptx(document, font_family=font_family)
        elif isinstance(document, Report) and format_name in {"md", "docx", "pdf"}:
            content = (
                build_markdown(document)
                if format_name == "md"
                else build_docx(document, font_family=font_family)
                if format_name == "docx"
                else build_pdf(document, font_path=font_path)
            )
        else:
            raise ArtifactError("invalid_document", "Document model does not match its format")
    elif operation == "document_inspect":
        return {"unit_count": len(extract(read_input(request), format_name))}
    elif operation == "document_search":
        query = params.get("query", "")
        if not isinstance(query, str) or not query.strip() or len(query) > 200:
            raise ArtifactError("invalid_query", "Document search needs a bounded query")
        if format_name == "pdf":
            from groupagent.media.validation import ocr_pdf

            recognized = ocr_pdf(read_input(request))
            search_units = [
                {"locator": f"page:{page.page_number}", "text": page.text}
                for page in recognized.pages
            ]
        else:
            search_units = [
                asdict(unit)
                for unit in extract(read_input(request), format_name, native_targets=True)
            ]
        matches = [unit for unit in search_units if query.casefold() in unit["text"].casefold()]
        return {"units": matches[:20], "match_count": len(matches), "complete": len(matches) <= 20}
    elif operation == "document_read":
        start, count = params.get("start_unit", 0), params.get("max_units", 200)
        if type(start) is not int or type(count) is not int or start < 0 or not 1 <= count <= 200:
            raise ArtifactError("invalid_range", "Document read range is invalid")
        if format_name == "pdf":
            from groupagent.media.validation import ocr_pdf

            from .formats import open_pdf

            content = read_input(request)
            total = len(open_pdf(content).pages)
            indices = tuple(range(start, min(start + count, total)))
            result = ocr_pdf(content, page_indices=indices)
            return {
                "units": [
                    {"locator": f"page:{page.page_number}", "text": page.text}
                    for page in result.pages
                ],
                "total_units": total,
                "complete": start == 0 and count >= total,
                "ocr_pages": [
                    page.page_number for page in result.pages if page.method == "tesseract"
                ],
                "metadata": {},
            }
        content = read_input(request)
        units = extract(content, format_name, native_targets=True)
        metadata = {}
        if format_name == "pptx":
            # Only the controlled document worker parses the actual Office
            # bytes. Never infer a title from filename, host params or prose.
            from pptx import Presentation

            title = Presentation(io.BytesIO(content)).core_properties.title
            if not isinstance(title, str) or len(title) > 1000:
                raise ArtifactError("document_limit", "PPTX metadata title exceeds its read limit")
            metadata["title"] = redact_credentials(title)
        return {
            "units": [asdict(unit) for unit in units[start : start + count]],
            "total_units": len(units),
            "complete": start == 0 and count >= len(units),
            "ocr_pages": [],
            "metadata": metadata,
        }
    elif operation == "document_edit_section":
        content = edit_report_section(
            read_input(request), format_name, params["section_index"], tuple(params["paragraphs"])
        )
    elif operation == "document_edit_slide":
        content = edit_deck_slide(
            read_input(request),
            params["slide_index"],
            title=params.get("title"),
            bullets=tuple(params["bullets"]) if params.get("bullets") is not None else None,
        )
    elif operation == "document_template":
        content = fill_template(read_input(request), format_name, params["replacements"])
    elif operation == "document_edit_target":
        content = edit_text_target(
            read_input(request),
            format_name,
            params["target"],
            params["expected_text"],
            params["replacement"],
        )
    elif operation == "document_chart":
        content = replace_chart(
            read_input(request),
            params["slide_index"],
            params["shape_name"],
            _chart(params["chart"]),
        )
    elif operation == "document_replace_image":
        content = replace_embedded_image(
            read_input(request),
            format_name,
            params["image_index"],
            ImageData(read_input(request, 1), params["alt_text"]),
        )
    elif operation == "document_extract_image":
        from groupagent.media.validation import inspect_image

        pixels, locator = embedded_image(read_input(request), format_name, params["image_index"])
        info, preview = inspect_image(pixels)
        local_path(outputs[0]).write_bytes(preview)
        return {**info.model_dump(mode="json"), "locator": locator}
    elif operation == "document_pdf_pages":
        content = pdf_pages(
            read_input(request),
            tuple(params["page_indices"]),
            {int(key): value for key, value in params.get("rotations", {}).items()},
        )
    elif operation == "document_pdf_combine":
        content = combine_pdfs(
            tuple(read_input(request, index) for index in range(len(request["inputs"])))
        )
    elif operation == "document_pdf_annotate":
        content = annotate_pdf(read_input(request), tuple(params["annotations"]))
    elif operation == "document_validate":
        with tempfile.TemporaryDirectory(prefix="ga-document-validation-") as directory:
            with ArtifactStore(Path(directory) / "store") as store:
                scope = ArtifactScope("worker", "worker", "worker")
                version = store.create(scope, "input." + format_name, read_input(request))
                service = DocumentService(
                    store, office_font_family=font_family, pdf_font_path=font_path
                )
                validation = service.validate(
                    scope,
                    version.file_id,
                    expected_text=tuple(params.get("expected_text", ())),
                    render=params.get("render", False),
                    ocr_fallback=True,
                )
                for index in range(len(validation.preview_sha256)):
                    if index >= len(outputs):
                        raise ArtifactError(
                            "page_limit", "Rendered preview count exceeds requested slots"
                        )
                    local_path(outputs[index]).write_bytes(
                        store.read_preview(
                            scope,
                            version.file_id,
                            version.version_id,
                            validation.validation_id,
                            page_index=index,
                        )
                    )
                return validation.to_dict()
    else:
        raise ArtifactError("unsupported_worker_operation", "Unknown document operation")
    if len(content) > (2 if format_name == "md" else 30) * 1024 * 1024:
        raise ArtifactError("file_too_large", "Generated document exceeds the product limit")
    units = extract(content, format_name)
    local_path(outputs[0]).write_bytes(content)
    return {"format": format_name, "unit_count": len(units)}
