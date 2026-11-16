"""Typed scoped document tools. No model-visible arbitrary filesystem paths."""

from __future__ import annotations

import json
import re
from collections.abc import Awaitable, Callable
from dataclasses import asdict
from typing import Any, ClassVar, Literal

from pydantic import Field, JsonValue, field_validator, model_validator

from groupagent.tools import IdempotencyMode, ToolContext, ToolData, ToolDefinition, ToolGateway
from groupagent.tools.models import ToolFailure

from .isolated import IsolatedDocumentService
from .models import ChartData, ChartSeries, Deck, ImageData, Report, Section, Slide, TableData
from .store import document_format


class ArtifactReference(ToolData):
    validation_hints: ClassVar[dict[str, str]] = {
        "file_id": "Use the exact file_id from a scoped input manifest or previous tool result, not a filename/work ID.",
        "version_id": "Use that artifact's exact version_id from the input manifest or previous tool result.",
    }
    file_id: str = Field(pattern=r"^[a-f0-9]{32}$")
    version_id: str = Field(pattern=r"^[a-f0-9]{32}$")


class TableInput(ToolData):
    headers: tuple[str, ...] = Field(
        min_length=1,
        max_length=6,
        description="Column names for row records. For key=value observations prefer name/value columns; records layout also canonicalizes a nonnumeric observation-name vector with exactly one numeric data row. Use columns layout only for an explicitly requested horizontal orientation.",
    )
    rows: tuple[tuple[str, ...], ...] = Field(
        max_length=50,
        description="Editable native table data, one record per row. Put each named scalar observation and its unchanged value in the same row; do not transpose observation names into column headers unless explicitly requested.",
    )
    layout: Literal["records", "columns"] = Field(
        default="records",
        description="Default records: named scalar observations are rows. A nonempty nonnumeric header vector with one entirely numeric data row is normalized to 项目/数值 records, preserving original strings and order. Use columns only when the user explicitly requests a horizontal/transposed table; it preserves the supplied layout.",
    )

    @model_validator(mode="after")
    def rectangular(self):
        if any(len(row) != len(self.headers) for row in self.rows):
            raise ValueError("Each table row must match the header count")
        return self


class SeriesInput(ToolData):
    name: str = Field(min_length=1, max_length=60)
    values: tuple[float, ...] = Field(min_length=1, max_length=20)


class ChartInput(ToolData):
    kind: Literal["bar", "line", "pie"]
    title: str = Field(min_length=1, max_length=120)
    categories: tuple[str, ...] = Field(min_length=1, max_length=20)
    series: tuple[SeriesInput, ...] = Field(min_length=1, max_length=4)


class ImageInput(ArtifactReference):
    alt_text: str = Field(
        min_length=1,
        max_length=500,
        description="Visible caption for the actual embedded image. Supply file_id and version_id from chart_render, a scoped image input or another image-producing tool; a caption alone does not embed pixels.",
    )
    width_inches: float = Field(
        default=5.5,
        ge=1,
        le=6.5,
        description="Image width in inches; aspect ratio is preserved. Use a smaller width when several figures must fit a page limit.",
    )


class SectionInput(ToolData):
    title: str = Field(min_length=1, max_length=180)
    paragraphs: tuple[str, ...] = Field(
        max_length=50,
        description="Plain text paragraphs; empty strings intentionally reserve blank form-writing space. For .md, do not put fenced code, headings or pipe tables here; write complete Markdown with workspace_write_text then artifact_from_workspace instead.",
    )
    tables: tuple[TableInput, ...] = Field(default=(), max_length=5)
    images: tuple[ImageInput, ...] = Field(
        default=(),
        max_length=3,
        description="Embed real scoped image bytes here: sections[].images=[{file_id,version_id,alt_text,width_inches}]. Copy exact artifact IDs from chart_render or image input results. Images are nested in a section, never a top-level document field.",
    )
    charts: tuple[ChartInput, ...] = Field(
        default=(),
        max_length=3,
        description="Render and embed bar/line/pie chart data directly in this section; no external plotting package or illustration service is needed. Use images for existing image artifacts.",
    )


class SlideInput(ToolData):
    title: str = Field(min_length=1, max_length=90)
    bullets: tuple[str, ...] = Field(max_length=6)
    table: TableInput | None = None
    image: ImageInput | None = Field(
        default=None,
        description="Embed the actual scoped image using its exact file_id/version_id, with alt_text and optional width_inches. For reports use sections[].images instead.",
    )
    chart: ChartInput | None = None


class DocumentCreateInput(ToolData):
    validation_hints: ClassVar[dict[str, str]] = {
        "name": "name is a filename with extension: report.md/report.docx/report.pdf or slides.pptx, never a path or extensionless title.",
        "kind": "kind=report uses sections and .md/.docx/.pdf; kind=deck uses slides and .pptx.",
        "images": "Embed images inside the document: report uses sections[].images=[{file_id,version_id,alt_text,width_inches}]; deck uses slides[].image={file_id,version_id,alt_text,width_inches}. Copy the exact file_id/version_id returned by chart_render or a scoped image input. Top-level images is unsupported; move the existing references into a section instead of dropping the figures. For new bar/line/pie charts you can also supply sections[].charts directly.",
        "value_error": "Match name/kind and sections/slides: report requires .md/.docx/.pdf plus nonempty sections; deck requires .pptx plus nonempty slides. Every table row must match its headers.",
        "*": 'Minimal report: {"name":"report.md","kind":"report","title":"Summary","sections":[{"title":"Findings","paragraphs":["Result 87."]}]}',
    }
    name: str = Field(
        min_length=1,
        max_length=240,
        description="Output basename including extension. report: .md/.docx/.pdf; deck: .pptx. No directories. At most 240 UTF-8 bytes.",
        examples=["course-report.docx", "course-slides.pptx"],
    )
    kind: Literal["report", "deck"]
    title: str = Field(min_length=1, max_length=180)
    subtitle: str = Field(default="", max_length=500)
    sections: tuple[SectionInput, ...] = Field(
        default=(),
        max_length=50,
        description="Report sections contain plain paragraphs, tables, images and charts. To include generated PNGs, put their exact file_id/version_id references in each section's images array.",
    )
    slides: tuple[SlideInput, ...] = Field(default=(), max_length=100)

    @field_validator("name")
    @classmethod
    def supported_name(cls, value: str) -> str:
        document_format(value)
        return value

    @model_validator(mode="after")
    def matched_kind(self):
        if (document_format(self.name) == "pptx") != (self.kind == "deck"):
            raise ValueError("Report/deck kind must match the filename extension")
        if self.kind == "report" and (not self.sections or self.slides):
            raise ValueError("Report requires sections only")
        if self.kind == "deck" and (not self.slides or self.sections):
            raise ValueError("Deck requires slides only")
        return self


class DocumentReadInput(ArtifactReference):
    start_unit: int = Field(default=0, ge=0)
    max_units: int = Field(default=20, ge=1, le=200)


class DocumentSearchInput(ArtifactReference):
    query: str = Field(min_length=1, max_length=200)


class DocumentEditInput(ToolData):
    validation_hints: ClassVar[dict[str, str]] = {
        "*": "Use exact file_id/expected_version_id. section needs section_index; slide needs slide_index plus title/bullets; text needs target/expected_text/replacement; template needs replacements; chart needs slide_index/shape_name/chart; PDF edits need page_indices or annotations."
    }
    file_id: str
    expected_version_id: str
    operation: Literal["section", "slide", "text", "template", "chart", "pdf_pages", "pdf_annotate"]
    section_index: int | None = Field(default=None, ge=0)
    paragraphs: tuple[str, ...] = ()
    slide_index: int | None = Field(default=None, ge=0)
    title: str | None = None
    bullets: tuple[str, ...] | None = None
    target: str | None = Field(
        default=None,
        description="Copy an editable=true unit's locator from document_read/search. Native DOCX paragraph/table/row/col and PPTX slide indexes are zero-based; PPTX also needs the exact unique shape name. Do not use a heading as a locator.",
    )
    expected_text: str | None = None
    replacement: str | None = None
    replacements: dict[str, str] = Field(default_factory=dict, max_length=100)
    shape_name: str | None = None
    chart: ChartInput | None = None
    page_indices: tuple[int, ...] = Field(default=(), max_length=200)
    rotations: dict[int, int] = Field(default_factory=dict)
    annotations: tuple[dict[str, JsonValue], ...] = Field(default=(), max_length=100)

    @model_validator(mode="after")
    def operation_fields(self):
        ready = {
            "section": self.section_index is not None,
            "slide": self.slide_index is not None
            and (self.title is not None or self.bullets is not None),
            "text": all(
                value is not None for value in (self.target, self.expected_text, self.replacement)
            ),
            "template": bool(self.replacements) and all(self.replacements),
            "chart": self.slide_index is not None
            and bool(self.shape_name)
            and self.chart is not None,
            "pdf_pages": bool(self.page_indices),
            "pdf_annotate": bool(self.annotations),
        }
        if not ready[self.operation]:
            raise ValueError("The selected operation is missing its explicit target/content fields")
        return self


class DocumentValidateInput(ArtifactReference):
    expected_text: tuple[str, ...] = Field(
        min_length=1,
        max_length=100,
        description="Literal text snippets that must occur in the actual document (whitespace ignored), not semantic claims. Native table cells are separate: use ['Alpha','12'], not an invented contiguous 'Alpha=12'. Missing literals are returned with zero-based index and exact text.",
    )

    @field_validator("expected_text")
    @classmethod
    def literal_snippets(cls, values: tuple[str, ...]) -> tuple[str, ...]:
        if any(not value.strip() or len(value) > 1000 for value in values):
            raise ValueError("Expected literals must be nonempty and at most 1000 characters")
        return values


class DocumentCombineInput(ToolData):
    validation_hints: ClassVar[dict[str, str]] = {
        "name": "PDF composition output name must be a .pdf basename, for example combined.pdf."
    }
    name: str = Field(min_length=1, max_length=240)
    inputs: tuple[ArtifactReference, ...] = Field(min_length=1, max_length=20)

    @field_validator("name")
    @classmethod
    def pdf_name(cls, value: str) -> str:
        if document_format(value) != "pdf":
            raise ValueError("Combined output must be a PDF")
        return value


class DocumentImageInput(ArtifactReference):
    image_index: int = Field(ge=0, le=99)


class ChartRenderInput(ToolData):
    validation_hints: ClassVar[dict[str, str]] = {
        "name": "chart_render outputs PNG; use a basename such as chart.png."
    }
    name: str = Field(default="chart.png", min_length=1, max_length=240)
    chart: ChartInput

    @field_validator("name")
    @classmethod
    def png_name(cls, value: str) -> str:
        if document_format(value, media=True) != "png":
            raise ValueError("Chart output must be PNG")
        return value


class ArtifactOutput(ToolData):
    file_id: str
    version_id: str
    name: str
    format: str
    sha256: str
    size_bytes: int
    validation_required: bool = True


class DocumentDataOutput(ToolData):
    data: dict[str, JsonValue]
    untrusted_content: Literal[True] = True

    @classmethod
    def from_data(cls, data: dict[str, Any]) -> DocumentDataOutput:
        return cls.model_validate_json(
            json.dumps({"data": data}, ensure_ascii=False, allow_nan=False)
        )


def register_document_tools(
    gateway: ToolGateway,
    service: IsolatedDocumentService,
    *,
    provenance_factory: Callable[[ToolContext], Awaitable[dict[str, Any]]] | None = None,
) -> None:
    def chart(value: ChartInput) -> ChartData:
        return ChartData(
            value.kind,
            value.title,
            value.categories,
            tuple(ChartSeries(series.name, series.values) for series in value.series),
        )

    def table(value: TableInput) -> TableData:
        numeric = re.compile(r"[+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][+-]?[0-9]+)?")
        if (
            value.layout == "records"
            and len(value.headers) > 1
            and len(value.rows) == 1
            and all(
                header.strip() and not numeric.fullmatch(header.strip()) for header in value.headers
            )
            and all(numeric.fullmatch(cell.strip()) for cell in value.rows[0])
        ):
            return TableData(
                ("项目", "数值"), tuple(zip(value.headers, value.rows[0], strict=True))
            )
        return TableData(value.headers, value.rows)

    def output(version) -> ArtifactOutput:
        return ArtifactOutput(
            **{
                key: getattr(version, key)
                for key in ArtifactOutput.model_fields
                if key != "validation_required"
            }
        )

    async def generate(arguments: DocumentCreateInput, context: ToolContext) -> ArtifactOutput:
        scope = await service._scope(context)
        document: Report | Deck
        image_sources: list[dict[str, Any]] = []
        image_events: set[str] = set()

        def picture(value: ImageInput, location: str) -> ImageData:
            version = service._get(scope, value.file_id, value.version_id)
            pixels = service._read(scope, version.file_id, version.version_id)
            image_sources.append(
                {
                    "location": location,
                    "file_id": version.file_id,
                    "version_id": version.version_id,
                    "sha256": version.sha256,
                }
            )
            image_events.update(service.store._source_events(version.provenance))
            return ImageData(
                pixels,
                value.alt_text,
                value.width_inches,
            )

        if arguments.kind == "report":
            document = Report(
                arguments.title,
                tuple(
                    Section(
                        section.title,
                        section.paragraphs,
                        tuple(table(value) for value in section.tables),
                        tuple(
                            picture(value, f"sections[{section_index}].images[{image_index}]")
                            for image_index, value in enumerate(section.images)
                        ),
                        tuple(chart(value) for value in section.charts),
                    )
                    for section_index, section in enumerate(arguments.sections)
                ),
                arguments.subtitle,
            )
        else:
            document = Deck(
                arguments.title,
                tuple(
                    Slide(
                        slide.title,
                        slide.bullets,
                        table(slide.table) if slide.table else None,
                        picture(slide.image, f"slides[{slide_index}].image")
                        if slide.image
                        else None,
                        chart(slide.chart) if slide.chart else None,
                    )
                    for slide_index, slide in enumerate(arguments.slides)
                ),
            )
        provenance = (
            await provenance_factory(context)
            if provenance_factory
            else {"work_id": context.work_id}
        )
        if image_sources:
            # Bind each placement to the exact scoped version whose bytes were
            # read. Retain source-event dependencies so deletion/revocation also
            # applies to this derived document, without copying image metadata.
            provenance = {
                **provenance,
                "source_image_refs": image_sources,
                "input_versions": list(
                    dict.fromkeys(
                        [
                            *provenance.get("input_versions", ()),
                            *(source["version_id"] for source in image_sources),
                        ]
                    )
                ),
                "source_event_ids": sorted(
                    image_events | set(service.store._source_events(provenance))
                ),
            }
        return output(
            await service.generate(context, arguments.name, document, provenance=provenance)
        )

    async def read(arguments: DocumentReadInput, context: ToolContext) -> DocumentDataOutput:
        result = await service.read(
            context,
            arguments.file_id,
            arguments.version_id,
            start_unit=arguments.start_unit,
            max_units=arguments.max_units,
        )
        return DocumentDataOutput.from_data(asdict(result))

    async def search(arguments: DocumentSearchInput, context: ToolContext) -> DocumentDataOutput:
        return DocumentDataOutput.from_data(
            await service.search(context, arguments.file_id, arguments.query, arguments.version_id)
        )

    async def edit(arguments: DocumentEditInput, context: ToolContext) -> ArtifactOutput:
        a = arguments
        kwargs = {"expected_version_id": a.expected_version_id}
        if a.operation == "section" and a.section_index is not None:
            version = await service.edit_section(
                context, a.file_id, **kwargs, section_index=a.section_index, paragraphs=a.paragraphs
            )
        elif a.operation == "slide" and a.slide_index is not None:
            version = await service.edit_slide(
                context,
                a.file_id,
                **kwargs,
                slide_index=a.slide_index,
                title=a.title,
                bullets=a.bullets,
            )
        elif (
            a.operation == "text"
            and a.target is not None
            and a.expected_text is not None
            and a.replacement is not None
        ):
            version = await service.edit_target(
                context,
                a.file_id,
                **kwargs,
                target=a.target,
                expected_text=a.expected_text,
                replacement=a.replacement,
            )
        elif a.operation == "template" and a.replacements:
            version = await service.fill_template(
                context, a.file_id, **kwargs, replacements=a.replacements
            )
        elif a.operation == "chart" and a.slide_index is not None and a.shape_name and a.chart:
            version = await service.edit_chart(
                context,
                a.file_id,
                **kwargs,
                slide_index=a.slide_index,
                shape_name=a.shape_name,
                chart=chart(a.chart),
            )
        elif a.operation == "pdf_pages" and a.page_indices:
            version = await service.select_pdf_pages(
                context, a.file_id, **kwargs, page_indices=a.page_indices, rotations=a.rotations
            )
        elif a.operation == "pdf_annotate" and a.annotations:
            version = await service.annotate_pdf(
                context, a.file_id, **kwargs, annotations=a.annotations
            )
        else:
            raise ToolFailure(
                "invalid_input",
                "Fields operation/target/content are incomplete; follow document_edit schema. No artifact version was published.",
                no_effect=True,
            )
        return output(version)

    async def validate(
        arguments: DocumentValidateInput, context: ToolContext
    ) -> DocumentDataOutput:
        result = await service.validate(
            context, arguments.file_id, arguments.version_id, expected_text=arguments.expected_text
        )
        data = DocumentDataOutput.from_data(result.to_dict())
        if not result.passed:
            raise ToolFailure(
                "document_validation_failed",
                result.failure_summary,
                output=data,
                no_effect=True,
            )
        return data

    async def combine(arguments: DocumentCombineInput, context: ToolContext) -> ArtifactOutput:
        return output(
            await service.combine_pdfs(
                context,
                arguments.name,
                tuple((item.file_id, item.version_id) for item in arguments.inputs),
            )
        )

    async def image(arguments: DocumentImageInput, context: ToolContext) -> ArtifactOutput:
        return output(
            await service.extract_image(
                context, arguments.file_id, arguments.version_id, image_index=arguments.image_index
            )
        ).model_copy(update={"validation_required": False})

    async def draw_chart(arguments: ChartRenderInput, context: ToolContext) -> ArtifactOutput:
        provenance = (
            await provenance_factory(context)
            if provenance_factory
            else {"work_id": context.work_id}
        )
        return output(
            await service.render_chart(
                context, arguments.name, chart(arguments.chart), provenance=provenance
            )
        ).model_copy(update={"validation_required": False})

    definitions = (
        (
            "chart_render",
            "Render reproducible bar/line/pie data into a real scoped PNG image without an illustration provider.",
            ChartRenderInput,
            ArtifactOutput,
            draw_chart,
            False,
        ),
        (
            "document_generate",
            'Create an actual scoped document. name includes its extension: kind=report uses sections with .md/.docx/.pdf; kind=deck uses slides with .pptx. Reports support plain paragraphs, native tables, images and data charts. PDF uses managed CJK fonts and verified report layout. Embed existing image bytes with sections[].images, or slides[].image for decks; copy exact file_id/version_id from chart_render or scoped image results. Report section example (replace the reference placeholders): {"title":"Figures","paragraphs":[],"images":[{"file_id":"<returned file_id>","version_id":"<returned version_id>","alt_text":"Figure 1","width_inches":4.5}]}. To render new bar/line/pie charts directly, use sections[].charts. Use document_validate to verify actual content and layout before delivery.',
            DocumentCreateInput,
            ArtifactOutput,
            generate,
            False,
        ),
        (
            "document_read",
            "Read a bounded range of visible text units plus metadata. PPTX data.metadata.title comes from actual file core properties and metadata.native_table_count is the string count of actual native Office tables, parsed in the controlled worker. Unit editable only means this tool supports a text-edit locator; it does not determine Office native editability. An aggregate table unit may have editable=false while native_table_count is positive. Units marked editable=true have exact text-edit locators: DOCX paragraph:N or table:N:row:N:col:N; PPTX slide:N:shape:NAME (all native indexes zero-based). Read units are not page counts. Other units are read-only by this API. Scanned PDF pages use OCR with page numbers. Image descriptions are metadata; extract pixels for visual analysis.",
            DocumentReadInput,
            DocumentDataOutput,
            read,
            True,
        ),
        (
            "document_search",
            "Search scoped document contents. Editable DOCX/PPTX results return the exact native text-edit locator with editable=true, matching document_read. This flag describes this API's text-edit support, not native Office object editability. Use document_read metadata.native_table_count for actual native tables. Other results are read-only by this API. Results report truncation.",
            DocumentSearchInput,
            DocumentDataOutput,
            search,
            True,
        ),
        (
            "document_edit",
            "Create an immutable new version using an exact expected version and declared section/slide/template/chart/PDF operation. For operation=text copy an editable=true locator from read/search and provide the exact existing substring as expected_text; only one occurrence may match. Native paragraph/table/row/col/slide indexes are zero-based.",
            DocumentEditInput,
            ArtifactOutput,
            edit,
            False,
        ),
        (
            "document_validate",
            "Actually render and verify required literal text and layout. expected_text is literal, not a semantic/paraphrase assertion or joined table cells. On failure read failed_expectations/failure_summary to repair the specific missing item. Structure alone is not validated.",
            DocumentValidateInput,
            DocumentDataOutput,
            validate,
            True,
        ),
        (
            "pdf_combine",
            "Combine scoped PDFs into a new real PDF, preserving the original versions.",
            DocumentCombineInput,
            ArtifactOutput,
            combine,
            False,
        ),
        (
            "document_extract_image",
            "Extract actual inline DOCX/PPTX picture pixels into a scoped image artifact.",
            DocumentImageInput,
            ArtifactOutput,
            image,
            False,
        ),
    )
    for name, description, input_model, output_model, handler, readonly in definitions:
        gateway.register(
            ToolDefinition(
                name=name,
                description=description,
                input_model=input_model,
                output_model=output_model,
                handler=handler,
                risk_class="read_only" if readonly else "local_draft",
                resource_scopes=("group", "work", "artifacts"),
                timeout_seconds=180,
                max_input_bytes=256000,
                max_output_bytes=512000,
                max_artifact_bytes=30 * 1024 * 1024,
                cost_meter="no_provider_charge",
                idempotency_mode=IdempotencyMode.READ_ONLY
                if readonly
                else IdempotencyMode.RECONCILE_BEFORE_RETRY,
            )
        )
