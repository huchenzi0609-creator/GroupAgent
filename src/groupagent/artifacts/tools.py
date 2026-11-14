"""Typed scoped document tools. No model-visible arbitrary filesystem paths."""

from __future__ import annotations

import json
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
    headers: tuple[str, ...] = Field(min_length=1, max_length=6)
    rows: tuple[tuple[str, ...], ...] = Field(max_length=50)

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
    alt_text: str = Field(min_length=1, max_length=500)
    width_inches: float = Field(default=5.5, ge=1, le=6.5)


class SectionInput(ToolData):
    title: str = Field(min_length=1, max_length=180)
    paragraphs: tuple[str, ...] = Field(
        max_length=50,
        description="Plain text paragraphs; empty strings intentionally reserve blank form-writing space. For .md, do not put fenced code, headings or pipe tables here; write complete Markdown with workspace_write_text then artifact_from_workspace instead.",
    )
    tables: tuple[TableInput, ...] = Field(default=(), max_length=5)
    images: tuple[ImageInput, ...] = Field(default=(), max_length=3)
    charts: tuple[ChartInput, ...] = Field(default=(), max_length=3)


class SlideInput(ToolData):
    title: str = Field(min_length=1, max_length=90)
    bullets: tuple[str, ...] = Field(max_length=6)
    table: TableInput | None = None
    image: ImageInput | None = None
    chart: ChartInput | None = None


class DocumentCreateInput(ToolData):
    validation_hints: ClassVar[dict[str, str]] = {
        "name": "name is a filename with extension: report.md/report.docx/report.pdf or slides.pptx, never a path or extensionless title.",
        "kind": "kind=report uses sections and .md/.docx/.pdf; kind=deck uses slides and .pptx.",
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
    sections: tuple[SectionInput, ...] = Field(default=(), max_length=50)
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

        def picture(value: ImageInput) -> ImageData:
            return ImageData(
                service._read(scope, value.file_id, value.version_id),
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
                        tuple(picture(value) for value in section.images),
                        tuple(chart(value) for value in section.charts),
                    )
                    for section in arguments.sections
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
                        picture(slide.image) if slide.image else None,
                        chart(slide.chart) if slide.chart else None,
                    )
                    for slide in arguments.slides
                ),
            )
        provenance = (
            await provenance_factory(context)
            if provenance_factory
            else {"work_id": context.work_id}
        )
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
        return DocumentDataOutput.from_data(result.to_dict())

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
            'Create a real scoped document; name MUST include its extension. Minimal report: {"name":"report.md","kind":"report","title":"Summary","sections":[{"title":"Findings","paragraphs":["Result 87."]}]}. For PPTX use {"name":"slides.pptx","kind":"deck","title":"Summary","slides":[{"title":"Findings","bullets":["Result 87."]}]}. Use document_validate before delivery.',
            DocumentCreateInput,
            ArtifactOutput,
            generate,
            False,
        ),
        (
            "document_read",
            "Read a bounded range of text units. Units marked editable=true have exact text-edit locators: DOCX paragraph:N or table:N:row:N:col:N; PPTX slide:N:shape:NAME (all native indexes zero-based). Read units are not page counts. Other units are read-only. Scanned PDF pages use OCR with page numbers. Image descriptions are metadata; extract pixels for visual analysis.",
            DocumentReadInput,
            DocumentDataOutput,
            read,
            True,
        ),
        (
            "document_search",
            "Search scoped document contents. Editable DOCX/PPTX results return the exact native text-edit locator with editable=true, matching document_read. Other results are read-only. Results report truncation.",
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
