"""Runtime document service: all attachment parsing/rendering occurs in SandboxRunner."""

from __future__ import annotations

import json
import re
from collections.abc import Awaitable, Callable
from dataclasses import asdict
from typing import Any

from groupagent.domain import DomainError
from groupagent.security import redact_credentials
from groupagent.tools import ToolContext
from groupagent.tools.models import ToolFailure, ToolFailureDisposition, ToolFailureOrigin

from .failures import document_input_phase
from .models import (
    ArtifactScope,
    ArtifactVersion,
    ChartData,
    Deck,
    DocumentRead,
    DocumentUnit,
    Report,
    ValidationRecord,
)
from .store import ArtifactStore, document_format


class IsolatedDocumentService:
    def __init__(
        self,
        store: ArtifactStore,
        worker: Any,
        *,
        policy_guard: Callable[[ToolContext], Awaitable[None]] | None = None,
        max_import_bytes: int = 30 * 1024 * 1024,
        max_markdown_bytes: int = 2 * 1024 * 1024,
        max_output_bytes: int = 25 * 1024 * 1024,
    ):
        if (
            type(max_import_bytes) is not int
            or not 0 < max_import_bytes <= 30 * 1024 * 1024
            or type(max_markdown_bytes) is not int
            or not 0 < max_markdown_bytes <= 2 * 1024 * 1024
            or type(max_output_bytes) is not int
            or not 0 < max_output_bytes <= 30 * 1024 * 1024
        ):
            raise ValueError("Document limits must be within 30 MiB and 2 MiB input Markdown")
        self.store, self.worker, self.policy_guard = store, worker, policy_guard
        self.max_import_bytes, self.max_markdown_bytes = max_import_bytes, max_markdown_bytes
        self.max_output_bytes = max_output_bytes

    def _output(self, content: bytes) -> bytes:
        if len(content) > self.max_output_bytes:
            raise ToolFailure(
                "file_too_large",
                "Generated output exceeds the configured file byte limit; no artifact was published.",
                no_effect=True,
            )
        return content

    @staticmethod
    def _format(name: str, *, media: bool = False) -> str:
        with document_input_phase(origin=ToolFailureOrigin.ARGUMENTS):
            return document_format(name, media=media)

    def _get(
        self, scope: ArtifactScope, file_id: str, version_id: str | None = None
    ) -> ArtifactVersion:
        with document_input_phase():
            return self.store.get(scope, file_id, version_id)

    def _read(self, scope: ArtifactScope, file_id: str, version_id: str | None = None) -> bytes:
        with document_input_phase():
            return self.store.read(scope, file_id, version_id)

    async def _run(
        self, context: ToolContext, operation: str, inputs: tuple[bytes, ...], **kwargs: Any
    ) -> tuple[dict[str, Any], tuple[bytes, ...]]:
        # These fixed operations prepare bytes in the isolated worker. Durable
        # create/revise/validation writes remain outside this exception boundary.
        with document_input_phase():
            return await self.worker.run(context, operation, inputs, **kwargs)

    async def _scope(self, context: ToolContext) -> ArtifactScope:
        with document_input_phase(origin=ToolFailureOrigin.AUTHORIZATION):
            if context.group_id != context.actor.group_id or context.cancelled.is_set():
                raise DomainError("cancelled", "Document work has no active scoped context")
            if self.policy_guard:
                await self.policy_guard(context)
            return ArtifactScope(context.group_id, context.work_id, context.actor.member_id)

    async def generate(
        self,
        context: ToolContext,
        name: str,
        document: Report | Deck,
        *,
        provenance: dict[str, Any] | None = None,
    ) -> ArtifactVersion:
        from .worker_operations import pack_document

        await self._scope(context)
        format_name = self._format(name)
        with document_input_phase(origin=ToolFailureOrigin.ARGUMENTS):
            if (format_name == "pptx") != isinstance(document, Deck):
                raise DomainError(
                    "unsupported_format", "Report/deck kind does not match its output extension"
                )
            packed, inputs = pack_document(document)
        _, outputs = await self._run(
            context,
            "document_generate",
            inputs,
            parameters={"format": format_name, "document": packed},
            outputs=("document." + format_name,),
        )
        scope = await self._scope(context)
        return self.store.create(scope, name, self._output(outputs[0]), provenance=provenance)

    async def render_chart(
        self,
        context: ToolContext,
        name: str,
        chart: ChartData,
        *,
        provenance: dict[str, Any] | None = None,
    ) -> ArtifactVersion:
        await self._scope(context)
        if self._format(name, media=True) != "png":
            raise ToolFailure(
                "unsupported_format",
                "Field name must be a .png basename for chart_render; no artifact was published.",
                no_effect=True,
            )
        metadata, outputs = await self._run(
            context, "chart_render", (), parameters={"chart": asdict(chart)}, outputs=("chart.png",)
        )
        scope = await self._scope(context)
        version = self.store.create(
            scope,
            name,
            self._output(outputs[0]),
            provenance={
                **(provenance or {}),
                "media_metadata": metadata,
                "chart_spec": asdict(chart),
                "image_method": "deterministic_chart",
            },
        )
        self.store.record_validation(
            scope,
            version.file_id,
            version.version_id,
            ValidationRecord(
                version.file_id,
                version.version_id,
                version.sha256,
                "png",
                True,
                True,
                media_decoded=True,
                checks=(
                    "bounded_chart_data",
                    "deterministic_raster",
                    "full_image_decode",
                    "sha256",
                ),
            ),
        )
        return version

    async def import_document(
        self,
        context: ToolContext,
        name: str,
        content: bytes,
        *,
        provenance: dict[str, Any] | None = None,
        generated_output: bool = False,
    ) -> ArtifactVersion:
        # Only the trusted artifact_from_workspace adoption path sets this;
        # channel/history imports retain the smaller input limits.
        if type(generated_output) is not bool:
            raise ValueError("Generated-output adoption must be an explicit boolean")
        await self._scope(context)
        format_name = self._format(name)
        limit = (
            self.max_output_bytes
            if generated_output
            else self.max_markdown_bytes
            if format_name == "md"
            else self.max_import_bytes
        )
        if len(content) > limit:
            raise ToolFailure(
                "file_too_large",
                "Field content exceeds the configured document import byte limit; no artifact was published.",
                no_effect=True,
            )
        await self._run(
            context,
            "document_inspect",
            (content,),
            parameters={"format": format_name, "max_import_bytes": limit},
        )
        scope = await self._scope(context)
        return self.store.create(scope, name, content, provenance=provenance)

    async def read(
        self,
        context: ToolContext,
        file_id: str,
        version_id: str | None = None,
        *,
        start_unit: int = 0,
        max_units: int = 200,
    ) -> DocumentRead:
        scope = await self._scope(context)
        version = self._get(scope, file_id, version_id)
        result, _ = await self._run(
            context,
            "document_read",
            (self._read(scope, file_id, version.version_id),),
            parameters={"format": version.format, "start_unit": start_unit, "max_units": max_units},
        )
        await self._scope(context)
        metadata = result.get("metadata", {})
        if not isinstance(metadata, dict) or any(
            not isinstance(key, str) or not isinstance(value, str) or len(value) > 1000
            for key, value in metadata.items()
        ):
            raise DomainError("invalid_document", "Document worker returned invalid metadata")
        allowed_metadata = (
            {"title", "native_table_count"}
            if version.format == "pptx"
            else {
                "native_table_count",
                "embedded_image_count",
                "distinct_embedded_image_count",
                "embedded_image_sha256",
            }
            if version.format == "docx"
            else set()
        )
        if not set(metadata).issubset(allowed_metadata) or (
            any(
                key in metadata and not re.fullmatch(r"0|[1-9][0-9]*", metadata[key])
                for key in (
                    "native_table_count",
                    "embedded_image_count",
                    "distinct_embedded_image_count",
                )
            )
        ):
            raise DomainError("invalid_document", "Document worker returned invalid metadata")
        if "embedded_image_sha256" in metadata:
            try:
                hashes = json.loads(metadata["embedded_image_sha256"])
                if (
                    not isinstance(hashes, list)
                    or len(hashes) > 10
                    or any(
                        not isinstance(value, str) or not re.fullmatch(r"[a-f0-9]{64}", value)
                        for value in hashes
                    )
                ):
                    raise ValueError("invalid image digest list")
            except (ValueError, TypeError):
                raise DomainError(
                    "invalid_document", "Document worker returned invalid image hashes"
                ) from None
        return DocumentRead(
            file_id,
            version.version_id,
            version.format,
            tuple(DocumentUnit(**unit) for unit in result["units"]),
            warnings=("Returned a bounded document range; additional units remain",)
            if not result.get("complete", True)
            else (),
            total_units=result.get("total_units"),
            complete=result.get("complete", True),
            ocr_pages=tuple(result.get("ocr_pages", ())),
            metadata={key: redact_credentials(value) for key, value in metadata.items()},
        )

    async def _edit(
        self,
        context: ToolContext,
        file_id: str,
        expected_version_id: str,
        operation: str,
        parameters: dict[str, Any],
    ) -> ArtifactVersion:
        scope = await self._scope(context)
        current = self._get(scope, file_id)
        if operation == "document_edit_target" and current.format not in {"docx", "pptx"}:
            with document_input_phase():
                raise DomainError(
                    "unsupported_text_target_format", "This document has no native text-target edit"
                )
        if current.version_id != expected_version_id:
            raise ToolFailure(
                "version_conflict",
                "Field expected_version_id is stale; read the current version before retrying. No artifact version was published.",
                no_effect=True,
                origin=ToolFailureOrigin.PRECONDITION,
                disposition=ToolFailureDisposition.REPAIR,
            )
        _, outputs = await self._run(
            context,
            operation,
            (self._read(scope, file_id, current.version_id),),
            parameters={"format": current.format, **parameters},
            outputs=("edited." + current.format,),
        )
        scope = await self._scope(context)
        return self.store.revise(
            scope, file_id, self._output(outputs[0]), expected_version_id=expected_version_id
        )

    async def search(
        self, context: ToolContext, file_id: str, query: str, version_id: str | None = None
    ) -> dict[str, Any]:
        scope = await self._scope(context)
        version = self._get(scope, file_id, version_id)
        result, _ = await self._run(
            context,
            "document_search",
            (self._read(scope, file_id, version.version_id),),
            parameters={"format": version.format, "query": query},
        )
        await self._scope(context)
        return {"file_id": file_id, "version_id": version.version_id, **result}

    async def edit_section(
        self,
        context: ToolContext,
        file_id: str,
        *,
        expected_version_id: str,
        section_index: int,
        paragraphs: tuple[str, ...],
    ) -> ArtifactVersion:
        return await self._edit(
            context,
            file_id,
            expected_version_id,
            "document_edit_section",
            {"section_index": section_index, "paragraphs": paragraphs},
        )

    async def edit_slide(
        self,
        context: ToolContext,
        file_id: str,
        *,
        expected_version_id: str,
        slide_index: int,
        title: str | None = None,
        bullets: tuple[str, ...] | None = None,
    ) -> ArtifactVersion:
        return await self._edit(
            context,
            file_id,
            expected_version_id,
            "document_edit_slide",
            {"slide_index": slide_index, "title": title, "bullets": bullets},
        )

    async def edit_target(
        self,
        context: ToolContext,
        file_id: str,
        *,
        expected_version_id: str,
        target: str,
        expected_text: str,
        replacement: str,
    ) -> ArtifactVersion:
        return await self._edit(
            context,
            file_id,
            expected_version_id,
            "document_edit_target",
            {"target": target, "expected_text": expected_text, "replacement": replacement},
        )

    async def fill_template(
        self,
        context: ToolContext,
        file_id: str,
        *,
        expected_version_id: str,
        replacements: dict[str, str],
    ) -> ArtifactVersion:
        return await self._edit(
            context,
            file_id,
            expected_version_id,
            "document_template",
            {"replacements": replacements},
        )

    async def edit_chart(
        self,
        context: ToolContext,
        file_id: str,
        *,
        expected_version_id: str,
        slide_index: int,
        shape_name: str,
        chart: ChartData,
    ) -> ArtifactVersion:
        return await self._edit(
            context,
            file_id,
            expected_version_id,
            "document_chart",
            {"slide_index": slide_index, "shape_name": shape_name, "chart": asdict(chart)},
        )

    async def extract_image(
        self, context: ToolContext, file_id: str, version_id: str | None = None, *, image_index: int
    ) -> ArtifactVersion:
        scope = await self._scope(context)
        version = self._get(scope, file_id, version_id)
        result, outputs = await self._run(
            context,
            "document_extract_image",
            (self._read(scope, file_id, version.version_id),),
            parameters={"format": version.format, "image_index": image_index},
            outputs=("embedded.png",),
        )
        scope = await self._scope(context)
        provenance = {
            **version.provenance,
            "declared_kind": "image",
            "sequence": version.provenance.get("sequence", 0),
            "attachment_id": str(version.provenance.get("attachment_id", file_id))
            + f"#image:{image_index}",
            "parent_document_version_id": version.version_id,
            "image_locator": result.pop("locator"),
            "media_metadata": result,
        }
        extracted = self.store.create(
            scope, f"embedded-{image_index}.png", self._output(outputs[0]), provenance=provenance
        )
        self.store.record_validation(
            scope,
            extracted.file_id,
            extracted.version_id,
            ValidationRecord(
                extracted.file_id,
                extracted.version_id,
                extracted.sha256,
                "png",
                True,
                True,
                media_decoded=True,
                checks=("document_image_reference", "full_image_decode", "pixel_limit"),
            ),
        )
        return extracted

    async def replace_image(
        self,
        context: ToolContext,
        file_id: str,
        *,
        expected_version_id: str,
        image_index: int,
        image_file_id: str,
        image_version_id: str,
        alt_text: str,
    ) -> ArtifactVersion:
        scope = await self._scope(context)
        current = self._get(scope, file_id)
        if current.version_id != expected_version_id:
            raise ToolFailure(
                "version_conflict",
                "Field expected_version_id is stale; read the current version before retrying. No artifact version was published.",
                no_effect=True,
                origin=ToolFailureOrigin.PRECONDITION,
                disposition=ToolFailureDisposition.REPAIR,
            )
        image = self._read(scope, image_file_id, image_version_id)
        _, outputs = await self._run(
            context,
            "document_replace_image",
            (self._read(scope, file_id, expected_version_id), image),
            parameters={"format": current.format, "image_index": image_index, "alt_text": alt_text},
            outputs=("edited." + current.format,),
        )
        scope = await self._scope(context)
        return self.store.revise(
            scope, file_id, self._output(outputs[0]), expected_version_id=expected_version_id
        )

    async def select_pdf_pages(
        self,
        context: ToolContext,
        file_id: str,
        *,
        expected_version_id: str,
        page_indices: tuple[int, ...],
        rotations: dict[int, int] | None = None,
    ) -> ArtifactVersion:
        return await self._edit(
            context,
            file_id,
            expected_version_id,
            "document_pdf_pages",
            {"page_indices": page_indices, "rotations": rotations or {}},
        )

    async def annotate_pdf(
        self,
        context: ToolContext,
        file_id: str,
        *,
        expected_version_id: str,
        annotations: tuple[dict[str, Any], ...],
    ) -> ArtifactVersion:
        return await self._edit(
            context,
            file_id,
            expected_version_id,
            "document_pdf_annotate",
            {"annotations": annotations},
        )

    async def combine_pdfs(
        self, context: ToolContext, name: str, references: tuple[tuple[str, str], ...]
    ) -> ArtifactVersion:
        scope = await self._scope(context)
        if self._format(name) != "pdf" or not references:
            raise ToolFailure(
                "unsupported_format",
                "Field name must end in .pdf and inputs must list at least one PDF reference; no artifact was published.",
                no_effect=True,
            )
        inputs = []
        provenance: dict[str, Any] = {}
        sources: set[str] = set()
        for file_id, version_id in references:
            version = self._get(scope, file_id, version_id)
            if version.format != "pdf":
                raise ToolFailure(
                    "unsupported_format",
                    "Field inputs must contain only PDF artifact versions; no artifact was published.",
                    no_effect=True,
                )
            inputs.append(self._read(scope, file_id, version_id))
            if not provenance:
                provenance = dict(version.provenance)
            if isinstance(version.provenance.get("source_event_id"), str):
                sources.add(version.provenance["source_event_id"])
            sources.update(
                value
                for value in version.provenance.get("source_event_ids", ())
                if isinstance(value, str)
            )
        _, outputs = await self._run(
            context,
            "document_pdf_combine",
            tuple(inputs),
            parameters={"format": "pdf"},
            outputs=("combined.pdf",),
        )
        scope = await self._scope(context)
        return self.store.create(
            scope,
            name,
            self._output(outputs[0]),
            provenance={
                **provenance,
                "declared_kind": "file",
                "input_versions": [version for _, version in references],
                "source_event_ids": sorted(sources),
            },
        )

    async def validate(
        self,
        context: ToolContext,
        file_id: str,
        version_id: str | None = None,
        *,
        expected_text: tuple[str, ...],
        render: bool = True,
    ) -> ValidationRecord:
        scope = await self._scope(context)
        version = self._get(scope, file_id, version_id)
        slots = (
            (100 if version.format == "pptx" else 200) if render and version.format != "md" else 0
        )
        result, outputs = await self._run(
            context,
            "document_validate",
            (self._read(scope, file_id, version.version_id),),
            parameters={"format": version.format, "expected_text": expected_text, "render": render},
            outputs=tuple(f"preview-{index}.png" for index in range(slots)),
        )
        if result["sha256"] != version.sha256:
            raise DomainError(
                "worker_integrity_failed", "Document validation describes different bytes"
            )
        result.update(file_id=file_id, version_id=version.version_id)
        validation = ValidationRecord.from_dict(result)
        scope = await self._scope(context)
        previews = outputs[: len(validation.preview_sha256)]
        return self.store.record_validation(
            scope, file_id, version.version_id, validation, previews=previews
        )
