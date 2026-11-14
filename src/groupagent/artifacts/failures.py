"""Reviewed document-input failures before persistent artifact publication only."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager

from groupagent.domain import DomainError
from groupagent.tools.models import ToolFailure

_HINTS = {
    "invalid_name": "Field name must be a basename without folders, at most 240 UTF-8 bytes, with the correct extension.",
    "unsupported_format": "Fields name/kind: report requires .md, .docx or .pdf; deck requires .pptx; charts require .png and PDF composition requires .pdf.",
    "invalid_reference": "Fields file_id/version_id must be exact 32-character artifact IDs returned by a prior tool or supplied input manifest; filenames and work IDs are not file references.",
    "artifact_not_found": "Fields file_id/version_id must select an available artifact in this group and work. Use an input manifest or prior tool result.",
    "scope_denied": "Fields file_id/version_id refer outside this work. Select a scoped input; changing identity fields cannot grant access.",
    "version_conflict": "Field expected_version_id is stale. Read the current artifact/version and review the intended change before retrying.",
    "file_too_large": "Field content exceeds the document byte limit: Markdown 2 MiB; Office/PDF 30 MiB. Reduce the input.",
    "invalid_document": "Fields name/content do not describe a readable supported document. Use the real format and complete source bytes.",
    "format_mismatch": "Fields name/content disagree. Do not rename unrelated bytes; provide the actual supported document format.",
    "invalid_content": "Field content must contain valid nonempty document bytes.",
    "invalid_input": "Document fields are inconsistent. Follow the tool schema and its minimal example.",
    "document_limit": "Fields sections/slides/paragraphs exceed the bounded document profile. Reduce the content or split it into documents.",
    "invalid_edit": "Fields operation/target/content must describe a supported bounded edit.",
    "invalid_target": "Field target must be an exact locator from document_read, not a filename or guessed path.",
    "edit_target_missing": "Field target was not found. Read the document locators before editing.",
    "ambiguous_target": "Field target matches more than one location. Supply an exact block/shape locator.",
    "section_not_found": "Field section_index is outside the document. Read the current sections first.",
    "slide_not_found": "Field slide_index is outside the deck. Read the current slides first.",
    "invalid_range": "Fields start_unit/max_units must select an existing bounded document range.",
    "invalid_query": "Field query must be nonempty and bounded.",
    "invalid_page_selection": "Field page_indices must select existing PDF pages using zero-based indices.",
    "page_limit": "The document exceeds the supported page/slide limit; reduce or split it.",
    "annotation_limit": "Field annotations exceeds the bounded annotation count.",
    "invalid_annotation": "Fields annotations.page_index/rect/kind must match supported PDF coordinates and note/highlight operations.",
    "invalid_chart": "Fields chart.categories/series must have matching finite values; a pie chart needs one nonnegative series with a positive total.",
    "chart_limit": "Fields chart.categories/series exceed the bounded chart profile.",
    "unsupported_chart": "Field chart.kind must be bar, line or pie.",
    "unsupported_layout": "The requested layout is outside the supported document profile. Simplify the layout.",
    "unsupported_edit": "Fields operation/format select an unsupported edit; use a supported native locator/operation.",
    "unsupported_template": "The template contains unsupported features; provide a simple supported native template.",
    "unsupported_feature": "The input has active or unsupported document features; provide a safe supported source.",
    "unsupported_markdown_block": "Field sections[].paragraphs contains a Markdown block (such as a code fence/table/heading). document_generate report paragraphs are plain text; for complete Markdown with fenced JSON/code, use workspace_write_text with the full .md text, then artifact_from_workspace and document_validate. Do not execute the example code.",
    "unsupported_text_target_format": 'Fields operation/format: text-target edits support DOCX/PPTX native locators only. For Markdown, use operation="template" with exact replacements, or operation="section" with section_index and plain paragraphs; preserve the current expected_version_id. A heading string is not a native locator.',
    "encrypted_document": "The input document is encrypted; provide an authorized unencrypted copy.",
    "unsafe_archive": "The Office archive failed safety checks; do not retry by bypassing validation.",
    "archive_limit": "The Office archive exceeds bounded expansion limits; provide a smaller source.",
    "unsupported_video": "Video is outside scope. Do not download, extract frames or extract its audio.",
    "invalid_image": "The referenced picture is not a fully decodable supported image.",
    "image_size_limit": "The referenced picture exceeds the image byte limit.",
    "image_pixel_limit": "The referenced picture exceeds the pixel limit.",
    "invalid_worker_input": "The isolated document parser rejected these fields or bytes. Check the input schema and supported format.",
}


@contextmanager
def document_input_phase() -> Iterator[None]:
    """Use only around reads/validation/isolated preparation before store writes.

    Never wrap ArtifactStore.create/revise/record_validation: a publication or
    persistence failure there can already have effects and needs reconciliation.
    """
    try:
        yield
    except DomainError as error:
        if isinstance(error, ToolFailure):
            raise
        hint = _HINTS.get(error.code)
        if hint is None or error.effect_state == "unknown":
            raise
        raise ToolFailure(
            error.code, "No artifact version was published. " + hint, no_effect=True
        ) from None
