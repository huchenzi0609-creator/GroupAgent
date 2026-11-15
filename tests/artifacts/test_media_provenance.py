from __future__ import annotations

import asyncio
import io

from pptx import Presentation
from pptx.util import Inches

from groupagent.artifacts import ArtifactScope, ArtifactStore, IsolatedDocumentService
from groupagent.artifacts.advanced import embedded_image
from groupagent.domain import ActorContext, Role
from groupagent.media import MediaService, MediaSource
from groupagent.media.validation import inspect_image
from groupagent.tools import ToolContext, ToolScope

from .test_extended_documents import picture


async def test_twenty_first_embedded_picture_retains_source_sequence(tmp_path):
    presentation = Presentation()
    pixels = picture().content
    for _ in range(21):
        presentation.slides.add_slide(presentation.slide_layouts[6]).shapes.add_picture(
            io.BytesIO(pixels), Inches(1), Inches(1), width=Inches(3)
        )
    output = io.BytesIO()
    presentation.save(output)
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    context = ToolContext(
        ToolScope(
            "group",
            "work",
            ActorContext(group_id="group", member_id="member", role=Role.OWNER),
            workspace.resolve(),
            frozenset(),
            "trace",
        ),
        "call",
        "key",
        asyncio.Event(),
    )
    source = MediaSource(
        group_id="group",
        work_id="work",
        source_event_id="synthetic-event",
        source_message_id="synthetic-message",
        member_id="member",
        attachment_id="deck-attachment",
        declared_kind="file",
        sequence=5,
        origin_kind="synthetic_fixture",
    )

    class SyntheticParserPort:
        async def run(self, context, operation, inputs, **kwargs):
            assert operation == "document_extract_image"
            raw, locator = embedded_image(inputs[0], "pptx", kwargs["parameters"]["image_index"])
            info, preview = inspect_image(raw)
            return {**info.model_dump(mode="json"), "locator": locator}, (preview,)

    with ArtifactStore(tmp_path / "store") as store:
        original = store.create(
            ArtifactScope("group", "work", "member"),
            "deck.pptx",
            output.getvalue(),
            provenance=source.model_dump(mode="json"),
        )
        worker = SyntheticParserPort()
        extracted = await IsolatedDocumentService(store, worker).extract_image(
            context, original.file_id, image_index=20
        )
        reference = MediaService(store, worker).load_ref(context, extracted.file_id)
        assert reference.source.sequence == 5
        assert reference.source.attachment_id == "deck-attachment#image:20"
        assert extracted.provenance["image_locator"].startswith("slide:21:")
        assert extracted.provenance["parent_document_version_id"] == original.version_id


async def test_combined_pdf_retains_media_source_for_followup_ocr(tmp_path):
    from groupagent.artifacts import Report, Section
    from groupagent.artifacts.advanced import combine_pdfs
    from groupagent.artifacts.formats import build_pdf

    workspace = tmp_path / "workspace"
    workspace.mkdir()
    context = ToolContext(
        ToolScope(
            "group",
            "work",
            ActorContext(group_id="group", member_id="member", role=Role.OWNER),
            workspace.resolve(),
            frozenset(),
            "trace",
        ),
        "combine",
        "combine-key",
        asyncio.Event(),
    )
    source = MediaSource(
        group_id="group",
        work_id="work",
        source_event_id="event-one",
        source_message_id="message-one",
        member_id="member",
        attachment_id="pdf-one",
        declared_kind="file",
    )

    class SyntheticPDFPort:
        async def run(self, context, operation, inputs, **kwargs):
            assert operation == "document_pdf_combine"
            return {"format": "pdf"}, (combine_pdfs(inputs),)

    with ArtifactStore(tmp_path / "store") as store:
        scope = ArtifactScope("group", "work", "member")
        payload = build_pdf(Report("Synthetic PDF", (Section("Result", ("Result 87",)),)))
        first = store.create(scope, "first.pdf", payload, provenance=source.model_dump(mode="json"))
        second = store.create(
            scope,
            "second.pdf",
            payload,
            provenance=source.model_copy(
                update={"source_event_id": "event-two", "attachment_id": "pdf-two"}
            ).model_dump(mode="json"),
        )
        worker = SyntheticPDFPort()
        combined = await IsolatedDocumentService(store, worker).combine_pdfs(
            context,
            "combined.pdf",
            ((first.file_id, first.version_id), (second.file_id, second.version_id)),
        )
        reference = MediaService(store, worker).load_ref(context, combined.file_id)
        assert reference.source == source
        assert combined.provenance["source_event_ids"] == ["event-one", "event-two"]
        assert combined.provenance["input_versions"] == [first.version_id, second.version_id]
