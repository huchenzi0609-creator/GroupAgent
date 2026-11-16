"""Real PG/Office bytes, explicit read/layout fixtures, no Docker or live model.

These composition tests check the independent native-table gate and the evidence
passed to semantic review. The fixture's successful layout record is synthetic;
actual worker extraction/rendering is covered by the artifact worker suites.
"""

import io
import json

import pytest
from pptx import Presentation
from pptx.util import Inches

from groupagent.artifacts import (
    ArtifactScope,
    DocumentRead,
    DocumentUnit,
    ValidationRecord,
)
from groupagent.domain import DomainError

from .test_toolkit_contract import candidate, prepare
from .test_toolkit_contract import toolkit_setup as toolkit_setup

pytestmark = pytest.mark.postgres

REQUEST = "请制作一页PPT，标题合成测量记录，包含原生表格，表格必须可编辑。"
CELLS = (("类别", "数值"), ("合成甲", "17"), ("合成乙", "29"))
ACTUAL_COUNT = object()


def presentation_bytes(*, native_table):
    presentation = Presentation()
    presentation.core_properties.title = "合成测量记录"
    slide = presentation.slides.add_slide(presentation.slide_layouts[6])
    title = slide.shapes.add_textbox(Inches(0.5), Inches(0.3), Inches(8), Inches(0.6))
    title.text = "合成测量记录"
    if native_table:
        table = slide.shapes.add_table(3, 2, Inches(0.5), Inches(1.2), Inches(7), Inches(2)).table
        for row_index, row in enumerate(CELLS):
            for column_index, value in enumerate(row):
                table.cell(row_index, column_index).text = value
    else:
        # Visible rows in a text box do not create an Office table object.
        text_box = slide.shapes.add_textbox(Inches(0.5), Inches(1.2), Inches(7), Inches(2))
        text_box.text = "\n".join(" | ".join(row) for row in CELLS)
    stream = io.BytesIO()
    presentation.save(stream)
    return stream.getvalue()


class ExplicitOfficeReadAndLayoutFixture:
    def __init__(self, version, payload, reported_count):
        self.version = version
        self.validation_calls = 0
        actual = Presentation(io.BytesIO(payload))
        self.actual_native_count = sum(
            bool(shape.has_table) for slide in actual.slides for shape in slide.shapes
        )
        metadata = {"title": actual.core_properties.title}
        if reported_count is ACTUAL_COUNT:
            metadata["native_table_count"] = str(self.actual_native_count)
        elif reported_count is not None:
            # Deliberately malformed service observations test the Runtime
            # gate independently of the isolated worker's metadata validation.
            metadata["native_table_count"] = reported_count
        text = []
        for slide in actual.slides:
            for shape in slide.shapes:
                if shape.has_text_frame:
                    text.append(shape.text)
                if shape.has_table:
                    text.extend(cell.text for row in shape.table.rows for cell in row.cells)
        self.result = DocumentRead(
            file_id=version.file_id,
            version_id=version.version_id,
            format="pptx",
            units=(DocumentUnit("slide:0", "\n".join(text), editable=False),),
            total_units=len(actual.slides),
            metadata=metadata,
        )

    async def read(self, context, file_id, version_id):
        assert (file_id, version_id) == (self.version.file_id, self.version.version_id)
        return self.result

    async def validate(self, context, file_id, version_id, *, expected_text, render):
        self.validation_calls += 1
        assert render and (file_id, version_id) == (
            self.version.file_id,
            self.version.version_id,
        )
        assert all(value in self.result.text for value in expected_text)
        return ValidationRecord(
            file_id=file_id,
            version_id=version_id,
            sha256=self.version.sha256,
            format="pptx",
            structural_passed=True,
            content_passed=True,
            rendered=True,
            layout_passed=True,
            page_count=1,
            renderer="explicit-synthetic-layout-fixture",
        )


async def registered_presentation(setup, *, native_table, reported_count):
    run = await prepare(
        setup,
        goal=REQUEST,
        request_text=REQUEST,
        scope={"outputs": [{"format": "pptx", "exact_units": 1}]},
    )
    payload = presentation_bytes(native_table=native_table)
    version = setup.env.artifacts.create(
        ArtifactScope(setup.env.actor.group_id, run.work.id, setup.env.actor.member_id),
        "synthetic-native-table.pptx",
        payload,
    )
    setup.env.registry.register(setup.env.actor, run.lease, version, purpose="result")
    documents = ExplicitOfficeReadAndLayoutFixture(version, payload, reported_count)
    _, media, context = setup.toolkit._services[run.lease.attempt_id]
    setup.toolkit._services[run.lease.attempt_id] = documents, media, context
    return run, version, documents


@pytest.mark.parametrize("reported_count", [None, "0", "-1", "invalid", "1.0", "１", 1, True])
async def test_missing_zero_or_invalid_native_count_blocks_before_semantic_review(
    toolkit_setup, reported_count
):
    setup = toolkit_setup
    run, _, documents = await registered_presentation(
        setup, native_table=False, reported_count=reported_count
    )
    assert documents.actual_native_count == 0 and setup.model.passed is True
    with pytest.raises(DomainError, match="actual Office file needs a native table") as failure:
        await setup.toolkit.validate(
            run.work,
            run.lease,
            setup.env.actor,
            candidate(run, text="合成测量记录已准备好。"),
            run.workspace,
        )
    assert failure.value.code == "validation_failed"
    assert documents.validation_calls == 0
    assert not setup.model.requests and not setup.http_calls


async def test_actual_native_table_with_noneditable_read_unit_reaches_semantic_review(
    toolkit_setup,
):
    setup = toolkit_setup
    run, version, documents = await registered_presentation(
        setup, native_table=True, reported_count=ACTUAL_COUNT
    )
    assert documents.actual_native_count == 1
    assert all(unit.editable is False for unit in documents.result.units)
    result = await setup.toolkit.validate(
        run.work,
        run.lease,
        setup.env.actor,
        candidate(run, text="合成测量记录已准备好。"),
        run.workspace,
    )
    assert result.checks["result"] and documents.validation_calls == 1
    assert len(setup.model.requests) == 1 and not setup.http_calls
    review = setup.model.requests[0]
    excerpt = json.loads(review.messages[1].content)["artifacts"][0]
    assert excerpt["format"] == "pptx" and excerpt["page_count"] == 1
    assert excerpt["metadata"] == documents.result.metadata
    assert excerpt["metadata"]["native_table_count"] == "1"
    assert "类别" in excerpt["text"] and "合成甲" in excerpt["text"]
    instruction = review.messages[0].content
    assert "unit.editable" in instruction and "编辑接口" in instruction
    assert "native_table_count" in instruction and "原生表格" in instruction
    assert any(draft.payload_ref == "artifact:" + version.version_id for draft in result.deliveries)
