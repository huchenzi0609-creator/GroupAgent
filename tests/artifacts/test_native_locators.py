from __future__ import annotations

import asyncio
import io
import zipfile

import pytest
from docx import Document
from pptx import Presentation

from groupagent.artifacts import (
    ArtifactError,
    ArtifactScope,
    ArtifactStore,
    Deck,
    IsolatedDocumentService,
    Report,
    Section,
    Slide,
    TableData,
)
from groupagent.artifacts.advanced import edit_text_target
from groupagent.artifacts.formats import extract
from groupagent.artifacts.worker_operations import document_operation
from groupagent.domain import ActorContext, Role
from groupagent.tools import ToolContext, ToolScope


class SyntheticNativeParserPort:
    """Actual format operations on these synthetic fixtures; not an isolation test."""

    def __init__(self, root):
        self.root, self.calls = root, []

    async def run(self, context, operation, inputs, *, parameters, outputs=()):
        self.calls.append(operation)
        directory = self.root / str(len(self.calls))
        directory.mkdir()
        request = {"parameters": parameters, "outputs": outputs}
        result = document_operation(
            operation,
            request,
            lambda request, index=0: inputs[index],
            lambda name: directory / name,
        )
        return result, tuple((directory / name).read_bytes() for name in outputs)


@pytest.fixture
def native_documents(tmp_path):
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    port = SyntheticNativeParserPort(workspace)
    context = ToolContext(
        ToolScope(
            "synthetic-group",
            "synthetic-work",
            ActorContext(group_id="synthetic-group", member_id="author", role=Role.OWNER),
            workspace.resolve(),
            frozenset(),
            "native-locator-fixture",
        ),
        "native-edit",
        "native-edit",
        asyncio.Event(),
    )
    with ArtifactStore(tmp_path / "store") as store:
        yield store, IsolatedDocumentService(store, port), context


def budget_report():
    return Report(
        "合成预算报告",
        (
            Section(
                "来源",
                ("数字仅用于合成测试。",),
                (TableData(("项目", "说明"), (("预留", "预留20元"),)),),
            ),
            Section("预算", ("预算100元。", "不要改动这一段。")),
        ),
    )


def parts(content):
    with zipfile.ZipFile(io.BytesIO(content)) as archive:
        return {name: archive.read(name) for name in archive.namelist()}


@pytest.mark.parametrize(
    "query,replacement,prefix",
    [("预算100元", "预算150元", "paragraph:"), ("预留20元", "预留30元", "table:")],
)
async def test_docx_generate_read_search_edit_preserves_all_other_package_content(
    native_documents, query, replacement, prefix
):
    store, service, context = native_documents
    scope = ArtifactScope(context.group_id, context.work_id, context.actor.member_id)
    original = await service.generate(context, "budget.docx", budget_report())
    before = store.read(scope, original.file_id, original.version_id)
    read = await service.read(context, original.file_id)
    matches = await service.search(context, original.file_id, query)
    assert matches["match_count"] == 1 and matches["complete"]
    unit = matches["units"][0]
    assert unit["editable"] and unit["locator"].startswith(prefix)
    assert any(item.locator == unit["locator"] and item.editable for item in read.units)
    revised = await service.edit_target(
        context,
        original.file_id,
        expected_version_id=original.version_id,
        target=unit["locator"],
        expected_text=query,
        replacement=replacement,
    )
    after = store.read(scope, revised.file_id, revised.version_id)
    before_parts, after_parts = parts(before), parts(after)
    assert before_parts.keys() == after_parts.keys()
    for name, content in before_parts.items():
        assert after_parts[name] == (
            content.replace(query.encode(), replacement.encode())
            if name == "word/document.xml"
            else content
        )
    assert revised.version_id != original.version_id
    assert store.read(scope, original.file_id, original.version_id) == before
    assert replacement in (await service.read(context, original.file_id)).text
    assert query not in (await service.read(context, original.file_id)).text


async def test_legacy_docx_block_maps_actual_interleaved_body_not_paragraph_number(
    native_documents,
):
    store, service, context = native_documents
    scope = ArtifactScope(context.group_id, context.work_id, context.actor.member_id)
    original = await service.generate(context, "budget.docx", budget_report())
    content = store.read(scope, original.file_id)
    legacy = next(unit for unit in extract(content, "docx") if "预算100元" in unit.text)
    native = next(
        unit for unit in extract(content, "docx", native_targets=True) if "预算100元" in unit.text
    )
    # The intervening table makes even a one-based paragraph conversion wrong.
    assert int(legacy.locator.split(":")[1]) - 1 != int(native.locator.split(":")[1])
    revised = await service.edit_target(
        context,
        original.file_id,
        expected_version_id=original.version_id,
        target=legacy.locator,
        expected_text="预算100元",
        replacement="预算150元",
    )
    actual = Document(io.BytesIO(store.read(scope, revised.file_id)))
    assert actual.paragraphs[int(native.locator.split(":")[1])].text == "预算150元。"
    assert actual.tables[0].cell(1, 1).text == "预留20元"


def test_docx_merged_cell_has_one_editable_target_and_out_of_range_column_is_rejected():
    document = Document()
    table = document.add_table(rows=2, cols=2)
    table.cell(0, 0).merge(table.cell(0, 1)).text = "Unique budget"
    table.cell(1, 0).text = "Do not touch"
    output = io.BytesIO()
    document.save(output)
    content = output.getvalue()
    units = extract(content, "docx", native_targets=True)
    matches = [unit for unit in units if unit.text == "Unique budget"]
    assert len(matches) == 1 and matches[0].locator == "table:0:row:0:col:0"
    changed = edit_text_target(content, "docx", matches[0].locator, "Unique budget", "New budget")
    assert Document(io.BytesIO(changed)).tables[0].cell(0, 1).text == "New budget"
    with pytest.raises(ArtifactError) as error:
        edit_text_target(content, "docx", "table:0:row:0:col:2", "Do not touch", "Wrong")
    assert error.value.code == "edit_target_missing"


async def test_pptx_read_and_search_provide_zero_based_exact_editable_shape(native_documents):
    store, service, context = native_documents
    scope = ArtifactScope(context.group_id, context.work_id, context.actor.member_id)
    original = await service.generate(
        context,
        "budget.pptx",
        Deck("Budget", (Slide("Plan", ("Budget 100",)), Slide("Keep", ("Untouched",)))),
    )
    before = store.read(scope, original.file_id)
    read = await service.read(context, original.file_id)
    matches = await service.search(context, original.file_id, "Budget 100")
    assert matches["match_count"] == 1
    unit = matches["units"][0]
    assert unit["locator"].startswith("slide:0:shape:") and unit["editable"]
    assert any(item.locator == unit["locator"] for item in read.units)
    revised = await service.edit_target(
        context,
        original.file_id,
        expected_version_id=original.version_id,
        target=unit["locator"],
        expected_text="Budget 100",
        replacement="Budget 150",
    )
    after = store.read(scope, revised.file_id)
    assert "Budget 150" in (await service.read(context, revised.file_id)).text
    assert parts(before)["ppt/slides/slide2.xml"] == parts(after)["ppt/slides/slide2.xml"]
    # Structural slide counting is unchanged by the native read projection.
    assert len(extract(after, "pptx")) == 2


def test_duplicate_pptx_shape_names_are_explicitly_read_only():
    presentation = Presentation()
    slide = presentation.slides.add_slide(presentation.slide_layouts[1])
    slide.shapes[0].name = slide.shapes[1].name = "duplicate"
    slide.shapes[0].text = "Budget 100"
    slide.shapes[1].text = "Keep"
    output = io.BytesIO()
    presentation.save(output)
    units = extract(output.getvalue(), "pptx", native_targets=True)
    assert len(units) == 2 and not any(unit.editable for unit in units)
    assert {unit.locator for unit in units} == {"slide:0:shape_index:0", "slide:0:shape_index:1"}
