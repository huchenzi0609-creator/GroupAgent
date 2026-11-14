"""Source-bound deck metadata requirements, independent of a model or parser."""

from copy import deepcopy
from datetime import UTC, datetime

import pytest

from groupagent.domain import EventKind, GroupEvent
from groupagent.runtime.toolkit import _output_requirements

from .test_tool_selection import work
from .test_toolkit_contract import call, candidate, prepare
from .test_toolkit_contract import toolkit_setup as toolkit_setup

REQUEST = (
    "请做成2页PPT，主题《合成主题》。第1页标题“观察”，写“输入数据”；"
    "第2页标题“讨论”，写“核对结果”。只用这些内容。"
)


def original(text):
    now = datetime.now(UTC)
    return GroupEvent(
        id="synthetic-request",
        source_key="synthetic-metadata",
        group_id="synthetic-group",
        actor_id="synthetic-member",
        kind=EventKind.TEXT,
        text=text,
        occurred_at=now,
        received_at=now,
    )


def deck():
    return work(
        REQUEST,
        outputs=[
            {"format": "pptx", "exact_units": 2, "expected_text": ["合成主题", "观察", "讨论"]}
        ],
    )


def test_document_topic_does_not_become_an_extra_visible_slide_literal():
    current = deck()
    before = deepcopy(current.model_dump())
    assert _output_requirements(current, original(REQUEST)) == [
        {
            "format": "pptx",
            "exact_units": 2,
            "expected_text": ["观察", "讨论"],
            "metadata_title": "合成主题",
        }
    ]
    assert current.model_dump() == before


@pytest.mark.parametrize(
    "text",
    [
        REQUEST + "请每页显示主题。",
        REQUEST.replace("观察", "合成主题"),
        REQUEST.replace("第2页标题“讨论”", "第二页写讨论"),
        REQUEST.replace("主题《合成主题》", "题目是合成主题"),
        REQUEST + "封面保留主题。",
    ],
)
def test_ambiguous_or_explicit_visible_title_keeps_original_literal_requirement(text):
    current = deck()
    assert _output_requirements(current, original(text)) == current.scope["outputs"]


@pytest.mark.parametrize("changed", ["revision", "input", "attachment", "format"])
def test_edits_imports_and_other_formats_keep_their_original_contract(changed):
    current = deck()
    if changed == "revision":
        current = current.model_copy(update={"goal_revision": 2})
    elif changed == "input":
        current = current.model_copy(update={"input_versions": ["synthetic-version"]})
    elif changed == "attachment":
        current.scope["attachment_event_ids"] = ["synthetic-input"]
    else:
        current.scope["outputs"][0]["format"] = "docx"
    assert _output_requirements(current, original(REQUEST)) == current.scope["outputs"]


@pytest.mark.parametrize(
    "declaration,wrapped",
    [
        ("标题《合成清单》", "《合成清单》"),
        ("题目为“合成清单”", "“合成清单”"),
        ('标题是"合成清单"', '"合成清单"'),
    ],
)
@pytest.mark.parametrize("format_name", ["docx", "pdf", "md", "pptx"])
def test_title_notation_delimiters_are_not_invented_as_visible_content(
    declaration, wrapped, format_name
):
    text = "请生成文件，" + declaration + "，内容为检查条目。"
    current = work(text, outputs=[{"format": format_name, "expected_text": [wrapped, "检查条目"]}])
    assert _output_requirements(current, original(text))[0]["expected_text"] == [
        "合成清单",
        "检查条目",
    ]
    assert current.scope["outputs"][0]["expected_text"][0] == wrapped


@pytest.mark.parametrize(
    "extra", ["请保留书名号。", "包括引号。", "逐字原样。", "正文也写《合成清单》。"]
)
def test_literal_or_repeated_title_marks_remain_required(extra):
    text = "标题《合成清单》，内容为检查条目。" + extra
    current = work(text, outputs=[{"format": "docx", "expected_text": ["《合成清单》"]}])
    assert _output_requirements(current, original(text))[0]["expected_text"] == ["《合成清单》"]


@pytest.mark.postgres
async def test_actual_objective_separates_topic_metadata_without_rewriting_stored_router_output(
    toolkit_setup,
):
    run = await prepare(
        toolkit_setup,
        scope=deepcopy(deck().scope),
        goal=REQUEST,
        request_text=REQUEST,
    )
    import json

    output = json.loads(run.prepared.request.objective)["required_outputs"][0]
    assert output["metadata_title"] == "合成主题"
    assert output["expected_text"] == ["观察", "讨论"]
    assert run.work.scope["outputs"][0]["expected_text"] == ["合成主题", "观察", "讨论"]
    assert not toolkit_setup.model.requests


@pytest.mark.postgres
async def test_title_notation_normalization_reaches_independent_file_validation(toolkit_setup):
    text = "请制作Markdown，标题《合成清单》，内容为检查条目。"
    run = await prepare(
        toolkit_setup,
        goal=text,
        request_text=text,
        scope={"outputs": [{"format": "md", "expected_text": ["《合成清单》", "检查条目"]}]},
    )
    created = await call(
        run,
        "document_generate",
        {
            "name": "checklist.md",
            "kind": "report",
            "title": "合成清单",
            "sections": [{"title": "内容", "paragraphs": ["检查条目"]}],
        },
    )
    assert created.success
    selected = await call(run, "artifact_deliver", {"version_ids": [created.output["version_id"]]})
    assert selected.success
    accepted = await toolkit_setup.toolkit.validate(
        run.work,
        run.lease,
        toolkit_setup.env.actor,
        candidate(run, "文件已准备好。", (created, selected)),
        run.workspace,
    )
    assert accepted.checks["result"]
    assert run.work.scope["outputs"][0]["expected_text"][0] == "《合成清单》"
