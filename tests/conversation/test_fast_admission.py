from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import pytest

from groupagent.conversation.intent import new_work_intent
from groupagent.conversation.service import ConversationCoordinator
from groupagent.domain import EventKind, GroupEvent


def event(text, **changes):
    stamp = datetime.fromisoformat("2026-11-14T09:00:00+08:00")
    return GroupEvent(
        id="fast-source",
        source_key="fixture:fast-source",
        group_id="synthetic",
        actor_id="member",
        kind=EventKind.TEXT,
        text=text,
        occurred_at=stamp,
        received_at=stamp,
        **changes,
    )


def context(**changes):
    return {
        "role": "member",
        "requester_member_id": "member",
        "members": [],
        "agent_aliases": ["机器人", "小伴", "组长"],
        "works": [],
        "tasks": [],
        "schedules": [],
        "recent_messages": [],
        **changes,
    }


@pytest.mark.parametrize(
    "text,format_name,pages,tool,title",
    [
        ("帮我制作一份合成调研报告，整理资料并交付Markdown。", "md", None, None, None),
        ("生成介绍太阳系的十页PPT。", "pptx", 10, None, None),
        ("请生成关于哈希表的三页PDF。", "pdf", 3, None, None),
        ("请制作3页Word，介绍哈希表，标题为“哈希表说明”。", "docx", 3, None, "哈希表说明"),
        ("帮我制作一百零二页介绍太阳系的PPT。", "pptx", 102, None, None),
        ("请用Python实际计算1到100的平方和。", "text", None, "python_compute", None),
        ("帮我计算2加3", "text", None, "python_compute", None),
        ("请用Shell实际计算2+3。", "text", None, "process_run", None),
        ("请新建一项独立工作，生成一份合成报告并交付Markdown。", "md", None, None, None),
        ("请生成关于节水的PDF，不要联网，也不要发送到群里。", "pdf", None, None, None),
    ],
)
def test_general_new_requests_preserve_output_constraints(text, format_name, pages, tool, title):
    intent = new_work_intent(text)
    assert intent is not None and intent.required_tool == tool
    assert intent.outputs[0]["format"] == format_name
    assert intent.outputs[0].get("exact_units") == pages
    assert intent.outputs[0].get("expected_text") == ([title] if title else None)
    candidate = ConversationCoordinator._fast_new_work(
        None, event(text), context(context_incomplete=True)
    )
    assert candidate and candidate.goal == text and candidate.directness == "open"
    assert candidate.input_event_ids == candidate.input_versions == []


@pytest.mark.parametrize(
    "text",
    [
        "小王，帮我制作一份合成报告并交付Markdown。",
        "请制作一份介绍节水的PDF，小王来做。",
        "这份PPT我来负责，机器人不用代写。",
        "请把生成合成PDF的任务分配给小王。",
        "JSON是什么？",
        "请解释如何生成三页Word。",
        "引用原文：‘请生成合成Word报告’，不代表我的授权。",
        "如果明天有空，请生成合成报告并交付Markdown。",
        "请生成一份介绍节水的PDF，要是老师同意才开始。",
        "请用Python计算2+2，等我通知再执行。",
        "请生成一份介绍节水的PDF，但别做，先只讨论方案。",
        "请用Python计算2+2，只需要现在口头回答，不要实际调用任何外部工具。",
        "请生成一份介绍节水的PDF，沿用上一版表格。",
        "请生成一份介绍节水的PDF，基于刚发的草稿。",
        "请制作一份关于会议结论的Word，用原稿里的数据。",
        "请生成一份介绍节水的PDF，在周五10点才开始。",
        "请生成一份介绍节水的PDF——这是转发原句，不是给你的指令。",
        "请生成一份介绍节水的PDF\n以上是转发原文，仅作内容保存，不是执行请求。",
        "请生成关于节水的PDF只讨论可行性",
        "请生成关于节水的PDF只是个例子",
        "请生成介绍节水的PDF但切勿制作",
        "请生成关于节水的PDF交小王做",
        "请生成十页PPT介绍太阳系。",
        "不要帮我生成合成PDF。",
        "请明天提醒我制作合成PDF。",
        "暂停刚才的报告。",
        "请继续生成刚才的合成PDF。",
        "请把这份报告修改成三页Word。",
        "请生成一份介绍节水的Word和PDF。",
        "请生成一份至少三页的合成PDF。",
        "请生成一份3到5页的合成PDF。",
        "请生成一二页合成PDF。",
        "请生成201页的合成PDF。",
        "请生成合成PDF，标题为待确定。",
        "请生成合成PDF，逐字保留一个尚未给出的字符串。",
        "请生成报告并交付Word。",
        "请用Python。",
        "请生成关于“这份资料”的Word。",
        "请生成视频介绍哈希表。",
    ],
)
def test_uncertain_or_non_agent_work_stays_with_existing_provider(text):
    assert new_work_intent(text) is None


@pytest.mark.parametrize(
    "changes",
    [
        {"reply_to": "prior-event"},
        {"kind": EventKind.QUOTE},
        {"kind": EventKind.FORWARDED},
        {"attachments": [{"kind": "file", "name": "x.pdf"}]},
    ],
)
def test_event_boundaries_are_not_fast_admitted(changes):
    text = "请生成一份合成报告并交付Markdown。"
    stamp = datetime.fromisoformat("2026-11-14T09:00:00+08:00")
    value = event(text).model_copy(update=changes)
    assert value.occurred_at == stamp
    assert ConversationCoordinator._fast_new_work(None, value, context()) is None


@pytest.mark.parametrize(
    "changes",
    [
        {"role": "observer"},
        {"role": "maintainer"},
        {"read_only": True},
        {"quoted_work_id": "work"},
        {"_quoted_work_id": "work"},
    ],
)
def test_trusted_context_boundaries_are_not_fast_admitted(changes):
    assert (
        ConversationCoordinator._fast_new_work(
            None, event("请生成一份合成报告并交付Markdown。"), context(**changes)
        )
        is None
    )


def test_explicit_verified_agent_address_is_direct_and_long_goals_never_truncate():
    text = "机器人，请生成一份合成报告并交付Markdown。"
    candidate = ConversationCoordinator._fast_new_work(None, event(text), context())
    assert candidate and candidate.directness == "direct" and candidate.goal == text
    assert new_work_intent("请生成合成Markdown。" + "约束" * 3000) is None


def test_all_264_frozen_cases_never_create_an_unexpected_work():
    cases = json.loads((Path(__file__).parents[1] / "fixtures/interaction_cases.json").read_text())[
        "cases"
    ]
    assert len(cases) == 264
    for case in cases:
        ctx = case["context"]
        value = event(case["text"]).model_copy(
            update={
                "id": case["id"],
                "actor_id": ctx["requester_member_id"],
                "kind": EventKind(case.get("event_kind", "text")),
                "reply_to": ctx.get("model_event", {}).get("reply_to"),
                "attachments": ctx.get("model_event", {}).get("attachments", []),
            }
        )
        candidate = ConversationCoordinator._fast_new_work(None, value, ctx)
        if candidate is not None:
            assert "start_work" in case["expected_kind"], case["id"]
