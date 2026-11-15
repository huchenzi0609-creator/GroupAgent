"""Strict whole-request PPT admission; literals are data, never authority."""

from datetime import UTC, datetime

import pytest

from groupagent.conversation.intent import new_work_intent
from groupagent.conversation.service import ConversationCoordinator
from groupagent.domain import EventKind, GroupEvent

SPEC = (
    "请生成并发送恰好2页可编辑PPTX，主题《合成森林观察》。"
    "第1页标题“观察目标”，写“记录两类树木”；"
    "第2页标题“观察结果”，写“甲类4棵”“乙类7棵”。"
    "只用给定文字，不生成插画。"
)
VISIBLE = ["观察目标", "记录两类树木", "观察结果", "甲类4棵", "乙类7棵"]


def event(text):
    stamp = datetime(2026, 11, 15, tzinfo=UTC)
    return GroupEvent(
        id="enumerated-source",
        source_key="fixture:enumerated-source",
        group_id="synthetic",
        actor_id="member",
        kind=EventKind.TEXT,
        text=text,
        occurred_at=stamp,
        received_at=stamp,
    )


def context(**changes):
    return {
        "role": "member",
        "requester_member_id": "member",
        "agent_aliases": ["机器人"],
        "members": [],
        "works": [],
        "tasks": [],
        "schedules": [],
        "recent_messages": [],
        **changes,
    }


@pytest.mark.parametrize(
    "text,count,visible",
    [
        (SPEC, 2, VISIBLE),
        (SPEC.replace("生成", "做成", 1).replace("2页", "二页"), 2, VISIBLE),
        (
            '创建一页幻灯片：第一页标题"Overview"，正文为"Synthetic visible body"。',
            1,
            ["Overview", "Synthetic visible body"],
        ),
        (
            "制作3页PPT，主题《海岸观察》。"
            "第1页标题「起点」，内容是「记录日期」；"
            "第2页标题『测量』，写『温度12度』；"
            "第3页标题“整理”，写“汇总两次读数”。仅使用这些内容，无需插画，不联网。",
            3,
            ["起点", "记录日期", "测量", "温度12度", "整理", "汇总两次读数"],
        ),
    ],
)
def test_enumerated_request_preserves_all_visible_literals_and_full_goal(text, count, visible):
    intent = new_work_intent(text)
    assert intent is not None and intent.required_tool is None
    assert intent.outputs == ({"format": "pptx", "exact_units": count, "expected_text": visible},)
    decision = ConversationCoordinator._fast_new_work(None, event(text), context())
    assert decision and decision.kind == "start_work" and decision.goal == text
    assert decision.input_event_ids == decision.input_versions == []
    assert "合成森林观察" not in visible


@pytest.mark.parametrize(
    "text",
    [
        SPEC.replace("恰好2页", "恰好3页"),
        SPEC.replace("第2页", "第3页"),
        SPEC.replace("第2页", "第1页"),
        SPEC.replace("第1页", "第2页", 1),
        SPEC.replace("恰好2页", "至少2页"),
        SPEC.replace("恰好2页", "恰好0页"),
        SPEC.replace("恰好2页", "恰好101页"),
        SPEC.replace("可编辑PPTX", "PDF"),
        SPEC.replace("写“记录两类树木”", "写附件中的内容"),
        SPEC.replace("写“记录两类树木”", "写记录两类树木"),
        SPEC.replace("标题“观察目标”", "标题观察目标"),
        SPEC.replace("写“记录两类树木”", "写“ ”"),
        SPEC.replace("“甲类4棵”", "“甲类4棵"),
        SPEC + "第3页标题“新增”，写“超出声明页数”。",
        SPEC + "请发送邮件到外部账号。",
        SPEC + "同时取消所有提醒。",
        SPEC + "请把它分配给成员甲执行。",
        SPEC + "如果老师同意再开始。",
        SPEC + "字体和图片照之前那份。",
        SPEC + "先只讨论方案，不要制作。",
        "如果有空，" + SPEC,
        "明天提醒我：" + SPEC,
        "请把这份PPT修改为：" + SPEC,
        "引用原文：“" + SPEC + "”，不是执行请求。",
    ],
)
def test_unknown_or_ambiguous_outer_text_never_becomes_a_fast_goal(text):
    assert new_work_intent(text) is None


def test_quoted_control_words_are_visible_data_and_do_not_create_a_control_action():
    text = SPEC.replace("记录两类树木", "停止其他任务")
    intent = new_work_intent(text)
    assert intent is not None and "停止其他任务" in intent.outputs[0]["expected_text"]
    decision = ConversationCoordinator._fast_new_work(None, event(text), context())
    assert decision and decision.kind == "start_work" and decision.action is None


@pytest.mark.parametrize(
    "changes",
    [
        {"kind": EventKind.QUOTE},
        {"kind": EventKind.FORWARDED},
        {"reply_to": "existing-work-source"},
        {"attachments": [{"kind": "file"}]},
    ],
)
def test_enumerated_text_does_not_bypass_event_boundaries(changes):
    assert (
        ConversationCoordinator._fast_new_work(
            None, event(SPEC).model_copy(update=changes), context()
        )
        is None
    )


@pytest.mark.parametrize(
    "changes",
    [
        {"role": "observer"},
        {"role": "maintainer"},
        {"read_only": True},
        {"quoted_work_id": "existing-work"},
        {"_quoted_work_id": "existing-work"},
    ],
)
def test_enumerated_text_does_not_bypass_context_authority(changes):
    assert ConversationCoordinator._fast_new_work(None, event(SPEC), context(**changes)) is None
