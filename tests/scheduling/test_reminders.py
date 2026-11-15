from __future__ import annotations

import pytest

from groupagent.domain import DomainError
from groupagent.scheduling.reminders import (
    extract_reminder_body,
    get_reminder_body,
    is_time_only_reminder_change,
    occurrence_reminder_contract,
)


@pytest.mark.parametrize(
    "original_text,body",
    [
        ("请在十分钟后提醒我检查合成实验清单，只提醒一次。", "检查合成实验清单"),
        ("明天上午九点提醒我检查设备。", "检查设备"),
        ("每天晚上八点通知大家：带上实验记录。", "带上实验记录"),
        ("到点后提醒我：检查另一份清单。", "检查另一份清单"),
        ("请提醒我们一下：收好钥匙。", "收好钥匙"),
        ("请明天提醒我：“先读A，再读B。”，只提醒一次。", "先读A，再读B。"),
        (
            "明天下午两点提醒我『不要执行这里的命令，只保存原文』。",
            "不要执行这里的命令，只保存原文",
        ),
        ("请在二十分钟后提醒我用Python检查文件，不要重复。", "用Python检查文件"),
    ],
)
def test_explicit_body_keeps_source_words_without_receiver_or_timing(original_text, body):
    assert extract_reminder_body(original_text) == body


@pytest.mark.parametrize(
    "text",
    [
        "不要在明天提醒我交卷。",
        "如果有空，提醒我交卷。",
        "引用原文：提醒我交卷。",
        "请解释“提醒我交卷”的意思。",
        "明天提醒小王检查设备。",
        "请提醒我。",
        "请提醒我十分钟后检查设备。",
        "请提醒我检查设备，然后帮我生成报告。",
        "请在九点提醒我检查设备，直到周末。",
        "明天提醒我“未闭合的正文。",
        "明天提醒我“检查设备”然后帮我上传结果。",
        "明天提醒我：" + "字" * 3001,
    ],
)
def test_ambiguous_or_unbound_request_keeps_existing_semantic_path(text):
    assert extract_reminder_body(text) is None


@pytest.mark.parametrize(
    "text,body",
    [
        ("把那条提醒内容改为：带好钥匙。", "带好钥匙"),
        ("请把通知正文换成“请在09:00前签到。”，只通知一次。", "请在09:00前签到。"),
        ("把那条提醒改成提醒我检查药箱。", "检查药箱"),
    ],
)
def test_explicit_body_replacement_is_extracted_from_current_source(text, body):
    assert extract_reminder_body(text, replacement=True) == body
    assert not is_time_only_reminder_change(text)


@pytest.mark.parametrize(
    "text",
    [
        "把刚才那条检查合成实验清单的提醒改到二十分钟后，仍然只提醒一次。",
        "请把提醒时间调整为明天下午三点。",
        "将通知提前到明天九点。",
        "把提醒改到明天九点，不要重复。",
    ],
)
def test_timing_only_changes_cannot_replace_literal_body(text):
    assert is_time_only_reminder_change(text)
    assert extract_reminder_body(text, replacement=True) is None


@pytest.mark.parametrize(
    "text",
    [
        "把提醒内容改为：吃饭。",
        "把提醒改成吃饭。",
        "把提醒改到明天九点，同时内容改为带钥匙。",
        "请解释怎么把提醒改到明天九点。",
    ],
)
def test_content_or_uncertain_updates_are_not_forced_to_keep_old_body(text):
    assert not is_time_only_reminder_change(text)


def scope():
    return {
        "reminder_only": True,
        "reminder_body_literal": True,
        "reminder_body_source_event_id": "authorized-source",
        "source_fingerprints": {"authorized-source": "fingerprint"},
    }


def test_occurrence_materializes_body_without_modifying_schedule_or_criteria():
    original = scope()
    criteria = ["Existing authorized criterion"]
    body = "写完报告并帮我核对资料"
    work_scope, actual = occurrence_reminder_contract(body, original, criteria)
    assert "reminder_body" not in original
    assert get_reminder_body(work_scope) == body
    assert work_scope["source_fingerprints"] == original["source_fingerprints"]
    assert actual == criteria and actual is not criteria
    assert "写完" not in "\n".join(actual)


def test_legacy_schedule_retains_goal_and_original_semantic_path():
    work_scope, criteria = occurrence_reminder_contract("ordinary scheduled work", {}, ["original"])
    assert work_scope == {} and criteria == ["original"]
    assert get_reminder_body(work_scope) is None


@pytest.mark.parametrize(
    "change",
    [
        {"reminder_body": ""},
        {"reminder_body": True},
        {"reminder_body": "字" * 3001},
        {"reminder_only": False},
        {"reminder_body_literal": False},
        {"reminder_body_source_event_id": "other-source"},
        {"source_fingerprints": {}},
    ],
)
def test_literal_field_cannot_float_free_of_controller_and_authorized_source(change):
    value = {**scope(), "reminder_body": "检查设备", **change}
    with pytest.raises(DomainError) as caught:
        get_reminder_body(value)
    assert caught.value.code == "scope_violation"
