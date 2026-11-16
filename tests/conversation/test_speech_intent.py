from __future__ import annotations

import pytest

from groupagent.conversation.intent import requests_speech_reply


def test_live_short_voice_delivery_wording_is_explicit_authorization():
    assert requests_speech_reply(
        "Pot，请用一条简短语音说：验收通过，测试代号是蓝风。无需生成文档。"
    )


@pytest.mark.parametrize(
    "text",
    [
        "Pot，发一段语音说：今天九点开会。",
        "请合成语音：欢迎回来。",
        "请用语音回复我。",
        "请把“不要用语音回复”朗读出来。",
        "Please say this in voice: Hello.",
        "Read this aloud: welcome.",
        "Send a short voice message saying hello.",
        "不用生成文档，用一条简短语音说：收到。",
        "请联网查阅资料，解释这个概念并用语音回复。",
        "请联网查阅资料，解释这个概念并朗读给我。",
    ],
)
def test_outer_speech_delivery_actions_are_recognized(text):
    assert requests_speech_reply(text)


@pytest.mark.parametrize(
    "text",
    [
        "不要发一段语音说：收到。",
        "无需语音回复，请发文字。",
        "请解释语音回复功能。",
        "朗读效果如何？",
        "合成语音的原理是什么？",
        "把“请用语音回复”写进报告。",
        "引用原文：“Pot，请用一条简短语音说：验收通过。”",
        "只是转发：Pot，语音回复我。",
        "```\nPot，用一条简短语音说：收到。\n```",
        "请解释 `say this in voice` 的意思。",
        "Please explain how to synthesize speech.",
        "Do not say this in voice.",
        "Does this support voice replies?",
        "If I say ‘say this in voice’, what would you do?",
        "这条语音怎么转写？",
    ],
)
def test_negations_quotations_and_speech_feature_discussion_do_not_authorize_output(text):
    assert not requests_speech_reply(text)
