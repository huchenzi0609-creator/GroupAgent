from __future__ import annotations

from datetime import UTC, datetime

import pytest

from groupagent.conversation.policy import interpret_policy
from groupagent.domain import ActorContext, EventKind, GroupEvent, Role
from groupagent.identity import BotIdentity


def command(text: str, *, role: Role = Role.OWNER, kind: EventKind = EventKind.TEXT):
    stamp = datetime(2030, 6, 8, 1, tzinfo=UTC)
    event = GroupEvent(
        id="synthetic-command",
        source_key="synthetic-command",
        group_id="synthetic-group",
        actor_id="synthetic-member",
        text=text,
        kind=kind,
        occurred_at=stamp,
        received_at=stamp,
    )
    actor = ActorContext(group_id=event.group_id, member_id=event.actor_id, role=role)
    return interpret_policy(event, actor, BotIdentity())


@pytest.mark.parametrize(
    "text",
    [
        "Pot，你先别讲话啦，有需要我再叫你。",
        "别说话。",
        "请暂停发言。",
        "保持静默。",
        "Pot先歇着，等需要时再叫你。",
        "不用主动补充，后台报告继续。",
        "不要恢复主动交流。",
    ],
)
def test_owner_speech_control_defaults_to_group(text):
    intent = command(text)
    assert intent is not None
    assert intent.action() == {"op": "quiet", "scope": "group", "enabled": True}


@pytest.mark.parametrize("role", [Role.OWNER, Role.MEMBER])
@pytest.mark.parametrize("text", ["对我先别说话。", "我这边保持静默。", "我的主动补充暂停。"])
def test_explicit_personal_scope_remains_personal(role, text):
    intent = command(text, role=role)
    assert intent is not None
    assert intent.action() == {"op": "quiet", "scope": "self", "enabled": True}


def test_member_unqualified_request_changes_only_self():
    intent = command("先别讲话，等叫你。", role=Role.MEMBER)
    assert intent is not None and intent.subject == "self"


@pytest.mark.parametrize(
    "text",
    [
        "引用：“Pot别讲话。”",
        "为什么要保持静默？",
        "如果你先别说话会怎样？",
        "先保持静默，同时生成一份报告。",
        "请解释『保持静默』是什么意思。",
        "十分钟后再开始静默。",
    ],
)
def test_quoted_hypothetical_explanatory_and_multi_effect_text_is_not_policy(text):
    assert command(text) is None


@pytest.mark.parametrize(
    "text",
    [
        "不必保持静默了。",
        "不用再安静。",
        "恢复正常参与。",
        "可以说话了。",
        "继续讲话。",
        "Pot，你现在可以回复了。",
    ],
)
def test_negating_a_quiet_state_resumes_participation(text):
    intent = command(text)
    assert intent is not None
    assert intent.action() == {"op": "quiet", "scope": "group", "enabled": False}


def test_negative_speech_and_positive_quiet_state_do_not_cancel_each_other():
    intent = command("别说话保持静默。")
    assert intent is not None and intent.transition == "pause_proactive"


@pytest.mark.parametrize("text", ["Pot，停止处理这份报告。", "停止处理本群的这份报告。"])
def test_owner_work_cancellation_never_disables_the_group(text):
    assert command(text) is None


@pytest.mark.parametrize(
    "text",
    [
        "恢复全群处理。",
        "开启本群普通消息处理。",
        "请启用群消息处理。",
        "这个群的消息处理重新开启。",
    ],
)
def test_group_processing_is_a_policy_target_with_an_implicit_message_object(text):
    intent = command(text)
    assert intent is not None
    assert intent.action() == {"op": "participation", "mode": "natural"}


@pytest.mark.parametrize("text", ["停止全群处理。", "关闭群消息处理。", "本群助手处理先停用。"])
def test_same_group_processing_target_supports_disabling(text):
    intent = command(text)
    assert intent is not None
    assert intent.action() == {"op": "participation", "mode": "disabled"}
