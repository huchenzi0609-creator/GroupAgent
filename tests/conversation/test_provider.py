from __future__ import annotations

import asyncio
import json
from datetime import datetime
from types import SimpleNamespace

import pytest

from groupagent.conversation.provider import ModelDecisionProvider
from groupagent.conversation.timeparse import normalize_time, shift_timing
from groupagent.domain import DomainError, EventKind, GroupEvent
from groupagent.execution.models import ModelReply, TokenUsage


class FixtureModel:
    name = "offline-structured-decision-fixture"

    def __init__(self, *responses):
        self.responses, self.requests = list(responses), []

    async def complete(self, request):
        self.requests.append(request)
        result = self.responses.pop(0)
        if isinstance(result, BaseException):
            raise result
        if isinstance(result, ModelReply):
            return result
        return ModelReply(
            text=result if isinstance(result, str) else json.dumps(result, ensure_ascii=False),
            usage=TokenUsage(input_tokens=100, output_tokens=50),
        )


def decision(kind="reply", **values):
    return {
        "kind": kind,
        "reason_code": "synthetic_contract",
        "directness": "direct",
        "confidence": 0.95,
        **({"response": "收到。"} if kind in {"reply", "clarify"} else {}),
        **values,
    }


@pytest.fixture
def environment():
    stamp = datetime.fromisoformat("2026-11-14T09:00:00+08:00")
    event = GroupEvent(
        id="event-current",
        source_key="synthetic:event",
        group_id="synthetic-group",
        actor_id="self",
        kind=EventKind.TEXT,
        text="请继续整理已有报告。",
        occurred_at=stamp,
        received_at=stamp,
    )
    context = {
        "now": stamp.isoformat(),
        "group_time_zone": "Asia/Shanghai",
        "role": "member",
        "read_only": False,
        "requester_member_id": "self",
        "works": [
            {
                "id": "work-report",
                "revision": 4,
                "requester_id": "self",
                "state": "running",
                "goal": "整理课程报告",
            }
        ],
        "tasks": [{"id": "task-notes", "revision": 3, "owner_id": "self", "title": "核对访谈笔记"}],
        "schedules": [
            {
                "id": "schedule-notes",
                "revision": 2,
                "creator_id": "self",
                "timing": {"kind": "daily", "local_time": "20:00"},
            }
        ],
        "members": [
            {"member_id": "self", "name": "陈念", "revision": 1, "membership_state": "present"},
            {"member_id": "other", "name": "唐禾", "revision": 1, "membership_state": "present"},
        ],
        "files": [
            {
                "version_id": "version-table",
                "file_id": "file-table",
                "format": "png",
                "name": "实验表格.png",
                "available": True,
            }
        ],
        "topics": [],
        "memory": [],
        "recent_messages": [],
    }
    return SimpleNamespace(event=event, context=context)


@pytest.mark.parametrize(
    "first",
    [
        decision("start_work", goal="制作报告", action={"op": "claim_task", "title": "制作报告"}),
        decision("reply", target_work_id="work-report", expected_revision=4, control="pause"),
        decision(
            "reply",
            action={
                "op": "update_task",
                "task_id": "work-report",
                "expected_revision": 4,
                "progress": "修改报告",
            },
        ),
        decision(
            "reply",
            action={
                "op": "control_schedule",
                "schedule_id": "work-report",
                "expected_revision": 4,
                "command": "pause",
            },
        ),
    ],
)
async def test_wrong_work_action_namespace_or_condition_is_repaired_before_return(
    environment, first
):
    correct = decision(
        "update_work",
        goal="整理课程报告并补充局限",
        target_work_id="work-report",
        expected_revision=4,
        action=None,
    )
    model = FixtureModel(first, correct)
    provider = ModelDecisionProvider(model)
    result = await provider.decide(environment.event, environment.context)
    assert result.kind == "update_work" and result.action is None
    assert result.target_work_id == "work-report" and len(model.requests) == 2
    assert all(not request.tools for request in model.requests)
    assert provider.diagnostics[0]["code"] == "provider_output_invalid"
    assert "尚无任何业务写" in model.requests[1].messages[-1].content


async def test_two_repairs_are_bounded_and_diagnostics_do_not_store_raw_response(environment):
    secret = "sk-synthetic_private_token_123456789012345678"
    private = "private person and payment details"
    invalid = {**decision("reply"), private: secret}
    model = FixtureModel("not valid JSON", invalid, decision("observe", directness="none"))
    provider = ModelDecisionProvider(model)
    result = await provider.decide(environment.event, environment.context)
    assert result.kind == "observe" and len(model.requests) == 3
    assert len(provider.diagnostics) == 2
    serialized = json.dumps(provider.diagnostics)
    assert (
        secret not in serialized and private not in serialized and "response_sha256" in serialized
    )
    assert secret not in model.requests[-1].messages[-2].content


async def test_schema_failure_stops_after_total_three_calls(environment):
    model = FixtureModel("{}", "{}", "{}", decision("observe"))
    provider = ModelDecisionProvider(model)
    with pytest.raises(DomainError) as error:
        await provider.decide(environment.event, environment.context)
    assert error.value.code == "provider_output_invalid" and not error.value.retryable
    assert len(model.requests) == len(provider.diagnostics) == 3


def schedule_candidate(timing, **changes):
    return decision(
        action={
            "op": "create_schedule",
            "title": "合成提醒",
            "goal": "提醒我休息",
            "timing": timing,
        },
        **changes,
    )


@pytest.mark.parametrize("max_retries", [0, 1, 2])
@pytest.mark.parametrize(
    "text,timing,error_code",
    [
        (
            "每隔四小时提醒我休息，持续到今天晚上。",
            {
                "kind": "every",
                "interval_seconds": 14400,
                "start_at": "2026-11-14T13:00:00+08:00",
            },
            "use_verified_time_resolution",
        ),
        (
            "明天下午提醒我复核。",
            {"kind": "at", "at": "2026-11-15T18:00:00+08:00"},
            "time_requires_clarification",
        ),
    ],
)
async def test_only_exhausted_time_mismatches_become_safe_clarification(
    environment, max_retries, text, timing, error_code
):
    candidate = schedule_candidate(timing)
    model = FixtureModel(*([candidate] * (max_retries + 1)), schedule_candidate(timing))
    provider = ModelDecisionProvider(model, max_retries=max_retries)
    result = await provider.decide(
        environment.event.model_copy(update={"text": text}), environment.context
    )
    assert result.kind == "clarify" and result.reason_code == "schedule_time_needs_confirmation"
    assert result.action is None and result.goal is None and result.target_work_id is None
    assert "时间" in result.response and "确认" in result.response
    assert "18:00" not in result.response and "13:00" not in result.response
    assert len(model.requests) == len(provider.diagnostics) == max_retries + 1
    assert all(not request.tools for request in model.requests)
    assert all(
        record["issues"] == [{"path": "action.timing", "code": error_code}]
        for record in provider.diagnostics
    )
    assert provider.diagnostics[-1]["fallback"] == "schedule_time_clarification"


async def test_valid_time_repair_finishes_before_clarification_fallback(environment):
    bad = schedule_candidate(
        {"kind": "every", "interval_seconds": 14400, "start_at": "2026-11-14T13:00:00+08:00"}
    )
    good = schedule_candidate({"kind": "every", "interval_seconds": 14400})
    model = FixtureModel(bad, bad, good)
    provider = ModelDecisionProvider(model)
    result = await provider.decide(
        environment.event.model_copy(update={"text": "每隔四小时提醒我休息。"}),
        environment.context,
    )
    assert result.kind == "reply" and result.action["timing"]["interval_seconds"] == 14400
    assert len(model.requests) == 3 and len(provider.diagnostics) == 2
    assert all("fallback" not in record for record in provider.diagnostics)


@pytest.mark.parametrize(
    "first",
    [
        "{}",
        schedule_candidate({"kind": "every", "interval_seconds": 14400}, confidence=0.1),
        schedule_candidate({"type": "every", "interval_seconds": 14400}),
        schedule_candidate({"kind": "every", "interval_seconds": 60}),
        decision(action={"op": "forget", "task_ids": ["another-persons-task"]}),
    ],
)
async def test_mixed_schema_or_permission_failures_cannot_use_time_fallback(environment, first):
    bad_time = schedule_candidate(
        {"kind": "every", "interval_seconds": 14400, "start_at": "2026-11-14T13:00:00+08:00"}
    )
    model = FixtureModel(first, bad_time, bad_time)
    provider = ModelDecisionProvider(model)
    with pytest.raises(DomainError) as error:
        await provider.decide(
            environment.event.model_copy(update={"text": "每隔四小时提醒我休息。"}),
            environment.context,
        )
    assert error.value.code == "provider_output_invalid" and len(model.requests) == 3
    assert all("fallback" not in record for record in provider.diagnostics)


async def test_transport_failure_cannot_be_hidden_by_time_clarification(environment, monkeypatch):
    async def no_delay(seconds):
        return None

    monkeypatch.setattr("groupagent.conversation.provider.asyncio.sleep", no_delay)
    bad_time = schedule_candidate(
        {"kind": "every", "interval_seconds": 14400, "start_at": "2026-11-14T13:00:00+08:00"}
    )
    model = FixtureModel(
        DomainError("provider_timeout", "fixture", retryable=True), bad_time, bad_time
    )
    provider = ModelDecisionProvider(model)
    with pytest.raises(DomainError) as error:
        await provider.decide(
            environment.event.model_copy(update={"text": "每隔四小时提醒我休息。"}),
            environment.context,
        )
    assert error.value.code == "provider_output_invalid" and len(model.requests) == 3
    assert provider.diagnostics[0]["provider_error"]
    assert all("fallback" not in record for record in provider.diagnostics)


async def test_unresolved_quiet_until_is_not_the_schedule_timing_fallback(environment):
    candidate = decision(
        action={
            "op": "quiet",
            "scope": "self",
            "enabled": True,
            "until": "2026-11-15T18:00:00+08:00",
        }
    )
    provider = ModelDecisionProvider(FixtureModel(candidate, candidate, candidate))
    with pytest.raises(DomainError) as error:
        await provider.decide(
            environment.event.model_copy(update={"text": "安静到明天下午。"}), environment.context
        )
    assert error.value.code == "provider_output_invalid"
    assert all(
        record["issues"] == [{"path": "action.until", "code": "time_requires_clarification"}]
        for record in provider.diagnostics
    )


@pytest.mark.parametrize("limit", [-1, 3, True, 2.5])
def test_attempt_budget_cannot_be_unbounded(limit):
    with pytest.raises(ValueError):
        ModelDecisionProvider(FixtureModel(), max_retries=limit)


async def test_provider_failure_and_cancellation_do_not_trigger_schema_writes(
    environment, monkeypatch
):
    model = FixtureModel(DomainError("budget_exhausted", "No fixture budget"), decision("observe"))
    with pytest.raises(DomainError) as error:
        await ModelDecisionProvider(model).decide(environment.event, environment.context)
    assert error.value.code == "budget_exhausted" and len(model.requests) == 1
    model = FixtureModel(asyncio.CancelledError(), decision("observe"))
    with pytest.raises(asyncio.CancelledError):
        await ModelDecisionProvider(model).decide(environment.event, environment.context)
    assert len(model.requests) == 1

    async def no_delay(seconds):
        return None

    monkeypatch.setattr("groupagent.conversation.provider.asyncio.sleep", no_delay)
    model = FixtureModel(
        DomainError("provider_timeout", "synthetic", retryable=True), "{}", decision("observe")
    )
    assert (
        await ModelDecisionProvider(model).decide(environment.event, environment.context)
    ).kind == "observe"
    assert len(model.requests) == 3


async def test_natural_self_claim_needs_no_prefix_or_attachments(environment):
    event = environment.event.model_copy(update={"text": "访谈摘要我来负责，不用你代写。"})
    model = FixtureModel(
        decision(action={"op": "claim_task", "title": "访谈摘要"}, directness="open")
    )
    result = await ModelDecisionProvider(model).decide(event, environment.context)
    assert result.action["op"] == "claim_task" and result.input_event_ids == []
    system = model.requests[0].messages[0].content
    assert "无需/组长等前缀" in system and "只登记真人任务" in system


async def test_available_image_and_explicit_source_references_are_in_payload(environment):
    event = environment.event.model_copy(
        update={
            "text": "分析这张表格截图。",
            "attachments": [{"kind": "image", "handle": {"message_id": "synthetic-message"}}],
        }
    )
    context = {
        **environment.context,
        "_private": "never-send-this",
        "model_event": {
            "id": event.id,
            "actor_id": "self",
            "text": event.text,
            "attachments": event.attachments,
            "occurred_at": "1999-01-01T00:00:00Z",
        },
    }
    model = FixtureModel(
        decision(
            "start_work",
            goal="用真实图片工具分析表格",
            input_versions=["version-table"],
            input_event_ids=[event.id],
            outputs=[{"format": "text"}],
        )
    )
    result = await ModelDecisionProvider(model).decide(event, context)
    assert result.kind == "start_work"
    payload = json.loads(model.requests[0].messages[1].content)
    assert payload["event"]["occurred_at"] == event.occurred_at.isoformat()
    assert payload["context"]["reference_catalog"]["attachment_event_ids"] == [event.id]
    assert "_private" not in payload["context"]
    assert "图片理解、真实像素输入" in model.requests[0].messages[0].content
    assert "独立插画/文生图服务已由用户排除" in model.requests[0].messages[0].content


@pytest.mark.parametrize(
    "values",
    [
        {"input_versions": ["unavailable-version"]},
        {"input_event_ids": ["unselected-other-message"]},
        {"outputs": [{"format": "mp4"}]},
        {"confidence": 0.6},
    ],
)
async def test_invalid_work_capabilities_or_sources_fail_closed(environment, values):
    model = FixtureModel(decision("start_work", goal="Synthetic work", **values))
    with pytest.raises(DomainError) as error:
        await ModelDecisionProvider(model, max_retries=0).decide(
            environment.event, environment.context
        )
    assert error.value.code == "provider_output_invalid"


async def test_owner_task_batch_checks_every_namespace_and_assignee(environment):
    context = {**environment.context, "role": "owner"}
    first = decision(
        action={
            "op": "task_batch",
            "actions": [
                {"op": "claim_task", "title": "访谈摘要"},
                {
                    "op": "update_task",
                    "task_id": "work-report",
                    "expected_revision": 4,
                    "progress": "Wrong namespace",
                },
            ],
        }
    )
    second = decision(
        action={
            "op": "task_batch",
            "actions": [
                {"op": "claim_task", "title": "访谈摘要"},
                {
                    "op": "assign_task",
                    "title": "现场记录",
                    "member_id": "other",
                    "member_name": "唐禾",
                },
            ],
        }
    )
    model = FixtureModel(first, second)
    provider = ModelDecisionProvider(model)
    result = await provider.decide(environment.event, context)
    assert result.kind == "reply" and result.action["op"] == "task_batch"
    assert provider.diagnostics[0]["issues"][0]["path"].startswith("action.actions.1.")
    assert len(model.requests) == 2
    with pytest.raises(DomainError):
        await ModelDecisionProvider(FixtureModel(second), max_retries=0).decide(
            environment.event, environment.context
        )


async def test_time_schema_and_date_are_repaired_from_actual_occurred_at(environment):
    event = environment.event.model_copy(
        update={
            "text": "明晚八点提醒我复核。",
            "occurred_at": datetime.fromisoformat("2026-11-13T15:00:00+08:00"),
        }
    )
    wrong = decision(
        action={
            "op": "create_schedule",
            "title": "复核",
            "goal": "提醒复核",
            "timing": {"type": "at", "at": "2026-11-15T20:00:00+08:00"},
        }
    )
    correct = decision(
        action={
            "op": "create_schedule",
            "title": "复核",
            "goal": "提醒复核",
            "timing": {"kind": "at", "at": "2026-11-14T20:00:00+08:00"},
        }
    )
    model = FixtureModel(wrong, correct)
    result = await ModelDecisionProvider(model).decide(event, environment.context)
    assert result.action["timing"]["at"] == "2026-11-14T20:00:00+08:00"
    payload = json.loads(model.requests[0].messages[1].content)
    assert payload["context"]["time_resolution"]["instant"] == "2026-11-14T20:00:00+08:00"
    assert len(model.requests) == 2


async def test_missing_time_or_dst_fold_cannot_be_filled_by_model(environment):
    for text, stamp, zone, guessed in [
        (
            "周五提醒我交报告",
            "2026-11-14T09:00:00+08:00",
            "Asia/Shanghai",
            "2026-11-20T09:00:00+08:00",
        ),
        (
            "11月1日凌晨1点半提醒我",
            "2026-10-31T18:00:00-04:00",
            "America/New_York",
            "2026-11-01T01:30:00-04:00",
        ),
    ]:
        event = environment.event.model_copy(
            update={"text": text, "occurred_at": datetime.fromisoformat(stamp)}
        )
        context = {**environment.context, "now": stamp, "group_time_zone": zone}
        wrong = decision(
            action={
                "op": "create_schedule",
                "title": "检查",
                "goal": "提醒检查",
                "timing": {"kind": "at", "at": guessed},
            }
        )
        clarify = decision("clarify", response="请补充具体时刻或偏移。")
        model = FixtureModel(wrong, clarify)
        assert (await ModelDecisionProvider(model).decide(event, context)).kind == "clarify"
        assert len(model.requests) == 2


@pytest.mark.parametrize(
    "text,expected",
    [
        ("明天晚上八点提醒我", "2026-11-15T20:00:00+08:00"),
        ("明早七点半提醒我", "2026-11-15T07:30:00+08:00"),
        ("下周一上午九点提醒我", "2026-11-16T09:00:00+08:00"),
        ("本周日中午十二点提醒我", "2026-11-15T12:00:00+08:00"),
        ("两小时后提醒我", "2026-11-14T11:00:00+08:00"),
        ("接下来十分钟先安静", "2026-11-14T09:10:00+08:00"),
        ("2026年11月18日18点提交", "2026-11-18T18:00:00+08:00"),
        ("昨天晚上八点", "2026-11-13T20:00:00+08:00"),
    ],
)
def test_verified_relative_time_arithmetic_uses_source_timestamp(text, expected):
    value = normalize_time(text, datetime.fromisoformat("2026-11-14T09:00:00+08:00"))
    assert value.status == "resolved" and value.instant == expected


@pytest.mark.parametrize(
    "text,zone,expected",
    [
        ("每天晚八点提醒", "Asia/Shanghai", {"kind": "daily", "local_time": "20:00"}),
        (
            "每周五下午六点提醒",
            "Asia/Shanghai",
            {"kind": "weekly", "local_time": "18:00", "weekdays": [4]},
        ),
        ("每隔四小时提醒", "Asia/Shanghai", {"kind": "every", "interval_seconds": 14400}),
        (
            "按纽约时间11月1日01:30-05:00提醒",
            "America/New_York",
            {"kind": "at", "at": "2026-11-01T01:30:00-05:00"},
        ),
    ],
)
def test_recurring_and_explicit_offset_time_contract(text, zone, expected):
    value = normalize_time(text, datetime.fromisoformat("2026-10-31T18:00:00-04:00"), zone)
    assert value.status == "resolved" and value.timing == expected


def test_nonexistent_dst_and_ambiguous_midnight_are_not_silently_resolved():
    value = normalize_time(
        "3月8日凌晨2点半提醒",
        datetime.fromisoformat("2026-03-07T18:00:00-05:00"),
        "America/New_York",
    )
    assert value.reason == "nonexistent_local_time"
    assert (
        normalize_time("明晚十二点", datetime.fromisoformat("2026-11-14T09:00:00+08:00")).status
        == "ambiguous"
    )
    assert shift_timing({"kind": "daily", "local_time": "20:00"}, 1800) == {
        "kind": "daily",
        "local_time": "20:30:00",
    }
    assert shift_timing({"kind": "daily", "local_time": "23:45"}, 1800) is None


async def test_completely_unspecified_reminder_time_cannot_be_invented(environment):
    event = environment.event.model_copy(update={"text": "提醒我交报告。"})
    invented = decision(
        action={
            "op": "create_schedule",
            "title": "交报告",
            "goal": "提醒交报告",
            "timing": {"kind": "daily", "local_time": "09:00"},
        }
    )
    model = FixtureModel(invented, decision("clarify", response="什么时候提醒你？"))
    provider = ModelDecisionProvider(model)
    assert (await provider.decide(event, environment.context)).kind == "clarify"
    assert len(model.requests) == 2
    assert provider.diagnostics[0]["issues"][0]["code"] == "time_requires_clarification"


@pytest.mark.parametrize(
    "text,timing",
    [
        ("把我每天检查文献的提醒改成晚上九点。", {"kind": "daily", "local_time": "21:00"}),
        (
            "我的文献提醒从每天改成每周一和周四。",
            {"kind": "weekly", "local_time": "20:00", "weekdays": [0, 3]},
        ),
        ("提醒时间往后挪半小时，原规则是20点。", {"kind": "daily", "local_time": "20:30"}),
    ],
)
async def test_changes_reuse_verified_existing_rule_clock_or_displacement(
    environment, text, timing
):
    event = environment.event.model_copy(update={"text": text})
    model = FixtureModel(
        decision(
            action={
                "op": "change_schedule",
                "schedule_id": "schedule-notes",
                "expected_revision": 2,
                "timing": timing,
            }
        )
    )
    result = await ModelDecisionProvider(model).decide(event, environment.context)
    assert result.action["op"] == "change_schedule" and len(model.requests) == 1


async def test_stale_or_missing_revision_and_readonly_role_are_not_guessed(environment):
    bad = decision(
        "update_work", goal="修订报告", target_work_id="work-report", expected_revision=2
    )
    for context in (
        environment.context,
        {**environment.context, "role": "observer", "read_only": True},
    ):
        model = FixtureModel(bad, decision("clarify", response="请提供当前可操作的工作对象。"))
        assert (
            await ModelDecisionProvider(model).decide(environment.event, context)
        ).action is None
        assert len(model.requests) == 2


@pytest.mark.parametrize("directness", ["other", "none"])
async def test_owner_explicit_human_assignment_does_not_need_to_address_bot(
    environment, directness
):
    context = {**environment.context, "role": "owner"}
    candidate = decision(
        action={
            "op": "assign_task",
            "title": "答辩彩排主持",
            "member_id": "other",
            "member_name": "唐禾",
        },
        directness=directness,
    )
    model = FixtureModel(candidate)
    result = await ModelDecisionProvider(model).decide(
        environment.event.model_copy(update={"text": "唐禾负责答辩彩排主持。"}), context
    )
    assert result.action["op"] == "assign_task" and len(model.requests) == 1
    unaddressed = await ModelDecisionProvider(FixtureModel(candidate), max_retries=0).decide(
        environment.event, environment.context
    )
    assert unaddressed.kind == "observe" and unaddressed.action is None


@pytest.mark.parametrize(
    "text,note,op",
    [
        ("我负责实验方法那一节，周一前交。", "周一前", "claim_task"),
        ("我这项任务改到周二下午交。", "周二下午", "update_task"),
        ("截止日我申请延到下周三，改的是自己的文献任务。", "下周三", "update_task"),
    ],
)
async def test_imprecise_human_deadline_keeps_verbatim_note_without_invented_time(
    environment, text, note, op
):
    event = environment.event.model_copy(update={"text": text})
    action = {"op": op, "due_at": None, "due_note": note}
    if op == "claim_task":
        action["title"] = "实验方法"
    else:
        action.update(task_id="task-notes", expected_revision=3)
    model = FixtureModel(decision(action=action, directness="open"))
    result = await ModelDecisionProvider(model).decide(event, environment.context)
    assert result.action["due_at"] is None and result.action["due_note"] == note
    assert len(model.requests) == 1


async def test_made_up_deadline_note_cannot_replace_missing_source_date(environment):
    candidate = decision(action={"op": "claim_task", "title": "整理资料", "due_note": "下周一"})
    with pytest.raises(DomainError):
        await ModelDecisionProvider(FixtureModel(candidate), max_retries=0).decide(
            environment.event, environment.context
        )


def test_multiple_unqualified_weekdays_form_bounded_weekly_rule():
    value = normalize_time(
        "周一和周四晚上八点提醒我读论文。", datetime.fromisoformat("2026-11-14T09:00:00+08:00")
    )
    assert value.timing == {"kind": "weekly", "local_time": "20:00", "weekdays": [0, 3]}
    specific = normalize_time(
        "下周一和周四晚上八点提醒。", datetime.fromisoformat("2026-11-14T09:00:00+08:00")
    )
    assert specific.status == "ambiguous"


async def test_verified_quoted_work_reference_and_actual_tool_requirement_are_preserved(
    environment,
):
    event = environment.event.model_copy(
        update={"text": "用Python实际算出前100个整数的平方和，给我文件。"}
    )
    context = {**environment.context, "quoted_work_id": "work-report"}
    model = FixtureModel(
        decision(
            "update_work",
            goal=event.text,
            target_work_id="work-report",
            expected_revision=4,
            outputs=[{"format": "md"}],
        )
    )
    result = await ModelDecisionProvider(model).decide(event, context)
    assert result.target_work_id == "work-report"
    system = model.requests[0].messages[0].content
    assert "context.quoted_work_id" in system and "历史做过相似计算不代表本次执行" in system


async def test_progress_patch_does_not_silently_clear_existing_deadline(environment):
    bad = decision(
        action={
            "op": "update_task",
            "task_id": "task-notes",
            "expected_revision": 3,
            "progress": "已核对作者",
            "due_at": None,
        }
    )
    good = decision(
        action={
            "op": "update_task",
            "task_id": "task-notes",
            "expected_revision": 3,
            "progress": "已核对作者",
        }
    )
    provider = ModelDecisionProvider(FixtureModel(bad, good))
    result = await provider.decide(
        environment.event.model_copy(update={"text": "文献进展更新为已核对作者。"}),
        environment.context,
    )
    assert "due_at" not in result.action
    assert provider.diagnostics == []


async def test_explicit_deadline_clear_uses_dedicated_flag(environment):
    event = environment.event.model_copy(update={"text": "取消我这项任务原来的截止日期。"})
    candidate = decision(
        action={
            "op": "update_task",
            "task_id": "task-notes",
            "expected_revision": 3,
            "clear_due": True,
            "due_at": None,
        }
    )
    result = await ModelDecisionProvider(FixtureModel(candidate)).decide(event, environment.context)
    assert result.action["clear_due"] is True and result.action["due_at"] is None


async def test_new_task_title_patch_uses_task_namespace_and_preserves_other_fields(environment):
    action = {
        "op": "update_task",
        "task_id": "task-notes",
        "expected_revision": 3,
        "title": "核对引文格式",
        "progress": None,
        "due_at": None,
    }
    result = await ModelDecisionProvider(FixtureModel(decision(action=action))).decide(
        environment.event.model_copy(update={"text": "把我的任务改名为核对引文格式。"}),
        environment.context,
    )
    assert (
        result.action["title"] == "核对引文格式"
        and "due_at" not in result.action
        and "progress" not in result.action
    )
    action["task_id"] = "work-report"
    with pytest.raises(DomainError):
        await ModelDecisionProvider(FixtureModel(decision(action=action)), max_retries=0).decide(
            environment.event, environment.context
        )


async def test_forget_task_uses_creator_permission_not_assignee_or_claimed_role(environment):
    context = {
        **environment.context,
        "tasks": [
            {**environment.context["tasks"][0], "created_by": "self", "source_event_ids": []}
        ],
    }
    action = {"op": "forget", "task_ids": ["task-notes"]}
    result = await ModelDecisionProvider(FixtureModel(decision(action=action))).decide(
        environment.event, context
    )
    assert result.action["task_ids"] == ["task-notes"]
    context["tasks"][0]["created_by"] = "another-creator"
    with pytest.raises(DomainError):
        await ModelDecisionProvider(FixtureModel(decision(action=action)), max_retries=0).decide(
            environment.event, context
        )
    assert (
        await ModelDecisionProvider(FixtureModel(decision(action=action))).decide(
            environment.event, {**context, "role": "owner"}
        )
    ).action["task_ids"] == ["task-notes"]
    with pytest.raises(DomainError):
        await ModelDecisionProvider(
            FixtureModel(decision(action={"op": "forget", "task_ids": ["work-report"]})),
            max_retries=0,
        ).decide(environment.event, context)


@pytest.mark.parametrize(
    "sources",
    [
        {"source_event_ids": ["legacy-source"]},
        {"source_refs": [{"event_id": "legacy-source", "actor_id": "self"}]},
    ],
)
async def test_compact_and_legacy_memory_source_references_remain_usable(environment, sources):
    context = {
        **environment.context,
        "memory": [
            {"id": "fact-one", "kind": "fact", "author_id": "self", "revision": 1, **sources}
        ],
    }
    candidate = decision(action={"op": "forget", "event_ids": ["legacy-source"]})
    result = await ModelDecisionProvider(FixtureModel(candidate)).decide(environment.event, context)
    assert result.action["event_ids"] == ["legacy-source"]
    with pytest.raises(DomainError):
        await ModelDecisionProvider(
            FixtureModel(decision(action={"op": "forget", "event_ids": ["not-in-context"]})),
            max_retries=0,
        ).decide(environment.event, context)


async def test_owner_assignee_clarification_is_not_suppressed_as_human_chat(environment):
    event = environment.event.model_copy(update={"text": "让小夏负责主持，但同名不能猜。"})
    result = await ModelDecisionProvider(
        FixtureModel(decision("clarify", response="请@你指的那位小夏。", directness="other"))
    ).decide(event, {**environment.context, "role": "owner"})
    assert result.kind == "clarify" and result.directness == "open" and result.action is None


async def test_unaddressed_invalid_human_assignment_does_not_become_an_intrusive_repair(
    environment,
):
    candidate = decision(
        action={
            "op": "assign_task",
            "title": "文献",
            "member_id": "unknown",
            "member_name": "不在名单的人",
        },
        directness="other",
    )
    model = FixtureModel(candidate)
    result = await ModelDecisionProvider(model).decide(environment.event, environment.context)
    assert result.kind == "observe" and result.action is None and len(model.requests) == 1


async def test_table_relations_become_cell_literals_without_changing_goal(environment):
    text = "制作PPT原生表格，数据为Alpha=12、Beta=24、Gamma=36。"
    proposal = decision(
        "start_work",
        goal=text,
        outputs=[{"format": "pptx", "expected_text": ["Alpha=12", "Beta=24", "Gamma=36"]}],
    )
    result = await ModelDecisionProvider(FixtureModel(proposal)).decide(
        environment.event.model_copy(update={"text": text}), environment.context
    )
    assert result.goal == text
    assert result.outputs[0]["expected_text"] == ["Alpha", "12", "Beta", "24", "Gamma", "36"]


async def test_explicitly_verbatim_table_adjacent_literal_is_not_weakened(environment):
    text = "请做一个PPT表格，旁边必须原样写字符串‘Alpha=12’。"
    proposal = decision(
        "start_work", goal=text, outputs=[{"format": "pptx", "expected_text": ["Alpha=12"]}]
    )
    result = await ModelDecisionProvider(FixtureModel(proposal)).decide(
        environment.event.model_copy(update={"text": text}), environment.context
    )
    assert result.outputs[0]["expected_text"] == ["Alpha=12"]


async def test_running_code_is_not_misclassified_as_table_rows(environment):
    text = "做一个PPT说明运行代码 Alpha=12 的过程。"
    proposal = decision(
        "start_work", goal=text, outputs=[{"format": "pptx", "expected_text": ["Alpha=12"]}]
    )
    result = await ModelDecisionProvider(FixtureModel(proposal)).decide(
        environment.event.model_copy(update={"text": text}), environment.context
    )
    assert result.outputs[0]["expected_text"] == ["Alpha=12"]
