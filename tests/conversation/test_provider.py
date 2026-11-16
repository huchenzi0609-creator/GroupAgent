"""Typed business validation plus the real graph's natural dialogue protocol.

The old Decision-JSON routing/repair loop is deliberately absent. Strict business
invariants remain tested directly and via normal graph tool observations.
"""

from __future__ import annotations

import asyncio
import json
from datetime import datetime
from types import SimpleNamespace

import pytest
from jsonschema import Draft202012Validator
from pydantic import ValidationError

from groupagent.conversation.provider import (
    ModelDecisionProvider,
    _InvalidCandidate,
    business_action_schema,
    decision_schema,
    reference_catalog,
    validate_candidate,
    work_action_schema,
    work_fact_tool_schema,
)
from groupagent.conversation.timeparse import normalize_time, shift_timing
from groupagent.domain import DomainError, EventKind, GroupEvent
from groupagent.execution.models import ModelReply, ModelToolCall, TokenUsage


def test_catalog_specific_input_schema_is_bounded_without_changing_static_compatibility():
    dynamic = decision_schema(
        {"attachment_event_ids": ["synthetic-b", "synthetic-a"], "file_versions": ["version-a"]}
    )
    fields = next(
        item["properties"]
        for item in dynamic["oneOf"]
        if item["properties"]["kind"]["const"] == "start_work"
    )
    assert fields["input_event_ids"]["items"] == {
        "type": "string",
        "enum": ["synthetic-a", "synthetic-b"],
    }
    assert fields["input_versions"]["items"]["enum"] == ["version-a"]
    static = decision_schema()
    fields = next(
        item["properties"]
        for item in static["oneOf"]
        if item["properties"]["kind"]["const"] == "start_work"
    )
    assert fields["input_event_ids"]["items"] == {"type": "string"}
    assert fields["input_versions"]["maxItems"] == 20


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


def test_multiple_unqualified_weekdays_form_bounded_weekly_rule():
    value = normalize_time(
        "周一和周四晚上八点提醒我读论文。", datetime.fromisoformat("2026-11-14T09:00:00+08:00")
    )
    assert value.timing == {"kind": "weekly", "local_time": "20:00", "weekdays": [0, 3]}
    specific = normalize_time(
        "下周一和周四晚上八点提醒。", datetime.fromisoformat("2026-11-14T09:00:00+08:00")
    )
    assert specific.status == "ambiguous"


def validate(value, environment, *, context=None, text=None):
    ctx = context or environment.context
    event = environment.event
    source = event.text if text is None else text
    ctx = {**ctx, "_source_text": source}
    return validate_candidate(
        json.dumps(value),
        ctx,
        reference_catalog(ctx, {"id": event.id, "text": source, "attachments": []}),
        normalize_time(source, event.occurred_at, "Asia/Shanghai"),
    )


@pytest.mark.parametrize(
    "value,path,code",
    [
        (decision("start_work"), "goal", "decision_goal_required"),
        (decision("command"), "action", "decision_action_required"),
        (
            decision(
                "start_work", goal="Synthetic", action={"op": "claim_task", "title": "Synthetic"}
            ),
            "action",
            "decision_action_kind",
        ),
        (
            decision("update_work", goal="Synthetic", target_work_id="work-report"),
            "expected_revision",
            "decision_revision_required",
        ),
        (
            decision("start_work", goal="Synthetic", work_selector={"basis": "focus"}),
            "work_selector",
            "decision_selector_kind",
        ),
    ],
)
def test_business_structural_errors_are_still_rejected(environment, value, path, code):
    with pytest.raises(ValidationError) as error:
        validate(value, environment)
    assert error.value.errors(include_input=False)[0]["type"] == code


@pytest.mark.parametrize(
    "value,code",
    [
        (
            decision("reply", outputs=[{"format": "docx"}]),
            "work_fields_require_start_or_update_work",
        ),
        (
            decision("start_work", goal="Synthetic", confidence=0.5),
            "write_requires_confidence_at_least_0_85",
        ),
        (
            decision("start_work", goal="Synthetic", input_event_ids=["guessed"]),
            "select_authorized_attachment_event",
        ),
        (
            decision("start_work", goal="Synthetic", input_versions=["guessed"]),
            "select_available_file_version",
        ),
        (
            decision(
                "update_work", goal="Synthetic", target_work_id="task-notes", expected_revision=3
            ),
            "select_id_from_works",
        ),
        (
            decision(
                "command",
                action={
                    "op": "update_task",
                    "task_id": "work-report",
                    "expected_revision": 4,
                    "progress": "x",
                },
            ),
            "select_id_from_tasks",
        ),
        (
            decision(
                "command",
                action={
                    "op": "control_schedule",
                    "schedule_id": "work-report",
                    "expected_revision": 4,
                    "command": "pause",
                },
            ),
            "select_id_from_schedules",
        ),
        (
            decision(
                "command",
                action={
                    "op": "assign_task",
                    "title": "Synthetic",
                    "member_id": "other",
                    "member_name": "唐禾",
                },
            ),
            "owner_and_unique_current_member_required",
        ),
        (
            decision("command", action={"op": "quiet", "scope": "group", "enabled": True}),
            "group_policy_requires_owner",
        ),
    ],
)
def test_business_namespace_provenance_and_permission_checks(environment, value, code):
    with pytest.raises(_InvalidCandidate) as error:
        validate(value, environment)
    assert error.value.issues[0]["code"] == code


@pytest.mark.parametrize("role", ["observer", "maintainer"])
def test_readonly_cannot_stage_business_write(environment, role):
    with pytest.raises(_InvalidCandidate) as error:
        validate(
            decision("start_work", goal="Synthetic"),
            environment,
            context={**environment.context, "role": role},
        )
    assert error.value.issues[0]["code"] == "read_only_actor_cannot_write"


def test_update_patch_does_not_clear_deadline_or_progress_without_explicit_change(environment):
    changed = validate(
        decision(
            "command",
            action={
                "op": "update_task",
                "task_id": "task-notes",
                "expected_revision": 3,
                "title": "核对引文格式",
                "due_at": None,
                "progress": None,
            },
        ),
        environment,
    )
    assert changed.action == {
        "op": "update_task",
        "task_id": "task-notes",
        "expected_revision": 3,
        "title": "核对引文格式",
    }
    clear = validate(
        decision(
            "command",
            action={
                "op": "update_task",
                "task_id": "task-notes",
                "expected_revision": 3,
                "clear_due": True,
                "due_at": None,
            },
        ),
        environment,
    )
    assert clear.action["clear_due"] and clear.action["due_at"] is None


def test_owner_batch_validates_every_child_and_does_not_leak_partial_action(environment):
    context = {**environment.context, "role": "owner"}
    actions = [
        {"op": "claim_task", "title": "Synthetic"},
        {
            "op": "update_task",
            "task_id": "work-report",
            "expected_revision": 4,
            "progress": "Wrong namespace",
        },
    ]
    with pytest.raises(_InvalidCandidate) as error:
        validate(
            decision("command", action={"op": "task_batch", "actions": actions}),
            environment,
            context=context,
        )
    assert error.value.issues[0]["path"].startswith("action.actions.1.")
    actions[1] = {
        "op": "assign_task",
        "title": "Synthetic",
        "member_id": "other",
        "member_name": "唐禾",
    }
    assert (
        len(
            validate(
                decision("command", action={"op": "task_batch", "actions": actions}),
                environment,
                context=context,
            ).action["actions"]
        )
        == 2
    )


def test_forget_uses_actual_creator_and_available_source_lineage(environment):
    ctx = {
        **environment.context,
        "tasks": [{**environment.context["tasks"][0], "created_by": "someone_else"}],
    }
    with pytest.raises(_InvalidCandidate):
        validate(
            decision("command", action={"op": "forget", "task_ids": ["task-notes"]}),
            environment,
            context=ctx,
        )
    assert validate(
        decision("command", action={"op": "forget", "task_ids": ["task-notes"]}),
        environment,
        context={**ctx, "role": "owner"},
    ).action["task_ids"] == ["task-notes"]
    for sources in (
        {"source_event_ids": ["legacy"]},
        {"source_refs": [{"event_id": "legacy", "actor_id": "self"}]},
    ):
        ctx = {
            **environment.context,
            "memory": [{"id": "fact", "kind": "fact", "author_id": "self", **sources}],
        }
        assert validate(
            decision("command", action={"op": "forget", "event_ids": ["legacy"]}),
            environment,
            context=ctx,
        ).action["event_ids"] == ["legacy"]


@pytest.mark.parametrize(
    "text,literals",
    [
        ("Word里放表格，Alpha=12，Beta=7。", ["Alpha", "12", "Beta", "7"]),
        ("Word里放表格，逐字保留字符串“Alpha=12”。", ["Alpha=12", "Beta", "7"]),
        ("用Python运行Alpha=12并计算。", ["Alpha=12", "Beta=7"]),
    ],
)
def test_table_relations_are_cells_but_verbatim_and_code_stay_literal(environment, text, literals):
    value = decision(
        "start_work",
        goal=text,
        outputs=[{"format": "docx", "expected_text": ["Alpha=12", "Beta=7"]}],
    )
    assert validate(value, environment, text=text).outputs[0]["expected_text"] == literals


def test_named_tool_contracts_are_small_operations_without_decision_routing(environment):
    schema = business_action_schema(reference_catalog(environment.context, {}))
    Draft202012Validator.check_schema(schema)
    assert set(schema["properties"]) == {"action"}
    assert not Draft202012Validator(schema).is_valid({"kind": "start_work", "goal": "x"})
    for kind in ("start_work", "update_work", "control_work"):
        operation = work_action_schema(kind)
        Draft202012Validator.check_schema(operation)
        assert not {"kind", "reason_code", "confidence", "directness"} & set(
            operation["properties"]
        )
        assert "action" not in operation["properties"]
    assert "goal" in work_action_schema("start_work")["required"]
    assert "work_selector" in work_action_schema("control_work")["required"]
    Draft202012Validator.check_schema(work_fact_tool_schema())


class ModelFixture:
    name = "offline-kernel-contract"

    def __init__(self, *steps):
        self.steps, self.requests = list(steps), []

    async def complete(self, request):
        self.requests.append(request)
        step = self.steps.pop(0)
        if isinstance(step, BaseException):
            raise step
        return step(request) if callable(step) else step


def text_reply(text):
    return ModelReply(text=text, usage=TokenUsage(input_tokens=1, output_tokens=1))


def tool_reply(name, arguments):
    if name == "apply_business_action" and "candidate" in arguments:
        candidate = arguments["candidate"]
        kind = candidate["kind"]
        if kind in {"start_work", "update_work", "control_work"}:
            name = kind
        arguments = {
            key: value
            for key, value in candidate.items()
            if key not in {"kind", "reason_code", "directness", "confidence"}
        }
    return ModelReply(
        tool_calls=(ModelToolCall(id="synthetic-call", name=name, arguments=arguments),),
        finish_reason="tool_calls",
        usage=TokenUsage(input_tokens=1, output_tokens=1),
    )


async def test_ordinary_natural_text_survives_without_decision_json_or_actions(environment):
    answer = "未确认是指缺少结果凭证。原始需求已经记录，不需要你重述。"
    model = ModelFixture(text_reply(answer))
    result = await ModelDecisionProvider(model).decide(environment.event, environment.context)
    assert result.kind == "reply" and result.response == answer and result.action is None
    request = model.requests[0]
    assert {tool["function"]["name"] for tool in request.tools} == {
        "read_work_facts",
        "start_work",
        "update_work",
        "control_work",
        "apply_business_action",
        "observe",
    }
    assert "Decision schema:" not in request.messages[0].content


async def test_invalid_action_is_observation_then_model_answers_current_question(environment):
    def after_rejection(request):
        observation = json.loads(request.messages[-1].content)
        assert observation["status"] == "rejected" and not observation["committed"]
        assert observation["issues"][0]["code"] == "owner_and_unique_current_member_required"
        return text_reply("你当前没有给另一位成员分工的权限。")

    model = ModelFixture(
        tool_reply(
            "apply_business_action",
            {
                "candidate": decision(
                    "command",
                    action={
                        "op": "assign_task",
                        "member_id": "other",
                        "member_name": "唐禾",
                        "title": "Synthetic",
                    },
                )
            },
        ),
        after_rejection,
    )
    provider = ModelDecisionProvider(model)
    result = await provider.decide(environment.event, environment.context)
    assert result.response == "你当前没有给另一位成员分工的权限。"
    assert provider.diagnostics[0]["code"] == "business_action_rejected"


async def test_write_host_receipt_precedes_final_natural_response(environment):
    seen = []

    async def apply(candidate, identifier):
        seen.append((candidate, identifier))
        return {"status": "committed", "committed": True, "task_id": "synthetic-task"}

    ctx = {**environment.context, "_apply_business_action": apply}

    def after_commit(request):
        assert seen and json.loads(request.messages[-1].content)["status"] == "committed"
        return text_reply("记下了，你负责访谈摘要。")

    model = ModelFixture(
        tool_reply(
            "apply_business_action",
            {"candidate": decision("command", action={"op": "claim_task", "title": "访谈摘要"})},
        ),
        after_commit,
    )
    result = await ModelDecisionProvider(model).decide(environment.event, ctx)
    assert result.response == "记下了，你负责访谈摘要。" and result.action is None


async def test_inspection_reads_evidence_then_preserves_question_specific_reply(environment):
    seen = []

    async def read(selector, evidence):
        seen.append((selector.title_query, evidence))
        return {"facts": [{"id": "work-report", "state": "paused", "artifacts": {"items": []}}]}

    def answer(request):
        assert seen == [("报告", True)]
        assert json.loads(request.messages[-1].content)["facts"][0]["state"] == "paused"
        return text_reply("我查了记录，尚无已验证的文档。这次检查没有恢复执行。")

    model = ModelFixture(
        tool_reply(
            "read_work_facts",
            {
                "selector": {"basis": "query", "title_query": "报告", "states": []},
                "include_evidence": True,
            },
        ),
        answer,
    )
    result = await ModelDecisionProvider(model).decide(
        environment.event, {**environment.context, "_read_work_facts": read}
    )
    assert result.response == "我查了记录，尚无已验证的文档。这次检查没有恢复执行。"


@pytest.mark.parametrize(
    "failure", [DomainError("budget_exhausted", "fixture"), asyncio.CancelledError()]
)
async def test_provider_failure_or_cancellation_never_becomes_an_action(environment, failure):
    model = ModelFixture(failure)
    with pytest.raises(type(failure)):
        await ModelDecisionProvider(model).decide(environment.event, environment.context)
    assert len(model.requests) == 1


async def test_kernel_timeout_remains_a_retryable_foreground_dependency_failure(environment):
    model = ModelFixture(TimeoutError())
    with pytest.raises(DomainError) as error:
        await ModelDecisionProvider(model).decide(environment.event, environment.context)
    assert error.value.code == "provider_timeout" and error.value.retryable
    assert len(model.requests) == 1


@pytest.mark.parametrize(
    "text,capability,output",
    [
        ("请实际核对这些观测值是否一致，保留可复现依据。", "compute", "text"),
        ("把原始材料编排成可编辑的课堂讲义，核对后交付。", "documents", "docx"),
        ("读取原图中的表格，核对数值再解释。", "image_analysis", "text"),
    ],
)
async def test_named_start_work_primitive_exposes_real_execution_without_decision_wrapper(
    environment, text, capability, output
):
    committed = []

    async def apply(candidate, identifier):
        committed.append(candidate)
        assert candidate.kind == "start_work" and candidate.goal == text
        return {"status": "committed", "committed": True, "work_id": "synthetic-work"}

    def after_commit(request):
        assert committed and json.loads(request.messages[-1].content)["status"] == "committed"
        return text_reply("已委派实际执行，结果会以执行和检查凭证为准。")

    proposal = ModelReply(
        tool_calls=(
            ModelToolCall(
                id="actual-delegation",
                name="start_work",
                arguments={
                    "goal": text,
                    "work_capabilities": [capability],
                    "outputs": [{"format": output}],
                },
            ),
        ),
        finish_reason="tool_calls",
        usage=TokenUsage(input_tokens=1, output_tokens=1),
    )
    model = ModelFixture(proposal, after_commit)
    current = environment.event.model_copy(update={"text": text})
    result = await ModelDecisionProvider(model).decide(
        current, {**environment.context, "_apply_business_action": apply}
    )
    assert result.response == "已委派实际执行，结果会以执行和检查凭证为准。"
    assert committed[0].work_capabilities == [capability]
    schema = next(
        tool["function"]["parameters"]
        for tool in model.requests[0].tools
        if tool["function"]["name"] == "start_work"
    )
    assert {"goal", "work_capabilities"} <= set(schema["required"])
    assert "candidate" not in schema["properties"] and "kind" not in schema["properties"]
