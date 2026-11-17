"""Typed time proposals through the real foreground graph and durable scheduler."""

import json
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest

from groupagent.domain import GroupEvent
from groupagent.execution.models import ModelReply

from .test_react_turn import USAGE, ModelScript, call, finish, tool_receipt


def ingest(env, text, occurred_at, received_at):
    event = GroupEvent(
        id=uuid4().hex,
        source_key=uuid4().hex,
        group_id=env.actor.group_id,
        actor_id=env.actor.member_id,
        kind="text",
        text=text,
        occurred_at=occurred_at,
        received_at=received_at,
    )
    with env.store.transaction(env.actor) as tx:
        tx.ingest_event(event, "fixture:" + event.id, "1", 0)
    return event


@pytest.mark.postgres
@pytest.mark.parametrize(("delay", "mixed"), [(10, False), (10, True), (360, False)])
async def test_relative_reminder_uses_verified_event_anchor_and_canonical_receipt(
    conversation_env, delay, mixed
):
    env = conversation_env
    occurred = datetime(2026, 11, 17, 20, 35, 22, tzinfo=UTC)
    received = occurred + timedelta(seconds=delay)
    due = occurred + timedelta(minutes=5)
    env.coordinator.clock = lambda: received
    text = "Pot，5分钟以后提醒我取快递可以吗？"
    if mixed:
        text += "顺便告诉我3加4是多少。"
    event = ingest(env, text, occurred, received)

    def propose(request):
        assert request.tool_choice == "required"
        payload = json.loads(request.messages[-1].content)
        assert payload["event"]["occurred_at"] == "2026-11-18T04:35:22+08:00"
        context = payload["context"]
        assert context["clock"]["occurred_at"] == "2026-11-18T04:35:22+08:00"
        assert context["clock"]["now_utc"] == received.isoformat()
        assert "time_resolution" not in context
        return call(
            "apply_business_action",
            "reminder",
            action={
                "op": "create_schedule",
                "title": "取快递",
                "timing": {"kind": "after", "seconds": 300},
                "prepared_message": {"purpose": "explicit_reminder", "text": "取快递"},
            },
        )

    def close(request):
        receipt = tool_receipt(request)
        if delay > 300:
            assert request.tool_choice == "required"
            assert receipt["code"] == "schedule_time_missed"
            assert not receipt["committed"]
            assert "04:40:22" in receipt["host_message"]
            return finish("原来的提醒时刻04:40:22已经过去，未创建提醒。")
        assert request.tool_choice == "auto"
        assert receipt["committed"], receipt
        assert datetime.fromisoformat(receipt["result"]["action_result"]["next_due_at"]) == due
        if mixed:
            return finish("3加4等于7。")
        return ModelReply(text="我会在五分钟后提醒你。", usage=USAGE)

    model = ModelScript(propose, close)
    env.models[event.id] = model
    result = await env.coordinator.process(event)
    assert len(model.requests) == 2
    assert result.delivery_id
    assert result.decision.response.count("04:40:22") == 1
    if mixed:
        assert "3加4等于7。" in result.decision.response
    assert "我会在五分钟后提醒你" not in result.decision.response
    with env.store.transaction(env.actor) as tx:
        assert len(tx.list_delivery()) == 1
        assert tx.get_decision(event.id)["turn_outcome"]["actions"] == [
            {"tool": "apply_business_action", "status": "rejected", "code": "schedule_time_missed"}
            if delay > 300
            else {"tool": "apply_business_action", "status": "committed"}
        ]
    rules = env.coordinator.scheduler.list(env.actor)
    if delay > 300:
        assert not rules and result.action_result is None
    else:
        assert len(rules) == 1 and rules[0].next_due_at == due
        assert (await env.coordinator.process(event)).duplicate


@pytest.mark.postgres
async def test_model_can_refresh_clock_then_answer_in_one_completion(conversation_env):
    env = conversation_env
    occurred = datetime(2026, 11, 17, 20, 35, 22, tzinfo=UTC)
    now = occurred
    env.coordinator.clock = lambda: now
    event = ingest(env, "Pot，北京时间现在几点？", occurred, now)

    def request_clock(request):
        nonlocal now
        now += timedelta(seconds=15)
        return call("read_group_context", "clock", resource="clock")

    def answer(request):
        observation = tool_receipt(request)
        assert observation["status"] == "observed" and not observation["committed"]
        assert observation["facts"]["now"] == "2026-11-18T04:35:37+08:00"
        assert observation["facts"]["occurred_at"] == "2026-11-18T04:35:22+08:00"
        return finish("北京时间现在是凌晨04:35:37。")

    model = ModelScript(request_clock, answer)
    env.models[event.id] = model
    result = await env.coordinator.process(event)
    assert len(model.requests) == 2
    assert result.decision.response == "北京时间现在是凌晨04:35:37。"
    assert result.delivery_id


@pytest.mark.postgres
async def test_iana_weekly_creation_and_clock_only_edit_keep_zone_and_all_weekdays(
    conversation_env,
):
    env = conversation_env
    now = datetime(2026, 11, 17, 20, 35, 22, tzinfo=UTC)
    env.coordinator.clock = lambda: now
    zone = "America/Los_Angeles"
    original = ingest(env, "Pot，洛杉矶时间每周四和周日17:30提醒我核对清单。", now, now)
    env.models[original.id] = ModelScript(
        call(
            "apply_business_action",
            "new-weekly",
            action={
                "op": "create_schedule",
                "title": "核对清单",
                "timing": {"kind": "weekly", "local_time": "17:30", "weekdays": [3, 6]},
                "time_zone": zone,
                "prepared_message": {"purpose": "explicit_reminder", "text": "核对清单"},
            },
            finish_on_commit=True,
        ),
    )
    created = await env.coordinator.process(original)
    assert created.action_result and created.action_result["time_zone"] == zone
    schedule = env.coordinator.scheduler.get(env.actor, created.action_result["schedule_id"])
    assert schedule.next_due_at == datetime(2026, 11, 20, 1, 30, tzinfo=UTC)
    assert schedule.timing.kind == "weekly" and schedule.timing.weekdays == [3, 6]

    now += timedelta(seconds=1)
    edit = ingest(env, "Pot，只把刚才那个提醒的时间改成18:15，日期别动。", now, now)

    def change_selected_rule(request):
        observed = tool_receipt(request)
        (fact,) = observed["facts"]["facts"]
        assert fact["id"] == schedule.id and fact["time_zone"] == zone
        return call(
            "apply_business_action",
            "clock-only-edit",
            action={
                "op": "change_schedule",
                "schedule_id": fact["id"],
                "expected_revision": fact["revision"],
                "timing": {"kind": "clock", "local_time": "18:15"},
            },
            finish_on_commit=True,
        )

    env.models[edit.id] = ModelScript(
        call("read_group_context", "weekly-facts", resource="schedules"),
        change_selected_rule,
    )
    changed = await env.coordinator.process(edit)
    assert changed.action_result and changed.action_result["schedule_id"] == schedule.id
    assert changed.action_result["timing"] == {
        "kind": "weekly",
        "local_time": "18:15:00",
        "weekdays": [3, 6],
    }
    revised = env.coordinator.scheduler.get(env.actor, schedule.id)
    assert revised.time_zone == zone and revised.goal == schedule.goal
    assert revised.revision == schedule.revision + 1
    assert revised.next_due_at == datetime(2026, 11, 20, 2, 15, tzinfo=UTC)
    assert env.coordinator.scheduler.wake(env.actor, schedule.next_due_at) == []
    (occurrence,) = env.coordinator.scheduler.wake(env.actor, revised.next_due_at)
    assert occurrence.delivery_id and occurrence.work_id is None


@pytest.mark.postgres
@pytest.mark.parametrize(
    ("date", "local_time", "fold", "expected"),
    [
        ("2026-03-08", "02:30", None, "nonexistent_local_time"),
        ("2026-11-01", "01:30", None, "ambiguous_dst_fold"),
        ("2026-11-01", "01:30", 0, datetime(2026, 11, 1, 8, 30, tzinfo=UTC)),
        ("2026-11-01", "01:30:12", 1, datetime(2026, 11, 1, 9, 30, 12, tzinfo=UTC)),
    ],
)
async def test_strict_wall_proposal_dst_requires_real_instant(
    conversation_env, date, local_time, fold, expected
):
    env = conversation_env
    now = datetime(2026, 3, 1, tzinfo=UTC)
    env.coordinator.clock = lambda: now
    request = ingest(env, "Pot，到指定的洛杉矶时间提醒我提交清单。", now, now)
    timing = {"kind": "wall", "date": date, "local_time": local_time}
    if fold is not None:
        timing["fold"] = fold

    def ask_for_valid_time(turn):
        receipt = tool_receipt(turn)
        assert receipt["code"] == expected and not receipt["committed"]
        return finish("请指定一个明确存在的本地时刻。", needs_input=True)

    action = call(
        "apply_business_action",
        "wall-time",
        action={
            "op": "create_schedule",
            "title": "提交清单",
            "timing": timing,
            "time_zone": "America/Los_Angeles",
            "prepared_message": {"purpose": "explicit_reminder", "text": "提交清单"},
        },
        finish_on_commit=True,
    )
    env.models[request.id] = (
        ModelScript(action, ask_for_valid_time)
        if isinstance(expected, str)
        else ModelScript(action)
    )
    result = await env.coordinator.process(request)
    rules = env.coordinator.scheduler.list(env.actor)
    if isinstance(expected, str):
        assert result.action_result is None and rules == []
    else:
        (rule,) = rules
        assert result.action_result and rule.timing.kind == "at"
        assert rule.next_due_at == expected
        assert rule.time_zone == "America/Los_Angeles"
