"""Verify final model output against real tool and source evidence."""

import json

import pytest
from pydantic import ValidationError

from groupagent.conversation.turns import FinishTurn
from groupagent.domain import DomainError

from .test_agent_dialogue import source
from .test_react_turn import ModelScript, call, finish, tool_receipt


@pytest.mark.parametrize("value", ["false", 0, 1, "yes", {}, []])
def test_finish_cannot_coerce_a_non_boolean_input_blocker(value):
    with pytest.raises(ValidationError):
        FinishTurn.model_validate({"disposition": "reply", "needs_input": value})


@pytest.mark.postgres
async def test_wrong_phase_cannot_claim_real_execution_from_an_admission_receipt(conversation_env):
    env = conversation_env
    event = source(env, "Pot，请实际用 Shell 算出 1 到 10 的平方和。")
    start = call(
        "start_work",
        "shell",
        goal=event.text,
        work_capabilities=["compute"],
        required_work_tools=["process_run"],
        outputs=[{"format": "text"}],
    )
    bad = finish("已经运行完了，结果是385。", reports=[("shell", "committed")])

    def correct(request):
        assert (
            json.loads(request.messages[-1].content)["code"]
            == "report_requires_current_matching_receipt"
        )
        return finish(reports=[("shell", "queued")])

    model = ModelScript(start, bad, correct)
    env.models[event.id] = model
    result = await env.coordinator.process(event)
    assert "实际运行这段程序" in result.decision.response
    assert "运行完了" not in result.decision.response
    assert len(model.requests) == 3
    with env.store.transaction(env.actor) as tx:
        assert tx.get_work(result.work_id).scope["required_tools"] == ["process_run"]
        assert len(tx.list_work()) == len(tx.list_delivery()) == 1


@pytest.mark.postgres
async def test_read_only_uses_actual_grants_and_can_explain_the_boundary(conversation_env):
    env = conversation_env
    cfg = env.coordinator.config
    env.coordinator.config = cfg.model_copy(
        update={
            "group": cfg.group.model_copy(
                update={
                    "observer_ids": ["test-observer"],
                    "read_only_qa_roles": ["observer"],
                }
            )
        }
    )
    event = source(env, "Pot，帮我做一份会议 PDF。", actor="test-observer")

    def blocked(request):
        assert {row["function"]["name"] for row in request.tools} == {
            "read_work_facts",
            "read_group_context",
            "finish_turn",
        }
        return finish("当前角色只能查询，不能交办这份 PDF。", blocker="read_only_scope")

    model = ModelScript(blocked)
    env.models[event.id] = model
    result = await env.coordinator.process(event)
    assert result.delivery_id and "只能查询" in result.decision.response
    assert len(model.requests) == 1
    with env.store.transaction(env.actor) as tx:
        assert not tx.list_work()


@pytest.mark.postgres
async def test_explicitly_silent_execution_still_needs_a_real_effect(conversation_env):
    env = conversation_env
    event = source(env, "Pot，搜集山雀的公开资料，直接做就好，不用发接单回复。")
    start = call(
        "start_work",
        "actual",
        goal=event.text,
        work_capabilities=["public_research"],
        outputs=[{"format": "text"}],
    )
    pretend = finish(disposition="observe", requested_silence=True, reports=[("missing", "queued")])
    silent = finish(disposition="observe", requested_silence=True, reports=[("actual", "queued")])
    model = ModelScript(pretend, start, silent)
    env.models[event.id] = model
    result = await env.coordinator.process(event)
    assert result.work_id and not result.delivery_id
    assert len(model.requests) == 3


@pytest.mark.postgres
async def test_repair_without_host_progress_stays_pending_and_never_sends(conversation_env):
    env = conversation_env
    event = source(env, "Pot，上网核实无人机比赛的报名入口。")
    start = call(
        "start_work",
        "actual",
        goal=event.text,
        work_capabilities=["public_research"],
        outputs=[{"format": "text"}],
    )
    misleading = finish("已经查完了。", reports=[("no-receipt", "committed")])
    model = ModelScript(start, misleading, misleading)
    env.models[event.id] = model
    with pytest.raises(DomainError, match="No new evidence"):
        await env.coordinator.process(event)
    with env.store.transaction(env.actor) as tx:
        saved = tx.get_decision(event.id)
        assert saved["agent_needs_execution"] and saved["agent_pending_response"]
        assert len(saved["action_receipts"]) == 1
        assert not tx.list_delivery()


@pytest.mark.postgres
async def test_self_declaration_records_a_task_with_no_work_or_acknowledgement(conversation_env):
    env = conversation_env
    event = source(env, "我来负责把演示要点复核一遍。")
    declare = call(
        "apply_business_action",
        "self-declare",
        silent_declaration=True,
        action={"op": "claim_task", "title": "复核演示要点"},
    )

    def remain_silent(request):
        assert tool_receipt(request)["status"] == "committed"
        return finish(disposition="observe", reports=[("self-declare", "committed")])

    env.models[event.id] = ModelScript(declare, remain_silent)
    result = await env.coordinator.process(event)
    assert not result.work_id and not result.delivery_id
    with env.store.transaction(env.actor) as tx:
        assert len(tx.list_team_tasks()) == 1 and not tx.list_work()
