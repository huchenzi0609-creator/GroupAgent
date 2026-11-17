"""Scripted model traces through the real foreground ReAct graph and host receipts."""

import json

import pytest

from groupagent.conversation.provider import ModelDecisionProvider
from groupagent.domain import DomainError
from groupagent.execution.models import ModelReply, ModelToolCall, TokenUsage

from .test_agent_dialogue import source

USAGE = TokenUsage(input_tokens=1, output_tokens=1)


def call(name, call_id, **arguments):
    return ModelReply(
        finish_reason="tool_calls",
        tool_calls=(ModelToolCall(name=name, id=call_id, arguments=arguments),),
        usage=USAGE,
    )


def finish(response=None, *, disposition="reply", reports=(), **kwargs):
    return call(
        "finish_turn",
        "finish-id",
        disposition=disposition,
        response=response,
        reports=[{"tool_call_id": identifier, "phase": phase} for identifier, phase in reports],
        **kwargs,
    )


def tool_receipt(request):
    return json.loads(next(row.content for row in reversed(request.messages) if row.role == "tool"))


class ModelScript:
    def __init__(self, *steps):
        self.steps, self.requests = list(steps), []

    async def complete(self, request):
        self.requests.append(request)
        assert "TURN_CONTRACT_V1" not in request.messages[0].content
        assert "TURN_OUTCOME_V1" not in request.messages[0].content
        step = self.steps.pop(0)
        if isinstance(step, Exception):
            raise step
        return step(request) if callable(step) else step


async def test_proactive_observation_uses_same_finish_protocol_without_write_grant():
    def observe(request):
        names = {t["function"]["name"] for t in request.tools}
        assert names == {"finish_turn"}
        assert json.loads(request.messages[1].content)["observation"]["kind"] == "work_changed"
        return finish("验收材料已齐，可以一起核对最后两项。", disposition="proactive")

    model = ModelScript(observe)
    decision = await ModelDecisionProvider(model).observe_event({"kind": "work_changed"}, {})
    assert decision.kind == "reply" and decision.directness == "proactive"
    assert len(model.requests) == 1


@pytest.mark.postgres
async def test_answer_or_observe_in_one_model_call_without_fast_semantics(conversation_env):
    env = conversation_env
    value = source(env, "Pot，解释例句「我确实执行了」里“确实”的用法。")
    model = ModelScript(finish("“确实”用来强调事情真的发生了。"))
    env.models[value.id] = model
    result = await env.coordinator.process(value)
    assert result.decision.response == "“确实”用来强调事情真的发生了。"
    assert len(model.requests) == 1
    assert {x["function"]["name"] for x in model.requests[0].tools} >= {
        "start_work",
        "read_work_facts",
        "apply_business_action",
        "finish_turn",
    }

    ambient = source(env, "我今天有点累。", actor="another-human")
    silent = ModelScript(finish(disposition="observe"))
    env.models[ambient.id] = silent
    result = await env.coordinator.process(ambient)
    assert result.decision.kind == "observe" and not result.delivery_id
    assert len(silent.requests) == 1


@pytest.mark.postgres
async def test_failed_probe_then_real_action_and_truthful_mixed_reply(conversation_env):
    env = conversation_env
    event = source(env, "Pot，这个数为什么大概是3？还请实际用 Python 算一下。")
    probe = call("read_work_facts", "probe", selector={"basis": "ids", "ids": []})

    def write(request):
        assert tool_receipt(request)["status"] == "rejected"
        return call(
            "start_work",
            "actual",
            goal=event.text,
            work_capabilities=["compute"],
            required_work_tools=["python_compute"],
            outputs=[{"format": "text"}],
        )

    def invalid_report(request):
        receipt = tool_receipt(request)
        assert receipt["status"] == "committed" and receipt["result"]["work_id"]
        with env.store.transaction(env.actor) as tx:
            assert len(tx.list_work()) == 1 and not tx.list_delivery()
        return finish("我已经算完了。", reports=[("invented-result", "committed")])

    def valid_report(request):
        feedback = json.loads(request.messages[-1].content)
        assert feedback["code"] == "report_requires_current_matching_receipt"
        return finish("3 是一个粗略近似。", reports=[("actual", "queued")])

    model = ModelScript(probe, write, invalid_report, valid_report)
    env.models[event.id] = model
    result = await env.coordinator.process(event)
    assert "3 是一个粗略近似。" in result.decision.response
    assert "实际运行 Python" in result.decision.response
    assert "已经算完" not in result.decision.response
    assert result.delivery_id
    assert len(model.requests) == 4


@pytest.mark.postgres
async def test_receipt_reused_after_model_interruption_and_event_replay(conversation_env):
    env = conversation_env
    event = source(env, "Pot，用 Python 实际算一遍 7 的平方。")
    start = call(
        "start_work",
        "python-start",
        goal=event.text,
        work_capabilities=["compute"],
        required_work_tools=["python_compute"],
        outputs=[{"format": "text"}],
    )
    env.models[event.id] = ModelScript(
        start, DomainError("offline", "Model interrupted", retryable=True)
    )
    with pytest.raises(DomainError, match="Model interrupted"):
        await env.coordinator.process(event)
    with env.store.transaction(env.actor) as tx:
        assert len(tx.list_work()) == 1 and not tx.list_delivery()
        body = tx.get_decision(event.id)
        assert body["agent_pending_response"] and body["agent_needs_execution"]

    def resumed(request):
        prior = json.loads(request.messages[1].content)["prior_receipts"]
        assert len(prior) == 1 and prior[0]["tool_call_id"] == "python-start"
        return finish(reports=[("python-start", "queued")])

    model = ModelScript(resumed)
    env.models[event.id] = model
    result = await env.coordinator.process(event)
    assert result.work_id and result.delivery_id
    replay = await env.coordinator.process(event)
    assert replay.duplicate and replay.work_id == result.work_id
    assert len(model.requests) == 1
    with env.store.transaction(env.actor) as tx:
        assert len(tx.list_work()) == len(tx.list_delivery()) == 1
