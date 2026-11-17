"""Verified completion of a sole foreground action under the shared ReAct loop."""

import json

import pytest

from groupagent.execution.kernel import KernelTool

from .test_react_turn import ModelScript, call, source

pytestmark = pytest.mark.postgres


def background(event, **options):
    return call(
        "start_work",
        "work-id",
        goal=event.text,
        work_capabilities=["compute"],
        required_work_tools=["python_compute"],
        outputs=[{"format": "text"}],
        **options,
    )


async def test_commit_ends_after_one_inference_and_one_dispatch(conversation_env):
    env = conversation_env
    event = source(env, "Pot，请用 Python 实际计算 19 的平方。")
    model = ModelScript(background(event, finish_on_commit=True))
    env.models[event.id] = model
    result = await env.coordinator.process(event)
    assert result.work_id and result.delivery_id and result.decision.response
    assert "Python" in result.decision.response and "已算完" not in result.decision.response
    assert len(model.requests) == 1
    duplicate = await env.coordinator.process(event)
    assert duplicate.duplicate and duplicate.delivery_id == result.delivery_id
    with env.store.transaction(env.actor) as tx:
        assert len(tx.list_work()) == len(tx.list_delivery()) == 1


async def test_mix_skips_slow_call_and_plain_text_reference_can_be_repaired(conversation_env):
    env = conversation_env
    event = source(env, "Pot，请实际用 Python 算出 38 的平方。")
    ran_web = []

    async def external(tool_call):
        ran_web.append(tool_call.id)
        return '{"status":"observed","evidence":"synthetic"}'

    env.coordinator.foreground_tools = lambda actor, event, context: (
        KernelTool(
            "web_search",
            "offline",
            {"type": "object", "properties": {"query": {"type": "string"}}},
            external,
        ),
    )

    def first(request):
        projected = json.loads(request.messages[1].content)
        invalid = background(event, input_event_ids=[projected["event"]["id"]])
        read = call("web_search", "read-id", query="public information")
        return invalid.model_copy(update={"tool_calls": (*invalid.tool_calls, *read.tool_calls)})

    def repair(request):
        receipts = {
            row.tool_call_id: json.loads(row.content)
            for row in request.messages
            if row.role == "tool"
        }
        assert receipts["work-id"]["issues"] == [
            {"path": "input_event_ids.0", "code": "visible_source_has_no_attachment"}
        ]
        assert receipts["work-id"]["available_inputs"] == {
            "input_event_ids": [],
            "input_versions": [],
        }
        assert receipts["read-id"]["code"] == "public_read_separate_from_admission"
        repaired = background(event, finish_on_commit=True)
        return repaired.model_copy(
            update={
                "tool_calls": (
                    repaired.tool_calls[0].model_copy(update={"id": "corrected-work-id"}),
                )
            }
        )

    model = ModelScript(first, repair)
    env.models[event.id] = model
    result = await env.coordinator.process(event)
    assert result.work_id and result.delivery_id and len(model.requests) == 2 and not ran_web
    with env.store.transaction(env.actor) as tx:
        assert len(tx.list_work()) == len(tx.list_delivery()) == 1


async def test_flag_in_multi_action_batch_never_commits_prematurely(conversation_env):
    env = conversation_env
    event = source(env, "Pot，请用 Python 运行一个很短的计算。")
    invalid = background(event, finish_on_commit=True)
    duplicate_id = invalid.tool_calls[0].model_copy(update={"id": "second-work-id"})
    batch = invalid.model_copy(update={"tool_calls": (*invalid.tool_calls, duplicate_id)})

    def retry(request):
        assert (
            json.loads(request.messages[-1].content)["code"]
            == "finish_on_commit_requires_sole_write"
        )
        with env.store.transaction(env.actor) as tx:
            assert not tx.list_work()
        return invalid

    model = ModelScript(batch, retry)
    env.models[event.id] = model
    result = await env.coordinator.process(event)
    assert result.work_id and result.delivery_id and len(model.requests) == 2
