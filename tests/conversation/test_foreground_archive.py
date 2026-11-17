"""Archive discovery yields without discarding current facts or granting writes."""

import json

import pytest

from groupagent.domain import WorkState

from .test_agent_dialogue import source
from .test_react_turn import ModelScript, call, finish, offered_properties, tool_receipt

pytestmark = pytest.mark.postgres


def archive_options(request):
    return offered_properties(request, "read_group_context")["resource"]["enum"]


async def test_elapsed_archive_window_preserves_initial_fact_and_live_clock(
    conversation_env, monkeypatch
):
    env = conversation_env
    elapsed = 0.0
    monkeypatch.setattr("groupagent.conversation.provider.monotonic", lambda: elapsed)
    evidence = source(env, "货架交接的登记代号为合成蓝镜803。")
    with env.store.transaction(env.actor) as tx:
        current = tx.create_team_task("货架交接（合成蓝镜803）", source_event_id=evidence.id)
    request = source(env, "Pot，群里登记的货架交接代号是什么？")
    loaded = []
    context_builder = env.coordinator._context

    def capture_context(actor, event, query=None, **options):
        if query is not None and "memory" in query.resources:
            loaded.extend(query.memory_terms)
        return context_builder(actor, event, query, **options)

    monkeypatch.setattr(env.coordinator, "_context", capture_context)

    def initial(model_request):
        nonlocal elapsed
        preview = json.loads(model_request.messages[1].content)["context"]["tasks"]
        assert any(row["id"] == current.id and "合成蓝镜803" in row["title"] for row in preview)
        elapsed = 2.0
        return call("read_group_context", "first-archive", resource="memory", term="第一份旧记录")

    def try_more(model_request):
        nonlocal elapsed
        assert tool_receipt(model_request)["status"] == "observed"
        assert "memory" in archive_options(model_request)
        elapsed = 7.0
        return call("read_group_context", "later-archive", resource="memory", term="另一份旧记录")

    def use_live_clock(model_request):
        receipt = tool_receipt(model_request)
        assert receipt["type"] == "foreground_archive_observation"
        assert receipt["code"] == "foreground_archive_deferred" and not receipt["committed"]
        assert receipt["prior_facts_retained"] and "facts" not in receipt
        assert set(archive_options(model_request)) == {
            "clock",
            "members",
            "tasks",
            "schedules",
            "runtime",
        }
        assert {"finish_turn", "read_work_facts", "control_work", "start_work"} <= {
            tool["function"]["name"] for tool in model_request.tools
        }
        return call("read_group_context", "still-live-clock", resource="clock")

    def respond(model_request):
        assert tool_receipt(model_request)["facts"]["time_zone"] == "Asia/Shanghai"
        return finish("已登记的货架交接代号是合成蓝镜803。")

    model = ModelScript(initial, try_more, use_live_clock, respond)
    env.models[request.id] = model
    result = await env.coordinator.process(request)
    assert (
        result.delivery_id and result.work_id is None and "合成蓝镜803" in result.decision.response
    )
    assert loaded == ["第一份旧记录"]
    with env.store.transaction(env.actor) as tx:
        assert not tx.list_work() and len(tx.list_delivery()) == 1
        assert tx.get_decision(request.id)["turn_outcome"]["actions"] == [
            {"tool": "read_group_context", "status": "observed"},
            {
                "tool": "read_group_context",
                "status": "rejected",
                "code": "foreground_archive_deferred",
            },
            {"tool": "read_group_context", "status": "observed"},
        ]


async def test_repeated_archive_results_yield_and_model_can_queue_deeper_work(
    conversation_env, monkeypatch
):
    env = conversation_env
    monkeypatch.setattr("groupagent.conversation.provider.monotonic", lambda: 0.0)
    request = source(env, "Pot，请深入整理群里保留的历次货架交接记录并交付Markdown。")
    topic = env.coordinator.memory.create_topic(env.actor, "合成货架记录")
    env.coordinator.memory.link_event(
        env.actor, request.id, topic.id, evidence_event_ids=[request.id]
    )
    first_facts = []

    def read_again(model_request):
        receipt = tool_receipt(model_request)
        assert receipt["status"] == "observed"
        first_facts.extend(receipt["facts"])
        assert any(row["id"] == topic.id for row in first_facts)
        return call("read_group_context", "topic-read-again", resource="topics", term="另一种查法")

    def queue(model_request):
        receipt = tool_receipt(model_request)
        assert (
            receipt["status"] == "observed"
            and receipt["code"] == "foreground_archive_no_new_evidence"
        )
        assert not receipt["new_evidence"] and receipt["facts"] == first_facts
        assert "topics" not in archive_options(model_request)
        assert {"memory", "files", "clock", "tasks"} <= set(archive_options(model_request))
        fields = offered_properties(model_request, "start_work")
        assert topic.id in fields["topic_id"]["enum"]
        return call(
            "start_work",
            "archive-handoff",
            goal="深入汇总获准的历次群内货架交接记录并交付 Markdown",
            topic_id=topic.id,
            outputs=[{"format": "md"}],
            work_capabilities=["documents"],
            finish_on_commit=True,
        )

    model = ModelScript(
        call("read_group_context", "topic-read", resource="topics", term="初步查询"),
        read_again,
        queue,
    )
    env.models[request.id] = model
    result = await env.coordinator.process(request)
    assert result.work_id and result.delivery_id and len(model.requests) == 3
    with env.store.transaction(env.actor) as tx:
        (work,) = tx.list_work()
        assert work.id == result.work_id and work.state == WorkState.QUEUED
        assert work.scope["topic_id"] == topic.id
        assert len(tx.list_delivery()) == 1
