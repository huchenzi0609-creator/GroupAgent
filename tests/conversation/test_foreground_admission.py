"""On-the-fly plan correction and revision-fenced foreground admission."""

import pytest

from groupagent.conversation.turns import durable_effect_identity
from groupagent.coordinator import Decision

from .test_agent_dialogue import source
from .test_react_turn import ModelScript, call, finish, tool_receipt


def test_progress_identity_excludes_proposals_timestamps_and_work_bookkeeping():
    receipt = {
        "kind": "start_work",
        "committed": True,
        "work_effects": [{"id": "work", "goal_revision": 1, "revision": 8, "goal": "first"}],
        "result": {"decision": {"goal": "proposal-one"}},
    }
    replay = {
        **receipt,
        "work_effects": [{"id": "work", "goal_revision": 1, "revision": 90, "goal": "second"}],
        "result": {"decision": {"goal": "proposal-two"}},
        "tool_call_id": "different",
    }
    assert durable_effect_identity(receipt) == durable_effect_identity(replay)
    changed = {**replay, "work_effects": [{"id": "work", "goal_revision": 2}]}
    assert durable_effect_identity(receipt) != durable_effect_identity(changed)
    first = {
        "kind": "read_work_facts",
        "committed": True,
        "observed_at": "first",
        "facts": [{"id": "work", "goal_revision": 1, "revision": 8, "state": "queued"}],
    }
    second = {
        **first,
        "observed_at": "later",
        "facts": [{"id": "work", "goal_revision": 1, "revision": 90, "state": "queued"}],
    }
    assert durable_effect_identity(first) == durable_effect_identity(second)


@pytest.mark.postgres
async def test_repair_authorization_cannot_overwrite_later_human_goal_revision(conversation_env):
    env = conversation_env
    value = source(env, "Pot，制作一份潮汐Word。")
    actor = env.coordinator.actor(value)
    current = env.coordinator._context(actor, value)
    env.coordinator._bind_agent_tools(actor, value, current)
    candidate = Decision(
        kind="start_work",
        goal="原始错误主题。",
        work_capabilities=["documents"],
        outputs=[{"format": "docx"}],
        reason_code="fixture",
        directness="direct",
        confidence=1,
    )
    receipt = await current["_apply_business_action"](candidate, "initial")
    effect = receipt["work_effects"][0]
    with env.store.transaction(actor) as tx:
        before = tx.get_work(effect["id"])
        tx.revise_work(before.id, "后续真人已经调整的目标。", expected_revision=before.revision)
    refused = await current["_authorize_work_repair"](effect["id"], effect["goal_revision"])
    assert refused["code"] == "goal_revision_changed"
    with env.store.transaction(actor) as tx:
        assert tx.get_work(effect["id"]).goal == "后续真人已经调整的目标。"


@pytest.mark.postgres
async def test_model_reads_then_revises_its_own_goal_inside_the_same_foreground_run(
    conversation_env,
):
    env = conversation_env
    event = source(env, "Pot，核实Aurora Desk的官方获取方式，在群里说明。")
    wrong = call(
        "start_work",
        "wrong",
        goal="制作关于火山的Word。",
        work_capabilities=["documents"],
        outputs=[{"format": "docx"}],
    )

    def check(request):
        receipt = tool_receipt(request)
        job = receipt["work_effects"][0]
        assert job["goal"] == "制作关于火山的Word。"
        return call("read_work_facts", "read", selector={"basis": "ids", "ids": [job["id"]]})

    def update(request):
        target = tool_receipt(request)["facts"][0]
        return call(
            "update_work",
            "correct",
            target_work_id=target["id"],
            expected_revision=target["revision"],
            goal=event.text,
            work_capabilities=["public_research"],
            outputs=[{"format": "text"}],
        )

    env.models[event.id] = ModelScript(
        wrong, check, update, finish(reports=[("correct", "committed")])
    )
    result = await env.coordinator.process(event)
    assert result.delivery_id
    assert "Word" not in result.decision.response
    with env.store.transaction(env.actor) as tx:
        works = tx.list_work()
        assert len(works) == 1 and works[0].goal == event.text and works[0].goal_revision == 2
        assert works[0].scope["research_required"] is True
        assert [output["format"] for output in works[0].scope["outputs"]] == ["text"]
