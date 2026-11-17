"""Host-only instruction provenance; synthetic events and real PostgreSQL writes."""

import json

import pytest

from groupagent.coordinator import Decision
from groupagent.domain import DomainError
from groupagent.execution.models import ModelReply, ModelToolCall

from .test_agent_dialogue import ScriptedModel, source
from .test_turn_contract import USAGE, reply

pytestmark = pytest.mark.postgres


def plan(kind, goal, **fields):
    return Decision(
        kind=kind,
        goal=goal,
        reason_code="synthetic_host_plan",
        directness="direct",
        confidence=1,
        work_capabilities=["documents"],
        outputs=[{"format": "docx"}],
        **fields,
    )


def apply(env, value, decision):
    actor = env.coordinator.actor(value)
    context = env.coordinator._context(actor, value)
    return env.coordinator._apply_decision(actor, value, decision, context)


def current(env, identifier):
    with env.store.transaction(env.actor) as tx:
        return tx.get_work(identifier)


def test_new_work_records_only_the_actual_human_event_not_expanded_plan(conversation_env):
    env = conversation_env
    source(env, "合成同学的旁支讨论。", actor="synthetic-peer")
    value = source(env, "Pot，制作潮汐说明Word。")
    result = apply(env, value, plan("start_work", "规划器自行扩写的各平台及同名候选说明。"))
    work = current(env, result.work_id)
    assert work.scope["human_request_event_ids"] == [value.id]
    assert work.scope["human_request_history_complete"] is True
    with env.store.transaction(env.actor) as tx:
        assert work.scope["human_request_fingerprints"] == {
            value.id: tx.event_fingerprint(value.id)
        }
    assert value.text not in json.dumps(
        work.scope["human_request_fingerprints"], ensure_ascii=False
    )


def test_goal_updates_preserve_order_fingerprints_and_each_goal_revision(conversation_env):
    env = conversation_env
    first = source(env, "Pot，制作潮汐说明Word。")
    result = apply(env, first, plan("start_work", "潮汐说明。"))
    identifiers = [first.id]
    expected_fingerprints = dict(current(env, result.work_id).scope["human_request_fingerprints"])
    for text, goal in (
        ("Pot，改成三页。", "三页潮汐说明。"),
        ("Pot，只讨论近海潮汐。", "三页近海潮汐说明。"),
    ):
        value = source(env, text)
        before = current(env, result.work_id)
        revised = apply(
            env,
            value,
            plan("update_work", goal, target_work_id=before.id, expected_revision=before.revision),
        )
        assert revised.work_id == before.id
        identifiers.append(value.id)
        with env.store.transaction(env.actor) as tx:
            expected_fingerprints[value.id] = tx.event_fingerprint(value.id)
            work = tx.get_work(before.id)
            assert work.scope["human_request_event_ids"] == identifiers
            assert work.scope["human_request_fingerprints"] == expected_fingerprints
            assert work.scope["human_request_history_complete"] is True
            stored = tx.conn.execute(
                "SELECT body FROM ga_work_revisions WHERE group_id=%s AND work_id=%s AND goal_revision=%s",
                (env.actor.group_id, work.id, work.goal_revision),
            ).fetchone()
            assert stored["body"]["scope"]["human_request_event_ids"] == identifiers
            assert stored["body"]["scope"]["human_request_fingerprints"] == expected_fingerprints
        duplicate = apply(
            env,
            value,
            plan("update_work", goal, target_work_id=before.id, expected_revision=before.revision),
        )
        assert duplicate.duplicate
        assert current(env, before.id).scope["human_request_event_ids"] == identifiers


def test_legacy_work_is_not_backfilled_and_first_real_update_marks_partial_history(
    conversation_env,
):
    env = conversation_env
    original = source(env, "合成旧版本原始要求。")
    with env.store.transaction(env.actor) as tx:
        work = tx.create_work("既有已授权目标。", original.id)
    env.coordinator._context(env.actor, original)
    assert "human_request_event_ids" not in current(env, work.id).scope
    change = source(env, "Pot，把这项旧工作改成三页。")
    result = apply(
        env,
        change,
        plan(
            "update_work",
            "既有目标改成三页。",
            target_work_id=work.id,
            expected_revision=work.revision,
        ),
    )
    updated = current(env, result.work_id)
    assert updated.scope["human_request_event_ids"] == [change.id]
    assert updated.scope["human_request_history_complete"] is False
    assert set(updated.scope["human_request_fingerprints"]) == {change.id}


async def test_same_turn_admission_repair_keeps_one_human_reference(conversation_env):
    env = conversation_env
    value = source(env, "Pot，核实Aurora Desk的官方获取方式。")
    actor = env.coordinator.actor(value)
    context = env.coordinator._context(actor, value)
    env.coordinator._bind_agent_tools(actor, value, context)
    initial = await context["_apply_business_action"](
        plan("start_work", "错误的Word主题。"), "initial"
    )
    effect = initial["work_effects"][0]
    target = await context["_authorize_work_repair"](effect["id"], effect["goal_revision"])
    corrected = Decision(
        kind="update_work",
        goal=value.text,
        target_work_id=effect["id"],
        expected_revision=target["expected_revision"],
        work_capabilities=["public_research"],
        outputs=[{"format": "text"}],
        reason_code="repair_current_turn_admission",
        directness="direct",
        confidence=1,
    )
    accepted = await context["_apply_business_action"](corrected, "repair")
    assert accepted["committed"] is True
    work = current(env, effect["id"])
    assert work.goal_revision == 2 and work.scope["human_request_event_ids"] == [value.id]
    assert work.scope["human_request_history_complete"] is True
    assert len(work.scope["human_request_fingerprints"]) == 1


async def test_model_cannot_supply_or_replace_host_request_chain(conversation_env):
    env = conversation_env
    value = source(env, "Pot，制作潮汐说明Word。")
    forged = {
        "human_request_event_ids": ["another-group-event"],
        "human_request_fingerprints": {"another-group-event": "a" * 64},
        "human_request_history_complete": False,
    }
    arguments = {
        "goal": "潮汐说明。",
        "work_capabilities": ["documents"],
        "outputs": [{"format": "docx"}],
    }
    invalid = ModelReply(
        tool_calls=(
            ModelToolCall(
                id="forged-scope", name="start_work", arguments={**arguments, "scope": forged}
            ),
        ),
        finish_reason="tool_calls",
        usage=USAGE,
    )
    valid = ModelReply(
        tool_calls=(ModelToolCall(id="valid-scope", name="start_work", arguments=arguments),),
        finish_reason="tool_calls",
        usage=USAGE,
    )

    def rejected(_):
        with env.store.transaction(env.actor) as tx:
            assert not tx.list_work()
        return valid

    env.models[value.id] = ScriptedModel(invalid, rejected, reply("文档已安排。"))
    result = await env.coordinator.process(value)
    work = current(env, result.work_id)
    assert work.scope["human_request_event_ids"] == [value.id]
    assert work.scope["human_request_history_complete"] is True
    assert "another-group-event" not in work.scope["human_request_fingerprints"]


def test_removed_request_cannot_be_reauthorized_as_a_new_human_reference(conversation_env):
    env = conversation_env
    value = source(env, "Pot，制作潮汐说明Word。")
    env.coordinator.memory.forget(env.actor, event_ids=[value.id])
    with env.store.transaction(env.actor) as tx:
        with pytest.raises(DomainError) as caught:
            env.coordinator._human_request_provenance(tx, value)
    assert caught.value.code == "source_unavailable"
