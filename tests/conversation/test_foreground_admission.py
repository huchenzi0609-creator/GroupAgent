"""Queued admission, truthful replies and bounded same-effect repair; all models are fixtures."""

import json

import pytest

from groupagent.conversation.turns import durable_effect_identity
from groupagent.coordinator import Decision
from groupagent.domain import DomainError
from groupagent.execution.models import ModelReply, ModelToolCall

from .test_agent_dialogue import source
from .test_turn_contract import GOAL, USAGE, ContractModel, reply, start


class PhaseModel(ContractModel):
    def __init__(self, *steps, reviews=(), **kwargs):
        super().__init__(*steps, **kwargs)
        self.reviews = list(reviews)

    async def complete(self, request):
        if (request.messages[0].content or "").startswith("TURN_OUTCOME_V1"):
            payload = json.loads(request.messages[1].content)
            assert payload["completion_phase"] == "foreground_admission"
            assert payload["background_completion_required"] is False
            self.assessments.append(request)
            review = self.reviews.pop(0)
            value = review(payload) if callable(review) else {"outcome": review}
            return reply(
                json.dumps(
                    {
                        "explanation": "Synthetic phase and target review.",
                        "required_actions": self.assessment.get("required_actions", []),
                        "reply_required": payload["trusted_recipient"]["recipient"] == "agent",
                        **value,
                    }
                )
            )
        return await super().complete(request)


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
async def test_queued_admission_repairs_wording_without_reexecuting(conversation_env):
    env = conversation_env
    value = source(env, GOAL)

    model = PhaseModel(start(), reply("我已经在网上搜了。"), reviews=["repair_reply"])
    env.models[value.id] = model
    result = await env.coordinator.process(value)
    with env.store.transaction(env.actor) as tx:
        assert len(tx.list_work()) == len(tx.list_delivery()) == 1
        assert len(tx.get_decision(value.id)["action_receipts"]) == 1
    assert result.decision.response == "我会查找资料并核对来源，再把结论发回来。"
    assert len(model.requests) == 2 and not model.reviews


@pytest.mark.postgres
async def test_visible_action_cannot_escape_with_observe_after_queued_commit(conversation_env):
    env = conversation_env
    value = source(env, GOAL)
    observe = ModelReply(
        tool_calls=(
            ModelToolCall(id="silent-escape", name="observe", arguments={"reason_code": "skip"}),
        ),
        finish_reason="tool_calls",
        usage=USAGE,
    )
    model = PhaseModel(
        start(), observe, reply("研究已排队，结果核实后发来。"), reviews=["committed"]
    )
    env.models[value.id] = model
    result = await env.coordinator.process(value)
    assert result.delivery_id and result.decision.kind != "observe"
    assert len(model.requests) == 3
    calls = {call.id for message in model.requests[-1].messages for call in message.tool_calls}
    observed = {
        message.tool_call_id for message in model.requests[-1].messages if message.role == "tool"
    }
    assert calls <= observed  # Provisional observe is audited, never a dangling or final escape.


@pytest.mark.postgres
async def test_same_work_effect_with_changed_proposals_defers_and_stays_pending(conversation_env):
    env = conversation_env
    value = source(env, GOAL)
    model = PhaseModel(
        start(),
        start("另一个文字描述，实际仍是同一项工作。", "second"),
        reply("检索已经完成。"),
        reviews=["missing_evidence"],
    )
    env.models[value.id] = model
    with pytest.raises(DomainError, match="host progress"):
        await env.coordinator.process(value)
    with env.store.transaction(env.actor) as tx:
        body = tx.get_decision(value.id)
        assert len(tx.list_work()) == 1 and not tx.list_delivery()
        assert body["agent_pending_response"] and body["agent_needs_execution"]
        assert len(body["action_receipts"]) == 2
        assert len({row["result"]["work_id"] for row in body["action_receipts"]}) == 1
    # Restored matched effects hide the mutation; varied proposals cannot buy
    # fresh progress or turn the visible request into a silent final result.
    resumed = PhaseModel(start("第三种描述。", "third"), start("第四种描述。", "fourth"))
    env.models[value.id] = resumed
    with pytest.raises(DomainError, match="host progress"):
        await env.coordinator.process(value)
    with env.store.transaction(env.actor) as tx:
        assert len(tx.list_work()) == 1 and not tx.list_delivery()
        assert (
            len({row["result"]["work_id"] for row in tx.get_decision(value.id)["action_receipts"]})
            == 1
        )


@pytest.mark.postgres
async def test_mismatched_plan_uses_fenced_full_update_on_same_work(conversation_env):
    env = conversation_env
    goal = "Pot，核实Aurora Desk的官方获取方式，直接在群里说明。"
    value = source(env, goal)
    wrong = ModelReply(
        tool_calls=(
            ModelToolCall(
                id="wrong-plan",
                name="start_work",
                arguments={
                    "goal": "制作关于火山的Word。",
                    "work_capabilities": ["documents"],
                    "outputs": [{"format": "docx"}],
                },
            ),
        ),
        finish_reason="tool_calls",
        usage=USAGE,
    )

    def mismatch(payload):
        effects = [
            effect
            for receipt in payload["host_receipts"]
            for effect in receipt.get("work_effects", [])
        ]
        assert effects[0]["goal"] != goal
        return {"outcome": "missing_evidence", "mismatched_work_ids": [effects[0]["id"]]}

    def update(request):
        feedback = json.loads(request.messages[-1].content)
        target = feedback["admission_repair_targets"][-1]
        assert target["status"] == "repair_available"
        assert "update_work" in {item["function"]["name"] for item in request.tools}
        return ModelReply(
            tool_calls=(
                ModelToolCall(
                    id="correct-plan",
                    name="update_work",
                    arguments={
                        "target_work_id": target["target_work_id"],
                        "expected_revision": target["expected_revision"],
                        "goal": goal,
                        "work_capabilities": ["public_research"],
                        "outputs": [{"format": "text"}],
                        "input_event_ids": [],
                        "input_versions": [],
                    },
                ),
            ),
            finish_reason="tool_calls",
            usage=USAGE,
        )

    model = PhaseModel(
        wrong,
        reply("文档已排队。"),
        update,
        reply("官方资料核实已排队，查明后在群里说明。"),
        reviews=[mismatch, "committed"],
        assessment={
            "recipient": "agent",
            "intent": "action",
            "information_basis": "public_verification",
            "required_actions": [{"kind": "start_work"}],
        },
    )
    env.models[value.id] = model
    result = await env.coordinator.process(value)
    with env.store.transaction(env.actor) as tx:
        work = tx.get_work(result.work_id)
        assert len(tx.list_work()) == 1 and work.goal == goal and work.goal_revision == 2
        assert work.scope["work_capabilities"] == ["public_research"]
        assert work.scope["research_required"] is True
        assert [row["format"] for row in work.scope["outputs"]] == ["text"]
        assert work.required_checks == ["result"]
        assert tx.get_decision(value.id)["action_receipts"][-1]["satisfies_kind"] == "start_work"
    assert result.delivery_id


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
