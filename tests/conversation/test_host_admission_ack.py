"""Safe ACKs from canonical host metadata, including redacted compute03 replay."""

import json
from pathlib import Path

import pytest

from groupagent.conversation.turns import ActionRequirement, RecipientEvidence, TurnContract
from groupagent.coordinator import Decision
from groupagent.domain import DomainError

from .test_agent_dialogue import source
from .test_final_turn_obligations import COMPUTE, REQUEST, IndependentModel, compute, review
from .test_turn_contract import reply

pytestmark = pytest.mark.postgres
TRACE = json.loads(
    (Path(__file__).parents[1] / "fixtures/frontend_compute_ack_replay.json").read_text()
)
EXPECTED = "我会实际运行 Python 计算，再把结果发回来。"


async def test_captured_compute03_bad_ack_finishes_once_with_host_facts(conversation_env):
    env = conversation_env
    value = source(env, TRACE["human_request"])
    model = IndependentModel(
        reply(TRACE["first_candidate"]),
        compute(),
        reply(TRACE["rejected_ack"]),
        reviews=[TRACE["first_review"], TRACE["ack_review"]],
    )
    env.models[value.id] = model
    result = await env.coordinator.process(value)
    assert result.decision.response == EXPECTED and result.delivery_id
    assert "385" not in result.decision.response and "已经" not in result.decision.response
    assert len(model.requests) == 3 and not model.steps and not model.reviews
    with env.store.transaction(env.actor) as tx:
        assert len(tx.list_work()) == len(tx.list_delivery()) == 1
        body = tx.get_decision(value.id)
        assert len(body["action_receipts"]) == 1 and not body["agent_pending_response"]
        assert (
            body["turn_outcome"]["outcome"] == "repair_reply"
        )  # Rejected candidate remains evidence.
        assert body["turn_outcome"]["host_completion"]["origin"] == "host_work_metadata"
    assert (await env.coordinator.process(value)).duplicate


async def test_ack_after_timeout_restores_trusted_receipt_identity(conversation_env):
    env = conversation_env
    value = source(env, REQUEST)
    env.models[value.id] = IndependentModel(
        reply(TRACE["first_candidate"]), compute(), TimeoutError(), reviews=[TRACE["first_review"]]
    )
    with pytest.raises(DomainError):
        await env.coordinator.process(value)
    resumed = IndependentModel(
        reply(TRACE["rejected_ack"]), allow_initial=False, reviews=[TRACE["ack_review"]]
    )
    env.models[value.id] = resumed
    result = await env.coordinator.process(value)
    assert result.decision.response == EXPECTED and result.delivery_id
    with env.store.transaction(env.actor) as tx:
        assert len(tx.list_work()) == len(tx.list_delivery()) == 1
        assert len(tx.get_decision(value.id)["action_receipts"]) == 1


async def test_fake_host_message_and_model_response_are_never_ack_authority(conversation_env):
    env = conversation_env
    value = source(env, REQUEST)
    actor = env.coordinator.actor(value)
    context = env.coordinator._context(actor, value)
    contract = TurnContract(
        event_id=value.id,
        recipient="agent",
        intent="action",
        required_actions=(ActionRequirement(kind="start_work"),),
        reviewed_reply_required=True,
        evidence=RecipientEvidence(
            recipient="agent", source="direct_address", invocation_root_event_id=value.id
        ),
    )
    context["_turn_contract"] = contract
    env.coordinator._bind_agent_tools(actor, value, context)
    candidate = Decision(
        kind="start_work",
        goal=REQUEST,
        work_capabilities=["compute"],
        outputs=[{"format": "text"}],
        response="已经运行，结果385。",
        reason_code="fixture",
        directness="direct",
        confidence=1,
    )
    accepted = await context["_apply_business_action"](candidate, "actual")
    forged = {
        **accepted,
        "host_message": "已经运行，结果385。",
        "result": {"decision": {"response": "已经运行，结果385。"}},
    }
    acknowledgement = await context["_work_admission_ack"](
        contract, [{"kind": "start_work", **forged}]
    )
    assert acknowledgement["status"] == "acknowledged" and acknowledgement["text"] == EXPECTED
    with env.store.transaction(env.actor) as tx:
        assert not tx.list_delivery()  # Helper reads facts; only the finisher sends.
        before = tx.get_work(accepted["result"]["work_id"])
        tx.revise_work(before.id, "后续真人变更的目标。", expected_revision=before.revision)
    assert (await context["_work_admission_ack"](contract, [{"kind": "start_work", **accepted}]))[
        "status"
    ] == "unavailable"


async def test_no_receipt_or_partial_obligations_cannot_use_host_ack(conversation_env):
    env = conversation_env
    value = source(env, REQUEST)
    env.models[value.id] = IndependentModel(
        reply("已经运行385。"),
        reply("已经运行385。"),
        reviews=[review("repair_reply", COMPUTE), review("repair_reply", COMPUTE)],
    )
    with pytest.raises(DomainError, match="host progress"):
        await env.coordinator.process(value)
    with env.store.transaction(env.actor) as tx:
        assert not tx.list_work() and not tx.list_delivery()


async def test_mixed_obligations_still_require_their_own_complete_response(conversation_env):
    env = conversation_env
    value = source(env, REQUEST + "并记下我负责核对。")
    required = [*COMPUTE, {"kind": "business", "operation": "claim_task"}]
    # The missing business action must never be covered by a valid start ACK.
    env.models[value.id] = IndependentModel(
        reply("385。"),
        compute(),
        reply("运行了385。"),
        reply("仍然运行了385。"),
        reviews=[
            review("missing_evidence", required),
            review("repair_reply", required),
            review("repair_reply", required),
        ],
    )
    with pytest.raises(DomainError, match="host progress"):
        await env.coordinator.process(value)
    with env.store.transaction(env.actor) as tx:
        assert len(tx.list_work()) == 1 and not tx.list_team_tasks() and not tx.list_delivery()


async def test_real_quiet_wins_over_a_matched_receipt_ack(conversation_env):
    env = conversation_env
    value = source(env, REQUEST)

    async def quiet_then_review(_):
        await env.coordinator.process(source(env, "Pot，先保持安静。"))
        return TRACE["ack_review"]

    env.models[value.id] = IndependentModel(
        reply(TRACE["first_candidate"]),
        compute(),
        reply(TRACE["rejected_ack"]),
        reviews=[TRACE["first_review"], quiet_then_review],
    )
    result = await env.coordinator.process(value)
    assert result.delivery_id is None and result.decision.kind == "observe"
    with env.store.transaction(env.actor) as tx:
        assert len(tx.list_work()) == 1 and not tx.list_delivery()
