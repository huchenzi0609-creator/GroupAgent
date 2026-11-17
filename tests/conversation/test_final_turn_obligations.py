"""The final independent check may correct a deliberately wrong first intent.

Models and judgments are explicit synthetic fixtures. Host commits, pending
recovery, source/quiet checks and outbox writes use actual PostgreSQL.
"""

import inspect
import json

import pytest
from jsonschema import Draft202012Validator

from groupagent.conversation.actions import ACTION_ADAPTER
from groupagent.conversation.turns import ActionRequirement, TurnContract, TurnOutcome
from groupagent.coordinator import Decision
from groupagent.domain import DomainError
from groupagent.execution.models import ModelReply, ModelToolCall

from .test_agent_dialogue import source
from .test_turn_contract import USAGE, reply

pytestmark = pytest.mark.postgres
REQUEST = "Pot，用 Python 算出 1 到 10 的平方和，告诉我结果。"
COMPUTE = [{"kind": "start_work"}]


def review(outcome, required=(), *, reply_required=True):
    return {
        "outcome": outcome,
        "explanation": "按原请求核对方法义务，正确数字和代码不是运行回执。",
        "required_actions": list(required),
        "reply_required": reply_required,
    }


def compute(identifier="actual-delegation"):
    return ModelReply(
        tool_calls=(
            ModelToolCall(
                id=identifier,
                name="start_work",
                arguments={
                    "goal": REQUEST,
                    "work_capabilities": ["compute"],
                    "outputs": [{"format": "text"}],
                },
            ),
        ),
        finish_reason="tool_calls",
        usage=USAGE,
    )


class IndependentModel:
    name = "synthetic-independent-final-obligation-review"

    def __init__(
        self,
        *steps,
        reviews=(),
        allow_initial=True,
        initial_intent="conversation",
        initial_required=(),
    ):
        self.steps, self.reviews = list(steps), list(reviews)
        self.requests, self.review_requests = [], []
        self.allow_initial = allow_initial
        self.initial_intent = initial_intent
        self.initial_required = initial_required

    async def complete(self, request):
        marker = request.messages[0].content or ""
        if marker.startswith("TURN_CONTRACT_V1"):
            assert self.allow_initial, "A resumed corrected obligation must not be reclassified"
            return reply(
                json.dumps(
                    {
                        "recipient": "agent",
                        "intent": self.initial_intent,
                        "information_basis": "stable_knowledge",
                        "required_actions": list(self.initial_required),
                    }
                )
            )
        if marker.startswith("TURN_OUTCOME_V1"):
            self.review_requests.append(request)
            payload = json.loads(request.messages[1].content)
            assert "human_request" in payload and "turn_contract" not in payload
            assert "missing_host_actions" not in payload
            assert "tentative_interpretation" not in payload and "reply_required" not in payload
            catalog = {item["tool_name"]: item for item in payload["action_catalog"]}
            assert catalog["start_work"]["kind"] == "start_work"
            assert "actually calculate" in catalog["start_work"]["description"]
            assert {
                item["operation"] for item in catalog["apply_business_action"]["operations"]
            } == set(ACTION_ADAPTER.json_schema()["discriminator"]["mapping"])
            step = self.reviews.pop(0)
            if isinstance(step, BaseException):
                raise step
            value = step(payload) if callable(step) else step
            if inspect.isawaitable(value):
                value = await value
            return reply(json.dumps(value))
        self.requests.append(request)
        step = self.steps.pop(0)
        if isinstance(step, BaseException):
            raise step
        return step(request) if callable(step) else step


@pytest.mark.parametrize("initial_intent", ["conversation", "observe"])
async def test_correct_number_and_code_cannot_replace_requested_execution(
    conversation_env, initial_intent
):
    env = conversation_env
    value = source(env, REQUEST)

    def delegate(request):
        assert "start_work" in {item["function"]["name"] for item in request.tools}
        with env.store.transaction(env.actor) as tx:
            pending = tx.get_decision(value.id)
            assert pending["turn_contract"]["intent"] == "action"
            assert pending["agent_pending_response"] and pending["agent_needs_execution"]
            assert pending["action_receipts"] == []
            assert (
                pending["result"]["work_id"] is None and pending["result"]["action_result"] is None
            )
            assert not tx.list_work() and not tx.list_delivery()
        assert "Host current turn contract" in request.messages[-1].content
        return compute()

    model = IndependentModel(
        reply("平方和是385，代码可写成sum(i*i for i in range(1,11))。"),
        delegate,
        reply("已安排用 Python 实际计算，得到结果后发来。"),
        reviews=[review("missing_evidence", COMPUTE), review("committed", COMPUTE)],
        initial_intent=initial_intent,
    )
    env.models[value.id] = model
    result = await env.coordinator.process(value)
    with env.store.transaction(env.actor) as tx:
        work = tx.get_work(result.work_id)
        assert len(tx.list_work()) == len(tx.list_delivery()) == 1
        assert "python_compute" in work.scope["required_tools"]
        assert not tx.get_decision(value.id)["agent_pending_response"]
    assert len(model.requests) == 3 and len(model.review_requests) == 2
    assert result.decision.response != "385"


async def test_corrected_contract_survives_timeout_before_any_action_receipt(conversation_env):
    env = conversation_env
    value = source(env, REQUEST)
    env.models[value.id] = IndependentModel(
        reply("385；这里有一段代码。"),
        TimeoutError(),
        reviews=[review("missing_evidence", COMPUTE)],
    )
    with pytest.raises(DomainError) as caught:
        await env.coordinator.process(value)
    assert caught.value.code == "provider_timeout"
    with env.store.transaction(env.actor) as tx:
        pending = tx.get_decision(value.id)
        assert [item["kind"] for item in pending["turn_contract"]["required_actions"]] == [
            "start_work"
        ]
        assert pending["agent_pending_response"] and pending["agent_needs_execution"]
        assert pending["action_receipts"] == [] and not tx.list_work() and not tx.list_delivery()
    resumed = IndependentModel(
        compute(),
        reply("已安排 Python 计算。"),
        allow_initial=False,
        reviews=[review("committed", COMPUTE)],
    )
    env.models[value.id] = resumed
    result = await env.coordinator.process(value)
    assert result.work_id and result.delivery_id
    assert (await env.coordinator.process(value)).duplicate


@pytest.mark.parametrize(
    "failure",
    [
        TimeoutError(),
        {"outcome": "committed", "explanation": "Missing independent obligations field"},
    ],
)
async def test_unreviewed_visible_reply_never_becomes_a_completed_turn(conversation_env, failure):
    env = conversation_env
    value = source(env, REQUEST)
    env.models[value.id] = IndependentModel(reply("385，代码如下。"), reviews=[failure])
    with pytest.raises(DomainError) as caught:
        await env.coordinator.process(value)
    assert caught.value.code in {"provider_timeout", "turn_assessment_invalid"}
    with env.store.transaction(env.actor) as tx:
        assert not tx.list_delivery() and not tx.list_work() and tx.get_decision(value.id) is None


@pytest.mark.parametrize(
    "question,answer",
    [
        ("Pot，RAG是什么？", "RAG先检索资料，再据此生成回答。"),
        ("Pot，1到10的平方和是多少？", "平方和是385。"),
    ],
)
@pytest.mark.parametrize("initial_intent", ["conversation", "observe"])
async def test_concepts_and_direct_arithmetic_need_no_execution_work(
    conversation_env, question, answer, initial_intent
):
    env = conversation_env
    value = source(env, question)
    model = IndependentModel(
        reply(answer), reviews=[review("committed")], initial_intent=initial_intent
    )
    env.models[value.id] = model
    result = await env.coordinator.process(value)
    assert result.decision.response == answer and not result.work_id
    with env.store.transaction(env.actor) as tx:
        assert not tx.list_work() and len(tx.list_delivery()) == 1
    assert len(model.review_requests) == 1


async def test_quiet_during_final_review_suppresses_new_obligation_before_tools(conversation_env):
    env = conversation_env
    value = source(env, REQUEST)

    async def became_quiet(_):
        await env.coordinator.process(source(env, "Pot，先保持安静。"))
        return review("missing_evidence", COMPUTE)

    model = IndependentModel(reply("385。"), reviews=[became_quiet])
    env.models[value.id] = model
    result = await env.coordinator.process(value)
    assert result.decision.kind == "observe" and result.delivery_id is None
    with env.store.transaction(env.actor) as tx:
        assert not tx.list_work() and not tx.list_delivery()
        assert not tx.get_decision(value.id)["agent_pending_response"]


async def test_late_obligation_cannot_reopen_a_closed_canonical_turn(conversation_env):
    env = conversation_env
    value = source(env, "Pot，解释一下平方和公式。")
    actor = env.coordinator.actor(value)
    context = env.coordinator._context(actor, value)
    completed = env.coordinator._apply_decision(
        actor,
        value,
        Decision(
            kind="reply",
            response="平方和公式已有直接解释。",
            reason_code="canonical_reply",
            directness="direct",
            confidence=1,
        ),
        context,
    )
    env.coordinator._bind_agent_tools(actor, value, context)
    with env.store.transaction(actor) as tx:
        before = tx.get_decision(value.id)
    corrected = TurnContract(
        event_id=value.id,
        recipient="agent",
        intent="action",
        required_actions=(ActionRequirement(kind="start_work"),),
        evidence=context["_recipient_evidence"],
    )
    assert (await context["_persist_turn_contract"](corrected))["status"] == "closed"
    with env.store.transaction(actor) as tx:
        assert tx.get_decision(value.id) == before
        assert len(tx.list_delivery()) == 1 and tx.get_delivery(completed.delivery_id)


async def test_new_obligation_respects_read_only_role_and_delivers_concrete_blocker(
    conversation_env,
):
    env = conversation_env
    config = env.coordinator.config
    env.coordinator.config = config.model_copy(
        update={
            "group": config.group.model_copy(
                update={
                    "observer_ids": ["synthetic-observer"],
                    "read_only_qa_roles": ["observer"],
                }
            )
        }
    )
    value = source(env, REQUEST, actor="synthetic-observer")
    env.models[value.id] = IndependentModel(
        reply("385；可用代码计算。"), reviews=[review("missing_evidence", COMPUTE)]
    )
    result = await env.coordinator.process(value)
    assert result.decision.reason_code == "read_only_scope" and result.decision.response
    assert result.delivery_id and result.work_id is None and result.action_result is None
    with env.store.transaction(env.actor) as tx:
        assert not tx.list_work()
        body = tx.get_decision(value.id)
        assert body["action_receipts"] == [] and not body["agent_pending_response"]


async def test_missed_self_declaration_creates_human_task_without_delegating_work(conversation_env):
    env = conversation_env
    value = source(env, "Pot，这次演示稿我来负责，请记下。")
    required = [{"kind": "business", "operation": "claim_task"}]
    claim = ModelReply(
        tool_calls=(
            ModelToolCall(
                id="real-human-claim",
                name="apply_business_action",
                arguments={
                    "action": {"op": "claim_task", "title": "演示稿"},
                },
            ),
        ),
        finish_reason="tool_calls",
        usage=USAGE,
    )
    env.models[value.id] = IndependentModel(
        reply("记下了，你负责演示稿。"),
        claim,
        reply("已记下你的分工。"),
        reviews=[review("missing_evidence", required), review("committed", required)],
    )
    result = await env.coordinator.process(value)
    with env.store.transaction(env.actor) as tx:
        assert not tx.list_work() and len(tx.list_team_tasks()) == 1
    assert result.delivery_id


def test_outcome_requires_an_explicit_typed_obligation_list():
    with pytest.raises(ValueError):
        TurnOutcome.model_validate(
            {
                "outcome": "committed",
                "explanation": "No obligation assessment",
                "reply_required": True,
            }
        )


@pytest.mark.parametrize("value", [None, "false", "true", 0, 1])
def test_reply_obligation_is_a_required_strict_boolean(value):
    with pytest.raises(ValueError):
        TurnOutcome.model_validate(
            {
                "outcome": "committed",
                "explanation": "Invalid reply obligation",
                "required_actions": [],
                "reply_required": value,
            }
        )


async def test_silent_reply_flag_does_not_discharge_unexecuted_obligations(conversation_env):
    env = conversation_env
    value = source(env, REQUEST)
    observe = ModelReply(
        tool_calls=(
            ModelToolCall(
                id="provisional-silence", name="observe", arguments={"reason_code": "silent"}
            ),
        ),
        finish_reason="tool_calls",
        usage=USAGE,
    )
    env.models[value.id] = IndependentModel(
        reply("385。"),
        observe,
        reply(""),
        reviews=[
            review("missing_evidence", COMPUTE, reply_required=False),
            review("missing_evidence", COMPUTE, reply_required=False),
        ],
    )
    with pytest.raises(DomainError, match="host progress"):
        await env.coordinator.process(value)
    with env.store.transaction(env.actor) as tx:
        assert not tx.list_work() and not tx.list_delivery()
        body = tx.get_decision(value.id)
        assert body["agent_needs_execution"] and body["agent_pending_response"]
        assert body["turn_contract"]["reviewed_reply_required"] is False


async def test_accepted_work_can_honor_a_human_request_for_no_admission_ack(conversation_env):
    env = conversation_env
    value = source(env, REQUEST + "受理后不用发确认。")
    env.models[value.id] = IndependentModel(
        reply("385。"),
        compute(),
        reply(""),
        reviews=[
            review("missing_evidence", COMPUTE, reply_required=False),
            review("committed", COMPUTE, reply_required=False),
        ],
    )
    result = await env.coordinator.process(value)
    with env.store.transaction(env.actor) as tx:
        assert len(tx.list_work()) == 1 and not tx.list_delivery()
        assert not tx.get_decision(value.id)["agent_pending_response"]
    assert result.work_id and result.decision.kind == "observe"


async def test_reply_only_correction_survives_timeout_without_creating_work(conversation_env):
    env = conversation_env
    value = source(env, "Pot，RAG是什么？")
    env.models[value.id] = IndependentModel(
        reply("这需要先训练大模型。"),
        TimeoutError(),
        initial_intent="observe",
        reviews=[review("repair_reply")],
    )
    with pytest.raises(DomainError):
        await env.coordinator.process(value)
    with env.store.transaction(env.actor) as tx:
        body = tx.get_decision(value.id)
        assert body["turn_contract"]["reviewed_reply_required"] is True
        assert body["turn_contract"]["intent"] == "conversation"
        assert not tx.list_work() and not tx.list_delivery()
    env.models[value.id] = IndependentModel(
        reply("RAG先检索材料，再基于材料生成回答。"),
        allow_initial=False,
        reviews=[review("committed")],
    )
    result = await env.coordinator.process(value)
    assert result.delivery_id and result.work_id is None


async def test_independent_complete_obligations_clear_an_uncommitted_initial_false_positive(
    conversation_env,
):
    env = conversation_env
    value = source(env, "Pot，给我一段计算1到10平方和的Python代码示例。")
    model = IndependentModel(
        reply("示例：sum(i*i for i in range(1, 11))"),
        initial_intent="action",
        initial_required=COMPUTE,
        reviews=[review("committed")],
    )
    env.models[value.id] = model
    result = await env.coordinator.process(value)
    assert result.delivery_id and result.work_id is None
    with env.store.transaction(env.actor) as tx:
        assert not tx.list_work()
        assert tx.get_decision(value.id)["turn_contract"]["required_actions"] == []


@pytest.mark.parametrize(
    "requirement",
    [
        {"kind": "business", "operation": "run_python"},
        {"kind": "business", "operation": None},
        {"kind": "start_work", "operation": "claim_task"},
        {"kind": "read_work_facts", "operation": "remember"},
    ],
)
async def test_invalid_operations_are_rejected_by_both_schema_and_plain_json(
    conversation_env, requirement
):
    env = conversation_env
    value = source(env, REQUEST)
    model = IndependentModel(reply("385。"), reviews=[review("missing_evidence", [requirement])])
    env.models[value.id] = model
    with pytest.raises(DomainError) as caught:
        await env.coordinator.process(value)
    assert caught.value.code == "turn_assessment_invalid" and caught.value.retryable
    parameters = model.review_requests[0].tools[0]["function"]["parameters"]
    action_schema = {"$defs": parameters["$defs"], "$ref": "#/$defs/ActionRequirement"}
    assert not Draft202012Validator(action_schema).is_valid(requirement)
    with env.store.transaction(env.actor) as tx:
        assert not tx.list_work() and not tx.list_delivery() and tx.get_decision(value.id) is None
