"""Scripted model traces through the real foreground ReAct graph and host receipts."""

import json

import pytest

from groupagent.conversation.policy import apply_quiet_barrier_in_tx, delivery_admission_in_tx
from groupagent.conversation.provider import ModelDecisionProvider
from groupagent.domain import DeliveryState, DomainError, WorkState
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
    assert model.requests[0].tool_choice == "required"
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
@pytest.mark.parametrize("control", ["resume", "retry"])
async def test_continuation_of_live_work_reports_current_facts_without_a_revision(
    conversation_env, control
):
    env = conversation_env
    original = source(env, "Pot，把两种封套的实际对照结果写成小结。")
    with env.store.transaction(env.actor) as tx:
        work = tx.create_work("两种封套的对照小结", original.id)
        tx.claim_work(work.id, "synthetic-active-worker", ttl_seconds=300)
        running = tx.get_work(work.id)

    event = source(env, "Pot，刚才那份小结有进展吗？接着做就好。")
    selector = {"basis": "ids", "ids": [work.id]}

    def check_current_state(request):
        receipt = tool_receipt(request)
        assert receipt["status"] == "observed" and receipt["committed"] is False
        assert receipt["code"] == "work_control_state_observed"
        (fact,) = receipt["facts"]["facts"]
        assert fact["id"] == work.id and fact["state"] == "running"
        assert fact["revision"] == running.revision
        return finish("两种封套的对照小结正在处理。")

    model = ModelScript(
        call("control_work", "continuation", work_selector=selector, control=control),
        check_current_state,
    )
    env.models[event.id] = model
    result = await env.coordinator.process(event)
    assert result.decision.response == "两种封套的对照小结正在处理。"
    assert result.delivery_id
    with env.store.transaction(env.actor) as tx:
        assert tx.get_work(work.id) == running
        assert len(tx.list_work()) == len(tx.list_delivery()) == 1


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


def queued_compute(event, identifier="compute", **fields):
    return call(
        "start_work",
        identifier,
        goal=event.text,
        work_capabilities=["compute"],
        required_work_tools=["python_compute"],
        outputs=[{"format": "text"}],
        **fields,
    )


def pair(write, closing):
    return write.model_copy(update={"tool_calls": (*write.tool_calls, *closing.tool_calls)})


def offered_properties(request, name):
    return next(
        row["function"]["parameters"]["properties"]
        for row in request.tools
        if row["function"]["name"] == name
    )


@pytest.mark.postgres
@pytest.mark.parametrize("supplement", [None, "平方是数与自身相乘。"])
async def test_paired_finish_uses_verified_commit_and_one_canonical_delivery(
    conversation_env, supplement
):
    env = conversation_env
    text = "Pot，请运行 Python 计算 13 的平方。"
    if supplement:
        text += "同时先告诉我平方是什么意思。"
    event = source(env, text)

    def plan(request):
        fields = offered_properties(request, "start_work")
        assert "topic_id" not in fields
        assert fields["input_versions"]["maxItems"] == fields["input_event_ids"]["maxItems"] == 0
        return pair(
            queued_compute(event, admission_ack="deferred"),
            finish(supplement, reports=[("compute", "queued")]),
        )

    model = ModelScript(plan)
    env.models[event.id] = model
    result = await env.coordinator.process(event)
    assert result.work_id and result.delivery_id and len(model.requests) == 1
    assert "实际运行 Python" in result.decision.response
    if supplement is not None:
        assert result.decision.response.count(supplement) == 1
    replay = await env.coordinator.process(event)
    assert replay.duplicate and replay.delivery_id == result.delivery_id
    with env.store.transaction(env.actor) as tx:
        assert len(tx.list_work()) == len(tx.list_delivery()) == 1
        trace = tx.get_decision(event.id)["turn_outcome"]
        assert trace["actions"] == [{"tool": "start_work", "status": "committed"}]
        assert trace["receipt_ids"] == ["compute"]


@pytest.mark.postgres
async def test_rejected_write_does_not_publish_paired_finish_and_topic_lookup_refreshes_schema(
    conversation_env, monkeypatch
):
    env = conversation_env
    event = source(env, "Pot，用 Python 实际计算 17 的平方。")
    topic = env.coordinator.memory.create_topic(env.actor, "Known synthetic topic")
    unlinked = env.coordinator.memory.create_topic(env.actor, "Unlinked synthetic topic")
    env.coordinator.memory.link_event(env.actor, event.id, topic.id, evidence_event_ids=[event.id])
    read_context = env.coordinator._context

    def bounded_preview(actor, event, query=None, **options):
        projected = read_context(actor, event, query, **options)
        # Exercise a preview that omitted the optional reference. The actual
        # topic read still uses the production snapshot and PostgreSQL query.
        return {**projected, "topics": []} if query is None else projected

    monkeypatch.setattr(env.coordinator, "_context", bounded_preview)
    bad = pair(
        queued_compute(event, "bad-reference", topic_id="invented"),
        finish("This response has no committed operation."),
    )

    def load(request):
        receipt = tool_receipt(request)
        assert receipt["status"] == "rejected"
        assert receipt["available_topic_ids"] == []
        assert "topic_id" not in offered_properties(request, "start_work")
        with env.store.transaction(env.actor) as tx:
            assert not tx.list_work() and not tx.list_delivery()
        return call("read_group_context", "topics", resource="topics")

    def corrected(request):
        assert tool_receipt(request)["facts"] == [
            {"id": topic.id, "title": topic.title, "visibility": "group"}
        ]
        fields = offered_properties(request, "start_work")
        assert fields["topic_id"]["enum"] == [topic.id]
        assert unlinked.id not in fields["topic_id"]["enum"]
        return pair(
            queued_compute(event, "authorized-topic", topic_id=topic.id),
            finish(reports=[("authorized-topic", "queued")]),
        )

    model = ModelScript(bad, load, corrected)
    env.models[event.id] = model
    result = await env.coordinator.process(event)
    assert result.work_id and result.delivery_id and len(model.requests) == 3
    assert "no committed operation" not in result.decision.response
    with env.store.transaction(env.actor) as tx:
        assert tx.get_work(result.work_id).scope["topic_id"] == topic.id
        assert len(tx.list_work()) == len(tx.list_delivery()) == 1


@pytest.mark.postgres
async def test_invalid_paired_report_recovers_without_reexecuting_committed_write(
    conversation_env,
):
    env = conversation_env
    event = source(env, "Pot，用 Python 实际计算 11 的平方。")

    def use_actual_receipt(request):
        feedback = json.loads(request.messages[-1].content)
        assert feedback["code"] == "report_requires_current_matching_receipt"
        assert feedback["available_reports"] == [{"tool_call_id": "compute", "phase": "queued"}]
        with env.store.transaction(env.actor) as tx:
            assert len(tx.list_work()) == 1 and not tx.list_delivery()
        return finish(reports=[("compute", "queued")])

    model = ModelScript(
        pair(queued_compute(event), finish("这已经完成了。", reports=[("compute", "committed")])),
        use_actual_receipt,
    )
    env.models[event.id] = model
    result = await env.coordinator.process(event)
    assert result.work_id and result.delivery_id and len(model.requests) == 2
    assert "这已经完成了" not in result.decision.response
    with env.store.transaction(env.actor) as tx:
        assert len(tx.list_work()) == len(tx.list_delivery()) == 1


@pytest.mark.postgres
@pytest.mark.parametrize("mode", ["immediate", "deferred", "paired", "finish_on_commit"])
async def test_canonical_research_receipt_names_the_committed_goal_on_every_path(
    conversation_env, mode
):
    env = conversation_env
    event = source(env, "Pot，请查证 Lumen Reader 的公开资料。")
    goal = "核对 Lumen Reader 的官方下载说明"
    start = call(
        "start_work",
        "lookup",
        goal=goal,
        work_capabilities=["public_research"],
        outputs=[{"format": "text"}],
        admission_ack="deferred" if mode == "deferred" else "immediate",
        finish_on_commit=mode == "finish_on_commit",
    )
    if mode == "paired":
        model = ModelScript(pair(start, finish("我已开始安排接下来的核对。")))
    elif mode == "finish_on_commit":
        model = ModelScript(start)
    else:
        model = ModelScript(start, finish())
    env.models[event.id] = model
    outcome = await env.coordinator.process(event)
    with env.store.transaction(env.actor) as tx:
        (delivery,) = tx.list_delivery()
        (work,) = tx.list_work()
        assert work.goal == goal and work.state == WorkState.QUEUED
        assert delivery.payload_ref == "text:" + outcome.decision.response
        assert goal in outcome.decision.response
        assert "接下来的核对" not in outcome.decision.response
        assert outcome.delivery_id == delivery.id
        assert delivery.idempotency_key == (
            ("early_ack:" if mode == "immediate" else "reply:") + event.id + ":1"
        )


@pytest.mark.postgres
async def test_same_turn_repair_retires_only_unattempted_receipt_with_stale_goal(conversation_env):
    env = conversation_env
    event = source(env, "Pot，核对 Lumen Reader 的官方安装资料，然后给我说明。")
    start = call(
        "start_work",
        "draft",
        goal="写一份关于火山的Word。",
        work_capabilities=["documents"],
        outputs=[{"format": "docx"}],
        admission_ack="immediate",
    )
    first_intents = []

    def repair(request):
        observed = tool_receipt(request)
        effect = observed["work_effects"][0]
        first_intents.append(observed["work_admission"]["delivery_id"])
        return call(
            "update_work",
            "repaired",
            target_work_id=effect["id"],
            expected_revision=effect["revision"],
            goal=event.text,
            work_capabilities=["public_research"],
            outputs=[{"format": "text"}],
        )

    def complete(request):
        assert tool_receipt(request)["committed"]
        assert tool_receipt(request)["work_admission"] is None
        return finish(reports=[("repaired", "committed")])

    env.models[event.id] = ModelScript(start, repair, complete)
    outcome = await env.coordinator.process(event)
    assert "Lumen Reader" in outcome.decision.response
    assert "Word" not in outcome.decision.response
    with env.store.transaction(env.actor) as tx:
        (work,) = tx.list_work()
        assert work.goal_revision == 2
        old = tx.get_delivery(first_intents[0])
        final = tx.get_delivery(outcome.delivery_id)
        assert old.state == DeliveryState.REJECTED and old.platform_message_id is None
        assert final.state == DeliveryState.PENDING and final.id != old.id
        assert final.request_event_id == old.request_event_id == event.id


@pytest.mark.postgres
@pytest.mark.parametrize("finisher_text", [None, "材料已经交给后台制作。"])
async def test_real_queued_ack_precedes_same_work_refinement_and_is_not_repeated(
    conversation_env, finisher_text
):
    env = conversation_env
    event = source(env, "Pot，请从同一份资料生成 Word、PDF 和 PPT。")
    initial = call(
        "start_work",
        "initial",
        admission_ack="immediate",
        goal=event.text,
        work_capabilities=["documents"],
        outputs=[{"format": "docx"}, {"format": "pdf"}],
    )
    admission_ids = []

    def refine(request):
        receipt = tool_receipt(request)
        work = receipt["work_effects"][0]
        admission = receipt["work_admission"]
        assert receipt["committed"] is True and work["goal_revision"] == 1
        with env.store.transaction(env.actor) as tx:
            (queued,) = tx.list_delivery()
            assert queued.id == admission["delivery_id"]
            assert queued.request_event_id == event.id
            assert queued.idempotency_key == "early_ack:" + event.id + ":1"
            assert queued.payload_ref.startswith("text:这项工作已排队。")
            original = tx.get_work(work["id"])
            assert original.state == WorkState.QUEUED
            assert original.goal in queued.payload_ref
            assert delivery_admission_in_tx(tx, queued).allowed
            claimed = tx.claim_delivery("fixture-sender", intent_id=queued.id)
            assert claimed is not None
            tx.finish_delivery(
                claimed.id,
                claimed.claim_token,
                DeliveryState.ACCEPTED,
                platform_message_id="fixture:work-admission",
            )
        admission_ids.append(queued.id)
        return call(
            "update_work",
            "refined",
            goal=event.text,
            target_work_id=work["id"],
            expected_revision=work["revision"],
            work_capabilities=["documents"],
            outputs=[{"format": "docx"}, {"format": "pdf"}, {"format": "pptx"}],
        )

    def close(request):
        receipt = tool_receipt(request)
        assert receipt["committed"] is True and receipt["satisfies_kind"] == "start_work", receipt
        assert receipt["work_admission"]["delivery_id"] == admission_ids[0]
        return finish(finisher_text, reports=[("refined", "committed")])

    model = ModelScript(initial, refine, close)
    env.models[event.id] = model
    result = await env.coordinator.process(event)
    assert result.work_id and len(model.requests) == 3
    with env.store.transaction(env.actor) as tx:
        work = tx.get_work(result.work_id)
        assert work.goal_revision == 2
        assert {row["format"] for row in work.scope["outputs"]} == {"docx", "pdf", "pptx"}
        assert len(tx.list_work()) == 1
        (admitted,) = tx.list_delivery()
        assert admitted.id == admission_ids[0]
        assert admitted.state == DeliveryState.ACCEPTED
        assert result.delivery_id == admitted.id
        assert result.decision.response == admitted.payload_ref.removeprefix("text:")
        assert tx.get_decision(event.id)["work_admission"]["delivery_id"] == admitted.id


@pytest.mark.postgres
async def test_untyped_model_ack_uses_the_already_queued_canonical_receipt(conversation_env):
    env = conversation_env
    event = source(env, "Pot，请计算一个合成数值。")
    env.models[event.id] = ModelScript(
        queued_compute(event, admission_ack="immediate"),
        ModelReply(text="好的，我已经把这个任务放进队列了。", usage=USAGE),
    )
    result = await env.coordinator.process(event)
    with env.store.transaction(env.actor) as tx:
        (admission,) = tx.list_delivery()
        assert result.delivery_id == admission.id
        assert result.decision.response == admission.payload_ref.removeprefix("text:")
        assert admission.idempotency_key == "early_ack:" + event.id + ":1"


@pytest.mark.postgres
async def test_immediate_admission_keeps_a_later_committed_action_receipt(conversation_env):
    env = conversation_env
    event = source(env, "Pot，请用 Python 算 13 的平方，并记下项目代号是山鲸。")
    additional = call(
        "apply_business_action",
        "remember-project",
        action={"op": "remember", "predicate": "项目代号", "value": "山鲸"},
    )
    acknowledgements = []

    def close(request):
        receipt = tool_receipt(request)
        assert receipt["committed"]
        acknowledgements.append(receipt["host_message"])
        return finish("我还安排了后台实算。")

    env.models[event.id] = ModelScript(
        queued_compute(event, admission_ack="immediate"), additional, close
    )
    result = await env.coordinator.process(event)
    assert result.work_id and result.decision.response == acknowledgements[0]
    with env.store.transaction(env.actor) as tx:
        assert {row.idempotency_key: row.payload_ref for row in tx.list_delivery()} == {
            "early_ack:" + event.id + ":1": "text:这项工作已排队。目标是「" + event.text + "」。",
            "reply:" + event.id + ":1": "text:" + acknowledgements[0],
        }


@pytest.mark.postgres
async def test_existing_legacy_ack_slot_preserves_work_and_falls_back_to_final_reply(
    conversation_env,
):
    env = conversation_env
    event = source(env, "Pot，请用 Python 计算一个平方。")
    with env.store.transaction(env.actor) as tx:
        legacy = tx.enqueue_early_ack(event.id, "Legacy placeholder.")
        claimed = tx.claim_delivery("fixture-sender", intent_id=legacy.id)
        assert claimed is not None
        tx.finish_delivery(
            legacy.id,
            claimed.claim_token,
            DeliveryState.ACCEPTED,
            platform_message_id="fixture:accepted-legacy",
        )

    def close(request):
        receipt = tool_receipt(request)
        assert receipt["committed"] is True and receipt["work_admission"] is None
        return finish("已经把计算任务交给后台。")

    env.models[event.id] = ModelScript(queued_compute(event, admission_ack="immediate"), close)
    result = await env.coordinator.process(event)
    assert result.work_id and result.delivery_id != legacy.id
    assert "交给后台" not in result.decision.response
    with env.store.transaction(env.actor) as tx:
        kept = tx.get_delivery(legacy.id)
        assert kept.state == DeliveryState.ACCEPTED
        assert kept.payload_ref == legacy.payload_ref
        assert tx.get_delivery(result.delivery_id).idempotency_key == "reply:" + event.id + ":1"
        assert len(tx.list_work()) == 1


@pytest.mark.postgres
async def test_explicit_silent_admission_keeps_work_without_a_visible_receipt(conversation_env):
    env = conversation_env
    event = source(env, "Pot，请计算并只在结果完成时发消息。")
    model = ModelScript(queued_compute(event, admission_ack="silent"), finish())
    env.models[event.id] = model
    result = await env.coordinator.process(event)
    assert result.work_id and not result.delivery_id
    assert result.decision.kind == "observe"
    with env.store.transaction(env.actor) as tx:
        assert len(tx.list_work()) == 1 and not tx.list_delivery()
        assert tx.get_decision(event.id)["work_admission"] == {
            "work_id": result.work_id,
            "silent": True,
        }


@pytest.mark.postgres
async def test_quiet_barrier_retires_unsent_admission_without_cancelling_work(conversation_env):
    env = conversation_env
    event = source(env, "Pot，请运行 Python 计算平方。")

    def quiet_before_send(request):
        receipt = tool_receipt(request)
        quiet = source(env, "Pot，请暂时保持安静。")
        with env.store.transaction(env.actor) as tx:
            tx.conn.execute(
                "INSERT INTO ga_conversation_policy(group_id,subject_id,quiet) VALUES(%s,'*',true)",
                (env.actor.group_id,),
            )
            assert apply_quiet_barrier_in_tx(tx, quiet, "*") == 1
            pending = tx.get_delivery(receipt["work_admission"]["delivery_id"])
            assert pending.state == DeliveryState.REJECTED and not pending.platform_message_id
        return finish()

    env.models[event.id] = ModelScript(
        queued_compute(event, admission_ack="immediate"), quiet_before_send
    )
    result = await env.coordinator.process(event)
    assert result.work_id and result.decision.kind == "observe"
    with env.store.transaction(env.actor) as tx:
        assert tx.get_work(result.work_id).state == WorkState.QUEUED
        (ack,) = tx.list_delivery()
        assert ack.state == DeliveryState.REJECTED


@pytest.mark.postgres
async def test_receipt_reused_after_model_interruption_and_event_replay(conversation_env):
    env = conversation_env
    event = source(env, "Pot，用 Python 实际算一遍 7 的平方。")
    start = call(
        "start_work",
        "python-start",
        admission_ack="immediate",
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
        assert len(tx.list_work()) == 1
        (ack,) = tx.list_delivery()
        assert ack.state == DeliveryState.PENDING
        body = tx.get_decision(event.id)
        assert body["agent_pending_response"] and body["agent_needs_execution"]
        assert body["work_admission"]["delivery_id"] == ack.id
        claimed = tx.claim_delivery("fixture-sender", intent_id=ack.id)
        assert claimed is not None
        tx.finish_delivery(
            claimed.id,
            claimed.claim_token,
            DeliveryState.ACCEPTED,
            platform_message_id="fixture:admitted-before-replay",
        )

    def resumed(request):
        prior = json.loads(request.messages[1].content)["prior_receipts"]
        assert len(prior) == 1 and prior[0]["tool_call_id"] == "python-start"
        assert prior[0]["work_admission"]["delivery_id"] == ack.id
        assert prior[0]["admission_ack"] == "immediate"
        return finish("计算已交给后台处理。", reports=[("python-start", "queued")])

    model = ModelScript(resumed)
    env.models[event.id] = model
    result = await env.coordinator.process(event)
    assert result.work_id and result.delivery_id == ack.id
    replay = await env.coordinator.process(event)
    assert replay.duplicate and replay.work_id == result.work_id
    assert len(model.requests) == 1
    with env.store.transaction(env.actor) as tx:
        assert len(tx.list_work()) == len(tx.list_delivery()) == 1


@pytest.mark.postgres
async def test_exact_addressed_stop_uses_existing_work_without_model(conversation_env):
    env = conversation_env
    event = source(env, "Pot，请整理一份活动流程表。")
    env.models[event.id] = ModelScript(
        call(
            "start_work",
            "make-plan",
            goal=event.text,
            work_capabilities=["documents"],
            outputs=[{"format": "docx"}],
        ),
        finish(),
    )
    work = await env.coordinator.process(event)
    stop = source(env, "Pot停止")
    model = ModelScript()
    env.models[stop.id] = model
    result = await env.coordinator.process(stop)
    assert not model.requests
    assert result.controlled_work_ids == [work.work_id]
    with env.store.transaction(env.actor) as tx:
        assert tx.get_work(work.work_id).state == WorkState.CANCELLED
