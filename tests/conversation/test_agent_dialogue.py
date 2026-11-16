"""Offline full dialogue through real create_agent and the PostgreSQL boundary.

All language/model turns are explicit synthetic fixtures, not live-model evidence.
The graph, tool dispatch, state reads, write commits and outbox writes are real.
"""

import asyncio
import json
from datetime import UTC, datetime
from uuid import uuid4

import pytest

from groupagent.domain import EventKind, GroupEvent, WorkState
from groupagent.execution.models import ModelReply, ModelToolCall, TokenUsage

pytestmark = pytest.mark.postgres
USAGE = TokenUsage(input_tokens=1, output_tokens=1)


def reply(text):
    return ModelReply(text=text, usage=USAGE)


def call(name, arguments, identifier=None):
    if name == "apply_business_action" and "candidate" in arguments:
        candidate = arguments["candidate"]
        kind = candidate["kind"]
        if kind in {"start_work", "update_work", "control_work"}:
            name = kind
        arguments = {
            key: value
            for key, value in candidate.items()
            if key not in {"kind", "reason_code", "directness", "confidence"}
        }
    return ModelReply(
        tool_calls=(ModelToolCall(id=identifier or uuid4().hex, name=name, arguments=arguments),),
        finish_reason="tool_calls",
        usage=USAGE,
    )


def action(kind, **values):
    return {
        "candidate": {
            "kind": kind,
            "reason_code": "synthetic_authorized_action",
            "directness": "direct",
            "confidence": 1,
            **values,
        }
    }


def fact_call(title="湿地", *, evidence=False):
    return call(
        "read_work_facts",
        {
            "selector": {"basis": "query", "title_query": title, "states": [], "quantifier": "one"},
            "include_evidence": evidence,
        },
    )


class ScriptedModel:
    name = "offline-natural-dialogue-through-real-langgraph"

    def __init__(self, *steps):
        self.steps, self.requests = list(steps), []

    async def complete(self, request):
        self.requests.append(request)
        assert {tool["function"]["name"] for tool in request.tools} == {
            "read_work_facts",
            "start_work",
            "update_work",
            "control_work",
            "apply_business_action",
            "observe",
        }
        assert "Decision schema:" not in request.messages[0].content
        step = self.steps.pop(0)
        if isinstance(step, BaseException):
            raise step
        if callable(step):
            step = step(request)
        return step


def last_observation(request):
    return json.loads(
        next(message.content for message in reversed(request.messages) if message.role == "tool")
    )


def source(env, text, *, actor=None, reply_to=None):
    stamp = datetime.now(UTC)
    event = GroupEvent(
        id=uuid4().hex,
        source_key=uuid4().hex,
        group_id=env.actor.group_id,
        actor_id=actor or env.actor.member_id,
        kind=EventKind.TEXT,
        text=text,
        occurred_at=stamp,
        received_at=stamp,
        reply_to=reply_to,
    )
    with env.store.transaction(env.coordinator.actor(event)) as tx:
        tx.ingest_event(event, "fixture:" + event.id, "1", 0)
    return event


async def turn(env, text, model, **changes):
    event = source(env, text, **changes)
    env.models[event.id] = model
    result = await env.coordinator.process(event)
    assert not model.steps
    return event, result


def work_rows(env):
    with env.store.transaction(env.actor) as tx:
        return tx.list_work()


async def test_full_document_dialogue_reads_facts_without_repetition_duplicate_or_resume(
    conversation_env,
):
    env = conversation_env
    goal = "Pot，请整理一份介绍沿海湿地的Word，配两张现成公开图片，核对图源后发到群里。"

    def accepted(request):
        observation = last_observation(request)
        assert observation["status"] == "committed" and observation["result"]["work_id"]
        with env.store.transaction(env.actor) as tx:
            assert len(tx.list_work()) == 1  # The host committed before this model turn.
            assert not tx.list_delivery()  # No pre-commit/model promise was enqueued.
        return reply("我已把湿地介绍和两张公开配图登记为同一项工作，做好并核对后发来。")

    _, started = await turn(
        env,
        goal,
        ScriptedModel(
            call(
                "apply_business_action",
                action(
                    "start_work",
                    goal=goal,
                    work_capabilities=["public_research", "public_images", "documents"],
                    public_image_count=2,
                    outputs=[{"format": "docx"}],
                ),
            ),
            accepted,
        ),
    )
    identifier = started.work_id
    assert identifier
    # The task remains durable through eight unrelated, independently selected turns.
    unrelated = [
        ("Pot，列表和元组有什么区别？", "列表可变，元组创建后不能修改元素。"),
        ("Pot，如何理解二分查找？", "每次排除一半候选范围。"),
        ("Pot，给今晚读书会起个名字。", "可以叫“灯下共读”。"),
        ("Pot，写一句关于秋天的短句。", "风过树梢，叶子把秋天递到窗边。"),
        ("Pot，解释一下押韵。", "押韵让句尾使用相近的韵母。"),
        ("Pot，为什么冰会浮在水上？", "通常冰的密度比液态水小。"),
        ("Pot，会议纪要怎么更简洁？", "按结论、负责人和截止时间组织。"),
        ("Pot，给我一个记单词的小方法。", "用短句复习，再隔一天回想。"),
    ]
    for question, answer in unrelated:
        model = ScriptedModel(reply(answer))
        _, result = await turn(env, question, model)
        assert result.decision.response == answer
        assert len(model.requests) == 1
        assert all(message.role != "tool" for message in model.requests[0].messages)
    with env.store.transaction(env.actor) as tx:
        work = tx.get_work(identifier)
        tx.transition_work(
            identifier,
            WorkState.PAUSED,
            expected_revision=work.revision,
            reason="candidate_validation_failed",
        )

    observations = []
    answers = [
        (
            "Pot，之前那份湿地Word到哪一步了？",
            "湿地文档暂停了。目前没有通过验证的Word，也没有已接收的发送记录。",
            False,
        ),
        (
            "Pot，你说的未确认是什么意思？",
            "这里的未确认指缺少能证明结果可用的验证记录，不是要你再次确认需求。",
            False,
        ),
        (
            "Pot，那两张图片已经放进去了吗？",
            "现有记录不能证明两张图片已经放进文档，输出和交付证据都还缺着。",
            False,
        ),
        (
            "Pot，能检查一下实际记录吗？",
            "我核对了执行、产物验证和交付记录：没有通过验证的Word，也没有发送回执；这次检查没有恢复任务。",
            True,
        ),
        (
            "Pot，别复读状态，告诉我现在到底缺什么。",
            "缺的是可核对的成品证据：Word验证通过记录，以及配图进入文档的验证结果。现有记录还不能支持已完成的结论。",
            True,
        ),
        (
            "Pot，所以是我还需要再批准一次吗？",
            "不需要重复批准原要求。任务记录仍在；这次暂停属于执行结果尚未通过检查，不能当作你没有授权。",
            False,
        ),
    ]
    for question, answer, evidence in answers:

        def inspect(request, expected=answer, inspecting=evidence):
            observed = last_observation(request)
            observations.append(observed)
            assert observed["facts"][0]["id"] == identifier
            assert observed["facts"][0]["state"] == "paused"
            assert observed["facts"][0]["outputs"]["validated_count"] == 0
            if inspecting:
                assert observed["inspection"]["performed"]
                assert not observed["inspection"]["work_resumed"]
            return reply(expected)

        model = ScriptedModel(fact_call(evidence=evidence), inspect)
        _, result = await turn(env, question, model)
        assert result.decision.response == answer
        assert result.work_id == identifier
        assert [
            call.name
            for request in model.requests
            for message in request.messages
            for call in message.tool_calls
        ] == ["read_work_facts"]
        assert len(work_rows(env)) == 1 and work_rows(env)[0].state == WorkState.PAUSED
    assert len({answer for _, answer, _ in answers}) == len(answers)
    assert len(observations) == len(answers)

    # A different member cannot kill someone else's task even if a model asks.
    denied = ScriptedModel(
        call(
            "apply_business_action",
            action(
                "control_work",
                control="cancel",
                work_selector={
                    "basis": "ids",
                    "ids": [identifier],
                    "states": [],
                    "quantifier": "one",
                },
            ),
        ),
        lambda request: reply("这项工作由另一位成员发起，当前没有取消权限。"),
    )
    _, rejection = await turn(env, "Pot，取消那份湿地Word。", denied, actor="synthetic-peer")
    assert rejection.work_id is None and work_rows(env)[0].state == WorkState.PAUSED
    with env.store.transaction(env.actor) as tx:
        before = len(tx.list_delivery())
    quiet = source(env, "Pot，没叫你就别回应，先安静。")
    quiet_result = await env.coordinator.process(quiet)
    assert quiet_result.decision.kind == "observe" and quiet_result.delivery_id is None
    with env.store.transaction(env.actor) as tx:
        assert len(tx.list_delivery()) == before
    assert work_rows(env)[0].state == WorkState.PAUSED


async def test_two_distinct_commits_return_ordered_receipts_then_one_natural_reply(
    conversation_env,
):
    env = conversation_env
    text = "Pot，帮我做一份节水Word，并记下我负责展示。"
    first = call(
        "apply_business_action",
        action(
            "start_work",
            goal="制作节水Word",
            work_capabilities=["documents"],
            outputs=[{"format": "docx"}],
        ),
        "first",
    )
    second = call(
        "apply_business_action",
        action("command", action={"op": "claim_task", "title": "负责展示"}),
        "second",
    )

    def after_first(request):
        assert last_observation(request)["status"] == "committed"
        return second

    def after_second(request):
        assert last_observation(request)["status"] == "committed"
        return reply("节水Word已经排队，也记下了你负责展示。")

    event, result = await turn(env, text, ScriptedModel(first, after_first, after_second))
    assert result.decision.response == "节水Word已经排队，也记下了你负责展示。"
    with env.store.transaction(env.actor) as tx:
        body = tx.get_decision(event.id)
        assert [row["tool_call_id"] for row in body["action_receipts"]] == ["first", "second"]
        assert not body["agent_pending_response"]
        assert len(tx.list_work()) == len(tx.list_team_tasks()) == len(tx.list_delivery()) == 1
    assert result.work_id == work_rows(env)[0].id
    assert result.action_result["work_ids"] == [result.work_id]
    duplicate = await env.coordinator.process(event)
    assert duplicate.duplicate


async def test_crash_after_commit_replays_receipt_and_finalizes_once(conversation_env):
    env = conversation_env
    model = ScriptedModel(
        call(
            "apply_business_action",
            action(
                "start_work",
                goal="制作节水Word",
                work_capabilities=["documents"],
                outputs=[{"format": "docx"}],
            ),
        ),
        asyncio.CancelledError(),
    )
    event = source(env, "Pot，帮我做一份节水Word。")
    env.models[event.id] = model
    with pytest.raises(asyncio.CancelledError):
        await env.coordinator.process(event)
    with env.store.transaction(env.actor) as tx:
        assert tx.get_decision(event.id)["agent_pending_response"]
        assert event.id in {row.id for row in tx.list_pending_events()}
        assert len(tx.list_work()) == 1 and not tx.list_delivery()
    # No model call on replay, so a newly generated tool ID cannot create a second work.
    result = await env.coordinator.process(event)
    assert result.delivery_id
    with env.store.transaction(env.actor) as tx:
        assert len(tx.list_work()) == len(tx.list_delivery()) == 1
        assert not tx.get_decision(event.id)["agent_pending_response"]
    assert (await env.coordinator.process(event)).duplicate


async def test_quiet_between_action_commit_and_final_text_suppresses_pending_ack(conversation_env):
    env = conversation_env
    event = source(env, "Pot，帮我做一份节水Word。")
    entered, release = asyncio.Event(), asyncio.Event()

    class WaitingModel(ScriptedModel):
        async def complete(self, request):
            if self.requests:
                entered.set()
                await release.wait()
            return await super().complete(request)

    model = WaitingModel(
        call(
            "apply_business_action",
            action(
                "start_work",
                goal="制作节水Word",
                work_capabilities=["documents"],
                outputs=[{"format": "docx"}],
            ),
        ),
        reply("已安排文档。"),
    )
    env.models[event.id] = model
    active = asyncio.create_task(env.coordinator.process(event))
    await asyncio.wait_for(entered.wait(), 5)
    quiet = source(env, "Pot，先保持安静。")
    assert (await env.coordinator.process(quiet)).delivery_id is None
    release.set()
    result = await asyncio.wait_for(active, 5)
    assert result.delivery_id is None
    with env.store.transaction(env.actor) as tx:
        assert not tx.list_delivery()
        assert not tx.get_decision(event.id)["agent_pending_response"]
        assert len(tx.list_work()) == 1


async def test_tool_call_and_same_action_replays_never_create_duplicate_work(conversation_env):
    env = conversation_env
    candidate = action(
        "start_work",
        goal="制作节水Word",
        work_capabilities=["documents"],
        outputs=[{"format": "docx"}],
    )
    first = call("apply_business_action", candidate, "initial")
    repeated = call("apply_business_action", candidate, "initial")
    alias = call("apply_business_action", candidate, "alias")
    conflicting = call(
        "apply_business_action",
        action("command", action={"op": "claim_task", "title": "未经此次调用授权的分工"}),
        "alias",
    )

    def repeat(request):
        assert last_observation(request)["status"] == "committed"
        return repeated

    def replay_alias(request):
        assert last_observation(request)["replayed"]
        return alias

    def conflict(request):
        assert last_observation(request)["replayed"]
        return conflicting

    def finish(request):
        observed = last_observation(request)
        assert observed["status"] == "rejected" and observed["code"] == "invalid_input"
        return reply("节水文档已排队；重复调用没有新增任务。")

    event, result = await turn(
        env, "Pot，帮我做节水Word。", ScriptedModel(first, repeat, replay_alias, conflict, finish)
    )
    assert result.delivery_id
    with env.store.transaction(env.actor) as tx:
        assert len(tx.list_work()) == 1 and not tx.list_team_tasks()
        receipts = tx.get_decision(event.id)["action_receipts"]
        assert [row["tool_call_id"] for row in receipts] == ["initial", "alias"]
        assert len({row["candidate_sha256"] for row in receipts}) == 1


async def test_withdrawn_source_during_post_commit_inference_never_delivers_old_text(
    conversation_env,
):
    env = conversation_env
    marker = "SYNTHETIC-REVOKED-SOURCE"
    original = source(env, marker + "是资料中的一句话。")
    request = source(env, "Pot，根据引用制作Word。", reply_to=original.id)
    entered, release = asyncio.Event(), asyncio.Event()

    class WaitingModel(ScriptedModel):
        async def complete(self, model_request):
            if self.requests:
                entered.set()
                await release.wait()
            return await super().complete(model_request)

    env.models[request.id] = WaitingModel(
        call(
            "apply_business_action",
            action(
                "start_work",
                goal="根据资料整理Word：" + marker,
                work_capabilities=["documents"],
                outputs=[{"format": "docx"}],
            ),
        ),
        reply("已经安排含有" + marker + "的文档。"),
    )
    active = asyncio.create_task(env.coordinator.process(request))
    await asyncio.wait_for(entered.wait(), 5)
    env.coordinator.memory.forget(env.actor, event_ids=[original.id])
    release.set()
    result = await asyncio.wait_for(active, 5)
    assert result.delivery_id is None and result.decision.kind in {"observe", "ignore"}
    with env.store.transaction(env.actor) as tx:
        body = tx.get_decision(request.id)
        assert body["privacy_redacted"] and marker not in json.dumps(body)
        assert not tx.list_delivery()
        assert tx.list_work()[0].state == WorkState.CANCELLED


async def test_forget_inside_open_turn_drops_pre_forget_receipts_and_old_model_text(
    conversation_env,
):
    env = conversation_env
    marker = "SYNTHETIC-ERASED-IN-TURN"
    original = source(env, marker + "是需要忘记的引用资料。")
    request = source(env, "Pot，依据引用创建独立文稿，随后忘记那条引用。", reply_to=original.id)
    first = call(
        "apply_business_action",
        action(
            "start_work",
            goal="整理引用中的内容：" + marker,
            work_capabilities=["documents"],
            outputs=[{"format": "docx"}],
        ),
        "create",
    )
    forget = call(
        "apply_business_action",
        action("command", action={"op": "forget", "event_ids": [original.id]}),
        "forget",
    )

    def after_create(model_request):
        assert last_observation(model_request)["status"] == "committed"
        return forget

    def after_forget(model_request):
        observed = last_observation(model_request)
        assert observed["status"] == "committed"
        return reply("已经处理，旧模型仍然记着：" + marker)

    env.models[request.id] = ScriptedModel(first, after_create, after_forget)
    result = await env.coordinator.process(request)
    assert marker not in (result.decision.response or "")
    with env.store.transaction(env.actor) as tx:
        body = tx.get_decision(request.id)
        assert marker not in json.dumps(body)
        assert body["agent_context_revoked"] and not body["agent_pending_response"]
        assert [receipt["tool_call_id"] for receipt in body["action_receipts"]] == ["forget"]
        assert all(marker not in delivery.payload_ref for delivery in tx.list_delivery())


@pytest.mark.parametrize("web_enabled", [False, True])
async def test_upper_bound_pdf_and_program_diagrams_do_not_gain_exact_pages_or_research(
    conversation_env,
    web_enabled,
):
    env = conversation_env
    env.coordinator.config = env.coordinator.config.model_copy(
        update={
            "features": env.coordinator.config.features.model_copy(
                update={"web_research": web_enabled}
            )
        }
    )
    text = (
        "Pot，请制作一份PDF，解释给定数值并放两张程序绘制的示意图。两页以内，不需要搜索或插画服务。"
    )

    def accepted(model_request):
        observed = last_observation(model_request)
        assert observed["status"] == "committed"
        with env.store.transaction(env.actor) as tx:
            current = tx.get_work(observed["result"]["work_id"])
            output = current.scope["outputs"][0]
            assert output["max_units"] == 2 and output.get("exact_units") is None
            assert current.scope["work_capabilities"] == ["compute", "documents"]
            assert current.scope["research_required"] is False
            assert not set(current.scope["required_tools"]) & {"web_search", "web_fetch"}
            assert current.acceptance_criteria == ["总页数不超过2页", "用程序绘制两张示意图"]
        return reply("已安排在两页以内完成PDF和两张程序图，不会追加搜索要求。")

    _, result = await turn(
        env,
        text,
        ScriptedModel(
            call(
                "start_work",
                {
                    "goal": text,
                    "work_capabilities": ["compute", "documents"],
                    "outputs": [{"format": "pdf", "max_units": 2}],
                    "acceptance_criteria": ["总页数不超过2页", "用程序绘制两张示意图"],
                },
            ),
            accepted,
        ),
    )
    assert (
        result.work_id
        and result.decision.response == "已安排在两页以内完成PDF和两张程序图，不会追加搜索要求。"
    )

    def explain_bound(model_request):
        observed = last_observation(model_request)
        output = observed["facts"][0]["requirements"]["required_outputs"][0]
        assert output["max_units"] == 2 and output.get("exact_units") is None
        return reply("上限是2页；内容完整并通过检查的1页PDF也符合要求，并不要求凑满2页。")

    _, checked = await turn(
        env,
        "Pot，这份PDF的页数上限是多少？",
        ScriptedModel(
            call(
                "read_work_facts",
                {
                    "selector": {
                        "basis": "ids",
                        "ids": [result.work_id],
                        "states": [],
                        "quantifier": "one",
                    }
                },
            ),
            explain_bound,
        ),
    )
    assert (
        checked.decision.response
        == "上限是2页；内容完整并通过检查的1页PDF也符合要求，并不要求凑满2页。"
    )
    assert len(work_rows(env)) == 1 and work_rows(env)[0].state == WorkState.QUEUED


async def test_revising_output_can_clear_page_bound_without_dropping_budget_nulls(conversation_env):
    from groupagent.conversation.actions import OutputRequirement

    env = conversation_env
    initial, started = await turn(
        env,
        "Pot，制作最多2页的PDF。",
        ScriptedModel(
            call(
                "start_work",
                {
                    "goal": "制作PDF，最多2页",
                    "work_capabilities": ["documents"],
                    "outputs": [{"format": "pdf", "max_units": 2}],
                },
            ),
            reply("PDF已排队，上限2页。"),
        ),
    )
    work = work_rows(env)[0]
    original_budget = dict(work.budget)
    assert work.scope["outputs"] == [{"format": "pdf", "max_units": 2, "expected_text": []}]
    _, revised = await turn(
        env,
        "Pot，继续同一份PDF，取消页数限制。",
        ScriptedModel(
            call(
                "update_work",
                {
                    "target_work_id": work.id,
                    "expected_revision": work.revision,
                    "goal": "制作同一份PDF，不设页数限制",
                    "work_capabilities": ["documents"],
                    "outputs": [{"format": "pdf", "max_units": None}],
                },
            ),
            reply("已移除同一份PDF的页数上限。"),
        ),
        reply_to=initial.id,
    )
    changed = work_rows(env)[0]
    assert revised.work_id == started.work_id == changed.id and len(work_rows(env)) == 1
    assert changed.goal_revision == 2
    assert changed.scope["outputs"] == [{"format": "pdf", "expected_text": []}]
    assert OutputRequirement.model_validate(changed.scope["outputs"][0]).accepts_unit_count(3)
    assert (
        changed.budget
        == original_budget
        == env.coordinator.config.execution.model_dump(mode="json")
    )
    assert any(value is None for value in original_budget.values())
