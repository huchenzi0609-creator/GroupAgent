"""Actual PostgreSQL/local-spool identity contracts, with offline model fixtures."""

from __future__ import annotations

import json
import re
from datetime import timedelta

import pytest

from groupagent.coordinator import Decision
from groupagent.domain import DeliveryState, DomainError, WorkState

from .test_app import app_for, event
from .test_application_controls import admit, waiting_work

pytestmark = pytest.mark.postgres


def outgoing(env):
    return [json.loads(path.read_text()) for path in (env.root / "channel/outgoing").glob("*.json")]


def assert_pot_texts(rows):
    assert rows
    for row in rows:
        text = row["payload"]["text"]
        assert text.startswith("【Pot】") and not text[5:].lstrip().startswith("【Pot】")
        assert "机器人组长" not in text


async def test_real_context_and_wire_identity_keep_shared_account_echo_receipt_based(runtime_env):
    env = runtime_env
    app, _ = app_for(env, {})
    app.config = app.config.model_copy(
        update={
            "group": app.config.group.model_copy(
                update={"bot_member_id": env.actor.member_id, "shared_account": True}
            )
        }
    )
    app.coordinator.config = app.config
    contexts = []

    class OfflineIdentityProvider:
        async def decide(self, value, ctx):
            contexts.append(ctx)
            return Decision(
                kind="reply",
                reason_code="synthetic_identity",
                response="【Pot】【Pot】我是Pot。",
                directness="direct",
                confidence=1,
            )

    app.coordinator.provider_factory = lambda *_: OfflineIdentityProvider()
    try:
        request = event(
            env, "pOt，请介绍你在群里的协作方式。", platform_message_id="synthetic-human-request"
        )
        await admit(app, env, request)
        assert contexts[0]["agent_name"] == "Pot"
        assert contexts[0]["bot_badge"] == "【Pot】" and "Pot" in contexts[0]["agent_aliases"]
        assert await app.dispatcher.tick() == 1
        rows = outgoing(env)
        assert_pot_texts(rows)
        wire = rows[0]
        assert wire["payload"]["text"] == "【Pot】我是Pot。"
        receipt_id = wire["receipt"]["platform_message_id"]

        echo = event(env, wire["payload"]["text"], platform_message_id=receipt_id)
        echo_result = await admit(app, env, echo)
        assert echo_result["result"]["decision"]["kind"] == "ignore"
        assert len(contexts) == 1

        # A human using the same account and exact badge/text is still a human
        # message when its authenticated channel message ID is different.
        human = event(env, wire["payload"]["text"], platform_message_id="synthetic-human-copy")
        human_result = await admit(app, env, human)
        assert human_result["result"]["decision"]["kind"] == "reply"
        assert len(contexts) == 2
    finally:
        await app.close()


async def test_pot_fast_admission_ack_and_case_insensitive_control_need_no_model(runtime_env):
    env = runtime_env
    app, models = app_for(env, {})

    class NoConversationModel:
        async def decide(self, *args):
            raise AssertionError("Pot's explicit independent work/control should route locally")

    app.coordinator.provider_factory = lambda *_: NoConversationModel()
    try:
        request = event(env, "Pot帮我生成介绍太阳系的十页PPT。")
        result = await admit(app, env, request)
        work_id = result["result"]["work_id"]
        with env.store.transaction(env.actor) as tx:
            work = tx.get_work(work_id)
            assert work.goal == request.text and work.scope["outputs"][0]["exact_units"] == 10
        assert await app.dispatcher.tick() == 1
        assert_pot_texts(outgoing(env))

        stop = event(env, "@pOt，暂停这个任务")
        env.channel.append_incoming(stop)
        await app.poll_intake()
        assert await app.process_pending(controls_only=True) == 1
        with env.store.transaction(env.actor) as tx:
            assert tx.get_work(work_id).state == WorkState.PAUSED
        assert not models
    finally:
        await app.close()


async def test_named_human_self_claim_creates_only_a_member_task(runtime_env):
    env = runtime_env
    request = event(env, "Pot，访谈摘要我来负责，不用你代写。")
    app, models = app_for(
        env,
        {
            request.id: Decision(
                kind="reply",
                reason_code="synthetic_named_self_claim",
                response="已记下你的分工。",
                action={"op": "claim_task", "title": "访谈摘要"},
                directness="direct",
                confidence=1,
            )
        },
    )
    try:
        await admit(app, env, request)
        with env.store.transaction(env.actor) as tx:
            tasks = tx.list_team_tasks()
            assert len(tasks) == 1 and tasks[0].owner_id == env.actor.member_id
            assert tasks[0].title == "访谈摘要" and tx.list_work() == []
        assert not models
        assert await app.dispatcher.tick() == 1
        assert_pot_texts(outgoing(env))
    finally:
        await app.close()


async def test_named_reminder_preserves_quoted_pot_call_as_recipient_data(runtime_env):
    env = runtime_env
    request = event(env, "Pot，请在十分钟后提醒我“并Pot生成PDF”。")
    when = request.occurred_at + timedelta(minutes=10)
    app, models = app_for(
        env,
        {
            request.id: Decision(
                kind="reply",
                reason_code="synthetic_named_reminder",
                response="到时提醒你。",
                action={
                    "op": "create_schedule",
                    "title": "合成提醒",
                    "prepared_message": {"text": "并Pot生成PDF", "purpose": "explicit_reminder"},
                    "timing": {"kind": "at", "at": when.isoformat()},
                },
                directness="direct",
                confidence=1,
            )
        },
    )
    try:
        await admit(app, env, request)
        rules = app.coordinator.scheduler.list(env.actor)
        assert len(rules) == 1 and rules[0].goal == "并Pot生成PDF"
        assert rules[0].execution_kind.value == "explicit_reminder"
        assert rules[0].scope["prepared_message_source_id"] == request.id
        occurrence = app.coordinator.scheduler.due(env.actor, when)[0]
        with env.store.transaction(env.actor) as tx:
            assert occurrence.work_id is None and occurrence.delivery_id
            assert tx.get_delivery(occurrence.delivery_id).payload_ref == "text:并Pot生成PDF"
            assert tx.list_work() == []
        assert not models
        assert await app.dispatcher.tick() == 2
        outputs = outgoing(env)
        assert_pot_texts(outputs)
        assert any(row["payload"]["text"] == "【Pot】并Pot生成PDF" for row in outputs)
    finally:
        await app.close()


async def test_budget_error_and_missing_input_notice_receive_one_pot_badge(runtime_env):
    env = runtime_env
    app, _ = app_for(env, {})

    class OfflineBudgetFailure:
        async def decide(self, *args):
            raise DomainError("budget_exhausted", "Synthetic budget stop")

    app.coordinator.provider_factory = lambda *_: OfflineBudgetFailure()
    try:
        request = event(env, "Pot，请解释这个问题。")
        await admit(app, env, request)
        waiting_work(env, app, question="【Pot】需要哪一年的资料？")
        assert await app.dispatcher.tick() == 2
        rows = outgoing(env)
        assert_pot_texts(rows)
        texts = [row["payload"]["text"] for row in rows]
        assert any("当前调用额度不足" in text for text in texts)
        assert "【Pot】需要哪一年的资料？" in texts
        with env.store.transaction(env.actor) as tx:
            assert all(row.state == DeliveryState.ACCEPTED for row in tx.list_delivery())
    finally:
        await app.close()


async def test_long_text_delivery_prefixes_each_chunk_once_and_keeps_the_full_body(runtime_env):
    env = runtime_env
    app, _ = app_for(env, {})
    app.dispatcher.chunk_characters = 120
    body = "完整的分段结果。\n" * 100
    request = event(env, "Pot，给我完整结果。")
    try:
        with env.store.transaction(env.actor) as tx:
            tx.ingest_event(request, "identity-long:" + request.id, "1", 0)
            intent = tx.enqueue_reply(request.id, "【pot】【Pot】" + body)
        assert await app.dispatcher.tick() == 1
        rows = outgoing(env)
        assert_pot_texts(rows)
        chunks = []
        for row in rows:
            match = re.fullmatch(r"【Pot】（(\d+)/(\d+)）(.*)", row["payload"]["text"], re.S)
            assert match and int(match[2]) == len(rows)
            chunks.append((int(match[1]), match[3]))
        assert len(chunks) > 2 and "".join(text for _, text in sorted(chunks)) == body
        with env.store.transaction(env.actor) as tx:
            assert tx.get_delivery(intent.id).state == DeliveryState.ACCEPTED
        assert await app.dispatcher.tick() == 0
    finally:
        await app.close()
