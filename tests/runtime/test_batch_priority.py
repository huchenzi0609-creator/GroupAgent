"""Wake-up priority reuses invocation evidence; execution gates stay authoritative."""

import time
from contextlib import contextmanager
from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

from groupagent.config import AppConfig, GroupConfig
from groupagent.coordinator import Decision
from groupagent.domain import ActorContext, EventKind, GroupEvent, Role
from groupagent.identity import BotIdentity
from groupagent.runtime import app as app_module
from groupagent.runtime.app import RuntimeApp

from .test_app import app_for, event


def sample(text, **changes):
    stamp = datetime(2030, 1, 2, tzinfo=UTC)
    return GroupEvent(
        id="synthetic-priority",
        source_key="synthetic:priority",
        group_id="group",
        actor_id="member",
        kind=EventKind.TEXT,
        text=text,
        occurred_at=stamp,
        received_at=stamp,
    ).model_copy(update=changes)


@pytest.mark.parametrize("root", [None, "trusted-root"])
@pytest.mark.parametrize(
    "source",
    [
        sample("Pot，联网查找Python官方文档首页，给出链接和一句用途说明。"),
        sample("Pot，把这份Word文件修改并导出PDF。"),
        sample(
            "继续把结果写入文件。",
            kind=EventKind.QUOTE,
            reply_to="receipt",
            attachments=[{"kind": "quote"}],
        ),
        sample("Pot，分析图片。", kind=EventKind.IMAGE, attachments=[{"kind": "image"}]),
        sample("什么是JSON？"),
    ],
)
def test_priority_uses_shared_invocation_result_without_content_filters(monkeypatch, source, root):
    app = RuntimeApp.__new__(RuntimeApp)
    app.identity = BotIdentity()
    app.config = AppConfig(
        group=GroupConfig(
            enabled=True,
            internal_id="group",
            external_group_id="synthetic@chatroom",
            owner_ids=["synthetic-owner"],
            bot_member_id="synthetic-bot",
            shared_account=True,
        )
    )
    app.technical = ActorContext(
        group_id="group", member_id="service:runtime", role=Role.MAINTAINER
    )
    tx, calls = object(), []

    @contextmanager
    def transaction(actor):
        assert actor == app.technical
        yield tx

    def invocation(actual_tx, actual_event, identity, *, bot_member_id, shared_account):
        calls.append(actual_event.id)
        assert actual_tx is tx and identity == app.identity
        assert bot_member_id == app.config.group.bot_member_id
        assert shared_account is app.config.group.shared_account
        return root

    app.store = SimpleNamespace(transaction=transaction)
    monkeypatch.setattr(app_module, "trusted_invocation_in_tx", invocation)
    assert app._bypass_batch(source) is (root is not None)
    assert calls == [source.id]


@pytest.mark.postgres
async def test_unaddressed_work_uses_regular_model_queue(runtime_env):
    env = runtime_env
    app, _ = app_for(env, {})
    try:
        request = await incoming(env, app, "请生成一份介绍太阳系的三页PPT。")
        assert not app._bypass_batch(request)
        assert await app.process_pending() == 0
        assert await app.process_pending(batched=False) == 1
    finally:
        await app.close()


async def incoming(env, app, text, **changes):
    source = event(env, text, **changes)
    env.channel.append_incoming(source)
    assert await app.poll_intake() == 1
    return source


def reply():
    return Decision(
        kind="reply",
        reason_code="synthetic_priority_answer",
        response="合成答复。",
        directness="direct",
        confidence=1,
    )


@pytest.mark.postgres
async def test_named_research_is_admitted_during_idle_window_without_model_execution(runtime_env):
    env = runtime_env
    decisions = {}
    app, models = app_for(env, decisions)
    try:
        source = await incoming(
            env, app, "Pot，联网查找Python官方文档首页，给出链接和一句用途说明。"
        )
        decisions[source.id] = Decision(
            kind="start_work",
            reason_code="synthetic_research_admission",
            goal=source.text,
            work_capabilities=["public_research"],
            outputs=[{"format": "text"}],
            directness="direct",
            confidence=1,
        )
        assert time.monotonic() - app._last_arrival < app.config.conversation.idle_batch_seconds
        assert await app.process_pending() == 1
        with env.store.transaction(env.actor) as tx:
            result = tx.get_decision(source.id)["result"]
            assert result["work_id"] and result["delivery_id"]
            assert tx.get_work(result["work_id"]).state.value == "queued"
            assert (
                tx.conn.execute(
                    "SELECT count(*) AS n FROM ga_budget_reservations WHERE group_id=%s",
                    (tx.group_id,),
                ).fetchone()["n"]
                == 0
            )
        assert models == []
    finally:
        await app.close()


@pytest.mark.postgres
async def test_same_requester_followup_and_native_receipt_quote_keep_priority(runtime_env):
    env = runtime_env
    decisions = {}
    app, _ = app_for(env, decisions)
    try:
        call = await incoming(env, app, "Pot，解释一下变量。")
        decisions[call.id] = reply()
        assert await app.process_pending() == 1
        followup = await incoming(env, app, "继续讲讲Word文档里的例子。")
        decisions[followup.id] = reply()
        assert app._bypass_batch(followup)
        assert await app.process_pending() == 1
        await app.dispatcher.tick()
        with env.store.transaction(env.actor) as tx:
            delivery_id = tx.get_decision(call.id)["result"]["delivery_id"]
            native_id = tx.get_delivery(delivery_id).platform_message_id
        await incoming(env, app, "另一位同学加入讨论。", actor_id="synthetic-other-human")
        assert await app.process_pending(batched=False) == 1
        unrelated = await incoming(env, app, "那还有什么？")
        assert not app._bypass_batch(unrelated)
        assert await app.process_pending() == 0
        assert await app.process_pending(batched=False) == 1
        quote = await incoming(
            env,
            app,
            "这个结果再说明一下。",
            kind=EventKind.QUOTE,
            reply_to="synthetic-not-yet-ingested-echo",
            trusted_actor_origin="agentwechat_authenticated_message_metadata",
            channel_account_id=env.config.account_id,
            attachments=[{"kind": "quote", "server_id": native_id, "state": "embedded"}],
        )
        decisions[quote.id] = reply()
        assert app._bypass_batch(quote)
        assert await app.process_pending() == 1
    finally:
        await app.close()


@pytest.mark.postgres
async def test_priority_does_not_admit_missing_sources_or_bypass_feature_gate(runtime_env):
    env = runtime_env
    decisions = {}
    app, models = app_for(env, decisions)
    try:
        text = "Pot，联网查找Python官方文档首页。"
        assert not app._bypass_batch(event(env, text))
        app.config = app.config.model_copy(
            update={"features": app.config.features.model_copy(update={"web_research": False})}
        )
        app.coordinator.config = app.config
        source = await incoming(env, app, text)
        decisions[source.id] = Decision(
            kind="start_work",
            reason_code="synthetic_disabled_request",
            goal=text,
            work_capabilities=["public_research"],
            outputs=[{"format": "text"}],
            directness="direct",
            confidence=1,
        )
        assert app._bypass_batch(source)
        assert await app.process_pending() == 1
        with env.store.transaction(env.actor) as tx:
            result = tx.get_decision(source.id)["result"]
            assert result["work_id"] is None
            assert result["decision"]["reason_code"] == "feature_disabled"
            assert tx.list_work() == []
        assert models == []
    finally:
        await app.close()
