"""Receipt-proven echoes are cheap; recall events keep their privacy handling."""

import json

import pytest

from groupagent.domain import EventKind

from .test_app import event
from .test_early_ack import intake, setup_chat

pytestmark = pytest.mark.postgres


async def accepted_reply(env, app):
    request = event(env, "Pot，你叫什么名字？")
    await intake(app, env, request)
    await app.process_pending(batched=False)
    assert await app.dispatcher.tick() == 1
    wire = next((env.root / "channel/outgoing").glob("*.json"))
    return json.loads(wire.read_text())["receipt"]["platform_message_id"]


async def test_echo_uses_control_lane_without_idle_batch_or_history(runtime_env, monkeypatch):
    env = runtime_env
    app, model, _ = setup_chat(env)
    model.text = "我叫Pot。"
    try:
        message_id = await accepted_reply(env, app)
        assert len(model.requests) == 1

        def forbidden(*args):
            raise AssertionError("A verified echo must not reconstruct conversation history")

        monkeypatch.setattr(app.coordinator, "_context", forbidden)
        echo = event(env, "【Pot】我叫Pot。", platform_message_id=message_id)
        await intake(app, env, echo)
        assert await app.process_pending(controls_only=True) == 1
        with env.store.transaction(env.actor) as tx:
            assert tx.get_decision(echo.id)["result"]["decision"]["kind"] == "ignore"
        assert len(model.requests) == 1
        assert len(list((env.root / "channel/outgoing").glob("*.json"))) == 1
    finally:
        await app.close()


async def test_recall_of_known_bot_message_still_runs_privacy_handler(runtime_env, monkeypatch):
    env = runtime_env
    app, _, _ = setup_chat(env)
    try:
        message_id = await accepted_reply(env, app)
        calls = []
        monkeypatch.setattr(
            app.coordinator.memory, "recall", lambda *args, **kwargs: calls.append(args)
        )

        def after_recall(*args):
            raise AssertionError("context_after_recall")

        monkeypatch.setattr(app.coordinator, "_context", after_recall)
        recall = event(env, "", kind=EventKind.RECALL, platform_message_id=message_id)
        await intake(app, env, recall)
        assert await app.process_pending(controls_only=True) == 0
        with pytest.raises(AssertionError, match="context_after_recall"):
            await app.coordinator.process(recall)
        assert calls and calls[0][1] == recall.id
    finally:
        await app.close()
