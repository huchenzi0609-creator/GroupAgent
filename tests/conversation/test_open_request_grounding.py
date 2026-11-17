"""Ambient context and actual open questions have distinct visible outcomes."""

import json

import pytest

from .test_react_turn import ModelScript, finish, source

pytestmark = pytest.mark.postgres


@pytest.mark.parametrize("open_request", [False, True])
async def test_ambient_reply_requires_an_invitation(conversation_env, open_request):
    env = conversation_env
    request = source(
        env,
        "大家谁方便说一下，正方形有几条边？"
        if open_request
        else "刚刚组里的聊天助手也提到了正方形。",
    )

    def decide(call):
        assert (
            json.loads(call.messages[1].content)["context"]["trusted_routing"]["recipient"]
            == "ambient"
        )
        return finish(
            "正方形有四条边。",
            open_request=True,
            open_request_evidence="大家谁方便说一下，正方形有几条边？" if open_request else None,
        )

    model = ModelScript(decide)
    env.models[request.id] = model
    result = await env.coordinator.process(request)
    assert len(model.requests) == 1
    assert bool(result.delivery_id) == open_request
    assert result.decision.kind == ("reply" if open_request else "observe")
