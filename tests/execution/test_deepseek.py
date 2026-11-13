import json

import httpx
import pytest

from groupagent.domain import DomainError
from groupagent.execution import ModelMessage, ModelRequest, ModelToolCall
from groupagent.execution.deepseek import DeepSeekHTTPModel


def wire_response(**overrides):
    payload = {
        "choices": [
            {
                "message": {"role": "assistant", "content": "Synthetic response"},
                "finish_reason": "stop",
            }
        ],
        "usage": {"prompt_tokens": 10, "completion_tokens": 4, "total_tokens": 14},
    }
    return payload | overrides


def model_request() -> ModelRequest:
    return ModelRequest(
        messages=(ModelMessage(role="user", content="Synthetic request"),),
        tools=(),
        max_output_tokens=32,
    )


async def test_http_adapter_serializes_function_calls_without_live_network() -> None:
    captured = []

    def handler(request: httpx.Request) -> httpx.Response:
        captured.append(request)
        return httpx.Response(
            200,
            json=wire_response(
                choices=[
                    {
                        "message": {
                            "role": "assistant",
                            "content": None,
                            "tool_calls": [
                                {
                                    "id": "synthetic-call",
                                    "type": "function",
                                    "function": {
                                        "name": "workspace_read_text",
                                        "arguments": '{"path":"a.txt"}',
                                    },
                                }
                            ],
                        },
                        "finish_reason": "tool_calls",
                    }
                ]
            ),
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        model = DeepSeekHTTPModel("synthetic-test-key", client=client)
        request = model_request().model_copy(
            update={
                "messages": (
                    ModelMessage(role="user", content="Read the draft"),
                    ModelMessage(
                        role="assistant",
                        tool_calls=(
                            ModelToolCall(
                                id="previous",
                                name="workspace_read_text",
                                arguments={"path": "old.txt"},
                            ),
                        ),
                    ),
                    ModelMessage(role="tool", tool_call_id="previous", content="old content"),
                )
            }
        )
        reply = await model.complete(request)
    assert reply.tool_calls[0].arguments == {"path": "a.txt"}
    assert reply.usage.total == 14
    body = json.loads(captured[0].content)
    assert body["max_tokens"] == 32 and body["stream"] is False
    assert body["thinking"] == {"type": "disabled"}
    assert "session_log" not in body
    assert body["messages"][1]["tool_calls"][0]["function"]["arguments"] == '{"path": "old.txt"}'
    assert captured[0].url == "https://api.deepseek.com/chat/completions"


@pytest.mark.parametrize(
    "status,code",
    [
        (401, "auth_required"),
        (429, "rate_limited"),
        (503, "provider_unavailable"),
        (400, "invalid_model_request"),
        (302, "provider_redirect_rejected"),
    ],
)
async def test_http_errors_are_sanitized_and_never_retried_or_redirected(status, code):
    calls = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        return httpx.Response(
            status,
            text="untrusted provider body synthetic-test-key",
            headers={"Location": "https://unapproved.example/steal"},
        )

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler), follow_redirects=True
    ) as client:
        model = DeepSeekHTTPModel("synthetic-test-key", client=client)
        with pytest.raises(DomainError) as failure:
            await model.complete(model_request())
    assert failure.value.code == code
    assert "synthetic-test-key" not in str(failure.value)
    assert "untrusted provider body" not in str(failure.value)
    assert len(calls) == 1


@pytest.mark.parametrize(
    "payload",
    [
        {
            "choices": [
                {"message": {"role": "assistant", "content": "Done"}, "finish_reason": "stop"}
            ]
        },
        wire_response(usage={"prompt_tokens": True, "completion_tokens": 4}),
        wire_response(usage={"prompt_tokens": 10, "completion_tokens": 4, "total_tokens": 0}),
        wire_response(
            choices=[
                {
                    "message": {
                        "role": "assistant",
                        "tool_calls": [
                            {
                                "id": "bad",
                                "type": "function",
                                "function": {"name": "read", "arguments": "not-json"},
                            }
                        ],
                    },
                    "finish_reason": "tool_calls",
                }
            ]
        ),
    ],
)
async def test_missing_usage_and_invalid_tool_json_fail_closed(payload) -> None:
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda _: httpx.Response(200, json=payload))
    ) as client:
        model = DeepSeekHTTPModel("synthetic-test-key", client=client)
        with pytest.raises(DomainError) as failure:
            await model.complete(model_request())
    assert failure.value.code == "invalid_model_response"


async def test_response_body_size_is_bounded() -> None:
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda _: httpx.Response(200, content=b"x" * 101))
    ) as client:
        model = DeepSeekHTTPModel("synthetic-test-key", client=client, max_response_bytes=100)
        with pytest.raises(DomainError) as failure:
            await model.complete(model_request())
    assert failure.value.code == "model_response_too_large"


@pytest.mark.parametrize(
    "url",
    [
        "http://api.deepseek.com",
        "https://example.com",
        "https://api.deepseek.com@evil.test",
        "https://api.deepseek.com:8443",
        "https://api.deepseek.com?secret=bad",
    ],
)
def test_provider_address_cannot_expand_network_scope(url: str) -> None:
    with pytest.raises(ValueError, match="HTTPS origin"):
        DeepSeekHTTPModel("synthetic-test-key", base_url=url)
