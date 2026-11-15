import json

import pytest

from groupagent.execution.harness_wire import chat_to_message, message_events, messages_to_chat


def test_messages_wire_preserves_tool_pairing_and_keeps_log_contributions_disabled():
    body = {
        "model": "synthetic",
        "max_tokens": 100,
        "system": [{"type": "text", "text": "System"}],
        "tools": [
            {"name": "scoped", "description": "Synthetic", "input_schema": {"type": "object"}}
        ],
        "messages": [
            {
                "role": "assistant",
                "content": [
                    {"type": "tool_use", "id": "call-1", "name": "scoped", "input": {"value": 3}}
                ],
            },
            {
                "role": "user",
                "content": [
                    {
                        "type": "tool_result",
                        "tool_use_id": "call-1",
                        "content": [{"type": "text", "text": "observed"}],
                    }
                ],
            },
        ],
    }
    result = messages_to_chat(body)
    assert result["messages"][0]["role"] == "system"
    assert result["messages"][1]["tool_calls"][0]["id"] == "call-1"
    assert result["messages"][2] == {
        "role": "tool",
        "tool_call_id": "call-1",
        "content": "observed",
    }
    with pytest.raises(ValueError, match="log contribution"):
        messages_to_chat({**body, "dsh_session_log": {"private": "synthetic"}})


def test_messages_relay_refuses_unsupported_media_instead_of_dropping_it():
    with pytest.raises(ValueError, match="Unsupported"):
        messages_to_chat(
            {
                "model": "synthetic",
                "max_tokens": 100,
                "messages": [{"role": "user", "content": [{"type": "image", "source": {}}]}],
            }
        )


def test_sse_tool_input_is_complete_and_usage_is_cumulative():
    reply = {
        "id": "synthetic",
        "model": "synthetic",
        "choices": [
            {
                "finish_reason": "tool_calls",
                "message": {
                    "content": None,
                    "tool_calls": [
                        {"id": "call-1", "function": {"name": "scoped", "arguments": '{"value":3}'}}
                    ],
                },
            }
        ],
        "usage": {"prompt_tokens": 10, "completion_tokens": 5},
    }
    events = message_events(chat_to_message(reply))
    assert events[0]["type"] == "message_start" and events[-1]["type"] == "message_stop"
    fragments = [
        event["delta"]["partial_json"] for event in events if event["type"] == "content_block_delta"
    ]
    assert json.loads("".join(fragments)) == {"value": 3}
    assert events[-2]["usage"]["output_tokens"] == 5
    assert events[-2]["delta"]["stop_reason"] == "tool_use"
