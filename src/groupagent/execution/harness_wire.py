"""Text/tool-only Messages-to-ModelPort relay serialization.

Verified transport references (2026-11-14):
https://api-docs.deepseek.com/guides/anthropic_api/
https://api-docs.deepseek.com/zh-cn/api/create-message/
The pinned Harness llm-deepseek-api-key adapter uses Messages, not Chat Completions.
"""

from __future__ import annotations

import json
from typing import Any


def _text(content: Any) -> str:
    if isinstance(content, str):
        return content
    if not isinstance(content, list) or any(part.get("type") != "text" for part in content):
        raise ValueError("Only text observations are admitted by this relay")
    return "\n".join(part["text"] for part in content)


def messages_to_chat(body: dict) -> dict:
    if any(key.startswith("dsh_") for key in body):
        raise ValueError("Extra runtime log contribution is disabled")
    messages: list[dict] = []
    if body.get("system"):
        messages.append({"role": "system", "content": _text(body["system"])})
    for message in body["messages"]:
        content = message["content"]
        if isinstance(content, str):
            messages.append({"role": message["role"], "content": content})
            continue
        text, calls = [], []
        for part in content:
            if part["type"] == "text":
                text.append(part["text"])
            elif part["type"] == "tool_use" and message["role"] == "assistant":
                calls.append(
                    {
                        "id": part["id"],
                        "type": "function",
                        "function": {
                            "name": part["name"],
                            "arguments": json.dumps(part["input"]),
                        },
                    }
                )
            elif part["type"] == "tool_result" and message["role"] == "user":
                messages.append(
                    {
                        "role": "tool",
                        "tool_call_id": part["tool_use_id"],
                        "content": _text(part.get("content", "")),
                    }
                )
            else:
                raise ValueError("Unsupported Messages content block")
        if text or calls:
            normalized: dict[str, Any] = {
                "role": message["role"],
                "content": "\n".join(text) or None,
            }
            if calls:
                normalized["tool_calls"] = calls
            messages.append(normalized)
    tools = [
        {
            "type": "function",
            "function": {
                "name": tool["name"],
                "description": tool.get("description", ""),
                "parameters": tool["input_schema"],
            },
        }
        for tool in body.get("tools") or []
    ]
    return {
        "messages": messages,
        "tools": tools,
        "model": body["model"],
        "max_tokens": body["max_tokens"],
        "source_request_keys": sorted(body),
    }


def chat_to_message(reply: dict) -> dict:
    choice = reply["choices"][0]
    content = []
    if choice["message"].get("content"):
        content.append({"type": "text", "text": choice["message"]["content"]})
    for call in choice["message"].get("tool_calls") or []:
        content.append(
            {
                "type": "tool_use",
                "id": call["id"],
                "name": call["function"]["name"],
                "input": json.loads(call["function"]["arguments"]),
            }
        )
    reason = {
        "stop": "end_turn",
        "tool_calls": "tool_use",
        "length": "max_tokens",
        "content_filter": "refusal",
    }[choice["finish_reason"]]
    return {
        "id": reply["id"],
        "type": "message",
        "role": "assistant",
        "model": reply["model"],
        "content": content,
        "stop_reason": reason,
        "stop_sequence": None,
        "usage": {
            "input_tokens": reply["usage"]["prompt_tokens"],
            "output_tokens": reply["usage"]["completion_tokens"],
            "cache_creation_input_tokens": 0,
            "cache_read_input_tokens": 0,
            "service_tier": "standard",
        },
    }


def message_events(message: dict) -> list[dict]:
    initial = {
        **message,
        "content": [],
        "stop_reason": None,
        "usage": {**message["usage"], "output_tokens": 0},
    }
    events: list[dict[str, Any]] = [{"type": "message_start", "message": initial}]
    for index, content in enumerate(message["content"]):
        if content["type"] == "text":
            start: dict[str, Any] = {"type": "text", "text": ""}
            delta: dict[str, Any] = {"type": "text_delta", "text": content["text"]}
        else:
            start = {**content, "input": {}}
            delta = {"type": "input_json_delta", "partial_json": json.dumps(content["input"])}
        events.extend(
            [
                {"type": "content_block_start", "index": index, "content_block": start},
                {"type": "content_block_delta", "index": index, "delta": delta},
                {"type": "content_block_stop", "index": index},
            ]
        )
    events.extend(
        [
            {
                "type": "message_delta",
                "delta": {"stop_reason": message["stop_reason"], "stop_sequence": None},
                "usage": message["usage"],
            },
            {"type": "message_stop"},
        ]
    )
    return events
