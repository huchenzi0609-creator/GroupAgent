"""Non-thinking DeepSeek HTTP tool calls. This is not a Harness SDK adapter.

Protocol reference (accessed 2026-11-13):
https://api-docs.deepseek.com/api/create-chat-completion/
https://api-docs.deepseek.com/guides/tool_calls/

Construction never discovers credentials. Network requests occur only on complete().
"""

from __future__ import annotations

import json
from typing import Any
from urllib.parse import urlsplit

import httpx
from pydantic import SecretStr, ValidationError

from groupagent.domain import DomainError

from .errors import ModelRejected, retry_after_seconds
from .models import ModelReply, ModelRequest, ModelToolCall, TokenUsage


class DeepSeekHTTPModel:
    def __init__(
        self,
        api_key: str | SecretStr,
        *,
        model: str = "deepseek-flash",
        base_url: str = "https://api.deepseek.com",
        allowed_hosts: frozenset[str] = frozenset({"api.deepseek.com"}),
        client: httpx.AsyncClient | None = None,
        max_response_bytes: int = 2_000_000,
    ) -> None:
        address = urlsplit(base_url)
        if (
            address.scheme != "https"
            or address.hostname not in allowed_hosts
            or address.username
            or address.password
            or address.query
            or address.fragment
            or address.port not in {None, 443}
        ):
            raise ValueError("The provider endpoint must be an explicitly allowed HTTPS origin")
        if not model or max_response_bytes < 1:
            raise ValueError("Model name and a positive response limit are required")
        self._api_key = api_key if isinstance(api_key, SecretStr) else SecretStr(api_key)
        if not self._api_key.get_secret_value().strip():
            raise ValueError("An explicit provider credential is required")
        self.model = model
        self.name = f"deepseek-http/{model}"
        self._url = base_url.rstrip("/") + "/chat/completions"
        self._owns_client = client is None
        self._client = client or httpx.AsyncClient(
            timeout=httpx.Timeout(60),
            follow_redirects=False,
            trust_env=False,
        )
        self._max_response_bytes = max_response_bytes

    async def aclose(self) -> None:
        if self._owns_client:
            await self._client.aclose()

    @staticmethod
    def _messages(request: ModelRequest) -> list[dict[str, Any]]:
        result = []
        for item in request.messages:
            message: dict[str, Any] = {"role": item.role, "content": item.content}
            if item.tool_call_id:
                message["tool_call_id"] = item.tool_call_id
            if item.tool_calls:
                message["tool_calls"] = [
                    {
                        "id": call.id,
                        "type": "function",
                        "function": {
                            "name": call.name,
                            "arguments": json.dumps(
                                call.arguments, ensure_ascii=False, allow_nan=False
                            ),
                        },
                    }
                    for call in item.tool_calls
                ]
            result.append(message)
        return result

    async def complete(self, request: ModelRequest) -> ModelReply:
        body: dict[str, Any] = {
            "model": self.model,
            "messages": self._messages(request),
            "max_tokens": request.max_output_tokens,
            "stream": False,
            "thinking": {"type": "disabled"},
        }
        if request.tools:
            body["tools"] = list(request.tools)
            body["tool_choice"] = "auto"
        try:
            async with self._client.stream(
                "POST",
                self._url,
                json=body,
                headers={"Authorization": f"Bearer {self._api_key.get_secret_value()}"},
                follow_redirects=False,
            ) as response:
                status = response.status_code
                if status in {401, 403}:
                    raise DomainError("auth_required", "The model provider rejected authentication")
                if status == 429:
                    raise ModelRejected(retry_after_seconds(response.headers.get("Retry-After")))
                if 300 <= status < 400:
                    raise DomainError("provider_redirect_rejected", "Provider redirect was refused")
                if status >= 500 or status == 408:
                    raise DomainError("provider_unavailable", "The model provider is unavailable")
                if status != 200:
                    raise DomainError(
                        "invalid_model_request", "The model provider rejected the request"
                    )
                data = bytearray()
                async for chunk in response.aiter_bytes():
                    data.extend(chunk)
                    if len(data) > self._max_response_bytes:
                        raise DomainError(
                            "model_response_too_large", "The model response exceeded its limit"
                        )
        except httpx.HTTPError:
            raise DomainError("provider_unavailable", "The model request failed") from None
        try:
            payload = json.loads(data)
            choices = payload["choices"]
            if not isinstance(choices, list) or len(choices) != 1:
                raise ValueError("Exactly one response choice is required")
            choice = choices[0]
            message = choice["message"]
            if message.get("role") != "assistant":
                raise ValueError("Expected an assistant response")
            usage = payload["usage"]
            incoming, outgoing = usage["prompt_tokens"], usage["completion_tokens"]
            if type(incoming) is not int or type(outgoing) is not int:
                raise ValueError("Usage must contain integer counters")
            if "total_tokens" in usage and usage["total_tokens"] != incoming + outgoing:
                raise ValueError("Provider usage counters disagree")
            calls = []
            for call in message.get("tool_calls") or []:
                if call.get("type") != "function":
                    raise ValueError("Only client function calls are supported")
                function = call["function"]
                if not isinstance(function["arguments"], str):
                    raise ValueError("Function arguments must be serialized JSON")
                arguments = json.loads(function["arguments"])
                if not isinstance(arguments, dict):
                    raise ValueError("Function arguments must be an object")
                # Reject NaN/Infinity accepted by the stdlib decoder.
                json.dumps(arguments, allow_nan=False)
                calls.append(
                    ModelToolCall(id=call["id"], name=function["name"], arguments=arguments)
                )
            if len(calls) > 256:
                raise ValueError("Too many model tool calls")
            return ModelReply(
                text=message.get("content"),
                tool_calls=tuple(calls),
                finish_reason=choice["finish_reason"],
                usage=TokenUsage(input_tokens=incoming, output_tokens=outgoing),
            )
        except (KeyError, TypeError, ValueError, AttributeError, ValidationError):
            # Never include provider response bodies or credentials in a public error.
            raise DomainError(
                "invalid_model_response", "The model response or usage was invalid"
            ) from None
