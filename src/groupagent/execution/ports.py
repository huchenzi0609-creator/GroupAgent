from __future__ import annotations

import json
from collections.abc import AsyncIterator
from typing import Protocol

from groupagent.domain import ActorContext

from .models import (
    EngineCheckpoint,
    EngineEvent,
    ExecutionBudget,
    ExecutionRequest,
    ModelReply,
    ModelRequest,
    RunSnapshot,
)


def estimate_input_tokens(request: ModelRequest) -> int:
    """Conservative UTF-8-byte estimate plus framing; not a provider tokenizer claim."""
    data = json.dumps(
        {"messages": [m.model_dump(mode="json") for m in request.messages], "tools": request.tools},
        ensure_ascii=False,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    return len(data) + 64 * len(request.messages) + 256


class ModelPort(Protocol):
    name: str

    async def complete(self, request: ModelRequest) -> ModelReply: ...


class EnginePort(Protocol):
    async def start(self, request: ExecutionRequest) -> str: ...

    async def append_input(
        self,
        run_id: str,
        actor: ActorContext,
        text: str,
        expected_revision: int,
        *,
        input_id: str | None = None,
    ) -> int: ...

    async def cancel(self, run_id: str, actor: ActorContext) -> None: ...

    async def resume(
        self,
        checkpoint: EngineCheckpoint,
        actor: ActorContext,
        *,
        attempt_id: str,
        budget: ExecutionBudget | None = None,
    ) -> str: ...

    def events(self, run_id: str, after_sequence: int = 0) -> AsyncIterator[EngineEvent]: ...

    async def inspect(self, run_id: str) -> RunSnapshot: ...

    async def wait(self, run_id: str) -> RunSnapshot: ...
