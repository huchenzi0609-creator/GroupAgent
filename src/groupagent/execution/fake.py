"""Explicit deterministic fake; it performs no network or model inference."""

from __future__ import annotations

import asyncio
from collections.abc import Sequence

from groupagent.domain import DomainError

from .models import ModelReply, ModelRequest


class ScriptedFakeModel:
    name = "scripted-fake-model/offline-v1"

    def __init__(self, replies: Sequence[ModelReply], *, delay_seconds: float = 0) -> None:
        self._replies = iter(replies)
        self.delay_seconds = delay_seconds
        self.requests: list[ModelRequest] = []
        self.started = asyncio.Event()
        self.cancelled = asyncio.Event()

    async def complete(self, request: ModelRequest) -> ModelReply:
        self.requests.append(request)
        self.started.set()
        try:
            if self.delay_seconds:
                await asyncio.sleep(self.delay_seconds)
            return next(self._replies)
        except StopIteration as exc:
            raise DomainError(
                "fake_script_exhausted", "The offline fake has no scripted response"
            ) from exc
        except asyncio.CancelledError:
            self.cancelled.set()
            raise
