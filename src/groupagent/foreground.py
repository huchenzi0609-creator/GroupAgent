"""Coalesced, content-free foreground pressure hints to the trusted worker broker."""

from __future__ import annotations

import asyncio
import json
import time
from pathlib import Path


class ForegroundPublisher:
    def __init__(self, socket_path: Path, *, interval_seconds: float = 2.0):
        self.socket_path = socket_path
        self.interval = interval_seconds
        self.pending = 0
        self._task: asyncio.Task | None = None
        self._wake = asyncio.Event()
        self._closed = False
        self.capacity: dict = {}
        self.capacity_at = 0.0

    @property
    def memory_pressure(self) -> bool:
        return time.monotonic() - self.capacity_at < 5 and self.capacity.get("memory_pressure") in (
            True,
            1,
        )

    def report(self, pending: int) -> None:
        self.pending = min(1000, max(0, pending))
        self._wake.set()
        if self._task is None and not self._closed:
            try:
                self._task = asyncio.get_running_loop().create_task(self._run())
            except RuntimeError:
                pass

    async def _send(self, pending: int) -> None:
        writer = None
        try:
            async with asyncio.timeout(0.5):
                reader, writer = await asyncio.open_unix_connection(str(self.socket_path))
                body = json.dumps({"pending": pending}, separators=(",", ":")).encode()
                writer.write(
                    b"POST /pot/foreground HTTP/1.1\r\nHost: pot-broker\r\n"
                    b"Content-Type: application/json\r\nConnection: close\r\nContent-Length: "
                    + str(len(body)).encode()
                    + b"\r\n\r\n"
                    + body
                )
                await writer.drain()
                raw = bytearray()
                while chunk := await reader.read(4096):
                    raw.extend(chunk)
                    if len(raw) > 8192:
                        return
                header, body = bytes(raw).split(b"\r\n\r\n", 1)
                if header.split(b"\r\n", 1)[0].split()[1] == b"200":
                    capacity = json.loads(body)
                    if isinstance(capacity, dict):
                        self.capacity, self.capacity_at = capacity, time.monotonic()
        except (OSError, TimeoutError, ValueError, IndexError, RecursionError):
            # Broker defaults to a conservative CPU ceiling if hints disappear.
            # A scheduling hint must never hold the chat response or fail a task.
            pass
        finally:
            if writer is not None:
                writer.close()
                try:
                    await writer.wait_closed()
                except OSError:
                    pass

    async def _run(self) -> None:
        while not self._closed:
            self._wake.clear()
            await self._send(self.pending)
            try:
                await asyncio.wait_for(self._wake.wait(), self.interval)
                await asyncio.sleep(0.1)
            except TimeoutError:
                pass

    async def aclose(self) -> None:
        self._closed = True
        if self._task is not None:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)
        await self._send(0)
