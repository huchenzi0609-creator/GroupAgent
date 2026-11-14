"""Optional external, durable deletion acknowledgement boundary."""

from typing import Any, Protocol


class DeletionSink(Protocol):
    def append_tombstones(self, markers: list[dict[str, Any]]) -> int:
        """Return only after durable append; raise before a DB deletion may commit."""
        ...
