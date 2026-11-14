"""Private append-only deletion journal, independent of a restorable database snapshot."""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
from pathlib import Path
from typing import Any

from groupagent.domain import ActorContext, DomainError
from groupagent.ops.common import canonical, digest, private_root, require_ops
from groupagent.ops.models import DeletionMarker
from groupagent.persistence import PostgresStore

_RANK = {"expired": 1, "recalled": 2, "forgotten": 3}


def _key(marker: DeletionMarker) -> tuple[str, str]:
    return ("task", marker.task_id) if marker.task_id else ("event", marker.source_key)


class DeletionJournal:
    def __init__(self, root: Path, *, max_bytes: int = 128 * 1024**2) -> None:
        self.root = private_root(root)
        self.max_bytes = max_bytes

    def _path(self, group_id: str) -> Path:
        return self.root / (hashlib.sha256(group_id.encode()).hexdigest() + ".jsonl")

    def load(self, group_id: str) -> list[DeletionMarker]:
        path = self._path(group_id)
        if not path.exists():
            return []
        if path.is_symlink() or path.stat().st_size > self.max_bytes:
            raise DomainError(
                "invalid_deletion_journal", "Deletion journal is not a bounded regular file"
            )
        latest: dict[tuple[str, str], DeletionMarker] = {}
        descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
        try:
            with os.fdopen(descriptor, "rb") as source:
                fcntl.flock(source.fileno(), fcntl.LOCK_SH)
                for line in source:
                    if len(line) > 65536 or not line.endswith(b"\n"):
                        raise ValueError("Incomplete deletion journal record")
                    record = json.loads(line)
                    if digest(record["marker"]) != record["sha256"]:
                        raise ValueError("Deletion journal checksum mismatch")
                    marker = DeletionMarker.model_validate(record["marker"])
                    if marker.group_id != group_id:
                        raise ValueError("Deletion journal scope mismatch")
                    old = latest.get(_key(marker))
                    if old is None or _RANK[marker.reason] >= _RANK[old.reason]:
                        latest[_key(marker)] = marker
        except (ValueError, TypeError, KeyError) as error:
            raise DomainError(
                "invalid_deletion_journal", "Deletion journal failed verification"
            ) from error
        return list(latest.values())

    def append(self, markers: list[DeletionMarker]) -> int:
        groups: dict[str, list[DeletionMarker]] = {}
        for marker in markers:
            groups.setdefault(marker.group_id, []).append(marker)
        added = 0
        for group, values in groups.items():
            previous = {_key(item): item for item in self.load(group)}
            needed = [
                item
                for item in values
                if _key(item) not in previous
                or _RANK[item.reason] > _RANK[previous[_key(item)].reason]
            ]
            if not needed:
                continue
            path = self._path(group)
            descriptor = os.open(
                path, os.O_WRONLY | os.O_APPEND | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0), 0o600
            )
            with os.fdopen(descriptor, "ab") as target:
                fcntl.flock(target.fileno(), fcntl.LOCK_EX)
                data = b"".join(
                    canonical(
                        {
                            "marker": item.model_dump(mode="json"),
                            "sha256": digest(item.model_dump(mode="json")),
                        }
                    )
                    + b"\n"
                    for item in needed
                )
                if os.fstat(target.fileno()).st_size + len(data) > self.max_bytes:
                    raise DomainError("quota_exceeded", "Deletion journal size bound reached")
                target.write(data)
                target.flush()
                os.fsync(target.fileno())
            directory = os.open(self.root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
            added += len(needed)
        return added

    def sync(self, store: PostgresStore, actor: ActorContext) -> int:
        require_ops(actor)
        with store.transaction(actor) as tx:
            rows = tx.conn.execute(
                "SELECT group_id,source_key,event_id,platform_message_id,channel_account_id,reason FROM ga_source_tombstones WHERE group_id=%s ORDER BY source_key",
                (actor.group_id,),
            ).fetchall()
            from groupagent.memory.task_privacy import task_markers

            ids = [
                row["task_id"]
                for row in tx.conn.execute(
                    "SELECT task_id FROM ga_task_tombstones WHERE group_id=%s", (actor.group_id,)
                ).fetchall()
            ]
            rows.extend(task_markers(tx, ids))
        return self.append([DeletionMarker.model_validate(row) for row in rows])

    def append_tombstones(self, markers: list[dict[str, Any]]) -> int:
        """MemoryService calls this before committing a privacy transaction."""
        return self.append([DeletionMarker.model_validate(marker) for marker in markers])
