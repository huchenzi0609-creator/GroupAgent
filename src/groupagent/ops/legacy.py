"""Read-only, hash-pinned migration of the reviewed legacy SQLite schema.

No legacy module is imported or executed. Conversation text/summaries and
credentials are never copied; reminders become explicit review items only.
"""

from __future__ import annotations

import sqlite3
from datetime import UTC, datetime
from pathlib import Path
from uuid import NAMESPACE_URL, uuid4, uuid5
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from psycopg.types.json import Jsonb

from groupagent.domain import ActorContext, DomainError, TeamTaskState
from groupagent.memory.common import memory_lock
from groupagent.ops.common import digest, file_hash, require_ops
from groupagent.ops.models import ImportResult, LegacyMember, LegacyPlan, LegacyReview, LegacyTask
from groupagent.persistence import PostgresStore

LEGACY_SOURCE_HASHES = {
    "leader.py": "65df4623799d1b5aab68c59c3422409d7a91508e69145106131163c7c723816f",
    "memory.py": "f2ad5df5ad93bb32f1565890a03b6a7d2ebc3f13c014081d87f3f3c0d461cd3e",
}
_STATUS = {
    "待认领": TeamTaskState.OPEN,
    "进行中": TeamTaskState.IN_PROGRESS,
    "已完成": TeamTaskState.COMPLETED,
}


def _due(value: str, time_zone: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value)
        if parsed.utcoffset() is None:
            zone = ZoneInfo(time_zone)
            first = parsed.replace(tzinfo=zone, fold=0)
            second = parsed.replace(tzinfo=zone, fold=1)
            if (
                first.utcoffset() != second.utcoffset()
                or first.astimezone(UTC).astimezone(zone).replace(tzinfo=None) != parsed
            ):
                raise ValueError("Ambiguous local deadline")
            parsed = first
        return parsed.astimezone(UTC)
    except (ValueError, TypeError, ZoneInfoNotFoundError):
        raise DomainError(
            "invalid_legacy_date", "Legacy deadline requires an explicit unambiguous timezone"
        ) from None


class LegacyImporter:
    def __init__(
        self, store: PostgresStore, *, max_bytes: int = 128 * 1024**2, max_rows: int = 100000
    ) -> None:
        self.store = store
        self.max_bytes = max_bytes
        self.max_rows = max_rows

    def _read(
        self,
        path: Path,
        actor: ActorContext,
        legacy_group_id: str,
        source_system: str,
        time_zone: str,
    ) -> LegacyPlan:
        if path.suffix.lower() not in {".db", ".sqlite", ".sqlite3"} or path.is_symlink():
            raise DomainError("invalid_legacy_input", "Provide an explicit regular SQLite snapshot")
        for suffix in ("-wal", "-journal"):
            sidecar = Path(str(path) + suffix)
            if sidecar.exists() and sidecar.stat().st_size:
                raise DomainError(
                    "unsafe_legacy_snapshot",
                    "Checkpoint or export the legacy database before migration",
                )
        checksum, size = file_hash(path, max_bytes=self.max_bytes)
        with path.open("rb") as source:
            if source.read(16) != b"SQLite format 3\x00":
                raise DomainError("invalid_legacy_input", "Input is not a SQLite database")
        try:
            ZoneInfo(time_zone)
        except (ValueError, ZoneInfoNotFoundError):
            raise DomainError("invalid_timezone", "Legacy time zone must be an IANA name") from None
        members: dict[str, LegacyMember] = {}
        tasks = []
        reviews = []
        blockers = []
        excluded = 0
        connection = sqlite3.connect(path.resolve().as_uri() + "?mode=ro&immutable=1", uri=True)
        connection.row_factory = sqlite3.Row
        try:
            connection.execute("PRAGMA query_only=ON")
            tables = {
                row["name"]: row
                for row in connection.execute(
                    "SELECT name,type,sql FROM sqlite_master WHERE name NOT LIKE 'sqlite_%'"
                )
            }
            for name, required in {
                "members": {"group_id", "sender_id", "name"},
                "tasks": {
                    "id",
                    "group_id",
                    "title",
                    "owner_id",
                    "owner_name",
                    "due",
                    "status",
                    "result",
                },
            }.items():
                entry = tables.get(name)
                if entry is None or entry["type"] != "table" or "VIRTUAL" in entry["sql"].upper():
                    raise DomainError(
                        "unsupported_legacy_schema",
                        "Legacy tables do not match the reviewed schema",
                    )
                columns = {
                    row["name"] for row in connection.execute("PRAGMA table_info(" + name + ")")
                }
                if not required.issubset(columns):
                    raise DomainError(
                        "unsupported_legacy_schema", "Required legacy columns are missing"
                    )
            if connection.execute("PRAGMA quick_check").fetchone()[0] != "ok":
                raise DomainError("invalid_legacy_input", "Legacy database integrity check failed")
            rows = connection.execute(
                "SELECT * FROM members WHERE group_id=? ORDER BY sender_id LIMIT ?",
                (legacy_group_id, self.max_rows + 1),
            ).fetchall()
            if len(rows) > self.max_rows:
                raise DomainError("quota_exceeded", "Legacy member count exceeds its bound")
            for row in rows:
                identifier = row["sender_id"]
                if not isinstance(identifier, str) or not identifier.strip():
                    blockers.append("member_missing_stable_id")
                    continue
                origin = row["name_source"] if "name_source" in row.keys() else "manual"
                display_name = row["name"].strip() if isinstance(row["name"], str) else None
                if display_name == "群昵称待补充":
                    display_name = None
                members[identifier] = LegacyMember(
                    member_id=identifier,
                    room_name=display_name if origin == "wechat" else None,
                    confirmed_name=display_name if origin == "confirmed" else None,
                    name_source=str(origin),
                )
                if display_name and origin not in {"wechat", "confirmed"}:
                    reviews.append(
                        LegacyReview(
                            kind="name_confirmation",
                            legacy_key=identifier,
                            body={
                                "member_id": identifier,
                                "candidate_name": display_name,
                                "legacy_name_source": str(origin),
                            },
                        )
                    )
            updates = {}
            if "updates" in tables:
                updates = {
                    str(row["task_id"]): row["note"]
                    for row in connection.execute(
                        "SELECT u.task_id,u.note FROM updates u JOIN tasks t ON t.id=u.task_id WHERE t.group_id=?",
                        (legacy_group_id,),
                    )
                }
            rows = connection.execute(
                "SELECT * FROM tasks WHERE group_id=? ORDER BY id LIMIT ?",
                (legacy_group_id, self.max_rows + 1),
            ).fetchall()
            if len(rows) > self.max_rows:
                raise DomainError("quota_exceeded", "Legacy task count exceeds its bound")
            for row in rows:
                legacy_id = str(row["id"])
                if (
                    row["status"] not in _STATUS
                    or not isinstance(row["title"], str)
                    or not row["title"].strip()
                ):
                    blockers.append("task_unsupported_status_or_title:" + legacy_id)
                    continue
                owner = (
                    row["owner_id"]
                    if isinstance(row["owner_id"], str) and row["owner_id"].strip()
                    else None
                )
                if owner and owner not in members:
                    members[owner] = LegacyMember(member_id=owner, name_source="unknown")
                note = updates.get(legacy_id, "") or ""
                result = row["result"] or ""
                if not isinstance(note, str) or not isinstance(result, str):
                    blockers.append("task_invalid_progress:" + legacy_id)
                    continue
                progress = (
                    ("进展：" + note + "\n成果：" + result)
                    if note and result and note != result
                    else note or result
                )
                try:
                    deadline = _due(row["due"], time_zone)
                except DomainError:
                    blockers.append("task_ambiguous_due:" + legacy_id)
                    continue
                target = str(
                    uuid5(
                        NAMESPACE_URL,
                        "groupagent-legacy:"
                        + digest(
                            [actor.group_id, source_system, legacy_group_id, "task", legacy_id]
                        ),
                    )
                )
                tasks.append(
                    LegacyTask(
                        legacy_id=legacy_id,
                        target_id=target,
                        title=row["title"],
                        owner_id=owner,
                        due_at=deadline,
                        status=_STATUS[row["status"]],
                        progress=progress,
                    )
                )
            if "reminders" in tables:
                for row in connection.execute(
                    "SELECT r.* FROM reminders r JOIN tasks t ON t.id=r.task_id WHERE t.group_id=?",
                    (legacy_group_id,),
                ):
                    reviews.append(
                        LegacyReview(
                            kind="reminder",
                            legacy_key=str(row["task_id"]) + ":" + str(row["kind"]),
                            body={
                                "legacy_task_id": str(row["task_id"]),
                                "kind": row["kind"],
                                "body": row["body"],
                                "previously_delivered": bool(row["delivered"]),
                            },
                        )
                    )
            if "checkins" in tables:
                for row in connection.execute(
                    "SELECT day,body,delivered FROM checkins WHERE group_id=?", (legacy_group_id,)
                ):
                    reviews.append(
                        LegacyReview(
                            kind="daily_followup",
                            legacy_key=str(row["day"]),
                            body={
                                "day": row["day"],
                                "body": row["body"],
                                "previously_delivered": bool(row["delivered"]),
                            },
                        )
                    )
            if "conversation_turns" in tables:
                excluded = connection.execute(
                    "SELECT count(*) FROM conversation_turns WHERE group_id=?", (legacy_group_id,)
                ).fetchone()[0]
            if "conversation_summary" in tables:
                count = connection.execute(
                    "SELECT count(*) FROM conversation_summary WHERE group_id=?", (legacy_group_id,)
                ).fetchone()[0]
                if count:
                    reviews.append(
                        LegacyReview(
                            kind="historical_summary",
                            legacy_key="summary",
                            body={"rows": count, "imported_as_authoritative_memory": False},
                        )
                    )
        except sqlite3.DatabaseError:
            raise DomainError(
                "invalid_legacy_input", "Legacy snapshot could not be read safely"
            ) from None
        finally:
            connection.close()
        if file_hash(path, max_bytes=self.max_bytes) != (checksum, size):
            raise DomainError("source_changed", "Legacy input changed during the read-only plan")
        raw = {
            "source_sha256": checksum,
            "source_bytes": size,
            "source_system": source_system,
            "legacy_group_id": legacy_group_id,
            "group_id": actor.group_id,
            "time_zone": time_zone,
            "source_code_hashes": LEGACY_SOURCE_HASHES,
            "members": [member.model_dump(mode="json") for member in members.values()],
            "tasks": [task.model_dump(mode="json") for task in tasks],
            "reviews": [item.model_dump(mode="json") for item in reviews],
            "blockers": blockers,
            "excluded_conversation_rows": excluded,
        }
        return LegacyPlan.model_validate({"id": uuid4().hex, "plan_sha256": digest(raw), **raw})

    def plan(
        self,
        actor: ActorContext,
        path: Path,
        legacy_group_id: str,
        *,
        time_zone: str = "Asia/Shanghai",
        source_system: str = "wechat-leader-v1",
    ) -> LegacyPlan:
        require_ops(actor)
        with self.store.transaction(actor):
            pass
        result = self._read(path, actor, legacy_group_id, source_system, time_zone)
        summary = {
            "members": len(result.members),
            "tasks": len(result.tasks),
            "review_items": len(result.reviews),
            "blockers": result.blockers,
            "excluded_conversation_rows": result.excluded_conversation_rows,
            "source_code_hashes": LEGACY_SOURCE_HASHES,
        }
        with self.store.transaction(actor) as tx:
            row = tx.conn.execute(
                "INSERT INTO ga_legacy_plans(id,group_id,source_system,legacy_group_id,source_sha256,plan_sha256,time_zone,summary,created_by) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING RETURNING id",
                (
                    result.id,
                    actor.group_id,
                    source_system,
                    legacy_group_id,
                    result.source_sha256,
                    result.plan_sha256,
                    time_zone,
                    Jsonb(summary),
                    actor.member_id,
                ),
            ).fetchone()
            if row is None:
                row = tx.conn.execute(
                    "SELECT id,plan_sha256 FROM ga_legacy_plans WHERE group_id=%s AND source_system=%s AND legacy_group_id=%s AND source_sha256=%s",
                    (actor.group_id, source_system, legacy_group_id, result.source_sha256),
                ).fetchone()
                if row is None or row["plan_sha256"] != result.plan_sha256:
                    raise DomainError(
                        "migration_conflict",
                        "An existing source plan uses different transformation semantics",
                    )
            return result.model_copy(update={"id": row["id"]})

    def apply(
        self, actor: ActorContext, path: Path, plan_id: str, *, expected_source_hash: str
    ) -> ImportResult:
        require_ops(actor)
        with self.store.transaction(actor) as tx:
            record = tx.conn.execute(
                "SELECT * FROM ga_legacy_plans WHERE group_id=%s AND id=%s",
                (actor.group_id, plan_id),
            ).fetchone()
        if record is None:
            raise DomainError("not_found", "Migration plan was not found in this group")
        if expected_source_hash != record["source_sha256"]:
            raise DomainError("source_changed", "Approved source hash does not match the plan")
        plan = self._read(
            path, actor, record["legacy_group_id"], record["source_system"], record["time_zone"]
        )
        if plan.source_sha256 != expected_source_hash or plan.plan_sha256 != record["plan_sha256"]:
            raise DomainError(
                "source_changed", "Legacy source or transformation changed after planning"
            )
        if plan.blockers:
            raise DomainError(
                "migration_blocked", "Resolve the dry-run schema/data blockers before applying"
            )
        with self.store.transaction(actor) as tx:
            memory_lock(tx)
            record = tx.conn.execute(
                "SELECT * FROM ga_legacy_plans WHERE group_id=%s AND id=%s FOR UPDATE",
                (actor.group_id, plan_id),
            ).fetchone()
            assert record is not None
            if record["state"] == "applied":
                return ImportResult(
                    plan_id=plan_id,
                    members=len(plan.members),
                    tasks=len(plan.tasks),
                    review_items=len(plan.reviews),
                    already_applied=True,
                )

            def mapped(kind, legacy_id, row_hash):
                item = tx.conn.execute(
                    "SELECT * FROM ga_legacy_map WHERE group_id=%s AND source_system=%s AND legacy_group_id=%s AND entity_kind=%s AND legacy_id=%s",
                    (actor.group_id, plan.source_system, plan.legacy_group_id, kind, legacy_id),
                ).fetchone()
                if item and item["source_row_sha256"] != row_hash:
                    raise DomainError(
                        "migration_conflict",
                        "Previously migrated source changed; review instead of overwriting current records",
                    )
                return item

            def remember(kind, legacy_id, target_id, row_hash, revision):
                tx.conn.execute(
                    "INSERT INTO ga_legacy_map(group_id,source_system,legacy_group_id,entity_kind,legacy_id,target_id,source_row_sha256,target_revision,import_id) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                    (
                        actor.group_id,
                        plan.source_system,
                        plan.legacy_group_id,
                        kind,
                        legacy_id,
                        target_id,
                        row_hash,
                        revision,
                        plan_id,
                    ),
                )

            for member in plan.members:
                row_hash = digest(member.model_dump(mode="json"))
                if mapped("member", member.member_id, row_hash):
                    continue
                current = tx.conn.execute(
                    "SELECT * FROM ga_members WHERE group_id=%s AND member_id=%s FOR UPDATE",
                    (actor.group_id, member.member_id),
                ).fetchone()
                if current:
                    updated = tx.conn.execute(
                        "UPDATE ga_members SET room_name=COALESCE(room_name,%s),confirmed_name=COALESCE(confirmed_name,%s),revision=revision+1,updated_at=clock_timestamp() WHERE group_id=%s AND member_id=%s RETURNING revision",
                        (member.room_name, member.confirmed_name, actor.group_id, member.member_id),
                    ).fetchone()
                else:
                    updated = tx.conn.execute(
                        "INSERT INTO ga_members(group_id,member_id,room_name,confirmed_name,membership_state) VALUES (%s,%s,%s,%s,'unknown') RETURNING revision",
                        (actor.group_id, member.member_id, member.room_name, member.confirmed_name),
                    ).fetchone()
                assert updated is not None
                remember(
                    "member", member.member_id, member.member_id, row_hash, updated["revision"]
                )
            for task in plan.tasks:
                row_hash = digest(task.model_dump(mode="json"))
                if mapped("team_task", task.legacy_id, row_hash):
                    continue
                inserted = tx.conn.execute(
                    "INSERT INTO ga_team_tasks(id,group_id,title,owner_id,assignment_kind,due_at,status,progress,evidence_refs) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING RETURNING revision",
                    (
                        task.target_id,
                        actor.group_id,
                        task.title,
                        task.owner_id,
                        "legacy_import" if task.owner_id else "unassigned",
                        task.due_at,
                        task.status.value,
                        task.progress,
                        Jsonb(["legacy:" + plan.source_system + ":task:" + task.legacy_id]),
                    ),
                ).fetchone()
                if inserted is None:
                    raise DomainError(
                        "migration_conflict",
                        "Target task identity already exists without this migration provenance",
                    )
                remember(
                    "team_task", task.legacy_id, task.target_id, row_hash, inserted["revision"]
                )
            for item in plan.reviews:
                tx.conn.execute(
                    "INSERT INTO ga_legacy_review(id,group_id,import_id,kind,legacy_key,body) VALUES (%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING",
                    (
                        uuid4().hex,
                        actor.group_id,
                        plan_id,
                        item.kind,
                        item.legacy_key,
                        Jsonb(item.body),
                    ),
                )
            tx.conn.execute(
                "UPDATE ga_legacy_plans SET state='applied',applied_at=clock_timestamp() WHERE group_id=%s AND id=%s",
                (actor.group_id, plan_id),
            )
            tx.conn.execute(
                "UPDATE ga_memory_epochs SET epoch=epoch+1 WHERE group_id=%s", (actor.group_id,)
            )
            tx._audit(
                "legacy_import_applied",
                plan_id,
                source_sha256=plan.source_sha256,
                members=len(plan.members),
                tasks=len(plan.tasks),
                pending_review=len(plan.reviews),
            )
            return ImportResult(
                plan_id=plan_id,
                members=len(plan.members),
                tasks=len(plan.tasks),
                review_items=len(plan.reviews),
            )

    def reviews(self, actor: ActorContext, plan_id: str) -> list[dict]:
        require_ops(actor)
        with self.store.transaction(actor) as tx:
            return tx.conn.execute(
                "SELECT id,kind,legacy_key,body,state,revision FROM ga_legacy_review WHERE group_id=%s AND import_id=%s ORDER BY kind,legacy_key",
                (actor.group_id, plan_id),
            ).fetchall()
