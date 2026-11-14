from __future__ import annotations

import hashlib
from datetime import UTC, datetime
from importlib.resources import files
from uuid import uuid4

import pytest

from groupagent.domain import DomainError, EventKind, GroupEvent, Role
from groupagent.memory import MemoryService
from groupagent.persistence import PostgresStore

pytestmark = pytest.mark.postgres


def test_old_task_creator_comes_from_actual_audit_not_the_referenced_author(
    database_factory, actor, member
):
    store = PostgresStore(database_factory())
    migrations = files("groupagent.persistence").joinpath("migrations")
    with store.connect() as conn:
        conn.execute(
            "CREATE TABLE ga_schema_migrations(version integer PRIMARY KEY,sha256 text NOT NULL,applied_at timestamptz NOT NULL DEFAULT clock_timestamp())"
        )
        for path in sorted(migrations.iterdir(), key=lambda value: value.name):
            if path.name.endswith(".sql") and int(path.name.split("_")[0]) <= 10:
                source = path.read_text()
                conn.execute(source)
                conn.execute(
                    "INSERT INTO ga_schema_migrations(version,sha256) VALUES(%s,%s)",
                    (int(path.name.split("_")[0]), hashlib.sha256(source.encode()).hexdigest()),
                )
    store.register_group(actor.group_id)
    actual_creator = actor.model_copy(
        update={"member_id": "synthetic-real-creator", "role": Role.OWNER}
    )
    tasks = []
    for audited in (True, False):
        stamp = datetime.now(UTC)
        event = GroupEvent(
            id=uuid4().hex,
            source_key=uuid4().hex,
            group_id=member.group_id,
            actor_id=member.member_id,
            kind=EventKind.TEXT,
            text="synthetic member context",
            occurred_at=stamp,
            received_at=stamp,
        )
        with store.transaction(member) as tx:
            tx.ingest_event(event, event.id, "1", 0)
        identifier = uuid4().hex
        with store.transaction(actual_creator) as tx:
            # Exact pre-011 storage shape: creator exists only in the trusted audit.
            tx.conn.execute(
                "INSERT INTO ga_team_tasks(id,group_id,title,owner_id,assignment_kind,source_event_id) VALUES(%s,%s,'owner assigned task',%s,'owner_assigned',%s)",
                (identifier, actor.group_id, member.member_id, event.id),
            )
            if audited:
                tx._audit(
                    "team_task_created",
                    identifier,
                    owner_id=member.member_id,
                    assignment_kind="owner_assigned",
                )
        tasks.append(identifier)
    initial = migrations.joinpath("011_task_provenance.sql").read_text()
    with store.connect() as conn:
        conn.execute(initial)
        conn.execute(
            "INSERT INTO ga_schema_migrations(version,sha256) VALUES(11,%s)",
            (hashlib.sha256(initial.encode()).hexdigest(),),
        )
        rows = conn.execute(
            "SELECT created_by FROM ga_team_tasks WHERE group_id=%s", (actor.group_id,)
        ).fetchall()
        assert {row["created_by"] for row in rows} == {member.member_id}
    stamp = datetime.now(UTC)
    later = GroupEvent(
        id=uuid4().hex,
        source_key=uuid4().hex,
        group_id=member.group_id,
        actor_id=actual_creator.member_id,
        kind=EventKind.TEXT,
        text="later member context",
        occurred_at=stamp,
        received_at=stamp,
    )
    with store.transaction(actual_creator) as tx:
        tx.ingest_event(later, later.id, "1", 0)
    with store.transaction(actual_creator) as tx:
        fresh = tx.create_team_task(
            "correctly attributed new task", owner_id=member.member_id, source_event_id=later.id
        )
        tx.conn.execute(
            "DELETE FROM ga_audit_events WHERE group_id=%s AND resource_ref=%s",
            (member.group_id, fresh.id),
        )
    # The corrective draft can be tested before freezing its immutable checksum.
    repair = migrations.joinpath("012_task_creator_authority.sql")
    if not repair.is_file():
        repair = migrations.joinpath("012_task_creator_authority.sql.draft")
    with store.connect() as conn:
        conn.execute(repair.read_text())
    with store.transaction(actual_creator) as tx:
        assert tx.get_team_task(tasks[0]).created_by == actual_creator.member_id
        assert tx.get_team_task(tasks[1]).created_by is None
        assert tx.get_team_task(fresh.id).created_by == actual_creator.member_id
    with pytest.raises(DomainError) as caught:
        MemoryService(store).forget(member, task_ids=[tasks[1]])
    assert caught.value.code == "permission_denied"
    MemoryService(store).forget(actual_creator, task_ids=[tasks[0]])
