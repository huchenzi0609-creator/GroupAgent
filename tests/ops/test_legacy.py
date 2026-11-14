from __future__ import annotations

import hashlib
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier

import pytest

from groupagent.domain import DomainError, Role
from groupagent.memory import MemberService
from groupagent.ops import LegacyImporter

pytestmark = pytest.mark.postgres


@pytest.fixture
def legacy(tmp_path):
    path = tmp_path / "synthetic-legacy.sqlite"
    with sqlite3.connect(path) as db:
        db.executescript("""
        CREATE TABLE members(group_id TEXT,sender_id TEXT,name TEXT,name_source TEXT,PRIMARY KEY(group_id,sender_id));
        CREATE TABLE tasks(id INTEGER PRIMARY KEY,group_id TEXT,title TEXT,owner_id TEXT,owner_name TEXT,due TEXT,status TEXT,result TEXT);
        CREATE TABLE updates(task_id INTEGER PRIMARY KEY,note TEXT,updated_at TEXT);
        CREATE TABLE reminders(task_id INTEGER,kind TEXT,body TEXT,delivered INTEGER,PRIMARY KEY(task_id,kind));
        CREATE TABLE checkins(group_id TEXT,day TEXT,body TEXT,delivered INTEGER);
        CREATE TABLE conversation_turns(group_id TEXT,user_text TEXT);
        CREATE TABLE conversation_summary(group_id TEXT,through_seq INTEGER,summary TEXT);
        """)
        db.executemany(
            "INSERT INTO members VALUES (?,?,?,?)",
            [
                ("legacy-group", "900719925474099312345", "群昵称甲", "wechat"),
                ("legacy-group", "member-confirmed", "人工确认乙", "confirmed"),
                ("legacy-group", "member-manual", "未确认个人名", "manual"),
                ("foreign-group", "foreign-member", "foreign-private-name", "wechat"),
            ],
        )
        db.executemany(
            "INSERT INTO tasks VALUES (?,?,?,?,?,?,?,?)",
            [
                (
                    1,
                    "legacy-group",
                    "已分工报告",
                    "900719925474099312345",
                    "不可信旧显示名",
                    "2030-01-01T20:00:00+08:00",
                    "进行中",
                    "结果草稿",
                ),
                (
                    2,
                    "legacy-group",
                    "待认领PPT",
                    None,
                    None,
                    "2030-01-02T20:00:00+08:00",
                    "待认领",
                    "",
                ),
                (
                    3,
                    "foreign-group",
                    "foreign private task",
                    "foreign-member",
                    "x",
                    "2030-01-02T20:00:00+08:00",
                    "已完成",
                    "foreign",
                ),
            ],
        )
        db.execute("INSERT INTO updates VALUES (1,'正在整理资料','2029-12-30T10:00:00+08:00')")
        db.execute(
            "INSERT INTO reminders VALUES (1,'24小时内到期','old reminder requires owner review',0)"
        )
        db.execute(
            "INSERT INTO checkins VALUES ('legacy-group','2029-12-30','old daily followup',1)"
        )
        db.execute("INSERT INTO conversation_turns VALUES ('legacy-group','DO NOT COPY RAW CHAT')")
        db.execute(
            "INSERT INTO conversation_summary VALUES ('legacy-group',1,'DO NOT PROMOTE OLD SUMMARY')"
        )
    return path


def test_dry_run_is_read_only_and_creates_no_business_work(store, actor, legacy):
    original = hashlib.sha256(legacy.read_bytes()).hexdigest()
    plan = LegacyImporter(store).plan(actor, legacy, "legacy-group")
    assert (
        plan.source_sha256 == original
        and hashlib.sha256(legacy.read_bytes()).hexdigest() == original
    )
    assert len(plan.members) == 3 and len(plan.tasks) == 2 and not plan.blockers
    assert plan.excluded_conversation_rows == 1
    assert {review.kind for review in plan.reviews} == {
        "name_confirmation",
        "reminder",
        "daily_followup",
        "historical_summary",
    }
    assert "foreign-private" not in plan.model_dump_json()
    with store.transaction(actor) as tx:
        assert tx.list_team_tasks() == [] and tx.list_work() == []
        assert tx.list_pending_events() == []


def test_apply_preserves_ids_name_exceptions_human_tasks_and_is_idempotent(store, actor, legacy):
    importer = LegacyImporter(store)
    plan = importer.plan(actor, legacy, "legacy-group")
    first = importer.apply(actor, legacy, plan.id, expected_source_hash=plan.source_sha256)
    second = importer.apply(actor, legacy, plan.id, expected_source_hash=plan.source_sha256)
    assert first.tasks == 2 and second.already_applied
    members = MemberService(store)
    assert members.get(actor, "900719925474099312345").display_name == "群昵称甲"
    assert members.get(actor, "member-confirmed").confirmed_name == "人工确认乙"
    assert members.get(actor, "member-manual").display_name == "群昵称待补充"
    with store.transaction(actor) as tx:
        tasks = tx.list_team_tasks()
        assigned = next(task for task in tasks if task.owner_id)
        unassigned = next(task for task in tasks if task.owner_id is None)
        assert (
            assigned.owner_id == "900719925474099312345"
            and assigned.assignment_kind == "legacy_import"
        )
        assert "正在整理资料" in assigned.progress and "结果草稿" in assigned.progress
        assert unassigned.assignment_kind == "unassigned"
        assert tx.list_work() == []
        assert (
            tx.conn.execute(
                "SELECT count(*) AS n FROM ga_schedules WHERE group_id=%s", (actor.group_id,)
            ).fetchone()["n"]
            == 0
        )
        assert (
            tx.conn.execute(
                "SELECT count(*) AS n FROM ga_memory_summaries WHERE group_id=%s", (actor.group_id,)
            ).fetchone()["n"]
            == 0
        )
    assert all(item["state"] == "pending" for item in importer.reviews(actor, plan.id))


def test_source_changed_after_plan_is_rejected_without_partial_apply(store, actor, legacy):
    importer = LegacyImporter(store)
    plan = importer.plan(actor, legacy, "legacy-group")
    with sqlite3.connect(legacy) as db:
        db.execute("UPDATE tasks SET title='changed after approval' WHERE id=1")
    with pytest.raises(DomainError) as caught:
        importer.apply(actor, legacy, plan.id, expected_source_hash=plan.source_sha256)
    assert caught.value.code == "source_changed"
    with store.transaction(actor) as tx:
        assert tx.list_team_tasks() == []


def test_unknown_status_blocks_apply_but_is_visible_in_dry_run(store, actor, legacy):
    with sqlite3.connect(legacy) as db:
        db.execute("UPDATE tasks SET status='unrecognized' WHERE id=1")
    importer = LegacyImporter(store)
    plan = importer.plan(actor, legacy, "legacy-group")
    assert plan.blockers
    with pytest.raises(DomainError) as caught:
        importer.apply(actor, legacy, plan.id, expected_source_hash=plan.source_sha256)
    assert caught.value.code == "migration_blocked"
    with store.transaction(actor) as tx:
        assert tx.list_team_tasks() == []


def test_replay_never_overwrites_current_task_edits(store, actor, legacy):
    importer = LegacyImporter(store)
    plan = importer.plan(actor, legacy, "legacy-group")
    importer.apply(actor, legacy, plan.id, expected_source_hash=plan.source_sha256)
    owner = actor.model_copy(update={"member_id": "owner", "role": Role.OWNER})
    with store.transaction(owner) as tx:
        task = tx.list_team_tasks()[0]
        tx.update_team_task(
            task.id, expected_revision=task.revision, progress="new system progress"
        )
    assert importer.apply(
        actor, legacy, plan.id, expected_source_hash=plan.source_sha256
    ).already_applied
    with store.transaction(actor) as tx:
        assert tx.get_team_task(task.id).progress == "new system progress"


def test_changed_export_cannot_overwrite_previously_migrated_rows(store, actor, legacy):
    importer = LegacyImporter(store)
    first = importer.plan(actor, legacy, "legacy-group")
    importer.apply(actor, legacy, first.id, expected_source_hash=first.source_sha256)
    with sqlite3.connect(legacy) as db:
        db.execute("UPDATE tasks SET title='changed legacy title' WHERE id=1")
    next_plan = importer.plan(actor, legacy, "legacy-group")
    with pytest.raises(DomainError) as caught:
        importer.apply(actor, legacy, next_plan.id, expected_source_hash=next_plan.source_sha256)
    assert caught.value.code == "migration_conflict"
    with store.transaction(actor) as tx:
        assert "changed legacy title" not in {task.title for task in tx.list_team_tasks()}


def test_current_name_and_departure_state_are_preserved(store, actor, legacy):
    with store.transaction(actor) as tx:
        tx.conn.execute(
            "INSERT INTO ga_members(group_id,member_id,room_name,membership_state) VALUES (%s,'900719925474099312345','current room name','left')",
            (actor.group_id,),
        )
    importer = LegacyImporter(store)
    plan = importer.plan(actor, legacy, "legacy-group")
    importer.apply(actor, legacy, plan.id, expected_source_hash=plan.source_sha256)
    person = MemberService(store).get(actor, "900719925474099312345")
    assert person.room_name == "current room name" and person.membership_state == "left"


def test_non_operator_cannot_inspect_or_apply_sqlite(store, member, legacy):
    with pytest.raises(DomainError) as caught:
        LegacyImporter(store).plan(member, legacy, "legacy-group")
    assert caught.value.code == "permission_denied"


def test_sqlite_sidecar_and_symlink_are_rejected(store, actor, legacy, tmp_path):
    sidecar = legacy.with_name(legacy.name + "-wal")
    sidecar.write_bytes(b"uncheckpointed data")
    with pytest.raises(DomainError) as caught:
        LegacyImporter(store).plan(actor, legacy, "legacy-group")
    assert caught.value.code == "unsafe_legacy_snapshot"
    sidecar.unlink()
    link = tmp_path / "linked.sqlite"
    link.symlink_to(legacy)
    with pytest.raises(DomainError) as caught:
        LegacyImporter(store).plan(actor, link, "legacy-group")
    assert caught.value.code == "invalid_legacy_input"


def test_unassigned_legacy_task_can_be_claimed_by_only_one_member(store, actor, member, legacy):
    importer = LegacyImporter(store)
    plan = importer.plan(actor, legacy, "legacy-group")
    importer.apply(actor, legacy, plan.id, expected_source_hash=plan.source_sha256)
    with store.transaction(member) as tx:
        task = next(item for item in tx.list_team_tasks() if item.owner_id is None)
    other = member.model_copy(update={"member_id": "member-b"})
    barrier = Barrier(2)

    def claim(context):
        barrier.wait()
        try:
            with store.transaction(context) as tx:
                return tx.claim_team_task(task.id, expected_revision=1).owner_id
        except DomainError as error:
            return error.code

    with ThreadPoolExecutor(max_workers=2) as executor:
        outcomes = list(executor.map(claim, [member, other]))
    assert outcomes.count("version_conflict") == 1
    winner = next(value for value in outcomes if value != "version_conflict")
    with store.transaction(actor) as tx:
        assigned = tx.get_team_task(task.id)
        assert assigned.owner_id == winner and assigned.assignment_kind == "self_claim"
        assert tx.list_work() == []
