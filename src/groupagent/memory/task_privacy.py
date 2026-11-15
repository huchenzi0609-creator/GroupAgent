"""Field provenance and fail-closed privacy propagation for durable human tasks."""

from __future__ import annotations

from psycopg.types.json import Jsonb

from groupagent.persistence import UnitOfWork


def redact_tasks(tx: UnitOfWork, event_ids: list[str], task_ids: list[str]) -> list[str]:
    for task_id in task_ids:
        # A recovery journal may know an erasure newer than the archive's first
        # appearance of this task. Keep an ID-only row so deterministic legacy
        # import cannot recreate it after restoring that older archive.
        tx.conn.execute(
            "INSERT INTO ga_team_tasks(id,group_id,title,owner_id,assignment_kind,status,content_state) VALUES(%s,%s,'[task removed]',NULL,'unassigned','cancelled','forgotten') ON CONFLICT(id) DO NOTHING",
            (task_id, tx.group_id),
        )
        tx.conn.execute(
            "INSERT INTO ga_task_tombstones(group_id,task_id,deleted_by) SELECT group_id,id,%s FROM ga_team_tasks WHERE group_id=%s AND id=%s AND content_state='forgotten' ON CONFLICT DO NOTHING",
            (tx.actor.member_id, tx.group_id, task_id),
        )
    rows = tx.conn.execute(
        "SELECT * FROM ga_team_tasks WHERE group_id=%s AND content_state='active' AND (id=ANY(%s) OR source_event_ids ?| %s::text[]) ORDER BY id FOR UPDATE",
        (tx.group_id, task_ids, event_ids),
    ).fetchall()
    changed = []
    for row in rows:
        fields = row["field_sources"]
        affected = {name for name, sources in fields.items() if set(sources) & set(event_ids)}
        whole = row["id"] in task_ids or "title" in affected
        if not affected and not whole:
            continue
        remaining = (
            {}
            if whole
            else {name: sources for name, sources in fields.items() if name not in affected}
        )
        sources = sorted({source for values in remaining.values() for source in values})
        tx.conn.execute(
            "UPDATE ga_team_tasks SET title=%s,owner_id=%s,status=%s,progress=%s,evidence_refs=%s,due_at=%s,assignment_kind=%s,content_state=%s,field_sources=%s,source_event_ids=%s,revision=revision+1,updated_at=clock_timestamp() WHERE group_id=%s AND id=%s",
            (
                "[task removed]" if whole else row["title"],
                None if whole or "owner_id" in affected else row["owner_id"],
                "cancelled" if whole else ("open" if "status" in affected else row["status"]),
                "" if whole or "progress" in affected else row["progress"],
                Jsonb([] if whole or "evidence_refs" in affected else row["evidence_refs"]),
                None if whole or "due_at" in affected else row["due_at"],
                "unassigned" if whole or "owner_id" in affected else row["assignment_kind"],
                "forgotten" if whole else "active",
                Jsonb(remaining),
                Jsonb(sources),
                tx.group_id,
                row["id"],
            ),
        )
        if whole:
            tx.conn.execute(
                "INSERT INTO ga_task_tombstones(group_id,task_id,deleted_by) VALUES(%s,%s,%s) ON CONFLICT DO NOTHING",
                (tx.group_id, row["id"], tx.actor.member_id),
            )
            tx.conn.execute(
                "UPDATE ga_legacy_review r SET body='{}',state='rejected',evidence_ref='task-forgotten',revision=revision+1 FROM ga_legacy_map m WHERE m.group_id=%s AND m.entity_kind='team_task' AND m.target_id=%s AND r.group_id=m.group_id AND r.import_id=m.import_id AND r.body->>'legacy_task_id'=m.legacy_id",
                (tx.group_id, row["id"]),
            )
        changed.append(row["id"])
    return changed


def task_dependents(tx: UnitOfWork, task_ids: list[str]) -> list[str]:
    tx.conn.execute(
        "UPDATE ga_schedules SET title='[source removed]',goal='[source removed]',acceptance_criteria='[]',state='cancelled',pause_reason=NULL,revision=revision+1 WHERE group_id=%s AND scope->'source_task_revisions' ?| %s::text[] AND state<>'cancelled'",
        (tx.group_id, task_ids),
    )
    tx.conn.execute(
        "UPDATE ga_trigger_rules SET goal='[source removed]',state='cancelled',revision=revision+1 WHERE group_id=%s AND scope->'source_task_revisions' ?| %s::text[] AND state<>'cancelled'",
        (tx.group_id, task_ids),
    )
    rows = tx.conn.execute(
        "SELECT event_id FROM ga_decisions WHERE group_id=%s AND body->'source_task_revisions' ?| %s::text[] "
        "UNION SELECT request_event_id FROM ga_work_orders WHERE group_id=%s AND scope->'source_task_revisions' ?| %s::text[] "
        "UNION SELECT scope->>'request_source_id' FROM ga_schedules WHERE group_id=%s AND scope->'source_task_revisions' ?| %s::text[] "
        "UNION SELECT scope->>'request_source_id' FROM ga_trigger_rules WHERE group_id=%s AND scope->'source_task_revisions' ?| %s::text[]",
        (
            tx.group_id,
            task_ids,
            tx.group_id,
            task_ids,
            tx.group_id,
            task_ids,
            tx.group_id,
            task_ids,
        ),
    ).fetchall()
    return [row["event_id"] for row in rows if row["event_id"]]


def task_markers(tx: UnitOfWork, task_ids: list[str]) -> list[dict]:
    return [
        dict(
            group_id=tx.group_id,
            source_key="__groupagent_task__:" + row["task_id"],
            task_id=row["task_id"],
            event_id=None,
            platform_message_id=None,
            channel_account_id=None,
            reason="forgotten",
        )
        for row in tx.conn.execute(
            "SELECT task_id FROM ga_task_tombstones WHERE group_id=%s AND task_id=ANY(%s)",
            (tx.group_id, task_ids),
        ).fetchall()
    ]
