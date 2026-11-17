from __future__ import annotations

from datetime import UTC, datetime, timedelta
from uuid import uuid4

from psycopg.types.json import Jsonb

from groupagent.domain import ActorContext, DomainError, EventKind, GroupEvent, Role
from groupagent.memory.common import invalidate_summaries, memory_lock, technical
from groupagent.memory.deletions import DeletionSink
from groupagent.memory.models import CleanupJob, ForgetResult
from groupagent.memory.task_privacy import redact_tasks, task_dependents, task_markers
from groupagent.persistence import PostgresStore, UnitOfWork
from groupagent.persistence.text_parts import redact_removed_text

_PRIVACY_RANK = {"active": 0, "expired": 1, "recalled": 2, "forgotten": 3}


class ForgetMethods:
    store: PostgresStore
    deletion_sink: DeletionSink | None = None

    @staticmethod
    def _privacy_closure(
        tx: UnitOfWork, rows, extra_roots: list[str], *, include_human_requests: bool
    ):
        """Follow stored provenance, never shared-account identity or text equality."""
        full = {row["id"]: row for row in rows}
        changed = set(full) | set(extra_roots)
        controls = set(changed)
        quotes: dict[str, dict] = {}
        works: set[str] = set()
        schedules: set[str] = set()
        derived_deliveries: set[str] = set()
        triggers: set[str] = set()
        features = tx.conn.execute(
            "SELECT to_regclass('ga_delivery_parts') AS parts,to_regclass('ga_progress_notices') AS notices,"
            "to_regclass('ga_trigger_rules') AS triggers,to_regclass('ga_react_observations') AS observations,"
            "EXISTS(SELECT 1 FROM pg_attribute WHERE attrelid=to_regclass('ga_schedule_occurrences') "
            "AND attname='delivery_sources' AND NOT attisdropped) AS prepared_deliveries"
        ).fetchone()
        while True:
            before = (
                len(full),
                len(changed),
                len(controls),
                len(quotes),
                len(works),
                len(schedules),
                len(derived_deliveries),
                len(triggers),
            )
            quoted = tx.conn.execute(
                "SELECT * FROM ga_events WHERE group_id=%s AND kind='quote' AND body->>'reply_to'=ANY(%s)",
                (tx.group_id, list(changed)),
            ).fetchall()
            for row in quoted:
                changed.add(row["id"])
                if row["body"].get("trusted_actor_origin") == "authenticated_audio_transcription":
                    full[row["id"]] = row
                elif row["id"] not in full:
                    quotes[row["id"]] = row
            controls.update(changed)
            decisions = tx.conn.execute(
                "SELECT event_id FROM ga_decisions WHERE group_id=%s AND (event_id=ANY(%s) OR body->'source_fingerprints' ?| %s::text[] OR body->'source_event_ids' ?| %s::text[])",
                (tx.group_id, list(controls), list(controls), list(controls)),
            ).fetchall()
            controls.update(row["event_id"] for row in decisions)
            if include_human_requests:
                # A decision can have looked at unrelated turns. Only evidence
                # explicitly captured by a schedule can revoke that grant.
                rules = tx.conn.execute(
                    "SELECT id FROM ga_schedules WHERE group_id=%s AND (request_key=ANY(%s) OR scope->'source_fingerprints' ?| %s::text[] OR scope->>'request_source_id'=ANY(%s))",
                    (
                        tx.group_id,
                        ["chat:" + identifier for identifier in changed],
                        list(changed),
                        list(changed),
                    ),
                ).fetchall()
                schedules.update(row["id"] for row in rules)
                if features and features["prepared_deliveries"]:
                    derived_deliveries.update(
                        row["delivery_id"]
                        for row in tx.conn.execute(
                            "SELECT delivery_id FROM ga_schedule_occurrences WHERE group_id=%s "
                            "AND delivery_id IS NOT NULL AND delivery_sources && %s::text[]",
                            (tx.group_id, list(changed)),
                        ).fetchall()
                    )
            if features and features["observations"]:
                derived_deliveries.update(
                    row["delivery_id"]
                    for row in tx.conn.execute(
                        "SELECT delivery_id FROM ga_react_observations WHERE group_id=%s "
                        "AND delivery_id IS NOT NULL AND source_fingerprints ?| %s::text[]",
                        (tx.group_id, list(controls)),
                    ).fetchall()
                )
            if features and features["triggers"]:
                trigger_rows = tx.conn.execute(
                    "SELECT id FROM ga_trigger_rules WHERE group_id=%s AND (request_key=ANY(%s) OR scope->'source_fingerprints' ?| %s::text[] OR scope->>'request_source_id'=ANY(%s))",
                    (
                        tx.group_id,
                        ["chat:" + identifier for identifier in controls],
                        list(controls),
                        list(controls),
                    ),
                ).fetchall()
                triggers.update(row["id"] for row in trigger_rows)
                works.update(
                    row["work_id"]
                    for row in tx.conn.execute(
                        "SELECT work_id FROM ga_trigger_occurrences WHERE group_id=%s AND rule_id=ANY(%s) AND work_id IS NOT NULL",
                        (tx.group_id, list(triggers)),
                    ).fetchall()
                )
            works.update(
                row["work_id"]
                for row in tx.conn.execute(
                    "SELECT work_id FROM ga_schedule_occurrences WHERE group_id=%s AND schedule_id=ANY(%s) AND work_id IS NOT NULL",
                    (tx.group_id, list(schedules)),
                ).fetchall()
            )
            work_rows = tx.conn.execute(
                "SELECT id,request_event_id FROM ga_work_orders WHERE group_id=%s AND (id=ANY(%s) OR request_event_id=ANY(%s) OR scope->'source_fingerprints' ?| %s::text[] OR (%s AND COALESCE(scope->'human_request_event_ids','[]'::jsonb) ?| %s::text[])) ORDER BY id FOR UPDATE",
                (
                    tx.group_id,
                    list(works),
                    list(controls),
                    list(controls),
                    include_human_requests,
                    list(controls),
                ),
            ).fetchall()
            works.update(row["id"] for row in work_rows)
            machine_requests = tx.conn.execute(
                "SELECT e.* FROM ga_events e WHERE group_id=%s AND id=ANY(%s) AND kind='system' AND body->>'trusted_actor_origin' IN ('authorized_schedule','authorized_trigger')",
                (tx.group_id, [row["request_event_id"] for row in work_rows]),
            ).fetchall()
            for row in machine_requests:
                full[row["id"]] = row
                changed.add(row["id"])
                controls.add(row["id"])
            notice_ids = []
            if features and features["notices"]:
                notice_ids = [
                    row["intent_id"]
                    for row in tx.conn.execute(
                        "SELECT intent_id FROM ga_progress_notices WHERE group_id=%s AND work_id=ANY(%s)",
                        (tx.group_id, list(works)),
                    ).fetchall()
                ]
            deliveries = tx.conn.execute(
                "SELECT id,platform_message_id,state FROM ga_deliveries WHERE group_id=%s AND (request_event_id=ANY(%s) OR work_id=ANY(%s) OR id=ANY(%s) OR idempotency_key=ANY(%s)) ORDER BY id FOR UPDATE",
                (
                    tx.group_id,
                    list(controls),
                    list(works),
                    sorted(derived_deliveries | set(notice_ids)),
                    ["reply:" + identifier + ":1" for identifier in controls],
                ),
            ).fetchall()
            platform_ids = {
                row["platform_message_id"]
                for row in deliveries
                if row["state"] == "accepted" and row["platform_message_id"]
            }
            if features and features["parts"]:
                platform_ids.update(
                    row["platform_message_id"]
                    for row in tx.conn.execute(
                        "SELECT platform_message_id FROM ga_delivery_parts WHERE group_id=%s AND intent_id=ANY(%s) AND state='accepted' AND platform_message_id IS NOT NULL",
                        (tx.group_id, [row["id"] for row in deliveries]),
                    ).fetchall()
                )
            echoes = tx.conn.execute(
                "SELECT * FROM ga_events WHERE group_id=%s AND kind NOT IN ('recall','system','membership') AND body->>'platform_message_id'=ANY(%s)",
                (tx.group_id, list(platform_ids)),
            ).fetchall()
            for row in echoes:
                full[row["id"]] = row
                changed.add(row["id"])
                controls.add(row["id"])
            after = (
                len(full),
                len(changed),
                len(controls),
                len(quotes),
                len(works),
                len(schedules),
                len(derived_deliveries),
                len(triggers),
            )
            if after == before:
                return (
                    list(full.values()),
                    [row for key, row in quotes.items() if key not in full],
                    list(controls),
                    list(works),
                    list(schedules),
                    list(triggers),
                    sorted(derived_deliveries),
                )

    @staticmethod
    def _redact_decision(tx: UnitOfWork, event_id: str, reason: str) -> None:
        row = tx.conn.execute(
            "SELECT body FROM ga_decisions WHERE group_id=%s AND event_id=%s",
            (tx.group_id, event_id),
        ).fetchone()
        if not row:
            return
        old = row["body"]
        result = old.get("result")
        decision = result.get("decision") if isinstance(result, dict) else None
        code = decision.get("reason_code") if isinstance(decision, dict) else None
        previous = code.removeprefix("source_") if isinstance(code, str) else ""
        if _PRIVACY_RANK.get(previous, 0) > _PRIVACY_RANK[reason]:
            reason = previous
        body = {
            "kind": "ignore",
            "privacy_redacted": True,
            "source_event_ids": old.get("source_event_ids", []),
            "source_fingerprints": old.get("source_fingerprints", {}),
            "source_task_revisions": old.get("source_task_revisions", {}),
            "result": {
                "event_id": event_id,
                "decision": {"kind": "ignore", "reason_code": "source_" + reason},
                "work_id": None,
                "delivery_id": None,
                "duplicate": True,
            },
        }
        tx.conn.execute(
            "UPDATE ga_decisions SET body=%s WHERE group_id=%s AND event_id=%s",
            (Jsonb(body), tx.group_id, event_id),
        )

    @staticmethod
    def _clock(now: datetime | None) -> datetime:
        value = now or datetime.now(UTC)
        if value.utcoffset() is None:
            raise DomainError(
                "invalid_input", "Privacy operation timestamps must include a timezone"
            )
        return value.astimezone(UTC)

    def forget(
        self,
        actor: ActorContext,
        *,
        event_ids: list[str] | None = None,
        fact_ids: list[str] | None = None,
        task_ids: list[str] | None = None,
        now: datetime | None = None,
    ) -> ForgetResult:
        now = self._clock(now)
        ids, facts = list(dict.fromkeys(event_ids or [])), list(dict.fromkeys(fact_ids or []))
        tasks = list(dict.fromkeys(task_ids or []))
        if not ids and not facts and not tasks or len(ids) + len(facts) + len(tasks) > 1000:
            raise DomainError(
                "invalid_input", "Forget request must identify a bounded source or fact set"
            )
        with self.store.transaction(actor) as tx:
            memory_lock(tx)
            task_rows = tx.conn.execute(
                "SELECT id,created_by,source_event_ids FROM ga_team_tasks WHERE group_id=%s AND id=ANY(%s) ORDER BY id FOR UPDATE",
                (actor.group_id, tasks),
            ).fetchall()
            if len(task_rows) != len(tasks):
                raise DomainError("not_found", "Task privacy resource was not found in this group")
            if actor.role != Role.OWNER and any(
                row["created_by"] != actor.member_id for row in task_rows
            ):
                raise DomainError(
                    "permission_denied",
                    "Only the actual task creator or group owner may forget a task",
                )
            ids = list(
                dict.fromkeys(
                    [*ids, *(source for task in task_rows for source in task["source_event_ids"])]
                )
            )
            rows = tx.conn.execute(
                "SELECT * FROM ga_events WHERE group_id=%s AND id=ANY(%s) FOR UPDATE",
                (actor.group_id, ids),
            ).fetchall()
            facts_rows = tx.conn.execute(
                "SELECT * FROM ga_memory_facts WHERE group_id=%s AND id=ANY(%s) FOR UPDATE",
                (actor.group_id, facts),
            ).fetchall()
            if len(rows) != len(ids) or len(facts_rows) != len(facts):
                raise DomainError(
                    "not_found", "A requested privacy resource was not found in this group"
                )
            if facts:
                dependent = tx.conn.execute(
                    "SELECT DISTINCT event_id FROM ga_fact_sources WHERE group_id=%s AND fact_id=ANY(%s)",
                    (actor.group_id, facts),
                ).fetchall()
                ids = list(dict.fromkeys([*ids, *(item["event_id"] for item in dependent)]))
                rows = tx.conn.execute(
                    "SELECT * FROM ga_events WHERE group_id=%s AND id=ANY(%s) FOR UPDATE",
                    (actor.group_id, ids),
                ).fetchall()
            if actor.role != Role.OWNER and (
                any(row["actor_id"] != actor.member_id for row in rows)
                or any(row["created_by"] != actor.member_id for row in facts_rows)
            ):
                raise DomainError(
                    "permission_denied", "Only the source author or group owner may forget it"
                )
            return self._purge(
                tx, rows, facts, reason="forgotten", now=now, forget_facts=True, task_ids=tasks
            )

    def expire(
        self,
        actor: ActorContext,
        *,
        before: datetime,
        now: datetime | None = None,
        limit: int = 1000,
    ) -> ForgetResult:
        now = self._clock(now)
        cutoff = self._clock(before)
        if cutoff > now or not 1 <= limit <= 1000:
            raise DomainError("invalid_input", "Retention cutoff must not be in the future")
        with self.store.transaction(actor) as tx:
            technical(tx)
            memory_lock(tx)
            rows = tx.conn.execute(
                "SELECT * FROM ga_events WHERE group_id=%s AND memory_state='active' AND occurred_at<%s ORDER BY sequence LIMIT %s FOR UPDATE",
                (actor.group_id, cutoff, limit),
            ).fetchall()
            return self._purge(tx, rows, [], reason="expired", now=now, forget_facts=False)

    def recall(
        self, actor: ActorContext, recall_event_id: str, *, now: datetime | None = None
    ) -> ForgetResult:
        now = self._clock(now)
        with self.store.transaction(actor) as tx:
            technical(tx)
            memory_lock(tx)
            recall = tx.get_event(recall_event_id)
            if (
                recall.kind != EventKind.RECALL
                or not recall.reply_to
                or recall.trusted_actor_origin
                not in {"agentwechat_authenticated_recall_metadata", "verified_channel_metadata"}
            ):
                raise DomainError(
                    "permission_denied",
                    "Recall requires verified channel metadata and a native target",
                )
            rows = tx.conn.execute(
                "SELECT * FROM ga_events WHERE group_id=%s AND id=%s FOR UPDATE",
                (actor.group_id, recall.reply_to),
            ).fetchall()
            if not rows:
                tx.conn.execute(
                    "INSERT INTO ga_source_tombstones(group_id,source_key,event_id,platform_message_id,channel_account_id,reason) "
                    "VALUES (%s,%s,%s,%s,%s,'recalled') ON CONFLICT DO NOTHING",
                    (
                        actor.group_id,
                        recall.source_key.removesuffix(":recall"),
                        recall.reply_to,
                        recall.platform_message_id,
                        recall.channel_account_id,
                    ),
                )
                tx._audit("source_recall_tombstoned", recall.reply_to, recall_event_id=recall.id)
            return self._purge(
                tx,
                rows,
                [],
                reason="recalled",
                now=now,
                forget_facts=True,
                referenced_event_ids=[recall.reply_to],
            )

    @staticmethod
    def _cleanup(
        tx: UnitOfWork,
        *,
        event_id: str | None,
        work_id: str | None,
        kind: str,
        refs: list[str],
        not_before: datetime,
    ) -> str:
        identifier = uuid4().hex
        tx.conn.execute(
            "INSERT INTO ga_memory_cleanup(id,group_id,event_id,work_id,resource_kind,resource_refs,not_before) VALUES (%s,%s,%s,%s,%s,%s,%s)",
            (identifier, tx.group_id, event_id, work_id, kind, Jsonb(refs), not_before),
        )
        return identifier

    def _purge(
        self,
        tx: UnitOfWork,
        rows,
        fact_ids: list[str],
        *,
        reason: str,
        now: datetime,
        forget_facts: bool,
        referenced_event_ids: list[str] | None = None,
        task_ids: list[str] | None = None,
    ) -> ForgetResult:
        by_id = {row["id"]: row for row in rows}
        for row in list(rows):
            body = row["body"]
            if body.get("platform_message_id"):
                aliases = tx.conn.execute(
                    "SELECT * FROM ga_events WHERE group_id=%s AND actor_id=%s AND kind NOT IN ('recall','system','membership') "
                    "AND body->>'platform_message_id'=%s AND body->>'channel_account_id' IS NOT DISTINCT FROM %s FOR UPDATE",
                    (
                        tx.group_id,
                        row["actor_id"],
                        body["platform_message_id"],
                        body.get("channel_account_id"),
                    ),
                ).fetchall()
                by_id.update({alias["id"]: alias for alias in aliases})
        rows = list(by_id.values())
        ids = list(by_id)
        (
            rows,
            quotes,
            related_ids,
            closure_works,
            schedule_ids,
            trigger_ids,
            derived_deliveries,
        ) = self._privacy_closure(
            tx, rows, referenced_event_ids or [], include_human_requests=forget_facts
        )
        affected_tasks: set[str] = set(task_ids or [])
        changed_task_ids: set[str] = set()
        if forget_facts:
            while True:
                changed = redact_tasks(tx, related_ids, task_ids or [])
                affected_tasks.update(changed)
                changed_task_ids.update(changed)
                if not changed:
                    break
                whole = [
                    identifier
                    for identifier in changed
                    if tx.get_team_task(identifier).content_state == "forgotten"
                ]
                extra = task_dependents(tx, whole) if whole else []
                (
                    rows,
                    quotes,
                    related_ids,
                    closure_works,
                    schedule_ids,
                    trigger_ids,
                    derived_deliveries,
                ) = self._privacy_closure(
                    tx,
                    rows,
                    list(set(related_ids) | set(extra) | set(referenced_event_ids or [])),
                    include_human_requests=forget_facts,
                )
        ids = [row["id"] for row in rows]
        affected_facts = set(fact_ids)
        if forget_facts and related_ids:
            affected_facts.update(
                row["fact_id"]
                for row in tx.conn.execute(
                    "SELECT DISTINCT fact_id FROM ga_fact_sources WHERE group_id=%s AND event_id=ANY(%s)",
                    (tx.group_id, related_ids),
                ).fetchall()
            )
        sources = set(related_ids)
        if affected_facts:
            sources.update(
                row["event_id"]
                for row in tx.conn.execute(
                    "SELECT DISTINCT event_id FROM ga_fact_sources WHERE group_id=%s AND fact_id=ANY(%s)",
                    (tx.group_id, list(affected_facts)),
                ).fetchall()
            )
            tx.conn.execute(
                "UPDATE ga_memory_facts SET state='forgotten',predicate='',value='null'::jsonb,revision=revision+1,updated_at=clock_timestamp() WHERE group_id=%s AND id=ANY(%s)",
                (tx.group_id, list(affected_facts)),
            )
            tx.conn.execute(
                "UPDATE ga_fact_revisions SET state='forgotten',predicate='',value='null'::jsonb WHERE group_id=%s AND fact_id=ANY(%s)",
                (tx.group_id, list(affected_facts)),
            )
        invalidated = invalidate_summaries(tx, list(sources), forgotten=forget_facts)
        jobs: list[str] = []
        backup_deadline = now + timedelta(days=7)
        for task_id in sorted(changed_task_ids):
            jobs.append(
                self._cleanup(
                    tx,
                    event_id=None,
                    work_id=None,
                    kind="backup_tombstone",
                    refs=["task:" + task_id],
                    not_before=backup_deadline,
                )
            )
        media_refs: dict[str, list[str]] = {}
        media_table = tx.conn.execute(
            "SELECT to_regclass('ga_media_inputs') AS table_name"
        ).fetchone()
        if media_table and media_table["table_name"] and related_ids:
            cached = tx.conn.execute(
                "SELECT event_id,body FROM ga_media_inputs WHERE group_id=%s AND (event_id=ANY(%s) OR source_fingerprints ?| %s::text[]) FOR UPDATE",
                (tx.group_id, related_ids, related_ids),
            ).fetchall()
            for item in cached:
                body = item["body"]
                candidates = [body.get("reference")]
                if isinstance(body.get("imports"), dict):
                    candidates.extend(body["imports"].values())
                refs = media_refs.setdefault(item["event_id"], [])
                for candidate in candidates:
                    if (
                        isinstance(candidate, dict)
                        and isinstance(candidate.get("file_id"), str)
                        and isinstance(candidate.get("version_id"), str)
                    ):
                        refs.append(
                            "artifact:" + candidate["file_id"] + ":" + candidate["version_id"]
                        )
            tx.conn.execute(
                "UPDATE ga_media_inputs SET body='{}'::jsonb,available=false WHERE group_id=%s AND (event_id=ANY(%s) OR source_fingerprints ?| %s::text[])",
                (tx.group_id, related_ids, related_ids),
            )
        for row in rows:
            original = GroupEvent.model_validate(row["body"])
            effective_reason = max(
                (row["memory_state"], reason), key=lambda value: _PRIVACY_RANK[value]
            )
            # Persistent metadata is the replay tombstone. No original text,
            # attachment handle, filename or payload URL survives in event body.
            redacted = original.model_copy(
                update={
                    "text": "",
                    "attachments": [],
                    "payload_ref": None,
                    "content_state": effective_reason,
                }
            )
            tx.conn.execute(
                "UPDATE ga_events SET memory_state=%s,body=%s WHERE group_id=%s AND id=%s",
                (effective_reason, Jsonb(redacted.model_dump(mode="json")), tx.group_id, row["id"]),
            )
            tx.conn.execute(
                "INSERT INTO ga_source_tombstones(group_id,source_key,event_id,platform_message_id,channel_account_id,reason) "
                "VALUES (%s,%s,%s,%s,%s,%s) ON CONFLICT (group_id,source_key) DO UPDATE SET reason=CASE WHEN ga_source_tombstones.reason='forgotten' THEN 'forgotten' WHEN ga_source_tombstones.reason='recalled' AND EXCLUDED.reason='expired' THEN 'recalled' ELSE EXCLUDED.reason END",
                (
                    tx.group_id,
                    original.source_key,
                    original.id,
                    original.platform_message_id,
                    original.channel_account_id,
                    effective_reason,
                ),
            )
            self._redact_decision(tx, original.id, effective_reason)
            if row["memory_state"] != effective_reason:
                refs = list(media_refs.get(original.id, []))
                for attachment in original.attachments:
                    file_id, version_id = attachment.get("file_id"), attachment.get("version_id")
                    if isinstance(file_id, str) and isinstance(version_id, str):
                        refs.append("artifact:" + file_id + ":" + version_id)
                jobs.append(
                    self._cleanup(
                        tx,
                        event_id=original.id,
                        work_id=None,
                        kind="event_attachments",
                        refs=refs,
                        not_before=now,
                    )
                )
                jobs.append(
                    self._cleanup(
                        tx,
                        event_id=original.id,
                        work_id=None,
                        kind="backup_tombstone",
                        refs=["event:" + original.id],
                        not_before=backup_deadline,
                    )
                )
        for quote in quotes:
            original = GroupEvent.model_validate(quote["body"])
            sanitized = tx._sanitize_quote(original)
            tx.conn.execute(
                "UPDATE ga_events SET body=%s,memory_state=%s WHERE group_id=%s AND id=%s",
                (
                    Jsonb(sanitized.model_dump(mode="json")),
                    sanitized.content_state,
                    tx.group_id,
                    original.id,
                ),
            )
            if sanitized.content_state != "active":
                tx.conn.execute(
                    "INSERT INTO ga_source_tombstones(group_id,source_key,event_id,platform_message_id,channel_account_id,reason) VALUES (%s,%s,%s,%s,%s,%s) ON CONFLICT(group_id,source_key) DO UPDATE SET reason=EXCLUDED.reason",
                    (
                        tx.group_id,
                        original.source_key,
                        original.id,
                        original.platform_message_id,
                        original.channel_account_id,
                        sanitized.content_state,
                    ),
                )
            self._redact_decision(tx, original.id, reason)
            if sanitized.attachments != original.attachments:
                jobs.append(
                    self._cleanup(
                        tx,
                        event_id=original.id,
                        work_id=None,
                        kind="event_attachments",
                        refs=media_refs.get(original.id, []),
                        not_before=now,
                    )
                )
                jobs.append(
                    self._cleanup(
                        tx,
                        event_id=original.id,
                        work_id=None,
                        kind="backup_tombstone",
                        refs=["event:" + original.id],
                        not_before=backup_deadline,
                    )
                )
        if related_ids:
            for event_id in related_ids:
                self._redact_decision(tx, event_id, reason)
            works = tx.conn.execute(
                "SELECT id,state,scope FROM ga_work_orders WHERE group_id=%s AND "
                "(id=ANY(%s) OR request_event_id=ANY(%s) OR COALESCE(scope->'source_fingerprints','{}'::jsonb) ?| %s::text[] OR (%s AND COALESCE(scope->'human_request_event_ids','[]'::jsonb) ?| %s::text[])) ORDER BY id FOR UPDATE",
                (tx.group_id, closure_works, related_ids, related_ids, forget_facts, related_ids),
            ).fetchall()
            work_ids = [work["id"] for work in works]
            for work in works:
                if work["state"] not in {"completed", "cancelled", "failed"}:
                    tx.conn.execute(
                        "UPDATE ga_work_orders SET state='cancelled',pause_reason=NULL,revision=revision+1 WHERE group_id=%s AND id=%s",
                        (tx.group_id, work["id"]),
                    )
                    tx._revoke(work["id"])
                if forget_facts:
                    safe_scope = {
                        key: value
                        for key, value in work["scope"].items()
                        if key
                        in {
                            "allowed_group_ids",
                            "delivery_target",
                            "source_event_ids",
                            "source_fingerprints",
                            "source_task_revisions",
                        }
                    }
                    safe_scope["privacy_redacted"] = True
                    replacement = {
                        "goal": "[content removed by privacy request]",
                        "acceptance_criteria": [],
                        "input_versions": [],
                        "scope": safe_scope,
                    }
                    tx.conn.execute(
                        "UPDATE ga_work_orders SET goal=%s,acceptance_criteria='[]',input_versions='[]',scope=%s,revision=revision+1,updated_at=clock_timestamp() WHERE group_id=%s AND id=%s",
                        (replacement["goal"], Jsonb(safe_scope), tx.group_id, work["id"]),
                    )
                    tx.conn.execute(
                        "UPDATE ga_work_revisions SET body=body || %s::jsonb WHERE group_id=%s AND work_id=%s",
                        (Jsonb(replacement), tx.group_id, work["id"]),
                    )
                    jobs.append(
                        self._cleanup(
                            tx,
                            event_id=None,
                            work_id=work["id"],
                            kind="work_artifacts",
                            refs=[],
                            not_before=now,
                        )
                    )
            catalog = tx.conn.execute(
                "SELECT to_regclass('ga_artifact_catalog') AS present"
            ).fetchone()
            if forget_facts and catalog and catalog["present"]:
                tx.conn.execute(
                    "UPDATE ga_artifact_catalog SET available=false WHERE group_id=%s AND work_id=ANY(%s)",
                    (tx.group_id, work_ids),
                )
            notices = tx.conn.execute(
                "SELECT to_regclass('ga_progress_notices') AS table_name"
            ).fetchone()
            notice_ids = (
                [
                    item["intent_id"]
                    for item in tx.conn.execute(
                        "SELECT intent_id FROM ga_progress_notices WHERE group_id=%s AND work_id=ANY(%s)",
                        (tx.group_id, work_ids),
                    ).fetchall()
                ]
                if notices and notices["table_name"]
                else []
            )
            tx.conn.execute(
                "UPDATE ga_deliveries SET state=CASE WHEN state='pending' THEN 'rejected' ELSE state END, "
                "reason=CASE WHEN reason='source_forgotten' THEN reason WHEN reason='source_recalled' AND %s='source_expired' THEN reason ELSE %s END, "
                "payload_ref=CASE WHEN left(payload_ref,5)='text:' THEN 'text:[content removed]' ELSE payload_ref END, "
                "revision=revision+1 WHERE group_id=%s AND (request_event_id=ANY(%s) OR work_id=ANY(%s) OR idempotency_key=ANY(%s) OR id=ANY(%s))",
                (
                    "source_" + reason,
                    "source_" + reason,
                    tx.group_id,
                    related_ids,
                    work_ids,
                    ["reply:" + identifier + ":1" for identifier in related_ids],
                    sorted(set(notice_ids) | set(derived_deliveries)),
                ),
            )
            redact_removed_text(tx.conn, tx.group_id)
        if forget_facts:
            tx.conn.execute(
                "UPDATE ga_schedules SET title='[source removed]',goal='[source removed]',acceptance_criteria='[]',state='cancelled',next_due_at=NULL,pause_reason=NULL,revision=revision+1,updated_at=clock_timestamp() WHERE group_id=%s AND id=ANY(%s)",
                (tx.group_id, schedule_ids),
            )
            tx.conn.execute(
                "UPDATE ga_schedule_occurrences SET state='skipped',reason='source_removed',revision=revision+1 WHERE group_id=%s AND schedule_id=ANY(%s) AND state='pending'",
                (tx.group_id, schedule_ids),
            )
            if trigger_ids:
                tx.conn.execute(
                    "UPDATE ga_trigger_rules SET goal='[source removed]',state='cancelled',revision=revision+1 WHERE group_id=%s AND id=ANY(%s)",
                    (tx.group_id, trigger_ids),
                )
        if self.deletion_sink is not None:
            marker_ids = sorted(
                set(ids) | {quote["id"] for quote in quotes} | set(referenced_event_ids or [])
            )
            markers = tx.conn.execute(
                "SELECT group_id,source_key,event_id,platform_message_id,channel_account_id,reason FROM ga_source_tombstones WHERE group_id=%s AND event_id=ANY(%s) ORDER BY source_key",
                (tx.group_id, marker_ids),
            ).fetchall()
            try:
                self.deletion_sink.append_tombstones(
                    [dict(marker) for marker in markers] + task_markers(tx, list(affected_tasks))
                )
            except Exception as error:
                raise DomainError(
                    "deletion_journal_failed",
                    "Privacy deletion could not be durably recorded outside the database",
                ) from error
        tx._audit(
            "memory_" + reason,
            None,
            event_ids=ids,
            redacted_quote_ids=[quote["id"] for quote in quotes],
            fact_ids=sorted(affected_facts),
            task_ids=sorted(affected_tasks),
            invalidated_summaries=invalidated,
            cleanup_ids=jobs,
        )
        return ForgetResult(
            event_ids=ids,
            redacted_quote_ids=[quote["id"] for quote in quotes],
            fact_ids=sorted(affected_facts),
            task_ids=sorted(affected_tasks),
            invalidated_summaries=invalidated,
            cleanup_ids=jobs,
            backups_delete_after=backup_deadline,
        )

    def pending_cleanup(
        self, actor: ActorContext, *, limit: int = 100, now: datetime | None = None
    ) -> list[CleanupJob]:
        with self.store.transaction(actor) as tx:
            technical(tx)
            rows = tx.conn.execute(
                "SELECT * FROM ga_memory_cleanup WHERE group_id=%s AND state='pending' AND not_before<=%s ORDER BY created_at,id LIMIT %s",
                (actor.group_id, self._clock(now), min(max(limit, 1), 1000)),
            ).fetchall()
            return [CleanupJob.model_validate(row) for row in rows]

    def complete_cleanup(
        self, actor: ActorContext, job_id: str, *, expected_revision: int, evidence_ref: str
    ) -> CleanupJob:
        if not evidence_ref.strip():
            raise DomainError("invalid_input", "Cleanup completion requires deletion evidence")
        with self.store.transaction(actor) as tx:
            technical(tx)
            row = tx.conn.execute(
                "SELECT * FROM ga_memory_cleanup WHERE group_id=%s AND id=%s FOR UPDATE",
                (actor.group_id, job_id),
            ).fetchone()
            if row is None:
                raise DomainError("not_found", "Cleanup job was not found in this group")
            tx._revision(row, expected_revision)
            updated = tx.conn.execute(
                "UPDATE ga_memory_cleanup SET state='complete',evidence_ref=%s,revision=revision+1 WHERE group_id=%s AND id=%s RETURNING *",
                (evidence_ref, actor.group_id, job_id),
            ).fetchone()
            tx._audit("memory_cleanup_completed", job_id, evidence_ref=evidence_ref)
            return CleanupJob.model_validate(updated)
