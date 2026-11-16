"""State-based progress and missing-input notices; no invented percent complete."""

from __future__ import annotations

import hashlib

from groupagent.domain import ActorContext, DomainError, WorkState
from groupagent.memory.common import memory_lock
from groupagent.persistence import PostgresStore, UnitOfWork

from .contracts import ActorResolver
from .work_status import work_status_detail


def check_notice(tx: UnitOfWork, intent_id: str) -> None:
    memory_lock(tx)
    row = tx.conn.execute(
        "SELECT work_id,notice_key FROM ga_progress_notices WHERE group_id=%s AND intent_id=%s",
        (tx.group_id, intent_id),
    ).fetchone()
    if row is None:
        return
    if (
        row["notice_key"].split(":", 1)[0] in {"running", "validating"}
        and tx.get_delivery(intent_id).state.value == "pending"
    ):
        raise DomainError("source_unavailable", "Generic active-work notices are disabled")
    work = tx._work_row(row["work_id"])
    tx._check_work_source(work)
    state, revision, *_ = row["notice_key"].split(":")
    if work["state"] != state or str(work["goal_revision"]) != revision:
        raise DomainError("source_unavailable", "Notice was superseded by current work state")
    scope = work["scope"]
    tx._validate_work_sources(
        scope.get("source_fingerprints", {}),
        scope.get("retained_fact_source_ids", []),
        work["input_versions"],
    )


class NoticeService:
    def __init__(
        self,
        store: PostgresStore,
        technical: ActorContext,
        resolve: ActorResolver,
        *,
        interval_seconds: int = 60,
    ):
        self.store, self.technical, self.resolve, self.interval = (
            store,
            technical,
            resolve,
            interval_seconds,
        )

    def tick(self) -> int:
        with self.store.transaction(self.technical) as tx:
            rows = tx.conn.execute(
                "SELECT id,requester_id FROM ga_work_orders WHERE group_id=%s AND state IN "
                "('waiting_input','paused','failed') ORDER BY updated_at LIMIT 100",
                (tx.group_id,),
            ).fetchall()
        count = 0
        for row in rows:
            try:
                actor = self.resolve(row["requester_id"])
                with self.store.transaction(actor) as tx:
                    memory_lock(tx)
                    work = tx.get_work(row["id"])
                    if work.scope.get("privacy_redacted") or work.pause_reason in {
                        "user_request",
                        "cancelled",
                        "group_paused",
                    }:
                        continue
                    key = f"{work.state.value}:{work.goal_revision}"
                    if work.state == WorkState.WAITING_INPUT:
                        question = work.scope.get("_runtime_input_request")
                        text = (
                            str(question["question"])
                            if isinstance(question, dict)
                            and question.get("goal_revision") == work.goal_revision
                            else work_status_detail(work)["message"]
                        )
                    elif work.state == WorkState.FAILED:
                        text = work_status_detail(work)["message"]
                    elif work.state == WorkState.PAUSED:
                        text = work_status_detail(work)["message"]
                    else:
                        continue
                    if tx.conn.execute(
                        "SELECT 1 FROM ga_progress_notices WHERE group_id=%s AND work_id=%s AND notice_key=%s",
                        (tx.group_id, work.id, key),
                    ).fetchone():
                        continue
                    notice = tx.enqueue_delivery(
                        None,
                        "text:" + text,
                        hashlib.sha256(text.encode()).hexdigest(),
                        f"notice:{work.id}:{key}",
                    )
                    tx.conn.execute(
                        "INSERT INTO ga_progress_notices(group_id,work_id,notice_key,intent_id) VALUES(%s,%s,%s,%s)",
                        (tx.group_id, work.id, key, notice.id),
                    )
                    check_notice(tx, notice.id)
                    count += 1
            except DomainError:
                continue
        return count
