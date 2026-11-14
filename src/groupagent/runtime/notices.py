"""State-based progress and missing-input notices; no invented percent complete."""

from __future__ import annotations

import hashlib

from groupagent.domain import ActorContext, DomainError, WorkState
from groupagent.memory.common import memory_lock
from groupagent.persistence import PostgresStore, UnitOfWork

from .contracts import ActorResolver


def check_notice(tx: UnitOfWork, intent_id: str) -> None:
    memory_lock(tx)
    row = tx.conn.execute(
        "SELECT work_id,notice_key FROM ga_progress_notices WHERE group_id=%s AND intent_id=%s",
        (tx.group_id, intent_id),
    ).fetchone()
    if row is None:
        return
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
                "('running','validating','waiting_input','paused','failed') ORDER BY updated_at LIMIT 100",
                (tx.group_id,),
            ).fetchall()
        count = 0
        for row in rows:
            try:
                actor = self.resolve(row["requester_id"])
                with self.store.transaction(actor) as tx:
                    memory_lock(tx)
                    work = tx.get_work(row["id"])
                    clock_row = tx.conn.execute("SELECT clock_timestamp() AS now").fetchone()
                    assert clock_row is not None
                    clock = clock_row["now"]
                    if work.scope.get("privacy_redacted") or work.pause_reason in {
                        "user_request",
                        "cancelled",
                        "group_paused",
                    }:
                        continue
                    key = f"{work.state.value}:{work.goal_revision}"
                    if work.state in {WorkState.RUNNING, WorkState.VALIDATING}:
                        if (clock - work.updated_at).total_seconds() < self.interval:
                            continue
                        last = tx.conn.execute(
                            "SELECT last_sent_at FROM ga_progress_notices WHERE group_id=%s AND work_id=%s ORDER BY last_sent_at DESC LIMIT 1",
                            (tx.group_id, work.id),
                        ).fetchone()
                        if last and (clock - last["last_sent_at"]).total_seconds() < self.interval:
                            continue
                        text = (
                            "这项工作仍在处理，完成验证后会把结果发回来。"
                            if work.state == WorkState.RUNNING
                            else "结果已生成，正在检查文件内容和格式。"
                        )
                    elif work.state == WorkState.WAITING_INPUT:
                        question = work.scope.get("_runtime_input_request")
                        text = (
                            str(question["question"])
                            if isinstance(question, dict)
                            and question.get("goal_revision") == work.goal_revision
                            else "这项工作需要补充可用资料，请把相关文件或要求发来。"
                        )
                    elif work.state == WorkState.FAILED:
                        text = "这项工作暂时未完成，已保留记录；你可以让我调整要求或重试。"
                    elif work.state == WorkState.PAUSED:
                        text = (
                            "这项工作已达到本次执行预算，已有进度和结果会保留；可以缩小范围后再继续。"
                            if work.pause_reason
                            and ("budget" in work.pause_reason or "limit" in work.pause_reason)
                            else "这项工作已暂停，费用或外部结果需要核对，暂时不会继续执行。"
                        )
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
