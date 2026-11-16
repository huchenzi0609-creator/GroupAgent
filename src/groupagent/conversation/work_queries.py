"""Render public work facts from current reads, independently of dialogue focus."""

from __future__ import annotations

from typing import Any

from groupagent.control import WorkSelector
from groupagent.coordinator import Decision
from groupagent.domain import GroupEvent
from groupagent.persistence import UnitOfWork
from groupagent.runtime.work_facts import WorkFacts


def compact_work_overview(snapshot: dict[str, Any]) -> dict[str, Any]:
    """Keep live awareness small; this display is neither focus nor full history."""
    fields = (
        "id",
        "requester_id",
        "request_event_id",
        "state",
        "revision",
        "goal_revision",
        "updated_at",
        "presentation",
        "status_detail",
        "outputs",
        "delivery",
    )
    return {key: value for key, value in snapshot.items() if key != "facts"} | {
        "facts": [
            {**{key: row[key] for key in fields if key in row}, "goal": row["goal"][:280]}
            for row in snapshot.get("facts", [])
        ]
    }


def query_work_in_tx(
    tx: UnitOfWork, event: GroupEvent, decision: Decision, context: dict[str, Any], facts: WorkFacts
) -> dict[str, Any]:
    """Reads refresh canonical state; they never claim a work or apply write CAS."""
    selector = decision.work_selector or WorkSelector(
        basis="ids", ids=(decision.target_work_id or "",)
    )
    identifiers = selector.ids
    if selector.basis == "focus":
        identifiers = tuple(context.get("_focus_work_ids") or ())
        if not identifiers and context.get("_focus_work_id"):
            identifiers = (context["_focus_work_id"],)
    elif selector.basis == "quote":
        identifiers = (context["_quoted_work_id"],) if context.get("_quoted_work_id") else ()
    if selector.basis in {"focus", "quote"} and not identifiers:
        clock = tx.conn.execute("SELECT clock_timestamp() AS observed_at").fetchone()
        assert clock is not None
        return {
            "status": "ambiguous",
            "complete": False,
            "facts": [],
            "observed_at": clock["observed_at"].isoformat(),
            "query_scope": {"kind": "lookup", "visibility": "public", "basis": selector.basis},
        }
    snapshot = facts.lookup_in_tx(
        tx,
        ids=identifiers,
        title=selector.title_query,
        states=tuple(state.value for state in selector.states),
        requester_id=tx.actor.member_id if selector.requester_scope == "self" else None,
        exclude_ids=selector.exclude_ids,
        through_sequence=context.get("_snapshot", {}).get("through_sequence"),
    )
    snapshot["query_scope"]["selector"] = selector.model_dump(mode="json")
    return snapshot


_ACTIVITIES = {
    "web_search": "检索公开资料",
    "web_fetch": "读取来源页面",
    "web_image_candidates": "查找图片来源",
    "web_image_import": "取得候选图片",
    "image_analyze": "核对图片内容",
    "image_convert": "转换图片格式",
    "audio_transcribe": "转写语音",
    "python_compute": "运行计算",
    "document_generate": "生成文档",
    "document_validate": "检查文档",
}


def _fact_text(fact: dict[str, Any]) -> str:
    title = fact["goal"].strip()
    title = title if len(title) <= 90 else title[:89] + "…"
    delivery = fact.get("delivery", {})
    phase = delivery.get("phase")
    if fact["state"] in {"cancelled", "failed"}:
        message = (
            "已取消后续执行。" if fact["state"] == "cancelled" else "执行失败，已有结果仍保留。"
        )
        if delivery.get("image_accepted", 0):
            message += f"此前已发出{delivery['image_accepted']}张图片。"
        elif delivery.get("accepted", 0):
            message += "此前已有部分结果发出。"
    elif phase == "uncertain":
        message = "发送回执尚未确认，正在核对。"
    elif phase in {"pending", "in_progress", "partially_accepted"}:
        message = (
            "结果正在发送。" if phase != "partially_accepted" else "部分结果已发出，其余仍在发送。"
        )
    elif phase == "accepted":
        image_count = delivery.get("image_accepted", 0)
        message = f"已发出{image_count}张图片。" if image_count else "结果已发出。"
        if fact["state"] == "completed":
            message = (
                f"已完成，{image_count}张图片已发出。" if image_count else "已完成，结果已发出。"
            )
    elif phase == "rejected":
        message = "这次发送未成功，执行结果仍保留。"
    elif fact["state"] == "completed":
        message = "记录显示已完成，目前没有可核对的发送回执。"
    else:
        message = fact.get("status_detail", {}).get("message", "工作状态暂时不可用。")
        if fact["state"] == "running":
            activity = fact.get("activity", {}).get("recent_tools", [])
            latest = next((item for item in activity if item.get("status") == "succeeded"), None)
            if latest and latest.get("tool_name") in _ACTIVITIES:
                message += "最近完成了" + _ACTIVITIES[latest["tool_name"]] + "。"
    return f"「{title}」：{message}"


def render_work_query(snapshot: dict[str, Any]) -> str:
    rows = snapshot.get("facts", [])
    if not rows:
        if snapshot.get("status") == "empty" and snapshot.get("complete"):
            return "我查了本群当前可见的工作记录，没有找到符合这些条件的工作。"
        if snapshot.get("status") == "ambiguous":
            return "你指哪项工作？可以告诉我名称或引用对应的消息。"
        return "目前还不能完整核对这项工作的记录，暂时无法确认它的状态。"
    answer = "\n".join(_fact_text(row) for row in rows)
    if not snapshot.get("complete"):
        answer += "\n这里只列出本次查到的部分记录。"
    return answer
