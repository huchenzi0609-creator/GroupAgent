"""User-facing explanations from durable state, without invented billing causes."""

from collections.abc import Mapping
from typing import Any

_REASONS = {
    "ambiguous_attachment": ("input", "同一条消息有多个附件版本，尚未确定应读取的版本。"),
    "attachment_pending": ("input", "附件还在下载，暂时没有取得可读取的内容。"),
    "attachment_unavailable": ("input", "目前无法取得所选附件，请重新发送需要处理的资料。"),
    "attachment_expired": ("input", "所选附件已过期，请重新发送。"),
    "attachment_retry_exhausted": ("input", "附件下载重试后仍未取得内容，请重新发送附件。"),
    "attachment_wait_timeout": ("input", "等待附件下载超时，请重新发送附件。"),
    "input_limit": ("input", "所选附件超过单次处理的数量、大小或尺寸限制，需要减少或分批处理。"),
    "source_unavailable": ("input", "原始资料已撤回、删除或不可用，不能继续引用。"),
    "resource_deferred": ("resources", "服务器当前余量不足，任务保留在队列中等待资源。"),
    "budget_exhausted": ("billing", "本次已授权的调用金额不足，暂时停止付费处理。"),
    "budget_not_configured": ("billing", "尚未配置本群的调用金额授权，不能开始付费处理。"),
    "budget_overrun": ("billing", "调用费用超过原预留金额，需要先核对账务。"),
    "provider_reconciliation_required": (
        "provider",
        "上次服务调用的结果尚未确认，需要先核对原调用，避免重复执行。",
    ),
    "delivery_uncertain": (
        "delivery",
        "平台尚未确认上次发送的结果，需要先核对回执，避免重复发送。",
    ),
    "effect_unknown": (
        "provider",
        "上一次工具调用的结果尚未确认，已保留进度并暂停自动重试，避免重复执行。",
    ),
    "auth_required": ("channel", "微信登录状态需要恢复，暂时无法继续使用通道。"),
    "user_request": ("control", "这项工作已按你的要求暂停。"),
    "group_paused": ("control", "本群的任务处理已暂停，进度仍然保留。"),
    "face_identification_unavailable": (
        "capability",
        "不能根据照片确认真人身份；可以描述画面或读取图中文字。",
    ),
}


def work_status_detail(work: Any) -> dict[str, str]:
    body = dict(work) if isinstance(work, Mapping) else work.model_dump(mode="json")
    preparation = body.get("scope", {}).get("_runtime_preparation", {})
    code = body.get("pause_reason") or preparation.get("error_code")
    if not isinstance(code, str):
        code = ""
    phase, message = _REASONS.get(
        code, ("execution", "这项工作尚未完成，执行记录已保留，暂时没有更具体的可靠原因。")
    )
    if code in {
        "model_round_limit",
        "tool_call_limit",
        "active_time_limit",
        "preparation_timeout",
        "round_budget_exhausted",
        "tool_budget_exhausted",
        "time_budget_exhausted",
    }:
        phase, message = "execution", "这项工作达到了本次执行的步骤或时长上限，已有进度会保留。"
    return {
        "code": code if code in _REASONS else "execution_incomplete",
        "phase": phase,
        "message": message,
    }
