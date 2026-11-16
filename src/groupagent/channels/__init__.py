"""Explicitly configured channel adapters; no automatic credential discovery."""

from .agentwechat import AgentWechatAdapter
from .models import (
    AgentWechatConfig,
    AttachmentContent,
    AttachmentHandle,
    ChannelCapabilities,
    ChannelCursor,
    ChannelEvent,
    ChannelReceipt,
    EventBatch,
    MemberPage,
    MemberRecord,
    MemberRefreshReceipt,
    TransferReceipt,
)

__all__ = [
    "AgentWechatAdapter",
    "AgentWechatConfig",
    "AttachmentContent",
    "AttachmentHandle",
    "ChannelCapabilities",
    "ChannelCursor",
    "ChannelEvent",
    "ChannelReceipt",
    "EventBatch",
    "MemberPage",
    "MemberRecord",
    "MemberRefreshReceipt",
    "TransferReceipt",
]
