"""Variant preference for copies of one already scoped message attachment."""

from __future__ import annotations

from collections.abc import Iterable

from .models import AttachmentHandle

# AgentWechat v2.2.1, 811d090770dd7b682b5bf69d54fc6fbd522cbe9b,
# wechat-core/src/media.rs::select_asset (reviewed 2026-11-16):
# https://github.com/thisnick/agent-wechat/blob/811d090770dd7b682b5bf69d54fc6fbd522cbe9b/packages/native-client-rust/crates/wechat-core/src/media.rs
_VARIANT_PRIORITY = {
    "original": 5,
    "normal": 4,
    "standard": 3,
    "thumbnail": 2,
    "encoded": 1,
    "best": 0,
}


def preferred_attachment_variants(
    handles: Iterable[AttachmentHandle], variant: str
) -> tuple[AttachmentHandle, ...]:
    """Narrow copies without hiding ties or relaxing a concrete variant request.

    Callers must establish message/asset scope and availability first. A tie stays
    a tie: only the caller can reject it as ambiguous rather than use list order.
    """
    candidates = tuple(handles)
    if variant != "best":
        return tuple(handle for handle in candidates if handle.variant == variant)
    priority = max((_VARIANT_PRIORITY.get(handle.variant, -1) for handle in candidates), default=-1)
    return tuple(
        handle for handle in candidates if _VARIANT_PRIORITY.get(handle.variant, -1) == priority
    )
