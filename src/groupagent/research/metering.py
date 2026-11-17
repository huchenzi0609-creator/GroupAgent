"""Account for a public read under either a work grant or an inbound request."""

from __future__ import annotations

import asyncio
import hashlib
from collections.abc import Awaitable, Callable
from decimal import Decimal
from typing import Any, TypeVar

from groupagent.domain import ActorContext, DomainError

from .models import FetchOutput, SearchOutput

_Read = TypeVar("_Read", SearchOutput, FetchOutput)


async def meter_public_read(
    ledger: Any,
    actor: ActorContext,
    *,
    key: str,
    operation: str,
    price_id: str,
    request: Callable[[], Awaitable[_Read]],
    work_id: str | None = None,
    source_event_id: str | None = None,
) -> _Read:
    units = {"calls": Decimal(1)}
    reservation = await asyncio.to_thread(
        ledger.reserve,
        actor,
        key,
        operation,
        price_id,
        units,
        work_id=work_id,
        source_event_id=source_event_id,
    )
    if not reservation.is_new or not await asyncio.to_thread(ledger.begin, actor, reservation.id):
        raise DomainError(
            "provider_reconciliation_required",
            "A public read already has a recorded provider attempt for this request",
        )
    try:
        result = await request()
        evidence = operation + ":" + hashlib.sha256(result.model_dump_json().encode()).hexdigest()
        settled = await asyncio.to_thread(ledger.settle, actor, reservation.id, units, evidence)
    except BaseException:
        await asyncio.to_thread(ledger.unknown, actor, reservation.id, "research_call_incomplete")
        raise
    if settled.overrun and settled.limits_enabled:
        raise DomainError("budget_overrun", "Research exceeded its monetary reservation")
    return result.model_copy(update={"budget_receipt": reservation.id})
