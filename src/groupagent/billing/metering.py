"""Currency admission around a provider model, independent of its token limits."""

from __future__ import annotations

import asyncio
import hashlib
from decimal import Decimal
from uuid import uuid4

from groupagent.domain import ActorContext, DomainError
from groupagent.execution.models import ModelReply, ModelRequest
from groupagent.execution.ports import ModelPort, estimate_input_tokens

from .ledger import BudgetLedger


class MeteredModel:
    def __init__(
        self,
        model: ModelPort,
        ledger: BudgetLedger,
        actor: ActorContext,
        *,
        price_id: str,
        work_id: str | None = None,
        source_event_id: str | None = None,
        operation: str = "text",
        read_only_authorized: bool = False,
    ) -> None:
        self.model, self.ledger, self.actor = model, ledger, actor
        self.price_id, self.work_id, self.source_event_id = price_id, work_id, source_event_id
        self.operation, self.read_only_authorized = operation, read_only_authorized
        self.name = "metered:" + model.name

    async def complete(self, request: ModelRequest) -> ModelReply:
        # One provider attempt has one reservation. Engine checkpoints persist an
        # in-flight call before invoking us; uncertain attempts are not replayed.
        reservation = self.ledger.reserve(
            self.actor,
            "model:" + uuid4().hex,
            self.operation,
            self.price_id,
            {
                "input_tokens": Decimal(estimate_input_tokens(request)),
                "output_tokens": Decimal(request.max_output_tokens),
            },
            work_id=self.work_id,
            source_event_id=self.source_event_id,
            read_only_authorized=self.read_only_authorized,
        )
        if not reservation.is_new or not self.ledger.begin(self.actor, reservation.id):
            raise DomainError(
                "reconciliation_required", "Provider call already has a billing attempt"
            )
        try:
            reply = await self.model.complete(request)
            settled = self.ledger.settle(
                self.actor,
                reservation.id,
                {
                    "input_tokens": Decimal(reply.usage.input_tokens),
                    "output_tokens": Decimal(reply.usage.output_tokens),
                },
                "usage:" + hashlib.sha256(reply.model_dump_json().encode()).hexdigest(),
            )
            if settled.overrun:
                raise DomainError("budget_exhausted", "Provider usage exceeded the admitted amount")
            return reply
        except (Exception, asyncio.CancelledError):
            self.ledger.unknown(self.actor, reservation.id)
            raise


class LedgerModelBudget:
    """Engine budget port: each attempted model round has one durable charge key."""

    def __init__(self, ledger: BudgetLedger, *, price_id: str = "deepseek-20261114-text") -> None:
        self.ledger, self.price_id = ledger, price_id
        self._actors: dict[str, ActorContext] = {}

    async def reserve(
        self, request, *, round_number: int, input_tokens: int, output_tokens: int
    ) -> str:
        reservation = self.ledger.reserve(
            request.actor,
            f"engine:{request.work_id}:{request.attempt_id}:{round_number}",
            "text",
            self.price_id,
            {"input_tokens": Decimal(input_tokens), "output_tokens": Decimal(output_tokens)},
            work_id=request.work_id,
        )
        if not reservation.is_new or not self.ledger.begin(request.actor, reservation.id):
            raise DomainError(
                "reconciliation_required", "Model attempt already exists in the monetary ledger"
            )
        self._actors[reservation.id] = request.actor
        return reservation.id

    async def settle(self, reservation_id: str, usage) -> None:
        actor = self._actors.get(reservation_id)
        if actor is None:
            raise DomainError(
                "reconciliation_required", "Accounting attempt needs persisted reconciliation"
            )
        if usage is None:
            self.ledger.unknown(actor, reservation_id)
            return
        result = self.ledger.settle(
            actor,
            reservation_id,
            {
                "input_tokens": Decimal(usage.input_tokens),
                "output_tokens": Decimal(usage.output_tokens),
            },
            "provider-usage:" + reservation_id,
        )
        if result.overrun:
            raise DomainError("budget_exhausted", "Provider usage exceeded its admission")
