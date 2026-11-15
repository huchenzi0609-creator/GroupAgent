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
        additional_retry_limit: int = 0,
    ) -> None:
        if type(additional_retry_limit) is not int or not 0 <= additional_retry_limit <= 5:
            raise ValueError("Additional model retries must be an integer from zero to five")
        self.model, self.ledger, self.actor = model, ledger, actor
        self.price_id, self.work_id, self.source_event_id = price_id, work_id, source_event_id
        self.operation, self.read_only_authorized = operation, read_only_authorized
        self.additional_retry_limit = additional_retry_limit
        self.name = "metered:" + model.name

    async def complete(self, request: ModelRequest) -> ModelReply:
        for attempt in range(self.additional_retry_limit + 1):
            # Admission is outside the inference retry boundary. Each new pure
            # inference gets a distinct charge; unknown earlier charges persist.
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
                # Await completion/unwinding of this call before another one can
                # start. This boundary never dispatches model-proposed tools.
                reply = await self.model.complete(request)
            except (Exception, asyncio.CancelledError) as error:
                self.ledger.unknown(self.actor, reservation.id)
                if isinstance(error, DomainError) and error.code in {
                    "provider_unavailable",
                    "invalid_model_response",
                }:
                    if attempt < self.additional_retry_limit:
                        await asyncio.sleep(min(2**attempt, 2))
                        continue
                    if self.additional_retry_limit and error.retryable:
                        # Do not multiply the exhausted transport allowance in
                        # a caller's separate schema-repair/retry loop.
                        raise DomainError(
                            error.code,
                            error.message,
                            retryable=False,
                            effect_state=error.effect_state,
                            trace_id=error.trace_id,
                            resource_ref=error.resource_ref,
                        ) from error
                raise
            try:
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
                    raise DomainError(
                        "budget_exhausted", "Provider usage exceeded the admitted amount"
                    )
                return reply
            except (Exception, asyncio.CancelledError):
                # Settlement failures must never cause another provider call,
                # even if an accounting adapter uses a model-like error code.
                self.ledger.unknown(self.actor, reservation.id)
                raise
        raise AssertionError("Bounded metered model retry loop ended unexpectedly")


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
