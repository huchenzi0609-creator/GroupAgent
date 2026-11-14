from __future__ import annotations

from datetime import datetime
from decimal import ROUND_CEILING, Decimal
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

from groupagent.domain import DomainError

QUANTUM = Decimal("0.000001")


class BillingModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class PriceSnapshot(BillingModel):
    id: str = Field(min_length=1, max_length=200)
    provider: str = Field(min_length=1, max_length=100)
    operation: str = Field(min_length=1, max_length=100)
    currency: Literal["CNY", "USD"]
    rates: dict[str, Decimal] = Field(min_length=1, max_length=20)
    source_url: str = Field(min_length=1)
    captured_at: datetime
    conservative: bool = True
    test_only: bool = False

    @field_validator("rates")
    @classmethod
    def rates_valid(cls, values: dict[str, Decimal]) -> dict[str, Decimal]:
        if any(
            not name.strip() or not value.is_finite() or value < 0 for name, value in values.items()
        ):
            raise ValueError("Prices need named finite nonnegative rates")
        return values

    @field_validator("captured_at")
    @classmethod
    def aware(cls, value: datetime) -> datetime:
        if value.tzinfo is None:
            raise ValueError("Price timestamp requires a timezone")
        return value

    def cost(self, units: dict[str, Decimal]) -> Decimal:
        if not units or set(units) - self.rates.keys():
            raise DomainError("price_missing", "Usage units are not covered by the price snapshot")
        total = Decimal(0)
        for key, raw in units.items():
            if isinstance(raw, bool):
                raise DomainError("invalid_input", "Usage must be finite and nonnegative")
            value = Decimal(str(raw))
            if not value.is_finite() or value < 0:
                raise DomainError("invalid_input", "Usage must be finite and nonnegative")
            total += value * self.rates[key]
        return total.quantize(QUANTUM, rounding=ROUND_CEILING)


class Reservation(BillingModel):
    id: str
    group_id: str
    actor_id: str
    key: str
    operation: str
    price_id: str
    currency: Literal["CNY", "USD"]
    state: Literal["reserved", "inflight", "unknown", "settled", "released"]
    reserved_amount: Decimal
    actual_amount: Decimal | None = None
    estimated_units: dict[str, Decimal]
    actual_units: dict[str, Decimal] | None = None
    work_id: str | None = None
    source_event_id: str | None = None
    policy_revision: int
    created_at: datetime
    overrun: bool = False
    is_new: bool = False
