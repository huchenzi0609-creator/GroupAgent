from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Protocol

from groupagent.domain import ActorContext, ExecutionLease, WorkOrder
from groupagent.execution import EnginePort, ExecutionRequest, RunSnapshot


@dataclass(frozen=True)
class ProcessLease:
    group_id: str
    name: str
    owner_id: str
    fencing_token: int
    expires_at: datetime


@dataclass(frozen=True)
class DeliveryDraft:
    payload_ref: str
    payload_hash: str
    idempotency_key: str


@dataclass(frozen=True)
class ValidatedCandidate:
    # Each key must match the WorkOrder's required_checks. Values are persisted
    # validation evidence references, not model claims or unchecked paths.
    checks: dict[str, str]
    deliveries: tuple[DeliveryDraft, ...]


@dataclass(frozen=True)
class PreparedExecution:
    engine: EnginePort
    request: ExecutionRequest


ActorResolver = Callable[[str], ActorContext]
ExecutionFactory = Callable[
    [WorkOrder, ExecutionLease, ActorContext, Path], Awaitable[PreparedExecution]
]


class CandidateValidator(Protocol):
    async def validate(
        self,
        work: WorkOrder,
        lease: ExecutionLease,
        actor: ActorContext,
        snapshot: RunSnapshot,
        workspace: Path,
    ) -> ValidatedCandidate: ...
