"""Opt-in, host-aware admission for one transient isolated worker at a time.

The governor reserves a full child cgroup limit, plus host-side transfer space
and a free-memory floor. Existing neighbours are already reflected in Linux's
MemAvailable; their resident memory is not subtracted again. Admission is a
non-blocking decision: the durable runtime owns retry order and waiting jobs.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Iterator
from contextlib import asynccontextmanager, contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from pathlib import Path

from groupagent.domain import DomainError
from groupagent.foreground import ForegroundPublisher

_MIB = 1024 * 1024
_CURRENT_RESERVATION: ContextVar[ResourceReservation | None] = ContextVar(
    "groupagent_resource_reservation", default=None
)


class ResourceDeferred(DomainError):
    """Admission refused before the caller begins an effectful operation."""

    def __init__(self) -> None:
        super().__init__(
            "resource_deferred",
            "Isolated work is waiting for safe resource capacity",
            retryable=True,
            effect_state="not_started",
        )


@dataclass(frozen=True)
class ResourcePolicy:
    enabled: bool = False
    reserve_memory_mb: int = 256
    transfer_memory_mb: int = 128

    def __post_init__(self) -> None:
        if type(self.enabled) is not bool or any(
            type(value) is not int or value < 0
            for value in (self.reserve_memory_mb, self.transfer_memory_mb)
        ):
            raise ValueError("Resource reserves must be nonnegative integer MiB values")


@dataclass(frozen=True)
class MemorySnapshot:
    host_available_bytes: int | None
    cgroup_headroom_bytes: int | None = None
    readable: bool = True

    @property
    def available_bytes(self) -> int | None:
        if not self.readable or self.host_available_bytes is None:
            return None
        values = [self.host_available_bytes]
        if self.cgroup_headroom_bytes is not None:
            values.append(self.cgroup_headroom_bytes)
        return max(0, min(values))


def linux_memory_snapshot(
    *,
    meminfo: Path = Path("/proc/meminfo"),
    process_cgroup: Path = Path("/proc/self/cgroup"),
    cgroup_root: Path = Path("/sys/fs/cgroup"),
) -> MemorySnapshot:
    """Read MemAvailable and every visible finite cgroup ancestor limit.

    Both cgroup v2 and the conventional v1 memory mount are supported. Missing
    host information or malformed visible limits fail closed when enabled.
    This samples the controller's cgroup; Docker children still require their
    own --memory limits because a daemon may place them in sibling cgroups.
    """
    try:
        entries = dict(line.split(":", 1) for line in meminfo.read_text().splitlines())
        amount, unit = entries["MemAvailable"].split()
        if unit != "kB" or int(amount) < 0:
            raise ValueError("Invalid MemAvailable")
        host = int(amount) * 1024
        memberships = [line.split(":", 2) for line in process_cgroup.read_text().splitlines()]
        if any(len(entry) != 3 for entry in memberships):
            raise ValueError("Invalid cgroup membership")
        paths: list[tuple[Path, str, str]] = []
        for _, controllers, membership in memberships:
            if controllers == "":
                paths.append((cgroup_root, membership, "v2"))
            elif "memory" in controllers.split(","):
                paths.append((cgroup_root / "memory", membership, "v1"))
        bounds = []
        for mount, membership, version in paths:
            relative = Path(membership.lstrip("/"))
            if ".." in relative.parts:
                raise ValueError("Invalid cgroup path")
            leaf = mount / relative
            # In a cgroup namespace the visible mount can itself be the current
            # cgroup even when proc reports its host path. Its limits still apply.
            if not leaf.is_dir():
                leaf = mount
            for directory in (leaf, *leaf.parents):
                if not directory.is_relative_to(mount):
                    break
                limit_path = directory / (
                    "memory.max" if version == "v2" else "memory.limit_in_bytes"
                )
                current_path = directory / (
                    "memory.current" if version == "v2" else "memory.usage_in_bytes"
                )
                if not limit_path.exists():
                    continue
                raw = limit_path.read_text().strip()
                if raw == "max" and version == "v2":
                    continue
                limit = int(raw)
                current = int(current_path.read_text().strip())
                if min(limit, current) < 0:
                    raise ValueError("Invalid cgroup memory counter")
                # v1 uses a page-aligned near-LONG_MAX value for unlimited.
                if version == "v1" and limit >= 1 << 60:
                    continue
                bounds.append(max(0, limit - current))
        return MemorySnapshot(host, min(bounds) if bounds else None)
    except (OSError, ValueError, KeyError):
        return MemorySnapshot(None, readable=False)


class ResourceGovernor:
    """One shared instance must be injected into every worker and gateway."""

    def __init__(
        self,
        policy: ResourcePolicy | None = None,
        *,
        sampler: Callable[[], MemorySnapshot] = linux_memory_snapshot,
        foreground_socket: Path | None = None,
    ) -> None:
        self.policy = policy or ResourcePolicy()
        self._sampler = sampler
        self._active: ResourceReservation | None = None
        self._poisoned = False
        self._foreground = ForegroundPublisher(foreground_socket) if foreground_socket else None

    def foreground_pending(self, pending: int) -> None:
        if self._foreground is not None:
            self._foreground.report(pending)

    async def aclose(self) -> None:
        if self._foreground is not None:
            await self._foreground.aclose()

    @property
    def busy(self) -> bool:
        return self._active is not None

    @property
    def poisoned(self) -> bool:
        return self._poisoned

    def poison(self) -> None:
        """Block admission after an unconfirmed worker/inventory operation."""
        self._poisoned = True

    def ready(self, memory_mb: int) -> bool:
        if type(memory_mb) is not int or memory_mb <= 0:
            raise ValueError("The full worker memory limit must be positive integer MiB")
        if not self.policy.enabled:
            return True
        if self.busy or self.poisoned:
            return False
        if self._foreground is not None and self._foreground.memory_pressure:
            return False
        try:
            available = self._sampler().available_bytes
        except (OSError, ValueError):
            return False
        required = (
            memory_mb + self.policy.reserve_memory_mb + self.policy.transfer_memory_mb
        ) * _MIB
        return available is not None and available >= required

    def reserve(self, memory_mb: int, *, owner: str = "") -> ResourceReservation:
        # No await between the decision and reservation: concurrent asyncio
        # callers cannot both observe a free lane. No waiter/task backlog lives
        # here; the runtime keeps the durable FIFO queue and rechecks later.
        if not self.ready(memory_mb):
            raise ResourceDeferred()
        reservation = ResourceReservation(self, memory_mb, owner)
        if self.policy.enabled:
            self._active = reservation
        return reservation

    def current(self, memory_mb: int) -> ResourceReservation | None:
        reservation = _CURRENT_RESERVATION.get()
        if reservation is None or reservation.governor is not self:
            return None
        if reservation.released or reservation.memory_mb < memory_mb:
            raise DomainError("resource_contract_invalid", "Worker exceeds its admitted resources")
        return reservation


class ResourceReservation:
    def __init__(self, governor: ResourceGovernor, memory_mb: int, owner: str) -> None:
        self.governor, self.memory_mb, self.owner = governor, memory_mb, owner
        self.released = False
        self._children: set[str] = set()
        # ContextVars propagate to gather/create_task children. Sharing the outer
        # reservation must never allow two actual containers to run together.
        self._child_lane = asyncio.Lock()

    @contextmanager
    def activate(self) -> Iterator[None]:
        if self.released:
            raise DomainError("resource_contract_invalid", "Resource reservation has ended")
        token = _CURRENT_RESERVATION.set(self)
        try:
            yield
        finally:
            _CURRENT_RESERVATION.reset(token)

    @asynccontextmanager
    async def child_slot(self, cancelled: asyncio.Event, deadline: float | None = None):
        acquisition = asyncio.create_task(self._child_lane.acquire())
        cancellation = asyncio.create_task(cancelled.wait())
        acquired = False
        try:
            timeout = (
                None if deadline is None else max(0, deadline - asyncio.get_running_loop().time())
            )
            done, _ = await asyncio.wait(
                {acquisition, cancellation}, timeout=timeout, return_when=asyncio.FIRST_COMPLETED
            )
            if cancellation in done or cancelled.is_set():
                raise DomainError("cancelled", "Worker admission was cancelled")
            if acquisition not in done:
                raise DomainError("time_budget_exhausted", "Worker deadline elapsed before launch")
            acquired = acquisition.result()
            if self.released or self.governor.poisoned:
                raise ResourceDeferred()
            yield
        finally:
            acquisition.cancel()
            cancellation.cancel()
            await asyncio.gather(acquisition, cancellation, return_exceptions=True)
            if acquired or (
                not acquisition.cancelled() and acquisition.done() and acquisition.result()
            ):
                self._child_lane.release()

    def container_started(self, name: str) -> None:
        if self.released or not self._child_lane.locked() or self._children:
            raise DomainError(
                "resource_contract_invalid", "Container lane is not exclusively owned"
            )
        self._children.add(name)

    def container_removed(self, name: str) -> None:
        self._children.discard(name)

    def poison(self) -> None:
        self.governor.poison()

    def release(self) -> None:
        if self.released:
            return
        if self._children or self._child_lane.locked():
            self.poison()
        self.released = True
        if self.governor._active is self:
            self.governor._active = None
