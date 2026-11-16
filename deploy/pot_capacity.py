"""Pure adaptive CPU policy and Linux host samples for the Pot Docker broker."""

from __future__ import annotations

import math
import os
import threading
import time
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class HostSample:
    cores: float
    busy: float | None
    available_mb: float = -1
    swap_mb_per_second: float = -1
    pressure: bool = False


class HostSampler:
    def __init__(self, root: Path = Path("/proc")):
        self.root = root
        self.previous: tuple[int, int, int, float] | None = None

    def sample(self, now: float) -> HostSample:
        cores = float(os.cpu_count() or 1)
        try:
            rows = (self.root / "stat").read_text().splitlines()
            counters = [int(value) for value in rows[0].split()[1:9]]
            if len(counters) < 8 or min(counters) < 0:
                raise ValueError("Invalid host CPU counters")
            cores = (
                float(sum(row.startswith("cpu") and row[3:4].isdigit() for row in rows)) or cores
            )
            total, idle = sum(counters), counters[3] + counters[4]
            memory = dict(
                line.split(":", 1) for line in (self.root / "meminfo").read_text().splitlines()
            )
            available = int(memory["MemAvailable"].split()[0]) / 1024
            swaps = 0
            vmstat = self.root / "vmstat"
            if vmstat.exists():
                values = dict(line.split() for line in vmstat.read_text().splitlines())
                swaps = int(values.get("pswpin", 0)) + int(values.get("pswpout", 0))
            psi = 0.0
            pressure_path = self.root / "pressure" / "memory"
            if pressure_path.exists():
                for row in pressure_path.read_text().splitlines():
                    if row.startswith("some "):
                        psi = float(dict(item.split("=", 1) for item in row.split()[1:])["avg10"])
            busy, swap_rate = None, 0.0
            if self.previous is not None:
                old_total, old_idle, old_swaps, old_time = self.previous
                elapsed, ticks = now - old_time, total - old_total
                if elapsed > 0 and ticks > 0 and idle >= old_idle and swaps >= old_swaps:
                    busy = cores * max(0.0, min(1.0, 1 - (idle - old_idle) / ticks))
                    swap_rate = (
                        (swaps - old_swaps) * os.sysconf("SC_PAGE_SIZE") / (1024**2 * elapsed)
                    )
            self.previous = total, idle, swaps, now
            if not all(math.isfinite(value) for value in (available, swap_rate, psi)):
                raise ValueError("Invalid host pressure counter")
            return HostSample(
                cores, busy, available, swap_rate, available < 192 or swap_rate > 4 or psi >= 10
            )
        except (OSError, ValueError, KeyError, IndexError):
            self.previous = None
            return HostSample(cores, None)


class CapacityControl:
    """Immediate decreases; smoothed, delayed increases; no self-CPU feedback."""

    def __init__(self, *, clock=time.monotonic):
        self.clock = clock
        self.lock = threading.RLock()
        self.limit = 0.5
        self.changed_at = clock()
        self.foreground_at: float | None = None
        self.pending = 0
        self.sample_at: float | None = None
        self.external_ewma: float | None = None
        self.host = HostSample(float(os.cpu_count() or 1), None)
        self.worker_busy = 0.0
        self.external_busy = 0.0
        self.stats_ok = False
        self.active_workers = 0
        self.update_ok = True
        self.applied_limit = 0.0

    def foreground(self, pending: int) -> None:
        if type(pending) is not int or not 0 <= pending <= 1000:
            raise ValueError("Foreground pending must be an integer from 0 to 1000")
        with self.lock:
            self.pending, self.foreground_at = pending, self.clock()
            if pending and self.limit > 0.5:
                self.limit, self.changed_at = 0.5, self.clock()

    def observe(self, host: HostSample, worker_busy: float | None, *, active_workers: int) -> float:
        with self.lock:
            now = self.clock()
            self.host, self.sample_at, self.active_workers = host, now, active_workers
            ceiling = min(1.5, max(0.25, host.cores - 0.35))
            valid = (
                host.busy is not None
                and worker_busy is not None
                and math.isfinite(host.busy)
                and math.isfinite(worker_busy)
                and 0 <= host.busy <= host.cores
                and 0 <= worker_busy <= host.cores
                and worker_busy <= host.busy + 0.25
            )
            self.stats_ok = valid
            if valid:
                assert host.busy is not None and worker_busy is not None
                self.worker_busy = worker_busy
                self.external_busy = max(0.0, host.busy - worker_busy)
                self.external_ewma = (
                    self.external_busy
                    if self.external_ewma is None
                    else 0.5 * self.external_busy + 0.5 * self.external_ewma
                )
                raw = max(0.25, min(ceiling, host.cores - self.external_busy - 0.35))
                smooth = max(0.25, min(ceiling, host.cores - self.external_ewma - 0.35))
                desired = min(raw, smooth) if raw < self.limit else smooth
            else:
                self.worker_busy = 0.0
                desired = min(self.limit, 0.5)
            fresh = self.foreground_at is not None and now - self.foreground_at <= 5
            if not fresh or self.pending:
                desired = min(desired, 0.5)
            if host.pressure:
                desired = 0.25
            if desired <= self.limit - 0.15 or (desired <= 0.5 < self.limit):
                self.limit, self.changed_at = desired, now
            elif valid and desired >= self.limit + 0.15 and now - self.changed_at >= 4:
                self.limit, self.changed_at = desired, now
            return self._effective(now)

    def _effective(self, now: float) -> float:
        result = self.limit
        if self.host.pressure:
            return 0.25
        if (
            self.sample_at is None
            or now - self.sample_at > 5
            or self.foreground_at is None
            or now - self.foreground_at > 5
            or self.pending
            or not self.stats_ok
        ):
            result = min(result, 0.5)
        return max(0.25, min(1.5, result))

    def plan(self) -> tuple[float, bool]:
        with self.lock:
            return self._effective(self.clock()), not self.host.pressure

    def snapshot(self) -> dict[str, int | float]:
        with self.lock:
            now = self.clock()
            return {
                "cpu_limit_cores": round(self._effective(now), 4),
                "applied_cpu_cores": round(self.applied_limit, 4),
                "total_cores": self.host.cores,
                "host_busy_cores": round(self.host.busy if self.host.busy is not None else -1, 4),
                "worker_busy_cores": round(self.worker_busy, 4),
                "external_busy_cores": round(self.external_busy, 4),
                "foreground_pending": self.pending,
                "foreground_age_seconds": round(now - self.foreground_at, 4)
                if self.foreground_at is not None
                else -1,
                "sample_age_seconds": round(now - self.sample_at, 4)
                if self.sample_at is not None
                else -1,
                "stats_ok": int(self.stats_ok),
                "update_ok": int(self.update_ok),
                "active_workers": self.active_workers,
                "memory_available_mb": round(self.host.available_mb, 4),
                "swap_mb_per_second": round(self.host.swap_mb_per_second, 4),
                "memory_pressure": int(self.host.pressure),
                "create_allowed": int(not self.host.pressure),
            }
