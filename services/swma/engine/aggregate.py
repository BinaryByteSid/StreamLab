"""Incremental (stateful) aggregation.

The aggregate never stores the events themselves, only running values:

    count <- count + 1
    sum   <- sum + latency
    min   <- min(min, latency)
    max   <- max(max, latency)
    avg    = sum / count         (computed when a result is emitted)
"""
from __future__ import annotations


class Aggregate:
    __slots__ = ("count", "sum", "min", "max", "errors", "client_errors",
                 "cpu_sum", "cpu_n", "mem_sum", "mem_n", "late", "first_ts", "last_ts")

    def __init__(self) -> None:
        self.count = 0
        self.sum = 0.0
        self.min: float | None = None
        self.max: float | None = None
        self.errors = 0
        self.client_errors = 0
        self.cpu_sum = 0.0
        self.cpu_n = 0
        self.mem_sum = 0.0
        self.mem_n = 0
        self.late = 0
        self.first_ts: int | None = None
        self.last_ts: int | None = None

    def add(self, latency: float, status: int, ts: int, cpu: float | None = None,
            mem: float | None = None, late: bool = False) -> None:
        self.count += 1
        self.sum += latency
        self.min = latency if self.min is None else min(self.min, latency)
        self.max = latency if self.max is None else max(self.max, latency)
        if status >= 500:
            self.errors += 1
        elif status >= 400:
            self.client_errors += 1
        if cpu is not None:
            self.cpu_sum += cpu
            self.cpu_n += 1
        if mem is not None:
            self.mem_sum += mem
            self.mem_n += 1
        if late:
            self.late += 1
        self.first_ts = ts if self.first_ts is None else min(self.first_ts, ts)
        self.last_ts = ts if self.last_ts is None else max(self.last_ts, ts)

    def result(self, window_seconds: float) -> dict:
        c = self.count
        return {
            "count": c,
            "sum": round(self.sum, 3),
            "avg": round(self.sum / c, 2) if c else None,
            "min": self.min,
            "max": self.max,
            "error_count": self.errors,
            "client_error_count": self.client_errors,
            "error_rate": round(100.0 * self.errors / c, 2) if c else 0.0,
            "throughput_eps": round(c / window_seconds, 3) if window_seconds else None,
            "cpu_avg": round(self.cpu_sum / self.cpu_n, 2) if self.cpu_n else None,
            "memory_avg": round(self.mem_sum / self.mem_n, 2) if self.mem_n else None,
            "late_events": self.late,
        }

    def to_dict(self) -> dict:
        return {k: getattr(self, k) for k in self.__slots__}

    @classmethod
    def from_dict(cls, d: dict) -> "Aggregate":
        a = cls()
        for k in cls.__slots__:
            if k in d:
                setattr(a, k, d[k])
        return a
