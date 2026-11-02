"""Watermark generation (bounded out-of-orderness, per partition).

    watermark = (largest event time seen) - max_out_of_orderness

The watermark only moves forward. It means "I believe every event older than
this has arrived". When a partition goes quiet for ``idle_timeout`` the
watermark follows the wall clock instead, otherwise the last windows would
never close once the traffic stops.
"""
from __future__ import annotations


class Watermark:
    def __init__(self, bound_ms: int, idle_timeout_ms: int) -> None:
        self.bound_ms = bound_ms
        self.idle_timeout_ms = idle_timeout_ms
        self.max_event_ts: int | None = None
        self.last_seen_wall: int | None = None
        self.current: int | None = None

    def observe(self, ts_ms: int, wall_ms: int) -> None:
        # A clock-skewed event from the future must not drag the watermark ahead.
        ts_ms = min(ts_ms, wall_ms)
        if self.max_event_ts is None or ts_ms > self.max_event_ts:
            self.max_event_ts = ts_ms
        self.last_seen_wall = wall_ms

    def value(self, wall_ms: int) -> int | None:
        candidate = None if self.max_event_ts is None else self.max_event_ts - self.bound_ms
        if self.last_seen_wall is not None and wall_ms - self.last_seen_wall > self.idle_timeout_ms:
            idle = wall_ms - self.idle_timeout_ms - self.bound_ms
            candidate = idle if candidate is None else max(candidate, idle)
        if candidate is not None and (self.current is None or candidate > self.current):
            self.current = candidate
        return self.current

    def is_idle(self, wall_ms: int) -> bool:
        return self.last_seen_wall is not None and wall_ms - self.last_seen_wall > self.idle_timeout_ms

    def to_dict(self) -> dict:
        return {"max_event_ts": self.max_event_ts, "current": self.current}

    def restore(self, d: dict, wall_ms: int) -> None:
        self.max_event_ts = d.get("max_event_ts")
        self.current = d.get("current")
        # Start the idle clock now; we don't know when the last event arrived.
        self.last_seen_wall = wall_ms
