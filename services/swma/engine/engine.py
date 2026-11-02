"""The per-partition stream processor: windows, watermark, triggers and state.

One ``PartitionEngine`` owns the state of one Kafka partition. Because the
producer keys every event by ``service``, all events of a service land on the
same partition, so a (service, window) state never needs data from another
partition.

Window life cycle (emission ``status``):

    open      watermark < end                 -> periodic partial results
    closed    end <= watermark                -> on-time result, late events still allowed
    updated   a late event changed a closed window
    final     end + allowed_lateness <= wm    -> result is frozen, state is dropped

An event whose windows are all final is *too late* and goes to the DLQ.
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Iterable

from .aggregate import Aggregate
from .watermark import Watermark
from .windows import sliding, tumbling, window_id


@dataclass
class EngineConfig:
    tumbling_ms: int = 60_000
    sliding_ms: int = 60_000
    slide_ms: int = 10_000
    lateness_ms: int = 10_000
    bound_ms: int = 5_000
    idle_ms: int = 10_000
    emit_interval_ms: int = 2_000
    dedup: bool = True
    early_firing: bool = True

    @classmethod
    def from_live(cls, c: dict) -> "EngineConfig":
        size = int(c["window/sliding_size_s"]) * 1000
        slide = min(int(c["window/sliding_slide_s"]) * 1000, size)
        return cls(
            tumbling_ms=int(c["window/tumbling_size_s"]) * 1000,
            sliding_ms=size,
            slide_ms=slide,
            lateness_ms=int(c["window/allowed_lateness_s"]) * 1000,
            bound_ms=int(c["watermark/max_out_of_orderness_s"]) * 1000,
            idle_ms=int(c["watermark/idle_timeout_s"]) * 1000,
            emit_interval_ms=int(c["trigger/emit_interval_s"]) * 1000,
            dedup=bool(c["features/dedup_enabled"]),
            early_firing=bool(c["features/early_firing"]),
        )


WindowKey = tuple  # (window_type, service, start_ms, end_ms)


class WindowState:
    __slots__ = ("wtype", "service", "start", "end", "agg", "phase", "dirty", "last_emit", "team", "tier")

    def __init__(self, wtype: str, service: str, start: int, end: int, phase: str = "open",
                 team: str | None = None, tier: str | None = None) -> None:
        self.wtype, self.service, self.start, self.end = wtype, service, start, end
        self.agg = Aggregate()
        self.phase = phase
        self.dirty = False
        self.last_emit = 0
        self.team, self.tier = team, tier

    def to_dict(self) -> dict:
        return {"t": self.wtype, "s": self.service, "b": self.start, "e": self.end,
                "p": self.phase, "a": self.agg.to_dict(), "team": self.team, "tier": self.tier}

    @classmethod
    def from_dict(cls, d: dict) -> "WindowState":
        w = cls(d["t"], d["s"], d["b"], d["e"], d.get("p", "open"), d.get("team"), d.get("tier"))
        w.agg = Aggregate.from_dict(d["a"])
        w.dirty = True  # re-emit after a restore so Redis is refreshed
        return w


@dataclass
class Outcome:
    kind: str  # on_time | out_of_order | late_accepted | too_late | duplicate
    windows: list
    dropped: list
    watermark: int | None


def _key_str(k: WindowKey) -> str:
    return "w|" + "|".join(str(x) for x in k)


def _iso(ms: int | None) -> str | None:
    if ms is None:
        return None
    from datetime import datetime, timezone
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


class PartitionEngine:
    def __init__(self, partition: int, cfg: EngineConfig | None = None) -> None:
        self.partition = partition
        self.cfg = cfg or EngineConfig()
        self.windows: dict[WindowKey, WindowState] = {}
        self.dedup: dict[str, int] = {}
        self.wm = Watermark(self.cfg.bound_ms, self.cfg.idle_ms)
        self.next_offset: int | None = None
        self.counters = {"on_time": 0, "out_of_order": 0, "late_accepted": 0, "too_late": 0, "duplicate": 0}
        self._changed: set[WindowKey] = set()
        self._removed: set[WindowKey] = set()
        self._dedup_added: set[str] = set()
        self._dedup_removed: set[str] = set()
        self._last_gc = 0

    # ------------------------------------------------------------------ config
    def update_config(self, cfg: EngineConfig) -> None:
        self.cfg = cfg
        self.wm.bound_ms = cfg.bound_ms
        self.wm.idle_timeout_ms = cfg.idle_ms

    # --------------------------------------------------------------- process
    def assign(self, ts: int) -> list[tuple[str, int, int]]:
        spans = [("tumbling", s, e) for s, e in tumbling(ts, self.cfg.tumbling_ms)]
        spans += [("sliding", s, e) for s, e in sliding(ts, self.cfg.sliding_ms, self.cfg.slide_ms)]
        return spans

    def process(self, event: dict, ts: int, wall_ms: int) -> Outcome:
        eid, service = event["event_id"], event["service"]
        if self.cfg.dedup and eid in self.dedup:
            self.counters["duplicate"] += 1
            return Outcome("duplicate", [], [], self.wm.current)

        wm = self.wm.value(wall_ms)
        prev_max = self.wm.max_event_ts
        lateness = self.cfg.lateness_ms
        assigned, dropped, any_late = [], [], False
        latency = float(event["response_time_ms"])
        status = int(event["status_code"])
        cpu, mem = event.get("cpu_percent"), event.get("memory_percent")

        for wtype, start, end in self.assign(ts):
            info = {"window_type": wtype, "window_id": window_id(start, end), "start": start, "end": end}
            if wm is not None and end + lateness <= wm:
                dropped.append(info)
                continue
            key = (wtype, service, start, end)
            w = self.windows.get(key)
            if w is None:
                phase = "closed" if (wm is not None and end <= wm) else "open"
                w = WindowState(wtype, service, start, end, phase, event.get("team"), event.get("tier"))
                self.windows[key] = w
            late = wm is not None and end <= wm
            w.agg.add(latency, status, ts, cpu, mem, late)
            w.dirty = True
            self._changed.add(key)
            any_late = any_late or late
            assigned.append({**info, "late": late})

        if not assigned:
            self.counters["too_late"] += 1
            return Outcome("too_late", [], dropped, wm)

        if self.cfg.dedup:
            self.dedup[eid] = ts
            self._dedup_added.add(eid)
        self.wm.observe(ts, wall_ms)

        if any_late:
            kind = "late_accepted"
        elif prev_max is not None and ts < prev_max:
            kind = "out_of_order"
        else:
            kind = "on_time"
        self.counters[kind] += 1
        return Outcome(kind, assigned, dropped, wm)

    # ---------------------------------------------------------------- trigger
    def tick(self, wall_ms: int) -> list[dict]:
        """Window Trigger & Emission. Call often (every ~0.5s)."""
        wm = self.wm.value(wall_ms)
        lateness, out = self.cfg.lateness_ms, []
        for key, w in list(self.windows.items()):
            if wm is not None and w.end + lateness <= wm:
                out.append(self._emission(w, "final", wm, wall_ms))
                del self.windows[key]
                self._changed.discard(key)
                self._removed.add(key)
            elif wm is not None and w.end <= wm and w.phase == "open":
                w.phase = "closed"
                out.append(self._emission(w, "closed", wm, wall_ms))
                w.dirty, w.last_emit = False, wall_ms
                self._changed.add(key)
            elif w.dirty and wall_ms - w.last_emit >= self.cfg.emit_interval_ms:
                if w.phase == "open" and not self.cfg.early_firing:
                    continue
                out.append(self._emission(w, "open" if w.phase == "open" else "updated", wm, wall_ms))
                w.dirty, w.last_emit = False, wall_ms
        if wm is not None and wall_ms - self._last_gc > 5_000:
            self._gc_dedup(wm)
            self._last_gc = wall_ms
        return out

    def _gc_dedup(self, wm: int) -> None:
        # A duplicate older than this would be "too late" anyway, so forget it.
        horizon = wm - max(self.cfg.tumbling_ms, self.cfg.sliding_ms) - self.cfg.lateness_ms
        for eid in [e for e, t in self.dedup.items() if t < horizon]:
            del self.dedup[eid]
            self._dedup_added.discard(eid)
            self._dedup_removed.add(eid)

    def _emission(self, w: WindowState, status: str, wm: int | None, wall_ms: int) -> dict:
        size_s = (w.end - w.start) / 1000
        return {
            "window_type": w.wtype,
            "window_id": window_id(w.start, w.end),
            "window_start": _iso(w.start),
            "window_end": _iso(w.end),
            "start_ms": w.start,
            "end_ms": w.end,
            "service": w.service,
            "team": w.team,
            "tier": w.tier,
            "status": status,
            "partition": self.partition,
            **w.agg.result(size_s),
            "watermark": _iso(wm),
            "emitted_at": _iso(wall_ms),
        }

    # ------------------------------------------------------------- inspection
    def describe(self, wall_ms: int) -> dict:
        wm = self.wm.value(wall_ms)
        phases = {"open": 0, "closed": 0}
        for w in self.windows.values():
            phases[w.phase] = phases.get(w.phase, 0) + 1
        return {
            "partition": self.partition,
            "watermark": _iso(wm),
            "watermark_ms": wm,
            "max_event_time": _iso(self.wm.max_event_ts),
            "watermark_delay_s": round((wall_ms - wm) / 1000, 1) if wm is not None else None,
            "idle": self.wm.is_idle(wall_ms),
            "open_windows": phases["open"],
            "closed_windows": phases["closed"],
            "dedup_keys": len(self.dedup),
            "next_offset": self.next_offset,
            "counters": dict(self.counters),
        }

    def active_windows(self, wall_ms: int, limit: int = 60) -> list[dict]:
        wm = self.wm.value(wall_ms)
        items = sorted(self.windows.values(), key=lambda w: (w.start, w.wtype, w.service))[-limit:]
        return [self._emission(w, w.phase, wm, wall_ms) for w in items]

    # ----------------------------------------------------- checkpoint / restore
    def drain_changes(self) -> tuple[dict[str, str], list[str]]:
        """Incremental checkpoint: only what changed since the last call."""
        puts: dict[str, str] = {}
        for key in self._changed:
            w = self.windows.get(key)
            if w is not None:
                puts[_key_str(key)] = json.dumps(w.to_dict())
        deletes = [_key_str(k) for k in self._removed]
        for eid in self._dedup_added:
            puts["d|" + eid] = str(self.dedup[eid])
        deletes += ["d|" + eid for eid in self._dedup_removed]
        puts["m|watermark"] = json.dumps(self.wm.to_dict())
        puts["m|counters"] = json.dumps(self.counters)
        if self.next_offset is not None:
            puts["m|offset"] = str(self.next_offset)
        self._changed.clear()
        self._removed.clear()
        self._dedup_added.clear()
        self._dedup_removed.clear()
        return puts, deletes

    def restore(self, items: Iterable[tuple[str, str]], wall_ms: int) -> int:
        """Rebuild state from checkpoint key/values. Returns windows restored."""
        n = 0
        for k, v in items:
            if k.startswith("w|"):
                w = WindowState.from_dict(json.loads(v))
                self.windows[(w.wtype, w.service, w.start, w.end)] = w
                n += 1
            elif k.startswith("d|"):
                self.dedup[k[2:]] = int(v)
            elif k == "m|watermark":
                self.wm.restore(json.loads(v), wall_ms)
            elif k == "m|offset":
                self.next_offset = int(v)
            elif k == "m|counters":
                self.counters.update(json.loads(v))
        return n

    def full_snapshot(self) -> dict:
        items = {_key_str(k): json.dumps(w.to_dict()) for k, w in self.windows.items()}
        items.update({"d|" + e: str(t) for e, t in self.dedup.items()})
        items["m|watermark"] = json.dumps(self.wm.to_dict())
        items["m|counters"] = json.dumps(self.counters)
        if self.next_offset is not None:
            items["m|offset"] = str(self.next_offset)
        return {"partition": self.partition, "offset": self.next_offset,
                "windows": len(self.windows), "dedup_keys": len(self.dedup), "items": items}
