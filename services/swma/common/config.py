"""Control Plane: live configuration stored in Consul KV.

Every key lives under ``swma/config/``. Services watch the prefix with Consul
*blocking queries*, so a change made in the Admin UI (or with
``python -m swma.cli set window/tumbling_size_s 30``) reaches the running
processor within about a second -- no restart needed.
"""
from __future__ import annotations

import base64
import logging
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Callable

import httpx

from . import settings

PREFIX = "swma/config/"
log = logging.getLogger("swma.config")


@dataclass(frozen=True)
class Spec:
    key: str
    default: Any
    kind: str  # int | float | bool | enum
    group: str
    label: str
    help: str
    min: float | None = None
    max: float | None = None
    options: tuple[str, ...] = field(default_factory=tuple)


SPECS: list[Spec] = [
    # --- Windows -----------------------------------------------------------
    Spec("window/tumbling_size_s", 60, "int", "Windows", "Tumbling window size (seconds)",
         "Length of each non-overlapping window. 60 means one result per minute.", 10, 600),
    Spec("window/sliding_size_s", 60, "int", "Windows", "Sliding window size (seconds)",
         "Length of each overlapping window.", 10, 600),
    Spec("window/sliding_slide_s", 10, "int", "Windows", "Sliding window slide (seconds)",
         "How often a new sliding window starts. Size / slide = windows per event.", 5, 300),
    Spec("window/allowed_lateness_s", 10, "int", "Windows", "Allowed lateness (seconds)",
         "How long after the watermark passes a window's end late events may still update it.", 0, 300),
    # --- Watermark & triggers ---------------------------------------------
    Spec("watermark/max_out_of_orderness_s", 5, "int", "Watermark", "Max out-of-orderness (seconds)",
         "Watermark = newest event time seen minus this bound.", 0, 120),
    Spec("watermark/idle_timeout_s", 10, "int", "Watermark", "Idle timeout (seconds)",
         "If a partition gets no events for this long, its watermark follows the wall clock so windows still close.", 2, 300),
    Spec("trigger/emit_interval_s", 2, "int", "Watermark", "Periodic trigger interval (seconds)",
         "How often partial (still open) window results are emitted.", 1, 60),
    # --- State & checkpointing --------------------------------------------
    Spec("state/checkpoint_interval_s", 5, "int", "State", "Checkpoint interval (seconds)",
         "How often window state + Kafka offsets are saved to RocksDB.", 1, 120),
    Spec("state/s3_backup_interval_s", 30, "int", "State", "S3 backup interval (seconds)",
         "How often a full state snapshot is copied to S3 for disaster recovery.", 10, 600),
    Spec("state/redis_ttl_s", 3600, "int", "State", "Redis TTL for windows (seconds)",
         "Old window results expire from Redis after this long.", 60, 86400),
    # --- Feature flags -----------------------------------------------------
    Spec("features/schema_validation", True, "bool", "Feature flags", "Schema validation",
         "Validate every event against the JSON schema from the Schema Registry."),
    Spec("features/dedup_enabled", True, "bool", "Feature flags", "Duplicate detection",
         "Drop events whose event_id was already counted."),
    Spec("features/early_firing", True, "bool", "Feature flags", "Early (periodic) results",
         "Emit partial results for windows that are still open."),
    Spec("features/enrichment", True, "bool", "Feature flags", "Event enrichment",
         "Add team / tier from the service catalog to every event."),
    # --- Processor & scaling ----------------------------------------------
    Spec("processor/simulated_work_ms", 0, "int", "Processor", "Simulated work per event (ms)",
         "Artificially slow every event down. Raise it to create backpressure and consumer lag.", 0, 200),
    Spec("processor/sink_failure_pct", 0, "int", "Processor", "Transient sink failure (%)",
         "Make Redis writes fail randomly so you can watch the retry mechanism.", 0, 90),
    Spec("scaling/mode", "auto", "enum", "Scaling", "Scaling mode",
         "auto = the autoscaler adds/removes workers from consumer lag. manual = you choose.",
         options=("auto", "manual")),
    Spec("scaling/desired_workers", 2, "int", "Scaling", "Workers (manual mode)",
         "Number of processor workers in manual mode.", 1, 4),
    Spec("scaling/min_workers", 2, "int", "Scaling", "Min workers", "Autoscaler lower bound.", 1, 4),
    Spec("scaling/max_workers", 4, "int", "Scaling", "Max workers",
         "Autoscaler upper bound. More workers than partitions (4) would sit idle.", 1, 4),
    Spec("scaling/scale_up_lag", 400, "int", "Scaling", "Scale up when lag above",
         "Total consumer lag (events) that triggers adding a worker.", 10, 100000),
    Spec("scaling/scale_down_lag", 50, "int", "Scaling", "Scale down when lag below",
         "Total consumer lag that allows removing a worker (after the cooldown).", 0, 100000),
    Spec("scaling/cooldown_s", 30, "int", "Scaling", "Cooldown (seconds)",
         "Minimum time between two scaling actions.", 5, 600),
    Spec("scaling/restart_crashed", True, "bool", "Scaling", "Self-heal crashed workers",
         "Restart a crashed worker automatically."),
    Spec("scaling/restart_delay_s", 8, "int", "Scaling", "Restart delay (seconds)",
         "Wait before restarting a crashed worker (long enough to watch the rebalance).", 1, 120),
    # --- Producer (event sources) -----------------------------------------
    Spec("producer/enabled", True, "bool", "Event sources", "Producer running",
         "Turn the simulated microservice traffic on or off."),
    Spec("producer/rate_eps", 25, "int", "Event sources", "Event rate (events/second)",
         "Total events per second across all services.", 1, 5000),
    Spec("producer/scenario", "normal", "enum", "Event sources", "Traffic scenario",
         "normal, traffic_spike (4x rate), latency_degradation (payment slows down), "
         "error_storm (inventory returns 5xx), hot_partition (80% payment traffic).",
         options=("normal", "traffic_spike", "latency_degradation", "error_storm", "hot_partition")),
    Spec("chaos/out_of_order_pct", 5, "int", "Chaos", "Out-of-order events (%)",
         "Events whose timestamp is a few seconds in the past (inside the watermark bound).", 0, 100),
    Spec("chaos/late_pct", 0, "int", "Chaos", "Very late events (%)",
         "Events older than window + allowed lateness. They should go to the dead letter queue.", 0, 100),
    Spec("chaos/duplicate_pct", 0, "int", "Chaos", "Duplicate events (%)",
         "Re-send an already sent event (same event_id).", 0, 100),
    Spec("chaos/malformed_pct", 0, "int", "Chaos", "Malformed events (%)",
         "Broken JSON or missing fields. Schema validation should reject them.", 0, 100),
    # --- API & alerts ------------------------------------------------------
    Spec("api/rate_limit_rps", 5, "int", "API", "Rate limit (requests/second)",
         "Public /api/v1 requests allowed per API key per second.", 1, 1000),
    Spec("api/rate_limit_burst", 10, "int", "API", "Rate limit burst",
         "Short bursts allowed above the steady rate.", 1, 2000),
    Spec("alerts/latency_ms", 500, "int", "Alerts", "Alert: average latency above (ms)",
         "Fires when a service's window average latency is higher.", 50, 10000),
    Spec("alerts/error_rate_pct", 5, "int", "Alerts", "Alert: error rate above (%)",
         "Fires when a service's window error rate is higher.", 1, 100),
    Spec("alerts/consumer_lag", 1000, "int", "Alerts", "Alert: consumer lag above (events)",
         "Fires when the processors fall this far behind.", 10, 1000000),
]

SPEC_BY_KEY = {s.key: s for s in SPECS}
DEFAULTS = {s.key: s.default for s in SPECS}


def coerce(key: str, raw: Any) -> Any:
    """Convert a raw value (usually a string from Consul) to the spec's type."""
    spec = SPEC_BY_KEY.get(key)
    if spec is None:
        return raw
    if spec.kind == "bool":
        if isinstance(raw, bool):
            return raw
        return str(raw).strip().lower() in ("1", "true", "yes", "on")
    if spec.kind in ("int", "float"):
        num = float(raw)
        if spec.min is not None:
            num = max(spec.min, num)
        if spec.max is not None:
            num = min(spec.max, num)
        return int(num) if spec.kind == "int" else num
    if spec.kind == "enum":
        val = str(raw).strip()
        if val not in spec.options:
            raise ValueError(f"{key} must be one of {', '.join(spec.options)}")
        return val
    return raw


def encode(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


class LiveConfig:
    """Watches ``swma/config/`` in Consul and keeps an always-current dict."""

    def __init__(self, consul_url: str | None = None):
        self.url = (consul_url or settings.CONSUL_URL).rstrip("/")
        self.values: dict[str, Any] = dict(DEFAULTS)
        self.version = 0
        self.connected = False
        self._listeners: list[Callable[[dict[str, Any]], None]] = []
        self._lock = threading.Lock()
        self._thread: threading.Thread | None = None

    def get(self, key: str) -> Any:
        return self.values.get(key, DEFAULTS.get(key))

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            return dict(self.values)

    def on_change(self, fn: Callable[[dict[str, Any]], None]) -> None:
        self._listeners.append(fn)

    def start(self) -> "LiveConfig":
        try:  # one synchronous read so callers start with real values
            self._apply(*self._fetch(0, wait=None))
        except Exception as exc:  # Consul not up yet: keep defaults, the watcher retries
            log.warning("config: initial read failed (%s), using defaults", exc)
        self._thread = threading.Thread(target=self._watch, name="consul-watch", daemon=True)
        self._thread.start()
        return self

    def _fetch(self, index: int, wait: str | None = "25s"):
        params = {"recurse": "true"}
        if wait:
            params.update(index=str(index), wait=wait)
        r = httpx.get(f"{self.url}/v1/kv/{PREFIX}", params=params, timeout=35)
        new_index = int(r.headers.get("X-Consul-Index", "0") or 0)
        if r.status_code == 404:
            return [], new_index
        r.raise_for_status()
        return r.json(), new_index

    def _apply(self, items: list[dict], index: int) -> None:
        values = dict(DEFAULTS)
        for item in items:
            key = item["Key"][len(PREFIX):]
            if key not in SPEC_BY_KEY or item.get("Value") is None:
                continue
            try:
                values[key] = coerce(key, base64.b64decode(item["Value"]).decode())
            except (ValueError, TypeError):
                log.warning("config: ignoring invalid value for %s", key)
        changed = values != self.values
        with self._lock:
            self.values = values
            self.version = index
            self.connected = True
        if changed:
            for fn in self._listeners:
                try:
                    fn(values)
                except Exception:
                    log.exception("config listener failed")

    def _watch(self) -> None:
        index, backoff = self.version, 1.0
        while True:
            try:
                items, new_index = self._fetch(index)
                if new_index < index:  # Consul restarted: reset the index
                    new_index = 0
                if new_index != index:
                    self._apply(items, new_index)
                index, backoff = new_index, 1.0
            except Exception as exc:
                self.connected = False
                log.warning("config: watch error (%s), retrying in %.0fs", exc, backoff)
                time.sleep(backoff)
                backoff = min(backoff * 2, 15)


def put(key: str, value: Any, consul_url: str | None = None, cas: int | None = None) -> bool:
    url = (consul_url or settings.CONSUL_URL).rstrip("/")
    params = {"cas": str(cas)} if cas is not None else None
    r = httpx.put(f"{url}/v1/kv/{PREFIX}{key}", params=params, content=encode(value), timeout=5)
    r.raise_for_status()
    return r.json() is True


def seed_defaults(consul_url: str | None = None) -> int:
    """Create every missing key with its default (cas=0 never overwrites)."""
    created = 0
    for spec in SPECS:
        if put(spec.key, spec.default, consul_url, cas=0):
            created += 1
    return created
