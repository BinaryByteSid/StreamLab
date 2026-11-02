"""Processor supervisor: runs 1-4 worker processes, heals crashes, autoscales.

    * Horizontal scaling  -- each worker is a separate consumer-group member,
                             Kafka spreads the 4 partitions across them.
    * Self-healing        -- a crashed worker is restarted after a delay.
    * Auto scaling        -- adds a worker when lag is high, removes one when
                             lag stays low (with a cooldown), inside min/max.
    * Control endpoint    -- :8090, used by the lab API (crash / restart / disaster).
    * Metrics             -- :9100, combined metrics of all worker processes.
"""
from __future__ import annotations

import glob
import json
import multiprocessing
import os
import signal
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import redis
from confluent_kafka import Consumer, KafkaException, TopicPartition
from prometheus_client import CollectorRegistry, multiprocess, start_http_server

from swma.common import labbus, log as logsetup, settings
from swma.common.config import LiveConfig
from swma.common.schema import iso, now_ms

from . import metrics as M
from .statestore import wipe_all
from .worker import run as run_worker


@dataclass
class Slot:
    id: str
    index: int
    proc: multiprocessing.Process | None = None
    started_at: float | None = None
    restarts: int = 0
    crashes: int = 0
    stopping: bool = False
    expect_exit: bool = False
    restart_at: float | None = None
    held: bool = False  # crashed and self-healing is off
    restart_reason: str = "crash-recovery"
    last_exit: str | None = None
    history: list = field(default_factory=list)


class Supervisor:
    def __init__(self) -> None:
        settings.INSTANCE = "processor-supervisor"
        self.log = logsetup.setup("processor")
        self.cfg = LiveConfig().start()
        self.r = redis.Redis.from_url(settings.REDIS_URL, decode_responses=True, socket_timeout=3)
        self.ctx = multiprocessing.get_context("spawn")
        self.lock = threading.RLock()
        self.slots = [Slot(f"processor-{i}", i) for i in range(1, settings.MAX_WORKERS + 1)]
        c = self.cfg.snapshot()
        start = c["scaling/desired_workers"] if c["scaling/mode"] == "manual" else max(
            c["scaling/min_workers"], min(c["scaling/desired_workers"], c["scaling/max_workers"]))
        self.target = int(start)
        self.last_scale = time.time()
        self.low_since: float | None = None
        self.decisions: deque = deque(maxlen=50)
        self.lag = {"committed": {}, "live": {}, "committed_total": 0, "live_total": 0}
        self.stop = False
        self.lag_consumer = self._lag_consumer()

    @staticmethod
    def _lag_consumer() -> Consumer:
        return Consumer({
            "bootstrap.servers": settings.KAFKA_BOOTSTRAP,
            "group.id": settings.CONSUMER_GROUP,
            "client.id": "lag-monitor",
            "enable.auto.commit": False,
            "topic.metadata.refresh.interval.ms": 5000,
        })

    # ------------------------------------------------------------- workers
    def start_worker(self, slot: Slot, reason: str) -> None:
        proc = self.ctx.Process(target=run_worker, args=(slot.id,), name=slot.id, daemon=True)
        proc.start()
        slot.proc, slot.started_at = proc, time.time()
        slot.stopping = slot.expect_exit = False
        slot.restart_at = None
        if reason != "initial":
            slot.restarts += 1
            M.WORKER_RESTARTS.labels(slot.id, reason).inc()
        self.log.info("started %s (pid %s, reason=%s)", slot.id, proc.pid, reason)

    def reap(self, slot: Slot) -> None:
        """Handle a worker process that exited."""
        proc = slot.proc
        if proc is None or proc.is_alive():
            return
        code = proc.exitcode
        multiprocess.mark_process_dead(proc.pid)
        slot.proc = None
        if slot.expect_exit:
            slot.last_exit = f"stopped gracefully (exit {code})"
            return
        slot.crashes += 1
        slot.last_exit = f"crashed (exit code {code})"
        auto = self.cfg.get("scaling/restart_crashed")
        delay = self.cfg.get("scaling/restart_delay_s")
        if auto:
            slot.restart_at = time.time() + delay
            slot.restart_reason = "crash-recovery"
            msg = (f"Supervisor detected that {slot.id} crashed (exit {code}). Kafka will move its partitions "
                   f"to the surviving workers after the session timeout (~6s); self-healing restarts it in {delay}s.")
        else:
            slot.held = True
            msg = f"{slot.id} crashed (exit {code}). Self-healing is OFF, so it stays down."
        labbus.emit(self.r, "crash", msg, worker=slot.id, exit_code=code)
        self.log.warning(msg)

    def reconcile(self) -> None:
        with self.lock:
            now = time.time()
            for slot in self.slots:
                self.reap(slot)
                wanted = slot.index <= self.target
                alive = slot.proc is not None and slot.proc.is_alive()
                if wanted and not alive and not slot.held:
                    if slot.restart_at is None or now >= slot.restart_at:
                        self.start_worker(slot, slot.restart_reason if slot.restart_at else
                                          ("initial" if slot.started_at is None else "scale-up"))
                elif not wanted and alive and not slot.stopping:
                    self.graceful_stop(slot)
                elif not wanted:
                    slot.held = False
                    slot.restart_at = None
            alive = sum(1 for s in self.slots if s.proc is not None and s.proc.is_alive())
            M.WORKERS_ALIVE.set(alive)
            M.WORKERS_DESIRED.set(self.target)
            for s in self.slots:
                M.WORKER_UP.labels(s.id).set(1 if s.proc is not None and s.proc.is_alive() else 0)

    def graceful_stop(self, slot: Slot) -> None:
        if slot.proc is not None and slot.proc.is_alive():
            slot.stopping = slot.expect_exit = True
            os.kill(slot.proc.pid, signal.SIGTERM)

    # ------------------------------------------------------------- control
    def crash(self, worker_id: str) -> dict:
        with self.lock:
            slot = self._slot(worker_id)
            if slot.proc is None or not slot.proc.is_alive():
                return {"ok": False, "error": f"{worker_id} is not running"}
            pid = slot.proc.pid
            os.kill(pid, signal.SIGKILL)  # no checkpoint, no goodbye: a real crash
            return {"ok": True, "worker": worker_id, "pid": pid}

    def restart(self, worker_id: str) -> dict:
        with self.lock:
            slot = self._slot(worker_id)
            slot.held = False
            if slot.proc is not None and slot.proc.is_alive():
                self.graceful_stop(slot)
                slot.restart_at = time.time() + 1
                slot.restart_reason = "restart"
                slot.expect_exit = True
            labbus.emit(self.r, "restart", f"Graceful restart of {worker_id} requested", worker=worker_id)
            return {"ok": True}

    def disaster(self) -> dict:
        """Simulate losing the local disk: kill every worker, delete RocksDB."""
        with self.lock:
            pids = []
            for slot in self.slots:
                if slot.proc is not None and slot.proc.is_alive():
                    pids.append(slot.proc.pid)
                    os.kill(slot.proc.pid, signal.SIGKILL)
                    slot.expect_exit = True
            for slot in self.slots:
                if slot.proc is not None:
                    slot.proc.join(5)
                    self.reap(slot)
                slot.restart_at = time.time() + 3
                slot.restart_reason = "disaster-recovery"
            wiped = wipe_all(settings.STATE_DIR)
        labbus.emit(self.r, "disaster", f"Disk failure simulated: killed {len(pids)} workers and deleted "
                    f"{wiped} local RocksDB stores. Workers restart in 3s and must restore from the S3 backups.")
        return {"ok": True, "killed": len(pids), "wiped_partitions": wiped}

    def _slot(self, worker_id: str) -> Slot:
        for s in self.slots:
            if s.id == worker_id:
                return s
        raise KeyError(worker_id)

    def describe(self) -> dict:
        with self.lock:
            workers = []
            for s in self.slots:
                alive = s.proc is not None and s.proc.is_alive()
                state = "running" if alive else ("restarting" if s.restart_at else
                                                 ("crashed" if s.held else
                                                  ("stopped" if s.index > self.target else "starting")))
                if alive and s.stopping:
                    state = "stopping"
                workers.append({
                    "worker": s.id, "state": state, "pid": s.proc.pid if alive else None,
                    "desired": s.index <= self.target, "restarts": s.restarts, "crashes": s.crashes,
                    "started_at": iso(int(s.started_at * 1000)) if s.started_at and alive else None,
                    "restart_in_s": round(s.restart_at - time.time(), 1) if s.restart_at else None,
                    "last_exit": s.last_exit,
                })
            return {
                "target": self.target, "mode": self.cfg.get("scaling/mode"), "workers": workers,
                "lag": self.lag, "decisions": list(self.decisions)[-15:],
                "policy": {k.split("/")[1]: self.cfg.get(k) for k in (
                    "scaling/min_workers", "scaling/max_workers", "scaling/scale_up_lag",
                    "scaling/scale_down_lag", "scaling/cooldown_s")},
                "cooldown_remaining_s": max(0, round(self.cfg.get("scaling/cooldown_s") - (time.time() - self.last_scale), 1)),
            }

    # ----------------------------------------------------------- autoscale
    def measure_lag(self) -> None:
        tps = [TopicPartition(settings.TOPIC_EVENTS, p) for p in range(settings.EVENT_PARTITIONS)]
        committed, live = {}, {}
        try:
            for tp in self.lag_consumer.committed(tps, timeout=5):
                lo, hi = self.lag_consumer.get_watermark_offsets(tp, timeout=2, cached=False)
                pos = tp.offset if tp.offset >= 0 else lo
                committed[tp.partition] = max(0, hi - pos)
        except KafkaException as exc:
            # Usually stale metadata after a leader moved (e.g. a broker restarted): reconnect.
            self.log.warning("lag check failed, reconnecting the lag monitor: %s", exc)
            try:
                self.lag_consumer.close()
            except Exception:
                pass
            self.lag_consumer = self._lag_consumer()
            return
        # Live lag reported by the owning worker (fresher than committed offsets).
        for key in self.r.scan_iter("lab:worker:*"):
            raw = self.r.get(key)
            if raw:
                for part in json.loads(raw).get("partitions", []):
                    live[part["partition"]] = part["lag"]
        for p, v in committed.items():
            live.setdefault(p, v)  # nobody owns it right now -> use the committed view
            M.GROUP_LAG.labels(str(p)).set(v)
        self.lag = {"committed": committed, "live": live,
                    "committed_total": sum(committed.values()), "live_total": sum(live.values())}
        M.GROUP_LAG_TOTAL.set(self.lag["committed_total"])
        M.LIVE_LAG_TOTAL.set(self.lag["live_total"])

    def autoscale(self) -> None:
        c = self.cfg.snapshot()
        lo, hi = c["scaling/min_workers"], max(c["scaling/min_workers"], c["scaling/max_workers"])
        if c["scaling/mode"] == "manual":
            new = max(1, min(settings.MAX_WORKERS, c["scaling/desired_workers"]))
            if new != self.target:
                self.record("manual", self.target, new, "desired workers changed in the Control Plane")
                self.target = new
            return
        lag, now = self.lag["live_total"], time.time()
        cooled = now - self.last_scale >= c["scaling/cooldown_s"]
        if self.target < lo or self.target > hi:
            new = max(lo, min(hi, self.target))
            self.record("policy", self.target, new, f"keeping workers within min={lo} / max={hi}")
            self.target = new
            return
        if lag > c["scaling/scale_up_lag"] and self.target < hi and cooled:
            self.record("up", self.target, self.target + 1,
                        f"lag {lag} > scale-up threshold {c['scaling/scale_up_lag']}")
            self.target += 1
        if lag < c["scaling/scale_down_lag"]:
            self.low_since = self.low_since or now
            if self.target > lo and cooled and now - self.low_since >= c["scaling/cooldown_s"]:
                self.record("down", self.target, self.target - 1,
                            f"lag {lag} < scale-down threshold {c['scaling/scale_down_lag']} "
                            f"for {c['scaling/cooldown_s']}s")
                self.target -= 1
                self.low_since = now
        else:
            self.low_since = None

    def record(self, direction: str, old: int, new: int, why: str) -> None:
        self.last_scale = time.time()
        item = {"at": iso(now_ms()), "direction": direction, "from": old, "to": new, "reason": why}
        self.decisions.append(item)
        M.SCALE_ACTIONS.labels(direction).inc()
        verb = {"up": "scaled UP", "down": "scaled DOWN"}.get(direction, "changed")
        labbus.emit(self.r, "scale", f"Autoscaler {verb}: {old} -> {new} workers ({why})", **item)

    # ---------------------------------------------------------------- main
    def run(self) -> None:
        signal.signal(signal.SIGTERM, lambda *_: setattr(self, "stop", True))
        signal.signal(signal.SIGINT, lambda *_: setattr(self, "stop", True))
        serve_control(self)
        labbus.emit(self.r, "supervisor_start", f"Processor supervisor started with {self.target} workers")
        last_lag = 0.0
        while not self.stop:
            now = time.time()
            if now - last_lag >= 2:
                self.measure_lag()
                self.autoscale()
                last_lag = now
                c = self.cfg.snapshot()
                M.CFG_VERSION.set(self.cfg.version)
                M.CFG_ALERT_LATENCY.set(c["alerts/latency_ms"])
                M.CFG_ALERT_ERRORS.set(c["alerts/error_rate_pct"])
                M.CFG_ALERT_LAG.set(c["alerts/consumer_lag"])
            self.reconcile()
            try:
                self.r.set("lab:supervisor", json.dumps(self.describe()), ex=10)
            except Exception:
                pass
            time.sleep(1)
        self.log.info("supervisor stopping: asking workers to stop gracefully")
        for slot in self.slots:
            self.graceful_stop(slot)
        for slot in self.slots:
            if slot.proc is not None:
                slot.proc.join(20)


def serve_control(sup: Supervisor) -> None:
    class Handler(BaseHTTPRequestHandler):
        def _send(self, code: int, body: dict) -> None:
            data = json.dumps(body).encode()
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def do_GET(self):  # noqa: N802
            if self.path in ("/health", "/"):
                return self._send(200, {"ok": True})
            if self.path == "/workers":
                return self._send(200, sup.describe())
            self._send(404, {"error": "not found"})

        def do_POST(self):  # noqa: N802
            parts = self.path.strip("/").split("/")
            try:
                if parts == ["disaster"]:
                    return self._send(200, sup.disaster())
                if len(parts) == 3 and parts[0] == "workers" and parts[2] == "crash":
                    return self._send(200, sup.crash(parts[1]))
                if len(parts) == 3 and parts[0] == "workers" and parts[2] == "restart":
                    return self._send(200, sup.restart(parts[1]))
            except KeyError:
                return self._send(404, {"error": "unknown worker"})
            self._send(404, {"error": "not found"})

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(("0.0.0.0", 8090), Handler)
    threading.Thread(target=server.serve_forever, name="control-http", daemon=True).start()


def main() -> None:
    mp_dir = os.environ.get("PROMETHEUS_MULTIPROC_DIR", "/tmp/prom-mp")
    os.makedirs(mp_dir, exist_ok=True)
    for f in glob.glob(os.path.join(mp_dir, "*.db")):
        os.remove(f)
    registry = CollectorRegistry()
    multiprocess.MultiProcessCollector(registry)
    start_http_server(9100, registry=registry)
    Supervisor().run()
