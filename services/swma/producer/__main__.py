"""Event Sources: simulated e-commerce microservices emitting telemetry.

Every event is keyed by ``service`` so Kafka sends all events of one service
to the same partition (needed for correct per-service window state).

Knobs (all live, from the Control Plane / Consul):
    producer/enabled, producer/rate_eps, producer/scenario
    chaos/out_of_order_pct, chaos/late_pct, chaos/duplicate_pct, chaos/malformed_pct

For the window-correctness check, every *valid* event (not duplicated,
malformed or deliberately too late) also increments a ground-truth counter in
Redis: ``truth:<tumbling size>:<service>:<window start>``.
"""
from __future__ import annotations

import json
import math
import random
import signal
import time
import uuid
from collections import deque

import redis
from confluent_kafka import Producer
from prometheus_client import Counter, Gauge, start_http_server

from swma.common import labbus, log as logsetup, settings
from swma.common.config import LiveConfig
from swma.common.schema import SERVICE_CATALOG, iso, now_ms

SENT = Counter("swma_producer_events_total", "Events produced", ["service", "kind"])
DELIVERED = Counter("swma_producer_delivered_total", "Events acknowledged by Kafka", ["partition"])
ERRORS = Counter("swma_producer_errors_total", "Delivery failures")
TARGET = Gauge("swma_producer_target_eps", "Configured target rate (scenario adjusted)")
RUNNING = Gauge("swma_producer_running", "1 while the producer is generating events")
CPU = Gauge("swma_simulated_service_cpu", "Simulated CPU % of each microservice", ["service"])

METHODS = {"/pay": "POST", "/refund": "POST", "/orders": "POST", "/reserve": "POST", "/login": "POST",
           "/cart/items": "POST"}


class Generator:
    def __init__(self) -> None:
        self.rnd = random.Random()
        self.cpu = {s: 35 + self.rnd.random() * 20 for s in SERVICE_CATALOG}
        self.mem = {s: 45 + self.rnd.random() * 20 for s in SERVICE_CATALOG}
        self.recent: deque = deque(maxlen=300)
        self.scenario = "normal"
        self.scenario_started = time.time()

    def set_scenario(self, name: str) -> None:
        if name != self.scenario:
            self.scenario, self.scenario_started = name, time.time()

    def weights(self) -> tuple[list[str], list[float]]:
        names = list(SERVICE_CATALOG)
        w = [SERVICE_CATALOG[s]["weight"] for s in names]
        if self.scenario == "hot_partition":
            total = sum(w) - SERVICE_CATALOG["payment-service"]["weight"]
            w = [4 * total if s == "payment-service" else SERVICE_CATALOG[s]["weight"] for s in names]
        return names, w

    def rate_multiplier(self) -> float:
        return 4.0 if self.scenario == "traffic_spike" else 1.0

    def event(self, wall: int) -> dict:
        names, w = self.weights()
        svc = self.rnd.choices(names, weights=w)[0]
        cat = SERVICE_CATALOG[svc]
        elapsed = time.time() - self.scenario_started
        latency = self.rnd.lognormvariate(math.log(cat["latency_ms"]), 0.35)
        err = cat["error_pct"] / 100
        if self.scenario == "latency_degradation" and svc == "payment-service":
            latency *= 1 + min(elapsed / 60, 1) * 5  # ramps to 6x over a minute
        if self.scenario == "error_storm" and svc == "inventory-service":
            err = 0.35
        if self.scenario == "traffic_spike":
            latency *= 1.6
        r = self.rnd.random()
        status = (self.rnd.choice([500, 502, 503]) if r < err else
                  404 if r < err + 0.02 else 201 if r < err + 0.1 else 200)
        self.cpu[svc] = min(98, max(5, self.cpu[svc] + self.rnd.uniform(-2, 2) + (0.5 if latency > 400 else 0)))
        self.mem[svc] = min(95, max(20, self.mem[svc] + self.rnd.uniform(-1, 1)))
        endpoint = self.rnd.choice(cat["endpoints"])
        return {
            "event_id": f"evt-{uuid.uuid4().hex[:16]}",
            "service": svc,
            "event_time": iso(wall),
            "endpoint": endpoint,
            "method": METHODS.get(endpoint, "GET"),
            "response_time_ms": round(latency, 1),
            "status_code": status,
            "cpu_percent": round(self.cpu[svc], 1),
            "memory_percent": round(self.mem[svc], 1),
            "region": "ap-south-1",
            "source": "simulator",
        }

    def malformed(self, wall: int) -> bytes:
        kind = self.rnd.choice(["broken_json", "missing_field", "wrong_type", "bad_time"])
        if kind == "broken_json":
            return b'{"event_id": "evt-broken", "service": "cart-service", "response_time_ms": 12'
        ev = self.event(wall)
        if kind == "missing_field":
            ev.pop("service")
        elif kind == "wrong_type":
            ev["response_time_ms"] = "fast"
        else:
            ev["event_time"] = "yesterday at noon"
        return json.dumps(ev).encode()


class ProducerService:
    def __init__(self) -> None:
        settings.INSTANCE = "producer"
        self.log = logsetup.setup("producer")
        self.cfg = LiveConfig().start()
        self.r = redis.Redis.from_url(settings.REDIS_URL, decode_responses=True, socket_timeout=3)
        self.gen = Generator()
        self.stop = False
        self.counts = {"valid": 0, "out_of_order": 0, "late": 0, "duplicate": 0, "malformed": 0}
        self.by_partition: dict[int, int] = {}
        self.sent_window: deque = deque()
        self.sample: dict | None = None
        self.kafka = Producer({
            "bootstrap.servers": settings.KAFKA_BOOTSTRAP,
            "client.id": "telemetry-simulator",
            "acks": "all",
            "enable.idempotence": True,
            "linger.ms": 10,
            "compression.type": "lz4",
        })

    def delivered(self, err, msg) -> None:
        if err is not None:
            ERRORS.inc()
            self.log.warning("delivery failed: %s", err)
            return
        DELIVERED.labels(str(msg.partition())).inc()
        self.by_partition[msg.partition()] = self.by_partition.get(msg.partition(), 0) + 1

    def emit_one(self, wall: int, c: dict) -> None:
        roll = self.gen.rnd.random() * 100
        cut_mal = c["chaos/malformed_pct"]
        cut_dup = cut_mal + c["chaos/duplicate_pct"]
        cut_late = cut_dup + c["chaos/late_pct"]
        cut_ooo = cut_late + c["chaos/out_of_order_pct"]
        if roll < cut_mal:
            payload = self.gen.malformed(wall)
            self.kafka.produce(settings.TOPIC_EVENTS, key=b"malformed", value=payload, on_delivery=self.delivered)
            self.counts["malformed"] += 1
            SENT.labels("unknown", "malformed").inc()
            return
        if roll < cut_dup and self.gen.recent:
            ev = dict(self.gen.rnd.choice(self.gen.recent))
            self._send(ev)
            self.counts["duplicate"] += 1
            SENT.labels(ev["service"], "duplicate").inc()
            return
        ev = self.gen.event(wall)
        kind = "valid"
        if roll < cut_late:
            back = (c["window/tumbling_size_s"] + c["window/allowed_lateness_s"]
                    + c["watermark/max_out_of_orderness_s"] + 20) * 1000
            ev["event_time"] = iso(wall - back)
            kind = "late"
        elif roll < cut_ooo:
            bound = max(1, c["watermark/max_out_of_orderness_s"]) * 1000
            ev["event_time"] = iso(wall - self.gen.rnd.randint(300, int(bound * 0.9)))
            kind = "out_of_order"
        self._send(ev)
        self.counts[kind] += 1
        SENT.labels(ev["service"], kind).inc()
        if kind != "late":
            self.gen.recent.append(ev)
            self._truth(ev, c)
        self.sample = ev

    def _send(self, ev: dict) -> None:
        self.kafka.produce(settings.TOPIC_EVENTS, key=ev["service"].encode(), value=json.dumps(ev).encode(),
                           on_delivery=self.delivered)
        self.sent_window.append(time.time())

    def _truth(self, ev: dict, c: dict) -> None:
        from swma.common.schema import parse_event_time
        size = c["window/tumbling_size_s"] * 1000
        ts = parse_event_time(ev["event_time"])
        start = ts - ts % size
        key = f"truth:{c['window/tumbling_size_s']}:{ev['service']}:{start}"
        self.truth_pipe.incr(key)
        self.truth_pipe.expire(key, 7200)

    def status(self, running: bool, target: float) -> None:
        now = time.time()
        while self.sent_window and now - self.sent_window[0] > 5:
            self.sent_window.popleft()
        doc = {
            "running": running, "scenario": self.gen.scenario, "target_eps": round(target, 1),
            "actual_eps": round(len(self.sent_window) / 5, 1), "counts": self.counts,
            "by_partition": self.by_partition, "sample": self.sample, "updated_at": iso(now_ms()),
        }
        try:
            self.r.set("lab:producer", json.dumps(doc), ex=10)
        except Exception:
            pass

    def run(self) -> None:
        signal.signal(signal.SIGTERM, lambda *_: setattr(self, "stop", True))
        start_http_server(9102)
        labbus.emit(self.r, "producer_start", "Telemetry simulator started (7 e-commerce microservices)")
        credit, last, last_status, was_running = 0.0, time.time(), 0.0, None
        while not self.stop:
            c = self.cfg.snapshot()
            running = bool(c["producer/enabled"])
            self.gen.set_scenario(c["producer/scenario"])
            target = c["producer/rate_eps"] * self.gen.rate_multiplier() if running else 0
            if running != was_running:
                if was_running is not None:
                    labbus.emit(self.r, "producer", "Event stream " + ("started" if running else "stopped"))
                was_running = running
            TARGET.set(target)
            RUNNING.set(1 if running else 0)
            now = time.time()
            credit = min(credit + (now - last) * target, max(target, 1))
            last = now
            n = int(credit)
            if n:
                credit -= n
                wall = now_ms()
                self.truth_pipe = self.r.pipeline(transaction=False)
                for _ in range(n):
                    self.emit_one(wall, c)
                try:
                    self.truth_pipe.execute()
                except Exception:
                    pass
            self.kafka.poll(0)
            if now - last_status >= 1:
                self.status(running, target)
                for svc, v in self.gen.cpu.items():
                    CPU.labels(svc).set(round(v, 1))
                last_status = now
            time.sleep(0.02)
        self.kafka.flush(10)


if __name__ == "__main__":
    ProducerService().run()
