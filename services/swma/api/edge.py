"""Edge cases & stress tests, run for real against the live pipeline.

Each run uses its own throw-away service names (``edge-<run>-<n>``) so its
events land in their own windows and never disturb the normal stream, the
dashboards or the correctness check. A run:

  1. sends its traffic to Kafka (bursts, duplicates, late, broken events ...)
  2. watches what the pipeline does with it (counted in windows, dead letters,
     consumer lag) and records a timeline
  3. checks the result against what a correct system must do

Runs execute in the background; progress is kept in Redis so either API
replica can answer the browser's polling.
"""
from __future__ import annotations

import asyncio
import json
import random
import time
import uuid
from collections import Counter

from confluent_kafka import Consumer, TopicPartition
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from starlette.concurrency import run_in_threadpool

from swma.common import labbus, settings
from swma.common.log import audit
from swma.common.schema import iso, now_ms
from swma.engine.windows import tumbling, window_id

from .deps import C, admin

router = APIRouter(prefix="/api/lab/edge", tags=["edge cases"])

SCENARIOS = {  # id -> (default n, max n)
    "burst": (1000, 10000),
    "hot_key": (1000, 5000),
    "duplicates": (300, 2000),
    "out_of_order": (500, 5000),
    "late": (200, 2000),
    "malformed": (120, 1200),
    "clock_skew": (100, 1000),
    "boundary": (3, 3),
    "crash_mid_burst": (3000, 10000),
    "oversized": (2, 2),
}
TERMINAL = ("passed", "failed", "error")


class RunRequest(BaseModel):
    scenario: str
    n: int | None = Field(None, ge=1, le=10000)


# --------------------------------------------------------------- helpers
def event(svc: str, ts: int, eid: str | None = None, latency: float | None = None, status: int = 200) -> dict:
    return {"event_id": eid or f"edge-{uuid.uuid4().hex[:14]}", "service": svc, "event_time": iso(ts),
            "endpoint": "/edge-case", "method": "GET",
            "response_time_ms": latency if latency is not None else round(random.uniform(20, 300), 1),
            "status_code": status, "cpu_percent": 40.0, "memory_percent": 55.0, "source": "edge-case-lab"}


def _produce(messages: list[tuple[str, bytes]]) -> dict:
    """Send (key, value) pairs; returns partition counts and errors."""
    parts: Counter = Counter()
    errors: Counter = Counter()

    def cb(err, msg):
        if err:
            errors[err.str()] += 1
        else:
            parts[msg.partition()] += 1

    for key, value in messages:
        while True:
            try:
                C.producer.produce(settings.TOPIC_EVENTS, key=key.encode(), value=value, on_delivery=cb)
                break
            except BufferError:  # local queue full: let it drain
                C.producer.poll(0.1)
            except Exception as exc:  # e.g. message too large, rejected before sending
                errors[str(exc)] += 1
                break
        C.producer.poll(0)
    C.producer.flush(60)
    return {"partitions": dict(parts), "errors": dict(errors)}


def _dlq_offsets() -> dict[int, int]:
    with C.consumer_lock:
        out = {}
        for p in range(2):
            try:
                _, hi = C.consumer.get_watermark_offsets(TopicPartition(settings.TOPIC_DLQ, p), timeout=5, cached=False)
                out[p] = hi
            except Exception:
                out[p] = 0
        return out


def _dlq_scan(start: dict[int, int], marker: str) -> Counter:
    """Dead-letter records written since `start` whose payload mentions `marker`, by reason."""
    c = Consumer({"bootstrap.servers": settings.KAFKA_BOOTSTRAP, "group.id": f"edge-scan-{uuid.uuid4().hex[:6]}",
                  "enable.auto.commit": False, "client.id": "edge-scan"})
    reasons: Counter = Counter()
    try:
        ends = {}
        for p, off in start.items():
            _, hi = c.get_watermark_offsets(TopicPartition(settings.TOPIC_DLQ, p), timeout=5)
            if hi > off:
                ends[p] = hi
        if not ends:
            return reasons
        c.assign([TopicPartition(settings.TOPIC_DLQ, p, start[p]) for p in ends])
        done, deadline = set(), time.time() + 10
        while len(done) < len(ends) and time.time() < deadline:
            for m in c.consume(500, 0.5):
                if m.error():
                    continue
                if m.offset() >= ends[m.partition()] - 1:
                    done.add(m.partition())
                try:
                    rec = json.loads(m.value())
                except Exception:
                    continue
                if marker in rec.get("original", ""):
                    reasons[rec.get("reason", "?")] += 1
    finally:
        c.close()
    return reasons


async def _counted(services: list[str]) -> int:
    """Events counted by the processor = sum of the tumbling-window counts of these services."""
    total = 0
    for svc in services:
        ids = await C.r.zrange(f"metrics:history:tumbling:{svc}", 0, -1)
        for wid in ids:
            raw = await C.r.hget(f"metrics:tumbling:{wid}", svc)
            if raw:
                total += json.loads(raw)["count"]
    return total


async def _lag() -> int:
    raw = await C.r.get("lab:supervisor")
    try:
        return int(json.loads(raw)["lag"]["live_total"]) if raw else 0
    except Exception:
        return 0


class Run:
    def __init__(self, rid: str, scenario: str, n: int, user: str):
        self.key = f"lab:edge:{rid}"
        self.t0 = time.time()
        self.s = {"id": rid, "scenario": scenario, "n": n, "user": user, "status": "running", "phase": "starting",
                  "started_at": iso(now_ms()), "progress": {}, "timeline": [], "checks": [], "facts": {}}

    async def save(self) -> None:
        self.s["elapsed_s"] = round(time.time() - self.t0, 1)
        await C.r.set(self.key, json.dumps(self.s), ex=6 * 3600)

    async def phase(self, text: str) -> None:
        self.s["phase"] = text
        await self.save()

    def check(self, name: str, expected, actual, ok: bool, why: str) -> None:
        self.s["checks"].append({"name": name, "expected": expected, "actual": actual, "pass": bool(ok), "why": why})

    async def watch(self, services: list[str], expect_counted: int, marker: str | None, dlq_start: dict | None,
                    expect_dlq: int, timeout: float = 60) -> tuple[int, Counter, float | None]:
        """Poll until the pipeline has done what we expect (or timeout). Returns counted, dlq reasons, seconds."""
        start, reasons, done_at, last_scan, peak = time.time(), Counter(), None, 0.0, 0
        counted = 0
        while time.time() - start < timeout:
            counted = await _counted(services)
            lag = await _lag()
            peak = max(peak, lag)
            if marker and dlq_start is not None and time.time() - last_scan >= 2:
                reasons = await run_in_threadpool(_dlq_scan, dlq_start, marker)
                last_scan = time.time()
            dlq = sum(reasons.values())
            t = round(time.time() - self.t0, 1)
            self.s["timeline"].append({"t": t, "counted": counted, "lag": lag, "dlq": dlq})
            self.s["progress"] = {"counted": counted, "expected_counted": expect_counted, "dlq": dlq,
                                  "expected_dlq": expect_dlq, "lag": lag, "lag_peak": peak}
            await self.save()
            if counted >= expect_counted and dlq >= expect_dlq and done_at is None:
                done_at = time.time() - start
                if expect_counted == 0 and expect_dlq == 0:
                    break
                # settle: make sure nothing *extra* shows up (double counting, stray dead letters)
                await asyncio.sleep(4)
                counted = await _counted(services)
                if marker and dlq_start is not None:
                    reasons = await run_in_threadpool(_dlq_scan, dlq_start, marker)
                self.s["progress"].update(counted=counted, dlq=sum(reasons.values()))
                break
            await asyncio.sleep(0.7)
        self.s["facts"]["lag_peak"] = peak
        return counted, reasons, done_at


# ------------------------------------------------------------- scenarios
async def sc_burst(run: Run, n: int, rid: str, hot: bool = False) -> None:
    services = [f"edge-{rid}-0"] if hot else [f"edge-{rid}-{i}" for i in range(8)]
    now = now_ms()
    msgs = []
    for i in range(n):
        svc = services[i % len(services)]
        msgs.append((svc, json.dumps(event(svc, now + random.randint(-200, 200))).encode()))
    await run.phase(f"sending {n} events at once" + (" (all with the same key)" if hot else f" for {len(services)} services"))
    t = time.time()
    res = await run_in_threadpool(_produce, msgs)
    send_s = time.time() - t
    run.s["facts"].update(sent=n, send_seconds=round(send_s, 2), ingest_eps=round(n / max(send_s, 1e-3)),
                          partitions=res["partitions"], send_errors=res["errors"])
    await run.phase("waiting for the processor to count them")
    counted, _, took = await run.watch(services, n, None, None, 0, timeout=90)
    run.s["facts"]["seconds_until_all_counted"] = round(took, 1) if took else None
    run.check("Every event counted exactly once", n, counted, counted == n,
              "Window counts must add up to exactly the number of events sent: none lost, none double-counted.")
    run.check("Kafka accepted the whole burst", 0, sum(res["errors"].values()), not res["errors"],
              "The broker buffers the spike; producers are not rejected.")
    if hot:
        run.check("One key → one partition", 1, len(res["partitions"]), len(res["partitions"]) == 1,
                  "All events of a service share its key, so they all land in the same partition and are processed by one worker, in order.")
    else:
        run.check("Burst spread over several partitions", ">1", len(res["partitions"]), len(res["partitions"]) > 1,
                  "Different keys hash to different partitions, so several workers share the load.")


async def sc_duplicates(run: Run, n: int, rid: str) -> None:
    services = [f"edge-{rid}-{i}" for i in range(4)]
    now = now_ms()
    originals = [event(services[i % 4], now) for i in range(n)]
    msgs = []
    for copy in range(3):
        for e in originals:
            msgs.append((e["service"], json.dumps(e).encode()))
    random.shuffle(msgs)
    await run.phase(f"sending {n} events, each one 3 times ({3 * n} messages) in random order")
    res = await run_in_threadpool(_produce, msgs)
    run.s["facts"].update(unique_events=n, messages_sent=3 * n, partitions=res["partitions"])
    await run.phase("waiting; duplicates should be ignored")
    counted, _, _ = await run.watch(services, n, None, None, 0, timeout=60)
    run.check("Counted once, not three times", n, counted, counted == n,
              "The processor remembers recent event_ids and ignores repeats (idempotent aggregation).")


async def sc_out_of_order(run: Run, n: int, rid: str) -> None:
    services = [f"edge-{rid}-{i}" for i in range(4)]
    bound = int(C.cfg.get("watermark/max_out_of_orderness_s")) * 1000
    now = now_ms()
    evs = [event(services[i % 4], now - random.randint(0, int(bound * 0.8))) for i in range(n)]
    random.shuffle(evs)
    await run.phase(f"sending {n} events with timestamps shuffled within the last {bound // 1000 * 0.8:.0f} s")
    await run_in_threadpool(_produce, [(e["service"], json.dumps(e).encode()) for e in evs])
    dlq0 = await run_in_threadpool(_dlq_offsets)
    counted, reasons, _ = await run.watch(services, n, f"edge-{rid}-", dlq0, 0, timeout=60)
    run.s["facts"].update(sent=n, watermark_bound_s=bound / 1000)
    run.check("All out-of-order events counted", n, counted, counted == n,
              "Arrival order does not matter: windows use event time, and the watermark waits for the out-of-orderness bound.")
    run.check("None sent to the dead letter queue", 0, sum(reasons.values()), not reasons,
              "Disorder inside the bound is normal and must not be treated as 'late'.")


async def sc_late(run: Run, n: int, rid: str) -> None:
    services = [f"edge-{rid}-{i}" for i in range(4)]
    c = C.cfg.snapshot()
    age = (c["window/tumbling_size_s"] + c["window/allowed_lateness_s"] + c["watermark/max_out_of_orderness_s"] + 120) * 1000
    dlq0 = await run_in_threadpool(_dlq_offsets)
    now = now_ms()
    await run.phase(f"sending {n} events that happened {age // 1000} s ago (older than window + allowed lateness)")
    await run_in_threadpool(_produce, [(services[i % 4], json.dumps(event(services[i % 4], now - age)).encode())
                                       for i in range(n)])
    counted, reasons, _ = await run.watch(services, 0, f"edge-{rid}-", dlq0, n, timeout=60)
    run.s["facts"].update(sent=n, event_age_s=age // 1000, dlq_reasons=dict(reasons))
    run.check("No final window was changed", 0, counted, counted == 0,
              "Results that are already final must not change silently.")
    run.check("Every late event went to the dead letter queue", n, reasons.get("too_late", 0),
              reasons.get("too_late", 0) == n, "They are parked in telemetry.dlq with reason 'too_late' for inspection.")


async def sc_malformed(run: Run, n: int, rid: str) -> None:
    svc = f"edge-{rid}-0"
    now = now_ms()
    kinds = ["broken_json", "not_an_object", "missing_field", "wrong_type", "negative_latency", "bad_status",
             "bad_time", "cpu_over_100"]
    msgs, per_kind = [], Counter()
    for i in range(n):
        k = kinds[i % len(kinds)]
        per_kind[k] += 1
        e = event(svc, now)
        if k == "broken_json":
            raw = json.dumps(e)[:-20].encode()
        elif k == "not_an_object":
            raw = json.dumps([svc, "this is a list"]).encode()
        else:
            if k == "missing_field":
                e.pop("response_time_ms")
            elif k == "wrong_type":
                e["status_code"] = "OK"
            elif k == "negative_latency":
                e["response_time_ms"] = -50
            elif k == "bad_status":
                e["status_code"] = 999
            elif k == "bad_time":
                e["event_time"] = "yesterday at noon"
            elif k == "cpu_over_100":
                e["cpu_percent"] = 150
            raw = json.dumps(e).encode()
        msgs.append((svc, raw))
    dlq0 = await run_in_threadpool(_dlq_offsets)
    await run.phase(f"sending {n} broken events ({len(kinds)} kinds of damage)")
    await run_in_threadpool(_produce, msgs)
    counted, reasons, _ = await run.watch([svc], 0, svc, dlq0, n, timeout=60)
    run.s["facts"].update(sent=n, kinds=dict(per_kind), dlq_reasons=dict(reasons))
    run.check("No broken event reached the aggregates", 0, counted, counted == 0,
              "Validation happens before aggregation, so bad data cannot corrupt the metrics.")
    run.check("Every broken event is in the dead letter queue", n, sum(reasons.values()), sum(reasons.values()) == n,
              "Nothing is silently dropped: each record keeps its reason and original payload.")
    overview = await C.r.get("lab:producer")
    run.check("The normal stream kept flowing", True, bool(overview), bool(overview),
              "Bad records are isolated; the pipeline does not crash or stop.")


async def sc_clock_skew(run: Run, n: int, rid: str) -> None:
    ok_svc, bad_svc = f"edge-{rid}-ok", f"edge-{rid}-bad"
    now = now_ms()
    half = max(1, n // 2)
    msgs = [(ok_svc, json.dumps(event(ok_svc, now + 60_000)).encode()) for _ in range(half)]
    msgs += [(bad_svc, json.dumps(event(bad_svc, now + 10 * 60_000)).encode()) for _ in range(half)]
    dlq0 = await run_in_threadpool(_dlq_offsets)
    await run.phase(f"sending {half} events stamped 1 min in the future and {half} stamped 10 min in the future")
    await run_in_threadpool(_produce, msgs)
    counted, reasons, _ = await run.watch([ok_svc], half, f"edge-{rid}-bad", dlq0, half, timeout=60)
    run.s["facts"].update(slightly_ahead=half, far_ahead=half, dlq_reasons=dict(reasons))
    run.check("Slightly-ahead clocks tolerated", half, counted, counted == half,
              "Up to 5 minutes of clock skew is accepted, and the watermark is clamped to the real clock so it can't jump ahead.")
    run.check("Far-future timestamps rejected", half, reasons.get("future_event_time", 0),
              reasons.get("future_event_time", 0) == half, "A clock 10 minutes ahead is treated as invalid data (dead letter).")


async def sc_boundary(run: Run, n: int, rid: str) -> None:
    svc = f"edge-{rid}-0"
    size = int(C.cfg.get("window/tumbling_size_s")) * 1000
    now = now_ms()
    boundary = now - now % size + size  # start of the next tumbling window
    cases = [("1 ms before the boundary", boundary - 1), ("exactly on the boundary", boundary),
             ("1 ms after the boundary", boundary + 1)]
    ids = []
    msgs = []
    for label, ts in cases:
        e = event(svc, ts)
        e["trace"] = True
        ids.append((label, ts, e["event_id"]))
        msgs.append((svc, json.dumps(e).encode()))
    await run.phase(f"sending 3 events around the window boundary {iso(boundary)}")
    await run_in_threadpool(_produce, msgs)
    await run.watch([svc], 3, None, None, 0, timeout=40)
    for label, ts, eid in ids:
        expected = window_id(*tumbling(ts, size)[0])
        actual = "not processed"
        for _ in range(20):
            raw = await C.r.hget(f"trace:{eid}", "windows")
            if raw:
                tw = [w["window_id"] for w in json.loads(raw) if w["window_type"] == "tumbling"]
                actual = tw[0] if tw else "none"
                break
            await asyncio.sleep(0.5)
        run.check(f"Event {label} → correct window", expected, actual, expected == actual,
                  "Windows are half-open [start, end): an event exactly at the end belongs to the NEXT window.")
    run.s["facts"].update(boundary=iso(boundary), window_size_s=size // 1000)


async def sc_crash_mid_burst(run: Run, n: int, rid: str) -> None:
    import httpx
    services = [f"edge-{rid}-{i}" for i in range(8)]
    now = now_ms()
    msgs = [(services[i % 8], json.dumps(event(services[i % 8], now)).encode()) for i in range(n)]
    chunks = [msgs[i::10] for i in range(10)]
    crashed = None
    await run.phase(f"sending {n} events in 10 chunks; a worker will be killed after chunk 4")
    for i, chunk in enumerate(chunks):
        await run_in_threadpool(_produce, chunk)
        if i == 3:
            d = json.loads(await C.r.get("lab:supervisor") or "{}")
            for w in d.get("workers", []):
                if w.get("state") == "running":
                    crashed = w["worker"]
                    async with httpx.AsyncClient(timeout=5) as h:
                        await h.post(f"{settings.SUPERVISOR_URL}/workers/{crashed}/crash")
                    await asyncio.to_thread(labbus.emit, C.rs, "chaos",
                                            f"Edge-case run {rid}: killed {crashed} in the middle of a burst")
                    break
            await run.phase(f"💥 killed {crashed} (SIGKILL) mid-burst; still sending")
        await asyncio.sleep(0.3)
    run.s["facts"].update(sent=n, crashed_worker=crashed)
    await run.phase("waiting for rebalance, state restore from checkpoint and replay")
    counted, _, took = await run.watch(services, n, None, None, 0, timeout=150)
    run.s["facts"]["seconds_until_all_counted"] = round(took, 1) if took else None
    run.check("A worker was crashed during the burst", "yes", crashed or "no", bool(crashed),
              "Simulates a real failure while data is in flight.")
    run.check("Exactly-once state despite the crash", n, counted, counted == n,
              "State + offsets are checkpointed together, so the replay after the crash neither loses nor double-counts events.")


async def sc_oversized(run: Run, n: int, rid: str) -> None:
    svc = f"edge-{rid}-0"
    now = now_ms()
    big_ok = event(svc, now)
    big_ok["endpoint"] = "/x" * 100_000  # ~200 KB, below Kafka's 1 MB message limit
    too_big = event(svc, now)
    too_big["endpoint"] = "/x" * 1_100_000  # ~2.2 MB, above the limit
    await run.phase("sending a 200 KB event and a 2.2 MB event")
    res_ok = await run_in_threadpool(_produce, [(svc, json.dumps(big_ok).encode())])
    res_big = await run_in_threadpool(_produce, [(svc, json.dumps(too_big).encode())])
    counted, _, _ = await run.watch([svc], 1, None, None, 0, timeout=40)
    run.s["facts"].update(ok_size_kb=len(json.dumps(big_ok)) // 1024, rejected_size_kb=len(json.dumps(too_big)) // 1024,
                          rejection=res_big["errors"])
    run.check("Large but allowed event is processed", 1, counted, counted == 1 and not res_ok["errors"],
              "Kafka accepts messages up to 1 MB by default; extra fields are kept, the metrics use what they need.")
    run.check("Oversized event rejected at the door", "rejected", "rejected" if res_big["errors"] else "accepted",
              bool(res_big["errors"]), "The producer refuses messages above the broker limit before they enter the stream.")


RUNNERS = {"burst": sc_burst, "hot_key": lambda r, n, rid: sc_burst(r, n, rid, hot=True),
           "duplicates": sc_duplicates, "out_of_order": sc_out_of_order, "late": sc_late, "malformed": sc_malformed,
           "clock_skew": sc_clock_skew, "boundary": sc_boundary, "crash_mid_burst": sc_crash_mid_burst,
           "oversized": sc_oversized}


async def _execute(run: Run, scenario: str, n: int, rid: str) -> None:
    try:
        await RUNNERS[scenario](run, n, rid)
        run.s["status"] = "passed" if all(c["pass"] for c in run.s["checks"]) else "failed"
        run.s["phase"] = "done"
    except Exception as exc:
        run.s["status"], run.s["phase"], run.s["error"] = "error", "error", f"{type(exc).__name__}: {exc}"
    run.s["finished_at"] = iso(now_ms())
    await run.save()
    await C.r.lpush("lab:edge:runs", run.key)
    await C.r.ltrim("lab:edge:runs", 0, 49)
    await asyncio.to_thread(labbus.emit, C.rs, "edge",
                            f"Edge case '{scenario}' ({n} events) {run.s['status'].upper()}")


_tasks: set = set()


@router.post("/run", summary="Start an edge-case scenario against the live pipeline")
async def start(body: RunRequest, user=Depends(admin)):
    if body.scenario not in SCENARIOS:
        raise HTTPException(400, f"scenario must be one of {sorted(SCENARIOS)}")
    default, maximum = SCENARIOS[body.scenario]
    n = min(body.n or default, maximum)
    rid = uuid.uuid4().hex[:6]
    run = Run(rid, body.scenario, n, user["sub"])
    await run.save()
    audit(__import__("logging").getLogger("api"), "edge_case", user=user["sub"], scenario=body.scenario, n=n)
    task = asyncio.create_task(_execute(run, body.scenario, n, rid))
    _tasks.add(task)
    task.add_done_callback(_tasks.discard)
    return {"id": rid, "scenario": body.scenario, "n": n}


@router.get("/runs/{rid}")
async def get_run(rid: str):
    raw = await C.r.get(f"lab:edge:{rid}")
    if not raw:
        raise HTTPException(404, "unknown run")
    return json.loads(raw)


@router.get("/runs")
async def list_runs():
    out = []
    for key in await C.r.lrange("lab:edge:runs", 0, 19):
        raw = await C.r.get(key)
        if raw:
            d = json.loads(raw)
            out.append({k: d.get(k) for k in ("id", "scenario", "n", "status", "started_at", "elapsed_s")})
    return out
