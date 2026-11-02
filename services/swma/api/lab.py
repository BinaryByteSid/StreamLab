"""Lab endpoints used by the virtual-lab portal (status, topology, controls)."""
from __future__ import annotations

import asyncio
import json
import logging
import time
import uuid

from confluent_kafka import TopicPartition
from fastapi import APIRouter, Body, Depends, HTTPException
from pydantic import BaseModel, Field
from starlette.concurrency import run_in_threadpool

from swma.common import labbus, settings
from swma.common.config import SPECS, SPEC_BY_KEY, coerce, encode, put
from swma.common.log import audit
from swma.common.schema import SERVICE_CATALOG, iso, now_ms
from swma.engine.windows import sliding, tumbling, window_id

from .deps import C, admin
from .public import history

router = APIRouter(prefix="/api/lab", tags=["lab"])
log = logging.getLogger("api")

# id, name, diagram section, check kind, target, public url key
COMPONENTS = [
    ("producer", "Event producer (7 microservices)", 1, "redis", "lab:producer", None),
    ("redpanda-0", "Kafka broker 0 (Redpanda)", 2, "http", "http://redpanda-0:9644/v1/status/ready", "console"),
    ("redpanda-1", "Kafka broker 1 (Redpanda)", 2, "http", "http://redpanda-1:9644/v1/status/ready", "console"),
    ("redpanda-2", "Kafka broker 2 (Redpanda)", 2, "http", "http://redpanda-2:9644/v1/status/ready", "console"),
    ("schema-registry", "Schema Registry", 2, "http", "http://redpanda-0:8081/subjects", "console"),
    ("redpanda-console", "Redpanda Console (Kafka UI)", 2, "http", "http://redpanda-console:8080/", "console"),
    ("processor", "Stream processor (supervisor + workers)", 3, "http", "http://processor:8090/health", None),
    ("redis", "Redis (metrics store)", 4, "redis-ping", None, "redisinsight"),
    ("redisinsight", "RedisInsight (Redis UI)", 4, "http", "http://redisinsight:5540/", "redisinsight"),
    ("api-1", "FastAPI instance 1", 4, "http", "http://api-1:8000/api/health", "api_docs"),
    ("api-2", "FastAPI instance 2", 4, "http", "http://api-2:8000/api/health", "api_docs"),
    ("dashboard", "Dashboard web server", 5, "http", "http://dashboard/", "lab"),
    ("consul", "Consul (config + service registry)", 6, "http", "http://consul:8500/v1/status/leader", "consul"),
    ("ops-agent", "Ops agent (CI/CD runner + chaos)", 7, "http", "http://ops-agent:8095/health", None),
    ("registry", "Container registry", 8, "http", "http://registry:5000/v2/", "registry"),
    ("registry-ui", "Registry UI", 8, "http", "http://registry-ui/", "registry"),
    ("alb", "Load balancer (nginx = AWS ALB)", 9, "http", "http://alb/alb/health", "lab"),
    ("s3", "S3 object storage (RustFS)", 9, "http", "http://rustfs:9000/", "s3"),
    ("prometheus", "Prometheus", 10, "http", "http://prometheus:9090/-/ready", "prometheus"),
    ("alertmanager", "Alertmanager", 10, "http", "http://alertmanager:9093/-/ready", "alertmanager"),
    ("grafana", "Grafana", 10, "http", "http://grafana:3000/api/health", "grafana"),
    ("loki", "Loki (logs)", 10, "http", "http://loki:3100/ready", "loki"),
    ("redis-exporter", "Redis exporter", 10, "http", "http://redis-exporter:9121/metrics", None),
]
_status_cache: dict = {"at": 0, "data": None}


async def _check(comp) -> dict:
    cid, name, section, kind, target, url_key = comp
    t0 = time.perf_counter()
    ok, detail = False, ""
    try:
        if kind == "http":
            r = await C.http.get(target, timeout=2.0)
            ok = r.status_code < 500 and not (cid == "loki" and r.status_code != 200)
            detail = f"HTTP {r.status_code}"
        elif kind == "redis-ping":
            ok = await C.r.ping()
            detail = "PONG"
        elif kind == "redis":
            raw = await C.r.get(target)
            ok = raw is not None
            detail = "reporting" if ok else "no heartbeat"
    except Exception as exc:
        detail = type(exc).__name__
    return {"id": cid, "name": name, "section": section, "ok": bool(ok), "detail": detail,
            "latency_ms": round((time.perf_counter() - t0) * 1000, 1),
            "url": settings.PUBLIC_URLS.get(url_key) if url_key else None}


@router.get("/status", summary="Health of every lab component")
async def status():
    if time.time() - _status_cache["at"] < 2 and _status_cache["data"]:
        return _status_cache["data"]
    items = await asyncio.gather(*(_check(c) for c in COMPONENTS))
    data = {"components": items, "healthy": sum(i["ok"] for i in items), "total": len(items),
            "served_by": settings.INSTANCE, "checked_at": iso(now_ms())}
    _status_cache.update(at=time.time(), data=data)
    return data


@router.get("/links")
async def links():
    return settings.PUBLIC_URLS


async def _prom(query: str) -> float | None:
    try:
        r = await C.http.get(f"{settings.PROMETHEUS_URL}/api/v1/query", params={"query": query}, timeout=2)
        res = r.json()["data"]["result"]
        return round(float(res[0]["value"][1]), 2) if res else 0.0
    except Exception:
        return None


@router.get("/overview", summary="One call with everything the home page needs")
async def overview():
    queries = {
        "produced_eps": "sum(rate(swma_producer_events_total[30s]))",
        "processed_eps": 'sum(rate(swma_processor_events_total{outcome!="invalid"}[30s]))',
        "dlq_eps": "sum(rate(swma_processor_dlq_total[30s]))",
        "emitted_per_s": "sum(rate(swma_window_emissions_total[30s]))",
        "p95_latency_s": "histogram_quantile(0.95, sum(rate(swma_processing_latency_seconds_bucket[1m])) by (le))",
        "api_rps": "sum(rate(swma_api_requests_total[30s]))",
    }
    vals = await asyncio.gather(*(_prom(q) for q in queries.values()))
    producer = await C.r.get("lab:producer")
    sup = await C.r.get("lab:supervisor")
    workers = await _worker_statuses()
    return {
        "rates": dict(zip(queries, vals)),
        "producer": json.loads(producer) if producer else None,
        "supervisor": json.loads(sup) if sup else None,
        "workers": workers,
        "config": C.cfg.snapshot(),
        "config_version": C.cfg.version,
        "served_by": settings.INSTANCE,
    }


async def _worker_statuses() -> list[dict]:
    keys = sorted([k async for k in C.r.scan_iter("lab:worker:*")])
    out = []
    for k in keys:
        raw = await C.r.get(k)
        if raw:
            out.append(json.loads(raw))
    return out


# ------------------------------------------------------------ Kafka topology
def _topology() -> dict:
    stale = False
    md = C.admin.list_topics(timeout=5)
    brokers = [{"id": b.id, "host": b.host, "port": b.port, "controller": b.id == md.controller_id}
               for b in sorted(md.brokers.values(), key=lambda b: b.id)]
    topics = []
    with C.consumer_lock:
        for name in (settings.TOPIC_EVENTS, settings.TOPIC_DLQ, settings.TOPIC_METRICS):
            t = md.topics.get(name)
            if t is None:
                continue
            parts = []
            for pid, p in sorted(t.partitions.items()):
                try:
                    lo, hi = C.consumer.get_watermark_offsets(TopicPartition(name, pid), timeout=2, cached=False)
                except Exception:
                    lo = hi = None
                parts.append({"partition": pid, "leader": p.leader, "replicas": list(p.replicas),
                              "isr": list(p.isrs), "low": lo, "high": hi,
                              "under_replicated": len(p.isrs) < len(p.replicas)})
            topics.append({"name": name, "partitions": parts,
                           "messages": sum((x["high"] or 0) - (x["low"] or 0) for x in parts)})
        tps = [TopicPartition(settings.TOPIC_EVENTS, p) for p in range(settings.EVENT_PARTITIONS)]
        try:
            committed = {tp.partition: tp.offset for tp in C.consumer.committed(tps, timeout=5)}
        except Exception:
            committed = {}
            stale = True
    group = {"group": settings.CONSUMER_GROUP, "state": "unknown", "members": []}
    try:
        desc = C.admin.describe_consumer_groups([settings.CONSUMER_GROUP])[settings.CONSUMER_GROUP].result(timeout=5)
        group["state"] = str(desc.state).split(".")[-1]
        group["coordinator"] = desc.coordinator.id if desc.coordinator else None
        group["protocol"] = desc.partition_assignor
        for m in desc.members:
            group["members"].append({"client_id": m.client_id, "host": m.host, "member_id": m.member_id,
                                     "partitions": sorted(tp.partition for tp in m.assignment.topic_partitions)})
        group["members"].sort(key=lambda m: m["client_id"])
    except Exception as exc:
        group["error"] = str(exc)
        stale = True
    if stale:
        C.reset_kafka()  # next call reconnects to the current coordinator
    events = next((t for t in topics if t["name"] == settings.TOPIC_EVENTS), None)
    lag = {}
    if events:
        for p in events["partitions"]:
            c = committed.get(p["partition"], -1)
            lag[p["partition"]] = {"committed": c if c >= 0 else None,
                                   "lag": (p["high"] - (c if c >= 0 else p["low"])) if p["high"] is not None else None}
    group["lag"] = lag
    group["total_lag"] = sum(v["lag"] or 0 for v in lag.values())
    return {"brokers": brokers, "controller": md.controller_id, "topics": topics, "group": group}


@router.get("/topology", summary="Brokers, topics, partitions, replicas, consumer group and lag")
async def topology():
    try:
        return await run_in_threadpool(_topology)
    except Exception as exc:
        raise HTTPException(503, f"Kafka not reachable: {exc}")


# ----------------------------------------------------------------- processor
@router.get("/processor", summary="Workers, partitions, watermarks and state")
async def processor():
    try:
        sup = (await C.http.get(f"{settings.SUPERVISOR_URL}/workers", timeout=2)).json()
    except Exception:
        raw = await C.r.get("lab:supervisor")
        sup = json.loads(raw) if raw else None
    workers = await _worker_statuses()
    active = []
    for p in range(settings.EVENT_PARTITIONS):
        raw = await C.r.get(f"lab:active:{p}")
        if raw:
            active.extend(json.loads(raw))
    return {"supervisor": sup, "workers": workers, "active_windows": active, "now": iso(now_ms())}


async def _supervisor_post(path: str) -> dict:
    try:
        r = await C.http.post(f"{settings.SUPERVISOR_URL}{path}", timeout=10)
        return r.json()
    except Exception as exc:
        raise HTTPException(503, f"processor supervisor not reachable: {exc}")


@router.post("/processor/workers/{worker}/crash", summary="Kill a worker with SIGKILL (a real crash)")
async def crash_worker(worker: str, user=Depends(admin)):
    audit(log, "crash_worker", user=user["sub"], worker=worker)
    res = await _supervisor_post(f"/workers/{worker}/crash")
    if res.get("ok"):
        await asyncio.to_thread(labbus.emit, C.rs, "chaos", f"{user['sub']} crashed {worker} (SIGKILL, pid {res.get('pid')}). "
                                "No checkpoint was written, so events since the last checkpoint will be replayed.",
                                worker=worker)
    return res


@router.post("/processor/workers/{worker}/restart", summary="Graceful restart (checkpoint, then leave)")
async def restart_worker(worker: str, user=Depends(admin)):
    audit(log, "restart_worker", user=user["sub"], worker=worker)
    return await _supervisor_post(f"/workers/{worker}/restart")


@router.post("/processor/disaster", summary="Delete all local RocksDB state (restore from S3)")
async def disaster(user=Depends(admin)):
    audit(log, "disaster", user=user["sub"])
    return await _supervisor_post("/disaster")


# ------------------------------------------------------------ control plane
@router.get("/config", summary="Control Plane: every setting with its current value")
async def get_config():
    vals = C.cfg.snapshot()
    return {"version": C.cfg.version, "connected": C.cfg.connected,
            "settings": [{"key": s.key, "value": vals.get(s.key), "default": s.default, "kind": s.kind,
                          "group": s.group, "label": s.label, "help": s.help, "min": s.min, "max": s.max,
                          "options": list(s.options), "consul_key": f"swma/config/{s.key}"} for s in SPECS]}


async def _apply_changes(changes: dict, user: str, source: str) -> dict:
    current = C.cfg.snapshot()
    applied = {}
    for key, value in changes.items():
        if key not in SPEC_BY_KEY:
            raise HTTPException(400, f"Unknown setting {key}")
        try:
            new = coerce(key, value)
        except (ValueError, TypeError) as exc:
            raise HTTPException(400, str(exc))
        if new != current.get(key):
            await asyncio.to_thread(put, key, new)
            applied[key] = {"from": current.get(key), "to": new}
    if applied:
        entry = {"at": iso(now_ms()), "user": user, "source": source, "changes": applied}
        await C.r.lpush("lab:audit", json.dumps(entry))
        await C.r.ltrim("lab:audit", 0, 199)
        audit(log, "config_change", user=user, changes=applied)
        text = ", ".join(f"{k} {v['from']} -> {v['to']}" for k, v in applied.items())
        await asyncio.to_thread(labbus.emit, C.rs, "config", f"Config pushed to Consul by {user}: {text}",
                                changes=applied)
    return {"applied": applied}


@router.put("/config", summary="Change settings (pushed live to every service through Consul)")
async def put_config(changes: dict = Body(...), user=Depends(admin)):
    return await _apply_changes(changes, user["sub"], "admin-ui")


@router.post("/config/reset", summary="Restore every default")
async def reset_config(user=Depends(admin)):
    return await _apply_changes({s.key: s.default for s in SPECS}, user["sub"], "reset")


@router.get("/audit")
async def audit_log(limit: int = 50):
    return [json.loads(x) for x in await C.r.lrange("lab:audit", 0, limit - 1)]


@router.get("/producer")
async def producer_status():
    raw = await C.r.get("lab:producer")
    return json.loads(raw) if raw else {"running": False, "error": "producer not reporting"}


# ------------------------------------------------------------- custom events
class CustomEvent(BaseModel):
    service: str = Field("payment-service", description="Which microservice sent it")
    response_time_ms: float = Field(120, ge=0)
    status_code: int = 200
    delay_s: float = Field(0, ge=0, le=3600, description="Event happened this many seconds ago")
    count: int = Field(1, ge=1, le=50)
    event_id: str | None = Field(None, description="Reuse an id to create a duplicate")
    malformed: str | None = Field(None, description="broken_json | missing_field | wrong_type | bad_time")
    trace: bool = True


def _publish(payloads: list[tuple[str, bytes, str]]) -> list[dict]:
    results: dict[str, dict] = {}

    def cb(err, msg, eid):
        results[eid] = {"error": str(err)} if err else {"partition": msg.partition(), "offset": msg.offset()}

    for key, value, eid in payloads:
        C.producer.produce(settings.TOPIC_EVENTS, key=key.encode(), value=value,
                           on_delivery=lambda err, msg, eid=eid: cb(err, msg, eid))
    C.producer.flush(10)
    return [{"event_id": eid, **results.get(eid, {"error": "no delivery report"})} for _, _, eid in payloads]


@router.post("/events", summary="Send your own events into the pipeline")
async def send_events(body: CustomEvent):
    c = C.cfg.snapshot()
    wall = now_ms()
    payloads, docs = [], []
    for i in range(body.count):
        eid = body.event_id if (body.event_id and body.count == 1) else f"lab-{uuid.uuid4().hex[:12]}"
        ts = wall - int(body.delay_s * 1000)
        ev = {"event_id": eid, "service": body.service, "event_time": iso(ts), "endpoint": "/lab",
              "method": "POST", "response_time_ms": body.response_time_ms, "status_code": body.status_code,
              "cpu_percent": 50.0, "memory_percent": 60.0, "region": "ap-south-1", "source": "virtual-lab",
              "trace": body.trace}
        raw: bytes
        if body.malformed == "broken_json":
            raw = json.dumps(ev).encode()[:-15]
        else:
            if body.malformed == "missing_field":
                ev.pop("response_time_ms")
            elif body.malformed == "wrong_type":
                ev["status_code"] = "OK"
            elif body.malformed == "bad_time":
                ev["event_time"] = "last tuesday"
            raw = json.dumps(ev).encode()
        payloads.append((body.service, raw, eid))
        docs.append((eid, ts, ev, raw))
    results = await run_in_threadpool(_publish, payloads)
    # ground truth for the correctness check (only events that should be counted)
    size = c["window/tumbling_size_s"] * 1000
    pipe = C.r.pipeline(transaction=False)
    for (eid, ts, ev, raw), res in zip(docs, results):
        windows = [{"window_type": "tumbling", "window_id": window_id(s, e)} for s, e in tumbling(ts, size)]
        windows += [{"window_type": "sliding", "window_id": window_id(s, e)} for s, e in
                    sliding(ts, c["window/sliding_size_s"] * 1000, c["window/sliding_slide_s"] * 1000)]
        pipe.hset(f"trace:{eid}", mapping={
            "published_at": iso(wall), "published_by": settings.INSTANCE, "event": raw.decode("utf-8", "replace"),
            "key": body.service, "topic": settings.TOPIC_EVENTS, "event_time": iso(ts),
            "kafka_partition": res.get("partition", ""), "kafka_offset": res.get("offset", ""),
            "expected_windows": json.dumps(windows)})
        pipe.expire(f"trace:{eid}", 3600)
        start = ts - ts % size
        wm_guess = wall - c["watermark/max_out_of_orderness_s"] * 1000
        # The first copy of an id counts; a repeat is a duplicate and must not.
        first_copy = await C.r.set(f"lab:seen-id:{eid}", 1, nx=True, ex=3600)
        counted = (not body.malformed and first_copy
                   and start + size + c["window/allowed_lateness_s"] * 1000 > wm_guess)
        if counted:
            pipe.incr(f"truth:{c['window/tumbling_size_s']}:{body.service}:{start}")
    await pipe.execute()
    await asyncio.to_thread(labbus.emit, C.rs, "custom_event", f"You sent {body.count} event(s) for {body.service}"
                            + (f" ({body.malformed})" if body.malformed else "")
                            + (f", {body.delay_s:g}s in the past" if body.delay_s else ""))
    return {"events": results}


@router.get("/trace/{event_id}", summary="Follow one event through every component")
async def trace(event_id: str):
    data = await C.r.hgetall(f"trace:{event_id}")
    if not data:
        raise HTTPException(404, "No trace for this event id (traces last one hour)")
    windows = json.loads(data.get("windows") or "[]") or []
    service = None
    try:
        service = json.loads(data.get("event", "{}")).get("service")
    except Exception:
        pass
    results = []
    if service:
        for w in windows:
            raw = await C.r.hget(f"metrics:{w['window_type']}:{w['window_id']}", service)
            results.append({**w, "redis_key": f"metrics:{w['window_type']}:{w['window_id']}",
                            "result": json.loads(raw) if raw else None})
    return {"event_id": event_id, "trace": data, "service": service, "window_results": results}


@router.get("/windows", summary="Recent window results across services")
async def windows(window_type: str = "tumbling", service: str | None = None, limit: int = 20):
    services = [service] if service else list(SERVICE_CATALOG)
    out = []
    for svc in services:
        out.extend(await history(svc, window_type, limit))
    out.sort(key=lambda w: (w["start_ms"], w["service"]), reverse=True)
    return out[: limit * (1 if service else 3)]


@router.get("/correctness", summary="Processor results vs. ground truth from the producer")
async def correctness(limit: int = 6):
    size_s = C.cfg.get("window/tumbling_size_s")
    rows = []
    for svc in SERVICE_CATALOG:
        for w in await history(svc, "tumbling", limit + 4):
            if w["status"] != "final" or (w["end_ms"] - w["start_ms"]) != size_s * 1000:
                continue
            truth = await C.r.get(f"truth:{size_s}:{svc}:{w['start_ms']}")
            if truth is None:
                continue
            rows.append({"service": svc, "window_id": w["window_id"], "window_start": w["window_start"],
                         "computed": w["count"], "expected": int(truth), "match": w["count"] == int(truth)})
    rows.sort(key=lambda r: (r["window_start"], r["service"]), reverse=True)
    rows = rows[: limit * 7]
    return {"rows": rows, "checked": len(rows), "matched": sum(r["match"] for r in rows)}


def _tail(topic: str, limit: int) -> list[dict]:
    from confluent_kafka import Consumer
    c = Consumer({"bootstrap.servers": settings.KAFKA_BOOTSTRAP, "group.id": f"lab-peek-{uuid.uuid4().hex[:6]}",
                  "enable.auto.commit": False, "client.id": "lab-peek"})
    try:
        md = c.list_topics(topic, timeout=5)
        tps = []
        for p in md.topics[topic].partitions:
            lo, hi = c.get_watermark_offsets(TopicPartition(topic, p), timeout=3)
            tps.append(TopicPartition(topic, p, max(lo, hi - limit)))
        c.assign(tps)
        out, deadline = [], time.time() + 3
        while time.time() < deadline:
            msgs = c.consume(200, 0.5)
            if not msgs and out:
                break
            for m in msgs:
                if m.error():
                    continue
                out.append(_msg(m))
        out.sort(key=lambda x: x["timestamp"] or 0, reverse=True)
        return out[:limit]
    finally:
        c.close()


def _msg(m) -> dict:
    raw = (m.value() or b"").decode("utf-8", "replace")
    try:
        value = json.loads(raw)
    except Exception:
        value = raw[:2000]
    return {"topic": m.topic(), "partition": m.partition(), "offset": m.offset(),
            "key": (m.key() or b"").decode("utf-8", "replace"), "timestamp": m.timestamp()[1], "value": value}


@router.get("/dlq", summary="Latest dead-letter records")
async def dlq(limit: int = 20):
    return await run_in_threadpool(_tail, settings.TOPIC_DLQ, min(limit, 100))


@router.get("/messages", summary="Latest messages of a topic")
async def messages(topic: str = settings.TOPIC_EVENTS, limit: int = 20):
    if topic not in (settings.TOPIC_EVENTS, settings.TOPIC_DLQ, settings.TOPIC_METRICS):
        raise HTTPException(400, "unknown topic")
    return await run_in_threadpool(_tail, topic, min(limit, 100))


@router.get("/timeline", summary="What just happened, in plain English")
async def timeline(limit: int = 80):
    items = await C.r.xrevrange(labbus.TIMELINE, count=limit)
    return [json.loads(fields["data"]) for _, fields in items]


# --------------------------------------------------------------------- alerts
@router.get("/alerts")
async def alerts():
    active = []
    try:
        r = await C.http.get(f"{settings.ALERTMANAGER_URL}/api/v2/alerts", timeout=3)
        active = r.json()
    except Exception:
        pass
    history_ = [json.loads(x) for x in await C.r.lrange("lab:alerts", 0, 49)]
    return {"active": active, "history": history_}


# ------------------------------------------------- ops agent: chaos and CI/CD
async def _ops(method: str, path: str, **kw):
    try:
        r = await C.http.request(method, f"{settings.OPS_AGENT_URL}{path}",
                                 headers={"X-Ops-Token": settings.OPS_AGENT_TOKEN}, timeout=30, **kw)
    except Exception as exc:
        raise HTTPException(503, f"ops agent not reachable: {exc}")
    if r.status_code >= 400:
        raise HTTPException(r.status_code, r.json().get("detail", r.text))
    return r.json()


@router.get("/containers", summary="Every lab container with CPU / memory")
async def containers():
    return await _ops("GET", "/containers")


@router.post("/chaos/{target}/{action}", summary="Stop / start / pause a container")
async def chaos(target: str, action: str, user=Depends(admin)):
    audit(log, "chaos", user=user["sub"], target=target, operation=action)
    res = await _ops("POST", f"/containers/{target}/{action}")
    await asyncio.to_thread(labbus.emit, C.rs, "chaos", f"{user['sub']}: {action} {target}", target=target,
                            action=action)
    return res


@router.post("/ci/run", summary="Run the CI/CD pipeline (tests, scan, build, push, deploy)")
async def ci_run(user=Depends(admin)):
    audit(log, "ci_run", user=user["sub"])
    return await _ops("POST", "/ci/run", json={"user": user["sub"]})


@router.get("/ci/latest")
async def ci_latest():
    return await _ops("GET", "/ci/latest")


alerts_router = APIRouter(tags=["alerts"])


@alerts_router.post("/api/alerts/webhook", summary="Alertmanager webhook receiver")
async def alert_webhook(payload: dict = Body(...)):
    for a in payload.get("alerts", []):
        item = {"status": a.get("status"), "name": a.get("labels", {}).get("alertname"),
                "labels": a.get("labels", {}), "summary": a.get("annotations", {}).get("summary"),
                "description": a.get("annotations", {}).get("description"), "starts_at": a.get("startsAt"),
                "received_at": iso(now_ms())}
        await C.r.lpush("lab:alerts", json.dumps(item))
        verb = "FIRING" if item["status"] == "firing" else "resolved"
        await asyncio.to_thread(labbus.emit, C.rs, "alert", f"Alert {verb}: {item['name']} - {item['summary'] or ''}",
                                alert=item)
    await C.r.ltrim("lab:alerts", 0, 199)
    return {"ok": True}
