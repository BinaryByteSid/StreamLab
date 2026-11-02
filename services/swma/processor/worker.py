"""One stream-processor worker (a member of the Kafka consumer group).

Pipeline per event (diagram section 3):
    Deserialize & Parse -> Schema Validation -> Extract Event Time ->
    Watermark -> Event Router & Enrichment -> Window Assignment ->
    Stateful Aggregation -> Window Trigger & Emission -> Redis / Kafka / S3
"""
from __future__ import annotations

import io
import json
import os
import random
import signal
import threading
import time

import redis
from confluent_kafka import OFFSET_STORED, Consumer, KafkaError, KafkaException, Producer, TopicPartition
from minio import Minio

from swma.common import labbus, log as logsetup, settings
from swma.common.config import LiveConfig
from swma.common.schema import SERVICE_CATALOG, Validator, fetch_registry_schema, iso, now_ms, parse_event_time
from swma.engine.engine import EngineConfig, PartitionEngine

from . import metrics as M
from .statestore import StateStore

FUTURE_LIMIT_MS = 5 * 60 * 1000

# Writes "latest" pointers only if the window is not older than the stored one.
SET_IF_NEWER = """
local cur = redis.call('HGET', KEYS[1], 'start_ms')
if (not cur) or tonumber(ARGV[1]) >= tonumber(cur) then
  redis.call('HSET', KEYS[1], 'start_ms', ARGV[1], 'json', ARGV[2])
  redis.call('EXPIRE', KEYS[1], ARGV[3])
  return 1
end
return 0
"""


class Worker:
    def __init__(self, worker_id: str) -> None:
        self.id = worker_id
        settings.INSTANCE = worker_id
        self.log = logsetup.setup("processor")
        self.stop = False
        self.cfg = LiveConfig().start()
        self.applied_version = -1
        self.ecfg = EngineConfig.from_live(self.cfg.snapshot())
        self.r = redis.Redis.from_url(settings.REDIS_URL, decode_responses=True, socket_timeout=3)
        self.set_if_newer = self.r.register_script(SET_IF_NEWER)
        self.s3 = Minio(settings.S3_ENDPOINT, settings.S3_ACCESS_KEY, settings.S3_SECRET_KEY, secure=False)
        schema, version = fetch_registry_schema(settings.SCHEMA_REGISTRY_URL, settings.SCHEMA_SUBJECT)
        self.validator = Validator(schema)
        self.schema_version = version
        self.engines: dict[int, PartitionEngine] = {}
        self.stores: dict[int, StateStore] = {}
        self.restored_from: dict[int, str] = {}
        self.pending_sink: list[dict] = []
        self.archive: list[dict] = []
        self.processed = 0
        self.started = time.time()
        self.eps = 0.0
        self._eps_mark = (time.time(), 0)
        self.producer = Producer({
            "bootstrap.servers": settings.KAFKA_BOOTSTRAP,
            "client.id": f"{worker_id}-emitter",
            "acks": "all",
            "linger.ms": 20,
            "enable.idempotence": True,
        })
        self.consumer = Consumer({
            "bootstrap.servers": settings.KAFKA_BOOTSTRAP,
            "group.id": settings.CONSUMER_GROUP,
            "client.id": worker_id,
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
            "session.timeout.ms": 6000,
            "heartbeat.interval.ms": 1500,
            "max.poll.interval.ms": 120000,
            "partition.assignment.strategy": "roundrobin",
        })

    # ------------------------------------------------------------- rebalance
    def on_assign(self, consumer: Consumer, partitions: list[TopicPartition]) -> None:
        wall = now_ms()
        for tp in partitions:
            p = tp.partition
            store = StateStore(settings.STATE_DIR, p)
            eng = PartitionEngine(p, self.ecfg)
            source, windows = "empty", 0
            if not store.is_empty():
                windows = eng.restore(store.items(), wall)
                source = "rocksdb"
            else:
                snap = self._s3_restore(p)
                if snap:
                    windows = eng.restore(snap["items"].items(), wall)
                    store.replace_all(snap["items"])
                    source = "s3"
            tp.offset = eng.next_offset if eng.next_offset is not None else OFFSET_STORED
            self.engines[p], self.stores[p], self.restored_from[p] = eng, store, source
            M.RESTORES.labels(source).inc()
            M.OWNER.labels(str(p), self.id).set(1)
            seen = self.r.get(f"lab:partition:{p}:processed")
            replay = None
            if seen and eng.next_offset is not None:
                replay = max(0, int(seen) - eng.next_offset)
            where = {"rocksdb": "its RocksDB checkpoint", "s3": "the S3 backup",
                     "empty": "nothing (fresh start; reading from the committed Kafka offset)"}[source]
            msg = f"{self.id} took partition {p}: restored {windows} windows from {where}"
            if eng.next_offset is not None:
                msg += f", resuming at offset {eng.next_offset}"
            if replay:
                msg += f" ({replay} events will be replayed)"
            labbus.emit(self.r, "assign", msg, worker=self.id, partition=p, source=source,
                        offset=eng.next_offset, windows=windows, replay=replay)
            self.log.info(msg)
        M.REBALANCES.labels(self.id, "assign").inc()
        consumer.assign(partitions)

    def on_revoke(self, consumer: Consumer, partitions: list[TopicPartition]) -> None:
        mine = [tp.partition for tp in partitions if tp.partition in self.engines]
        if mine:
            self.checkpoint(sync=True, only=mine)
        for p in mine:
            self._release(p)
        if mine:
            labbus.emit(self.r, "revoke", f"{self.id} released partitions {mine} (state checkpointed first)",
                        worker=self.id, partitions=mine)
        M.REBALANCES.labels(self.id, "revoke").inc()

    def on_lost(self, consumer: Consumer, partitions: list[TopicPartition]) -> None:
        lost = [tp.partition for tp in partitions if tp.partition in self.engines]
        for p in lost:
            self._release(p)
        labbus.emit(self.r, "lost", f"{self.id} lost partitions {lost} without a checkpoint", worker=self.id)

    def _release(self, p: int) -> None:
        self.engines.pop(p, None)
        store = self.stores.pop(p, None)
        if store:
            store.close()
        M.OWNER.labels(str(p), self.id).set(0)

    def _s3_restore(self, p: int) -> dict | None:
        try:
            resp = self.s3.get_object(settings.BUCKET_CHECKPOINTS, f"partition-{p}/latest.json")
            try:
                return json.loads(resp.read())
            finally:
                resp.close()
                resp.release_conn()
        except Exception:
            return None

    # --------------------------------------------------------------- events
    def handle(self, m) -> None:
        if m.error():
            if m.error().code() != KafkaError._PARTITION_EOF:
                self.log.warning("kafka error: %s", m.error())
            return
        p = m.partition()
        eng = self.engines.get(p)
        if eng is None:
            return
        raw = m.value() or b""
        wall = now_ms()
        M.STAGE.labels("received").inc()
        try:
            ev = json.loads(raw)
            if not isinstance(ev, dict):
                raise ValueError("event is not a JSON object")
        except Exception:
            return self.dlq(m, None, "deserialize_error", "payload is not a JSON object")
        M.STAGE.labels("deserialized").inc()

        if self.cfg.get("features/schema_validation"):
            errs = self.validator.errors(ev)
            if errs:
                return self.dlq(m, ev, "schema_invalid", errs)
        M.STAGE.labels("validated").inc()

        try:
            ts = parse_event_time(ev["event_time"])
        except Exception as exc:
            return self.dlq(m, ev, "bad_event_time", str(exc))
        if ts > wall + FUTURE_LIMIT_MS:
            return self.dlq(m, ev, "future_event_time", "event_time is more than 5 minutes ahead")
        M.STAGE.labels("event_time").inc()

        if self.cfg.get("features/enrichment"):
            cat = SERVICE_CATALOG.get(ev["service"], {})
            ev["team"] = cat.get("team", "unknown")
            ev["tier"] = cat.get("tier", "unknown")
        M.STAGE.labels("routed").inc()

        work = self.cfg.get("processor/simulated_work_ms")
        if work:
            time.sleep(work / 1000)

        out = eng.process(ev, ts, wall)
        eng.next_offset = m.offset() + 1
        M.EVENTS.labels(self.id, out.kind).inc()
        if out.kind == "too_late":
            self.dlq(m, ev, "too_late", {
                "event_time": ev.get("event_time"), "watermark": iso(out.watermark) if out.watermark else None,
                "allowed_lateness_s": self.ecfg.lateness_ms // 1000,
                "closed_windows": [w["window_id"] for w in out.dropped][:7]}, counted=True)
        elif out.kind != "duplicate":
            M.STAGE.labels("windowed").inc()
            M.STAGE.labels("aggregated").inc()
        ts_type, kafka_ts = m.timestamp()
        if kafka_ts > 0:
            M.LATENCY.observe(max(0, wall - kafka_ts) / 1000)
        self.processed += 1
        if ev.get("trace"):
            self.trace(ev, m, out.kind, out.windows, out.watermark, None)

    def dlq(self, m, ev, reason: str, detail, counted: bool = False) -> None:
        raw = (m.value() or b"").decode("utf-8", "replace")
        record = {
            "reason": reason, "detail": detail, "worker": self.id, "failed_at": iso(now_ms()),
            "source_topic": m.topic(), "source_partition": m.partition(), "source_offset": m.offset(),
            "original": raw[:4000],
        }
        self.producer.produce(settings.TOPIC_DLQ, key=m.key(), value=json.dumps(record))
        M.DLQ.labels(reason).inc()
        if not counted:
            M.EVENTS.labels(self.id, "invalid").inc()
        eng = self.engines.get(m.partition())
        if eng is not None:
            eng.next_offset = m.offset() + 1
        self.processed += 1
        if isinstance(ev, dict) and ev.get("trace") and ev.get("event_id"):
            self.trace(ev, m, "dead_letter", [], None, reason)

    def trace(self, ev, m, outcome, windows, wm, dlq_reason) -> None:
        eid = str(ev.get("event_id"))
        data = {
            "processed_at": iso(now_ms()), "worker": self.id, "partition": m.partition(), "offset": m.offset(),
            "outcome": outcome, "windows": json.dumps(windows), "watermark": iso(wm) if wm else "",
            "dlq_reason": dlq_reason or "", "team": ev.get("team", ""), "tier": ev.get("tier", ""),
        }
        try:
            self.r.hset(f"trace:{eid}", mapping=data)
            self.r.expire(f"trace:{eid}", 3600)
            labbus.emit(self.r, "trace", f"Traced event {eid}: {outcome} on partition {m.partition()}",
                        event_id=eid, outcome=outcome)
        except Exception:
            pass

    # ------------------------------------------------------- trigger & sinks
    def tick(self) -> None:
        wall = now_ms()
        emissions = list(self.pending_sink)
        self.pending_sink.clear()
        for eng in self.engines.values():
            emissions.extend(eng.tick(wall))
        if emissions:
            self.sink(emissions)

    def sink(self, emissions: list[dict]) -> None:
        ttl = int(self.cfg.get("state/redis_ttl_s"))
        fail_pct = self.cfg.get("processor/sink_failure_pct")
        for attempt in range(5):
            try:
                if fail_pct and random.random() * 100 < fail_pct:
                    raise ConnectionError("injected transient Redis failure")
                pipe = self.r.pipeline(transaction=False)
                for e in emissions:
                    js = json.dumps(e)
                    wt, wid, svc, start = e["window_type"], e["window_id"], e["service"], e["start_ms"]
                    k = f"metrics:{wt}:{wid}"
                    pipe.hset(k, svc, js)
                    pipe.expire(k, ttl)
                    hist = f"metrics:history:{wt}:{svc}"
                    pipe.zadd(hist, {wid: start})
                    pipe.zremrangebyrank(hist, 0, -301)
                    pipe.expire(hist, ttl)
                    idx = f"metrics:windows:{wt}"
                    pipe.zadd(idx, {wid: start})
                    pipe.zremrangebyrank(idx, 0, -301)
                    self.set_if_newer(keys=[f"metrics:latest:{wt}:{svc}"], args=[start, js, ttl], client=pipe)
                    if e["status"] != "open":
                        self.set_if_newer(keys=[f"metrics:last_closed:{wt}:{svc}"], args=[start, js, ttl], client=pipe)
                pipe.publish("swma:updates", json.dumps({"type": "windows", "items": emissions}))
                pipe.execute()
                break
            except Exception as exc:
                M.SINK_RETRIES.inc()
                if attempt == 4:
                    M.SINK_FAILURES.inc()
                    self.log.error("redis sink failed after retries: %s", exc)
                    self.pending_sink = emissions[-5000:]
                    return
                time.sleep(0.1 * 2 ** attempt)
        for e in emissions:
            M.EMISSIONS.labels(e["window_type"], e["status"]).inc()
            self.producer.produce(settings.TOPIC_METRICS, key=e["service"], value=json.dumps(e))
            if e["status"] in ("closed", "updated"):
                lbl = (e["service"], e["window_type"])
                if e["avg"] is not None:
                    M.W_AVG.labels(*lbl).set(e["avg"])
                    M.W_MAX.labels(*lbl).set(e["max"])
                    M.W_MIN.labels(*lbl).set(e["min"])
                M.W_ERR.labels(*lbl).set(e["error_rate"])
                M.W_COUNT.labels(*lbl).set(e["count"])
                M.W_TPUT.labels(*lbl).set(e["throughput_eps"] or 0)
                if e["cpu_avg"] is not None:
                    M.W_CPU.labels(*lbl).set(e["cpu_avg"])
            if e["status"] == "final":
                self.archive.append(e)
        M.STAGE.labels("emitted").inc(len(emissions))

    # ------------------------------------------------- checkpoint & backups
    def checkpoint(self, sync: bool = False, only: list[int] | None = None) -> None:
        self.producer.flush(5)  # DLQ + results must be durable before offsets move
        offsets = []
        for p in (only if only is not None else list(self.engines)):
            eng, store = self.engines.get(p), self.stores.get(p)
            if eng is None or store is None:
                continue
            t0 = time.time()
            puts, deletes = eng.drain_changes()
            store.write(puts, deletes)
            M.CHECKPOINT_SECONDS.observe(time.time() - t0)
            if eng.next_offset is not None:
                offsets.append(TopicPartition(settings.TOPIC_EVENTS, p, eng.next_offset))
        if offsets:
            try:
                self.consumer.commit(offsets=offsets, asynchronous=not sync)
            except KafkaException as exc:
                self.log.warning("offset commit failed: %s", exc)
        M.CHECKPOINTS.labels(self.id).inc()

    def backup_to_s3(self) -> None:
        snaps = []
        for p, eng in self.engines.items():
            snap = eng.full_snapshot()
            snap.update(worker=self.id, taken_at=iso(now_ms()))
            snaps.append((p, json.dumps(snap).encode()))

        def upload():
            for p, data in snaps:
                try:
                    for key in (f"partition-{p}/latest.json", f"partition-{p}/history/{int(time.time())}.json"):
                        self.s3.put_object(settings.BUCKET_CHECKPOINTS, key, io.BytesIO(data), len(data),
                                           content_type="application/json")
                    M.S3_BACKUPS.labels("ok").inc()
                except Exception as exc:
                    M.S3_BACKUPS.labels("error").inc()
                    self.log.warning("s3 backup failed for partition %s: %s", p, exc)

        threading.Thread(target=upload, daemon=True).start()

    def flush_archive(self) -> None:
        if not self.archive:
            return
        batch, self.archive = self.archive, []
        body = "\n".join(json.dumps(e) for e in batch).encode()
        t = time.gmtime()
        key = (f"windows/{time.strftime('%Y-%m-%d', t)}/{time.strftime('%H', t)}/"
               f"{self.id}-{int(time.time() * 1000)}.jsonl")
        try:
            self.s3.put_object(settings.BUCKET_ARCHIVE, key, io.BytesIO(body), len(body),
                               content_type="application/x-ndjson")
        except Exception as exc:
            self.log.warning("archive upload failed: %s", exc)

    # ---------------------------------------------------------------- status
    def report(self, with_size: bool) -> None:
        wall = now_ms()
        now = time.time()
        t0, n0 = self._eps_mark
        if now - t0 >= 2:
            self.eps = (self.processed - n0) / (now - t0)
            self._eps_mark = (now, self.processed)
        parts = []
        pipe = self.r.pipeline(transaction=False)
        for p, eng in sorted(self.engines.items()):
            d = eng.describe(wall)
            try:
                lo, hi = self.consumer.get_watermark_offsets(TopicPartition(settings.TOPIC_EVENTS, p),
                                                             timeout=1, cached=False)
            except KafkaException:
                lo, hi = 0, 0
            pos = eng.next_offset if eng.next_offset is not None else lo
            d["high_watermark"], d["lag"] = hi, max(0, hi - pos)
            store = self.stores[p]
            d["restored_from"] = self.restored_from.get(p)
            d["checkpoints"] = store.checkpoints
            d["last_checkpoint"] = iso(int(store.last_checkpoint * 1000)) if store.last_checkpoint else None
            if with_size:
                d["state_bytes"] = store.size_bytes()
                M.STATE_BYTES.labels(str(p)).set(d["state_bytes"])
            parts.append(d)
            M.PARTITION_LAG.labels(str(p)).set(d["lag"])
            if d["watermark_delay_s"] is not None:
                M.WATERMARK_DELAY.labels(str(p)).set(d["watermark_delay_s"])
            M.STATE_WINDOWS.labels(str(p), "open").set(d["open_windows"])
            M.STATE_WINDOWS.labels(str(p), "closed").set(d["closed_windows"])
            M.STATE_DEDUP.labels(str(p)).set(d["dedup_keys"])
            if eng.next_offset is not None:
                pipe.set(f"lab:partition:{p}:processed", eng.next_offset, ex=3600)
            pipe.set(f"lab:active:{p}", json.dumps(eng.active_windows(wall)), ex=5)
        status = {
            "worker": self.id, "pid": os.getpid(), "started_at": iso(int(self.started * 1000)),
            "partitions": parts, "events_per_sec": round(self.eps, 1), "processed": self.processed,
            "config_version": self.cfg.version, "schema_source": self.validator.source,
            "schema_version": self.schema_version, "simulated_work_ms": self.cfg.get("processor/simulated_work_ms"),
        }
        pipe.set(f"lab:worker:{self.id}", json.dumps(status), ex=5)
        try:
            pipe.execute()
        except Exception:
            pass

    # ------------------------------------------------------------------ loop
    def apply_config(self) -> None:
        if self.cfg.version == self.applied_version:
            return
        self.applied_version = self.cfg.version
        self.ecfg = EngineConfig.from_live(self.cfg.snapshot())
        for eng in self.engines.values():
            eng.update_config(self.ecfg)

    def run(self) -> None:
        signal.signal(signal.SIGTERM, lambda *_: setattr(self, "stop", True))
        signal.signal(signal.SIGINT, lambda *_: setattr(self, "stop", True))
        self.consumer.subscribe([settings.TOPIC_EVENTS], on_assign=self.on_assign,
                                on_revoke=self.on_revoke, on_lost=self.on_lost)
        labbus.emit(self.r, "worker_start", f"{self.id} started (pid {os.getpid()}) and joined consumer group "
                    f"'{settings.CONSUMER_GROUP}'", worker=self.id)
        last = {"tick": 0.0, "ckpt": time.time(), "status": 0.0, "size": 0.0, "s3": time.time(), "arch": time.time()}
        while not self.stop:
            self.apply_config()
            work = self.cfg.get("processor/simulated_work_ms")
            batch_size = 500 if not work else max(1, min(500, int(800 / work)))
            for m in self.consumer.consume(num_messages=batch_size, timeout=0.2):
                self.handle(m)
            self.producer.poll(0)
            now = time.time()
            if now - last["tick"] >= 0.5:
                self.tick()
                last["tick"] = now
            if now - last["ckpt"] >= self.cfg.get("state/checkpoint_interval_s"):
                self.checkpoint()
                last["ckpt"] = now
            if now - last["status"] >= 1:
                with_size = now - last["size"] >= 5
                self.report(with_size)
                last["status"] = now
                if with_size:
                    last["size"] = now
            if now - last["s3"] >= self.cfg.get("state/s3_backup_interval_s"):
                self.backup_to_s3()
                last["s3"] = now
            if now - last["arch"] >= 30:
                self.flush_archive()
                last["arch"] = now
        self.shutdown()

    def shutdown(self) -> None:
        """Graceful shutdown: finish in-flight work, checkpoint, leave the group."""
        self.log.info("%s shutting down gracefully", self.id)
        try:
            self.tick()
            self.checkpoint(sync=True)
            self.flush_archive()
        finally:
            parts = sorted(self.engines)
            self.consumer.close()  # leaves the group -> immediate rebalance
            self.producer.flush(5)
            for store in self.stores.values():
                store.close()
            self.r.delete(f"lab:worker:{self.id}")
            labbus.emit(self.r, "worker_stop", f"{self.id} stopped gracefully: checkpointed partitions {parts} "
                        "and left the group", worker=self.id)


def run(worker_id: str) -> None:
    Worker(worker_id).run()
