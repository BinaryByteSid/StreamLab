"""Amazon CloudWatch bridge (runs only in the AWS deployment).

Makes the stream visible inside the AWS console, with no extra login:

* CloudWatch Metrics, namespace ``StreamLab`` -- every 60 s: events produced /
  processed per second, consumer lag, workers alive, dead letters, p95 latency,
  window results and API requests (read from Prometheus). 8 metrics, inside
  the free tier's 10.
* CloudWatch Logs, group ``/streamlab/stream`` -- two live streams you can
  follow with "Live Tail":
    raw-events      one sampled raw event per second from ``telemetry.events``
    window-results  every on-time window result from ``metrics.windows``

* Amazon S3 (``S3_STREAM_BUCKET``) -- every 60 s the same raw-event sample and
  window results are written as JSON-lines files under ``stream/``, so the
  stream also lands in S3 (useful where CloudWatch Logs is not allowed).

Each output can be switched off: ``CW_METRICS=off``, ``CW_LOGS=off``, empty
``S3_STREAM_BUCKET``. Credentials: the EC2 instance role, or ``AWS_ACCESS_KEY_ID``
/ ``AWS_SECRET_ACCESS_KEY`` in the environment (hybrid mode on a laptop).
"""
from __future__ import annotations

import json
import math
import os
import signal
import time
import uuid

import io

import boto3
import httpx
from confluent_kafka import Consumer

from swma.common import log as logsetup, settings
from swma.common.storage import s3_client

NAMESPACE = os.getenv("CW_NAMESPACE", "StreamLab")
LOG_GROUP = os.getenv("CW_LOG_GROUP", "/streamlab/stream")
CW_METRICS = os.getenv("CW_METRICS", "on") != "off"
CW_LOGS = os.getenv("CW_LOGS", "on") != "off"
S3_STREAM_BUCKET = os.getenv("S3_STREAM_BUCKET", "")
METRICS = {  # CloudWatch metric name -> (PromQL, unit)
    "EventsProducedPerSec": ("sum(rate(swma_producer_events_total[1m]))", "Count/Second"),
    "EventsProcessedPerSec": ('sum(rate(swma_processor_events_total{outcome!="invalid"}[1m]))', "Count/Second"),
    "ConsumerLag": ("swma_group_lag_total", "Count"),
    "WorkersAlive": ("swma_workers_alive", "Count"),
    "DeadLettersPerSec": ("sum(rate(swma_processor_dlq_total[1m]))", "Count/Second"),
    "P95ProcessingLatencyMs": (
        "1000 * histogram_quantile(0.95, sum(rate(swma_processing_latency_seconds_bucket[1m])) by (le))",
        "Milliseconds"),
    "WindowResultsPerSec": ("sum(rate(swma_window_emissions_total[1m]))", "Count/Second"),
    "ApiRequestsPerSec": ("sum(rate(swma_api_requests_total[1m]))", "Count/Second"),
}


class Bridge:
    def __init__(self) -> None:
        settings.INSTANCE = "cloudwatch-bridge"
        self.log = logsetup.setup("cloudwatch")
        region = os.getenv("AWS_REGION") or settings.S3_REGION
        self.cw = boto3.client("cloudwatch", region_name=region)
        self.logs = boto3.client("logs", region_name=region)
        self.stop = False
        self.buffers: dict[str, list] = {"raw-events": [], "window-results": []}
        self.s3_buffers: dict[str, list] = {"raw-events": [], "window-results": []}
        self.s3 = s3_client() if S3_STREAM_BUCKET else None
        self.consumer = Consumer({
            "bootstrap.servers": settings.KAFKA_BOOTSTRAP,
            "group.id": f"cloudwatch-bridge-{uuid.uuid4().hex[:6]}",  # never commits: a pure viewer
            "client.id": "cloudwatch-bridge",
            "auto.offset.reset": "latest",
            "enable.auto.commit": False,
        })

    def ensure_streams(self) -> None:
        for name in self.buffers:
            try:
                self.logs.create_log_stream(logGroupName=LOG_GROUP, logStreamName=name)
            except self.logs.exceptions.ResourceAlreadyExistsException:
                pass

    def publish_metrics(self) -> None:
        data = []
        for name, (query, unit) in METRICS.items():
            try:
                r = httpx.get(f"{settings.PROMETHEUS_URL}/api/v1/query", params={"query": query}, timeout=5)
                res = r.json()["data"]["result"]
                value = float(res[0]["value"][1]) if res else 0.0
            except Exception:
                continue
            if math.isfinite(value):
                data.append({"MetricName": name, "Value": value, "Unit": unit})
        if data:
            self.cw.put_metric_data(Namespace=NAMESPACE, MetricData=data)

    def flush_s3(self) -> None:
        t = time.gmtime()
        for stream, lines in self.s3_buffers.items():
            if not lines:
                continue
            batch, self.s3_buffers[stream] = lines, []
            body = ("\n".join(batch) + "\n").encode()
            key = (f"stream/{stream}/{time.strftime('%Y/%m/%d/%H', t)}/"
                   f"{time.strftime('%H%M%S', t)}-{uuid.uuid4().hex[:6]}.jsonl")
            self.s3.put_object(S3_STREAM_BUCKET, key, io.BytesIO(body), len(body),
                               content_type="application/x-ndjson")

    def flush_logs(self) -> None:
        for stream, events in self.buffers.items():
            if not events:
                continue
            batch, self.buffers[stream] = events[:5000], events[5000:]
            batch.sort(key=lambda e: e["timestamp"])
            try:
                self.logs.put_log_events(logGroupName=LOG_GROUP, logStreamName=stream, logEvents=batch)
            except self.logs.exceptions.ResourceNotFoundException:
                self.ensure_streams()

    def run(self) -> None:
        signal.signal(signal.SIGTERM, lambda *_: setattr(self, "stop", True))
        if CW_LOGS:
            self.ensure_streams()
        self.consumer.subscribe([settings.TOPIC_EVENTS, settings.TOPIC_METRICS])
        last_metrics = last_flush = last_raw = 0.0
        last_s3 = time.time()
        self.log.info("CloudWatch metrics=%s (namespace %s), CloudWatch logs=%s (group %s), S3 stream export=%s",
                      CW_METRICS, NAMESPACE, CW_LOGS, LOG_GROUP, S3_STREAM_BUCKET or "off")
        while not self.stop:
            for m in self.consumer.consume(200, 0.5):
                if m.error():
                    continue
                now = time.time()
                if m.topic() == settings.TOPIC_EVENTS:
                    if now - last_raw < 1:  # sample: one raw event per second
                        continue
                    last_raw = now
                    stream = "raw-events"
                    msg = f"partition={m.partition()} offset={m.offset()} {(m.value() or b'').decode('utf-8', 'replace')}"
                else:
                    try:
                        w = json.loads(m.value())
                    except Exception:
                        continue
                    if w.get("status") != "closed":  # one on-time result per window
                        continue
                    stream = "window-results"
                    msg = json.dumps({k: w.get(k) for k in (
                        "window_type", "service", "window_start", "window_end", "count", "avg", "min", "max",
                        "error_rate", "throughput_eps", "late_events")})
                if CW_LOGS:
                    self.buffers[stream].append({"timestamp": int(now * 1000), "message": msg})
                if self.s3 is not None:
                    self.s3_buffers[stream].append(json.dumps({"ts": int(now * 1000), "message": msg}))
            now = time.time()
            try:
                if CW_LOGS and now - last_flush >= 5:
                    self.flush_logs()
                    last_flush = now
                if self.s3 is not None and now - last_s3 >= 60:
                    last_s3 = now
                    self.flush_s3()
                if CW_METRICS and now - last_metrics >= 60:
                    self.publish_metrics()
                    last_metrics = now
            except Exception as exc:  # throttling or a transient AWS error: try again next round
                self.log.warning("cloudwatch publish failed: %s", exc)
        if CW_LOGS:
            self.flush_logs()
        if self.s3 is not None:
            self.flush_s3()
        self.consumer.close()


if __name__ == "__main__":
    Bridge().run()
