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

Credentials come from the EC2 instance role; nothing is stored in files.
"""
from __future__ import annotations

import json
import math
import os
import signal
import time
import uuid

import boto3
import httpx
from confluent_kafka import Consumer

from swma.common import log as logsetup, settings

NAMESPACE = os.getenv("CW_NAMESPACE", "StreamLab")
LOG_GROUP = os.getenv("CW_LOG_GROUP", "/streamlab/stream")
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
        self.ensure_streams()
        self.consumer.subscribe([settings.TOPIC_EVENTS, settings.TOPIC_METRICS])
        last_metrics = last_flush = last_raw = 0.0
        self.log.info("publishing to CloudWatch namespace %s and log group %s", NAMESPACE, LOG_GROUP)
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
                self.buffers[stream].append({"timestamp": int(now * 1000), "message": msg})
            now = time.time()
            try:
                if now - last_flush >= 5:
                    self.flush_logs()
                    last_flush = now
                if now - last_metrics >= 60:
                    self.publish_metrics()
                    last_metrics = now
            except Exception as exc:  # throttling or a transient AWS error: try again next round
                self.log.warning("cloudwatch publish failed: %s", exc)
        self.flush_logs()
        self.consumer.close()


if __name__ == "__main__":
    Bridge().run()
