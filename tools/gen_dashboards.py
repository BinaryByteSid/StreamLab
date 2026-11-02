"""Generates the Grafana dashboards in infra/grafana/dashboards/.

    python tools/gen_dashboards.py
"""
import json
import pathlib

OUT = pathlib.Path(__file__).resolve().parent.parent / "infra" / "grafana" / "dashboards"
PROM = {"type": "prometheus", "uid": "prometheus"}
LOKI = {"type": "loki", "uid": "loki"}


class Board:
    def __init__(self, uid, title, description):
        self.uid, self.title, self.description = uid, title, description
        self.panels, self.y, self.x, self.row_h, self.id = [], 0, 0, 0, 1

    def _place(self, w, h):
        if self.x + w > 24:
            self.x, self.y = 0, self.y + self.row_h
            self.row_h = 0
        pos = {"x": self.x, "y": self.y, "w": w, "h": h}
        self.x += w
        self.row_h = max(self.row_h, h)
        return pos

    def row(self, title):
        self.x, self.y = 0, self.y + self.row_h
        self.row_h = 0
        self.panels.append({"type": "row", "title": title, "id": self.id, "collapsed": False,
                            "gridPos": {"x": 0, "y": self.y, "w": 24, "h": 1}})
        self.id += 1
        self.y += 1

    def add(self, kind, title, targets, w=8, h=7, unit=None, desc="", ds=PROM, opts=None, field=None):
        p = {"type": kind, "title": title, "id": self.id, "datasource": ds, "description": desc,
             "gridPos": self._place(w, h),
             "targets": [{"refId": chr(65 + i), "datasource": ds, "expr": e, "legendFormat": l}
                         for i, (e, l) in enumerate(targets)],
             "fieldConfig": {"defaults": {"unit": unit or "short", **(field or {})}, "overrides": []},
             "options": opts or {}}
        if kind == "timeseries":
            p["fieldConfig"]["defaults"]["custom"] = {"lineWidth": 2, "fillOpacity": 12, "showPoints": "never"}
            p["options"] = {"legend": {"displayMode": "list", "placement": "bottom"}, "tooltip": {"mode": "multi"},
                            **(opts or {})}
        self.panels.append(p)
        self.id += 1

    def save(self, name):
        doc = {"uid": self.uid, "title": self.title, "description": self.description, "tags": ["streamlab"],
               "timezone": "browser", "refresh": "5s", "time": {"from": "now-15m", "to": "now"},
               "schemaVersion": 39, "editable": True, "panels": self.panels}
        (OUT / name).write_text(json.dumps(doc, indent=1))


def stat(b, title, expr, unit=None, desc="", w=4, thresholds=None):
    field = {}
    if thresholds:
        field["thresholds"] = {"mode": "absolute", "steps": thresholds}
        field["color"] = {"mode": "thresholds"}
    b.add("stat", title, [(expr, "")], w=w, h=4, unit=unit, desc=desc, field=field,
          opts={"colorMode": "background" if thresholds else "value", "graphMode": "area",
                "reduceOptions": {"calcs": ["lastNotNull"]}})


OK_WARN_BAD = lambda warn, bad: [{"color": "green", "value": None}, {"color": "orange", "value": warn},
                                 {"color": "red", "value": bad}]


def overview():
    b = Board("streamlab-pipeline", "StreamLab - 1. Pipeline Overview",
              "Is the streaming pipeline healthy? Events in, events out, lag, latency, workers.")
    b.row("Right now")
    stat(b, "Events produced / s", "sum(rate(swma_producer_events_total[30s]))", "short", "Events the simulated microservices send to Kafka")
    stat(b, "Events processed / s", 'sum(rate(swma_processor_events_total{outcome!="invalid"}[30s]))', "short")
    stat(b, "Consumer lag", "swma_group_lag_total", "short", "Events in Kafka not yet processed", 4, OK_WARN_BAD(200, 1000))
    stat(b, "Workers alive", "swma_workers_alive", "short", "Processor worker processes running")
    stat(b, "p95 processing latency", "histogram_quantile(0.95, sum(rate(swma_processing_latency_seconds_bucket[1m])) by (le))",
         "s", "Kafka append -> processed", 4, OK_WARN_BAD(0.5, 2))
    stat(b, "Dead letters / s", "sum(rate(swma_processor_dlq_total[1m]))", "short", "", 4, OK_WARN_BAD(0.1, 1))
    b.row("Flow")
    b.add("timeseries", "Throughput: in vs out", [
        ("sum(rate(swma_producer_events_total[30s]))", "produced"),
        ('sum(rate(swma_processor_events_total{outcome!="invalid"}[30s]))', "processed"),
        ("sum(rate(swma_processor_dlq_total[30s]))", "dead letters")], w=12, unit="short")
    b.add("timeseries", "Consumer lag per partition (committed)", [("swma_group_lag", "partition {{partition}}")],
          w=12, desc="Rises when processors can't keep up (backpressure).")
    b.add("timeseries", "Event outcomes", [("sum by (outcome) (rate(swma_processor_events_total[30s]))", "{{outcome}}")],
          w=8, desc="on_time, out_of_order, late_accepted, too_late, duplicate, invalid")
    b.add("timeseries", "Pipeline stages / s", [("sum by (stage) (rate(swma_processor_stage_total[30s]))", "{{stage}}")], w=8)
    b.add("timeseries", "Window results emitted / s", [
        ("sum by (status) (rate(swma_window_emissions_total[30s]))", "{{status}}")], w=8)
    b.row("Processor internals")
    b.add("timeseries", "Workers: desired vs alive", [("swma_workers_desired", "desired"), ("swma_workers_alive", "alive")],
          w=8, desc="The autoscaler changes 'desired'. A crash drops 'alive'.")
    b.add("timeseries", "Watermark delay per partition", [("swma_partition_watermark_delay_seconds", "p{{partition}}")],
          w=8, unit="s", desc="Wall clock minus watermark = out-of-orderness bound + idle time")
    b.add("timeseries", "Processing latency percentiles", [
        ("histogram_quantile(0.5, sum(rate(swma_processing_latency_seconds_bucket[1m])) by (le))", "p50"),
        ("histogram_quantile(0.95, sum(rate(swma_processing_latency_seconds_bucket[1m])) by (le))", "p95"),
        ("histogram_quantile(0.99, sum(rate(swma_processing_latency_seconds_bucket[1m])) by (le))", "p99")], w=8, unit="s")
    b.add("timeseries", "Windows in state", [("sum by (phase) (swma_state_windows)", "{{phase}}")], w=6)
    b.add("timeseries", "Checkpoints / min", [("sum(rate(swma_checkpoints_total[1m]))*60", "checkpoints"),
                                              ("sum(rate(swma_s3_backups_total[5m]))*60", "S3 backups")], w=6)
    b.add("timeseries", "State restores", [("sum by (source) (increase(swma_state_restores_total[5m]))", "{{source}}")], w=6,
          desc="rocksdb = local checkpoint, s3 = disaster recovery")
    b.add("timeseries", "Retries & failures", [("sum(rate(swma_sink_retries_total[1m]))", "Redis retries"),
                                               ("sum(rate(swma_sink_failures_total[1m]))", "Redis failures")], w=6)
    b.save("pipeline.json")


def services():
    b = Board("streamlab-services", "StreamLab - 2. Microservice Metrics (window results)",
              "The business view: what the aggregator computed for each e-commerce service.")
    b.row("Latest 60s sliding window per service")
    b.add("bargauge", "Average latency", [('swma_window_avg_latency_ms{window_type="sliding"}', "{{service}}")], w=8, h=8,
          unit="ms", opts={"orientation": "horizontal", "displayMode": "gradient", "reduceOptions": {"calcs": ["lastNotNull"]}},
          field={"thresholds": {"mode": "absolute", "steps": OK_WARN_BAD(250, 500)}})
    b.add("bargauge", "Error rate", [('swma_window_error_rate{window_type="sliding"}', "{{service}}")], w=8, h=8,
          unit="percent", opts={"orientation": "horizontal", "displayMode": "gradient", "reduceOptions": {"calcs": ["lastNotNull"]}},
          field={"thresholds": {"mode": "absolute", "steps": OK_WARN_BAD(2, 5)}})
    b.add("bargauge", "Requests in window", [('swma_window_requests{window_type="sliding"}', "{{service}}")], w=8, h=8,
          opts={"orientation": "horizontal", "displayMode": "basic", "reduceOptions": {"calcs": ["lastNotNull"]}})
    b.row("Over time")
    b.add("timeseries", "Average latency (sliding 60s)", [('swma_window_avg_latency_ms{window_type="sliding"}', "{{service}}")],
          w=12, unit="ms")
    b.add("timeseries", "Error rate (sliding 60s)", [('swma_window_error_rate{window_type="sliding"}', "{{service}}")],
          w=12, unit="percent")
    b.add("timeseries", "Max latency (tumbling 1 min)", [('swma_window_max_latency_ms{window_type="tumbling"}', "{{service}}")],
          w=8, unit="ms")
    b.add("timeseries", "Throughput (tumbling)", [('swma_window_throughput_eps{window_type="tumbling"}', "{{service}}")],
          w=8, unit="reqps")
    b.add("timeseries", "CPU (sliding)", [('swma_window_cpu_avg{window_type="sliding"}', "{{service}}")], w=8, unit="percent")
    b.save("services.json")


def infra():
    b = Board("streamlab-infra", "StreamLab - 3. Kafka, Redis, API & Containers",
              "The infrastructure under the pipeline.")
    b.row("Kafka (Redpanda, 3 brokers)")
    stat(b, "Brokers up", 'sum(up{job="redpanda"})', w=4, thresholds=[{"color": "red", "value": None},
                                                                    {"color": "orange", "value": 2}, {"color": "green", "value": 3}])
    stat(b, "Under-replicated", "sum(redpanda_kafka_under_replicated_replicas)", w=4, thresholds=OK_WARN_BAD(1, 1))
    b.add("timeseries", "Broker bytes / s", [
        ('sum by (redpanda_request) (rate(redpanda_kafka_request_bytes_total[30s]))', "{{redpanda_request}}")],
        w=8, h=4, unit="Bps")
    b.add("timeseries", "Messages delivered per partition / s", [
        ("sum by (partition) (rate(swma_producer_delivered_total[30s]))", "partition {{partition}}")], w=8, h=4,
        desc="Key = service, so a hot service makes a hot partition")
    b.row("Redis")
    b.add("timeseries", "Redis commands / s", [("rate(redis_commands_processed_total[30s])", "commands")], w=8)
    b.add("timeseries", "Redis memory", [("redis_memory_used_bytes", "used")], w=8, unit="bytes")
    b.add("timeseries", "Redis keys", [("sum(redis_db_keys)", "keys")], w=8)
    b.row("FastAPI (2 replicas behind the load balancer)")
    b.add("timeseries", "Requests / s by instance", [("sum by (instance) (rate(swma_api_requests_total[30s]))", "{{instance}}")], w=8)
    b.add("timeseries", "Responses by status", [("sum by (status) (rate(swma_api_requests_total[30s]))", "{{status}}")], w=8)
    b.add("timeseries", "Rate limited (429) / s", [("sum(rate(swma_api_rate_limited_total[30s]))", "429s")], w=8)
    b.row("Containers")
    b.add("timeseries", "CPU % by container", [("swma_container_cpu_percent", "{{container}}")], w=12, unit="percent")
    b.add("timeseries", "Memory by container", [("swma_container_memory_bytes", "{{container}}")], w=12, unit="bytes")
    b.add("table", "Scrape targets", [("up", "")], w=24, h=8,
          opts={"showHeader": True}, desc="1 = Prometheus can scrape it")
    b.save("infra.json")


def logs():
    b = Board("streamlab-logs", "StreamLab - 4. Logs (Loki)", "Application, error and audit logs from every service.")
    b.add("timeseries", "Log lines / s by service",
          [('sum by (service) (rate({kind=~".+"}[1m]))', "{{service}}")], w=12, h=7, ds=LOKI)
    b.add("timeseries", "Errors / s", [('sum by (service) (rate({kind="error"}[1m]))', "{{service}}")], w=12, h=7, ds=LOKI)
    b.add("logs", "Audit log (who changed what)", [('{kind="audit"}', "")], w=24, h=9, ds=LOKI,
          opts={"showTime": True, "wrapLogMessage": True, "sortOrder": "Descending"})
    b.add("logs", "All logs", [('{kind=~".+"}', "")], w=24, h=14, ds=LOKI,
          opts={"showTime": True, "wrapLogMessage": True, "sortOrder": "Descending"})
    b.save("logs.json")


if __name__ == "__main__":
    OUT.mkdir(parents=True, exist_ok=True)
    overview()
    services()
    infra()
    logs()
    print("dashboards written to", OUT)
