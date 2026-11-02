"""Prometheus metrics for the processor (multi-process mode).

The supervisor and every worker process write to PROMETHEUS_MULTIPROC_DIR;
the supervisor serves the combined view on :9100/metrics.
"""
from prometheus_client import Counter, Gauge, Histogram

EVENTS = Counter("swma_processor_events_total", "Events by processing outcome", ["worker", "outcome"])
STAGE = Counter("swma_processor_stage_total", "Events that passed each pipeline stage", ["stage"])
DLQ = Counter("swma_processor_dlq_total", "Records sent to the dead letter queue", ["reason"])
LATENCY = Histogram("swma_processing_latency_seconds", "Kafka append time -> processed",
                    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 30, 60))
EMISSIONS = Counter("swma_window_emissions_total", "Window results emitted", ["window_type", "status"])
CHECKPOINTS = Counter("swma_checkpoints_total", "State checkpoints written", ["worker"])
CHECKPOINT_SECONDS = Histogram("swma_checkpoint_duration_seconds", "Checkpoint duration",
                               buckets=(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1))
RESTORES = Counter("swma_state_restores_total", "Partition state restores", ["source"])
REBALANCES = Counter("swma_rebalances_total", "Partition assignment changes", ["worker", "kind"])
SINK_RETRIES = Counter("swma_sink_retries_total", "Redis write retries")
SINK_FAILURES = Counter("swma_sink_failures_total", "Redis writes that failed after all retries")
S3_BACKUPS = Counter("swma_s3_backups_total", "State snapshots copied to S3", ["result"])

PARTITION_LAG = Gauge("swma_partition_lag", "Live lag seen by the owning worker", ["partition"],
                      multiprocess_mode="livemostrecent")
WATERMARK_DELAY = Gauge("swma_partition_watermark_delay_seconds", "Wall clock minus watermark", ["partition"],
                        multiprocess_mode="livemostrecent")
STATE_WINDOWS = Gauge("swma_state_windows", "Windows held in state", ["partition", "phase"],
                      multiprocess_mode="livemostrecent")
STATE_DEDUP = Gauge("swma_state_dedup_keys", "Event ids kept for duplicate detection", ["partition"],
                    multiprocess_mode="livemostrecent")
STATE_BYTES = Gauge("swma_state_bytes", "RocksDB size on disk", ["partition"], multiprocess_mode="livemostrecent")
OWNER = Gauge("swma_partition_owner", "1 when the worker owns the partition", ["partition", "worker"],
              multiprocess_mode="liveall")

W_AVG = Gauge("swma_window_avg_latency_ms", "Average latency of the latest closed window",
              ["service", "window_type"], multiprocess_mode="livemostrecent")
W_MAX = Gauge("swma_window_max_latency_ms", "Max latency of the latest closed window",
              ["service", "window_type"], multiprocess_mode="livemostrecent")
W_MIN = Gauge("swma_window_min_latency_ms", "Min latency of the latest closed window",
              ["service", "window_type"], multiprocess_mode="livemostrecent")
W_ERR = Gauge("swma_window_error_rate", "Error rate (%) of the latest closed window",
              ["service", "window_type"], multiprocess_mode="livemostrecent")
W_COUNT = Gauge("swma_window_requests", "Event count of the latest closed window",
                ["service", "window_type"], multiprocess_mode="livemostrecent")
W_TPUT = Gauge("swma_window_throughput_eps", "Throughput of the latest closed window",
               ["service", "window_type"], multiprocess_mode="livemostrecent")
W_CPU = Gauge("swma_window_cpu_avg", "Average CPU % of the latest closed window",
              ["service", "window_type"], multiprocess_mode="livemostrecent")

# --- supervisor -----------------------------------------------------------
WORKERS_DESIRED = Gauge("swma_workers_desired", "Workers the supervisor wants", multiprocess_mode="livemostrecent")
WORKERS_ALIVE = Gauge("swma_workers_alive", "Workers currently running", multiprocess_mode="livemostrecent")
WORKER_UP = Gauge("swma_worker_up", "1 if the worker process is alive", ["worker"], multiprocess_mode="livemostrecent")
WORKER_RESTARTS = Counter("swma_worker_restarts_total", "Worker restarts", ["worker", "reason"])
SCALE_ACTIONS = Counter("swma_autoscaler_actions_total", "Autoscaler decisions", ["direction"])
GROUP_LAG = Gauge("swma_group_lag", "Committed consumer-group lag", ["partition"], multiprocess_mode="livemostrecent")
GROUP_LAG_TOTAL = Gauge("swma_group_lag_total", "Committed consumer-group lag (sum)", multiprocess_mode="livemostrecent")
LIVE_LAG_TOTAL = Gauge("swma_live_lag_total", "Live lag (sum) used by the autoscaler", multiprocess_mode="livemostrecent")
CFG_VERSION = Gauge("swma_config_version", "Consul ModifyIndex of the applied config", multiprocess_mode="livemostrecent")
CFG_ALERT_LATENCY = Gauge("swma_config_alert_latency_ms", "Latency alert threshold", multiprocess_mode="livemostrecent")
CFG_ALERT_ERRORS = Gauge("swma_config_alert_error_rate_pct", "Error-rate alert threshold", multiprocess_mode="livemostrecent")
CFG_ALERT_LAG = Gauge("swma_config_alert_consumer_lag", "Lag alert threshold", multiprocess_mode="livemostrecent")
