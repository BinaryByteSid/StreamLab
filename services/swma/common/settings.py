"""Static settings that come from environment variables (set in docker-compose.yml).

Anything a learner may want to change *while the lab runs* lives in the
Control Plane (Consul KV) instead -- see swma/common/config.py.
"""
import os
import socket


def _env(name: str, default: str) -> str:
    return os.getenv(name, default)


KAFKA_BOOTSTRAP = _env("KAFKA_BOOTSTRAP", "redpanda-0:9092,redpanda-1:9092,redpanda-2:9092")
SCHEMA_REGISTRY_URL = _env("SCHEMA_REGISTRY_URL", "http://redpanda-0:8081")
REDPANDA_ADMIN_URL = _env("REDPANDA_ADMIN_URL", "http://redpanda-0:9644")
REDIS_URL = _env("REDIS_URL", "redis://redis:6379/0")
CONSUL_URL = _env("CONSUL_URL", "http://consul:8500")
LOKI_URL = _env("LOKI_URL", "http://loki:3100")
PROMETHEUS_URL = _env("PROMETHEUS_URL", "http://prometheus:9090")
ALERTMANAGER_URL = _env("ALERTMANAGER_URL", "http://alertmanager:9093")
GRAFANA_URL = _env("GRAFANA_URL", "http://grafana:3000")
REGISTRY_URL = _env("REGISTRY_URL", "http://registry:5000")
OPS_AGENT_URL = _env("OPS_AGENT_URL", "http://ops-agent:8095")
OPS_AGENT_TOKEN = _env("OPS_AGENT_TOKEN", "streamlab-internal")
SUPERVISOR_URL = _env("SUPERVISOR_URL", "http://processor:8090")

S3_ENDPOINT = _env("S3_ENDPOINT", "rustfs:9000")
S3_ACCESS_KEY = _env("S3_ACCESS_KEY", "streamlab")
S3_SECRET_KEY = _env("S3_SECRET_KEY", "streamlab-secret")
BUCKET_CHECKPOINTS = "swma-checkpoints"
BUCKET_ARCHIVE = "swma-archive"
BUCKET_LOGS = "swma-logs"

TOPIC_EVENTS = _env("TOPIC_EVENTS", "telemetry.events")
TOPIC_DLQ = _env("TOPIC_DLQ", "telemetry.dlq")
TOPIC_METRICS = _env("TOPIC_METRICS", "metrics.windows")
EVENT_PARTITIONS = int(_env("EVENT_PARTITIONS", "4"))
REPLICATION_FACTOR = int(_env("REPLICATION_FACTOR", "3"))
CONSUMER_GROUP = _env("CONSUMER_GROUP", "swma-processors")
SCHEMA_SUBJECT = f"{TOPIC_EVENTS}-value"

STATE_DIR = _env("STATE_DIR", "/state")
MAX_WORKERS = int(_env("MAX_WORKERS", "4"))

SERVICE_NAME = _env("SWMA_SERVICE", "swma")
INSTANCE = _env("SWMA_INSTANCE", socket.gethostname())

# Demo credentials for the local lab (override them in .env).
LAB_ADMIN_USER = _env("LAB_ADMIN_USER", "admin")
LAB_ADMIN_PASSWORD = _env("LAB_ADMIN_PASSWORD", "streamlab")
LAB_API_KEYS = [k.strip() for k in _env("LAB_API_KEYS", "lab-public-key").split(",") if k.strip()]
JWT_SECRET = _env("JWT_SECRET", "change-me-local-lab-only")

# Addresses the *browser* uses to open each tool's own UI.
PUBLIC_URLS = {
    "lab": _env("PUBLIC_LAB_URL", "http://localhost:8080"),
    "console": _env("PUBLIC_CONSOLE_URL", "http://localhost:8081"),
    "grafana": _env("PUBLIC_GRAFANA_URL", "http://localhost:3001"),
    "prometheus": _env("PUBLIC_PROMETHEUS_URL", "http://localhost:9091"),
    "alertmanager": _env("PUBLIC_ALERTMANAGER_URL", "http://localhost:9093"),
    "consul": _env("PUBLIC_CONSUL_URL", "http://localhost:8500"),
    "redisinsight": _env("PUBLIC_REDISINSIGHT_URL", "http://localhost:5540"),
    "s3": _env("PUBLIC_S3_CONSOLE_URL", "http://localhost:9001/rustfs/console/"),
    "registry": _env("PUBLIC_REGISTRY_UI_URL", "http://localhost:5080"),
    "loki": _env("PUBLIC_LOKI_URL", "http://localhost:3001/explore"),
    "api_docs": _env("PUBLIC_API_DOCS_URL", "http://localhost:8080/api/docs"),
}
