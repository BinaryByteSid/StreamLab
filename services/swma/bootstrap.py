"""One-shot setup job, run before the pipeline starts.

1. Kafka topics (partitions + replication factor 3)
2. JSON schema registered in the Redpanda Schema Registry
3. Control Plane defaults + service catalog in Consul KV
4. Service registry: every lab component registered in Consul with a health check
5. S3 buckets for checkpoints, the cold-path archive and Loki's log chunks
"""
from __future__ import annotations

import json
import sys
import time

import httpx
from confluent_kafka.admin import AdminClient, NewTopic
from minio import Minio

from swma.common import config, log as logsetup, settings
from swma.common.schema import EVENT_SCHEMA, SERVICE_CATALOG

log = logsetup.setup("bootstrap", loki=False)

TOPICS = [
    (settings.TOPIC_EVENTS, settings.EVENT_PARTITIONS, {"retention.ms": str(24 * 3600 * 1000)},
     "Raw telemetry events from the microservices"),
    (settings.TOPIC_DLQ, 2, {"retention.ms": str(7 * 24 * 3600 * 1000)},
     "Dead letter queue: invalid and too-late events"),
    (settings.TOPIC_METRICS, settings.EVENT_PARTITIONS, {"retention.ms": str(24 * 3600 * 1000)},
     "Aggregated window results emitted by the processor"),
]

# name, address, port, health check (http path or None for TCP), tags
SERVICES = [
    ("redpanda-0", "redpanda-0", 9092, "http://redpanda-0:9644/v1/status/ready", ["kafka", "broker", "data-plane"]),
    ("redpanda-1", "redpanda-1", 9092, "http://redpanda-1:9644/v1/status/ready", ["kafka", "broker", "data-plane"]),
    ("redpanda-2", "redpanda-2", 9092, "http://redpanda-2:9644/v1/status/ready", ["kafka", "broker", "data-plane"]),
    ("schema-registry", "redpanda-0", 8081, "http://redpanda-0:8081/subjects", ["kafka", "schema"]),
    ("redpanda-console", "redpanda-console", 8080, "http://redpanda-console:8080/admin/health", ["ui", "kafka"]),
    ("producer", "producer", 9102, "http://producer:9102/metrics", ["data-plane", "event-source"]),
    ("processor", "processor", 8090, "http://processor:8090/health", ["data-plane", "stream-processing"]),
    ("redis", "redis", 6379, None, ["storage", "serving"]),
    ("api-1", "api-1", 8000, "http://api-1:8000/api/health", ["api", "serving"]),
    ("api-2", "api-2", 8000, "http://api-2:8000/api/health", ["api", "serving"]),
    ("alb", "alb", 80, "http://alb/alb/health", ["load-balancer"]),
    ("dashboard", "dashboard", 80, "http://dashboard/", ["ui"]),
    ("prometheus", "prometheus", 9090, "http://prometheus:9090/-/ready", ["observability"]),
    ("alertmanager", "alertmanager", 9093, "http://alertmanager:9093/-/ready", ["observability"]),
    ("grafana", "grafana", 3000, "http://grafana:3000/api/health", ["observability", "ui"]),
    ("loki", "loki", 3100, "http://loki:3100/ready", ["observability", "logs"]),
    ("s3", "rustfs", 9000, None, ["storage", "s3"]),
    ("registry", "registry", 5000, "http://registry:5000/v2/", ["devops", "registry"]),
    ("redisinsight", "redisinsight", 5540, None, ["ui", "redis"]),
    ("ops-agent", "ops-agent", 8095, "http://ops-agent:8095/health", ["devops", "chaos"]),
]


def wait(what: str, fn, timeout: float = 240) -> None:
    deadline = time.time() + timeout
    while True:
        try:
            if fn():
                log.info("%s ready", what)
                return
        except Exception as exc:
            last = exc
        else:
            last = "not ready"
        if time.time() > deadline:
            log.error("%s not ready after %ss: %s", what, timeout, last)
            sys.exit(1)
        time.sleep(2)


def kafka() -> None:
    admin = AdminClient({"bootstrap.servers": settings.KAFKA_BOOTSTRAP})
    wait("kafka cluster (3 brokers)", lambda: len(admin.list_topics(timeout=5).brokers) >= settings.REPLICATION_FACTOR)
    existing = admin.list_topics(timeout=10).topics
    new = [NewTopic(name, num_partitions=parts, replication_factor=settings.REPLICATION_FACTOR, config=cfg)
           for name, parts, cfg, _ in TOPICS if name not in existing]
    for name, fut in (admin.create_topics(new).items() if new else []):
        try:
            fut.result()
            log.info("created topic %s", name)
        except Exception as exc:
            log.warning("topic %s: %s", name, exc)


def schema_registry() -> None:
    url = f"{settings.SCHEMA_REGISTRY_URL}/subjects/{settings.SCHEMA_SUBJECT}/versions"
    wait("schema registry", lambda: httpx.get(f"{settings.SCHEMA_REGISTRY_URL}/subjects", timeout=5).status_code == 200)
    r = httpx.post(url, json={"schemaType": "JSON", "schema": json.dumps(EVENT_SCHEMA)},
                   headers={"Content-Type": "application/vnd.schemaregistry.v1+json"}, timeout=10)
    log.info("schema registry: %s %s", r.status_code, r.text[:200])


def consul() -> None:
    wait("consul leader", lambda: httpx.get(f"{settings.CONSUL_URL}/v1/status/leader", timeout=5).text.strip('"'))
    created = config.seed_defaults()
    log.info("control plane: %s default keys created", created)
    httpx.put(f"{settings.CONSUL_URL}/v1/kv/swma/catalog/services",
              content=json.dumps({k: {"team": v["team"], "tier": v["tier"]} for k, v in SERVICE_CATALOG.items()},
                                 indent=2), timeout=5)
    httpx.put(f"{settings.CONSUL_URL}/v1/kv/swma/catalog/topics",
              content=json.dumps({n: {"partitions": p, "replication_factor": settings.REPLICATION_FACTOR,
                                      "description": d} for n, p, _, d in TOPICS}, indent=2), timeout=5)
    for name, addr, port, check, tags in SERVICES:
        body = {"ID": name, "Name": name, "Address": addr, "Port": port, "Tags": tags,
                "Meta": {"lab": "streamlab"},
                "Check": ({"HTTP": check, "Interval": "10s", "Timeout": "3s"} if check else
                          {"TCP": f"{addr}:{port}", "Interval": "10s", "Timeout": "3s"})}
        body["Check"]["DeregisterCriticalServiceAfter"] = "90m"
        httpx.put(f"{settings.CONSUL_URL}/v1/agent/service/register", json=body, timeout=5)
    log.info("registered %s services in the Consul service catalog", len(SERVICES))


def s3() -> None:
    client = Minio(settings.S3_ENDPOINT, settings.S3_ACCESS_KEY, settings.S3_SECRET_KEY, secure=False)
    wait("s3 (rustfs)", lambda: client.list_buckets() is not None)
    for bucket in (settings.BUCKET_CHECKPOINTS, settings.BUCKET_ARCHIVE, settings.BUCKET_LOGS):
        if not client.bucket_exists(bucket):
            client.make_bucket(bucket)
            log.info("created bucket %s", bucket)


def main() -> None:
    s3()
    consul()
    kafka()
    schema_registry()
    log.info("bootstrap complete")


if __name__ == "__main__":
    main()
