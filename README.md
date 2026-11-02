# StreamLab: Streaming Window Metrics Aggregator (Virtual Lab)

A complete, working implementation of the ACC project *Streaming Window Metrics Aggregator*,
packaged as a **virtual lab** that someone with zero background can use to learn real-time stream
processing. Every block of the architecture diagram runs as a real container on your machine. A web
portal teaches the theory, guides you through 14 experiments, and lets you look inside every service:
Kafka, the processor, Redis, Consul, Grafana, Loki, S3, the registry and more.

## Start it

**Windows:** install and start **Docker Desktop**, then double-click **`START-LAB.bat`**.
**macOS / Linux:** `./start-lab.sh`.

The browser opens at **http://localhost:8080**. The first start builds images and takes 5–10 minutes;
later starts take about a minute. To stop, double-click `STOP-LAB.bat` (or run `docker compose down`).
Your data is kept between runs. `docker compose down -v` deletes it.

Needs about **3 GB of free RAM** for Docker (23 containers).

## What's inside the portal (http://localhost:8080)

| Page | What you do there |
|---|---|
| Start here | Live KPIs, health of all 23 components, start/pause the stream |
| Architecture map | The diagram, clickable: what each block does, its containers, live numbers |
| Theory | 11 zero-knowledge lessons with in-browser simulations (partitions, consumer groups, windows, aggregation, watermarks) |
| Experiments | 14 guided labs (Aim, Theory, Procedure with auto-checked steps, Quiz, Result) |
| Send & trace an event | Build your own event and follow it through Kafka → worker → windows → Redis → API |
| Live dashboard | Per-service window metrics pushed over a WebSocket, tumbling/sliding views, history |
| Kafka stream | Live messages per partition for every topic, brokers, leaders, replicas, ISR, consumer lag |
| Stream processor | Pipeline stages, workers (crash/restart buttons), partitions, watermarks, window state |
| Redis & API | Browse Redis keys, call the REST API, see which replica answered |
| Control plane | Every setting (Consul KV), applied live; audit log; service registry |
| Observability | Embedded Grafana, PromQL, Loki logs, alerts |
| CI/CD & registry | One click runs the real pipeline: tests → bandit → docker build → push → rolling deploy |
| AWS deployment | Local ↔ AWS mapping, VPC and security groups, Terraform |
| Fault tolerance | Correctness vs ground truth, lag/autoscaling, S3 backups, chaos controls |
| All tools & UIs | Links to every service's own UI, container list with CPU/memory |

Admin actions (crash a worker, change config, run CI) ask for the demo sign-in **admin / streamlab**.
The public API key is **lab-public-key**.

## The 14 experiments

1. Follow one event through the whole system
2. Watch the stream inside Kafka (hot partition / data skew)
3. Tumbling vs sliding windows (change sizes live)
4. Out-of-order and late events (watermark, allowed lateness, DLQ)
5. Duplicates and invalid events (dedup, schema validation)
6. Crash a worker: rebalance and exactly-once state recovery
7. Backpressure and auto scaling
8. Kill a Kafka broker: replication and leader election
9. Change the system live from the control plane (UI + CLI)
10. API keys, JWT, rate limiting and load balancing
11. Make an alert fire (latency degradation)
12. Run the CI/CD pipeline
13. Disaster recovery from S3
14. Transient failures and retries

## Architecture → containers

| # | Diagram block | Container(s) | Own UI |
|---|---|---|---|
| 1 | Event sources | `producer` (7 simulated e-commerce microservices), custom events from the lab | – |
| 2 | Message broker | `redpanda-0/1/2` (3 brokers, RF 3, schema registry), `redpanda-console` | http://localhost:8081 |
| 3 | Stream processing engine | `processor`: supervisor + 1–4 worker processes, RocksDB state per partition | – |
| 4 | Storage & serving | `redis`, `redisinsight`, `api-1`, `api-2` (FastAPI, REST + WebSocket, JWT/API key, rate limit) | http://localhost:5540, http://localhost:8080/api/docs |
| 5 | Dashboard | `dashboard` (the portal) | http://localhost:8080 |
| 6 | Control plane | `consul` (KV config + service registry), `python -m swma.cli` | http://localhost:8500 |
| 7 | DevOps & CI/CD | `ops-agent` (pipeline runner, chaos, container stats), `.github/workflows` | – |
| 8 | Container registry | `registry`, `registry-ui` | http://localhost:5080 |
| 9 | AWS deployment | `alb` (nginx = ALB), `rustfs` (= S3), `deploy/aws/terraform` | http://localhost:9001/rustfs/console/ (streamlab / streamlab-secret) |
| 10 | Observability | `prometheus`, `alertmanager`, `grafana`, `loki` (chunks in S3), `redis-exporter` | http://localhost:3001 (admin / streamlab), http://localhost:9091, http://localhost:9093 |
| 11 | Resilience | autoscaler, checkpoints, replication, DLQ, retries, graceful shutdown | – |
| 12 | Key metrics | count, sum, avg, min, max, error rate, throughput, latency, CPU, memory | – |

Kafka is also reachable from your own code at `localhost:29092,localhost:29093,localhost:29094`.
All ports bind to 127.0.0.1 only.

## How the processing works

* Events are keyed by `service`, so a service's events always land on the same partition, and one
  worker owns that service's window state.
* Window assignment: tumbling (60 s) and sliding (60 s / 10 s slide), half-open `[start, end)`.
* Watermark per partition = newest event time − 5 s; idle partitions follow the wall clock.
* Window states move `open` → `closed` (watermark ≥ end) → `updated` (late event within the
  10 s allowed lateness) → `final` (watermark ≥ end + lateness). Events for final windows go to `telemetry.dlq`.
* Incremental aggregation: count, sum, min, max, errors, CPU, memory; average on emission.
* Every 5 s, window state, dedup ids and the next Kafka offset are written to the partition's RocksDB
  in one atomic batch, then the offset is committed. After a crash the new owner restores that
  checkpoint and replays from its offset, so each event is counted exactly once **in the state**.
  Output to Redis is idempotent (overwrite). The `metrics.windows` topic is at-least-once.
* Every 30 s a full snapshot also goes to S3, which is used when the local state is lost.
* The producer records ground-truth counts, and the lab compares them with the computed final windows.

## Tests

```bash
# unit tests (windows, aggregation, watermark, out-of-order, late, duplicates, crash recovery)
docker compose exec -T api-1 python -m pytest tests/unit -q
# end-to-end against the running stack
docker compose exec -T -e STREAMLAB_E2E=1 api-1 python -m pytest tests/e2e -q
```

## Project layout

```
docker-compose.yml        all 23 services
services/                 Python code (one Dockerfile, one target per component)
  swma/engine/            pure windowing / watermark / aggregation logic (unit tested)
  swma/processor/         supervisor, workers, RocksDB state store, metrics
  swma/producer/          telemetry simulator
  swma/api/               FastAPI: public API, lab API, explorers, WebSockets
  swma/opsagent/          CI/CD runner, chaos actions, container stats
  swma/bootstrap.py       creates topics, schema, Consul keys/services, S3 buckets
portal/                   the virtual lab web app (plain HTML/CSS/JS, no build step)
infra/                    nginx ALB, Prometheus + alert rules, Alertmanager, Loki, Grafana, Consul, Console
deploy/aws/terraform/     VPC, security group, S3, ECR, EC2 with user-data
.github/workflows/        ci.yml and cd.yml
tools/gen_dashboards.py   regenerates the Grafana dashboards
```

## Troubleshooting

* `docker compose ps` shows what is running; `docker compose logs -f processor` shows a service's logs.
* The header shows "x/23 healthy". Hover it to see which component is down.
* Port already in use: change the left side of the `ports:` entry in `docker-compose.yml`.
* The `ops-agent` mounts the Docker socket so the lab can stop/start containers and build images.
  Keep this lab on your own machine; don't expose it to a network.
