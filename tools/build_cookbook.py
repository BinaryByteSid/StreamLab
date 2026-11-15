"""Builds docs/StreamLab_Cookbook.html (then printed to PDF with Edge).

Every code excerpt is read from the real source files at build time, with its
file and line range, so the cookbook can never drift from the code.

    python tools/build_cookbook.py
"""
from __future__ import annotations

import ast
import html
import pathlib
import re

ROOT = pathlib.Path(__file__).resolve().parent.parent
OUT = ROOT / "docs" / "StreamLab_Cookbook.html"
TITLE = "StreamLab Virtual Lab - Architectural Blueprint &amp; Engineering Cookbook"


# ------------------------------------------------------------ code excerpts
def _read(rel: str) -> list[str]:
    return (ROOT / rel).read_text(encoding="utf-8").splitlines()


def code(rel: str, a: int, b: int, lang: str = "") -> str:
    lines = _read(rel)[a - 1:b]
    while lines and not lines[-1].strip():
        lines.pop()
        b -= 1
    return (f'<div class="src">Source: <code>{rel}</code> (lines {a}-{b})</div>'
            f'<pre class="code {lang}">{html.escape(chr(10).join(lines))}</pre>')


def fn(rel: str, qual: str, max_lines: int = 70) -> str:
    """Excerpt a function / method / class / constant by name ('Class.method')."""
    tree = ast.parse((ROOT / rel).read_text(encoding="utf-8"))
    parts = qual.split(".")
    nodes = tree.body
    node = None
    for part in parts:
        node = next(n for n in nodes if (
            isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)) and n.name == part) or (
            isinstance(n, ast.Assign) and any(getattr(t, "id", None) == part for t in n.targets)) or (
            isinstance(n, ast.AnnAssign) and getattr(n.target, "id", None) == part))
        nodes = getattr(node, "body", [])
    start = min([node.lineno] + [d.lineno for d in getattr(node, "decorator_list", [])])
    end = node.end_lineno
    if end - start + 1 > max_lines:
        return code(rel, start, start + max_lines - 1) + '<div class="src">… (excerpt)</div>'
    return code(rel, start, end)


def grep(rel: str, pattern: str, after: int, before: int = 0) -> str:
    lines = _read(rel)
    i = next(i for i, l in enumerate(lines) if re.search(pattern, l))
    return code(rel, i + 1 - before, i + 1 + after)


# ----------------------------------------------------------------- helpers
def table(head: list[str], rows: list[list[str]], cls: str = "") -> str:
    th = "".join(f"<th>{h}</th>" for h in head)
    tr = "".join("<tr>" + "".join(f"<td>{c}</td>" for c in r) + "</tr>" for r in rows)
    return f'<table class="{cls}"><thead><tr>{th}</tr></thead><tbody>{tr}</tbody></table>'


def pre(text: str) -> str:
    return f'<pre class="code">{html.escape(text.strip(chr(10)))}</pre>'


def note(text: str) -> str:
    return f'<div class="note">{text}</div>'


SECTIONS: list[tuple[str, str, str]] = []  # (anchor, title, html)


def section(anchor: str, title: str, body: str, toc: bool = True) -> None:
    SECTIONS.append((anchor if toc else "", title, f'<section id="{anchor}"><h1>{title}</h1>{body}</section>'))


# ===================================================================== content
section("philosophy", "1. Architecture &amp; System Design Philosophy", f"""
<h2>The core problem: an endless, unordered, unreliable stream</h2>
<p>Microservices emit telemetry forever. Events arrive late and out of order, are delivered at least once (so
duplicates happen), some are malformed, and any machine can crash at any moment. The aggregator must still
publish <b>correct</b> per-service metrics (count, sum, average, min, max, error rate, throughput) over
<b>tumbling</b> and <b>sliding</b> windows with low latency, and keep doing so while it scales and fails.</p>
<h2>Architectural invariants of this system</h2>
<ol>
<li><b>Event time, not arrival time.</b> Every event is placed in the windows of the moment it <i>happened</i>
(<code>event_time</code>). Arrival order never changes a result inside the out-of-orderness bound.</li>
<li><b>Key = service = partition owner.</b> The producer keys every event by <code>service</code>, so all events of a
service land on one Kafka partition and one worker owns that service's window state. No state is ever shared
between workers.</li>
<li><b>State and offset move together.</b> A checkpoint writes the window state, the duplicate-detection ids and the
next Kafka offset in <i>one atomic RocksDB batch</i>. After a crash the new owner restores that batch and replays from
that offset, so every event is counted <b>exactly once in the state</b> although Kafka delivers at least once.</li>
<li><b>Results are frozen, never silently rewritten.</b> A window becomes <code>final</code> when the watermark passes
<code>end + allowed lateness</code>; anything older goes to the dead letter queue with its reason, not into
published numbers.</li>
<li><b>Bad data is isolated, never fatal.</b> Deserialisation, schema, time and lateness failures are routed to
<code>telemetry.dlq</code> with the original payload; the pipeline keeps running.</li>
<li><b>Everything is observable and controllable at runtime.</b> Every component exports Prometheus metrics and
JSON logs; every tunable lives in Consul and is applied live; every notable moment is written to a human-readable
timeline the lab displays.</li>
</ol>
<h2>Stack</h2>
{table(["Layer (diagram §)", "Technology", "Role"], [
    ["1 Event sources", "Python producer", "7 simulated e-commerce microservices, scenarios, chaos injection"],
    ["2 Message broker", "Redpanda ×3 (Kafka API), Schema Registry, Console", "Durable partitioned log, RF 3"],
    ["3 Stream processing", "Python + confluent-kafka + RocksDB", "Supervisor + 1-4 workers, windows, watermark, state"],
    ["4 Storage &amp; serving", "Redis 7, FastAPI ×2", "Latest window results, REST + WebSocket, JWT / API keys"],
    ["5 Dashboard", "Static HTML/JS portal (nginx)", "Live dashboard + the virtual lab"],
    ["6 Control plane", "Consul", "Live configuration (KV) and service registry"],
    ["7-8 DevOps", "Ops agent, Docker Registry, GitHub Actions", "Tests → scan → build → push → rolling deploy"],
    ["9 Deployment", "nginx (ALB), RustFS/S3, Terraform", "Local stand-ins; AWS EC2 or hybrid S3 + CloudWatch"],
    ["10 Observability", "Prometheus, Grafana, Loki, Alertmanager", "Metrics, dashboards, logs, alerts"],
])}
""")

section("dataflow", "2. Data Flow &amp; Component Interactions", f"""
{pre('''
                               INGESTION
 [7 microservices / Event Lab / edge-case runner]
        |  JSON event, key = service  (acks=all, idempotent producer)
        v
 [Redpanda topic telemetry.events]   4 partitions x RF 3  (leader + 2 followers)
        |
        |  consumer group "swma-processors"  (1-4 workers, one owner per partition)
        v
 [Worker: per-event pipeline]
        |-- 1 Deserialize & parse ............. fail -> telemetry.dlq (deserialize_error)
        |-- 2 Schema validation (registry) .... fail -> telemetry.dlq (schema_invalid)
        |-- 3 Extract event time .............. fail -> telemetry.dlq (bad_event_time / future)
        |-- 4 Enrich (team, tier)
        |-- 5 Duplicate check (event_id) ...... seen -> ignored
        |-- 6 Watermark = max_event_time - bound   (idle + caught-up -> wall clock)
        |-- 7 Assign windows: 1 tumbling + size/slide sliding
        |       '-- window already final? ..... -> telemetry.dlq (too_late)
        '-- 8 Incremental aggregate (count, sum, min, max, errors, cpu, mem)
        |
        |  every 0.5 s: trigger  open -> closed -> updated -> final
        v
 [Emit]  Redis metrics:<type>:<window_id>  +  pub/sub swma:updates
         Kafka metrics.windows   +   S3 archive (final windows)
        |
        |  every 5 s: checkpoint {windows, dedup ids, next offset} -> RocksDB (atomic)
        |             then commit offset to Kafka;  every 30 s: snapshot -> S3
        v
 [FastAPI x2 behind nginx ALB]  REST /api/v1/*, WebSocket /ws/live, /ws/stream
        v
 [Dashboard / virtual lab]          [Prometheus -> Grafana, Alertmanager -> API webhook]
''')}
<h2>Window life cycle (per service x window)</h2>
{table(["Status", "Condition", "What is emitted / done"], [
    ["<code>open</code>", "watermark &lt; end", "Partial result every <code>emit_interval</code> (periodic trigger)"],
    ["<code>closed</code>", "end &le; watermark", "On-time result; late events are still accepted"],
    ["<code>updated</code>", "a late event arrived while closed", "Corrected result is re-emitted"],
    ["<code>final</code>", "end + allowed_lateness &le; watermark", "Last result; state is dropped; later events -&gt; DLQ"],
])}
""")

section("tools", "Required Tools", f"""
<p>Only Docker is needed to run the lab. The other tools are for the cloud recipes.</p>
{table(["Tool", "Used for", "Install (Windows)"], [
    ["Docker Desktop (WSL 2)", "Runs all 23 containers", "<code>winget install -e --id Docker.DockerDesktop</code>"],
    ["Git", "Clone / push the repository, CI", "<code>winget install -e --id Git.Git</code>"],
    ["Python 3.12+ (optional)", "Run unit tests and tools on the host", "<code>winget install -e --id Python.Python.3.12</code>"],
    ["Terraform 1.6+", "AWS provisioning (Recipe 11)", "Use AWS CloudShell: <code>sudo dnf install -y terraform</code> after adding the HashiCorp repo"],
    ["AWS account + CloudShell", "Cloud recipes", "No local install: CloudShell is signed in already"],
])}
{pre('''
docker version            # engine running?
docker compose version    # v2
git --version
python --version          # optional
''')}
{note("Docker Desktop needs about 3 GB of free memory for the stack. Stop containers of other projects before a demo if the laptop is busy.")}
""")

section("r1", "Recipe 1: Local Development &amp; One-Click Startup", f"""
<h2>Step 1: start everything</h2>
{pre('''
git clone https://github.com/BinaryByteSid/StreamLab.git
cd StreamLab
docker compose up -d --build        # or double-click START-LAB.bat on Windows
''')}
<p>A one-shot <code>bootstrap</code> container prepares the infrastructure before the pipeline starts: topics with
replication factor 3, the JSON schema in the registry, default settings and service registrations in Consul, and the
S3 buckets.</p>
{fn("services/swma/bootstrap.py", "kafka")}
<h2>Step 2: open the lab and the tools</h2>
{table(["URL", "What"], [
    ["http://localhost:8080", "Virtual lab + dashboard (nginx ALB → api-1 / api-2)"],
    ["http://localhost:8081", "Redpanda Console (topics, messages, consumer groups)"],
    ["http://localhost:3001", "Grafana (admin / streamlab)"],
    ["http://localhost:9091 · :9093", "Prometheus · Alertmanager"],
    ["http://localhost:8500", "Consul UI (key/value config, service health)"],
    ["http://localhost:5540", "RedisInsight"],
    ["http://localhost:9001/rustfs/console/", "S3 console (streamlab / streamlab-secret)"],
    ["http://localhost:5080", "Container registry UI"],
    ["localhost:29092-29094", "Kafka for your own clients"],
])}
<h2>Step 3: stop or reset</h2>
{pre('''
docker compose down        # keep all data
docker compose down -v     # delete all volumes (fresh start)
''')}
""")

section("r2", "Recipe 2: Event Contract, Schema Registry &amp; Partitioning", f"""
<p>The event model follows the project document. The same JSON Schema is registered in the Redpanda Schema
Registry (<code>telemetry.events-value</code>) and fetched by every worker at start-up.</p>
{fn("services/swma/common/schema.py", "EVENT_SCHEMA", 30)}
<h2>Keys decide partitions</h2>
<p>The producer sends every event with <code>key = service</code>. confluent-kafka's default partitioner is
<code>CRC32(key) mod partitions</code>, so the mapping is fixed and visible:</p>
{table(["Partition", "Services (CRC32 mod 4)", "Share of traffic"], [
    ["0", "order-service, cart-service", "~29%"],
    ["1", "api-gateway, payment-service, user-service", "~43%"],
    ["2", "(none)", "0%: idle"],
    ["3", "product-service, inventory-service", "~29%"],
])}
{note("<b>Data skew is a design lesson, not a bug.</b> With four partitions and seven keys one partition is idle and one carries 43% of the load. Adding workers cannot split a partition; you must change the key or the partition count.")}
{grep("services/swma/producer/__main__.py", r"def _send", 4)}
""")

section("r3", "Recipe 3: Event-Time Windows: Tumbling &amp; Sliding", f"""
<h2>Mathematical formulation</h2>
<p>Windows are epoch-aligned and <b>half-open</b> <code>[start, end)</code>. For an event at time <i>t</i> (ms):</p>
{pre('''
tumbling:  start = t - (t mod size)                 -> exactly 1 window
sliding :  starts = { s : s = t - (t mod slide) - k*slide,  s > t - size }   -> size / slide windows
           size 60 s, slide 10 s  =>  6 windows per event
''')}
{fn("services/swma/engine/windows.py", "tumbling")}
{fn("services/swma/engine/windows.py", "sliding")}
{note("An event at exactly <code>10:01:00.000</code> belongs to <code>[10:01:00, 10:02:00)</code>, not to the window that ends there. The edge-case suite checks this at 1 ms resolution.")}
<p>Changing the size or slide in the control plane affects new windows only; open windows keep their boundaries
(the window key contains start <i>and</i> end).</p>
""")

section("r4", "Recipe 4: Watermarks, Allowed Lateness &amp; Idle Partitions", f"""
<h2>Bounded out-of-orderness</h2>
{pre('''
watermark(p) = max( watermark(p), max_event_time(p) - bound )      monotonic, per partition
closed  when  end            <= watermark
final   when  end + lateness <= watermark
''')}
<p>Defaults: bound 5 s, allowed lateness 10 s, idle timeout 30 s. More lateness means more complete results but
later final answers and more state held in memory.</p>
<h2>Idle partitions, done safely</h2>
<p>If the producer stops, no event moves the watermark and the last windows would never close. So an idle partition's
watermark follows the wall clock, but <b>only when the consumer has caught up</b> (position = high-water mark).
Otherwise a worker that is merely catching up after a restart would mark its own backlog as "too late".</p>
{fn("services/swma/engine/watermark.py", "Watermark.value")}
{grep("services/swma/processor/worker.py", r"eng.caught_up = pos >= hi", 1, 4)}
{note("A clock-skewed event from the future is clamped to the wall clock in <code>observe()</code>, so it cannot drag the watermark ahead and close windows early. Events more than 5 minutes ahead are rejected.")}
""")

section("r5", "Recipe 5: Stateful Incremental Aggregation &amp; Duplicate Detection", f"""
<p>The aggregate never stores events, only running values, so memory per window is constant:</p>
{pre('''
count <- count + 1          min <- min(min, latency)       errors <- errors + [status >= 500]
sum   <- sum + latency      max <- max(max, latency)       avg = sum / count   (on emission)
''')}
{fn("services/swma/engine/aggregate.py", "Aggregate.add")}
<h2>Processing one event</h2>
<p>The per-partition engine checks duplicates, reads the watermark, updates every window the event belongs to,
and classifies the event (<code>on_time</code>, <code>out_of_order</code>, <code>late_accepted</code>,
<code>too_late</code>, <code>duplicate</code>).</p>
{fn("services/swma/engine/engine.py", "PartitionEngine.process", 52)}
{note("Duplicate ids are remembered only as long as a duplicate could still change a window: <code>horizon = watermark - max(window size) - lateness</code>. Older ids are garbage-collected.")}
""")

section("r6", "Recipe 6: Exactly-Once State with RocksDB Checkpoints &amp; S3 Backups", f"""
<h2>Atomic checkpoint</h2>
<p>Every <code>checkpoint_interval</code> (5 s) each worker writes, per partition, only what changed (window states,
new and expired dedup ids, watermark, counters, <b>next offset</b>) in one RocksDB <code>WriteBatch</code>, then commits
that offset to Kafka. DLQ and result messages are flushed first, so an offset is never committed before its side
effects are durable.</p>
{fn("services/swma/processor/worker.py", "Worker.checkpoint")}
<h2>Restore on assignment</h2>
{fn("services/swma/processor/worker.py", "Worker.on_assign", 40)}
<h2>Why this is exactly-once for the state</h2>
{pre('''
crash after checkpoint C (offset o_C) and before checkpoint C+1
new owner: state := RocksDB(C) ;  seek(o_C) ;  replay events o_C .. o_crash
=> every event before o_C is in the state once (it was in C),
   every event from o_C on is applied once (replayed into a state that never saw it)
''')}
{note("Output to Redis is idempotent (each emission overwrites the window's full result). The <code>metrics.windows</code> topic is at-least-once. Every 30 s a full snapshot also goes to S3; if the local disk is lost the new owner restores from S3 and replays from the snapshot's offset.")}
""")

section("r7", "Recipe 7: Consumer Groups, Rebalancing, Self-Healing &amp; Autoscaling", f"""
<p>The <code>processor</code> container runs a supervisor that starts 1-4 worker processes. Each is a member of
consumer group <code>swma-processors</code>; Kafka gives every partition to exactly one member and rebalances when one
joins, leaves gracefully (SIGTERM: checkpoint, leave) or crashes (SIGKILL: detected after the 6 s session timeout).</p>
<h2>Self-healing</h2>
<p>A crashed worker is restarted after <code>restart_delay_s</code> (8 s), long enough to watch the partitions move.</p>
<h2>Autoscaling on consumer lag</h2>
{pre('''
lag(p) = high_watermark(p) - next_offset(p)          (live, reported by the owning worker)
scale up   if  sum lag > scale_up_lag   and cooled down and workers < max
scale down if  sum lag < scale_down_lag for a whole cooldown and workers > min
''')}
{fn("services/swma/processor/supervisor.py", "Supervisor.autoscale")}
""")

section("r8", "Recipe 8: Low-Latency Serving: Redis, REST, WebSockets &amp; Security", f"""
<h2>Redis key design</h2>
{table(["Key", "Type", "Content"], [
    ["<code>metrics:tumbling:&lt;window_id&gt;</code>", "hash", "one field per service: the window's latest result JSON"],
    ["<code>metrics:sliding:&lt;window_id&gt;</code>", "hash", "same for sliding windows"],
    ["<code>metrics:latest|last_closed:&lt;type&gt;:&lt;service&gt;</code>", "hash", "newest window / newest complete window"],
    ["<code>metrics:history:&lt;type&gt;:&lt;service&gt;</code>", "sorted set", "window ids by start time (last 300)"],
    ["pub/sub <code>swma:updates</code>", "channel", "every emission, forwarded to browsers over <code>/ws/live</code>"],
])}
<p>A Lua script makes the "latest" pointers race-free: an older window can never overwrite a newer one.</p>
{fn("services/swma/processor/worker.py", "SET_IF_NEWER")}
<h2>Authentication and rate limiting</h2>
<p>Public reads (<code>/api/v1/*</code>) need an API key (<code>X-API-Key</code>) or a JWT; actions that change the system
need an admin JWT from <code>POST /api/auth/token</code>. A token bucket in Redis, shared by both API replicas,
answers <code>429</code> with <code>Retry-After</code>:</p>
{pre('''
tokens := min(burst, tokens + elapsed_s * rate) ;  allow if tokens >= 1 (then tokens -= 1)
''')}
{fn("services/swma/api/deps.py", "TOKEN_BUCKET")}
""")

section("r9", "Recipe 9: Control Plane: Live Configuration with Consul", f"""
<p>37 settings (window size and slide, allowed lateness, watermark bound, triggers, feature flags, scaling policy,
producer rate and scenario, chaos percentages, rate limits, alert thresholds) live under <code>swma/config/</code> in
Consul KV. Each service keeps a <b>blocking query</b> open; Consul answers the moment anything changes, so new values
apply in about a second without restarts.</p>
{fn("services/swma/common/config.py", "LiveConfig._fetch")}
<h2>Three ways to change a setting</h2>
{pre('''
# 1. Admin UI: lab -> "6 · Control plane" -> Save (writes an audit entry)
# 2. CLI inside the stack
docker compose exec api-1 python -m swma.cli set window/tumbling_size_s 30
docker compose exec api-1 python -m swma.cli list
# 3. REST
curl -X PUT localhost:8080/api/lab/config -H "Authorization: Bearer $JWT" \\
     -H "Content-Type: application/json" -d '{"window/allowed_lateness_s": 20}'
''')}
{note("Alert thresholds also live in Consul. The supervisor exports them as Prometheus gauges, and the alert rules compare against those gauges, so a threshold changed in the UI changes the alert live.")}
""")

section("r10", "Recipe 10: Observability: PromQL, LogQL &amp; Alerts", f"""
<h2>Useful PromQL</h2>
{pre('''
sum(rate(swma_producer_events_total[1m]))                                   # events in / s
sum(rate(swma_processor_events_total{outcome!="invalid"}[1m]))             # events out / s
sum by (outcome) (rate(swma_processor_events_total[1m]))                   # on_time, out_of_order, ...
swma_group_lag_total                                                         # committed consumer lag
histogram_quantile(0.95, sum(rate(swma_processing_latency_seconds_bucket[1m])) by (le))  # p95
sum by (reason) (increase(swma_processor_dlq_total[1h]))                    # dead letters by reason
swma_window_avg_latency_ms{window_type="sliding"}                            # business metric per service
''')}
<h2>Alert rules (thresholds from Consul)</h2>
{grep("infra/prometheus/alerts.yml", r"alert: HighServiceLatency", 7)}
<h2>Useful LogQL (Loki, chunks stored in S3)</h2>
{pre('''
{kind="audit"}                                   # who changed what
{kind="error"}                                   # all errors
{service="processor"} |= "restored"              # state restores after rebalances
sum by (service) (rate({kind=~".+"}[1m]))        # log volume per service
''')}
{table(["Grafana dashboard", "Shows"], [
    ["1. Pipeline overview", "throughput in/out, lag per partition, outcomes, stages, workers, watermark delay, checkpoints, retries"],
    ["2. Microservice metrics", "latency, error rate and requests per service from window results"],
    ["3. Kafka, Redis, API &amp; containers", "brokers, under-replicated partitions, Redis, API rate and 429s, container CPU / memory"],
    ["4. Logs", "log volume, errors, audit log, all logs"],
])}
""")

section("r11", "Recipe 11: Cloud Deployment on AWS: Full &amp; Hybrid", f"""
<h2>Option A: everything on EC2 (<code>deploy/aws</code>)</h2>
<p>Terraform creates a VPC and security group (ports 80 and 3000 only), an <code>m7i-flex.large</code> EC2 instance
with an Elastic IP, three S3 buckets, CloudWatch log groups, dashboard and alarms, SNS e-mail, an AWS Budget and a
least-privilege IAM role (no keys on the server). User-data installs Docker and starts the stack with
<code>docker-compose.aws.yml</code>.</p>
{pre('''
# AWS CloudShell
git clone https://github.com/BinaryByteSid/StreamLab.git && cd StreamLab/deploy/aws/terraform
export TF_DATA_DIR=/tmp/tfdata && terraform init && terraform apply -var="alert_email=you@example.com"
terraform output            # lab_url, grafana_url, cloudwatch_dashboard, ...
''')}
<h2>Option B: hybrid (<code>deploy/aws-hybrid</code>), for accounts that may not run servers</h2>
<p>Some sandbox accounts deny EC2, Lightsail, Lambda and ECS through an organization policy but allow S3,
CloudWatch and IAM. The lab then runs locally and uses those services: checkpoints, archive, Loki chunks and a
per-minute JSON-lines copy of the live stream in <b>Amazon S3</b>, live metrics on a <b>CloudWatch</b> dashboard.</p>
{pre('''
cd StreamLab/deploy/aws-hybrid && export TF_DATA_DIR=/tmp/tfdata && terraform init && terraform apply
terraform output -raw env_file      # -> save as VirtualLab/.env.aws on the laptop
# laptop:  START-LAB-AWS.bat   (compose with docker-compose.hybrid.yml)
''')}
<h2>Least-privilege runtime policy</h2>
{grep("deploy/aws-hybrid/main.tf", r'resource "aws_iam_user_policy" "lab"', 15)}
{note("Diagnose an organization block quickly: <code>aws ec2 describe-availability-zones --region ap-south-1</code> returning <i>explicit deny in a service control policy</i> means no Terraform change can help; use the hybrid mode.")}
""")

section("r12", "Recipe 12: Verification, Edge Cases &amp; CI/CD", f"""
<h2>Unit and end-to-end tests</h2>
{pre('''
docker compose exec -T api-1 python -m pytest tests/unit -q                       # 17 tests
docker compose exec -T -e STREAMLAB_E2E=1 api-1 python -m pytest tests/e2e -q     # 7 tests, live stack
''')}
<p>The unit suite covers window boundaries, sliding membership, aggregation maths, watermark monotonicity, idle
handling, out-of-order, late and too-late events, duplicates, a comparison with a batch reference implementation on
600 shuffled events, and crash recovery (checkpoint, crash, replay, exact count).</p>
{fn("services/tests/unit/test_engine.py", "test_crash_recovery_from_checkpoint_gives_exact_counts")}
<h2>Self-checking edge cases on the live pipeline</h2>
<p>Lab page <i>Edge cases &amp; stress tests</i> (API <code>POST /api/lab/edge/run</code>). Each run uses throw-away
services and verifies the outcome. Measured on the reference laptop:</p>
{table(["Scenario", "Result"], [
    ["1,000 events at the same instant", "sent in 0.03 s (~35,000 ev/s), 4 partitions, all 1,000 counted once in 0.7 s"],
    ["Hot key (one service)", "1 partition, in order, all counted"],
    ["Every event sent 3 times", "900 messages, counted 300"],
    ["Out-of-order within the bound", "500 counted, 0 dead letters"],
    ["Older than allowed lateness", "200 / 200 to DLQ (too_late), no final window changed"],
    ["8 kinds of malformed events", "120 / 120 to DLQ with reasons, stream unaffected"],
    ["Clock skew +1 min / +10 min", "accepted 50 / rejected 50"],
    ["Exact window boundary (±1 ms)", "half-open windows confirmed"],
    ["200 KB / 2.2 MB message", "processed / refused by Kafka (message too large)"],
    ["Worker killed mid-burst (3,000 events)", "lag peak 1,569, recovered in 7.8 s, exactly 3,000 counted"],
])}
<h2>CI/CD</h2>
<p>Locally the ops agent runs: commit info, pytest, bandit, Docker build of 5 images, push to the registry, rolling
restart of both API replicas behind the load balancer (measured: 449 requests, 0 failed). On GitHub,
<code>.github/workflows/ci.yml</code> runs unit tests, bandit + Trivy, the full-stack e2e suite and image builds.</p>
""")

section("cheat", "Quick Reference Cheat Sheet", f"""
<h2>REST API (via http://localhost:8080)</h2>
{table(["Endpoint", "Method", "Auth", "Purpose"], [
    ["/api/v1/metrics?window_type=", "GET", "API key", "Latest complete + live window for every service"],
    ["/api/v1/metrics/{service}[/latest|/history]", "GET", "API key", "One service, latest or history"],
    ["/api/v1/windows/{type}/{window_id}", "GET", "API key", "One window, all services"],
    ["/api/auth/token", "POST", "-", "Admin JWT (username / password)"],
    ["/api/lab/events", "POST", "-", "Send your own events (delay, duplicate, malformed)"],
    ["/api/lab/trace/{event_id}", "GET", "-", "Journey of one event"],
    ["/api/lab/topology · /processor · /status", "GET", "-", "Kafka cluster, workers, health of all components"],
    ["/api/lab/config", "GET / PUT", "JWT for PUT", "Control-plane settings (Consul)"],
    ["/api/lab/edge/run · /edge/runs/{id}", "POST / GET", "JWT for POST", "Edge-case runs and their results"],
    ["/ws/live · /ws/stream?topic=", "WebSocket", "-", "Live window results / live Kafka tail"],
])}
<h2>Key settings (Consul <code>swma/config/…</code>)</h2>
{table(["Key", "Default", "Meaning"], [
    ["window/tumbling_size_s", "60", "Tumbling window length"],
    ["window/sliding_size_s · sliding_slide_s", "60 · 10", "Sliding window length and step"],
    ["window/allowed_lateness_s", "10", "Grace period after the watermark passes a window end"],
    ["watermark/max_out_of_orderness_s", "5", "Watermark = newest event time minus this"],
    ["watermark/idle_timeout_s", "30", "Quiet, caught-up partition follows the clock after this"],
    ["trigger/emit_interval_s", "2", "Partial results for open windows"],
    ["state/checkpoint_interval_s · s3_backup_interval_s", "5 · 30", "RocksDB checkpoint / S3 snapshot"],
    ["scaling/min_workers · max_workers · scale_up_lag", "2 · 4 · 400", "Autoscaler policy"],
    ["producer/rate_eps · producer/scenario", "25 · normal", "Simulated traffic"],
    ["api/rate_limit_rps · rate_limit_burst", "5 · 10", "Public API token bucket"],
])}
<h2>Topics</h2>
{table(["Topic", "Partitions × RF", "Content"], [
    ["telemetry.events", "4 × 3", "raw events, key = service"],
    ["telemetry.dlq", "2 × 3", "dead letters with reason, source offset and original payload"],
    ["metrics.windows", "4 × 3", "every window result emitted by the processor"],
])}
""")


# ------------------------------------------------------------------- render
CSS = """
@page { size: A4; margin: 18mm 16mm 18mm 16mm;
  @top-center { content: "StreamLab Virtual Lab - Architectural Blueprint & Engineering Cookbook"; font: 8.5pt 'Segoe UI', Arial; color: #64748b; }
  @bottom-right { content: "Page " counter(page); font: 8.5pt 'Segoe UI', Arial; color: #64748b; } }
@page :first { @top-center { content: none; } @bottom-right { content: none; } }
* { box-sizing: border-box; }
body { font-family: 'Segoe UI', Arial, sans-serif; font-size: 10.3pt; line-height: 1.45; color: #0f172a; margin: 0; }
h1 { font-size: 18pt; color: #13286b; border-bottom: 2.5px solid #2563eb; padding-bottom: 4px; margin: 0 0 10px; }
h2 { font-size: 12.5pt; color: #1e3a8a; margin: 16px 0 6px; break-after: avoid; }
.src { break-after: avoid; }
section { page-break-before: always; }
p { margin: 5px 0 8px; }
ol, ul { margin: 4px 0 8px 18px; padding: 0; } li { margin: 3px 0; }
code { font-family: Consolas, 'Cascadia Code', monospace; font-size: 9pt; background: #eef2ff; padding: 0 3px; border-radius: 3px; color: #1e3a8a; }
pre.code { font-family: Consolas, 'Cascadia Code', monospace; font-size: 8.2pt; line-height: 1.35; background: #0f172a; color: #e2e8f0;
  padding: 9px 11px; border-radius: 6px; white-space: pre-wrap; word-break: break-word; margin: 3px 0 10px; break-inside: auto; }
.src { font-size: 8.3pt; color: #475569; margin-top: 8px; font-style: italic; }
.src code { font-style: normal; }
table { width: 100%; border-collapse: collapse; margin: 6px 0 12px; font-size: 9.1pt; break-inside: auto; }
th { background: #1e3a8a; color: #fff; text-align: left; padding: 5px 7px; font-weight: 600; }
td { border-bottom: 1px solid #e2e8f0; padding: 4px 7px; vertical-align: top; }
tr:nth-child(even) td { background: #f8fafc; }
tr { break-inside: avoid; }
.note { background: #fffbeb; border-left: 4px solid #f59e0b; padding: 7px 10px; margin: 8px 0 12px; font-size: 9.5pt; border-radius: 0 6px 6px 0; break-inside: avoid; }
.cover { height: 255mm; display: flex; flex-direction: column; }
.cover .band { background: linear-gradient(120deg, #0f2560, #1d4ed8 60%, #0ea5e9); color: #fff; border-radius: 10px; padding: 26px 26px 22px; }
.cover h1 { color: #fff; border: 0; font-size: 27pt; margin: 0 0 6px; padding: 0; }
.cover .sub { font-size: 14pt; color: #dbeafe; font-weight: 600; }
.cover .lead { font-size: 10.8pt; color: #e0e7ff; margin-top: 12px; max-width: 165mm; }
.cover h2 { margin-top: 22px; }
.cover .foot { margin-top: auto; font-size: 9pt; color: #64748b; border-top: 1px solid #e2e8f0; padding-top: 8px; }
.toc a { color: #0f172a; text-decoration: none; }
.toc li { font-size: 11pt; margin: 6px 0; list-style: none; border-bottom: 1px dotted #cbd5e1; padding-bottom: 3px; display: flex; justify-content: space-between; }
.toc .pg { color: #475569; }
.toc ol { margin-left: 0; }
"""

cover = f"""
<div class="cover">
  <div class="band">
    <h1>StreamLab Virtual Lab</h1>
    <div class="sub">Streaming Window Metrics Aggregator · Architectural Blueprint &amp; Engineering Cookbook</div>
    <p class="lead">A practical guide to building a fault-tolerant, event-time stream processor that turns an endless
    stream of microservice telemetry into correct tumbling- and sliding-window metrics, based on the StreamLab
    repository. Covers architecture and invariants, the maths of windows, watermarks and incremental aggregation,
    exactly-once state, scaling, serving, live configuration, observability, AWS deployment and twelve practical
    recipes with real code.</p>
  </div>
  <h2>Stack</h2>
  {table(["Component", "Role"], [
      ["Redpanda ×3 (Kafka API) + Schema Registry", "Durable, partitioned, replicated event log (RF 3)"],
      ["Python stream processor + RocksDB", "Event-time windows, watermark, stateful aggregation, checkpoints"],
      ["Redis + FastAPI ×2 behind nginx", "Low-latency results, REST + WebSocket, JWT / API keys, rate limiting"],
      ["Consul", "Live configuration and service registry"],
      ["Prometheus · Grafana · Loki · Alertmanager", "Metrics, dashboards, logs, alerts"],
      ["Docker Compose · GitHub Actions · Terraform · AWS (EC2 / S3 / CloudWatch)", "Packaging, CI/CD, cloud deployment"],
  ])}
  <div class="foot">Repository: github.com/BinaryByteSid/StreamLab · Advanced Cloud Computing mini-project · Code excerpts are generated from the source files.</div>
</div>"""

import json, os
PAGES = json.loads(pathlib.Path(os.environ["COOKBOOK_PAGES"]).read_text()) if os.environ.get("COOKBOOK_PAGES") else {}
toc = '<section id="toc"><h1>Table of Contents</h1><ol class="toc">' + "".join(
    f'<li><a href="#{a}">{t}</a><span class="pg">{PAGES.get(a, "")}</span></li>' for a, t, _ in SECTIONS if a) + "</ol></section>"

doc = f"""<!doctype html><html lang="en"><head><meta charset="utf-8"><title>StreamLab Cookbook</title>
<style>{CSS}</style></head><body>{cover}{toc}{''.join(h for _, _, h in SECTIONS)}</body></html>"""

OUT.parent.mkdir(exist_ok=True)
OUT.write_text(doc, encoding="utf-8")
print("written", OUT)
(OUT.parent / ".toc_titles.json").write_text(json.dumps([[a, html.unescape(t)] for a, t, _ in SECTIONS if a]))
