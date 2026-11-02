// All the learning text of the lab: architecture sections, lessons, glossary, quiz.

// The 12 sections of the architecture diagram, mapped to real containers.
export const SECTIONS = [
  { n: 1, title: 'Event Sources', color: 'var(--s1)', page: '#/event-lab', wide: 1,
    items: ['7 simulated e-commerce microservices', 'User events, app logs, system metrics', 'Your own custom events'],
    what: 'Every request handled by a microservice (payment, order, cart …) produces a small JSON message called an <b>event</b>: which service, when it happened, how long it took, whether it failed, CPU and memory.',
    analogy: 'Think of each microservice as a shop counter that writes a receipt for every customer. The receipts are the events.',
    containers: ['producer'], tools: [] },
  { n: 2, title: 'Message Broker (Kafka / Redpanda)', color: 'var(--s2)', page: '#/kafka', wide: 1,
    items: ['Topic telemetry.events, 4 partitions', '3 brokers, replication factor 3', 'Schema Registry + Console UI'],
    what: 'Kafka (here: Redpanda, a Kafka-compatible broker) is a durable, ordered log. Producers append events; consumers read them at their own speed. The topic is split into <b>partitions</b> so several processors can work in parallel, and every partition is copied to 3 brokers so a broker can die without losing data.',
    analogy: 'A post office with 4 sorting belts. Every letter for "payment-service" always goes on the same belt, and each belt is photocopied to 3 buildings.',
    containers: ['redpanda-0', 'redpanda-1', 'redpanda-2', 'redpanda-console'], tools: ['console'] },
  { n: 3, title: 'Stream Processing Engine (Python)', color: 'var(--s3)', page: '#/processor', wide: 2,
    items: ['Consumer group of 1–4 worker processes', 'Validate → event time → watermark → windows', 'Stateful aggregation + checkpoints (RocksDB)', 'Late & out-of-order handling, DLQ'],
    what: 'The processor reads events from Kafka and turns an endless stream into finite <b>windows</b> (e.g. one minute). For each service and window it keeps running totals (count, sum, min, max) instead of storing every event, decides when a window is complete using a <b>watermark</b>, and saves its state regularly so it can recover after a crash.',
    analogy: 'An accountant who keeps one running tally per shop per minute. When the clock says the minute is over (plus a little grace time for late receipts) the tally is published.',
    containers: ['processor'], tools: [] },
  { n: 4, title: 'Metrics Storage & Serving', color: 'var(--s4)', page: '#/storage', wide: 1,
    items: ['Redis: latest window results (TTL)', 'FastAPI × 2: REST + WebSocket', 'JWT / API key auth, rate limiting'],
    what: 'Finished window results go into <b>Redis</b>, an in-memory database that answers in well under a millisecond. Two <b>FastAPI</b> instances read Redis and serve the results over REST and push live updates over WebSockets.',
    analogy: 'Redis is the shop window where today\'s results are displayed; FastAPI is the shop assistant who answers questions about them.',
    containers: ['redis', 'redisinsight', 'api-1', 'api-2'], tools: ['redisinsight', 'api_docs'] },
  { n: 5, title: 'Dashboard (Web UI)', color: 'var(--s5)', page: '#/dashboard', wide: 1,
    items: ['Real-time charts per service', 'Tumbling & sliding views', 'Live updates via WebSocket'],
    what: 'The dashboard shows the computed metrics, never raw events. It receives every new window result instantly through a WebSocket.',
    analogy: 'The scoreboard in a stadium: it shows the score, not every pass.',
    containers: ['dashboard', 'alb'], tools: ['lab'] },
  { n: 6, title: 'Control Plane', color: 'var(--s6)', page: '#/control', wide: 1,
    items: ['Consul KV: window size, lateness, scaling…', 'Service registry with health checks', 'Admin UI + CLI'],
    what: 'Settings such as window size, allowed lateness and scaling policy live in <b>Consul</b>. The services watch Consul and apply changes within a second, without restarting. Consul also keeps a registry of every service and its health.',
    analogy: 'The control room: change a dial and every machine on the floor adjusts.',
    containers: ['consul'], tools: ['consul'] },
  { n: 7, title: 'DevOps & CI/CD', color: 'var(--s7)', page: '#/devops', wide: 1,
    items: ['Unit tests, security scan', 'Build images, push to registry', 'Rolling deploy behind the load balancer'],
    what: 'Every code change goes through an automated pipeline: tests, a security scan, Docker image build, push to a registry and a deployment. In this lab you can run the real pipeline with one click; GitHub Actions files do the same in the cloud.',
    analogy: 'A factory quality line: nothing ships unless every check passes.',
    containers: ['ops-agent'], tools: [] },
  { n: 8, title: 'Container Registry', color: 'var(--s8)', page: '#/devops', wide: 1,
    items: ['Local Docker registry (= Docker Hub / ECR)', 'Versioned images per commit'],
    what: 'Built images are stored in a registry with a version tag (the git commit). Servers pull exactly that version to run it.',
    analogy: 'A warehouse of sealed, labelled boxes ready to ship.',
    containers: ['registry', 'registry-ui'], tools: ['registry'] },
  { n: 9, title: 'Deployment (AWS Cloud)', color: 'var(--s9)', page: '#/cloud', wide: 1,
    items: ['nginx = Application Load Balancer', 'RustFS = Amazon S3', 'Terraform for EC2 / VPC / SG'],
    what: 'Locally, nginx plays the AWS Application Load Balancer and RustFS plays S3. The deploy/aws folder contains Terraform to run the same containers on EC2.',
    analogy: 'A rehearsal on a stage built exactly like the real theatre.',
    containers: ['alb', 'rustfs'], tools: ['s3'] },
  { n: 10, title: 'Observability & Monitoring', color: 'var(--s10)', page: '#/observability', wide: 1,
    items: ['Prometheus metrics, Grafana dashboards', 'Loki logs (stored in S3)', 'Alertmanager alerts'],
    what: 'A streaming system must watch itself: how far behind is it (lag), how fast, how many errors. Prometheus collects numbers every 5 seconds, Loki collects logs, Grafana draws them and Alertmanager raises alarms.',
    analogy: 'The car dashboard: speed, fuel and warning lights.',
    containers: ['prometheus', 'grafana', 'loki', 'alertmanager', 'redis-exporter'], tools: ['grafana', 'prometheus', 'alertmanager'] },
  { n: 11, title: 'Scalability, Fault Tolerance & Resilience', color: 'var(--s11)', page: '#/resilience', wide: 2,
    items: ['Horizontal & auto scaling', 'Checkpoints, replication, DLQ', 'Backpressure, retries, graceful shutdown'],
    what: 'The system keeps working when parts fail: crashed workers are replaced and their state restored, brokers keep copies of data, bad events are parked in a dead letter queue, and extra workers are added when the load grows.',
    analogy: 'A team where anyone can take over a colleague\'s desk because the notes are always up to date.',
    containers: [], tools: [] },
  { n: 12, title: 'Key Metrics Generated (per window)', color: 'var(--s12)', page: '#/dashboard', wide: 2,
    items: ['Count, Sum, Average, Min, Max', 'Error rate, Throughput', 'Processing latency, CPU, memory'],
    what: 'For each service and each window the aggregator outputs: request count, sum and average latency, minimum and maximum latency, error count and rate, throughput (events per second) and average CPU / memory.',
    analogy: 'The end-of-shift report for each counter.',
    containers: [], tools: [] },
];

export const LESSONS = [
  { id: 'events', title: '1. What is an event and a stream?', time: '4 min', body: `
    <p>An <b>event</b> is a small record that says <i>"something happened at a certain time"</i>. In this lab every event is one HTTP request handled by a microservice:</p>
    <pre><code>{
  "event_id": "evt-3f9a1c",          ← unique id
  "service": "payment-service",     ← who
  "event_time": "2026-11-02T10:00:05.250Z",  ← when it happened
  "response_time_ms": 185.4,        ← how long it took (latency)
  "status_code": 200,               ← 2xx ok, 5xx = error
  "cpu_percent": 41.2, "memory_percent": 63.0
}</code></pre>
    <p>A <b>stream</b> is an endless sequence of events. It never "finishes", so you cannot say "load all the data, then compute the average" like you would with a spreadsheet. That is <b>batch</b> processing. <b>Stream</b> processing computes answers continuously, while the data flows.</p>
    <div class="analogy">🧾 Batch = counting all of last month's receipts at the end of the month. Stream = a counter on the till that updates with every sale.</div>
    <p>Here, 7 microservices (api-gateway, order, payment, product, cart, inventory, user) generate about 25 events per second.</p>`,
    try: '#/event-lab' },
  { id: 'kafka', title: '2. Kafka: topics, partitions, offsets', time: '6 min', body: `
    <p><b>Kafka</b> (we use <b>Redpanda</b>, which speaks the same protocol) stores events in a <b>topic</b>: an append-only log. New events go to the end; nothing is changed in place.</p>
    <ul>
      <li><b>Partition</b>: a topic is split into several logs so many machines can read in parallel. Ours has <b>4</b>.</li>
      <li><b>Key</b>: every event has a key (here the service name). Same key → same partition, always. So all payment-service events are in order in one partition.</li>
      <li><b>Offset</b>: the position of an event inside its partition (0, 1, 2 …). A reader remembers "I am at offset 1520" and can resume from there.</li>
      <li><b>Replication</b>: every partition is copied to 3 brokers. One is the <b>leader</b>, the others are <b>followers</b> (the in-sync replicas, ISR). If the leader dies, a follower takes over.</li>
    </ul>
    <div class="playground" id="pg-partition"></div>
    <div class="analogy">📮 Kafka is a post office with numbered sorting belts. The address (key) decides the belt. Each letter on a belt gets a number (offset).</div>`,
    try: '#/kafka' },
  { id: 'consumer-groups', title: '3. Consumer groups and scaling', time: '4 min', body: `
    <p>Several processor <b>workers</b> join one <b>consumer group</b> (<code>swma-processors</code>). Kafka gives each partition to exactly one worker in the group. With 4 partitions:</p>
    <ul><li>1 worker → it reads all 4 partitions</li><li>2 workers → 2 partitions each</li><li>4 workers → 1 each</li><li>5 workers → one sits idle! (more workers than partitions don't help)</li></ul>
    <p>When a worker joins or leaves, Kafka <b>rebalances</b>: it re-deals the partitions. This is how the system <b>scales horizontally</b> and survives crashes.</p>
    <div class="playground" id="pg-group"></div>
    <p><b>Consumer lag</b> = events written to Kafka but not processed yet. Lag growing = processors too slow = time to add workers (that is what the autoscaler does).</p>`,
    try: '#/experiments/scaling' },
  { id: 'event-time', title: '4. Event time vs processing time', time: '4 min', body: `
    <p>Every event has two times:</p>
    <ul><li><b>Event time</b>: when it actually happened (written inside the event).</li><li><b>Processing time</b>: when our processor reads it.</li></ul>
    <p>Networks are slow and phones go offline, so an event from 10:00:05 may arrive at 10:00:12, after newer events. That is an <b>out-of-order</b> event. If we grouped by processing time we would put it in the wrong minute. So this system always uses <b>event time</b>.</p>
    <div class="analogy">✈️ A postcard written on Monday may arrive on Thursday. It still belongs to Monday's holiday story.</div>`,
    try: '#/experiments/late-events' },
  { id: 'windows', title: '5. Tumbling and sliding windows', time: '6 min', body: `
    <p>To get numbers out of an endless stream we cut it into <b>windows</b>:</p>
    <ul>
      <li><b>Tumbling window</b>: fixed size, no overlap. 10:00–10:01, 10:01–10:02 … Each event is in exactly <b>one</b> window. Good for "requests per minute".</li>
      <li><b>Sliding window</b>: fixed size that moves forward by a smaller step (the <b>slide</b>). Size 60s, slide 10s → 10:00:00–10:01:00, 10:00:10–10:01:10 … Each event is in <b>size ÷ slide = 6</b> windows. Good for a smooth "last 60 seconds" view.</li>
    </ul>
    <p>Windows are <b>half-open</b>: <code>[start, end)</code>. An event at exactly 10:01:00 belongs to the window that <i>starts</i> at 10:01:00.</p>
    <div class="playground" id="pg-windows"></div>`,
    try: '#/experiments/windows' },
  { id: 'aggregation', title: '6. Stateful, incremental aggregation', time: '4 min', body: `
    <p>Recomputing the average of 10,000 events every time one more arrives would be wasteful. Instead the processor keeps a tiny <b>state</b> per (service, window):</p>
    <pre><code>count ← count + 1
sum   ← sum + latency
min   ← min(min, latency)
max   ← max(max, latency)
average = sum / count   (computed when needed)</code></pre>
    <p>Because the new state depends on the old state, the aggregator is <b>stateful</b>. Step through it:</p>
    <div class="playground" id="pg-agg"></div>`,
    try: '#/processor' },
  { id: 'watermarks', title: '7. Watermarks and allowed lateness', time: '7 min', body: `
    <p>When is the 10:00–10:01 window "done"? We can never be 100% sure a late event won't arrive. A <b>watermark</b> is the processor's best guess of how far event time has progressed:</p>
    <pre><code>watermark = newest event time seen − max out-of-orderness (5 s)</code></pre>
    <p>It means "I believe all events older than this have arrived." The window life cycle:</p>
    <ol>
      <li><b>open</b>: watermark &lt; window end. Partial results are emitted every 2 s (periodic trigger).</li>
      <li><b>closed</b>: watermark ≥ end. The on-time result is emitted.</li>
      <li><b>updated</b>: a <b>late</b> event arrived within the <b>allowed lateness</b> (10 s) and corrected the result.</li>
      <li><b>final</b>: watermark ≥ end + allowed lateness. The result is frozen and the state is deleted.</li>
    </ol>
    <p>An event whose windows are all final is <b>too late</b>: it goes to the <b>dead letter queue</b> (DLQ) instead of silently changing old numbers.</p>
    <p><b>Trade-off:</b> more allowed lateness → more complete results but slower final answers and more memory.</p>
    <div class="playground" id="pg-watermark"></div>`,
    try: '#/experiments/late-events' },
  { id: 'faults', title: '8. Checkpoints, crashes and exactly-once state', time: '6 min', body: `
    <p>Every 5 seconds each worker writes a <b>checkpoint</b> to its local <b>RocksDB</b> state store: the window states, the duplicate-detection ids <i>and</i> the Kafka offset they correspond to, all in one atomic write. Then it commits that offset to Kafka.</p>
    <p>If a worker crashes (no warning, no checkpoint) another worker takes its partitions, loads the last checkpoint and <b>replays</b> the events after that offset. Because state and offset were saved together, every event is counted <b>exactly once in the state</b>, even though some are read twice.</p>
    <p>Every 30 s a copy of the state also goes to <b>S3</b>. If the local disk is lost, workers restore from S3.</p>
    <p><b>Duplicates</b> (the same event sent twice by a service) are caught by remembering recent <code>event_id</code>s.</p>
    <div class="analogy">💾 Like a video game that autosaves every 5 seconds: if it crashes you replay only the last few seconds, and the save file remembers exactly where you were.</div>`,
    try: '#/experiments/crash' },
  { id: 'serving', title: '9. Redis, REST APIs and WebSockets', time: '4 min', body: `
    <p><b>Redis</b> keeps data in memory, so reads take microseconds. Window results are stored as <code>metrics:tumbling:&lt;window_id&gt;</code> hashes (one field per service) and expire after an hour (TTL).</p>
    <p><b>FastAPI</b> serves them: <code>GET /api/v1/metrics/payment-service</code>. A <b>REST</b> call is "ask and get an answer". A <b>WebSocket</b> is a phone line kept open: the server pushes every new result the moment it is computed.</p>
    <p>The public API needs an <b>API key</b>, admin actions need a <b>JWT</b>, and a <b>rate limiter</b> (token bucket in Redis) answers <code>429 Too Many Requests</code> if a client calls too fast.</p>
    <p>Two API instances sit behind a <b>load balancer</b>; look at the <code>X-Served-By</code> header to see which one answered.</p>`,
    try: '#/storage' },
  { id: 'observability', title: '10. Observability: metrics, logs, alerts', time: '4 min', body: `
    <ul>
      <li><b>Metrics</b> (Prometheus): numbers over time: events/s, lag, latency percentiles, workers alive.</li>
      <li><b>Logs</b> (Loki): text lines from every service, including an <b>audit log</b> of who changed what.</li>
      <li><b>Dashboards</b> (Grafana): charts on top of both.</li>
      <li><b>Alerts</b> (Alertmanager): rules like "average latency &gt; 500 ms for 10 s" that fire a notification.</li>
    </ul>
    <p><b>p95 latency</b> = 95% of events were faster than this. It shows the slow tail that an average hides.</p>`,
    try: '#/observability' },
  { id: 'devops', title: '11. Containers, CI/CD and the cloud', time: '5 min', body: `
    <p>Each component runs in its own <b>container</b>: a sealed box with the program and everything it needs. <code>docker compose</code> starts all of them with one command.</p>
    <p><b>CI/CD</b> (Continuous Integration / Delivery) means every change is automatically tested, scanned, packaged into an image, pushed to a <b>registry</b> and deployed. A <b>rolling deploy</b> restarts one API instance at a time so users never see downtime.</p>
    <p>In <b>AWS</b>, the same containers run on EC2 machines inside a VPC, behind an Application Load Balancer, with S3 for backups and ECR as the registry. Security Groups act as firewalls.</p>`,
    try: '#/devops' },
];

export const GLOSSARY = [
  ['Allowed lateness', 'Extra time after the watermark passes a window\'s end during which late events may still update it.'],
  ['API key', 'A secret string a client sends (X-API-Key header) to identify itself to the public API.'],
  ['Autoscaler', 'Logic that adds or removes workers based on a measurement (here: consumer lag).'],
  ['Backpressure', 'When producers are faster than consumers, work piles up (lag grows). The system must slow down or scale out.'],
  ['Broker', 'A Kafka server. We run three: redpanda-0, -1, -2.'],
  ['Checkpoint', 'A saved copy of the processing state plus the Kafka offset it belongs to.'],
  ['Consumer group', 'A set of consumers sharing the work of a topic: each partition goes to one member.'],
  ['Consumer lag', 'How many events are in Kafka but not processed yet.'],
  ['Dead letter queue (DLQ)', 'A separate topic (telemetry.dlq) where invalid or too-late events are parked for inspection.'],
  ['Event time', 'When the event happened (inside the event). Opposite: processing time.'],
  ['Idempotent', 'Doing it twice has the same effect as once. Writing the same window result to Redis twice is idempotent.'],
  ['ISR', 'In-sync replicas: the copies of a partition that are fully up to date.'],
  ['JWT', 'JSON Web Token: a signed token proving who you are and what role you have.'],
  ['Leader / follower', 'For each partition one broker (leader) handles reads and writes; followers copy it.'],
  ['Offset', 'Position of a message within a partition.'],
  ['Out-of-order event', 'An event that arrives after a newer event.'],
  ['Partition', 'One of the ordered logs a topic is split into. Unit of parallelism.'],
  ['p95', '95th percentile: 95% of values are below it.'],
  ['Rate limiting', 'Rejecting requests (HTTP 429) when a client exceeds an allowed rate.'],
  ['Rebalance', 'Kafka re-assigning partitions when consumers join or leave the group.'],
  ['Replication factor', 'How many brokers hold a copy of each partition (3 here).'],
  ['RocksDB', 'An embedded key-value database on local disk; used as the processor\'s state store.'],
  ['Schema registry', 'A service that stores the agreed structure (schema) of events so producers and consumers agree.'],
  ['Sliding window', 'Overlapping windows of fixed size that advance by a slide; an event belongs to several.'],
  ['State store', 'Where the processor keeps its running aggregates.'],
  ['Tumbling window', 'Fixed-size, non-overlapping windows; an event belongs to exactly one.'],
  ['TTL', 'Time to live: Redis deletes a key automatically after it.'],
  ['Watermark', 'The processor\'s estimate of how far event time has progressed.'],
  ['WebSocket', 'A connection that stays open so the server can push data to the browser.'],
];

export const QUIZ = [
  { q: 'An event happened at 10:00:58 but reached the processor at 10:01:03. Which tumbling (1 min) window does it belong to?',
    o: ['10:01–10:02, because that is when it arrived', '10:00–10:01, because the system uses event time', 'Both windows', 'Neither, it is dropped'], a: 1,
    e: 'The processor groups by event time, so it goes to 10:00–10:01 (as long as that window is not final yet).' },
  { q: 'Sliding window size 60 s, slide 10 s. In how many windows is each event counted?', o: ['1', '6', '10', '60'], a: 1, e: 'size ÷ slide = 60 ÷ 10 = 6 overlapping windows.' },
  { q: 'Why does the producer use the service name as the Kafka key?', o: ['To compress messages', 'So all events of a service go to the same partition and one worker owns that service\'s window state', 'It is required by Kafka', 'To make messages unreadable'], a: 1, e: 'Same key → same partition → one owner for that state, kept in order.' },
  { q: 'The topic has 4 partitions. How many workers can actively consume at most?', o: ['2', '4', '8', 'Unlimited'], a: 1, e: 'Each partition is consumed by one group member, so a 5th worker would be idle.' },
  { q: 'Watermark = newest event time − 5 s. Newest event: 10:02:07. Window 10:01–10:02, allowed lateness 10 s. What is the window\'s status?', o: ['open', 'closed (accepting late events)', 'final', 'deleted'], a: 1, e: 'Watermark = 10:02:02 ≥ end 10:02:00 → closed. Final only when watermark ≥ 10:02:10.' },
  { q: 'A worker crashes 4 s after its last checkpoint. What happens to those 4 s of events?', o: ['They are lost forever', 'They are counted twice', 'They are replayed from Kafka and counted exactly once in the restored state', 'They go to the DLQ'], a: 2, e: 'State and offset are checkpointed together, so the replay starts exactly where the restored state ends.' },
  { q: 'Consumer lag keeps growing. What is the most likely cause?', o: ['The producer is too slow', 'The processors cannot keep up (backpressure)', 'Redis is full', 'The dashboard is closed'], a: 1, e: 'Lag = produced − processed. Add workers (scale out) or make processing faster.' },
  { q: 'Which component makes the dashboard update without refreshing?', o: ['Prometheus', 'A WebSocket from FastAPI', 'Consul', 'The registry'], a: 1, e: 'The API pushes each new window result over a WebSocket.' },
  { q: 'Where is the window size configured so it can change without restarting anything?', o: ['In the Dockerfile', 'In Consul KV (control plane)', 'In Grafana', 'In Redis'], a: 1, e: 'Services watch Consul and apply new values within about a second.' },
  { q: 'What does a replication factor of 3 protect against?', o: ['Slow consumers', 'Losing data when one broker fails', 'Duplicate events', 'Malformed JSON'], a: 1, e: 'Each partition has copies on 3 brokers; if the leader dies an in-sync follower becomes leader.' },
  { q: 'An event with "response_time_ms": "fast" arrives. What happens?', o: ['It is counted as 0', 'Schema validation rejects it and it goes to the dead letter queue', 'The processor crashes', 'It is retried forever'], a: 1, e: 'Invalid events never touch the aggregates; they are parked in telemetry.dlq.' },
  { q: 'What does "p95 processing latency = 120 ms" mean?', o: ['The average is 120 ms', '95% of events were processed within 120 ms', '95 events took 120 ms', 'The maximum is 120 ms'], a: 1, e: 'Percentiles describe the distribution and reveal slow outliers.' },
];
