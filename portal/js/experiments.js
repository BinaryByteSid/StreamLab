// Guided experiments (virtual-lab style: Aim → Theory → Procedure → Observation → Quiz → Result).
import { $, api, act, esc, fmt, links, progress, route, sleep, toast } from './core.js';
import { kafkaLanes, partitionsTable, timeline, windowsTimeline, workersTable } from './widgets.js';
import { alertsList, ciView, correctness, lagChart, promChart, topology, traceView } from './widgets-more.js';

const cfg = (body) => api('/api/lab/config', { method: 'PUT', body });
const send = (body) => api('/api/lab/events', { method: 'POST', body });
const overview = () => api('/api/lab/overview');
const state = {}; // values remembered between steps of an experiment

async function firstRunningWorker() {
  const d = await api('/api/lab/processor');
  const w = d.supervisor.workers.find((x) => x.state === 'running' && (d.workers.find((s) => s.worker === x.worker)?.partitions.length));
  if (!w) throw new Error('No running worker with partitions right now; wait a few seconds.');
  return w.worker;
}

export const EXPERIMENTS = [
  {
    id: 'first-event', n: 1, title: 'Follow one event through the whole system', level: 'Beginner', time: '5 min', sections: [1, 2, 3, 4, 5],
    aim: 'See every component an event passes through: producer → Kafka partition → processor worker → windows → Redis → API → dashboard.',
    theory: 'Events are small JSON records. Kafka stores them in a partition chosen from the key. The processor assigns each event to a tumbling window and 6 sliding windows and updates their running totals.',
    live: ['trace'],
    steps: [
      { text: 'Send one event for <b>payment-service</b> with a latency of 999 ms (an easy number to spot).',
        action: { label: 'Send the event', run: async () => { const r = await send({ service: 'payment-service', response_time_ms: 999, status_code: 200 }); state.eid = r.events[0].event_id; return `Sent ${state.eid} → partition ${r.events[0].partition}, offset ${r.events[0].offset}`; } },
        observe: 'The answer tells you the Kafka partition and offset. Same service → always the same partition.',
        check: async () => !!state.eid },
      { text: 'Look at the journey below. Wait until step 3 says "Processed by processor-N".',
        check: async () => { if (!state.eid) return false; const d = await api(`/api/lab/trace/${state.eid}`); return !!d.trace.processed_at; },
        observe: 'Usually under a second. The worker that owns that partition processed it.' },
      { text: 'Open the <a href="#/dashboard">Live dashboard</a> in a new tab and find payment-service: its max latency in the current window is now at least 999 ms.',
        observe: 'The dashboard never reads raw events. It shows the aggregated window result that came from Redis via the WebSocket.' },
    ],
    quiz: [
      { q: 'Why did the event go to that specific partition?', o: ['Random', 'Its key (payment-service) is hashed to choose the partition', 'The least busy one', 'Partition 0 always'], a: 1, e: 'Same key → same partition, so per-service order and state stay on one worker.' },
      { q: 'How many windows did the event update (default settings)?', o: ['1', '2', '7', '60'], a: 2, e: '1 tumbling + 6 sliding (60s size / 10s slide).' },
    ],
  },
  {
    id: 'kafka-stream', n: 2, title: 'Watch the stream inside Kafka', level: 'Beginner', time: '6 min', sections: [2],
    aim: 'Understand topics, partitions, offsets and keys by watching real messages arrive, and compare with Redpanda Console.',
    theory: 'The topic telemetry.events has 4 partitions. Each message has a key (the service name) and gets the next offset in its partition.',
    live: ['lanes'],
    steps: [
      { text: 'Watch the 4 lanes below. Each card is a real Kafka message: offset, key and a summary. Click one to see its full JSON.',
        observe: 'Each partition only ever contains certain services. Offsets only grow, never change.' },
      { text: 'Switch the traffic to the <b>hot partition</b> scenario (80% of traffic is payment-service).',
        action: { label: 'Make payment-service hot', run: async () => { await cfg({ 'producer/scenario': 'hot_partition' }); return 'Scenario: hot_partition'; } },
        check: async () => (await overview()).config['producer/scenario'] === 'hot_partition',
        observe: 'One lane now moves much faster: that is a hot partition (data skew). Adding workers would not help because one partition can only be read by one worker.' },
      { text: 'Open <b>Redpanda Console</b> (button below) → Topics → telemetry.events → Messages. It is the same data in a professional Kafka UI. Also look at the Consumer Groups tab.', tool: 'console' },
      { text: 'Put the traffic back to normal.', action: { label: 'Back to normal', run: async () => { await cfg({ 'producer/scenario': 'normal' }); return 'Scenario: normal'; } },
        check: async () => (await overview()).config['producer/scenario'] === 'normal' },
    ],
    quiz: [
      { q: 'What uniquely identifies a message in Kafka?', o: ['Its key', 'Topic + partition + offset', 'Its timestamp', 'event_id'], a: 1, e: 'Offsets are per partition.' },
      { q: 'Why can a hot partition not be fixed by adding workers?', o: ['Workers are too slow', 'A partition is consumed by only one worker of the group', 'Kafka forbids it', 'It can'], a: 1, e: 'Parallelism is limited per partition; you need a better key strategy or more partitions.' },
    ],
  },
  {
    id: 'windows', n: 3, title: 'Tumbling vs sliding windows', level: 'Beginner', time: '8 min', sections: [3, 12],
    aim: 'See windows open, close and finalize, and change their size live.',
    theory: 'Tumbling windows do not overlap (each event in 1 window). Sliding windows overlap (each event in size ÷ slide windows). A window is closed when the watermark passes its end and final after the allowed lateness.',
    live: ['windows-tumbling', 'windows-sliding'],
    steps: [
      { text: 'Watch the <b>tumbling</b> timeline below: one bar per service per minute. Blue bars are open; they turn orange (closed) when the watermark (amber line) passes their end.',
        observe: 'The watermark trails "now" by about 5 seconds: the max out-of-orderness bound.' },
      { text: 'Look at the <b>sliding</b> timeline: overlapping bars, a new one every 10 seconds.' },
      { text: 'Make tumbling windows 20 seconds long and sliding windows 30 s with a 10 s slide.',
        action: { label: 'Apply 20s / 30s / 10s', run: async () => { await cfg({ 'window/tumbling_size_s': 20, 'window/sliding_size_s': 30, 'window/sliding_slide_s': 10 }); return 'Pushed to Consul. Workers apply it within ~1s'; } },
        check: async () => (await overview()).config['window/tumbling_size_s'] === 20,
        observe: 'New windows get the new size. Already open windows keep their old boundaries until they finish.' },
      { text: 'Restore the defaults (60 s tumbling, 60 s / 10 s sliding).',
        action: { label: 'Restore defaults', run: async () => { await cfg({ 'window/tumbling_size_s': 60, 'window/sliding_size_s': 60, 'window/sliding_slide_s': 10 }); return 'Defaults restored'; } },
        check: async () => (await overview()).config['window/tumbling_size_s'] === 60 },
    ],
    quiz: [
      { q: 'With size 30 s and slide 10 s, how many windows contain each event?', o: ['1', '3', '10', '30'], a: 1, e: '30 ÷ 10 = 3.' },
      { q: 'When does a window become final?', o: ['At its end time', 'When the watermark passes end + allowed lateness', 'After 1 hour', 'When Redis is full'], a: 1, e: 'Then its state is dropped and later events are "too late".' },
    ],
  },
  {
    id: 'late-events', n: 4, title: 'Out-of-order and late events', level: 'Intermediate', time: '8 min', sections: [3],
    aim: 'Send events with old timestamps and see how the watermark and allowed lateness decide what happens.',
    theory: 'Watermark = newest event time − 5 s. A window is closed when watermark ≥ end and final when watermark ≥ end + 10 s (allowed lateness). Events for final windows go to the dead letter queue.',
    live: ['trace', 'lanes-dlq'],
    steps: [
      { text: 'Send an <b>out-of-order</b> event: it happened 3 seconds ago (inside the 5 s bound).',
        action: { label: 'Send (3 s old)', run: async () => { const r = await send({ service: 'cart-service', response_time_ms: 77, delay_s: 3 }); state.eid = r.events[0].event_id; return `Sent ${state.eid}`; } },
        check: async () => { const d = await api(`/api/lab/trace/${state.eid}`).catch(() => null); return d?.trace.outcome === 'out_of_order' || d?.trace.outcome === 'on_time'; },
        observe: 'Outcome: out_of_order (or on_time). It still counts in its normal window.' },
      { text: 'Send a <b>late</b> event from 12 seconds ago. Its window may already be closed, but it is within the allowed lateness.',
        action: { label: 'Send (12 s old)', run: async () => { const r = await send({ service: 'cart-service', response_time_ms: 77, delay_s: 12 }); state.eid = r.events[0].event_id; return `Sent ${state.eid}`; } },
        check: async () => { const d = await api(`/api/lab/trace/${state.eid}`).catch(() => null); return !!d?.trace.processed_at; },
        observe: 'If its 10-second sliding windows had closed, it shows "late but accepted" and the window is re-emitted with status "updated".' },
      { text: 'Send a <b>very late</b> event from 3 minutes ago. All its windows are final.',
        action: { label: 'Send (180 s old)', run: async () => { const r = await send({ service: 'cart-service', response_time_ms: 77, delay_s: 180 }); state.eid = r.events[0].event_id; return `Sent ${state.eid}`; } },
        check: async () => { const d = await api(`/api/lab/trace/${state.eid}`).catch(() => null); return d?.trace.outcome === 'too_late'; },
        observe: 'Outcome: too_late. Look at the dead letter queue lane: a new record with reason "too_late".' },
      { text: 'Now raise the allowed lateness to 300 s and send another 180 s old event. What changes?',
        action: { label: 'Lateness 300 s + send', run: async () => { await cfg({ 'window/allowed_lateness_s': 300 }); await sleep(1500); const r = await send({ service: 'cart-service', response_time_ms: 77, delay_s: 180 }); state.eid = r.events[0].event_id; return 'Sent with lateness 300 s'; } },
        observe: 'Windows that are already final stay final, so this event is still too late. Only windows that close from now on will wait 300 s. Bigger lateness = more complete but slower final results and more state.' },
      { text: 'Restore allowed lateness to 10 s.', action: { label: 'Restore 10 s', run: async () => { await cfg({ 'window/allowed_lateness_s': 10 }); return 'Restored'; } },
        check: async () => (await overview()).config['window/allowed_lateness_s'] === 10 },
    ],
    quiz: [
      { q: 'Max out-of-orderness 5 s. Newest event 10:00:30. What is the watermark?', o: ['10:00:30', '10:00:25', '10:00:35', '10:00:20'], a: 1, e: '10:00:30 − 5 s.' },
      { q: 'Where do too-late events go?', o: ['They are silently dropped', 'To the dead letter queue topic', 'Back to the producer', 'Into the next window'], a: 1, e: 'Parked in telemetry.dlq for inspection, without corrupting old results.' },
    ],
  },
  {
    id: 'dirty-data', n: 5, title: 'Duplicates and invalid events', level: 'Beginner', time: '5 min', sections: [3],
    aim: 'Prove that duplicate and malformed events never distort the metrics.',
    theory: 'Schema validation (JSON schema from the Schema Registry) rejects broken events to the DLQ. Duplicate detection remembers recent event_ids.',
    live: ['trace', 'lanes-dlq'],
    steps: [
      { text: 'Send an event with a fixed id <code>lab-dup-demo-' + Date.now() + '</code>.',
        action: { label: 'Send original', run: async () => { state.dup = `lab-dup-${Date.now()}`; const r = await send({ service: 'order-service', response_time_ms: 50, event_id: state.dup }); state.eid = state.dup; return `Sent ${r.events[0].event_id}`; } },
        check: async () => { const d = await api(`/api/lab/trace/${state.dup}`).catch(() => null); return !!d?.trace.processed_at; } },
      { text: 'Send the <b>same event_id</b> again (a duplicate, like a retry from a flaky network).',
        action: { label: 'Send duplicate', run: async () => { await send({ service: 'order-service', response_time_ms: 50, event_id: state.dup }); state.eid = state.dup; return 'Duplicate sent'; } },
        check: async () => { const d = await api(`/api/lab/trace/${state.dup}`).catch(() => null); return d?.trace.outcome === 'duplicate'; },
        observe: 'The trace now says "duplicate": it was ignored and counted only once.' },
      { text: 'Send a <b>malformed</b> event (status_code = "OK" instead of a number).',
        action: { label: 'Send malformed', run: async () => { const r = await send({ service: 'order-service', malformed: 'wrong_type' }); state.eid = r.events[0].event_id; return 'Malformed event sent'; } },
        check: async () => { const d = await api(`/api/lab/trace/${state.eid}`).catch(() => null); return d?.trace.outcome === 'dead_letter'; },
        observe: 'Rejected by schema validation (reason schema_invalid) and visible in the DLQ lane with the exact error.' },
      { text: 'Turn on background chaos: 5% duplicates and 3% malformed events, then check the correctness table on the <a href="#/resilience">Fault tolerance</a> page after a minute or two: computed counts still match.',
        action: { label: 'Enable chaos', run: async () => { await cfg({ 'chaos/duplicate_pct': 5, 'chaos/malformed_pct': 3 }); return 'Chaos on'; } } },
      { text: 'Turn the chaos off again.', action: { label: 'Chaos off', run: async () => { await cfg({ 'chaos/duplicate_pct': 0, 'chaos/malformed_pct': 0 }); return 'Chaos off'; } },
        check: async () => (await overview()).config['chaos/duplicate_pct'] === 0 },
    ],
    quiz: [{ q: 'What field is used to detect duplicates?', o: ['service', 'event_time', 'event_id', 'status_code'], a: 2, e: 'event_id is unique per real event.' }],
  },
  {
    id: 'crash', n: 6, title: 'Crash a worker: rebalance and recovery', level: 'Intermediate', time: '8 min', sections: [3, 11],
    aim: 'Kill a processor worker without warning and watch Kafka move its partitions, the state restore from RocksDB, and the counts stay exactly right.',
    theory: 'Each worker checkpoints window state + Kafka offset atomically every 5 s. After a crash, another worker loads that checkpoint and replays the events after it. Result: no lost and no double-counted events in the state.',
    live: ['workers', 'timeline', 'correctness'],
    steps: [
      { text: 'Look at the workers table: each running worker owns some partitions.' },
      { text: 'Crash a worker (SIGKILL: it gets no chance to save anything).',
        action: { label: '💥 Crash a worker', run: async () => { const w = await firstRunningWorker(); state.crashed = w; await api(`/api/lab/processor/workers/${w}/crash`, { method: 'POST' }); return `${w} killed`; } },
        check: async () => !!state.crashed,
        observe: 'The timeline says the supervisor detected the crash. For ~6 s nobody reads those partitions (Kafka waits for the session timeout), so their lag grows.' },
      { text: 'Wait for the <b>rebalance</b>: the surviving worker takes the partitions and says "restored N windows from its RocksDB checkpoint … X events will be replayed".',
        check: async () => (await api('/api/lab/timeline?limit=30')).some((e) => e.kind === 'assign' && e.source === 'rocksdb'),
        observe: 'Replay is fine: state and offset were saved together, so replayed events are applied to exactly the state that did not include them yet.' },
      { text: 'After ~8 s self-healing restarts the crashed worker and partitions are spread out again (another rebalance).',
        check: async () => { const d = await api('/api/lab/processor'); return d.supervisor.workers.find((w) => w.worker === state.crashed)?.state === 'running'; } },
      { text: 'Check the correctness table in a minute or two: the window that contained the crash still matches the ground truth ✅.' },
    ],
    quiz: [
      { q: 'Why are replayed events not counted twice?', o: ['Kafka deletes them', 'The restored state is from the same moment as the restored offset', 'Redis ignores them', 'They are counted twice'], a: 1, e: 'Atomic checkpoint of state + offset.' },
      { q: 'Why does it take ~6 s before partitions move after a crash?', o: ['Docker is slow', 'Kafka waits for the consumer session timeout', 'Python startup', 'Random'], a: 1, e: 'A crashed consumer sends no goodbye, so the coordinator waits for missing heartbeats.' },
    ],
  },
  {
    id: 'scaling', n: 7, title: 'Backpressure and auto scaling', level: 'Intermediate', time: '10 min', sections: [3, 9, 11],
    aim: 'Make processing slow, watch consumer lag grow, and see the autoscaler add workers until the lag drains.',
    theory: 'Lag = produced − processed. When it passes the scale-up threshold the autoscaler adds a worker (max 4 = number of partitions); when it stays low for the cooldown it removes one.',
    live: ['lag', 'workers', 'timeline'],
    steps: [
      { text: 'Make every event take 25 ms of work and raise the rate to 60 events/s. processor-2 owns partitions 1 and 3, which carry ~71% of the traffic: 43 events/s × 25 ms = more than one second of work per second. It cannot keep up.',
        action: { label: 'Create backpressure', run: async () => { await cfg({ 'processor/simulated_work_ms': 25, 'producer/rate_eps': 60, 'scaling/mode': 'auto' }); return 'Processing slowed down'; } },
        check: async () => (await overview()).config['processor/simulated_work_ms'] === 25 },
      { text: 'Watch the lag chart climb. When live lag passes 400, the autoscaler scales up (see timeline: "Autoscaler scaled UP").',
        check: async () => (await api('/api/lab/timeline?limit=50')).some((e) => e.kind === 'scale' && e.direction === 'up'),
        observe: 'Each scale-up causes a rebalance. The cooldown (30 s) stops it from adding workers too fast.' },
      { text: 'With 3 workers partitions 1 and 3 get a worker each and the lag drains back towards zero.',
        check: async () => { const o = await overview(); return o.workers.length >= 3 && (o.supervisor?.lag?.live_total ?? 999) < 200; },
        observe: 'Partition 2 carries no traffic at all (no service name hashes to it) and partition 1 carries 43%. That is data skew: at 100 events/s even 4 workers could not save partition 1, because one partition is always read by exactly one worker.' },
      { text: 'Remove the slowness and go back to 25 events/s. After the cooldown the autoscaler scales back down to 2.',
        action: { label: 'Back to normal', run: async () => { await cfg({ 'processor/simulated_work_ms': 0, 'producer/rate_eps': 25 }); return 'Normal load'; } },
        check: async () => (await overview()).config['processor/simulated_work_ms'] === 0 },
    ],
    quiz: [{ q: 'Why is the maximum 4 workers?', o: ['License limit', 'The topic has 4 partitions; a 5th worker would be idle', 'CPU limit', 'Redis limit'], a: 1, e: 'One partition → one consumer in the group.' }],
  },
  {
    id: 'broker', n: 8, title: 'Kill a Kafka broker: replication', level: 'Advanced', time: '7 min', sections: [2, 11],
    aim: 'Stop one of the 3 brokers and see leaders move to the in-sync replicas while no data is lost.',
    theory: 'Every partition has 3 replicas. The leader handles reads and writes; if it dies, an in-sync follower becomes leader. Producers use acks=all so a write is confirmed only when replicas have it.',
    live: ['topology', 'timeline'],
    steps: [
      { text: 'Note which broker leads which partition in the topology below.' },
      { text: 'Stop broker <b>redpanda-1</b>.',
        action: { label: 'Stop redpanda-1', run: async () => { await api('/api/lab/chaos/redpanda-1/stop', { method: 'POST' }); return 'redpanda-1 stopped'; } },
        check: async () => { const t = await api('/api/lab/topology'); return !t.brokers.some((b) => b.id === 1); },
        observe: 'Broker 1 disappears. Partitions it led get a new leader, ISR shrinks to 2 and they show "under-replicated". Events keep flowing (check the Kafka page).' },
      { text: 'Within ~30 s Prometheus fires the <b>KafkaBrokerDown</b> alert (see <a href="#/observability">Observability</a>).' },
      { text: 'Start the broker again. It catches up and rejoins the ISR.',
        action: { label: 'Start redpanda-1', run: async () => { await api('/api/lab/chaos/redpanda-1/start', { method: 'POST' }); return 'redpanda-1 starting'; } },
        check: async () => { const t = await api('/api/lab/topology'); return t.brokers.some((b) => b.id === 1) && t.topics[0].partitions.every((p) => !p.under_replicated); } },
    ],
    quiz: [{ q: 'With replication factor 3, how many brokers can fail without losing data?', o: ['0', '1 (2 with care)', '3', 'All'], a: 1, e: 'As long as one in-sync replica survives, the data is safe.' }],
  },
  {
    id: 'control-plane', n: 9, title: 'Change the system live from the control plane', level: 'Beginner', time: '5 min', sections: [6],
    aim: 'Use the Admin UI and see the change land in Consul and every service without a restart.',
    theory: 'Settings are keys in Consul KV (swma/config/...). Services hold a blocking query open and receive the new value within ~1 s.',
    steps: [
      { text: 'Open the <a href="#/control">Control Plane</a> page and change "Periodic trigger interval" to 5 s (or use the button).',
        action: { label: 'Set emit interval 5 s', run: async () => { await cfg({ 'trigger/emit_interval_s': 5 }); return 'Done'; } },
        check: async () => (await overview()).config['trigger/emit_interval_s'] === 5,
        observe: 'Window results for open windows now arrive every 5 s instead of 2 s (watch the dashboard).' },
      { text: 'Open the <b>Consul UI</b> → Key/Value → swma/config/trigger/emit_interval_s. The value is 5.', tool: 'consul' },
      { text: 'The same can be done from a terminal (the "CLI" in the diagram): <pre><code>docker compose exec api-1 python -m swma.cli set trigger/emit_interval_s 2</code></pre> Or click:',
        action: { label: 'Set back to 2 s', run: async () => { await cfg({ 'trigger/emit_interval_s': 2 }); return 'Done'; } },
        check: async () => (await overview()).config['trigger/emit_interval_s'] === 2 },
      { text: 'Every change is written to the audit log (Control Plane page and Grafana → Logs dashboard).' },
    ],
    quiz: [{ q: 'Why keep configuration in Consul rather than in code?', o: ['It is faster', 'It can change at runtime for all instances without redeploying', 'Docker requires it', 'Security'], a: 1, e: 'Dynamic configuration is the job of the control plane.' }],
  },
  {
    id: 'api', n: 10, title: 'API keys, JWT, rate limiting and load balancing', level: 'Intermediate', time: '6 min', sections: [4, 9],
    aim: 'Call the public API, get blocked without a key, hit the rate limit, and see two API replicas share the load.',
    theory: 'Read endpoints need an API key; admin actions need a JWT. A token bucket in Redis allows 5 req/s with a burst of 10 per key. nginx (the "ALB") alternates between api-1 and api-2.',
    live: ['api-test'],
    steps: [
      { text: 'Use the API tester below: call <code>/api/v1/metrics/payment-service</code> without a key → <b>401</b>.' },
      { text: 'Call it with the demo key <code>lab-public-key</code> → <b>200</b>. Look at <code>X-Served-By</code>: api-1 or api-2.' },
      { text: 'Fire 30 requests at once → the first ~10 succeed (burst), the rest get <b>429 Too Many Requests</b>.' },
      { text: 'Open the Swagger docs (button below), click <b>Authorize</b>, enter the key and try the endpoints.', tool: 'api_docs' },
    ],
    quiz: [{ q: 'What does HTTP 429 mean?', o: ['Not found', 'Too many requests (rate limited)', 'Unauthorized', 'Server error'], a: 1, e: 'Retry later; the Retry-After header says when.' }],
  },
  {
    id: 'alerts', n: 11, title: 'Make an alert fire', level: 'Intermediate', time: '6 min', sections: [10, 12],
    aim: 'Degrade a service and follow the signal from window metrics → Prometheus → alert rule → Alertmanager → API webhook → dashboard.',
    theory: 'The processor exports the latest window averages to Prometheus. A rule compares them with the threshold from Consul; Alertmanager sends the alert to the API, which pushes it to your browser.',
    live: ['alerts', 'latency-chart'],
    steps: [
      { text: 'Start the <b>latency degradation</b> scenario: payment-service gets slower and slower (up to 6×).',
        action: { label: 'Degrade payment-service', run: async () => { await cfg({ 'producer/scenario': 'latency_degradation' }); return 'Scenario started'; } },
        check: async () => (await overview()).config['producer/scenario'] === 'latency_degradation' },
      { text: 'Watch the latency chart cross the red 500 ms line. About 20 s later <b>HighServiceLatency</b> fires.',
        check: async () => (await api('/api/lab/alerts')).active.some((a) => a.labels.alertname === 'HighServiceLatency') },
      { text: 'Open Alertmanager and Grafana to see the same alert there.', tool: 'alertmanager' },
      { text: 'Stop the scenario; the alert resolves after the windows recover.',
        action: { label: 'Back to normal', run: async () => { await cfg({ 'producer/scenario': 'normal' }); return 'Normal'; } },
        check: async () => (await overview()).config['producer/scenario'] === 'normal' },
    ],
    quiz: [{ q: 'Which component decides that an alert should fire?', o: ['Grafana', 'Prometheus (alert rules)', 'Redis', 'Kafka'], a: 1, e: 'Prometheus evaluates rules; Alertmanager routes notifications.' }],
  },
  {
    id: 'cicd', n: 12, title: 'Run the CI/CD pipeline', level: 'Beginner', time: '5 min', sections: [7, 8, 9],
    aim: 'Run the real pipeline: tests, security scan, Docker builds, push to the registry and a zero-downtime rolling deploy.',
    theory: 'CI/CD automates quality checks and delivery. Images are tagged with the git commit and stored in a registry; a rolling deploy restarts one replica at a time behind the load balancer.',
    live: ['ci'],
    steps: [
      { text: 'Click <b>Run the pipeline</b> below and watch the 6 stages (the first build takes ~1–2 minutes).',
        check: async () => ['passed', 'failed'].includes((await api('/api/lab/ci/latest')).status) },
      { text: 'Open the registry UI: the images <code>swma/api</code>, <code>swma/processor</code>, … now have a tag for your commit.', tool: 'registry' },
      { text: 'Read the deploy log: how many requests failed during the rolling restart? (Should be 0.)' },
      { text: 'The same pipeline for GitHub is in <code>.github/workflows/ci.yml</code> and <code>cd.yml</code> (pushes to Amazon ECR, deploys to EC2).' },
    ],
    quiz: [{ q: 'What makes the deploy zero-downtime?', o: ['Restarting everything at once', 'Restarting one API replica at a time while the load balancer uses the other', 'Caching', 'Luck'], a: 1, e: 'Rolling update behind a load balancer.' }],
  },
  {
    id: 'disaster', n: 13, title: 'Disaster recovery from S3', level: 'Advanced', time: '6 min', sections: [3, 9, 11],
    aim: 'Lose every worker and the whole local state store at once, and recover from the S3 backups.',
    theory: 'Every 30 s each worker copies its full state snapshot to S3 (bucket swma-checkpoints). If the local RocksDB is gone, the new owner restores from S3 and replays from the snapshot\'s offset.',
    live: ['workers', 'timeline'],
    steps: [
      { text: 'Look at the S3 backups on the <a href="#/resilience">Fault tolerance</a> page (or the S3 console).', tool: 's3' },
      { text: 'Trigger the disaster: all workers are killed and /state is deleted.',
        action: { label: '🔥 Simulate disk failure', run: async () => { await api('/api/lab/processor/disaster', { method: 'POST' }); state.disaster = Date.now(); return 'Disk wiped'; } },
        check: async () => !!state.disaster },
      { text: 'Watch the timeline: the restarted workers say "restored … from the S3 backup".',
        check: async () => (await api('/api/lab/timeline?limit=40')).some((e) => e.kind === 'assign' && e.source === 's3') },
    ],
    quiz: [{ q: 'Compared with the 5 s local checkpoint, what is the cost of restoring from a 30 s S3 backup?', o: ['None', 'More events must be replayed from Kafka', 'Data is lost', 'Kafka must be restarted'], a: 1, e: 'Kafka still has the events, so they are replayed from the older offset.' }],
  },
  {
    id: 'retries', n: 14, title: 'Transient failures and retries', level: 'Intermediate', time: '4 min', sections: [11],
    aim: 'Make Redis writes fail randomly and see the retry mechanism with exponential backoff keep results flowing.',
    theory: 'A transient failure goes away by itself (a network blip). Retrying with growing waits (0.1 s, 0.2 s, 0.4 s…) usually succeeds without overloading the target.',
    live: ['retries-chart'],
    steps: [
      { text: 'Make 40% of Redis writes fail.',
        action: { label: 'Inject 40% failures', run: async () => { await cfg({ 'processor/sink_failure_pct': 40 }); return 'Failures injected'; } },
        check: async () => (await overview()).config['processor/sink_failure_pct'] === 40 },
      { text: 'The retries chart climbs but failures stay at 0, and the dashboard keeps updating.' },
      { text: 'Turn it off.', action: { label: 'Stop failures', run: async () => { await cfg({ 'processor/sink_failure_pct': 0 }); return 'Stopped'; } },
        check: async () => (await overview()).config['processor/sink_failure_pct'] === 0 },
    ],
    quiz: [{ q: 'Why use exponential backoff?', o: ['To be faster', 'To avoid hammering a struggling service', 'It is required by Redis', 'To lose data'], a: 1, e: 'Growing pauses give the target time to recover.' }],
  },
];

// ------------------------------------------------------------- live panels
async function apiTester(el) {
  el.innerHTML = `<div class="row"><button class="btn" data-t="nokey">GET without key</button><button class="btn" data-t="key">GET with key</button><button class="btn danger" data-t="burst">Fire 30 requests</button></div><div id="at-out" style="margin-top:10px"></div>`;
  const out = $('#at-out', el);
  const call = async (key) => {
    const r = await fetch('/api/v1/metrics/payment-service', { headers: key ? { 'X-API-Key': 'lab-public-key' } : {} });
    return { status: r.status, by: r.headers.get('X-Served-By'), left: r.headers.get('X-RateLimit-Remaining') };
  };
  el.querySelectorAll('[data-t]').forEach((b) => {
    b.onclick = async () => {
      if (b.dataset.t === 'burst') {
        const res = await Promise.all(Array.from({ length: 30 }, () => call(true)));
        const c = (s) => res.filter((r) => r.status === s).length;
        out.innerHTML = `<p><span class="pill ok">${c(200)} × 200 OK</span> <span class="pill bad">${c(429)} × 429 Too Many Requests</span></p>
          <p class="small">Served by: api-1 × ${res.filter((r) => r.by === 'api-1').length}, api-2 × ${res.filter((r) => r.by === 'api-2').length}</p>
          <div class="row">${res.map((r) => `<span class="pill ${r.status === 200 ? 'ok' : 'bad'}" title="${r.by}">${r.status}</span>`).join('')}</div>`;
      } else {
        const r = await call(b.dataset.t === 'key');
        out.innerHTML = `<p>HTTP <b>${r.status}</b> ${r.status === 401 ? '(Unauthorized: no API key)' : ''} · X-Served-By: <code>${esc(r.by)}</code> ${r.left !== null ? `· X-RateLimit-Remaining: ${r.left}` : ''}</p>`;
      }
    };
  });
}

function mountLive(key, el, ctx) {
  const box = (title) => { const c = document.createElement('div'); c.className = 'card'; c.innerHTML = `<h3>${title}</h3><div></div>`; el.appendChild(c); return c.lastChild; };
  switch (key) {
    case 'lanes': return kafkaLanes(box('Live Kafka stream: telemetry.events'), ctx);
    case 'lanes-dlq': return kafkaLanes(box('Dead letter queue: telemetry.dlq'), ctx, { topic: 'telemetry.dlq', perLane: 4 });
    case 'workers': return workersTable(box('Processor workers'), ctx);
    case 'partitions': return partitionsTable(box('Partitions'), ctx);
    case 'timeline': return timeline(box('What just happened'), ctx, { limit: 12 });
    case 'windows-tumbling': return windowsTimeline(box('Tumbling windows (active)'), ctx, { type: 'tumbling' });
    case 'windows-sliding': return windowsTimeline(box('Sliding windows of payment-service (active)'), ctx, { type: 'sliding', service: 'payment-service' });
    case 'correctness': return correctness(box('Correctness check'), ctx);
    case 'lag': return lagChart(box('Consumer lag and workers (last 10 min)'), ctx);
    case 'topology': return topology(box('Kafka cluster'), ctx);
    case 'alerts': return alertsList(box('Alerts'), ctx);
    case 'ci': return ciView(box('CI/CD pipeline'), ctx);
    case 'api-test': return apiTester(box('API tester'));
    case 'latency-chart': return promChart(box('Average latency per service (60 s sliding window)'), ctx, [['swma_window_avg_latency_ms{window_type="sliding"}']], { legend: (m) => m.service, unit: ' ms' });
    case 'retries-chart': return promChart(box('Redis write retries and failures / s'), ctx, [['sum(rate(swma_sink_retries_total[30s]))', 'retries'], ['sum(rate(swma_sink_failures_total[30s]))', 'failures']]);
    case 'trace': {
      const el2 = box('Event journey (last event you sent)');
      ctx.every(() => (state.eid ? traceView(el2, state.eid) : (el2.innerHTML = '<p class="muted">Send an event to see its journey here.</p>')), 1500);
      return null;
    }
    default: return null;
  }
}

// ----------------------------------------------------------------- routes
route(/^\/experiments$/, async (main) => {
  const prog = progress.all();
  main.innerHTML = `<div class="page-head"><div class="kicker" style="color:var(--s3)">Hands-on</div><h1>Experiments</h1>
    <p>Each experiment works on the real running system. Follow the steps; the lab checks automatically when a step is done (✓). Actions that change the system ask you to sign in as the lab admin once.</p></div>
    <div class="grid g3">${EXPERIMENTS.map((e) => `<a class="card exp-card" href="#/experiments/${e.id}">
      <div class="row between"><span class="tag">Exp ${e.n}</span>${prog[`exp:${e.id}`]?.done ? '<span class="pill ok">done</span>' : ''}</div>
      <b>${esc(e.title)}</b><span class="small">${esc(e.aim)}</span><span class="meta">${e.level} · ${e.time} · diagram sections ${e.sections.join(', ')}</span></a>`).join('')}</div>`;
});

route(/^\/experiments\/([\w-]+)$/, async (main, ctx) => {
  const e = EXPERIMENTS.find((x) => x.id === ctx.params[0]);
  if (!e) { main.innerHTML = '<div class="card">Experiment not found.</div>'; return; }
  Object.keys(state).forEach((k) => delete state[k]);
  const L = await links();
  const prog = progress.get(`exp:${e.id}`);
  const done = new Set(prog.steps || []);
  main.innerHTML = `<div class="page-head"><a href="#/experiments" class="small">← All experiments</a><div class="kicker" style="color:var(--s3)">Experiment ${e.n} · ${e.level} · ${e.time}</div><h1>${esc(e.title)}</h1></div>
    <div class="tabs" role="tablist">${['Aim & theory', 'Procedure', 'Quiz', 'Result'].map((t, i) => `<button role="tab" data-tab="${i}" class="${i === 1 ? 'on' : ''}">${t}</button>`).join('')}</div>
    <div data-pane="0" hidden><div class="card"><h2>Aim</h2><p>${esc(e.aim)}</p><h2>Theory</h2><p>${e.theory}</p><p class="small muted">Architecture sections: ${e.sections.join(', ')}. Need more background? See the <a href="#/learn">theory lessons</a>.</p></div></div>
    <div data-pane="1"><ol class="steps" id="steps"></ol><div id="live"></div></div>
    <div data-pane="2" hidden><div id="qz"></div></div>
    <div data-pane="3" hidden><div class="card" id="result"></div></div>`;
  main.querySelectorAll('[data-tab]').forEach((b) => {
    b.onclick = () => {
      main.querySelectorAll('[data-tab]').forEach((x) => x.classList.toggle('on', x === b));
      main.querySelectorAll('[data-pane]').forEach((p) => { p.hidden = p.dataset.pane !== b.dataset.tab; });
      if (b.dataset.tab === '3') drawResult();
    };
  });
  const steps = $('#steps');
  steps.innerHTML = e.steps.map((s, i) => `<li class="step ${done.has(i) ? 'done' : ''}" data-i="${i}">
    <div>${s.text}</div>
    ${s.action || s.tool ? `<div class="row" style="margin-top:8px">${s.action ? `<button class="btn primary sm" data-act="${i}">${esc(s.action.label)}</button>` : ''}${s.tool && L[s.tool] ? `<a class="btn sm" href="${esc(L[s.tool])}" target="_blank" rel="noopener">Open ${esc(s.tool)} ↗</a>` : ''}<span class="small muted" data-msg></span></div>` : ''}
    ${s.observe ? `<div class="observe">👀 <b>Observe:</b> ${s.observe}</div>` : ''}
    ${!s.check ? `<button class="link small" data-mark="${i}" style="margin-top:6px">mark step as done</button>` : ''}</li>`).join('');
  const markDone = (i) => {
    done.add(i);
    steps.querySelector(`[data-i="${i}"]`).classList.add('done');
    const all = done.size >= e.steps.length;
    progress.set(`exp:${e.id}`, { steps: [...done], done: all || prog.done });
    if (all) toast(`Experiment ${e.n} complete! Take the quiz tab.`, 'ok');
  };
  steps.querySelectorAll('[data-act]').forEach((b) => {
    const i = +b.dataset.act;
    b.onclick = async () => {
      const msg = await act(b, e.steps[i].action.run);
      if (msg) { b.parentElement.querySelector('[data-msg]').textContent = msg; if (!e.steps[i].check) markDone(i); }
    };
  });
  steps.querySelectorAll('[data-mark]').forEach((b) => { b.onclick = () => markDone(+b.dataset.mark); });
  ctx.every(async () => {
    for (const [i, s] of e.steps.entries()) {
      if (s.check && !done.has(i)) { try { if (await s.check()) markDone(i); } catch { /* not yet */ } }
    }
  }, 2500);
  const live = $('#live');
  (e.live || []).forEach((k) => mountLive(k, live, ctx));
  const { renderQuiz } = await import('./pages-main.js');
  renderQuiz($('#qz'), e.quiz, `quiz:${e.id}`);
  const drawResult = () => {
    const p = progress.get(`exp:${e.id}`), q = progress.get(`quiz:${e.id}`);
    $('#result').innerHTML = `<h2>Result</h2><p>Steps completed: <b>${(p.steps || []).length} / ${e.steps.length}</b></p><p>Quiz: <b>${q.score ?? '–'} / ${q.of ?? e.quiz.length}</b></p>
      <p>${(p.steps || []).length >= e.steps.length ? `✅ You have successfully performed: <i>${esc(e.aim)}</i>` : 'Finish the remaining steps in the Procedure tab.'}</p>
      ${EXPERIMENTS[e.n] ? `<a class="btn primary" href="#/experiments/${EXPERIMENTS[e.n].id}">Next: ${esc(EXPERIMENTS[e.n].title)} →</a>` : '<a class="btn primary" href="#/quiz">Final quiz →</a>'}`;
  };
});

export { fmt };
