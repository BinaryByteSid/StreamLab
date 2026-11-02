// Control plane, observability, DevOps, cloud, resilience and the tools page.
import { $, api, act, esc, fmt, json, links, route } from './core.js';
import { showJson, statusGrid, timeline, workersTable } from './widgets.js';
import { alertsList, ciView, correctness, lagChart, promChart, topology } from './widgets-more.js';

const head = (n, color, title, text) => `<div class="page-head"><div class="kicker" style="color:${color}">Section ${n}</div><h1>${title}</h1><p>${text}</p></div>`;

// ----------------------------------------------------------- control plane
route(/^\/control$/, async (main, ctx) => {
  const L = await links();
  main.innerHTML = `${head(6, 'var(--s6)', 'Control plane: configuration &amp; management', 'Every setting below is a key in Consul KV. When you save, the API writes it to Consul and every service watching Consul applies it within about a second. No restarts.')}
    <div class="card row between"><span id="cfg-ver" class="small muted"></span><div class="row"><a class="btn" href="${esc(L.consul)}/ui/dc1/kv/swma/config/" target="_blank" rel="noopener">Open Consul UI ↗</a><button class="btn danger" id="reset">Reset everything to defaults</button></div></div>
    <div id="forms"></div>
    <div class="grid g2"><div class="card"><h2>Audit log (who changed what)</h2><div id="audit" class="scroll small"></div></div>
      <div class="card"><h2>Same thing from the CLI</h2><p>The diagram's "Admin UI / CLI": every change you make here can be made from a terminal.</p>
      <pre><code>docker compose exec api-1 python -m swma.cli list
docker compose exec api-1 python -m swma.cli set window/tumbling_size_s 30
docker compose exec api-1 python -m swma.cli reset</code></pre>
      <h3>Service registry</h3><div id="reg" class="scroll small"></div></div></div>`;
  const draw = async () => {
    const d = await api('/api/lab/config');
    $('#cfg-ver').textContent = `Config version (Consul ModifyIndex) ${d.version} · ${d.connected ? 'connected to Consul' : 'Consul unreachable, showing defaults'}`;
    const groups = [...new Set(d.settings.map((s) => s.group))];
    $('#forms').innerHTML = groups.map((g) => `<div class="card"><h2>${esc(g)}</h2><div class="form-grid" style="grid-template-columns:repeat(auto-fill,minmax(260px,1fr))">
      ${d.settings.filter((s) => s.group === g).map((s) => {
        let input;
        if (s.kind === 'bool') input = `<select data-key="${s.key}"><option value="true" ${s.value ? 'selected' : ''}>on</option><option value="false" ${!s.value ? 'selected' : ''}>off</option></select>`;
        else if (s.kind === 'enum') input = `<select data-key="${s.key}">${s.options.map((o) => `<option ${o === s.value ? 'selected' : ''}>${o}</option>`).join('')}</select>`;
        else input = `<input type="number" data-key="${s.key}" value="${s.value}" min="${s.min ?? ''}" max="${s.max ?? ''}">`;
        return `<label title="${esc(s.help)}"><b style="color:var(--ink)">${esc(s.label)}</b> ${s.value !== s.default ? '<span class="pill warn">changed</span>' : ''}${input}<span class="small">${esc(s.help)}</span><br><code class="small">${esc(s.consul_key)}</code></label>`;
      }).join('')}</div><div class="row end" style="margin-top:10px"><button class="btn primary" data-save="${esc(g)}">Save ${esc(g)} → push to Consul</button></div></div>`).join('');
    $('#forms').querySelectorAll('[data-save]').forEach((b) => {
      b.onclick = () => {
        const body = {};
        b.closest('.card').querySelectorAll('[data-key]').forEach((i) => { body[i.dataset.key] = i.value; });
        act(b, () => api('/api/lab/config', { method: 'PUT', body }), (r) => (Object.keys(r.applied).length ? `Applied: ${Object.entries(r.applied).map(([k, v]) => `${k} ${v.from}→${v.to}`).join(', ')}` : 'Nothing changed')).then((r) => r && setTimeout(draw, 800));
      };
    });
  };
  $('#reset').onclick = (e) => act(e.target, () => api('/api/lab/config/reset', { method: 'POST' }), 'All settings back to defaults').then(() => setTimeout(draw, 800));
  await draw();
  ctx.every(async () => {
    const a = await api('/api/lab/audit?limit=30').catch(() => []);
    $('#audit').innerHTML = a.map((x) => `<div class="ev"><time>${fmt.time(x.at)}</time><b>${esc(x.user)}</b> (${esc(x.source)}): ${Object.entries(x.changes).map(([k, v]) => `<code>${esc(k)}</code> ${esc(v.from)} → <b>${esc(v.to)}</b>`).join(', ')}</div>`).join('') || '<p class="muted">No changes yet.</p>';
    const s = await api('/api/explore/consul/services').catch(() => []);
    $('#reg').innerHTML = `<table><tbody>${s.filter((x) => x.service).map((x) => `<tr><td>${esc(x.service)}</td><td><span class="pill ${x.status === 'passing' ? 'ok' : 'bad'}">${esc(x.status)}</span></td></tr>`).join('')}</tbody></table>`;
  }, 5000);
});

// ----------------------------------------------------------- observability
route(/^\/observability$/, async (main, ctx) => {
  const L = await links();
  const g = (uid) => `${L.grafana}/d/${uid}?orgId=1&kiosk&refresh=5s&theme=light`;
  main.innerHTML = `${head(10, 'var(--s10)', 'Observability &amp; monitoring', 'The pipeline monitors itself: Prometheus scrapes numbers from every service every 5 s, Loki stores the logs (in S3), Grafana draws both, and Alertmanager sends alerts to the API.')}
    <div class="card"><div class="row between"><div class="tabs" id="gtabs" style="margin:0;border:0">
      <button data-g="streamlab-pipeline" class="on">Pipeline overview</button><button data-g="streamlab-services">Microservice metrics</button><button data-g="streamlab-infra">Kafka, Redis, API, containers</button><button data-g="streamlab-logs">Logs</button></div>
      <div class="row"><a class="btn sm" href="${esc(L.grafana)}" target="_blank" rel="noopener">Grafana ↗</a><a class="btn sm" href="${esc(L.prometheus)}" target="_blank" rel="noopener">Prometheus ↗</a><a class="btn sm" href="${esc(L.alertmanager)}" target="_blank" rel="noopener">Alertmanager ↗</a></div></div>
      <iframe class="embed" id="gframe" title="Grafana dashboard" src="${esc(g('streamlab-pipeline'))}"></iframe>
      <p class="small muted">Embedded Grafana (anonymous viewer). Admin login in the full UI: admin / streamlab.</p></div>
    <div class="grid g2"><div class="card"><h2>Alerts</h2><div id="alerts"></div></div>
      <div class="card"><h2>Ask Prometheus (PromQL)</h2>
        <div class="row"><select id="pq" style="flex:1">
          <option>sum(rate(swma_producer_events_total[1m]))</option><option>swma_group_lag</option><option>swma_window_avg_latency_ms{window_type="sliding"}</option>
          <option>histogram_quantile(0.95, sum(rate(swma_processing_latency_seconds_bucket[1m])) by (le))</option><option>up</option><option>swma_container_memory_bytes</option></select><button class="btn sm primary" id="pgo">Run</button></div>
        <div id="pres" class="scroll small"></div>
        <h3 style="margin-top:12px">Scrape targets</h3><div id="targets" class="scroll small"></div></div></div>
    <div class="card"><div class="row between"><h2>Logs (Loki)</h2><div class="row"><select id="lq"><option value='{kind=~".+"}'>all logs</option><option value='{kind="error"}'>errors only</option><option value='{kind="audit"}'>audit log</option><option value='{service="processor"}'>processor</option><option value='{service="api"}'>api</option></select></div></div><div id="logs" class="scroll small"></div></div>`;
  $('#gtabs').querySelectorAll('[data-g]').forEach((b) => { b.onclick = () => { $('#gtabs').querySelectorAll('button').forEach((x) => x.classList.toggle('on', x === b)); $('#gframe').src = g(b.dataset.g); }; });
  alertsList($('#alerts'), ctx);
  const run = async () => {
    const r = await api(`/api/explore/prometheus/query?query=${encodeURIComponent($('#pq').value)}`).catch((e) => ({ error: e.message }));
    const rows = r?.data?.result || [];
    $('#pres').innerHTML = rows.length ? `<table><tbody>${rows.map((x) => `<tr><td><code>${esc(JSON.stringify(x.metric))}</code></td><td class="num"><b>${fmt.num(+x.value[1], 2)}</b></td></tr>`).join('')}</tbody></table>` : `<p class="muted">${esc(r.error || 'no data')}</p>`;
  };
  $('#pgo').onclick = run; run();
  ctx.every(async () => {
    const t = await api('/api/explore/prometheus/targets').catch(() => []);
    $('#targets').innerHTML = `<table><tbody>${t.map((x) => `<tr><td>${esc(x.job)}</td><td>${esc(x.instance)}</td><td><span class="pill ${x.health === 'up' ? 'ok' : 'bad'}">${esc(x.health)}</span></td></tr>`).join('')}</tbody></table>`;
  }, 10000);
  const logs = async () => {
    const d = await api(`/api/explore/loki?query=${encodeURIComponent($('#lq').value)}&limit=60`).catch(() => []);
    $('#logs').innerHTML = `<table><tbody>${d.map((l) => `<tr><td>${fmt.time(l.line.ts)}</td><td><span class="pill ${l.labels.level === 'error' ? 'bad' : l.labels.kind === 'audit' ? 'purple' : ''}">${esc(l.labels.service)}</span></td><td>${esc(l.line.instance || '')}</td><td>${esc(l.line.msg)}${l.line.changes ? ` <code>${esc(JSON.stringify(l.line.changes))}</code>` : ''}</td></tr>`).join('') || '<tr><td class="muted">No logs</td></tr>'}</tbody></table>`;
  };
  $('#lq').onchange = logs;
  ctx.every(logs, 5000);
});

// ----------------------------------------------------------------- devops
route(/^\/devops$/, async (main, ctx) => {
  const L = await links();
  main.innerHTML = `${head('7 · 8', 'var(--s7)', 'DevOps, CI/CD &amp; container registry', 'Code commit → build &amp; test → security scan → build Docker image → push to registry → deploy. Here it really runs, on your machine, against the code in this folder.')}
    <div class="card"><h2>CI/CD pipeline</h2><div id="ci"></div></div>
    <div class="grid g2"><div class="card"><div class="row between"><h2>Container registry</h2><a class="btn sm" href="${esc(L.registry)}" target="_blank" rel="noopener">Registry UI ↗</a></div><p class="small muted">A local Docker registry standing in for Docker Hub / Amazon ECR. Each pipeline run pushes images tagged with the git commit.</p><div id="reg"></div></div>
      <div class="card"><h2>Automated tests in this project</h2><ul class="small">
        <li><b>Unit</b>: window boundaries, sliding membership, aggregation maths (services/tests/unit)</li>
        <li><b>Window correctness</b>: engine vs a batch reference implementation on 600 shuffled events</li>
        <li><b>Out-of-order &amp; late</b>, <b>duplicate</b> and <b>failure recovery</b> (checkpoint → crash → replay = exact counts)</li>
        <li><b>Integration / e2e</b>: tests/e2e against the running stack (STREAMLAB_E2E=1)</li>
        <li><b>Load</b>: raise the producer to 1000–5000 events/s and watch lag (Experiment 7)</li></ul>
        <h3>The same pipeline on GitHub</h3><p class="small"><code>.github/workflows/ci.yml</code> (lint, tests, bandit, Trivy, build) and <code>cd.yml</code> (push to Amazon ECR, deploy to EC2 over SSM).</p></div></div>`;
  ciView($('#ci'), ctx);
  ctx.every(async () => {
    const r = await api('/api/explore/registry').catch(() => []);
    $('#reg').innerHTML = r.length ? `<table><thead><tr><th>Image</th><th>Tags</th></tr></thead><tbody>${r.map((x) => `<tr><td><code>${esc(x.repository)}</code></td><td>${x.tags.map((t) => `<span class="pill">${esc(t)}</span>`).join(' ')}</td></tr>`).join('')}</tbody></table>` : '<p class="muted">Empty. Run the pipeline to push the first images.</p>';
  }, 6000);
});

// ------------------------------------------------------------------ cloud
route(/^\/cloud$/, async (main) => {
  const rows = [
    ['nginx (alb)', 'Application Load Balancer', 'Spreads requests over api-1 / api-2, health checks'],
    ['producer, processor, api-1/2, dashboard', 'EC2 instances (Auto Scaling Group for processors)', 'Same Docker images, pulled from ECR'],
    ['redpanda-0/1/2', 'Amazon MSK or Redpanda on EC2', '3 brokers across availability zones'],
    ['redis', 'Amazon ElastiCache for Redis', 'Managed, replicated'],
    ['rustfs (S3)', 'Amazon S3', 'Checkpoint backups, archive, Loki chunks'],
    ['registry', 'Amazon ECR', 'Image registry'],
    ['consul', 'Consul on EC2 or AWS Systems Manager Parameter Store', 'Dynamic config'],
    ['prometheus / grafana / loki', 'Amazon Managed Prometheus / Grafana, or on EC2', 'Observability'],
    ['docker network + published ports', 'VPC, subnets and Security Groups', 'Only 80/443 public; 9092 internal'],
  ];
  main.innerHTML = `${head(9, 'var(--s9)', 'Deployment on AWS', 'Locally every AWS service has a stand-in container, so you can learn the architecture for free. The deploy/aws folder holds Terraform to run the same stack on AWS.')}
    <div class="card"><h2>Local ↔ AWS mapping</h2><div class="table-wrap"><table><thead><tr><th>In this lab</th><th>On AWS</th><th>Purpose</th></tr></thead><tbody>${rows.map((r) => `<tr><td><code>${esc(r[0])}</code></td><td><b>${esc(r[1])}</b></td><td>${esc(r[2])}</td></tr>`).join('')}</tbody></table></div></div>
    <div class="grid g2">
      <div class="card"><h2>VPC layout</h2><pre><code>VPC 10.0.0.0/16
 ├─ public subnets (2 AZs)
 │    └─ Application Load Balancer  :80/:443  ← internet
 └─ private subnets (2 AZs)
      ├─ EC2: api, dashboard        (SG: from ALB only)
      ├─ EC2 ASG: processor 1..4    (scales on consumer lag)
      ├─ MSK / Redpanda 3 brokers   (SG: 9092 from app SG)
      ├─ ElastiCache Redis          (SG: 6379 from app SG)
      └─ S3 (VPC endpoint), ECR</code></pre></div>
      <div class="card"><h2>Free-tier path (feasibility study)</h2><ol class="small">
        <li>Create an AWS account and an IAM user with programmatic access.</li>
        <li><code>cd deploy/aws/terraform</code> → <code>terraform init</code> → <code>terraform apply</code>. Creates a VPC, a security group, an S3 bucket, ECR repositories and one t3.medium EC2 instance (the stack needs about 3 GB of RAM, so t2.micro is too small and this is not free-tier).</li>
        <li>The instance's user-data installs Docker and runs <code>docker compose up</code> with this repository.</li>
        <li>Open the ALB / instance URL. Run <code>terraform destroy</code> afterwards to avoid charges.</li></ol>
        <p class="small muted">Nothing is deployed to AWS from this lab automatically; the CI/CD "deploy" stage performs a rolling restart locally instead.</p></div></div>
    <div class="card"><h2>Security groups (firewall rules)</h2><div class="table-wrap"><table><thead><tr><th>Group</th><th>Inbound</th><th>Why</th></tr></thead><tbody>
      <tr><td>alb-sg</td><td>80, 443 from 0.0.0.0/0</td><td>Public entry point</td></tr><tr><td>app-sg</td><td>8000, 80 from alb-sg</td><td>Only the ALB reaches the apps</td></tr>
      <tr><td>kafka-sg</td><td>9092 from app-sg</td><td>Producers and processors only</td></tr><tr><td>redis-sg</td><td>6379 from app-sg</td><td>Never public</td></tr></tbody></table></div></div>`;
});

// ------------------------------------------------------------- resilience
route(/^\/resilience$/, async (main, ctx) => {
  const L = await links();
  main.innerHTML = `${head(11, 'var(--s11)', 'Scalability, fault tolerance &amp; resilience', 'All eight mechanisms from section 11 of the diagram, each with a live view and a button to test it.')}
    <div class="grid g4" id="mech"></div>
    <div class="grid g2"><div class="card"><h2>Window correctness (ground truth)</h2><div id="corr"></div></div><div class="card"><h2>Lag &amp; workers</h2><div id="lag"></div></div></div>
    <div class="card"><h2>Workers</h2><div id="workers"></div></div>
    <div class="grid g2"><div class="card"><div class="row between"><h2>State checkpoints in S3</h2><a class="btn sm" href="${esc(L.s3)}" target="_blank" rel="noopener">S3 console ↗</a></div><div id="s3" class="scroll small"></div></div>
      <div class="card"><h2>Chaos controls</h2><p class="small">Stop and start real containers (needs admin sign-in).</p><div id="chaos"></div></div></div>
    <div class="card"><h2>What just happened</h2><div id="tl"></div></div>`;
  const mech = [
    ['Horizontal scaling', '1–4 processor workers in a consumer group', '#/experiments/scaling'],
    ['Kafka partitioning', '4 partitions = up to 4 parallel workers', '#/kafka'],
    ['State checkpointing', 'RocksDB every 5 s + S3 every 30 s', '#/experiments/crash'],
    ['Replication', '3 brokers, replication factor 3', '#/experiments/broker'],
    ['Backpressure handling', 'Lag is monitored; the autoscaler reacts', '#/experiments/scaling'],
    ['Retry mechanism', 'Redis writes retried with exponential backoff', '#/experiments/retries'],
    ['Dead letter queue', 'Invalid and too-late events to telemetry.dlq', '#/experiments/dirty-data'],
    ['Graceful shutdown', 'SIGTERM → checkpoint → leave group', '#/processor'],
  ];
  $('#mech').innerHTML = mech.map(([t, d, h]) => `<a class="card exp-card" href="${h}" style="margin:0"><b>${t}</b><span class="small">${d}</span></a>`).join('');
  correctness($('#corr'), ctx);
  lagChart($('#lag'), ctx);
  workersTable($('#workers'), ctx);
  timeline($('#tl'), ctx, { limit: 15 });
  ctx.every(async () => {
    const parts = [0, 1, 2, 3];
    const out = [];
    for (const p of parts) {
      const objs = await api(`/api/explore/s3/objects?bucket=swma-checkpoints&prefix=partition-${p}/`).catch(() => []);
      const latest = objs.find((o) => o.key.endsWith('latest.json'));
      out.push(`<tr><td>partition-${p}/latest.json</td><td>${latest ? fmt.bytes(latest.size) : '–'}</td><td>${latest ? fmt.ago(latest.modified) : 'no backup yet'}</td><td>${latest ? `<button class="link" data-o="${esc(latest.key)}">view</button>` : ''}</td></tr>`);
    }
    $('#s3').innerHTML = `<table><tbody>${out.join('')}</tbody></table>`;
    $('#s3').querySelectorAll('[data-o]').forEach((b) => { b.onclick = async () => { const o = await api(`/api/explore/s3/object?bucket=swma-checkpoints&key=${encodeURIComponent(b.dataset.o)}`); const j = o.json || {}; showJson(b.dataset.o, { ...j, items: `${Object.keys(j.items || {}).length} keys (windows, dedup ids, offset)` }); }; });
  }, 10000);
  const draw = async () => {
    const cs = await api('/api/lab/containers').catch(() => []);
    $('#chaos').innerHTML = `<table><tbody>${cs.filter((c) => c.chaos_allowed).map((c) => `<tr><td><code>${esc(c.service)}</code></td><td><span class="pill ${c.status === 'running' ? 'ok' : 'bad'}">${esc(c.status)}</span></td><td>${c.status === 'running' ? `<button class="btn sm danger" data-a="stop" data-s="${esc(c.service)}">Stop</button>` : `<button class="btn sm" data-a="start" data-s="${esc(c.service)}">Start</button>`}</td></tr>`).join('')}</tbody></table>`;
    $('#chaos').querySelectorAll('[data-a]').forEach((b) => { b.onclick = () => act(b, () => api(`/api/lab/chaos/${b.dataset.s}/${b.dataset.a}`, { method: 'POST' }), `${b.dataset.a} ${b.dataset.s}`).then(() => setTimeout(draw, 1500)); });
  };
  ctx.every(draw, 8000);
});

// ---------------------------------------------------------- all the tools
route(/^\/services$/, async (main, ctx) => {
  const L = await links();
  const tools = [
    ['console', 'Redpanda Console', 2, 'Kafka topics, messages, partitions, consumer groups and lag, schema registry, brokers.', 'Topics → telemetry.events → Messages'],
    ['grafana', 'Grafana', 10, '4 provisioned dashboards: pipeline, microservices, infrastructure, logs. Login admin / streamlab.', 'Dashboards → StreamLab'],
    ['prometheus', 'Prometheus', 10, 'The metrics database. Try the query swma_group_lag or open Status → Targets.', 'Graph → swma_'],
    ['alertmanager', 'Alertmanager', 10, 'Active alerts and silences.', 'Alerts'],
    ['consul', 'Consul', 6, 'Key/Value (all lab settings) and Services (health of every component).', 'Key/Value → swma → config'],
    ['redisinsight', 'RedisInsight', 4, 'Browse the Redis keys. Accept the terms once, then open "StreamLab metrics store".', 'Browser → metrics:*'],
    ['s3', 'RustFS (S3) console', 9, 'Buckets for checkpoints, the cold-path archive and Loki log chunks. Login streamlab / streamlab-secret.', 'swma-checkpoints'],
    ['registry', 'Registry UI', 8, 'Images pushed by the CI/CD pipeline.', 'swma/*'],
    ['api_docs', 'FastAPI Swagger docs', 4, 'Try every endpoint. Click Authorize and use the API key lab-public-key.', 'GET /api/v1/metrics'],
  ];
  main.innerHTML = `<div class="page-head"><div class="kicker" style="color:var(--navy)">Tools</div><h1>All tools &amp; UIs</h1><p>Every service in the architecture has its own real user interface. They open in a new tab on your computer (localhost only).</p></div>
    <div class="grid g3">${tools.map(([k, n, s, d, w]) => `<div class="card tool-card"><div class="row between"><b>${n}</b><span class="tag">§${s}</span></div><p class="small">${d}</p><p class="small muted">Where to look: ${w}</p><div class="row"><a class="btn primary sm" href="${esc(L[k] || '#')}" target="_blank" rel="noopener">Open ↗</a><code class="small">${esc(L[k] || '')}</code></div></div>`).join('')}</div>
    <div class="card"><h2>Every container</h2><div id="cont" class="table-wrap"></div></div>
    <div class="card"><h2>Health checks</h2><div id="status"></div></div>
    <div class="card"><h2>Kafka from your own code</h2><p>The brokers are also reachable from your computer at <code>localhost:29092,localhost:29093,localhost:29094</code>. For example with Python:</p>
      <pre><code>pip install confluent-kafka
from confluent_kafka import Consumer
c = Consumer({"bootstrap.servers": "localhost:29092", "group.id": "me", "auto.offset.reset": "latest"})
c.subscribe(["telemetry.events"])
while True:
    m = c.poll(1.0)
    if m: print(m.partition(), m.offset(), m.value())</code></pre></div>`;
  ctx.every(async () => {
    const cs = await api('/api/lab/containers').catch(() => []);
    $('#cont').innerHTML = `<table><thead><tr><th>§</th><th>Container</th><th>Role</th><th>Status</th><th>Image</th><th class="num">CPU</th><th class="num">Memory</th><th class="num">Restarts</th></tr></thead><tbody>
      ${cs.map((c) => `<tr><td>${esc(c.section || '')}</td><td><code>${esc(c.service)}</code></td><td class="small">${esc(c.role || '')}</td><td><span class="pill ${c.status === 'running' ? 'ok' : c.status === 'exited' && c.service === 'bootstrap' ? '' : 'bad'}">${esc(c.status)}</span></td><td class="small">${esc(c.image)}</td><td class="num">${c.cpu ?? '–'}%</td><td class="num">${fmt.bytes(c.mem_bytes)}</td><td class="num">${c.restart_count}</td></tr>`).join('')}</tbody></table>`;
  }, 10000);
  statusGrid($('#status'), ctx);
});

export { json };
