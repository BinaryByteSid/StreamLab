// Live system pages: dashboard, Kafka, processor, Redis & API, event lab.
import { $, api, act, esc, fmt, json, lineChart, links, route, Series, SERVICES, svcColor } from './core.js';
import { kafkaLanes, partitionsTable, showJson, timeline, windowsTimeline, workersTable } from './widgets.js';
import { correctness, promChart, topology, traceView } from './widgets-more.js';

const head = (n, color, title, text) => `<div class="page-head"><div class="kicker" style="color:${color}">Section ${n}</div><h1>${title}</h1><p>${text}</p></div>`;

// --------------------------------------------------------------- dashboard
route(/^\/dashboard$/, async (main, ctx) => {
  main.innerHTML = `${head(5, 'var(--s5)', 'Live dashboard', 'The end result of the pipeline: metrics per microservice, updated live over a WebSocket the moment a window result is computed. This page never touches raw events.')}
    <div class="card row between"><div class="row"><b>View:</b>
      <label class="row"><input type="radio" name="wt" value="tumbling" checked> Tumbling (1 per minute)</label>
      <label class="row"><input type="radio" name="wt" value="sliding"> Sliding (last 60 s, every 10 s)</label></div>
      <span class="small muted" id="ws-info">waiting for live updates…</span></div>
    <div id="alert-banner"></div>
    <div class="grid g4" id="cards"></div>
    <div class="grid g2">
      <div class="card"><h3>Average latency per window (ms)</h3><div id="c-lat"></div></div>
      <div class="card"><h3>Requests per window</h3><div id="c-cnt"></div></div>
      <div class="card"><h3>Error rate per window (%)</h3><div id="c-err"></div></div>
      <div class="card"><h3>Max latency per window (ms)</h3><div id="c-max"></div></div>
    </div>
    <div class="card"><div class="row between"><h3>Historical query</h3><div class="row"><select id="hs">${SERVICES.map((s) => `<option>${s}</option>`).join('')}</select><button class="btn sm" id="hq">Query last 30 windows</button></div></div><div id="hist" class="table-wrap scroll"></div></div>`;
  let wtype = 'tumbling';
  const latest = {}; // service -> {complete, live}
  let updates = 0;
  const hist = {};
  const loadHist = async () => {
    for (const s of SERVICES) {
      const d = await api(`/api/lab/windows?window_type=${wtype}&service=${s}&limit=30`);
      hist[s] = d.filter((w) => w.status !== 'open').reverse();
    }
    drawCharts();
  };
  const drawCards = () => {
    $('#cards').innerHTML = SERVICES.map((s) => {
      const c = latest[s]?.complete, l = latest[s]?.live;
      const bad = c && (c.avg > 500 || c.error_rate > 5);
      return `<div class="card tight svc-card" style="--c:${svcColor(s)};margin:0"><div class="row between"><b>${s}</b>${bad ? '<span class="pill bad">alert</span>' : '<span class="pill ok">ok</span>'}</div>
        ${c ? `<div class="small muted">last complete window ${fmt.window(c)}</div>
        <div class="grid g2" style="gap:4px;margin-top:6px">
          <div><div class="small muted">requests</div><b>${fmt.num(c.count)}</b></div><div><div class="small muted">avg latency</div><b>${fmt.ms(c.avg)}</b></div>
          <div><div class="small muted">min / max</div><b>${fmt.num(c.min)} / ${fmt.num(c.max)}</b></div><div><div class="small muted">error rate</div><b>${fmt.pct(c.error_rate)}</b></div>
          <div><div class="small muted">throughput</div><b>${fmt.num(c.throughput_eps, 2)}/s</b></div><div><div class="small muted">CPU</div><b>${fmt.pct(c.cpu_avg)}</b></div></div>` : '<p class="muted small">waiting for the first complete window…</p>'}
        ${l && l.status === 'open' ? `<div class="small" style="margin-top:6px">🔵 live window so far: ${l.count} req, avg ${fmt.ms(l.avg)}</div>` : ''}</div>`;
    }).join('');
  };
  const drawCharts = () => {
    const mk = (key) => SERVICES.map((s) => ({ name: s, color: svcColor(s), points: (hist[s] || []).map((w) => [w.end_ms, w[key]]) }));
    lineChart($('#c-lat'), mk('avg'), { markers: [{ value: 500, label: 'alert 500 ms' }] });
    lineChart($('#c-cnt'), mk('count'));
    lineChart($('#c-err'), mk('error_rate'), { markers: [{ value: 5, label: 'alert 5%' }] });
    lineChart($('#c-max'), mk('max'));
  };
  const loadLatest = async () => {
    const d = await fetch(`/api/v1/metrics?window_type=${wtype}`, { headers: { 'X-API-Key': 'lab-public-key' } }).then((r) => r.json()).catch(() => null);
    if (d?.services) Object.entries(d.services).forEach(([s, v]) => { latest[s] = { complete: v.last_complete, live: v.live }; });
    drawCards();
  };
  main.querySelectorAll('[name=wt]').forEach((r) => { r.onchange = () => { wtype = r.value; loadLatest(); loadHist(); }; });
  await loadLatest();
  loadHist();
  ctx.every(loadHist, 15000);
  ctx.on((m) => {
    if (m.type !== 'windows') return;
    updates += m.items.length;
    m.items.forEach((w) => {
      if (w.window_type !== wtype) return;
      latest[w.service] = latest[w.service] || {};
      if (w.status !== 'open' && (!latest[w.service].complete || w.start_ms >= latest[w.service].complete.start_ms)) latest[w.service].complete = w;
      if (!latest[w.service].live || w.start_ms >= latest[w.service].live.start_ms) latest[w.service].live = w;
      if (w.status === 'closed' || w.status === 'updated') {
        const arr = hist[w.service] = (hist[w.service] || []).filter((x) => x.window_id !== w.window_id);
        arr.push(w); arr.sort((a, b) => a.start_ms - b.start_ms); while (arr.length > 30) arr.shift();
      }
    });
    $('#ws-info').textContent = `🟢 ${updates} live window updates received over the WebSocket`;
    drawCards();
  });
  ctx.every(drawCharts, 5000);
  ctx.every(async () => {
    const a = await api('/api/lab/alerts').catch(() => null);
    const act_ = a ? a.active.filter((x) => x.status?.state === 'active') : [];
    $('#alert-banner').innerHTML = act_.length ? `<div class="card bad"><b>🔔 ${act_.length} alert(s) firing:</b> ${act_.map((x) => esc(x.annotations.summary)).join(' · ')} <a href="#/observability">details</a></div>` : '';
  }, 5000);
  $('#hq').onclick = async () => {
    const s = $('#hs').value;
    const d = await api(`/api/lab/windows?window_type=${wtype}&service=${s}&limit=30`);
    $('#hist').innerHTML = `<table><thead><tr><th>Window</th><th>Status</th><th class="num">Count</th><th class="num">Avg</th><th class="num">Min</th><th class="num">Max</th><th class="num">Errors</th><th class="num">Late</th></tr></thead><tbody>
      ${d.map((w) => `<tr><td>${fmt.window(w)}</td><td>${esc(w.status)}</td><td class="num">${w.count}</td><td class="num">${w.avg}</td><td class="num">${w.min}</td><td class="num">${w.max}</td><td class="num">${w.error_count} (${w.error_rate}%)</td><td class="num">${w.late_events}</td></tr>`).join('')}</tbody></table>
      <p class="small muted">Older finalized windows are archived to S3 (bucket swma-archive) — the cold path. Browse them on the <a href="#/services">tools page</a>.</p>`;
  };
});

// ------------------------------------------------------------------ kafka
route(/^\/kafka$/, async (main, ctx) => {
  const L = await links();
  main.innerHTML = `${head(2, 'var(--s2)', 'Kafka stream (Redpanda)', 'Watch the real messages flowing through the topic, partition by partition. The broker cluster has 3 nodes and every partition is replicated 3 times.')}
    <div class="card row between"><div class="row"><b>Topic:</b><select id="topic"><option value="telemetry.events">telemetry.events (raw events)</option><option value="metrics.windows">metrics.windows (aggregated results)</option><option value="telemetry.dlq">telemetry.dlq (dead letters)</option></select></div>
      <div class="row"><a class="btn" href="${esc(L.console)}" target="_blank" rel="noopener">Open Redpanda Console ↗</a><a class="btn" href="#/event-lab">Send your own event</a></div></div>
    <div class="explain"><b>How to read this:</b> each row is a partition, each card a message (newest on the left). The number after # is the <b>offset</b>. The key decides the partition, so the same services always show up in the same row.</div>
    <div class="card" id="lanes-box"></div>
    <div class="card"><h2>Cluster, partitions, replicas and consumer lag</h2><div id="topo"></div></div>
    <div class="card"><h2>Messages per partition / s</h2><div id="ppc"></div></div>`;
  let current = null;
  const mount = () => {
    if (current) current();
    const sub = { add: (fn) => { current = fn; } };
    kafkaLanes($('#lanes-box'), { ...ctx, add: sub.add }, { topic: $('#topic').value });
  };
  $('#topic').onchange = mount;
  mount();
  ctx.add(() => current && current());
  topology($('#topo'), ctx);
  promChart($('#ppc'), ctx, [['sum by (partition) (rate(swma_producer_delivered_total[30s]))']], { legend: (m) => `partition ${m.partition}` });
});

// -------------------------------------------------------------- processor
route(/^\/processor$/, async (main, ctx) => {
  main.innerHTML = `${head(3, 'var(--s3)', 'Stream processing engine', 'A supervisor runs 1–4 worker processes. Each worker is a member of the Kafka consumer group and owns some partitions; for each it keeps window state in its own RocksDB database.')}
    <div class="card"><h2>Pipeline (events per second through each stage)</h2><div class="pipeline" id="pipe"></div>
      <p class="small muted">Deserialize & parse → schema validation (JSON schema from the Schema Registry) → extract event time → watermark → event router & enrichment (team / tier from the service catalog) → window assignment → stateful aggregation → trigger & emission.</p></div>
    <div class="card"><h2>Workers (consumer group members)</h2><div id="workers"></div>
      <div class="explain"><b>Try it:</b> press 💥 Crash on a worker and watch "What just happened". Its partitions move to the other worker after ~6 s, the state is restored from RocksDB and the counts stay correct. Then the supervisor restarts it.</div></div>
    <div class="card"><h2>Partitions: offsets, watermarks and state</h2><div id="parts"></div></div>
    <div class="grid g2"><div class="card"><h2>Tumbling windows in state</h2><div id="wt"></div></div><div class="card"><h2>Sliding windows (payment-service)</h2><div id="ws"></div></div></div>
    <div class="grid g2"><div class="card"><h2>Event outcomes / s</h2><div id="outc"></div></div><div class="card"><h2>What just happened</h2><div id="tl"></div></div></div>`;
  workersTable($('#workers'), ctx);
  partitionsTable($('#parts'), ctx);
  windowsTimeline($('#wt'), ctx, { type: 'tumbling' });
  windowsTimeline($('#ws'), ctx, { type: 'sliding', service: 'payment-service' });
  timeline($('#tl'), ctx, { limit: 12 });
  promChart($('#outc'), ctx, [['sum by (outcome) (rate(swma_processor_events_total[30s]))']], { legend: (m) => m.outcome });
  const stages = ['received', 'deserialized', 'validated', 'event_time', 'routed', 'windowed', 'aggregated', 'emitted'];
  const names = { received: 'Read from Kafka', deserialized: 'Deserialize & parse', validated: 'Schema validation', event_time: 'Extract event time', routed: 'Router & enrichment', windowed: 'Window assignment', aggregated: 'Stateful aggregation', emitted: 'Results emitted' };
  ctx.every(async () => {
    const r = await api(`/api/explore/prometheus/query?query=${encodeURIComponent('sum by (stage) (rate(swma_processor_stage_total[30s]))')}`).catch(() => null);
    const v = Object.fromEntries((r?.data?.result || []).map((x) => [x.metric.stage, +x.value[1]]));
    $('#pipe').innerHTML = stages.map((s) => `<div class="stage"><div class="v">${fmt.num(v[s] || 0, 1)}</div><div class="l">${names[s]}</div></div>`).join('');
  }, 4000);
});

// ---------------------------------------------------------- redis and api
route(/^\/storage$/, async (main, ctx) => {
  const L = await links();
  main.innerHTML = `${head(4, 'var(--s4)', 'Metrics storage & serving: Redis + FastAPI', 'Window results are written to Redis (in-memory, sub-millisecond reads) and served by two FastAPI instances behind the load balancer.')}
    <div class="card"><div class="row between"><h2>Redis keys</h2><div class="row"><input id="pat" value="metrics:*" aria-label="Key pattern"><button class="btn sm" id="scan">Scan</button><a class="btn sm" href="${esc(L.redisinsight)}" target="_blank" rel="noopener">Open RedisInsight ↗</a></div></div>
      <div class="explain"><b>Key design:</b> <code>metrics:tumbling:&lt;window_id&gt;</code> (hash: one field per service, as in the diagram), <code>metrics:latest:&lt;type&gt;:&lt;service&gt;</code> (newest window), <code>metrics:history:&lt;type&gt;:&lt;service&gt;</code> (sorted set for history), all with a TTL so old windows expire.</div>
      <div class="grid g2"><div id="keys" class="scroll small"></div><div id="val"></div></div></div>
    <div class="card"><div class="row between"><h2>Try the REST API</h2><a class="btn sm" href="${esc(L.api_docs)}" target="_blank" rel="noopener">Swagger docs ↗</a></div>
      <div class="row"><select id="ep">
        <option>/api/v1/metrics/payment-service</option><option>/api/v1/metrics/payment-service/latest?window_type=sliding</option>
        <option>/api/v1/metrics/order-service/history?limit=5</option><option>/api/v1/metrics?window_type=tumbling</option><option>/api/v1/services</option></select>
        <label class="row"><input type="checkbox" id="usekey" checked> send <code>X-API-Key: lab-public-key</code></label><button class="btn primary sm" id="call">Send request</button></div>
      <div id="resp"></div></div>
    <div class="grid g2"><div class="card"><h2>API requests / s per instance</h2><div id="apic"></div></div><div class="card"><h2>Redis memory</h2><div id="redc"></div></div></div>`;
  const scan = async () => {
    const d = await api(`/api/explore/redis/keys?pattern=${encodeURIComponent($('#pat').value)}&limit=150`);
    $('#keys').innerHTML = `<p>${d.dbsize} keys in Redis · ${esc(d.used_memory_human)} used. Showing ${d.keys.length}:</p><table><tbody>${d.keys.map((k) => `<tr><td><button class="link" data-k="${esc(k.key)}">${esc(k.key)}</button></td><td>${esc(k.type)}</td><td class="num">${k.ttl > 0 ? `${k.ttl}s` : ''}</td></tr>`).join('')}</tbody></table>`;
    $('#keys').querySelectorAll('[data-k]').forEach((b) => {
      b.onclick = async () => {
        const v = await api(`/api/explore/redis/key?key=${encodeURIComponent(b.dataset.k)}`);
        let val = v.value;
        if (v.type === 'hash') val = Object.fromEntries(Object.entries(val).map(([k, x]) => { try { return [k, JSON.parse(x)]; } catch { return [k, x]; } }));
        if (v.type === 'string') { try { val = JSON.parse(val); } catch { /* text */ } }
        $('#val').innerHTML = `<p><b>${esc(v.key)}</b> · ${esc(v.type)} · TTL ${v.ttl}s</p><pre><code>${json(val)}</code></pre>`;
      };
    });
  };
  $('#scan').onclick = scan;
  scan();
  $('#call').onclick = async () => {
    const t0 = performance.now();
    const r = await fetch($('#ep').value, { headers: $('#usekey').checked ? { 'X-API-Key': 'lab-public-key' } : {} });
    const ms = performance.now() - t0;
    const body = await r.json().catch(() => null);
    $('#resp').innerHTML = `<p>HTTP <b>${r.status}</b> in ${ms.toFixed(0)} ms · X-Served-By <code>${esc(r.headers.get('X-Served-By'))}</code> · X-RateLimit-Remaining <code>${esc(r.headers.get('X-RateLimit-Remaining'))}</code></p><pre><code>${json(body)}</code></pre>`;
  };
  promChart($('#apic'), ctx, [['sum by (instance) (rate(swma_api_requests_total[30s]))']], { legend: (m) => m.instance });
  promChart($('#redc'), ctx, [['redis_memory_used_bytes', 'used bytes']]);
});

// ---------------------------------------------------------------- event lab
route(/^\/event-lab$/, async (main, ctx) => {
  main.innerHTML = `${head(1, 'var(--s1)', 'Send & trace your own event', 'Create an event as if a microservice had sent it, then follow it through every component. Change the delay to make it out-of-order or late, reuse an id to make a duplicate, or break it on purpose.')}
    <div class="grid g2">
      <div class="card"><h2>1. Build the event</h2>
        <div class="form-grid">
          <label>Service <select id="f-svc">${SERVICES.map((s) => `<option ${s === 'payment-service' ? 'selected' : ''}>${s}</option>`).join('')}</select></label>
          <label>Latency (ms) <input id="f-lat" type="number" value="250" min="0"></label>
          <label>Status code <select id="f-st"><option>200</option><option>201</option><option>404</option><option>500</option><option>503</option></select></label>
          <label>Happened … seconds ago <input id="f-delay" type="number" value="0" min="0" max="3600"></label>
          <label>How many <input id="f-n" type="number" value="1" min="1" max="50"></label>
          <label>Event id (optional) <input id="f-id" placeholder="reuse one to duplicate"></label>
          <label>Break it <select id="f-mal"><option value="">no, valid event</option><option value="broken_json">broken JSON</option><option value="missing_field">missing response_time_ms</option><option value="wrong_type">status_code = "OK"</option><option value="bad_time">event_time = "last tuesday"</option></select></label>
        </div>
        <div class="explain small"><b>Hints:</b> 0 s = on time · 3 s = out of order (inside the 5 s watermark bound) · 12 s = maybe late but accepted · 120 s = too late → dead letter queue.</div>
        <pre id="preview"></pre>
        <button class="btn primary" id="send">Send to Kafka</button> <span id="sent" class="small"></span>
      </div>
      <div class="card"><h2>2. Follow its journey</h2><div id="trace"><p class="muted">Send an event to start.</p></div></div>
    </div>
    <div class="card"><h2>Your recent events</h2><div id="mine" class="small"></div></div>`;
  const mine = [];
  let current = null;
  const body = () => {
    const b = { service: $('#f-svc').value, response_time_ms: +$('#f-lat').value, status_code: +$('#f-st').value, delay_s: +$('#f-delay').value, count: +$('#f-n').value };
    if ($('#f-id').value.trim()) b.event_id = $('#f-id').value.trim();
    if ($('#f-mal').value) b.malformed = $('#f-mal').value;
    return b;
  };
  const preview = () => {
    const b = body();
    const ev = { event_id: b.event_id || 'lab-…(generated)', service: b.service, event_time: new Date(Date.now() - b.delay_s * 1000).toISOString(), response_time_ms: b.response_time_ms, status_code: b.status_code, trace: true };
    $('#preview').innerHTML = `<code>${json(ev)}</code>`;
  };
  main.querySelectorAll('input,select').forEach((i) => i.addEventListener('input', preview));
  preview();
  $('#send').onclick = (e) => act(e.target, async () => {
    const r = await api('/api/lab/events', { method: 'POST', body: body() });
    r.events.forEach((x) => mine.unshift({ ...x, at: new Date().toISOString(), svc: body().service }));
    current = r.events[0].event_id;
    $('#sent').textContent = `Sent ${r.events.length} event(s). First: partition ${r.events[0].partition}, offset ${r.events[0].offset}`;
    drawMine();
    return r;
  });
  const drawMine = () => {
    $('#mine').innerHTML = `<table><thead><tr><th>Time</th><th>Event id</th><th>Service</th><th>Partition</th><th>Offset</th><th></th></tr></thead><tbody>${mine.slice(0, 15).map((m) => `<tr><td>${fmt.time(m.at)}</td><td><code>${esc(m.event_id)}</code></td><td>${esc(m.svc)}</td><td>${m.partition ?? '–'}</td><td>${m.offset ?? '–'}</td><td><button class="link" data-t="${esc(m.event_id)}">trace</button> <button class="link" data-c="${esc(m.event_id)}">copy id</button></td></tr>`).join('')}</tbody></table>`;
    $('#mine').querySelectorAll('[data-t]').forEach((b) => { b.onclick = () => { current = b.dataset.t; }; });
    $('#mine').querySelectorAll('[data-c]').forEach((b) => { b.onclick = () => { $('#f-id').value = b.dataset.c; preview(); }; });
  };
  ctx.every(() => current && traceView($('#trace'), current), 1500);
});

export { showJson };
