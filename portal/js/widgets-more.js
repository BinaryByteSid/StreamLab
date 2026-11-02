// More live widgets: event trace, correctness, topology, lag chart, alerts, CI pipeline.
import { $, api, act, esc, fmt, json, lineChart, Series, PALETTE } from './core.js';

// ------------------------------------------------------------- trace view
export async function traceView(el, eventId) {
  let d;
  try { d = await api(`/api/lab/trace/${encodeURIComponent(eventId)}`); } catch (e) { el.innerHTML = `<p class="muted">${esc(e.message)}</p>`; return false; }
  const t = d.trace;
  const processed = !!t.processed_at;
  const outcome = t.outcome || '';
  const okOutcome = ['on_time', 'out_of_order', 'late_accepted'].includes(outcome);
  const step = (cls, ic, title, body) => `<li class="${cls}"><span class="ic">${ic}</span><b>${title}</b><div class="small">${body}</div></li>`;
  const describe = {
    on_time: 'On time: its windows were still open.',
    out_of_order: 'Out of order: a newer event had already arrived, but it was within the watermark bound, so it still counted normally.',
    late_accepted: 'Late but accepted: the watermark had passed a window end, but it was within the allowed lateness, so the window was updated.',
    too_late: 'Too late: every window it belongs to was already final. It went to the dead letter queue.',
    duplicate: 'Duplicate: an event with this event_id was already counted, so it was ignored.',
    dead_letter: `Rejected (${esc(t.dlq_reason)}): sent to the dead letter queue (topic telemetry.dlq).`,
  };
  let windows = [];
  try { windows = JSON.parse(t.windows || '[]'); } catch { /* ignore */ }
  const wr = d.window_results || [];
  el.innerHTML = `<ol class="journey">
    ${step('ok', '1', 'Created', `By ${esc(t.published_by || 'the lab')} at ${fmt.time(t.published_at)}. Event time <code>${esc(t.event_time)}</code>.`)}
    ${step(t.kafka_partition !== '' ? 'ok' : 'bad', '2', 'Stored in Kafka', `Topic <code>${esc(t.topic)}</code>, key <code>${esc(t.key)}</code> → <b>partition ${esc(t.kafka_partition)}</b>, offset <b>${esc(t.kafka_offset)}</b>. Replicated to 3 brokers.`)}
    ${processed ? step(okOutcome ? 'ok' : 'bad', '3', `Processed by ${esc(t.worker)}`, `At ${fmt.time(t.processed_at)} (consumer group <code>swma-processors</code>). Watermark was ${fmt.time(t.watermark)}. ${describe[outcome] || esc(outcome)}`)
      : step('wait', '3', 'Waiting for the processor…', 'The worker that owns this partition has not processed it yet (refresh in a second).')}
    ${processed && okOutcome ? step('ok', '4', `Aggregated into ${windows.length} windows`, windows.map((w) => `<span class="pill ${w.late ? 'warn' : 'blue'}">${w.window_type} ${fmt.hm(new Date(w.start).toISOString())}–${fmt.hm(new Date(w.end).toISOString())}${w.late ? ' (late)' : ''}</span>`).join(' ')) : ''}
    ${processed && okOutcome ? step(wr.some((w) => w.result) ? 'ok' : 'wait', '5', 'Stored in Redis', `Window results are written to Redis keys like <code>${esc(wr[0]?.redis_key || '')}</code> (field <code>${esc(d.service)}</code>) and pushed over the WebSocket.`) : ''}
    ${processed && okOutcome ? step('ok', '6', 'Served by the API', `<code>GET /api/v1/metrics/${esc(d.service)}</code> now includes it. See the <a href="#/dashboard">dashboard</a>.`) : ''}
  </ol>
  ${wr.length ? `<details><summary>Current results of its windows</summary><div class="table-wrap"><table><thead><tr><th>Window</th><th>Status</th><th class="num">count</th><th class="num">avg</th><th class="num">min</th><th class="num">max</th><th class="num">late</th></tr></thead><tbody>
    ${wr.map((w) => `<tr><td>${esc(w.window_type)} ${esc(w.window_id)}</td><td>${esc(w.result?.status || 'not emitted yet')}</td><td class="num">${w.result?.count ?? '–'}</td><td class="num">${w.result?.avg ?? '–'}</td><td class="num">${w.result?.min ?? '–'}</td><td class="num">${w.result?.max ?? '–'}</td><td class="num">${w.result?.late_events ?? '–'}</td></tr>`).join('')}</tbody></table></div></details>` : ''}
  <details><summary>Raw trace record</summary><pre><code>${json(t)}</code></pre></details>`;
  return processed;
}

// ---------------------------------------------------------- correctness
export function correctness(el, ctx) {
  ctx.every(async () => {
    let d;
    try { d = await api('/api/lab/correctness?limit=4'); } catch { return; }
    el.innerHTML = `<p>${d.checked ? `<span class="pill ${d.matched === d.checked ? 'ok' : 'bad'}">${d.matched} / ${d.checked} final windows match the ground truth</span>` : '<span class="muted">Waiting for the first final windows (about 75 s after start)…</span>'}</p>
      <p class="small muted">The producer secretly counts every valid event it sends per service and minute. Here we compare that with what the processor computed. Crashes, duplicates and out-of-order events should never make them differ.</p>
      <div class="table-wrap scroll"><table><thead><tr><th>Window</th><th>Service</th><th class="num">Computed</th><th class="num">Expected</th><th></th></tr></thead><tbody>
      ${d.rows.slice(0, 21).map((r) => `<tr><td>${fmt.hm(r.window_start)}</td><td>${esc(r.service)}</td><td class="num">${r.computed}</td><td class="num">${r.expected}</td><td>${r.match ? '✅' : '❌'}</td></tr>`).join('')}</tbody></table></div>`;
  }, 5000);
}

// --------------------------------------------------------------- topology
export function topology(el, ctx) {
  ctx.every(async () => {
    let d;
    try { d = await api('/api/lab/topology'); } catch (e) { el.innerHTML = `<p class="error">${esc(e.message)}</p>`; return; }
    const events = d.topics.find((t) => t.name === 'telemetry.events');
    const brokerIds = [0, 1, 2];
    el.innerHTML = `
      <div class="grid g3">${brokerIds.map((id) => {
        const b = d.brokers.find((x) => x.id === id);
        const leads = events ? events.partitions.filter((p) => p.leader === id).map((p) => `P${p.partition}`) : [];
        return `<div class="card tight" style="margin:0;border-left:4px solid ${b ? 'var(--s2)' : '#94a3b8'}"><b>Broker ${id}</b> ${b ? '<span class="pill ok">up</span>' : '<span class="pill bad">down</span>'} ${b?.controller ? '<span class="pill purple">controller</span>' : ''}
          <div class="small">leader for: ${leads.join(', ') || '–'}</div></div>`;
      }).join('')}</div>
      ${d.topics.map((t) => `<h3 style="margin-top:14px">${esc(t.name)} <span class="tag">${fmt.num(t.messages)} messages</span></h3>
        <div class="table-wrap"><table><thead><tr><th>Partition</th><th>Leader</th><th>Replicas</th><th>In-sync (ISR)</th><th class="num">Offsets (low → high)</th>${t.name === 'telemetry.events' ? '<th class="num">Committed</th><th class="num">Lag</th>' : ''}</tr></thead><tbody>
        ${t.partitions.map((p) => `<tr><td>P${p.partition}</td><td>broker ${p.leader}</td><td>${p.replicas.join(', ')}</td><td>${p.isr.join(', ')} ${p.under_replicated ? '<span class="pill bad">under-replicated</span>' : ''}</td><td class="num">${fmt.num(p.low)} → ${fmt.num(p.high)}</td>
          ${t.name === 'telemetry.events' ? `<td class="num">${fmt.num(d.group.lag[p.partition]?.committed)}</td><td class="num">${fmt.num(d.group.lag[p.partition]?.lag)}</td>` : ''}</tr>`).join('')}</tbody></table></div>`).join('')}
      <h3 style="margin-top:14px">Consumer group <code>${esc(d.group.group)}</code> <span class="pill ${d.group.state === 'Stable' || d.group.state === 'STABLE' ? 'ok' : 'warn'}">${esc(d.group.state)}</span></h3>
      <div class="table-wrap"><table><thead><tr><th>Member (client id)</th><th>Partitions</th></tr></thead><tbody>
      ${d.group.members.map((m) => `<tr><td>${esc(m.client_id)}</td><td>${m.partitions.map((p) => `<span class="pill blue">P${p}</span>`).join(' ')}</td></tr>`).join('') || '<tr><td colspan="2" class="muted">No members (rebalancing or all workers down)</td></tr>'}</tbody></table></div>`;
  }, 3000);
}

// --------------------------------------------------------------- lag chart
export function lagChart(el, ctx, { minutes = 10 } = {}) {
  ctx.every(async () => {
    try {
      const [lag, wk] = await Promise.all([
        api(`/api/explore/prometheus/range?query=${encodeURIComponent('swma_live_lag_total')}&minutes=${minutes}&step=5`),
        api(`/api/explore/prometheus/range?query=${encodeURIComponent('swma_workers_alive')}&minutes=${minutes}&step=5`),
      ]);
      const pts = (r) => (r.data.result[0]?.values || []).map(([t, v]) => [t * 1000, +v]);
      const w = pts(wk);
      lineChart(el, [{ name: 'consumer lag (events)', color: '#dc2626', points: pts(lag) },
        { name: 'workers alive (×100)', color: '#2563eb', points: w.map(([t, v]) => [t, v * 100]) }], { height: 200 });
    } catch { /* ignore */ }
  }, 5000);
}

// Generic prometheus range chart
export function promChart(el, ctx, queries, { minutes = 10, unit = '', height = 180, legend = null } = {}) {
  ctx.every(async () => {
    try {
      const series = [];
      for (const [i, [q, name]] of queries.entries()) {
        const r = await api(`/api/explore/prometheus/range?query=${encodeURIComponent(q)}&minutes=${minutes}&step=5`);
        r.data.result.forEach((s, j) => {
          const label = legend ? legend(s.metric) : name || Object.values(s.metric).join(' ');
          series.push({ name: label, color: PALETTE[(i + j) % PALETTE.length], points: s.values.map(([t, v]) => [t * 1000, +v]) });
        });
      }
      lineChart(el, series, { height, fmtY: (v) => fmt.num(v, v < 10 ? 1 : 0) + unit });
    } catch { /* ignore */ }
  }, 5000);
}

// ------------------------------------------------------------------ alerts
export function alertsList(el, ctx) {
  ctx.every(async () => {
    let d;
    try { d = await api('/api/lab/alerts'); } catch { return; }
    const active = d.active.filter((a) => a.status?.state === 'active');
    el.innerHTML = `<h3>Firing now ${active.length ? `<span class="pill bad">${active.length}</span>` : '<span class="pill ok">none</span>'}</h3>
      ${active.map((a) => `<div class="explain" style="background:#fff5f5;border-color:#fecaca"><b>🔔 ${esc(a.labels.alertname)}</b> <span class="pill ${a.labels.severity === 'critical' ? 'bad' : 'warn'}">${esc(a.labels.severity)}</span><br>${esc(a.annotations.summary)}<div class="small muted">${esc(a.annotations.description)} · since ${fmt.time(a.startsAt)}</div></div>`).join('')}
      <h3 style="margin-top:12px">Notification history (received by the API's webhook)</h3>
      <div class="scroll"><table><tbody>${d.history.slice(0, 20).map((h) => `<tr><td>${fmt.time(h.received_at)}</td><td><span class="pill ${h.status === 'firing' ? 'bad' : 'ok'}">${esc(h.status)}</span></td><td><b>${esc(h.name)}</b> ${esc(h.summary || '')}</td></tr>`).join('') || '<tr><td class="muted">No alerts yet.</td></tr>'}</tbody></table></div>`;
  }, 4000);
}

// -------------------------------------------------------------- CI/CD view
export function ciView(el, ctx) {
  const icon = { pending: '⏳', running: '🔄', passed: '✅', failed: '❌', skipped: '⏭️' };
  const draw = async () => {
    let run;
    try { run = await api('/api/lab/ci/latest'); } catch (e) { el.innerHTML = `<p class="error">${esc(e.message)}</p>`; return; }
    const html = `<div class="row between"><div>${run.status === 'never-run' ? 'The pipeline has not run yet.' : `Run #${run.id} by ${esc(run.triggered_by)} · <span class="pill ${run.status === 'passed' ? 'ok' : run.status === 'failed' ? 'bad' : 'warn'}">${esc(run.status)}</span> ${run.commit ? `· commit <code>${esc(run.commit.sha)}</code> ${esc(run.commit.message || '')}` : ''}`}</div>
      <button class="btn primary" id="ci-run" ${run.status === 'running' ? 'disabled' : ''}>▶ Run the pipeline</button></div>
      <div class="pipeline" style="margin:12px 0">${run.stages.map((s) => `<div class="stage"><div class="v">${icon[s.status] || ''}</div><div class="l"><b>${esc(s.name)}</b><br>${esc(s.status)}${s.seconds ? ` · ${s.seconds}s` : ''}</div></div>`).join('')}</div>
      ${run.stages.filter((s) => s.log).map((s) => `<details ${s.status === 'failed' ? 'open' : ''}><summary>${icon[s.status]} ${esc(s.name)} log</summary><pre><code>${esc(s.log)}</code></pre></details>`).join('')}
      ${run.deploy ? `<div class="explain"><b>Zero-downtime check:</b> while both API instances were restarted one by one, ${run.deploy.requests_ok} requests through the load balancer succeeded and ${run.deploy.requests_failed} failed.</div>` : ''}`;
    if (html === el.dataset.last) return;
    const open = new Set([...el.querySelectorAll('details[open] summary')].map((x) => x.textContent));
    el.innerHTML = html;
    el.dataset.last = html;
    el.querySelectorAll('details summary').forEach((x) => { if (open.has(x.textContent)) x.parentElement.open = true; });
    $('#ci-run', el).onclick = (e) => act(e.target, () => api('/api/lab/ci/run', { method: 'POST' }), 'Pipeline started').then(draw);
  };
  ctx.every(draw, 2500);
}

export { Series };
