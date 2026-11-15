// Edge cases & stress tests: real runs against the live pipeline, with automatic checks.
import { $, api, act, esc, fmt, json, lineChart, route } from './core.js';

const SCENARIOS = [
  { id: 'burst', icon: '🌊', title: '1,000 events arrive at the same instant', sizes: [1000, 3000, 10000],
    what: 'A traffic spike: thousands of events hit Kafka in the same second, spread over 8 services.',
    why: 'Real systems get flash crowds (sales, outages, retries). A naive system drops or double-counts under pressure.',
    expect: 'Kafka absorbs the burst, lag rises briefly, workers catch up, and the window counts add up to exactly the number sent.' },
  { id: 'hot_key', icon: '🔥', title: 'Hot key: all traffic from one service', sizes: [1000, 3000, 5000],
    what: 'Every event in the burst has the same key, so they all go to ONE partition.',
    why: 'Data skew is the classic scaling trap: adding workers does not help because one partition is read by one worker.',
    expect: 'All events land in a single partition, are processed in order by one worker, and are all counted.' },
  { id: 'duplicates', icon: '👯', title: 'Every event sent three times', sizes: [300, 1000, 2000],
    what: 'Each event is re-sent twice more with the same event_id, in random order (like network retries).',
    why: 'At-least-once delivery means duplicates WILL happen. Counting them would inflate every metric.',
    expect: 'The processor remembers recent event_ids, so each event is counted exactly once.' },
  { id: 'out_of_order', icon: '🔀', title: 'Events arrive out of order', sizes: [500, 2000, 5000],
    what: 'Timestamps are shuffled within the watermark bound, so newer events often arrive before older ones.',
    why: 'Mobile clients, retries and network paths reorder events all the time.',
    expect: 'Everything is counted in the right windows (by event time), and nothing is treated as late.' },
  { id: 'late', icon: '🐢', title: 'Very late events (older than the allowed lateness)', sizes: [200, 1000, 2000],
    what: 'Events that happened minutes ago, after their windows were already finalized.',
    why: 'A system must decide what to do with data that is too old. Silently changing published results is dangerous.',
    expect: 'Final windows stay unchanged; every late event is parked in the dead letter queue with reason "too_late".' },
  { id: 'malformed', icon: '🧨', title: 'Broken and invalid events', sizes: [120, 480, 1200],
    what: '8 kinds of damage: broken JSON, a list instead of an object, missing fields, wrong types, negative latency, status 999, an unreadable time, CPU 150%.',
    why: 'One bad producer must not poison the metrics or crash the pipeline.',
    expect: 'Schema validation rejects all of them into the dead letter queue; the normal stream keeps flowing.' },
  { id: 'clock_skew', icon: '⏰', title: 'Clock skew: timestamps from the future', sizes: [100, 400, 1000],
    what: 'Half the events claim to be 1 minute in the future, half 10 minutes in the future.',
    why: 'Servers with wrong clocks are common. A future timestamp could push the watermark ahead and close windows early.',
    expect: 'Small skew (≤ 5 min) is accepted without moving the watermark ahead; large skew is rejected as invalid.' },
  { id: 'boundary', icon: '📏', title: 'Events exactly on a window boundary', sizes: [3],
    what: 'Three events: 1 ms before, exactly on, and 1 ms after the end of a tumbling window.',
    why: 'Off-by-one errors at boundaries double-count or lose events in real systems.',
    expect: 'Windows are half-open [start, end): the event exactly on the boundary belongs to the NEXT window.' },
  { id: 'oversized', icon: '🐘', title: 'Oversized message', sizes: [2],
    what: 'One 200 KB event and one 2.2 MB event (Kafka\'s default limit is 1 MB).',
    why: 'Huge payloads can block brokers and consumers.',
    expect: 'The 200 KB event is processed normally; the 2.2 MB one is refused at the producer before entering the stream.' },
  { id: 'crash_mid_burst', icon: '💥', title: 'A worker crashes in the middle of a burst', sizes: [3000, 6000, 10000],
    what: 'Thousands of events are sent in chunks; after 40% a processor worker is killed with SIGKILL (no warning, no checkpoint).',
    why: 'The hardest case: failure while data is in flight. Most simple systems lose or double-count events here.',
    expect: 'Kafka moves the partitions, the new owner restores the last checkpoint and replays: the final count is still exact.' },
];

function resultView(d) {
  const p = d.progress || {};
  const done = ['passed', 'failed', 'error'].includes(d.status);
  const statusPill = d.status === 'passed' ? '<span class="pill ok">PASSED</span>' : d.status === 'failed' ? '<span class="pill bad">FAILED</span>'
    : d.status === 'error' ? '<span class="pill bad">ERROR</span>' : '<span class="pill warn">running…</span>';
  const pct = p.expected_counted ? Math.min(100, Math.round((p.counted / p.expected_counted) * 100))
    : p.expected_dlq ? Math.min(100, Math.round((p.dlq / p.expected_dlq) * 100)) : (done ? 100 : 0);
  const facts = Object.entries(d.facts || {}).filter(([, v]) => v !== null && v !== undefined && !(typeof v === 'object' && !Object.keys(v).length));
  return `<div class="row between"><div>${statusPill} <span class="small muted">${esc(d.phase || '')}</span></div><span class="small muted">${d.elapsed_s ?? 0}s · run ${esc(d.id)}</span></div>
    <div class="progressbar" style="margin:8px 0"><i style="width:${pct}%"></i></div>
    <div class="grid g4" style="gap:8px">
      <div class="card tight kpi" style="margin:0"><div class="l">counted in windows</div><div class="v">${fmt.num(p.counted)}</div><div class="h">expected ${fmt.num(p.expected_counted)}</div></div>
      <div class="card tight kpi" style="margin:0"><div class="l">dead letters</div><div class="v">${fmt.num(p.dlq)}</div><div class="h">expected ${fmt.num(p.expected_dlq)}</div></div>
      <div class="card tight kpi" style="margin:0"><div class="l">consumer lag now</div><div class="v">${fmt.num(p.lag)}</div><div class="h">events waiting in Kafka</div></div>
      <div class="card tight kpi" style="margin:0"><div class="l">peak lag</div><div class="v">${fmt.num(p.lag_peak)}</div><div class="h">during this run</div></div>
    </div>
    <div data-chart style="margin-top:8px"></div>
    ${d.checks?.length ? `<h3 style="margin-top:10px">Checks</h3><table><tbody>${d.checks.map((c) => `<tr><td>${c.pass ? '✅' : '❌'}</td><td><b>${esc(c.name)}</b><div class="small muted">${esc(c.why)}</div></td><td class="small">expected <b>${esc(String(c.expected))}</b><br>actual <b>${esc(String(c.actual))}</b></td></tr>`).join('')}</tbody></table>` : ''}
    ${facts.length ? `<details style="margin-top:6px"><summary>Measurements</summary><pre><code>${json(Object.fromEntries(facts))}</code></pre></details>` : ''}
    ${d.error ? `<p class="error">${esc(d.error)}</p>` : ''}`;
}

function drawChart(el, d) {
  const tl = d.timeline || [];
  if (tl.length < 2) return;
  const t0 = Date.parse(d.started_at);
  const pts = (k) => tl.map((x) => [t0 + x.t * 1000, x[k]]);
  const series = [{ name: 'counted', color: '#16a34a', points: pts('counted') }, { name: 'consumer lag', color: '#dc2626', points: pts('lag') }];
  if ((d.progress || {}).expected_dlq) series.push({ name: 'dead letters', color: '#7c3aed', points: pts('dlq') });
  lineChart(el, series, { height: 170 });
}

route(/^\/edge$/, async (main, ctx) => {
  main.innerHTML = `<div class="page-head"><div class="kicker" style="color:#b45309">Stress &amp; edge cases</div><h1>What happens when things get weird?</h1>
    <p>Each scenario below runs <b>for real</b> against the live pipeline: it sends its traffic into Kafka, watches the processor handle it, and then
    <b>checks automatically</b> that the result is correct. Every run uses its own throw-away test services, so your normal stream, dashboards and
    correctness check are not affected. While a test runs, keep the <a href="#/kafka">Kafka</a> or <a href="#/processor">processor</a> page open in another tab to watch it.</p></div>
    <div class="explain"><b>Tip for a demo:</b> start with <b>1,000 events at once</b>, then <b>duplicates</b>, <b>late</b> and <b>broken events</b>, and finish with
    <b>a crash in the middle of a burst</b>, the hardest case.</div>
    <div id="edge-list"></div>
    <div class="card"><h2>Recent runs</h2><div id="edge-hist" class="small"></div></div>`;
  const list = $('#edge-list');
  list.innerHTML = SCENARIOS.map((s) => `<div class="card" id="sc-${s.id}">
    <div class="row between"><h2 style="margin:0">${s.icon} ${esc(s.title)}</h2>
      <div class="row">${s.sizes.length > 1 ? `<label class="row small">events <select data-n>${s.sizes.map((n) => `<option value="${n}">${n.toLocaleString()}</option>`).join('')}</select></label>` : ''}
      <button class="btn primary" data-run="${s.id}">▶ Run it</button></div></div>
    <div class="grid g3" style="margin-top:8px;gap:10px">
      <div><b class="small">What happens</b><p class="small">${esc(s.what)}</p></div>
      <div><b class="small">Why it matters</b><p class="small">${esc(s.why)}</p></div>
      <div><b class="small">A correct system should…</b><p class="small">${esc(s.expect)}</p></div></div>
    <div data-result></div></div>`).join('');

  const timers = new Set();
  ctx.add(() => timers.forEach((t) => clearTimeout(t)));
  const follow = (card, id) => {
    const box = $('[data-result]', card);
    const tick = async () => {
      let d;
      try { d = await api(`/api/lab/edge/runs/${id}`); } catch { return; }
      box.innerHTML = `<div class="card tight" style="margin:10px 0 0;background:#fbfcff">${resultView(d)}</div>`;
      drawChart($('[data-chart]', box), d);
      if (['passed', 'failed', 'error'].includes(d.status)) { $('[data-run]', card).disabled = false; history(); return; }
      timers.add(setTimeout(tick, 1500));
    };
    tick();
  };
  list.querySelectorAll('[data-run]').forEach((b) => {
    b.onclick = async () => {
      const card = b.closest('.card');
      const sel = $('[data-n]', card);
      const r = await act(b, () => api('/api/lab/edge/run', { method: 'POST', body: { scenario: b.dataset.run, n: sel ? +sel.value : undefined } }),
        (x) => `Started: ${x.n} events`);
      if (r) { b.disabled = true; follow(card, r.id); }
    };
  });
  const history = async () => {
    const runs = await api('/api/lab/edge/runs').catch(() => []);
    $('#edge-hist').innerHTML = runs.length ? `<table><thead><tr><th>Started</th><th>Scenario</th><th class="num">Events</th><th>Result</th><th class="num">Time</th></tr></thead><tbody>
      ${runs.map((r) => `<tr><td>${fmt.time(r.started_at)}</td><td>${esc(SCENARIOS.find((s) => s.id === r.scenario)?.title || r.scenario)}</td><td class="num">${fmt.num(r.n)}</td>
      <td><span class="pill ${r.status === 'passed' ? 'ok' : r.status === 'running' ? 'warn' : 'bad'}">${esc(r.status)}</span></td><td class="num">${r.elapsed_s}s</td></tr>`).join('')}</tbody></table>` : '<p class="muted">No runs yet.</p>';
  };
  history();
  // re-attach to a run still in progress (e.g. after navigating away and back)
  const runs = await api('/api/lab/edge/runs').catch(() => []);
  runs.filter((r) => r.status === 'running').forEach((r) => { const card = $(`#sc-${r.scenario}`); if (card) { $('[data-run]', card).disabled = true; follow(card, r.id); } });
});
