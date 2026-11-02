// Live widgets reused by several pages and experiments.
import { $, api, act, esc, fmt, json, pill, svcColor, streamSocket, PALETTE } from './core.js';

export function evLine(e) {
  return `<div class="ev"><time>${fmt.time(e.ts)}</time><span class="ev-k ${esc(e.kind)}">${esc(e.kind)}</span>${esc(e.message)}</div>`;
}

// "What just happened" list, live.
export function timeline(el, ctx, { limit = 25, kinds = null } = {}) {
  let items = [];
  const draw = () => {
    const shown = kinds ? items.filter((e) => kinds.includes(e.kind)) : items;
    el.innerHTML = `<div class="timeline">${shown.slice(0, limit).map(evLine).join('') || '<p class="muted">Nothing yet. Events appear here as they happen.</p>'}</div>`;
  };
  api('/api/lab/timeline?limit=80').then((d) => { items = d; draw(); }).catch(() => draw());
  ctx.on((m) => { if (m.type === 'lab' && !['trace', 'custom_event'].includes(m.event.kind)) { items.unshift(m.event); items = items.slice(0, 100); draw(); } });
}

// Processor workers table with crash / restart buttons.
export function workersTable(el, ctx, { onChange } = {}) {
  const draw = async () => {
    let d;
    try { d = await api('/api/lab/processor'); } catch (e) { el.innerHTML = `<p class="error">${esc(e.message)}</p>`; return; }
    const sup = d.supervisor || { workers: [] };
    const byId = Object.fromEntries((d.workers || []).map((w) => [w.worker, w]));
    const rows = sup.workers.map((w) => {
      const st = byId[w.worker];
      const parts = st ? st.partitions.map((p) => `<span class="pill blue" title="offset ${p.next_offset}, lag ${p.lag}">P${p.partition}</span>`).join(' ') : '';
      const state = { running: 'ok', crashed: 'bad', restarting: 'warn', starting: 'warn', stopping: 'warn', stopped: '' }[w.state] ?? '';
      return `<tr>
        <td><b>${esc(w.worker)}</b><div class="small muted">pid ${w.pid ?? '–'}</div></td>
        <td><span class="pill ${state}">${esc(w.state)}${w.restart_in_s ? ` in ${Math.max(0, w.restart_in_s).toFixed(0)}s` : ''}</span></td>
        <td>${parts || (w.state === 'running' ? '<span class="muted small">no partitions (idle or rebalancing)</span>' : '–')}</td>
        <td class="num">${st ? fmt.num(st.events_per_sec, 1) : '–'}</td>
        <td class="num">${st ? fmt.num(st.partitions.reduce((a, p) => a + p.lag, 0)) : '–'}</td>
        <td class="num">${w.crashes}/${w.restarts}</td>
        <td>${w.pid ? `<button class="btn danger sm" data-crash="${w.worker}" title="SIGKILL: no checkpoint, no goodbye">💥 Crash</button> <button class="btn sm" data-restart="${w.worker}" title="Checkpoint, leave group, restart">↻ Graceful restart</button>` : ''}</td></tr>`;
    }).join('');
    const html = `<div class="row between small muted" style="margin-bottom:6px"><span>Mode <b>${esc(sup.mode)}</b> · target <b>${sup.target}</b> workers · live lag <b>${sup.lag?.live_total ?? '–'}</b> · committed lag <b>${sup.lag?.committed_total ?? '–'}</b></span><span>${sup.cooldown_remaining_s ? `autoscaler cooldown ${sup.cooldown_remaining_s}s` : ''}</span></div>
      <div class="table-wrap"><table><thead><tr><th>Worker</th><th>State</th><th>Partitions</th><th class="num">events/s</th><th class="num">lag</th><th class="num">crashes / restarts</th><th>Actions</th></tr></thead><tbody>${rows}</tbody></table></div>`;
    if (html !== el.dataset.last) { el.innerHTML = html; el.dataset.last = html; }
  };
  // Delegated handler: survives the table being redrawn every 2 seconds.
  el.addEventListener('click', (e) => {
    const b = e.target.closest('[data-crash],[data-restart]');
    if (!b) return;
    if (b.dataset.crash) act(null, () => api(`/api/lab/processor/workers/${b.dataset.crash}/crash`, { method: 'POST' }), `${b.dataset.crash} killed. Watch the rebalance!`).then(() => { draw(); onChange && onChange(); });
    else act(null, () => api(`/api/lab/processor/workers/${b.dataset.restart}/restart`, { method: 'POST' }), 'Graceful restart requested').then(draw);
  });
  ctx.every(draw, 2000);
}

// Partition table: owner, offsets, lag, watermark.
export function partitionsTable(el, ctx) {
  ctx.every(async () => {
    let d;
    try { d = await api('/api/lab/processor'); } catch { return; }
    const parts = (d.workers || []).flatMap((w) => w.partitions.map((p) => ({ ...p, worker: w.worker }))).sort((a, b) => a.partition - b.partition);
    el.innerHTML = `<div class="table-wrap"><table><thead><tr><th>Partition</th><th>Owner</th><th class="num">Next offset</th><th class="num">Lag</th><th>Watermark</th><th class="num">Delay</th><th class="num">Open / closed windows</th><th class="num">Dedup ids</th><th>Last checkpoint</th><th>Restored from</th></tr></thead><tbody>
      ${parts.map((p) => `<tr><td><b>P${p.partition}</b></td><td>${esc(p.worker)}</td><td class="num">${fmt.num(p.next_offset)}</td><td class="num">${fmt.num(p.lag)}</td>
      <td>${fmt.time(p.watermark)} ${p.idle ? '<span class="pill warn" title="No events for a while: watermark follows the clock">idle</span>' : ''}</td><td class="num">${p.watermark_delay_s ?? '–'}s</td>
      <td class="num">${p.open_windows} / ${p.closed_windows}</td><td class="num">${fmt.num(p.dedup_keys)}</td><td>${fmt.ago(p.last_checkpoint)}</td><td>${esc(p.restored_from || '–')}</td></tr>`).join('') || '<tr><td colspan="10" class="muted">No partitions assigned right now (a rebalance may be in progress).</td></tr>'}
      </tbody></table></div>`;
  }, 2000);
}

// Live Kafka message lanes, one per partition.
export function kafkaLanes(el, ctx, { topic = 'telemetry.events', perLane = 8, onPick } = {}) {
  el.innerHTML = '<div class="lanes" id="lanes"></div><p class="small muted" id="lane-info"></p>';
  const lanes = {};
  const laneEl = (p) => {
    if (!lanes[p]) {
      const div = document.createElement('div');
      div.className = 'lane';
      div.innerHTML = `<div class="lane-head"><b>Partition ${p}</b><div class="small" data-o>offset –</div><div class="small muted" data-k></div></div><div class="lane-msgs"></div>`;
      const all = $('#lanes', el);
      lanes[p] = { div, msgs: div.querySelector('.lane-msgs'), keys: new Set() };
      const sorted = [...Object.keys(lanes)].map(Number).sort((a, b) => a - b);
      const idx = sorted.indexOf(p);
      all.insertBefore(div, all.children[idx] || null);
    }
    return lanes[p];
  };
  let total = 0, skipped = 0;
  const close = streamSocket(topic, (m) => {
    if (m.type !== 'messages') return;
    skipped += m.skipped || 0;
    m.items.forEach((it) => {
      total++;
      const L = laneEl(it.partition);
      const v = it.value;
      const isObj = v && typeof v === 'object';
      const svc = isObj ? (v.service || '') : '';
      L.keys.add(it.key);
      const b = document.createElement('button');
      b.className = `msg${!isObj || v.reason ? ' bad' : ''}`;
      b.style.setProperty('--mc', isObj && svc ? svcColor(svc) : '#dc2626');
      let summary;
      if (!isObj) summary = '<span class="error">not JSON!</span>';
      else if (topic === 'telemetry.dlq') summary = `<b>${esc(v.reason)}</b><br>from P${v.source_partition}@${v.source_offset}`;
      else if (topic === 'metrics.windows') summary = `<b>${esc(svc)}</b><br>${esc(v.window_type)} ${esc(v.status)} n=${v.count}`;
      else summary = `<b>${esc(svc || '??')}</b><br>${v.response_time_ms ?? '?'} ms · ${v.status_code ?? '?'}`;
      b.innerHTML = `<div class="o">#${it.offset} · key ${esc(it.key || '–')}</div>${summary}`;
      b.onclick = () => (onPick ? onPick(it) : showJson(`Partition ${it.partition}, offset ${it.offset}`, it));
      L.msgs.prepend(b);
      while (L.msgs.children.length > perLane) L.msgs.lastChild.remove();
      L.div.querySelector('[data-o]').textContent = `offset ${it.offset}`;
      L.div.querySelector('[data-k]').textContent = `keys: ${[...L.keys].slice(-4).join(', ')}`;
    });
    const info = $('#lane-info', el);
    if (info) info.textContent = `${total} messages shown live${skipped ? ` (${skipped} skipped to keep your browser fast)` : ''}. Click any message to see its full JSON.`;
  });
  ctx.add(close);
}

export function showJson(title, obj) {
  const wrap = document.createElement('div');
  wrap.className = 'modal-wrap';
  wrap.innerHTML = `<div class="modal wide" role="dialog" aria-modal="true"><div class="row between"><h2>${esc(title)}</h2><button class="btn sm" data-x>Close</button></div><pre><code>${json(obj)}</code></pre></div>`;
  wrap.onclick = (e) => { if (e.target === wrap || e.target.dataset.x !== undefined) wrap.remove(); };
  document.body.appendChild(wrap);
}

// Active windows of every partition on a time axis (with watermark).
export function windowsTimeline(el, ctx, { type = 'tumbling', service = null } = {}) {
  ctx.every(async () => {
    let d;
    try { d = await api('/api/lab/processor'); } catch { return; }
    const wins = d.active_windows.filter((w) => w.window_type === type && (!service || w.service === service));
    if (!wins.length) { el.innerHTML = '<div class="empty" style="height:80px">No active windows yet</div>'; return; }
    const now = new Date(d.now).getTime();
    const t0 = Math.min(...wins.map((w) => w.start_ms)), t1 = Math.max(now + 5000, ...wins.map((w) => w.end_ms));
    const X = (t) => ((t - t0) / (t1 - t0)) * 100;
    const services = [...new Set(wins.map((w) => w.service))].sort();
    const color = { open: '#2563eb', closed: '#d97706', final: '#64748b' };
    let rows = '';
    services.forEach((s) => {
      const ws = wins.filter((w) => w.service === s);
      const lanes = type === 'sliding' ? Math.max(...ws.map((w, i) => i)) + 1 : 1;
      ws.forEach((w, i) => {
        rows += `<div class="wtl-row"><div class="wtl-bar" style="left:${X(w.start_ms)}%;width:${X(w.end_ms) - X(w.start_ms)}%;background:${color[w.status] || '#2563eb'}" title="${esc(s)} ${fmt.window(w)} ${w.status}: count ${w.count}, avg ${w.avg} ms">${esc(s)} · ${w.status} · n=${w.count} · avg ${w.avg ?? '–'}ms</div></div>`;
        if (type === 'tumbling' || i === lanes - 1) { /* one row per window */ }
      });
    });
    const wmTimes = (d.workers || []).flatMap((w) => w.partitions.map((p) => p.watermark_ms)).filter(Boolean);
    const wm = wmTimes.length ? Math.min(...wmTimes) : null;
    el.innerHTML = `<div class="wtl">${rows}<div class="wtl-now" style="left:${X(now)}%" title="now"></div>${wm ? `<div class="wtl-wm" style="left:${X(wm)}%" title="watermark (slowest partition)"></div>` : ''}</div>
      <div class="legend"><span><i style="background:#2563eb"></i>open (collecting)</span><span><i style="background:#d97706"></i>closed (watermark passed end, still accepting late events)</span><span><i style="background:#0f172a"></i>now</span><span><i style="background:#f59e0b"></i>watermark</span></div>`;
  }, 2000);
}

// Small key-value block
export function kv(obj) {
  return `<dl class="kv">${Object.entries(obj).map(([k, v]) => `<dt>${esc(k)}</dt><dd>${v}</dd>`).join('')}</dl>`;
}

export function statusGrid(el, ctx) {
  ctx.every(async () => {
    let d;
    try { d = await api('/api/lab/status'); } catch (e) { el.innerHTML = `<p class="error">API not reachable: ${esc(e.message)}</p>`; return; }
    el.innerHTML = `<div class="grid g4">${d.components.map((c) => `<div class="card tight" style="margin:0"><div class="row between"><b class="small">${esc(c.name)}</b>${pill(c.ok)}</div>
      <div class="small muted">§${c.section} · ${esc(c.detail)} · ${c.latency_ms} ms</div>${c.url ? `<a class="small" href="${esc(c.url)}" target="_blank" rel="noopener">open UI ↗</a>` : ''}</div>`).join('')}</div>`;
  }, 5000);
}

export function sectionColor(n) { return `var(--s${n})`; }
export { PALETTE };
