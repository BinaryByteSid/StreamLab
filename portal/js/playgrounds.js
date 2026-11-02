// Interactive in-browser mini simulations used inside the theory lessons.
import { $, esc, PALETTE, svcColor, SERVICES } from './core.js';

// confluent-kafka (librdkafka) default partitioner: CRC32(key) % partitions
const CRC_TABLE = (() => {
  const t = new Uint32Array(256);
  for (let n = 0; n < 256; n++) {
    let c = n;
    for (let k = 0; k < 8; k++) c = c & 1 ? 0xedb88320 ^ (c >>> 1) : c >>> 1;
    t[n] = c >>> 0;
  }
  return t;
})();
export function crc32(str) {
  const bytes = new TextEncoder().encode(str);
  let c = 0xffffffff;
  for (const b of bytes) c = CRC_TABLE[(c ^ b) & 0xff] ^ (c >>> 8);
  return (c ^ 0xffffffff) >>> 0;
}
export const partitionFor = (key, n = 4) => crc32(key) % n;

function partitionPlayground(el) {
  el.innerHTML = `<h3>Try it: which partition does a key go to?</h3>
    <div class="row"><label>Key <input id="pk" value="payment-service"></label>
    <label>Partitions <select id="pn"><option>2</option><option>3</option><option selected>4</option><option>6</option></select></label></div>
    <p id="pout"></p><div id="pall" class="small"></div>`;
  const draw = () => {
    const n = +$('#pn', el).value, key = $('#pk', el).value;
    $('#pout', el).innerHTML = `CRC32("${esc(key)}") = ${crc32(key)} → ${crc32(key)} mod ${n} = <b>partition ${partitionFor(key, n)}</b>`;
    const rows = Array.from({ length: n }, (_, p) => `<div><b>P${p}</b>: ${SERVICES.filter((s) => partitionFor(s, n) === p).map((s) => `<span class="pill" style="background:${svcColor(s)}22;color:${svcColor(s)}">${s}</span>`).join(' ') || '<span class="muted">(no service, idle)</span>'}</div>`);
    $('#pall', el).innerHTML = `<p>Where our 7 services land:</p>${rows.join('')}<p class="muted">Notice the partitions are not equally loaded — that is <b>data skew</b>. Java Kafka clients use murmur2 instead of CRC32, so they would map keys differently.</p>`;
  };
  el.addEventListener('input', draw);
  draw();
}

function groupPlayground(el) {
  el.innerHTML = `<h3>Try it: deal 4 partitions to N workers</h3>
    <div class="row"><button class="btn sm" data-d="-1">− worker</button><b id="gn">2</b> workers<button class="btn sm" data-d="1">+ worker</button></div>
    <div id="gout" class="grid g4" style="margin-top:10px"></div>`;
  let n = 2;
  const draw = () => {
    $('#gn', el).textContent = n;
    const cards = [];
    for (let w = 0; w < n; w++) {
      const parts = [0, 1, 2, 3].filter((p) => p % n === w);
      cards.push(`<div class="card tight" style="margin:0"><b>processor-${w + 1}</b><br>${parts.length ? parts.map((p) => `<span class="pill blue">P${p}</span>`).join(' ') : '<span class="pill warn">idle</span>'}</div>`);
    }
    $('#gout', el).innerHTML = cards.join('');
  };
  el.addEventListener('click', (e) => { const d = e.target.dataset.d; if (d) { n = Math.max(1, Math.min(6, n + +d)); draw(); } });
  draw();
}

function windowsPlayground(el) {
  el.innerHTML = `<h3>Try it: click the timeline to drop events</h3>
    <div class="row"><label>Window type <select id="wt"><option value="tumbling">Tumbling</option><option value="sliding">Sliding</option></select></label>
    <label>Size (s) <select id="ws"><option>10</option><option selected>20</option><option>30</option></select></label>
    <label>Slide (s) <select id="wsl"><option selected>5</option><option>10</option></select></label>
    <button class="btn sm" id="wr">+ 5 random events</button><button class="btn sm ghost" id="wc">Clear</button></div>
    <div class="timeline-canvas" id="wcv" style="margin-top:10px" aria-label="Timeline from 0 to 60 seconds"></div>
    <div id="wtab" class="small table-wrap"></div>`;
  const events = [12, 18, 27, 33, 41].map((t, i) => ({ t, v: [120, 80, 200, 150, 90][i] }));
  const cv = $('#wcv', el);
  const draw = () => {
    const type = $('#wt', el).value, size = +$('#ws', el).value, slide = +$('#wsl', el).value;
    const W = 60, wins = [];
    if (type === 'tumbling') for (let s = 0; s < W; s += size) wins.push([s, s + size]);
    else for (let s = -size + slide; s < W; s += slide) wins.push([s, s + size]);
    const pct = (t) => `${(Math.max(0, Math.min(W, t)) / W) * 100}%`;
    let html = '';
    wins.forEach(([s, e], i) => {
      const inside = events.filter((ev) => ev.t >= s && ev.t < e);
      const top = type === 'tumbling' ? 120 : 20 + (i % Math.ceil(size / slide)) * 26;
      html += `<div class="wtl-bar" style="left:${pct(s)};width:calc(${pct(e)} - ${pct(s)});top:${top}px;background:${PALETTE[i % PALETTE.length]}cc">[${s},${e}) n=${inside.length}</div>`;
    });
    events.forEach((ev) => { html += `<div title="t=${ev.t}s latency ${ev.v}ms" style="position:absolute;left:${pct(ev.t)};bottom:6px;transform:translateX(-50%);width:12px;height:12px;border-radius:50%;background:#0f172a"></div>`; });
    for (let t = 0; t <= W; t += 10) html += `<div style="position:absolute;left:${pct(t)};bottom:22px;font-size:11px;color:#64748b;transform:translateX(-50%)">${t}s</div>`;
    cv.innerHTML = html;
    const rows = wins.filter(([s, e]) => e > 0 && s < W).map(([s, e]) => {
      const vs = events.filter((ev) => ev.t >= s && ev.t < e).map((ev) => ev.v);
      return `<tr><td>[${s}s, ${e}s)</td><td class="num">${vs.length}</td><td class="num">${vs.length ? (vs.reduce((a, b) => a + b, 0) / vs.length).toFixed(0) : '–'}</td><td class="num">${vs.length ? Math.min(...vs) : '–'}</td><td class="num">${vs.length ? Math.max(...vs) : '–'}</td></tr>`;
    });
    const per = type === 'tumbling' ? 1 : Math.ceil(size / slide);
    $('#wtab', el).innerHTML = `<p>Each event is counted in <b>${per}</b> window${per > 1 ? 's' : ''}.</p><table><thead><tr><th>Window</th><th class="num">count</th><th class="num">avg</th><th class="num">min</th><th class="num">max</th></tr></thead><tbody>${rows.join('')}</tbody></table>`;
  };
  cv.addEventListener('click', (e) => {
    const r = cv.getBoundingClientRect();
    events.push({ t: Math.round(((e.clientX - r.left) / r.width) * 60 * 10) / 10, v: Math.round(50 + Math.random() * 250) });
    draw();
  });
  $('#wr', el).onclick = () => { for (let i = 0; i < 5; i++) events.push({ t: Math.round(Math.random() * 600) / 10, v: Math.round(50 + Math.random() * 250) }); draw(); };
  $('#wc', el).onclick = () => { events.length = 0; draw(); };
  el.addEventListener('change', draw);
  draw();
}

function aggPlayground(el) {
  const lat = [100, 200, 300, 400, 150, 820, 60];
  let i = 0;
  el.innerHTML = `<h3>Try it: one event at a time</h3><p class="small muted">Latencies arriving: ${lat.join(', ')} ms</p>
    <div class="row"><button class="btn primary sm" id="an">Next event →</button><button class="btn sm ghost" id="ar">Reset</button></div><div id="ao" style="margin-top:8px"></div>`;
  const draw = () => {
    const seen = lat.slice(0, i);
    const s = { count: seen.length, sum: seen.reduce((a, b) => a + b, 0), min: seen.length ? Math.min(...seen) : null, max: seen.length ? Math.max(...seen) : null };
    const last = i ? `New event: <b>${lat[i - 1]} ms</b> → count ${s.count - 1}+1, sum ${s.sum - lat[i - 1]}+${lat[i - 1]}` : 'No events yet.';
    $('#ao', el).innerHTML = `<p>${last}</p><div class="grid g4">${['count', 'sum', 'min', 'max'].map((k) => `<div class="card tight kpi" style="margin:0"><div class="l">${k}</div><div class="v">${s[k] ?? '–'}</div></div>`).join('')}</div>
      <p>average = sum / count = <b>${s.count ? (s.sum / s.count).toFixed(1) : '–'} ms</b>. State size stays 4 numbers no matter how many events arrive.</p>`;
  };
  $('#an', el).onclick = () => { if (i < lat.length) i++; draw(); };
  $('#ar', el).onclick = () => { i = 0; draw(); };
  draw();
}

function watermarkPlayground(el) {
  // events: [event time, arrival order]; windows of 10s
  const evs = [3, 7, 12, 9, 15, 21, 18, 26, 6, 31, 14, 38];
  let i = 0;
  el.innerHTML = `<h3>Try it: watch the watermark move</h3>
    <div class="row"><label>Max out-of-orderness (s) <input id="wb" type="number" min="0" max="10" value="3" style="width:80px"></label>
    <label>Allowed lateness (s) <input id="wl" type="number" min="0" max="20" value="5" style="width:80px"></label>
    <button class="btn primary sm" id="wn">Next arrival →</button><button class="btn sm ghost" id="wrs">Reset</button></div>
    <p class="small muted">Windows are 10 s tumbling: [0,10) [10,20) [20,30) [30,40). Event times arrive in this order: ${evs.join(', ')}</p>
    <div id="wo"></div>`;
  const draw = () => {
    const bound = +$('#wb', el).value, late = +$('#wl', el).value;
    let maxT = -Infinity, wm = -Infinity;
    const counts = {}, log = [];
    evs.slice(0, i).forEach((t) => {
      const start = Math.floor(t / 10) * 10, end = start + 10;
      let res;
      if (end + late <= wm) res = '<span class="pill bad">too late → DLQ</span>';
      else if (end <= wm) { res = '<span class="pill warn">late, accepted</span>'; counts[start] = (counts[start] || 0) + 1; }
      else { res = t < maxT ? '<span class="pill purple">out of order, on time</span>' : '<span class="pill ok">on time</span>'; counts[start] = (counts[start] || 0) + 1; }
      maxT = Math.max(maxT, t); wm = Math.max(wm, maxT - bound);
      log.push(`<tr><td>t=${t}s</td><td>[${start},${end})</td><td>${res}</td><td class="num">${wm}s</td></tr>`);
    });
    const wins = [0, 10, 20, 30].map((s) => {
      const st = wm >= s + 10 + late ? '<span class="pill">final</span>' : wm >= s + 10 ? '<span class="pill warn">closed</span>' : '<span class="pill blue">open</span>';
      return `<div class="card tight" style="margin:0"><b>[${s},${s + 10})</b> ${st}<br>count = ${counts[s] || 0}</div>`;
    }).join('');
    $('#wo', el).innerHTML = `<p>Watermark = max event time − ${bound}s = <b>${Number.isFinite(wm) ? wm + 's' : '–'}</b></p><div class="grid g4">${wins}</div>
      <div class="table-wrap"><table style="margin-top:10px"><thead><tr><th>Arrived</th><th>Window</th><th>Decision</th><th class="num">Watermark after</th></tr></thead><tbody>${log.reverse().join('')}</tbody></table></div>`;
  };
  $('#wn', el).onclick = () => { if (i < evs.length) i++; draw(); };
  $('#wrs', el).onclick = () => { i = 0; draw(); };
  el.addEventListener('input', draw);
  draw();
}

export function mountPlaygrounds(root) {
  const map = { 'pg-partition': partitionPlayground, 'pg-group': groupPlayground, 'pg-windows': windowsPlayground, 'pg-agg': aggPlayground, 'pg-watermark': watermarkPlayground };
  Object.entries(map).forEach(([id, fn]) => { const el = root.querySelector(`#${id}`); if (el) fn(el); });
}
