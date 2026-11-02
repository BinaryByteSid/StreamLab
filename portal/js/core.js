// Shared helpers for the StreamLab portal: API, WebSockets, router, charts, UI bits.

export const $ = (sel, el = document) => el.querySelector(sel);
export const $$ = (sel, el = document) => [...el.querySelectorAll(sel)];
export const esc = (s) => String(s ?? '').replace(/[&<>"']/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
export const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

export const fmt = {
  num: (v, d = 0) => (v === null || v === undefined || Number.isNaN(v) ? '–' : Number(v).toLocaleString(undefined, { maximumFractionDigits: d, minimumFractionDigits: d })),
  ms: (v) => (v === null || v === undefined ? '–' : `${Number(v).toFixed(v < 10 ? 1 : 0)} ms`),
  pct: (v) => (v === null || v === undefined ? '–' : `${Number(v).toFixed(1)}%`),
  time: (iso) => (iso ? new Date(iso).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' }) : '–'),
  hm: (iso) => (iso ? new Date(iso).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' }) : '–'),
  ago: (iso) => {
    if (!iso) return '–';
    const s = Math.round((Date.now() - new Date(iso).getTime()) / 1000);
    if (s < 2) return 'just now';
    if (s < 60) return `${s}s ago`;
    if (s < 3600) return `${Math.round(s / 60)}m ago`;
    return `${Math.round(s / 3600)}h ago`;
  },
  bytes: (b) => {
    if (b === null || b === undefined) return '–';
    const u = ['B', 'KB', 'MB', 'GB'];
    let i = 0;
    while (b >= 1024 && i < u.length - 1) { b /= 1024; i++; }
    return `${b.toFixed(i ? 1 : 0)} ${u[i]}`;
  },
  window: (w) => (w ? `${fmt.hm(w.window_start)} → ${fmt.hm(w.window_end)}` : '–'),
};

// ------------------------------------------------------------------ API
const TOKEN_KEY = 'streamlab.token';
export const auth = {
  get token() { try { return localStorage.getItem(TOKEN_KEY); } catch { return null; } },
  set token(v) { try { v ? localStorage.setItem(TOKEN_KEY, v) : localStorage.removeItem(TOKEN_KEY); } catch { /* private mode */ } },
};

export class ApiError extends Error {
  constructor(status, detail, headers) { super(detail); this.status = status; this.headers = headers; }
}

export async function api(path, { method = 'GET', body, headers = {}, raw = false, retryLogin = true } = {}) {
  const h = { ...headers };
  if (body !== undefined) h['Content-Type'] = 'application/json';
  if (auth.token && !h.Authorization) h.Authorization = `Bearer ${auth.token}`;
  const res = await fetch(path, { method, headers: h, body: body !== undefined ? JSON.stringify(body) : undefined });
  if (raw) return res;
  let data = null;
  const text = await res.text();
  try { data = text ? JSON.parse(text) : null; } catch { data = text; }
  if (res.status === 401 && retryLogin && method !== 'GET') {
    const ok = await loginDialog();
    if (ok) return api(path, { method, body, headers, raw, retryLogin: false });
  }
  if (!res.ok) throw new ApiError(res.status, (data && data.detail) || res.statusText, res.headers);
  return data;
}

// Admin sign-in: teaches what a JWT is, prefilled with the lab's demo account.
export function loginDialog() {
  return new Promise((resolve) => {
    const wrap = document.createElement('div');
    wrap.className = 'modal-wrap';
    wrap.innerHTML = `
      <div class="modal" role="dialog" aria-modal="true" aria-labelledby="login-title">
        <h2 id="login-title">Sign in as lab admin</h2>
        <p>This action <b>changes the running system</b>, so the API asks for proof of who you are.
        You sign in once and the API gives you a <b>JWT</b> (JSON Web Token): a signed note that says
        "this user is an admin until …". The browser sends it with every change you make.</p>
        <p class="muted">Demo account for this local lab: <code>admin</code> / <code>streamlab</code></p>
        <label>Username <input id="lg-user" value="admin" autocomplete="username"></label>
        <label>Password <input id="lg-pass" type="password" value="streamlab" autocomplete="current-password"></label>
        <p class="error" id="lg-err" hidden></p>
        <div class="row end">
          <button class="btn ghost" id="lg-cancel">Cancel</button>
          <button class="btn primary" id="lg-ok">Sign in</button>
        </div>
      </div>`;
    document.body.appendChild(wrap);
    const done = (v) => { wrap.remove(); updateAuthBadge(); resolve(v); };
    $('#lg-cancel', wrap).onclick = () => done(false);
    const submit = async () => {
      try {
        const t = await api('/api/auth/token', { method: 'POST', body: { username: $('#lg-user', wrap).value, password: $('#lg-pass', wrap).value }, retryLogin: false });
        auth.token = t.access_token;
        toast('Signed in. Your JWT is stored in this browser for 8 hours.', 'ok');
        done(true);
      } catch (e) {
        const err = $('#lg-err', wrap); err.hidden = false; err.textContent = e.message;
      }
    };
    $('#lg-ok', wrap).onclick = submit;
    $('#lg-pass', wrap).addEventListener('keydown', (e) => e.key === 'Enter' && submit());
    $('#lg-ok', wrap).focus();
  });
}

export function updateAuthBadge() {
  const el = $('#auth-badge');
  if (!el) return;
  if (auth.token) {
    let user = 'admin';
    try { user = JSON.parse(atob(auth.token.split('.')[1])).sub; } catch { /* ignore */ }
    el.innerHTML = `<span class="dot ok"></span> ${esc(user)} <button class="link" id="logout">sign out</button>`;
    $('#logout').onclick = () => { auth.token = null; updateAuthBadge(); toast('Signed out'); };
  } else {
    el.innerHTML = `<button class="link" id="login">Admin sign-in</button>`;
    $('#login').onclick = () => loginDialog();
  }
}

// Run an action button: disables it, shows result as a toast.
export async function act(btn, fn, okMsg) {
  if (btn) btn.disabled = true;
  try {
    const res = await fn();
    if (okMsg) toast(typeof okMsg === 'function' ? okMsg(res) : okMsg, 'ok');
    return res;
  } catch (e) {
    toast(e.message || String(e), 'bad');
    return null;
  } finally {
    if (btn) btn.disabled = false;
  }
}

export function toast(msg, kind = 'info') {
  let box = $('#toasts');
  if (!box) { box = document.createElement('div'); box.id = 'toasts'; box.setAttribute('aria-live', 'polite'); document.body.appendChild(box); }
  const t = document.createElement('div');
  t.className = `toast ${kind}`;
  t.textContent = msg;
  box.appendChild(t);
  setTimeout(() => t.classList.add('out'), 4200);
  setTimeout(() => t.remove(), 4800);
}

// ------------------------------------------------------------ WebSockets
const wsBase = () => `${location.protocol === 'https:' ? 'wss' : 'ws'}://${location.host}`;
const liveListeners = new Set();
let liveWs = null;
export const live = { connected: false };

function openLive() {
  liveWs = new WebSocket(`${wsBase()}/ws/live`);
  liveWs.onopen = () => { live.connected = true; document.body.classList.add('ws-on'); };
  liveWs.onclose = () => { live.connected = false; document.body.classList.remove('ws-on'); setTimeout(openLive, 2000); };
  liveWs.onmessage = (e) => {
    let msg; try { msg = JSON.parse(e.data); } catch { return; }
    liveListeners.forEach((fn) => { try { fn(msg); } catch (err) { console.error(err); } });
  };
}
export function onLive(fn) {
  if (!liveWs) openLive();
  liveListeners.add(fn);
  return () => liveListeners.delete(fn);
}

export function streamSocket(topic, onMsg, rate = 25) {
  let ws, closed = false;
  const open = () => {
    ws = new WebSocket(`${wsBase()}/ws/stream?topic=${encodeURIComponent(topic)}&rate=${rate}`);
    ws.onmessage = (e) => { try { onMsg(JSON.parse(e.data)); } catch { /* ignore */ } };
    ws.onclose = () => { if (!closed) setTimeout(open, 2000); };
  };
  open();
  return () => { closed = true; ws && ws.close(); };
}

// ---------------------------------------------------------------- router
const routes = [];
let cleanup = [];
export function route(pattern, render) { routes.push({ pattern, render }); }

export function page(ctx) {
  // ctx helpers given to every page: timers & listeners that stop on navigation
  return ctx;
}

export async function navigate() {
  cleanup.forEach((fn) => { try { fn(); } catch { /* ignore */ } });
  cleanup = [];
  const hash = location.hash.replace(/^#/, '') || '/';
  const main = $('#main');
  for (const r of routes) {
    const m = hash.match(r.pattern);
    if (m) {
      const ctx = {
        params: m.slice(1).map(decodeURIComponent),
        every(fn, ms) { fn(); const id = setInterval(fn, ms); cleanup.push(() => clearInterval(id)); },
        on(fn) { cleanup.push(onLive(fn)); },
        add(fn) { cleanup.push(fn); },
      };
      $$('#nav a').forEach((a) => a.classList.toggle('active', hash === a.getAttribute('href').slice(1) || (!!a.dataset.prefix && hash.startsWith(a.dataset.prefix))));
      main.innerHTML = '';
      main.scrollTop = 0;
      window.scrollTo(0, 0);
      try { await r.render(main, ctx); } catch (e) { console.error(e); main.innerHTML = `<div class="card bad">Page error: ${esc(e.message)}</div>`; }
      $('#main').focus({ preventScroll: true });
      return;
    }
  }
  main.innerHTML = '<div class="card">Page not found. <a href="#/">Go home</a></div>';
}

// --------------------------------------------------------------- charts
// Tiny SVG line chart: series = [{name, color, points: [[t, v], ...]}]
export function lineChart(el, series, { height = 180, unit = '', yMin = 0, fmtY = (v) => fmt.num(v, v < 10 ? 1 : 0), markers = [] } = {}) {
  const w = el.clientWidth || 600, h = height, pad = { l: 46, r: 10, t: 10, b: 22 };
  const pts = series.flatMap((s) => s.points);
  if (!pts.length) { el.innerHTML = `<div class="empty" style="height:${h}px">Waiting for data…</div>`; return; }
  const xs = pts.map((p) => p[0]), ys = pts.map((p) => p[1]).filter((v) => v !== null && !Number.isNaN(v));
  const x0 = Math.min(...xs), x1 = Math.max(...xs) || x0 + 1;
  let y1 = Math.max(...ys, ...markers.map((m) => m.value));
  y1 = y1 <= yMin ? yMin + 1 : y1 * 1.1;
  const X = (t) => pad.l + ((t - x0) / Math.max(1, x1 - x0)) * (w - pad.l - pad.r);
  const Y = (v) => pad.t + (1 - (v - yMin) / (y1 - yMin)) * (h - pad.t - pad.b);
  const ticks = 4;
  let grid = '';
  for (let i = 0; i <= ticks; i++) {
    const v = yMin + ((y1 - yMin) * i) / ticks;
    grid += `<line x1="${pad.l}" x2="${w - pad.r}" y1="${Y(v)}" y2="${Y(v)}" class="grid"/><text x="${pad.l - 6}" y="${Y(v) + 4}" text-anchor="end" class="axis">${esc(fmtY(v))}</text>`;
  }
  const tl = [x0, (x0 + x1) / 2, x1].map((t) => `<text x="${X(t)}" y="${h - 6}" text-anchor="middle" class="axis">${new Date(t).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' })}</text>`).join('');
  const lines = series.map((s) => {
    const d = s.points.filter((p) => p[1] !== null).map((p, i) => `${i ? 'L' : 'M'}${X(p[0]).toFixed(1)},${Y(p[1]).toFixed(1)}`).join('');
    return `<path d="${d}" fill="none" stroke="${s.color}" stroke-width="2"><title>${esc(s.name)}</title></path>`;
  }).join('');
  const marks = markers.map((m) => `<line x1="${pad.l}" x2="${w - pad.r}" y1="${Y(m.value)}" y2="${Y(m.value)}" class="marker"/><text x="${w - pad.r}" y="${Y(m.value) - 4}" text-anchor="end" class="axis marker-t">${esc(m.label)}</text>`).join('');
  const legend = series.length > 1 ? `<div class="legend">${series.map((s) => `<span><i style="background:${s.color}"></i>${esc(s.name)}</span>`).join('')}</div>` : '';
  el.innerHTML = `<svg viewBox="0 0 ${w} ${h}" width="100%" height="${h}" role="img" aria-label="chart">${grid}${tl}${marks}${lines}</svg>${legend}`;
}

export const PALETTE = ['#2563eb', '#dc2626', '#16a34a', '#d97706', '#7c3aed', '#0891b2', '#db2777', '#4b5563'];
export const SERVICES = ['api-gateway', 'order-service', 'payment-service', 'product-service', 'cart-service', 'inventory-service', 'user-service'];
export const svcColor = (s) => PALETTE[SERVICES.indexOf(s) >= 0 ? SERVICES.indexOf(s) : 7];

// Rolling time series kept in memory while a page is open.
export class Series {
  constructor(max = 120) { this.max = max; this.data = new Map(); }
  push(name, t, v) {
    if (!this.data.has(name)) this.data.set(name, []);
    const arr = this.data.get(name);
    arr.push([t, v]);
    if (arr.length > this.max) arr.shift();
  }
  list(colors = {}) { return [...this.data.entries()].map(([name, points], i) => ({ name, points, color: colors[name] || PALETTE[i % PALETTE.length] })); }
}

// -------------------------------------------------------------- progress
const PROG = 'streamlab.progress';
export const progress = {
  all() { try { return JSON.parse(localStorage.getItem(PROG) || '{}'); } catch { return {}; } },
  get(id) { return this.all()[id] || {}; },
  set(id, patch) { const a = this.all(); a[id] = { ...(a[id] || {}), ...patch }; try { localStorage.setItem(PROG, JSON.stringify(a)); } catch { /* ignore */ } },
};

export function json(obj) {
  const s = JSON.stringify(obj, null, 2);
  return esc(s).replace(/(&quot;[^&]*?&quot;)(\s*:)/g, '<span class="j-k">$1</span>$2')
    .replace(/: (&quot;.*?&quot;)/g, ': <span class="j-s">$1</span>')
    .replace(/: (-?\d+\.?\d*)/g, ': <span class="j-n">$1</span>')
    .replace(/: (true|false|null)/g, ': <span class="j-b">$1</span>');
}

export function pill(ok, yes = 'up', no = 'down') {
  return `<span class="pill ${ok ? 'ok' : 'bad'}">${ok ? yes : no}</span>`;
}

export function links() {
  return api('/api/lab/links').catch(() => ({}));
}
