// Entry point: loads every page module, wires the header, menu and live feed.
import { $, api, esc, navigate, onLive, updateAuthBadge } from './core.js';
import { evLine } from './widgets.js';
import './pages-main.js';
import './experiments.js';
import './pages-live.js';
import './pages-ops.js';

window.addEventListener('hashchange', () => { navigate(); $('#nav').classList.remove('open'); });
$('#menu-btn').onclick = () => $('#nav').classList.toggle('open');
updateAuthBadge();

// Live "what just happened" feed (bottom right)
const feed = $('#feed'), body = $('#feed-body'), count = $('#feed-count');
let unseen = 0;
const items = [];
const drawFeed = () => { body.innerHTML = items.map(evLine).join('') || '<p class="muted">Nothing yet.</p>'; count.textContent = unseen ? unseen : ''; };
$('#feed-toggle').onclick = () => {
  const open = feed.classList.toggle('open');
  $('#feed-toggle').setAttribute('aria-expanded', open);
  unseen = 0;
  drawFeed();
};
api('/api/lab/timeline?limit=30').then((d) => { items.push(...d); drawFeed(); }).catch(() => {});
onLive((m) => {
  if (m.type !== 'lab' || m.event.kind === 'trace') return;
  items.unshift(m.event);
  items.length = Math.min(items.length, 60);
  if (!feed.classList.contains('open')) unseen++;
  drawFeed();
});

// Health badge in the header
async function health() {
  try {
    const d = await api('/api/lab/status');
    const el = $('#health-badge');
    el.textContent = `${d.healthy}/${d.total} healthy`;
    el.style.background = d.healthy === d.total ? 'rgba(74,222,128,.25)' : 'rgba(248,113,113,.35)';
    el.title = d.components.filter((c) => !c.ok).map((c) => `${c.name}: down`).join('\n') || 'All services healthy';
  } catch { $('#health-badge').textContent = 'API offline'; }
}
health();
setInterval(health, 10000);
navigate();
export { esc };
