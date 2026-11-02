// Start page, architecture map, theory lessons, glossary and quiz.
import { $, api, act, esc, fmt, progress, route, links, toast } from './core.js';
import { GLOSSARY, LESSONS, QUIZ, SECTIONS } from './content.js';
import { mountPlaygrounds } from './playgrounds.js';
import { statusGrid, timeline } from './widgets.js';
import { EXPERIMENTS } from './experiments.js';

// ------------------------------------------------------------------ home
route(/^\/$/, async (main, ctx) => {
  const prog = progress.all();
  const doneExp = EXPERIMENTS.filter((e) => prog[`exp:${e.id}`]?.done).length;
  const doneLes = LESSONS.filter((l) => prog[`lesson:${l.id}`]?.done).length;
  main.innerHTML = `
    <section class="card hero">
      <h1>Learn real-time stream processing by running one</h1>
      <p>This virtual lab is a complete, working <b>Streaming Window Metrics Aggregator</b>: 7 e-commerce microservices
      send telemetry into Kafka, a Python stream processor groups it into tumbling and sliding windows, and the results flow
      through Redis and FastAPI to live dashboards, all monitored by Prometheus, Grafana and Loki. <b>Every box in the
      architecture diagram is a real container running on your computer right now.</b> You don't need any prior knowledge.</p>
      <div class="row"><a class="btn primary" href="#/learn">1 · Start with the theory</a><a class="btn" href="#/experiments">2 · Do the experiments</a><a class="btn" href="#/architecture">See the architecture</a></div>
    </section>
    <div class="grid g4" id="kpis"></div>
    <div class="grid g2">
      <div class="card"><h2>Your learning path</h2>
        <ol class="stack">
          <li><a href="#/learn">Theory</a>: 11 short lessons with interactive mini-simulations <span class="pill ${doneLes === LESSONS.length ? 'ok' : ''}">${doneLes}/${LESSONS.length} done</span></li>
          <li><a href="#/experiments">Experiments</a>: guided hands-on labs on the live system (crash a worker, send late events, scale out…) <span class="pill ${doneExp === EXPERIMENTS.length ? 'ok' : ''}">${doneExp}/${EXPERIMENTS.length} done</span></li>
          <li><a href="#/event-lab">Send &amp; trace an event</a>: follow one event through every component</li>
          <li>Explore each component: <a href="#/kafka">Kafka</a>, <a href="#/processor">processor</a>, <a href="#/storage">Redis &amp; API</a>, <a href="#/observability">Grafana</a>…</li>
          <li><a href="#/quiz">Final quiz</a> to check yourself</li>
        </ol>
        <div class="progressbar" style="margin-top:12px"><i style="width:${Math.round(((doneExp + doneLes) / (EXPERIMENTS.length + LESSONS.length)) * 100)}%"></i></div>
      </div>
      <div class="card"><h2>The event stream</h2>
        <p>The simulated microservices produce events continuously. You can pause them, change the speed, or switch scenario.</p>
        <div id="prod"></div>
        <div class="explain"><b>New here?</b> Leave the stream running and open the <a href="#/kafka">Kafka stream</a> page: you will see the events flowing through the 4 partitions in real time.</div>
      </div>
    </div>
    <div class="card"><div class="row between"><h2>System health <span class="tag">23 components</span></h2><a href="#/services" class="small">All tools &amp; UIs →</a></div><div id="status"></div></div>
    <div class="card"><h2>What just happened</h2><div id="tl"></div></div>`;

  const kpis = $('#kpis');
  ctx.every(async () => {
    try {
      const o = await api('/api/lab/overview');
      const lag = o.supervisor?.lag?.live_total;
      const cards = [
        ['Events produced / s', fmt.num(o.rates.produced_eps, 1), 'from 7 microservices'],
        ['Events processed / s', fmt.num(o.rates.processed_eps, 1), `${o.workers.length} worker(s) in the consumer group`],
        ['Consumer lag', fmt.num(lag), 'events waiting in Kafka'],
        ['p95 processing latency', o.rates.p95_latency_s === null ? '–' : `${fmt.num(o.rates.p95_latency_s * 1000)} ms`, 'Kafka → processed'],
      ];
      kpis.innerHTML = cards.map(([l, v, h]) => `<div class="card kpi"><div class="l">${l}</div><div class="v">${v}</div><div class="h">${h}</div></div>`).join('');
      const p = o.producer;
      const c = o.config;
      $('#prod').innerHTML = p ? `
        <p>Status: ${p.running ? '<span class="pill ok">streaming</span>' : '<span class="pill warn">paused</span>'} · scenario <b>${esc(p.scenario)}</b> · ${fmt.num(p.actual_eps, 1)} events/s (target ${p.target_eps})</p>
        <div class="row">
          <button class="btn ${p.running ? '' : 'primary'}" id="toggle">${p.running ? '⏸ Pause the stream' : '▶ Start the stream'}</button>
          <label>Rate <select id="rate">${[5, 25, 100, 300].map((r) => `<option ${r === c['producer/rate_eps'] ? 'selected' : ''}>${r}</option>`).join('')}</select></label>
        </div>` : '<p class="muted">Producer not reporting yet…</p>';
      const t = $('#toggle');
      if (t) t.onclick = () => act(t, () => api('/api/lab/config', { method: 'PUT', body: { 'producer/enabled': !p.running } }), p.running ? 'Stream paused' : 'Stream started');
      const r = $('#rate');
      if (r) r.onchange = () => act(r, () => api('/api/lab/config', { method: 'PUT', body: { 'producer/rate_eps': +r.value } }), `Rate set to ${r.value} events/s`);
    } catch (e) { kpis.innerHTML = `<div class="card bad">The lab API is not reachable (${esc(e.message)}). Is the stack running? Try <code>docker compose ps</code>.</div>`; }
  }, 3000);
  statusGrid($('#status'), ctx);
  timeline($('#tl'), ctx, { limit: 10 });
});

// ---------------------------------------------------------- architecture
route(/^\/architecture$/, async (main, ctx) => {
  const L = await links();
  main.innerHTML = `
    <div class="page-head"><div class="kicker" style="color:var(--navy)">Architecture</div><h1>Streaming Window Metrics Aggregator</h1>
    <p>Ingest → Process → Window → Aggregate → Store → Serve → Visualize. Click any block to see what it does, which containers implement it and where to look at it live.</p></div>
    <div class="arch" id="arch"></div>
    <div class="card" id="detail" style="margin-top:16px"></div>`;
  const live = {};
  const draw = (sel) => {
    $('#arch').innerHTML = SECTIONS.map((s) => `
      <button class="sec ${s.wide === 2 ? 'wide2' : ''} ${sel === s.n ? 'sel' : ''}" style="--c:${s.color}" data-n="${s.n}">
        <h3><span class="n">${s.n}</span>${esc(s.title)}</h3>
        <ul>${s.items.map((i) => `<li>${esc(i)}</li>`).join('')}</ul>
        <span class="live">${live[s.n] || ''}</span>
      </button>`).join('');
    $('#arch').querySelectorAll('.sec').forEach((b) => { b.onclick = () => { draw(+b.dataset.n); detail(+b.dataset.n); }; });
  };
  const detail = async (n) => {
    const s = SECTIONS.find((x) => x.n === n);
    let containers = [];
    try { containers = (await api('/api/lab/containers')).filter((c) => s.containers.includes(c.service)); } catch { /* ops agent down */ }
    if (!$('#detail')) return; // navigated away meanwhile
    $('#detail').innerHTML = `<h2 style="color:${s.color}">${s.n}. ${esc(s.title)}</h2>
      <p>${s.what}</p><div class="analogy">💡 ${esc(s.analogy)}</div>
      ${containers.length ? `<h3>Running containers</h3><div class="table-wrap"><table><thead><tr><th>Container</th><th>Role</th><th>Status</th><th class="num">CPU</th><th class="num">Memory</th></tr></thead><tbody>
        ${containers.map((c) => `<tr><td><code>${esc(c.service)}</code></td><td>${esc(c.role || '')}</td><td>${c.status === 'running' ? '<span class="pill ok">running</span>' : `<span class="pill bad">${esc(c.status)}</span>`}</td><td class="num">${c.cpu ?? '–'}%</td><td class="num">${fmt.bytes(c.mem_bytes)}</td></tr>`).join('')}</tbody></table></div>` : ''}
      <div class="row" style="margin-top:10px"><a class="btn primary" href="${s.page}">Explore it live →</a>${s.tools.map((t) => L[t] ? `<a class="btn" href="${esc(L[t])}" target="_blank" rel="noopener">Open ${esc(t)} ↗</a>` : '').join('')}</div>`;
  };
  draw(3);
  detail(3);
  ctx.every(async () => {
    try {
      const o = await api('/api/lab/overview');
      live[1] = `<span class="pill blue">${fmt.num(o.rates.produced_eps, 0)}/s</span>`;
      live[3] = `<span class="pill blue">${o.workers.length} workers</span>`;
      live[2] = `<span class="pill">${fmt.num(o.supervisor?.lag?.live_total)} lag</span>`;
      live[4] = `<span class="pill">${fmt.num(o.rates.api_rps, 1)} req/s</span>`;
      live[12] = `<span class="pill">${fmt.num(o.rates.emitted_per_s, 0)} results/s</span>`;
      document.querySelectorAll('#arch .sec').forEach((b) => { b.querySelector('.live').innerHTML = live[b.dataset.n] || ''; });
    } catch { /* ignore */ }
  }, 3000);
});

// ---------------------------------------------------------------- learn
route(/^\/learn$/, async (main) => {
  const prog = progress.all();
  main.innerHTML = `<div class="page-head"><div class="kicker" style="color:var(--s1)">Theory</div><h1>From zero to stream processing</h1>
    <p>Eleven short lessons. Each one has a small simulation you can play with in your browser and a link to the same idea running live.</p></div>
    <div class="grid g3">${LESSONS.map((l) => `<a class="card exp-card" href="#/learn/${l.id}"><b>${esc(l.title)}</b><span class="meta">${l.time} read ${prog[`lesson:${l.id}`]?.done ? '· <span class="pill ok">done</span>' : ''}</span></a>`).join('')}</div>`;
});

route(/^\/learn\/([\w-]+)$/, async (main, ctx) => {
  const i = LESSONS.findIndex((l) => l.id === ctx.params[0]);
  const l = LESSONS[i];
  if (!l) { main.innerHTML = '<div class="card">Lesson not found.</div>'; return; }
  const prev = LESSONS[i - 1], next = LESSONS[i + 1];
  main.innerHTML = `<div class="page-head"><a href="#/learn" class="small">← All lessons</a><h1>${esc(l.title)}</h1><p>${l.time} read</p></div>
    <article class="card">${l.body}</article>
    <div class="card row between"><a class="btn primary" href="${l.try}">See it live →</a>
      <div class="row">${prev ? `<a class="btn" href="#/learn/${prev.id}">← ${esc(prev.title.split('. ')[1])}</a>` : ''}
      <button class="btn" id="done">✓ Mark as done</button>
      ${next ? `<a class="btn" href="#/learn/${next.id}">${esc(next.title.split('. ')[1])} →</a>` : '<a class="btn" href="#/experiments">Experiments →</a>'}</div></div>`;
  mountPlaygrounds(main);
  $('#done').onclick = () => { progress.set(`lesson:${l.id}`, { done: true }); toast('Lesson marked as done', 'ok'); if (next) location.hash = `#/learn/${next.id}`; };
});

// -------------------------------------------------------------- glossary
route(/^\/glossary$/, async (main) => {
  main.innerHTML = `<div class="page-head"><h1>Glossary</h1><p>Every term used in the lab, in one sentence.</p></div>
    <div class="card"><input id="gq" placeholder="Filter…" aria-label="Filter glossary" style="width:100%;margin-bottom:10px"><dl class="kv" id="gl"></dl></div>`;
  const draw = () => {
    const q = $('#gq').value.toLowerCase();
    $('#gl').innerHTML = GLOSSARY.filter(([t, d]) => (t + d).toLowerCase().includes(q)).map(([t, d]) => `<dt><b>${esc(t)}</b></dt><dd>${esc(d)}</dd>`).join('');
  };
  $('#gq').oninput = draw;
  draw();
});

// ------------------------------------------------------------------ quiz
export function renderQuiz(el, questions, key) {
  const answers = {};
  el.innerHTML = questions.map((q, i) => `<div class="quiz-q" data-i="${i}"><p><b>Q${i + 1}.</b> ${esc(q.q)}</p>
    ${q.o.map((o, j) => `<button class="btn opt" data-j="${j}">${esc(o)}</button>`).join('')}<p class="small" data-e hidden></p></div>`).join('') +
    `<div class="card" id="score" hidden></div>`;
  el.querySelectorAll('.quiz-q').forEach((box) => {
    const q = questions[+box.dataset.i];
    box.querySelectorAll('.opt').forEach((b) => {
      b.onclick = () => {
        if (answers[box.dataset.i] !== undefined) return;
        const j = +b.dataset.j;
        answers[box.dataset.i] = j === q.a;
        b.classList.add(j === q.a ? 'right' : 'wrong');
        box.querySelector(`[data-j="${q.a}"]`).classList.add('right');
        const e = box.querySelector('[data-e]');
        e.hidden = false;
        e.innerHTML = `${j === q.a ? '✅ Correct.' : '❌ Not quite.'} ${esc(q.e)}`;
        const n = Object.keys(answers).length;
        if (n === questions.length) {
          const s = Object.values(answers).filter(Boolean).length;
          const sc = $('#score', el);
          sc.hidden = false;
          sc.innerHTML = `<h2>Score: ${s} / ${questions.length}</h2><p>${s === questions.length ? 'Perfect! 🎉' : 'Review the explanations above, then try the related experiments.'}</p>`;
          if (key) progress.set(key, { score: s, of: questions.length, done: s >= Math.ceil(questions.length * 0.6) });
        }
      };
    });
  });
}

route(/^\/quiz$/, async (main) => {
  main.innerHTML = `<div class="page-head"><div class="kicker" style="color:var(--s12)">Assessment</div><h1>Final quiz</h1><p>${QUIZ.length} questions covering everything in the lab. Each answer comes with an explanation.</p></div><div id="qz"></div>`;
  renderQuiz($('#qz'), QUIZ, 'quiz:final');
});
