// Agent Bus — session console.
//
// A Tier-1 dashboard on the JS SDK: connect, start workflows, watch each one's
// live event trace + iteration counter, and terminate stalled/runaway ones.
// Scoped to the workflows this browser starts (per-connection isolation).

import { AgentBusClient } from "agent-bus-client";

const STALL_MS = 15000; // no new event for this long (while running) => stalled

const $ = (id) => document.getElementById(id);
const el = (tag, cls, text) => {
  const n = document.createElement(tag);
  if (cls) n.className = cls;
  if (text != null) n.textContent = text;
  return n;
};

const cards = new Map(); // cid -> card state

// ---- theme toggle (persisted) -------------------------------------------

function applyTheme(theme) {
  document.documentElement.dataset.theme = theme;
  // Label shows the theme the button switches TO (the action), not the current one.
  $("theme").textContent = theme === "light" ? "🌙 dark" : "☀ light";
}
applyTheme(localStorage.getItem("agentbus-theme") || "light");
$("theme").onclick = () => {
  const next = document.documentElement.dataset.theme === "light" ? "dark" : "light";
  localStorage.setItem("agentbus-theme", next);
  applyTheme(next);
};

// ---- connection ----------------------------------------------------------

// Derive the path prefix from the current page so the app works at "/" (dev)
// and under a reverse-proxy sub-path like "/bus/". The Socket.IO endpoint and
// all assets live under this same prefix.
const basePath = window.location.pathname.replace(/[^/]*$/, ""); // dir w/ trailing slash
const client = new AgentBusClient(window.location.origin, { path: basePath + "socket.io/" });

(async () => {
  try {
    await client.connect();
    $("dot").classList.replace("off", "on");
    $("conn-text").textContent = "connected";
    $("stream").textContent = "stream " + client.streamId;
  } catch (err) {
    $("conn-text").textContent = "connection failed: " + err.message;
  }
})();

// ---- start a workflow ----------------------------------------------------

$("start").onclick = startWorkflow;
$("text").addEventListener("keydown", (e) => {
  if (e.key === "Enter") startWorkflow();
});
$("clear").onclick = clearFinished;

async function startWorkflow() {
  const text = $("text").value.trim() || "ping";
  const budget = parseInt($("budget").value, 10) || 0;
  const wf = await client.start(text);
  addCard(wf, budget, text);
  $("text").value = "";
}

// ---- card rendering ------------------------------------------------------

function addCard(wf, budget, text) {
  const root = el("div", "card running");
  const head = el("div", "card-head");
  const title = el("div", "card-title");
  title.append(el("span", "cid", wf.cid.slice(0, 8)));
  const badges = el("span", "badges");
  title.append(badges);
  head.append(title);

  const meter = el("div", "meter");
  const sidEl = el("span", "sid", "0");
  meter.append(el("span", "meter-label", "step"), sidEl);
  if (budget > 0) meter.append(el("span", "budget", "/ " + budget));
  head.append(meter);

  const actions = el("div", "card-actions");
  const killBtn = el("button", "kill", "Terminate");
  killBtn.onclick = () => wf.terminate();
  const dropBtn = el("button", "ghost", "✕");
  dropBtn.title = "remove from view";
  dropBtn.onclick = () => removeCard(wf.cid);
  actions.append(killBtn, dropBtn);
  head.append(actions);

  const sub = el("div", "card-sub muted", text);
  const list = el("div", "events");

  root.append(head, sub, list);
  $("board").prepend(root);

  const card = {
    cid: wf.cid, wf, budget, root, badges, sidEl, list, killBtn,
    status: "running", lastEventTs: Date.now(), sid: 0,
  };
  cards.set(wf.cid, card);
  renderBadges(card);
  updateSummary();

  consume(card);
}

async function consume(card) {
  for await (const ev of card.wf) {
    card.sid = ev.sid;
    card.lastEventTs = Date.now();
    card.sidEl.textContent = ev.sid;
    appendEvent(card, ev);
    if (card.budget > 0 && ev.sid > card.budget && !ev.isTerminal) card.runaway = true;
    renderBadges(card);
    updateSummary();
  }
  // iterator ended: terminated or disconnected
  const ended = await card.wf.completed;
  card.status = ended ? "terminated" : "disconnected";
  card.root.classList.remove("running");
  card.root.classList.add(card.status);
  card.killBtn.disabled = true;
  renderBadges(card);
  updateSummary();
}

function appendEvent(card, ev) {
  const row = el("div", "ev ev-" + ev.type.replace(/\./g, "-"));
  row.append(el("span", "ev-sid", String(ev.sid)));
  row.append(el("span", "ev-type", ev.type));
  row.append(el("span", "ev-sender muted", ev.sender));
  const payload = JSON.stringify(ev.data);
  if (payload && payload !== "{}") {
    const dataEl = el("span", "ev-data", payload);
    dataEl.title = payload; // full text on hover, in addition to wrapping
    row.append(dataEl);
  }
  card.list.append(row);
  card.list.scrollTop = card.list.scrollHeight;
}

function renderBadges(card) {
  card.badges.textContent = "";
  const add = (cls, label) => card.badges.append(el("span", "badge " + cls, label));
  if (card.status === "terminated") add("done", "terminated");
  else if (card.status === "disconnected") add("off", "disconnected");
  else {
    add("live", "running");
    if (card.runaway) add("runaway", "runaway");
    if (card.stalled) add("stalled", "stalled");
  }
}

// ---- stalled detection (no progress while still running) -----------------

setInterval(() => {
  const now = Date.now();
  let changed = false;
  for (const card of cards.values()) {
    if (card.status !== "running") continue;
    const stalled = now - card.lastEventTs > STALL_MS;
    if (stalled !== !!card.stalled) {
      card.stalled = stalled;
      renderBadges(card);
      changed = true;
    }
  }
  if (changed) updateSummary();
}, 1000);

// ---- summary + housekeeping ---------------------------------------------

function updateSummary() {
  let active = 0, runaway = 0, stalled = 0;
  for (const c of cards.values()) {
    if (c.status === "running") {
      active++;
      if (c.runaway) runaway++;
      if (c.stalled) stalled++;
    }
  }
  $("n-active").textContent = active;
  $("n-runaway").textContent = runaway;
  $("n-stalled").textContent = stalled;
}

function removeCard(cid) {
  const card = cards.get(cid);
  if (card) {
    card.root.remove();
    cards.delete(cid);
    updateSummary();
  }
}

function clearFinished() {
  for (const [cid, card] of cards) {
    if (card.status !== "running") removeCard(cid);
  }
}
