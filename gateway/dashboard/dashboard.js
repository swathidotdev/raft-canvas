/**
 * Mini-RAFT Cluster Dashboard
 *
 * Polls each replica's /metrics every POLL_MS and rerenders. Also drives
 * the fault-injection panel by posting to /admin/fault/* endpoints.
 * Pure vanilla JS — no build step, no framework.
 */

const REPLICAS = [
  { id: "replica1", url: `http://${window.location.hostname}:4001` },
  { id: "replica2", url: `http://${window.location.hostname}:4002` },
  { id: "replica3", url: `http://${window.location.hostname}:4003` },
];
const POLL_MS = 1000;

const nodesEl    = document.getElementById("nodes");
const timelineEl = document.getElementById("timeline");
const summaryEl  = document.getElementById("cluster-summary");
const chipLeader = document.getElementById("chip-leader");
const chipTerm   = document.getElementById("chip-term");
const chipCommit = document.getElementById("chip-commit");
const chipPoll   = document.getElementById("chip-poll");
const targetSel  = document.getElementById("fault-target");
const peerSel    = document.getElementById("fault-peer");
const latencyIn  = document.getElementById("fault-latency-ms");
const dropIn     = document.getElementById("fault-drop-prob");

// Populate target/peer dropdowns.
for (const r of REPLICAS) {
  const opt1 = document.createElement("option"); opt1.value = r.id; opt1.textContent = r.id; targetSel.appendChild(opt1);
  const opt2 = document.createElement("option"); opt2.value = r.id; opt2.textContent = r.id; peerSel.appendChild(opt2);
}

// ─── polling ──────────────────────────────────────────────────────
async function poll() {
  const t0 = performance.now();
  const results = await Promise.all(REPLICAS.map((r) => fetchMetrics(r)));
  chipPoll.textContent = `poll: ${Math.round(performance.now() - t0)}ms`;
  render(results);
  scheduleNext();
}
async function fetchMetrics(r) {
  try {
    const res = await fetch(`${r.url}/metrics`, { signal: AbortSignal.timeout(1500) });
    if (!res.ok) throw new Error(`http ${res.status}`);
    return { replica: r, ok: true, data: await res.json() };
  } catch (err) {
    return { replica: r, ok: false, error: err.message };
  }
}
function scheduleNext() { setTimeout(poll, POLL_MS); }

// ─── rendering ────────────────────────────────────────────────────
function render(results) {
  renderSummary(results);
  renderNodes(results);
  renderTimeline(results);
}

function renderSummary(results) {
  const alive = results.filter((r) => r.ok);
  const leader = alive.find((r) => r.data.status.role === "leader");
  chipLeader.textContent = `leader: ${leader ? leader.replica.id : "—"}`;
  const term = Math.max(0, ...alive.map((r) => r.data.status.term));
  chipTerm.textContent = `term: ${term}`;
  const commit = Math.max(-1, ...alive.map((r) => r.data.status.commitIndex));
  chipCommit.textContent = `commit: ${commit}`;
  summaryEl.textContent = `${alive.length}/${REPLICAS.length} online — leader: ${leader ? leader.replica.id : "none"}`;
}

function renderNodes(results) {
  nodesEl.innerHTML = "";
  for (const r of results) nodesEl.appendChild(renderNodeCard(r));
}

function renderNodeCard(r) {
  const card = document.createElement("div");
  card.className = "node-card";

  if (!r.ok) {
    card.classList.add("down");
    card.innerHTML = `
      <div class="node-header">
        <div class="node-id">${r.replica.id}</div>
        <div class="role-badge down">DOWN</div>
      </div>
      <div class="node-stats"><span>${escapeHtml(r.error)}</span></div>
      <div class="node-actions">
        <button data-node="${r.replica.id}" data-do="wait">(waiting for restart)</button>
      </div>
    `;
    return card;
  }

  const s = r.data.status;
  const f = r.data.faults;
  card.classList.add(s.role);

  const faultBits = [];
  if (f.partitionedPeers.length)         faultBits.push(`partitioned from ${f.partitionedPeers.map(shortPeer).join(", ")}`);
  if (Object.keys(f.peerLatencyMs).length) {
    const pieces = Object.entries(f.peerLatencyMs).map(([p, ms]) => `${shortPeer(p)}+${ms}ms`);
    faultBits.push(`latency ${pieces.join(", ")}`);
  }
  if (Object.keys(f.peerDropProb).length) {
    const pieces = Object.entries(f.peerDropProb).map(([p, prob]) => `${shortPeer(p)} drop ${Math.round(prob*100)}%`);
    faultBits.push(pieces.join(", "));
  }
  if (f.globalLatencyMs > 0) faultBits.push(`global +${f.globalLatencyMs}ms`);
  if (f.globalDropProb  > 0) faultBits.push(`global drop ${Math.round(f.globalDropProb*100)}%`);

  const uptime = Math.round(s.uptimeMs / 1000);

  card.innerHTML = `
    <div class="node-header">
      <div class="node-id">${s.replicaId}</div>
      <div class="role-badge ${s.role}">${s.role.toUpperCase()}</div>
    </div>
    <div class="node-stats">
      <span>term</span><b>${s.term}</b>
      <span>leader</span><b>${s.leaderId ?? "—"}</b>
      <span>log range</span><b>${s.firstIndex}..${s.lastIndex}</b>
      <span>commit / applied</span><b>${s.commitIndex} / ${s.lastApplied}</b>
      <span>snapshots</span><b>${s.snapshotCount} (@ ${s.lastSnapshotIndex})</b>
      <span>restarts / uptime</span><b>${s.restartCount} / ${uptime}s</b>
      <span>dedupe entries</span><b>${s.dedupeSize}</b>
      <span>voted for</span><b>${s.votedFor ?? "—"}</b>
    </div>
    <div class="node-faults">${faultBits.join(" • ")}</div>
    <div class="node-actions">
      <button data-node="${s.replicaId}" data-do="use">Use as fault target</button>
    </div>
  `;
  return card;
}

function renderTimeline(results) {
  // Merge timelines from all nodes into one sorted list.
  const rows = [];
  for (const r of results) {
    if (!r.ok) continue;
    for (const ev of r.data.timeline) rows.push({ ...ev, node: r.replica.id });
  }
  rows.sort((a, b) => b.ts - a.ts);
  const html = rows.slice(0, 60).map((ev) => {
    const cls = ev.type.startsWith("role") && ev.detail?.term ? (ev.detail && ev.type === "role:leader" ? "leader" : "role")
             : ev.type.startsWith("fault") ? "fault"
             : ev.type === "snapshot" ? "snap"
             : ev.type === "boot" ? "boot"
             : "";
    return `<div class="timeline-row ${cls}">
      <span class="ts">${fmtTime(ev.ts)}</span>
      <span class="node">${ev.node}</span>
      <span class="type">${escapeHtml(ev.type)}</span>
      <span class="detail" title='${escapeHtml(JSON.stringify(ev.detail))}'>${escapeHtml(fmtDetail(ev.detail))}</span>
    </div>`;
  }).join("");
  timelineEl.innerHTML = html || `<div class="timeline-row"><span class="detail">no events yet</span></div>`;
}

// ─── fault buttons ────────────────────────────────────────────────
document.querySelectorAll(".btnrow button").forEach((btn) => {
  btn.addEventListener("click", () => handleFaultAction(btn.dataset.action));
});
nodesEl.addEventListener("click", (e) => {
  const btn = e.target.closest("button[data-do='use']");
  if (btn) { targetSel.value = btn.dataset.node; targetSel.dispatchEvent(new Event("change")); }
});

async function handleFaultAction(action) {
  const targetId = targetSel.value;
  const target   = REPLICAS.find((r) => r.id === targetId);
  if (!target) return;
  const peerParam = peerSel.value;   // "all" or replicaN — server resolves
  const latency   = parseInt(latencyIn.value || "0", 10);
  const drop      = parseFloat(dropIn.value || "0");

  const post = (path, body) =>
    fetch(`${target.url}${path}`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body || {}),
    }).catch((err) => console.warn("admin post failed", err));

  switch (action) {
    case "partition": await post("/admin/fault/partition", { peer: peerParam }); break;
    case "heal":      await post("/admin/fault/heal",      { peer: peerParam }); break;
    case "latency":   await post("/admin/fault/latency",   { peer: peerParam, ms: latency }); break;
    case "drop":      await post("/admin/fault/drop",      { peer: peerParam, probability: drop }); break;
    case "clear":     await post("/admin/fault/clear",     {}); break;
    case "crash":
      if (!confirm(`Crash ${targetId}? It will restart automatically (docker restart:on-failure).`)) return;
      await post("/admin/crash", {});
      break;
    case "isolate":
      // Isolate = partition target from every OTHER replica, AND
      // every other replica from target. Two-sided so nothing gets
      // through in either direction. This is the "kill the leader
      // network-wise" demo.
      if (!confirm(`Isolate ${targetId} from the whole cluster?`)) return;
      const others = REPLICAS.filter((r) => r.id !== targetId);
      // Target refuses all outbound
      for (const o of others) await post("/admin/fault/partition", { peer: o.id });
      // Every other node refuses to reach target
      await Promise.all(others.map((o) =>
        fetch(`${o.url}/admin/fault/partition`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ peer: targetId }),
        }).catch(() => {})
      ));
      break;
    case "clear-all":
      await Promise.all(REPLICAS.map((r) =>
        fetch(`${r.url}/admin/fault/clear`, { method: "POST", headers: { "Content-Type": "application/json" }, body: "{}" })
          .catch(() => {})
      ));
      break;
  }
}

// ─── utils ────────────────────────────────────────────────────────
function shortPeer(url) {
  const m = url.match(/\/\/([^:/]+)/);
  return m ? m[1] : url;
}
function fmtTime(ts) {
  const d = new Date(ts);
  return d.toTimeString().slice(0, 8) + "." + String(d.getMilliseconds()).padStart(3, "0");
}
function fmtDetail(d) {
  if (d == null) return "";
  if (typeof d === "string") return d;
  return Object.entries(d).map(([k, v]) => `${k}=${JSON.stringify(v)}`).join(" ");
}
function escapeHtml(s) {
  return String(s ?? "").replace(/[&<>"']/g, (c) => ({ "&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;" }[c]));
}

// Kick off.
poll();