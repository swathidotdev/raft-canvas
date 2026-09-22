/**
 * Mini-RAFT Replica Server (stage 3 — chaos + observability).
 *
 * Endpoints:
 *   Compat (unchanged shape):
 *     GET  /status
 *     POST /request-vote
 *     POST /append-entries
 *     POST /install-snapshot
 *     POST /heartbeat        (compat shim → append-entries)
 *     POST /sync-log         (compat shim → append-entries)
 *     GET  /log
 *     POST /stroke
 *   Stage 1 addition:
 *     GET  /read-linearizable
 *   Stage 3 additions:
 *     GET  /metrics
 *     POST /admin/fault/partition   { peer: url | "all" }
 *     POST /admin/fault/heal        { peer: url | "all" }
 *     POST /admin/fault/latency     { peer: url | "all", ms: N }
 *     POST /admin/fault/drop        { peer: url | "all", probability: 0..1 }
 *     POST /admin/fault/clear
 *     GET  /admin/fault/status
 *     POST /admin/crash             (graceful process exit — docker restarts)
 */

const express = require("express");
const axios   = require("axios");

const { Persistence }  = require("./raft/Persistence");
const { Transport }    = require("./raft/Transport");
const { StateMachine } = require("./raft/StateMachine");
const { RaftNode, ENTRY_TYPE } = require("./raft/RaftNode");
const { EventLog }     = require("./raft/EventLog");
const { createLogger } = require("./raft/logger");

// ─── env config ──────────────────────────────────────────────────────
const REPLICA_ID  = process.env.REPLICA_ID   || "replica1";
const PORT        = parseInt(process.env.REPLICA_PORT || "4001");
const PEER_URLS   = (process.env.PEERS || "").split(",").filter(Boolean);
const GATEWAY_URL = process.env.GATEWAY_URL  || "http://gateway:3000";
const DATA_DIR    = process.env.DATA_DIR     || "/app/data";

// ─── module wiring ───────────────────────────────────────────────────
let node;
let gatewayNotifyTimer = null;
const logger = createLogger(REPLICA_ID, () => ({ term: node?.currentTerm, role: node?.role }));

const persistence  = new Persistence({ dataDir: DATA_DIR, replicaId: REPLICA_ID, logger });
const transport    = new Transport   ({ replicaId: REPLICA_ID, logger });
const stateMachine = new StateMachine({ logger });
const events       = new EventLog({ capacity: 100 });

node = new RaftNode({
  replicaId: REPLICA_ID,
  peers:     PEER_URLS,
  persistence, transport, stateMachine, logger,
});

// Feed the dashboard timeline off RaftNode's events.
node.on("role", (role) => {
  events.add(`role:${role}`, { term: node.currentTerm });
  if (role === "leader") notifyGateway();
  else if (gatewayNotifyTimer) {
    clearTimeout(gatewayNotifyTimer);
    gatewayNotifyTimer = null;
  }
});
node.on("commit", (commitIndex) => {
  // Commit events happen every write — noisy, so we skip them here and
  // let the dashboard read commitIndex directly from /status instead.
});
node.on("applied", (entry) => {
  if (entry.type !== ENTRY_TYPE.STROKE) return;
  const payload = { stroke: { ...entry.payload }, logIndex: entry.index };
  axios.post(`${GATEWAY_URL}/broadcast`, payload, { timeout: 500 })
    .catch(() => logger.warn("Could not broadcast to gateway"));
});
// Snapshots are worth showing on the timeline — wrap _maybeSnapshot's
// existing log line by watching snapshotCount.
let lastSnapshotCount = 0;
setInterval(() => {
  if (node.snapshotCount > lastSnapshotCount) {
    lastSnapshotCount = node.snapshotCount;
    const meta = persistence.loadMeta();
    events.add("snapshot", {
      lastIncludedIndex: meta.lastSnapshotIndex,
      lastIncludedTerm:  meta.lastSnapshotTerm,
    });
  }
}, 250);

async function notifyGateway() {
  gatewayNotifyTimer = null;
  try {
    await axios.post(`${GATEWAY_URL}/leader`, {
      leaderId: REPLICA_ID,
      leaderUrl: `http://${REPLICA_ID}:${PORT}`,
    }, { timeout: 500 });
    logger.info("Notified gateway of leadership");
  } catch {
    logger.warn("Could not reach gateway — retrying while leader");
    if (node.role === "leader") {
      gatewayNotifyTimer = setTimeout(notifyGateway, 1000);
    }
  }
}

// ─── HTTP shell ──────────────────────────────────────────────────────
const app = express();
app.use(express.json({ limit: "1mb" }));
app.use((req, res, next) => {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET,POST,OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type");
  next();
});
app.options("*", (req, res) => res.sendStatus(204));

// ─── existing endpoints (unchanged) ──────────────────────────────────
app.get("/status", (req, res) => {
  const s = node.getStatus();
  res.json({
    replicaId:    s.replicaId,
    state:        s.role,
    term:         s.term,
    leaderId:     s.leaderId,
    logLength:    s.lastIndex + 1,
    commitIndex:  s.commitIndex,
    lastApplied:  s.lastApplied,
    firstIndex:   s.firstIndex,
    matchIndex:   s.matchIndex,
    nextIndex:    s.nextIndex,
    restartCount: s.restartCount,
    uptimeMs:     s.uptimeMs,
    snapshotCount: s.snapshotCount,
    lastSnapshotIndex: s.lastSnapshotIndex,
    lastSnapshotTerm:  s.lastSnapshotTerm,
    dedupeSize:   s.dedupeSize,
  });
});

app.post("/request-vote",     (req, res) => res.json(node.handleRequestVote(req.body)));
app.post("/append-entries",   (req, res) => res.json(node.handleAppendEntries(req.body)));
app.post("/install-snapshot", (req, res) => res.json(node.handleInstallSnapshot(req.body)));

app.post("/heartbeat", (req, res) => {
  const b = req.body;
  res.json(node.handleAppendEntries({
    term: b.term, leaderId: b.leaderId,
    prevLogIndex: b.prevLogIndex ?? persistence.lastIndex(),
    prevLogTerm:  b.prevLogTerm  ?? persistence.lastTerm(),
    entries: [],
    leaderCommit: b.commitIndex ?? b.leaderCommit ?? -1,
  }));
});

app.post("/sync-log", (req, res) => {
  const { term, leaderId, entries, leaderCommit } = req.body;
  if (!entries || entries.length === 0) return res.json({ term: node.currentTerm, success: true });
  const prevIdx  = entries[0].index - 1;
  const prevTerm = prevIdx >= 0 ? (persistence.getEntry(prevIdx)?.term ?? -1) : -1;
  res.json(node.handleAppendEntries({
    term, leaderId,
    prevLogIndex: prevIdx, prevLogTerm: prevTerm,
    entries, leaderCommit,
  }));
});

app.get("/log", (req, res) => {
  const from = parseInt(req.query.from || "0");
  const to   = node.commitIndex;
  const entries = to >= from ? persistence.getRange(from, to) : [];
  const wire = entries
    .filter((e) => e.type === ENTRY_TYPE.STROKE)
    .map((e) => ({ index: e.index, term: e.term, stroke: e.payload }));
  res.json({ entries: wire, commitIndex: node.commitIndex, term: node.currentTerm });
});

app.post("/stroke", async (req, res) => {
  const { stroke } = req.body;
  if (node.role !== "leader") {
    return res.status(403).json({
      error: "Not the leader",
      leaderId: node.leaderId,
      leaderUrl: node.leaderId ? `http://${node.leaderId}:${_portFor(node.leaderId)}` : null,
    });
  }
  try {
    const committed = await node.submit({ type: ENTRY_TYPE.STROKE, payload: stroke });
    res.json({ success: true, logIndex: committed.index, deduped: !!committed.deduped });
  } catch (err) {
    if (err.code === "NOT_LEADER") return res.status(403).json({ error: "Not the leader", leaderId: err.leaderId });
    if (err.code === "LOST_LEADERSHIP" || err.code === "COMMIT_TIMEOUT") {
      return res.status(503).json({ error: err.message, code: err.code });
    }
    logger.error("Stroke submit failed", err.message);
    res.status(500).json({ error: "internal", detail: err.message });
  }
});

app.get("/read-linearizable", async (req, res) => {
  try {
    const { readIndex } = await node.readIndex();
    res.json({ readIndex, strokes: stateMachine.strokes, term: node.currentTerm });
  } catch (err) {
    if (err.code === "NOT_LEADER")         return res.status(403).json({ error: err.message, leaderId: err.leaderId });
    if (err.code === "LEADER_WARMING_UP")  return res.status(503).json({ error: err.message, code: err.code });
    res.status(500).json({ error: err.message, code: err.code });
  }
});

// ─── stage 3: /metrics ───────────────────────────────────────────────
// Superset of /status plus chaos and RPC counters plus timeline.
// The dashboard reads this endpoint on every poll.
app.get("/metrics", (req, res) => {
  res.json({
    status:   node.getStatus(),
    faults:   transport.getFaultStatus(),
    rpc:      transport.getMetrics(),
    timeline: events.list(),
    now:      Date.now(),
  });
});

// ─── stage 3: /admin/fault/* ─────────────────────────────────────────
// "peer" accepts either a full URL from the node's PEERS env, or a
// short id like "replica2" which we resolve against our peer list.
function resolvePeer(input) {
  if (!input) return null;
  if (input === "all") return "all";
  if (PEER_URLS.includes(input)) return input;
  const match = PEER_URLS.find((u) => u.includes(`${input}:`));
  return match || null;
}

function applyToPeers(target, fn) {
  if (target === "all") { for (const p of PEER_URLS) fn(p); return PEER_URLS; }
  fn(target); return [target];
}

app.post("/admin/fault/partition", (req, res) => {
  const target = resolvePeer(req.body.peer);
  if (!target) return res.status(400).json({ error: "unknown peer", got: req.body.peer });
  const affected = applyToPeers(target, (p) => transport.partitionPeer(p));
  events.add("fault:partition", { affected });
  logger.warn("Partition INJECTED", { affected });
  res.json({ ok: true, affected });
});

app.post("/admin/fault/heal", (req, res) => {
  const target = resolvePeer(req.body.peer);
  if (!target) return res.status(400).json({ error: "unknown peer", got: req.body.peer });
  if (target === "all") transport.healAll();
  else transport.healPeer(target);
  events.add("fault:heal", { target: target === "all" ? "all peers" : target });
  logger.info("Partition HEALED", { target });
  res.json({ ok: true });
});

app.post("/admin/fault/latency", (req, res) => {
  const ms = parseInt(req.body.ms ?? 0);
  if (!Number.isFinite(ms) || ms < 0) return res.status(400).json({ error: "bad ms" });
  const target = resolvePeer(req.body.peer);
  if (!target) return res.status(400).json({ error: "unknown peer", got: req.body.peer });
  if (target === "all") transport.setGlobalLatency(ms);
  else transport.setPeerLatency(target, ms);
  events.add("fault:latency", { target: target === "all" ? "all peers" : target, ms });
  logger.warn("Latency INJECTED", { target, ms });
  res.json({ ok: true });
});

app.post("/admin/fault/drop", (req, res) => {
  const prob = parseFloat(req.body.probability ?? 0);
  if (!Number.isFinite(prob) || prob < 0 || prob > 1) return res.status(400).json({ error: "bad probability" });
  const target = resolvePeer(req.body.peer);
  if (!target) return res.status(400).json({ error: "unknown peer", got: req.body.peer });
  if (target === "all") transport.setGlobalDrop(prob);
  else transport.setPeerDrop(target, prob);
  events.add("fault:drop", { target: target === "all" ? "all peers" : target, probability: prob });
  logger.warn("Drop INJECTED", { target, probability: prob });
  res.json({ ok: true });
});

app.post("/admin/fault/clear", (req, res) => {
  transport.clearAllFaults();
  events.add("fault:cleared", null);
  logger.info("All faults CLEARED");
  res.json({ ok: true });
});

app.get("/admin/fault/status", (req, res) => res.json(transport.getFaultStatus()));

// ─── stage 3: /admin/crash ───────────────────────────────────────────
// Graceful process exit. In docker with `restart: on-failure` the
// container comes back on its own — the dashboard shows the recovery
// live. Nicer demo than `docker stop`.
app.post("/admin/crash", (req, res) => {
  logger.warn("CRASH requested via admin API — exiting");
  res.json({ ok: true, bye: true });
  // Small delay so response actually flushes.
  setTimeout(() => { try { persistence.close(); } catch {} process.exit(1); }, 50);
});

function _portFor(id) {
  const num = parseInt((id || "").replace("replica", "") || "1");
  return 4000 + num;
}

// ─── startup ─────────────────────────────────────────────────────────
async function main() {
  await node.start();
  events.add("boot", { restartCount: node.restartCount });
  app.listen(PORT, () => {
    console.log("╔══════════════════════════════════════════╗");
    console.log(`║  ${REPLICA_ID} on port ${PORT}  (data: ${DATA_DIR})`);
    console.log(`║  peers: ${PEER_URLS.length}   restart#: ${node.restartCount}`);
    console.log("╚══════════════════════════════════════════╝");
  });
}

process.on("SIGTERM", () => { node.stop(); persistence.close(); process.exit(0); });
process.on("SIGINT",  () => { node.stop(); persistence.close(); process.exit(0); });

main().catch((err) => { console.error("FATAL:", err); process.exit(1); });