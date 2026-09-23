const express   = require("express");
const http      = require("http");
const path      = require("path");
const WebSocket = require("ws");
const axios     = require("axios");

const app    = express();
const server = http.createServer(app);
const wss    = new WebSocket.Server({ server });

app.use(express.json());
// ─── stage 3: serve dashboard at /dashboard ─────────────────────────
app.use("/dashboard", express.static(path.join(__dirname, "dashboard")));
app.use((req, res, next) => {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET,POST,OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type");
  next();
});
app.options("*", (req, res) => res.sendStatus(204));

// ─────────────────────────────────────────────────────────────
//  CONFIG
// ─────────────────────────────────────────────────────────────
const PORT = parseInt(process.env.GATEWAY_PORT || "3000");

// All replica URLs to poll when we don't know who the leader is
const REPLICA_URLS = [
  "http://replica1:4001",
  "http://replica2:4002",
  "http://replica3:4003",
];

// ─────────────────────────────────────────────────────────────
//  LEADER STATE
// ─────────────────────────────────────────────────────────────
let currentLeaderUrl = null;   // e.g. "http://replica1:4001"
let currentLeaderId  = null;   // e.g. "replica1"

// ─────────────────────────────────────────────────────────────
//  LOGGING HELPER
// ─────────────────────────────────────────────────────────────
function log(msg, detail = "") {
  const t = new Date().toISOString().slice(11, 23);
  console.log(`[${t}] [GATEWAY] ${msg}${detail ? " | " + detail : ""}`);
}

// ─────────────────────────────────────────────────────────────
//  CONNECTED CLIENTS (WebSocket set)
// ─────────────────────────────────────────────────────────────
const clients = new Set();

wss.on("connection", (ws) => {
  clients.add(ws);
  log("Client connected", `total=${clients.size}`);

  // Client sends a stroke as JSON: { stroke: { x1, y1, x2, y2, color } }
  ws.on("message", async (data) => {
    let stroke;
    try {
      const parsed = JSON.parse(data);
      stroke = parsed.stroke ?? parsed;
    } catch {
      log("Bad message from client — ignoring");
      return;
    }
    log("Stroke received from client", JSON.stringify(stroke).slice(0, 80));
    try {
      await forwardStrokeToLeader(stroke);
    } catch (err) {
      log("Stroke forwarding failed", err.message);
    }
  });

  ws.on("close", () => {
    clients.delete(ws);
    log("Client disconnected", `total=${clients.size}`);
  });

  ws.on("error", (err) => {
    log("Client WS error", err.message);
    clients.delete(ws);
  });
});

// ─────────────────────────────────────────────────────────────
//  FORWARD STROKE → LEADER
// ─────────────────────────────────────────────────────────────
async function forwardStrokeToLeader(stroke) {
  for (let attempt = 0; attempt < 3; attempt++) {
    if (!currentLeaderUrl) {
      log("No leader known — polling replicas...");
      await discoverLeader();
    }

    if (!currentLeaderUrl) {
      if (attempt === 2) {
        const err = new Error("No leader available");
        err.code = "NO_LEADER";
        throw err;
      }
      log("No leader found — retrying discovery");
      continue;
    }

    try {
      const res = await axios.post(
        `${currentLeaderUrl}/stroke`,
        { stroke },
        { timeout: 1000 },
      );
      log("Stroke forwarded ✓", `leader=${currentLeaderId} logIndex=${res.data.logIndex}`);
      return res.data;
    } catch (err) {
      const redirectUrl = err.response?.data?.leaderUrl;
      const redirectId  = err.response?.data?.leaderId;
      if (redirectUrl) {
        log("Redirected to real leader", `${redirectId} @ ${redirectUrl}`);
        currentLeaderUrl = redirectUrl;
        currentLeaderId  = redirectId;
      } else {
        currentLeaderUrl = null;
      }
      if (attempt === 2) throw err;
      log("Forward failed — retrying", err.message);
    }
  }
}

// ─────────────────────────────────────────────────────────────
//  LEADER DISCOVERY — ask each replica who is leader
// ─────────────────────────────────────────────────────────────
async function discoverLeader() {
  for (const url of REPLICA_URLS) {
    try {
      const res = await axios.get(`${url}/status`, { timeout: 500 });
      if (res.data.state === "leader") {
        currentLeaderUrl = url;
        currentLeaderId  = res.data.replicaId;
        log("Leader discovered via poll", `${currentLeaderId} @ ${url}`);
        return;
      }
    } catch {
      // This replica is down — try next
    }
  }
  log("Discovery failed — no replica is leader yet");
}

// ─────────────────────────────────────────────────────────────
//  HTTP ENDPOINTS
// ─────────────────────────────────────────────────────────────

/**
 * POST /stroke
 * Forward one stroke to the current leader and return its commit ack.
 * The same payload is retried so Raft can dedupe a lost response.
 */
app.post("/stroke", async (req, res) => {
  const { stroke } = req.body;
  if (!stroke) return res.status(400).json({ error: "stroke required" });

  try {
    const result = await forwardStrokeToLeader(stroke);
    return res.json(result);
  } catch (err) {
    log("HTTP stroke failed", err.message);
    return res.status(503).json({ error: err.message, code: err.code || "FORWARD_FAILED" });
  }
});

/**
 * POST /leader
 * Called automatically by a replica when it wins an election.
 * Body: { leaderId: "replica1", leaderUrl: "http://replica1:4001" }
 */
app.post("/leader", (req, res) => {
  const { leaderId, leaderUrl } = req.body;
  if (!leaderId || !leaderUrl)
    return res.status(400).json({ error: "leaderId and leaderUrl required" });

  log("🏆 New leader registered", `id=${leaderId} url=${leaderUrl}`);
  currentLeaderId  = leaderId;
  currentLeaderUrl = leaderUrl;
  return res.json({ success: true });
});

/**
 * POST /broadcast
 * Called by the leader after a stroke is committed by majority.
 * Body: { stroke: {...}, logIndex: N }
 * Gateway pushes stroke to ALL connected WebSocket clients.
 */
app.post("/broadcast", (req, res) => {
  const { stroke, logIndex } = req.body;
  if (!stroke)
    return res.status(400).json({ error: "stroke required" });

  const payload = JSON.stringify({ type: "stroke", stroke, logIndex });
  let sent = 0;

  for (const ws of clients) {
    if (ws.readyState === WebSocket.OPEN) {
      ws.send(payload);
      sent++;
    } else {
      clients.delete(ws); // prune dead connections
    }
  }

  log("Broadcast complete", `logIndex=${logIndex} clients=${sent}`);
  return res.json({ success: true, sentTo: sent });
});

/**
 * GET /status
 * Health check — useful for debugging and docker healthcheck.
 */
app.get("/status", (req, res) => {
  res.json({
    service:          "gateway",
    connectedClients: clients.size,
    currentLeaderId,
    currentLeaderUrl,
  });
});

// ─────────────────────────────────────────────────────────────
//  START SERVER
// ─────────────────────────────────────────────────────────────
server.listen(PORT, () => {
  console.log("╔══════════════════════════════════════════╗");
  console.log(`║  Gateway listening on port ${PORT}           ║`);
  console.log("╚══════════════════════════════════════════╝");
  log("Ready — waiting for replica leader registration");
});