/**
 * canvas.js — Distributed Drawing Board Frontend
 * Connects to the RAFT Gateway via WebSocket.
 *
 * Wire format (stage 2 — idempotent):
 *   SEND:    { stroke: { x1, y1, x2, y2, color, size, clientId, seq } }
 *   RECEIVE: { type: "stroke", stroke: { ...same fields... }, logIndex }
 *
 * (clientId, seq) is the idempotency key the replica's state machine
 * uses to deduplicate retried strokes. A retry — from a lost response,
 * a leader failover, or a WS reconnect — that carries the same key is
 * silently collapsed to the original commit; every replica agrees on
 * that because dedupe state is part of the replicated log itself.
 *
 * History replay: GET http://localhost:400X/log?from=0
 *   Returns: { entries: [{ index, term, stroke }], commitIndex }
 */

// ─────────────────────────────────────────────────────────────
//  CONFIG  (change these if your ports differ)
// ─────────────────────────────────────────────────────────────
const FRONTEND_HOST = window.location.hostname || "localhost";
const FRONTEND_PROTOCOL = window.location.protocol === "https:" ? "https" : "http";
const FRONTEND_WS_PROTOCOL = window.location.protocol === "https:" ? "wss" : "ws";
const GATEWAY_WS_URL  = `${FRONTEND_WS_PROTOCOL}://${FRONTEND_HOST}:3000`;
const GATEWAY_HTTP    = `${FRONTEND_PROTOCOL}://${FRONTEND_HOST}:3000`;
const REPLICA_URLS    = [
  `${FRONTEND_PROTOCOL}://${FRONTEND_HOST}:4001`,
  `${FRONTEND_PROTOCOL}://${FRONTEND_HOST}:4002`,
  `${FRONTEND_PROTOCOL}://${FRONTEND_HOST}:4003`,
];
const RECONNECT_DELAY_MS  = 2000;   // initial reconnect wait
const RECONNECT_MAX_MS    = 10000;  // max backoff cap

// ─────────────────────────────────────────────────────────────
//  DOM REFS
// ─────────────────────────────────────────────────────────────
const canvas          = document.getElementById("drawing-canvas");
const ctx             = canvas.getContext("2d");
const wsIndicator     = document.getElementById("ws-indicator");
const wsLabel         = document.getElementById("ws-label");
const leaderChip      = document.getElementById("leader-chip");
const logChip         = document.getElementById("log-chip");
const reconnectOverlay = document.getElementById("reconnect-overlay");
const brushSizeInput  = document.getElementById("brush-size");
const sizeValueLabel  = document.getElementById("size-value");
const swatches        = document.querySelectorAll(".swatch");
const customColorInput = document.getElementById("custom-color");
const clearBtn        = document.getElementById("clear-btn");
const eraserBtn       = document.getElementById("eraser-btn");
const debugBtn        = document.getElementById("debug-btn");
const debugPanel      = document.getElementById("debug-panel");
const debugClose      = document.getElementById("debug-close");
const debugRefresh    = document.getElementById("debug-refresh");

// ─────────────────────────────────────────────────────────────
//  DRAWING STATE
// ─────────────────────────────────────────────────────────────

let currentColor    = "#1a1a2e";
let currentSize     = 4;
let isDrawing       = false;
let lastX           = 0;
let lastY           = 0;
let localLogIndex   = -1;   // highest logIndex we've received/drawn
let isEraser        = false;
// ─────────────────────────────────────────────────────────────
//  ERASER TOOL
// ─────────────────────────────────────────────────────────────
eraserBtn.addEventListener("click", () => {
  isEraser = !isEraser;
  if (isEraser) {
    eraserBtn.classList.add("active");
    // Set color to white and visually deactivate swatches
    currentColor = "#ffffff";
    swatches.forEach(s => s.classList.remove("active"));
  } else {
    eraserBtn.classList.remove("active");
    // Restore color to last selected swatch or custom color
    const activeSwatch = Array.from(swatches).find(s => s.classList.contains("active"));
    if (activeSwatch) {
      currentColor = activeSwatch.dataset.color;
    } else {
      currentColor = customColorInput.value;
    }
  }
});

// ─────────────────────────────────────────────────────────────
//  IDEMPOTENCY IDENTITY (stage 2)
// ─────────────────────────────────────────────────────────────
// We use sessionStorage, not localStorage, on purpose:
//   • sessionStorage is per-TAB, so opening two tabs gives two
//     distinct clientIds. If we shared a clientId across tabs,
//     both tabs' seq counters would race and produce duplicate
//     (clientId, seq) pairs — one stroke would get deduped and
//     lost.
//   • sessionStorage still survives a page reload within the same
//     tab, so if a stroke was in flight when the user hit F5, the
//     new page load keeps the same identity and monotonic seq;
//     any lingering retry from the server still dedupes cleanly.
const CLIENT_ID = (() => {
  let id = sessionStorage.getItem("raft.clientId");
  if (!id) {
    id = (typeof crypto !== "undefined" && crypto.randomUUID)
      ? crypto.randomUUID()
      : `client-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
    sessionStorage.setItem("raft.clientId", id);
  }
  return id;
})();

let seqCounter = parseInt(sessionStorage.getItem("raft.seq") || "0", 10);
function nextSeq() {
  seqCounter += 1;
  sessionStorage.setItem("raft.seq", String(seqCounter));
  return seqCounter;
}

// Pending queue: strokes drawn locally, awaiting server echo. On WS
// reconnect we resend the whole queue — safe because the server
// dedupes on (clientId, seq).
const pendingStrokes   = [];

// ─────────────────────────────────────────────────────────────
//  WEBSOCKET STATE
// ─────────────────────────────────────────────────────────────
let ws              = null;
let reconnectDelay  = RECONNECT_DELAY_MS;
let reconnectTimer  = null;
let isReconnecting  = false;

// ─────────────────────────────────────────────────────────────
//  CANVAS SIZING  (fills the wrapper div exactly)
// ─────────────────────────────────────────────────────────────
function resizeCanvas() {
  // Save current drawing before resize
  const imageData = ctx.getImageData(0, 0, canvas.width, canvas.height);
  const wrapper   = canvas.parentElement;
  canvas.width    = wrapper.clientWidth;
  canvas.height   = wrapper.clientHeight;
  // Restore drawing
  ctx.putImageData(imageData, 0, 0);
  setupCtxDefaults();
}

function setupCtxDefaults() {
  ctx.lineCap     = "round";
  ctx.lineJoin    = "round";
  ctx.imageSmoothingEnabled = true;
}

window.addEventListener("resize", resizeCanvas);

// ─────────────────────────────────────────────────────────────
//  DRAW A SINGLE SEGMENT  (the fundamental drawing primitive)
// ─────────────────────────────────────────────────────────────
function drawSegment(x1, y1, x2, y2, color, size) {
  ctx.beginPath();
  ctx.moveTo(x1, y1);
  ctx.lineTo(x2, y2);
  ctx.strokeStyle = color || "#1a1a2e";
  ctx.lineWidth   = size  || 4;
  ctx.lineCap     = "round";
  ctx.lineJoin    = "round";
  ctx.stroke();
}

// ─────────────────────────────────────────────────────────────
//  MOUSE / TOUCH EVENTS
// ─────────────────────────────────────────────────────────────
function getPos(e) {
  const rect = canvas.getBoundingClientRect();
  const scaleX = canvas.width  / rect.width;
  const scaleY = canvas.height / rect.height;

  if (e.touches) {
    const t = e.touches[0];
    return {
      x: (t.clientX - rect.left) * scaleX,
      y: (t.clientY - rect.top)  * scaleY,
    };
  }
  return {
    x: (e.clientX - rect.left) * scaleX,
    y: (e.clientY - rect.top)  * scaleY,
  };
}

function onPointerDown(e) {
  e.preventDefault();
  isDrawing = true;
  const pos = getPos(e);
  lastX = pos.x;
  lastY = pos.y;
  // Draw a dot at start point
  drawSegment(pos.x, pos.y, pos.x + 0.01, pos.y + 0.01, currentColor, currentSize);
  sendStroke(pos.x, pos.y, pos.x + 0.01, pos.y + 0.01);
}

function onPointerMove(e) {
  if (!isDrawing) return;
  e.preventDefault();
  const pos  = getPos(e);
  const x2   = pos.x;
  const y2   = pos.y;

  // Draw locally immediately (feels instant)
  drawSegment(lastX, lastY, x2, y2, currentColor, currentSize);
  sendStroke(lastX, lastY, x2, y2);

  lastX = x2;
  lastY = y2;
}

function onPointerUp(e) {
  isDrawing = false;
}

canvas.addEventListener("mousedown",  onPointerDown);
canvas.addEventListener("mousemove",  onPointerMove);
canvas.addEventListener("mouseup",    onPointerUp);
canvas.addEventListener("mouseleave", onPointerUp);

canvas.addEventListener("touchstart",  onPointerDown, { passive: false });
canvas.addEventListener("touchmove",   onPointerMove, { passive: false });
canvas.addEventListener("touchend",    onPointerUp);
canvas.addEventListener("touchcancel", onPointerUp);

// ─────────────────────────────────────────────────────────────
//  SEND STROKE TO GATEWAY  (over WebSocket)
// ─────────────────────────────────────────────────────────────
function makeStroke(x1, y1, x2, y2) {
  return {
    clientId: CLIENT_ID,
    seq:      nextSeq(),
    x1:       Math.round(x1 * 100) / 100,
    y1:       Math.round(y1 * 100) / 100,
    x2:       Math.round(x2 * 100) / 100,
    y2:       Math.round(y2 * 100) / 100,
    color:    currentColor,
    size:     currentSize,
  };
}

function flushPendingStrokes() {
  if (!ws || ws.readyState !== WebSocket.OPEN) return;
  // Resending on reconnect is safe: the server dedupes on (clientId, seq),
  // so any stroke that already committed just returns its original logIndex
  // and no duplicate is created.
  for (const stroke of pendingStrokes) {
    ws.send(JSON.stringify({ stroke }));
  }
}

function sendStroke(x1, y1, x2, y2) {
  const stroke = makeStroke(x1, y1, x2, y2);
  pendingStrokes.push(stroke);

  if (!ws || ws.readyState !== WebSocket.OPEN) return;
  ws.send(JSON.stringify({ stroke }));
}

// ─────────────────────────────────────────────────────────────
//  WEBSOCKET CONNECTION
// ─────────────────────────────────────────────────────────────
function connectWebSocket() {
  if (ws) {
    ws.onopen = ws.onmessage = ws.onclose = ws.onerror = null;
    try { ws.close(); } catch (_) {}
  }

  setStatus("connecting");
  ws = new WebSocket(GATEWAY_WS_URL);

  ws.onopen = () => {
    setStatus("connected");
    reconnectDelay = RECONNECT_DELAY_MS;
    isReconnecting = false;
    reconnectOverlay.classList.add("hidden");
    flushPendingStrokes();
  };

  ws.onmessage = (event) => {
    let msg;
    try {
      msg = JSON.parse(event.data);
    } catch {
      console.warn("[canvas] Bad WS message:", event.data);
      return;
    }

    if (msg.type === "stroke" && msg.stroke) {
      const s = msg.stroke;

      // Match echoed stroke to our pending queue on (clientId, seq).
      // If we find it, we already drew it locally — nothing to redraw.
      // If we don't, it came from another client — draw it now.
      const pendingIndex = pendingStrokes.findIndex(
        (p) => p.clientId === s.clientId && p.seq === s.seq,
      );
      if (pendingIndex !== -1) {
        pendingStrokes.splice(pendingIndex, 1);
      } else {
        drawSegment(s.x1, s.y1, s.x2, s.y2, s.color, s.size);
      }

      if (msg.logIndex > localLogIndex) {
        localLogIndex = msg.logIndex;
        logChip.textContent = `Log: ${localLogIndex + 1}`;
      }
    }
  };

  ws.onclose = () => {
    setStatus("disconnected");
    scheduleReconnect();
  };

  ws.onerror = (err) => {
    console.error("[canvas] WS error:", err);
    ws.close();
  };
}

function scheduleReconnect() {
  if (reconnectTimer) return;
  isReconnecting = true;
  setStatus("reconnecting");

  // Only show the overlay after 1s of disconnect (avoids flicker on fast failovers)
  const overlayTimer = setTimeout(() => {
    if (isReconnecting) reconnectOverlay.classList.remove("hidden");
  }, 1000);

  reconnectTimer = setTimeout(() => {
    reconnectTimer = null;
    connectWebSocket();
    reconnectDelay = Math.min(reconnectDelay * 1.5, RECONNECT_MAX_MS);
  }, reconnectDelay);
}

// ─────────────────────────────────────────────────────────────
//  STATUS UI
// ─────────────────────────────────────────────────────────────
function setStatus(state) {
  wsIndicator.className = "indicator " + state;
  switch (state) {
    case "connected":
      wsLabel.textContent = "Connected";
      break;
    case "connecting":
      wsLabel.textContent = "Connecting…";
      break;
    case "reconnecting":
      wsLabel.textContent = "Reconnecting…";
      break;
    case "disconnected":
      wsLabel.textContent = "Disconnected";
      break;
  }
}

// ─────────────────────────────────────────────────────────────
//  HISTORY REPLAY  (fetch full committed log on page load)
// ─────────────────────────────────────────────────────────────
async function replayHistory() {
  for (const replicaUrl of REPLICA_URLS) {
    try {
      const res = await fetch(`${replicaUrl}/log?from=0`, { signal: AbortSignal.timeout(2000) });
      if (!res.ok) continue;

      const data = await res.json();
      if (!data.entries || data.entries.length === 0) {
        // No entries here — try the next replica instead of stopping early.
        continue;
      }

      console.log(`[canvas] Replaying ${data.entries.length} strokes from ${replicaUrl}`);

      for (const entry of data.entries) {
        const s = entry.stroke;
        if (s) drawSegment(s.x1, s.y1, s.x2, s.y2, s.color, s.size);
      }

      localLogIndex = data.commitIndex ?? data.entries.length - 1;
      logChip.textContent = `Log: ${localLogIndex + 1}`;
      break; // success — no need to try other replicas

    } catch (err) {
      console.warn(`[canvas] Could not fetch log from ${replicaUrl}:`, err.message);
    }
  }
}

// ─────────────────────────────────────────────────────────────
//  CLEAR CANVAS
// ─────────────────────────────────────────────────────────────
clearBtn.addEventListener("click", () => {
  if (!confirm("Clear the visible canvas locally? This will not affect other users.")) return;
  ctx.clearRect(0, 0, canvas.width, canvas.height);
  ctx.fillStyle = "#ffffff";
  ctx.fillRect(0, 0, canvas.width, canvas.height);
  localLogIndex = -1;
  logChip.textContent = "Log: 0";
});

// ─────────────────────────────────────────────────────────────
//  TOOLBAR — COLOR & SIZE
// ─────────────────────────────────────────────────────────────
swatches.forEach(swatch => {
  swatch.addEventListener("click", () => {
    swatches.forEach(s => s.classList.remove("active"));
    swatch.classList.add("active");
    currentColor = swatch.dataset.color;
    customColorInput.value = currentColor;
    isEraser = false;
    eraserBtn.classList.remove("active");
  });
});

customColorInput.addEventListener("input", (e) => {
  currentColor = e.target.value;
  swatches.forEach(s => s.classList.remove("active"));
  isEraser = false;
  eraserBtn.classList.remove("active");
});

brushSizeInput.addEventListener("input", (e) => {
  currentSize = parseInt(e.target.value);
  sizeValueLabel.textContent = currentSize;
});

// ─────────────────────────────────────────────────────────────
//  DEBUG PANEL
// ─────────────────────────────────────────────────────────────
debugBtn.addEventListener("click", () => {
  debugPanel.classList.toggle("hidden");
  if (!debugPanel.classList.contains("hidden")) fetchDebugInfo();
});

debugClose.addEventListener("click", () => debugPanel.classList.add("hidden"));
debugRefresh.addEventListener("click", fetchDebugInfo);

async function fetchDebugInfo() {
  // Gateway status
  try {
    const res = await fetch(`${GATEWAY_HTTP}/status`, { signal: AbortSignal.timeout(1500) });
    const data = await res.json();
    document.getElementById("dbg-gateway").textContent    = "online";
    document.getElementById("dbg-leader").textContent     = data.currentLeaderId || "none";
    document.getElementById("dbg-clients").textContent    = data.connectedClients ?? "?";
    leaderChip.textContent = `Leader: ${data.currentLeaderId || "—"}`;
  } catch {
    document.getElementById("dbg-gateway").textContent = "offline";
  }

  // Replica statuses
  const replicaIds = ["replica1", "replica2", "replica3"];
  for (let i = 0; i < REPLICA_URLS.length; i++) {
    const url  = REPLICA_URLS[i];
    const id   = replicaIds[i];
    const el   = document.getElementById(`dbg-${id}`);
    const badge = el.querySelector(".badge");

    try {
      const res  = await fetch(`${url}/status`, { signal: AbortSignal.timeout(1000) });
      const data = await res.json();
      badge.textContent = data.state || "?";
      badge.className   = `badge ${data.state === "leader" ? "leader" : "follower"}`;

      el.title = `term=${data.term} logLen=${data.logLength} commitIndex=${data.commitIndex}`;
    } catch {
      badge.textContent = "down";
      badge.className   = "badge down";
    }
  }
}

// ─────────────────────────────────────────────────────────────
//  INIT
// ─────────────────────────────────────────────────────────────
(async function init() {
  // 1. Size the canvas to fill its container
  resizeCanvas();
  setupCtxDefaults();

  // 2. Fill background white (avoids transparent canvas)
  ctx.fillStyle = "#ffffff";
  ctx.fillRect(0, 0, canvas.width, canvas.height);

  // 3. Connect to gateway WebSocket
  connectWebSocket();

  // 4. Replay full stroke history from replicas
  //    (small delay so WebSocket handshake completes first)
  setTimeout(replayHistory, 800);
})();