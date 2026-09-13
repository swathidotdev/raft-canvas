/**
 * Smoke test — runs three RaftNodes in one process using a fake in-memory
 * transport that just calls each other's handler methods directly. No
 * HTTP, no docker. This proves stage 1 works before we spend time
 * rebuilding containers.
 *
 * Checks:
 *   1. A leader is elected within a second or two.
 *   2. Client submissions commit on all three nodes and land in the
 *      state machine.
 *   3. After a simulated crash-and-restart of a follower, its persisted
 *      log survives and catches up.
 */

const fs   = require("fs");
const path = require("path");

const { Persistence }  = require("./raft/Persistence");
const { StateMachine } = require("./raft/StateMachine");
const { RaftNode, ENTRY_TYPE }     = require("./raft/RaftNode");
const { createLogger } = require("./raft/logger");

const DATA_DIR = path.join(__dirname, ".smoketest-data");
// Fresh run every time.
if (fs.existsSync(DATA_DIR)) fs.rmSync(DATA_DIR, { recursive: true, force: true });
fs.mkdirSync(DATA_DIR, { recursive: true });

// Registry mapping "peer URL" → the node it belongs to. The fake
// transport looks nodes up here and calls their handler methods.
const registry = new Map();  // url → RaftNode

class FakeTransport {
  constructor({ replicaId }) {
    this.replicaId = replicaId;
    this._partitionedPeers = new Set();
    this._latencyMs = 0;
    this._dropProb  = 0;
  }
  async call(peerUrl, path, body) {
    const target = registry.get(peerUrl);
    if (!target) throw new Error("no such peer " + peerUrl);
    // Route by path
    if (path === "/request-vote")   return { data: target.handleRequestVote(body) };
    if (path === "/append-entries") return { data: target.handleAppendEntries(body) };
    throw new Error("unknown path " + path);
  }
  async broadcast(peerUrls, path, body) {
    const settled = await Promise.allSettled(
      peerUrls.map((p) => this.call(p, path, body))
    );
    return settled.map((s, i) => s.status === "fulfilled"
      ? { peer: peerUrls[i], ok: true, response: s.value }
      : { peer: peerUrls[i], ok: false, error: s.reason });
  }
}

function makeNode(id, peerIds) {
  const logger = createLogger(id, () => ({ term: node?.currentTerm, role: node?.role }));
  const persistence  = new Persistence({ dataDir: DATA_DIR, replicaId: id, logger });
  const transport    = new FakeTransport({ replicaId: id });
  const stateMachine = new StateMachine({ logger });
  const peers = peerIds.map((p) => `mem://${p}`);
  let node;
  node = new RaftNode({ replicaId: id, peers, persistence, transport, stateMachine, logger });
  registry.set(`mem://${id}`, node);
  return { node, persistence, stateMachine, logger };
}

function sleep(ms) { return new Promise((r) => setTimeout(r, ms)); }

async function waitForLeader(nodes, timeoutMs = 3000) {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    const leader = nodes.find((n) => n.node.role === "leader");
    if (leader) return leader;
    await sleep(30);
  }
  throw new Error("no leader elected within timeout");
}

async function waitUntil(fn, timeoutMs = 3000) {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    if (fn()) return;
    await sleep(20);
  }
  throw new Error("condition not met within timeout");
}

async function main() {
  const a = makeNode("A", ["B", "C"]);
  const b = makeNode("B", ["A", "C"]);
  const c = makeNode("C", ["A", "B"]);
  const nodes = [a, b, c];

  console.log("── starting all three nodes ──");
  await Promise.all(nodes.map((n) => n.node.start()));

  console.log("── waiting for leader ──");
  const leaderRec = await waitForLeader(nodes);
  console.log("leader:", leaderRec.node.replicaId, "term:", leaderRec.node.currentTerm);

  console.log("── submitting 5 strokes ──");
  for (let i = 0; i < 5; i++) {
    await leaderRec.node.submit({
      type: ENTRY_TYPE.STROKE,
      payload: { x1: i, y1: i, x2: i + 1, y2: i + 1, color: "#000", size: 3 },
    });
  }

  console.log("── waiting for all nodes to apply all strokes ──");
  // Each node should have 5 strokes applied. The leader also has a NOOP
  // in the log, so lastApplied will be 5 (indices 0-noop, 1..5 strokes).
  await waitUntil(() => nodes.every((n) => n.stateMachine.strokes.length === 5));

  for (const n of nodes) {
    console.log(
      n.node.replicaId,
      "role=" + n.node.role,
      "term=" + n.node.currentTerm,
      "lastIndex=" + n.persistence.lastIndex(),
      "commit=" + n.node.commitIndex,
      "applied=" + n.stateMachine.lastApplied,
      "strokes=" + n.stateMachine.strokes.length,
    );
  }

  // Sanity: every replica should hold identical committed strokes.
  const shape = JSON.stringify(a.stateMachine.strokes);
  for (const n of nodes) {
    if (JSON.stringify(n.stateMachine.strokes) !== shape) {
      throw new Error("DIVERGENCE — " + n.node.replicaId + " differs");
    }
  }
  console.log("✓ all replicas converged on identical strokes");

  console.log("── simulating crash + restart of a follower ──");
  const follower = nodes.find((n) => n.node.role === "follower");
  console.log("crashing:", follower.node.replicaId);
  follower.node.stop();
  follower.persistence.close();

  // Meanwhile, submit 2 more strokes so we can see the follower catch up.
  for (let i = 0; i < 2; i++) {
    await leaderRec.node.submit({
      type: ENTRY_TYPE.STROKE,
      payload: { x1: 100 + i, y1: 100 + i, x2: 101 + i, y2: 101 + i, color: "#f00", size: 2 },
    });
  }
  console.log("leader submitted 2 more strokes while follower was down");

  // Now rebuild the follower from disk — this is the crash-recovery path.
  registry.delete("mem://" + follower.node.replicaId);
  const rebuilt = makeNode(follower.node.replicaId, ["A", "B", "C"].filter((x) => x !== follower.node.replicaId));
  await rebuilt.node.start();
  console.log(
    rebuilt.node.replicaId,
    "recovered from disk: term=" + rebuilt.node.currentTerm,
    "lastIndex=" + rebuilt.persistence.lastIndex(),
    "votedFor=" + rebuilt.node.votedFor,
  );

  // Put rebuilt into the nodes array so leader's peer list can reach it.
  // (peer list was set at construction; registry is what matters.)
  const idx = nodes.indexOf(follower);
  nodes[idx] = rebuilt;

  // Wait for the rebuilt follower to catch up to 7 strokes.
  await waitUntil(() => rebuilt.stateMachine.strokes.length === 7, 4000);
  console.log("✓ recovered follower caught up to 7 strokes");

  console.log("── final state ──");
  for (const n of nodes) {
    console.log(
      n.node.replicaId,
      "role=" + n.node.role,
      "term=" + n.node.currentTerm,
      "strokes=" + n.stateMachine.strokes.length,
      "restarts=" + n.node.restartCount,
    );
  }

  // Clean shutdown.
  for (const n of nodes) { n.node.stop(); n.persistence.close(); }
  console.log("\n✓ SMOKE TEST PASSED");
  process.exit(0);
}

main().catch((err) => {
  console.error("SMOKE TEST FAILED:", err);
  process.exit(1);
});
