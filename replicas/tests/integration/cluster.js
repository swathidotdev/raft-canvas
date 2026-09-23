/**
 * Cluster — in-process test harness for the Raft implementation.
 *
 * Spins up N real RaftNode instances backed by real SQLite persistence,
 * wired together via a fake in-memory transport that routes RPCs by
 * looking peers up in a registry. No HTTP, no docker, no ports — the
 * whole cluster runs in one process, elections happen in milliseconds,
 * and failure injection is a method call.
 *
 * The fake transport implements the SAME interface as the real one
 * (call / broadcast + chaos hooks), so RaftNode can't tell the
 * difference. That's what makes these tests actually meaningful.
 */

const fs   = require("fs");
const path = require("path");
const os   = require("os");

const { Persistence }         = require("../../raft/Persistence");
const { StateMachine }        = require("../../raft/StateMachine");
const { RaftNode, ENTRY_TYPE } = require("../../raft/RaftNode");
const { createLogger }        = require("../../raft/logger");

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

class FakeTransport {
  constructor(replicaId, registry) {
    this.replicaId = replicaId;
    this.registry = registry;
    this._partitionedPeers = new Set();
    this._peerLatencyMs = new Map();
    this._peerDropProb  = new Map();
    this._globalLatencyMs = 0;
    this._globalDropProb  = 0;
    this._peerStats = new Map();
  }

  partitionPeer(u) { this._partitionedPeers.add(u); }
  healPeer(u)      { this._partitionedPeers.delete(u); }
  healAll()        { this._partitionedPeers.clear(); }
  setPeerLatency(u, ms) { ms <= 0 ? this._peerLatencyMs.delete(u) : this._peerLatencyMs.set(u, ms); }
  setPeerDrop(u, p)     { p  <= 0 ? this._peerDropProb.delete(u)  : this._peerDropProb.set(u, Math.min(1, p)); }
  setGlobalLatency(ms) { this._globalLatencyMs = Math.max(0, ms); }
  setGlobalDrop(p)     { this._globalDropProb  = Math.max(0, Math.min(1, p)); }
  clearAllFaults() {
    this._partitionedPeers.clear();
    this._peerLatencyMs.clear();
    this._peerDropProb.clear();
    this._globalLatencyMs = 0;
    this._globalDropProb  = 0;
  }
  getFaultStatus() { return {}; }
  getMetrics()     { return { perPeer: Object.fromEntries(this._peerStats) }; }

  async call(peerUrl, rpcPath, body) {
    // Sender-side chaos
    if (this._partitionedPeers.has(peerUrl)) {
      const err = new Error("partitioned"); err.code = "PARTITIONED"; throw err;
    }
    const drop = Math.max(this._peerDropProb.get(peerUrl) || 0, this._globalDropProb);
    if (drop > 0 && Math.random() < drop) {
      const err = new Error("dropped"); err.code = "DROPPED"; throw err;
    }
    const latency = Math.max(this._peerLatencyMs.get(peerUrl) || 0, this._globalLatencyMs);
    if (latency > 0) await sleep(latency);

    const target = this.registry.get(peerUrl);
    if (!target) { const err = new Error("no such peer"); err.code = "UNREACHABLE"; throw err; }

    // Receiver-side chaos: if TARGET has partitioned us, its response can't get back.
    if (target.transport._partitionedPeers.has(this._selfUrl)) {
      const err = new Error("target partitioned back"); err.code = "PARTITIONED"; throw err;
    }

    if (rpcPath === "/request-vote")     return { data: target.node.handleRequestVote(body) };
    if (rpcPath === "/append-entries")   return { data: target.node.handleAppendEntries(body) };
    if (rpcPath === "/install-snapshot") return { data: target.node.handleInstallSnapshot(body) };
    throw new Error("unknown rpc " + rpcPath);
  }

  async broadcast(peerUrls, rpcPath, body, timeoutMs) {
    const settled = await Promise.allSettled(peerUrls.map((u) => this.call(u, rpcPath, body, timeoutMs)));
    return settled.map((s, i) => s.status === "fulfilled"
      ? { peer: peerUrls[i], ok: true, response: s.value }
      : { peer: peerUrls[i], ok: false, error: s.reason });
  }
}

class Cluster {
  /**
   * @param {object} opts
   * @param {number} opts.n                Number of nodes
   * @param {number} [opts.snapshotThreshold=100]
   * @param {object} [opts.raftConfig]     Extra RaftNode config
   */
  constructor({ n, snapshotThreshold = 100, raftConfig = {} }) {
    this.n = n;
    this.snapshotThreshold = snapshotThreshold;
    this.raftConfig = raftConfig;
    this.dataDir = fs.mkdtempSync(path.join(os.tmpdir(), "raft-cluster-"));
    this.registry = new Map();
    this.nodes = [];
    this._stopped = false;
  }

  _urlFor(id) { return `mem://${id}`; }

  async start() {
    const ids = Array.from({ length: this.n }, (_, i) => `n${i + 1}`);
    for (const id of ids) {
      const peerIds = ids.filter((x) => x !== id);
      const rec = await this._spawn(id, peerIds);
      this.nodes.push(rec);
    }
    return this;
  }

  async _spawn(id, peerIds) {
    let rec;
    const logger = createLogger(id, () => ({ term: rec?.node?.currentTerm, role: rec?.node?.role }));
    const persistence  = new Persistence({ dataDir: this.dataDir, replicaId: id, logger });
    const transport    = new FakeTransport(id, this.registry);
    const stateMachine = new StateMachine({ logger });
    const url = this._urlFor(id);
    transport._selfUrl = url;
    const peers = peerIds.map((p) => this._urlFor(p));
    const node = new RaftNode({
      replicaId: id, peers, persistence, transport, stateMachine, logger,
      config: { snapshotThreshold: this.snapshotThreshold, ...this.raftConfig },
    });
    rec = { id, url, node, transport, stateMachine, persistence };
    this.registry.set(url, rec);
    await node.start();
    return rec;
  }

  live() { return this.nodes.filter((r) => r); }
  byId(id) { return this.nodes.find((r) => r && r.id === id); }

  async waitForLeader(timeoutMs = 3000) {
    const t = Date.now();
    while (Date.now() - t < timeoutMs) {
      const l = this.live().find((r) => r.node.role === "leader");
      if (l) return l;
      await sleep(20);
    }
    throw new Error("no leader elected within " + timeoutMs + "ms");
  }

  async waitUntil(fn, timeoutMs = 3000) {
    const t = Date.now();
    while (Date.now() - t < timeoutMs) {
      if (fn()) return;
      await sleep(15);
    }
    throw new Error("condition not met within " + timeoutMs + "ms");
  }

  async submit(payload) {
    const leader = await this.waitForLeader();
    return leader.node.submit({ type: ENTRY_TYPE.STROKE, payload });
  }

  fireSubmit(payload) {
    const leader = this.live().find((r) => r.node.role === "leader");
    if (!leader) throw new Error("no leader for fireSubmit");
    return leader.node.submit({ type: ENTRY_TYPE.STROKE, payload });
  }

  killNode(id) {
    const rec = this.byId(id);
    if (!rec) return;
    rec.node.stop();
    rec.persistence.close();
    this.registry.delete(rec.url);
    const idx = this.nodes.indexOf(rec);
    this.nodes[idx] = null;
  }

  async restartNode(id) {
    const peerIds = this.nodes.filter(Boolean).map((r) => r.id).filter((x) => x !== id).concat(
      this.nodes.map((r, i) => r === null ? `n${i + 1}` : null).filter((x) => x && x !== id)
    );
    const uniq = Array.from(new Set(peerIds));
    const rec = await this._spawn(id, uniq);
    const idx = this.nodes.findIndex((r) => r === null);
    if (idx >= 0) this.nodes[idx] = rec;
    else this.nodes.push(rec);
    return rec;
  }

  partition(idA, idB) {
    const a = this.byId(idA), b = this.byId(idB);
    if (!a || !b) return;
    a.transport.partitionPeer(b.url);
    b.transport.partitionPeer(a.url);
  }

  isolate(id) {
    const target = this.byId(id);
    if (!target) return;
    for (const other of this.live()) {
      if (other.id === id) continue;
      target.transport.partitionPeer(other.url);
      other.transport.partitionPeer(target.url);
    }
  }

  heal() {
    for (const r of this.live()) r.transport.clearAllFaults();
  }

  setLinkLatency(idA, idB, ms) {
    const a = this.byId(idA), b = this.byId(idB);
    if (!a || !b) return;
    a.transport.setPeerLatency(b.url, ms);
    b.transport.setPeerLatency(a.url, ms);
  }

  setLinkDrop(idA, idB, prob) {
    const a = this.byId(idA), b = this.byId(idB);
    if (!a || !b) return;
    a.transport.setPeerDrop(b.url, prob);
    b.transport.setPeerDrop(a.url, prob);
  }

  /**
   * The strongest invariant a Raft cluster must uphold:
   *   All live nodes hold identical prefixes of the committed log.
   * We check the applied strokes (post-dedupe) match on every live node.
   */
  assertConvergence() {
    const live = this.live();
    if (live.length < 2) return;
    const ref = live[0];
    const refJson = JSON.stringify(ref.stateMachine.strokes);
    for (const r of live.slice(1)) {
      const rJson = JSON.stringify(r.stateMachine.strokes);
      if (rJson !== refJson) {
        throw new Error(
          `DIVERGENCE: ${ref.id} has ${ref.stateMachine.strokes.length} strokes, ` +
          `${r.id} has ${r.stateMachine.strokes.length}`
        );
      }
    }
  }

  async stop() {
    if (this._stopped) return;
    this._stopped = true;
    for (const r of this.live()) {
      r.node.stop();
      try { r.persistence.close(); } catch {}
    }
    try { fs.rmSync(this.dataDir, { recursive: true, force: true }); } catch {}
  }
}

module.exports = { Cluster, sleep, ENTRY_TYPE };