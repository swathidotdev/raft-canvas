/**
 * Transport — chaos-capable HTTP client for replica-to-replica RPC.
 *
 * Every outbound peer call in the system flows through this class, so
 * chaos policies (partitions, latency, message drops) are enforced in
 * exactly one place. Same choke point also gives us per-peer RPC
 * counters that the dashboard reads via /metrics.
 *
 * CHAOS MODEL — per-node, outbound-only:
 *   • A "partition from B" means: this node refuses to send to B.
 *     For a bidirectional partition, hit BOTH nodes' /admin/fault
 *     endpoints — the dashboard button does this for you.
 *   • Per-peer latency and drop probabilities compose with global
 *     ones by taking the MAX (so you can layer a global 50ms jitter
 *     with a targeted 500ms link degradation without them cancelling).
 *   • None of this affects the replica's OWN internal state — a
 *     "partitioned" node still runs its election timer, still tries
 *     to campaign; it just can't reach anyone. Which is exactly the
 *     failure mode Raft's minority-can't-elect property is designed
 *     to survive.
 */

const axios = require("axios");

class Transport {
  constructor({ replicaId, logger, defaultTimeoutMs = 300 }) {
    this.replicaId = replicaId;
    this.logger = logger;
    this.defaultTimeoutMs = defaultTimeoutMs;

    // Chaos config
    this._partitionedPeers = new Set();
    this._peerLatencyMs    = new Map();   // peerUrl → extra ms
    this._peerDropProb     = new Map();   // peerUrl → [0..1]
    this._globalLatencyMs  = 0;
    this._globalDropProb   = 0;

    // Metrics — per-peer counters, plus a global rollup.
    this._peerStats = new Map();          // peerUrl → { sent, ok, failed, dropped, partitioned }
  }

  // ─── chaos admin (called from HTTP admin routes) ──────────────────
  partitionPeer(peerUrl)  { this._partitionedPeers.add(peerUrl); }
  healPeer(peerUrl)       { this._partitionedPeers.delete(peerUrl); }
  healAll()               { this._partitionedPeers.clear(); }

  setPeerLatency(peerUrl, ms) {
    if (ms <= 0) this._peerLatencyMs.delete(peerUrl);
    else this._peerLatencyMs.set(peerUrl, ms);
  }
  setGlobalLatency(ms) { this._globalLatencyMs = Math.max(0, ms); }

  setPeerDrop(peerUrl, prob) {
    if (prob <= 0) this._peerDropProb.delete(peerUrl);
    else this._peerDropProb.set(peerUrl, Math.min(1, prob));
  }
  setGlobalDrop(prob) { this._globalDropProb = Math.max(0, Math.min(1, prob)); }

  clearAllFaults() {
    this._partitionedPeers.clear();
    this._peerLatencyMs.clear();
    this._peerDropProb.clear();
    this._globalLatencyMs = 0;
    this._globalDropProb  = 0;
  }

  getFaultStatus() {
    return {
      partitionedPeers: Array.from(this._partitionedPeers),
      peerLatencyMs:    Object.fromEntries(this._peerLatencyMs),
      peerDropProb:     Object.fromEntries(this._peerDropProb),
      globalLatencyMs:  this._globalLatencyMs,
      globalDropProb:   this._globalDropProb,
    };
  }

  getMetrics() {
    const perPeer = {};
    for (const [peer, s] of this._peerStats.entries()) perPeer[peer] = { ...s };
    return { perPeer };
  }

  // ─── internal helpers ─────────────────────────────────────────────
  _stats(peerUrl) {
    let s = this._peerStats.get(peerUrl);
    if (!s) {
      s = { sent: 0, ok: 0, failed: 0, dropped: 0, partitioned: 0 };
      this._peerStats.set(peerUrl, s);
    }
    return s;
  }

  _effectiveLatency(peerUrl) {
    const peer = this._peerLatencyMs.get(peerUrl) || 0;
    return Math.max(peer, this._globalLatencyMs);
  }
  _effectiveDrop(peerUrl) {
    const peer = this._peerDropProb.get(peerUrl) || 0;
    return Math.max(peer, this._globalDropProb);
  }

  // ─── the one place outbound RPCs go ───────────────────────────────
  async call(peerUrl, path, body, timeoutMs) {
    const s = this._stats(peerUrl);
    s.sent++;

    if (this._partitionedPeers.has(peerUrl)) {
      s.partitioned++;
      const err = new Error(`partitioned from ${peerUrl}`);
      err.code = "PARTITIONED";
      throw err;
    }

    const drop = this._effectiveDrop(peerUrl);
    if (drop > 0 && Math.random() < drop) {
      s.dropped++;
      const err = new Error(`dropped call to ${peerUrl}${path}`);
      err.code = "DROPPED";
      throw err;
    }

    const latency = this._effectiveLatency(peerUrl);
    if (latency > 0) await new Promise((r) => setTimeout(r, latency));

    try {
      const res = await axios.post(
        `${peerUrl}${path}`,
        body,
        { timeout: timeoutMs ?? this.defaultTimeoutMs },
      );
      s.ok++;
      return res;
    } catch (err) {
      s.failed++;
      throw err;
    }
  }

  /** Fan out; each result carries its peer and outcome. Never throws. */
  async broadcast(peerUrls, path, body, timeoutMs) {
    const settled = await Promise.allSettled(
      peerUrls.map((peer) => this.call(peer, path, body, timeoutMs)),
    );
    return settled.map((s, i) => (
      s.status === "fulfilled"
        ? { peer: peerUrls[i], ok: true, response: s.value }
        : { peer: peerUrls[i], ok: false, error: s.reason }
    ));
  }
}

module.exports = { Transport };