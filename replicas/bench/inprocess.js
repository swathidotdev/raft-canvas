/**
 * In-process benchmark driver.
 *
 * Uses the same Cluster harness the tests use — 3 RaftNodes with real
 * SQLite persistence, fake in-memory transport. Numbers here isolate
 * the CONSENSUS overhead: no HTTP, no axios, no network stack. That
 * makes this the right knob-turning target when tuning heartbeat
 * intervals, batching, or the commit loop.
 */

const { Cluster, ENTRY_TYPE, sleep } = require("../tests/integration/cluster");

class InProcessDriver {
  constructor({ n = 3, raftConfig = {} } = {}) {
    this.n = n;
    this.raftConfig = raftConfig;
    this.cluster = null;
  }

  async setup() {
    this.cluster = await new Cluster({
      n: this.n,
      raftConfig: {
        // Slightly faster than production defaults so bench runs don't
        // spend seconds waiting on election timeouts.
        heartbeatMs: 50, electionMinMs: 200, electionMaxMs: 350,
        rpcTimeoutMs: 200, ...this.raftConfig,
      },
    }).start();
    await this.cluster.waitForLeader();
  }

  async teardown() {
    if (this.cluster) await this.cluster.stop();
    this.cluster = null;
  }

  currentLeader() {
    return this.cluster.live().find((r) => r.node.role === "leader");
  }

  async submitOne(clientId, seq, payload = {}) {
    for (let attempt = 0; attempt < 5; attempt++) {
      const leader = this.currentLeader();
      if (!leader) { await sleep(10); continue; }
      try {
        return await leader.node.submit({
          type: ENTRY_TYPE.STROKE,
          payload: { clientId, seq, x1: seq, y1: seq, x2: seq + 1, y2: seq + 1, ...payload },
        });
      } catch (err) {
        if (attempt === 4) throw err;
        await sleep(20);
      }
    }
  }

  // Fault injection — identical semantics to the real Transport.
  setGlobalLatency(ms) {
    for (const r of this.cluster.live()) r.transport.setGlobalLatency(ms);
  }
  setGlobalDrop(prob) {
    for (const r of this.cluster.live()) r.transport.setGlobalDrop(prob);
  }
  clearFaults() {
    for (const r of this.cluster.live()) r.transport.clearAllFaults();
  }
  killLeader() {
    const l = this.currentLeader();
    if (l) this.cluster.killNode(l.id);
    return l ? l.id : null;
  }
  async waitForLeader(timeoutMs = 5000) {
    return this.cluster.waitForLeader(timeoutMs);
  }
}

module.exports = { InProcessDriver };