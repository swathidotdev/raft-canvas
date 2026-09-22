/**
 * HTTP benchmark driver.
 *
 * Talks to a RUNNING cluster (docker compose up) via the gateway and
 * admin APIs. Numbers here include real end-to-end overhead:
 *   client → HTTP → gateway → HTTP → leader → Raft → SQLite fsync → ack
 *
 * These are the numbers you actually put on a resume, because they
 * reflect what a real client would experience.
 *
 * Assumes gateway on :3000 and replicas on :4001/:4002/:4003 (the
 * docker-compose defaults). Override via constructor opts if different.
 */

const axios = require("axios");
const { sleep } = require("../tests/integration/cluster");

class HttpDriver {
  constructor({
    gatewayUrl = "http://localhost:3000",
    replicas = [
      { id: "replica1", url: "http://localhost:4001" },
      { id: "replica2", url: "http://localhost:4002" },
      { id: "replica3", url: "http://localhost:4003" },
    ],
  } = {}) {
    this.gatewayUrl = gatewayUrl;
    this.replicas   = replicas;
  }

  async setup() {
    for (const r of this.replicas) {
      try {
        await axios.get(`${r.url}/status`, { timeout: 1500 });
      } catch {
        throw new Error(
          `HTTP driver: cannot reach ${r.url}. ` +
          `Is the cluster running? Try 'docker compose up' from ./docker`,
        );
      }
    }
    await this.waitForLeader();
    await this.clearFaults();
  }

  async teardown() {
    await this.clearFaults();
  }

  async findLeader() {
    for (const r of this.replicas) {
      try {
        const res = await axios.get(`${r.url}/status`, { timeout: 800 });
        if (res.data.state === "leader") return r;
      } catch {}
    }
    return null;
  }

  async waitForLeader(timeoutMs = 8000) {
    const t = Date.now();
    while (Date.now() - t < timeoutMs) {
      const l = await this.findLeader();
      if (l) return l;
      await sleep(50);
    }
    throw new Error("no leader elected within " + timeoutMs + "ms");
  }

  async submitOne(clientId, seq, payload = {}) {
    const stroke = {
      clientId, seq,
      x1: seq, y1: seq, x2: seq + 1, y2: seq + 1,
      color: "#000", size: 2, ...payload,
    };
    for (let attempt = 0; attempt < 8; attempt++) {
      try {
        const res = await axios.post(
          `${this.gatewayUrl}/stroke`,
          { stroke },
          { timeout: 3000 },
        );
        return res.data;
      } catch (err) {
        if (attempt === 7) throw err;
        await sleep(50);
      }
    }
  }

  // Fault injection broadcasts to every replica.
  async _broadcastAdmin(path, body) {
    await Promise.all(this.replicas.map((r) =>
      axios.post(`${r.url}${path}`, body || {}, { timeout: 1000 }).catch(() => {})
    ));
  }
  async setGlobalLatency(ms)  { await this._broadcastAdmin("/admin/fault/latency", { peer: "all", ms }); }
  async setGlobalDrop(prob)   { await this._broadcastAdmin("/admin/fault/drop",    { peer: "all", probability: prob }); }
  async clearFaults()         { await this._broadcastAdmin("/admin/fault/clear",   {}); }

  async killLeader() {
    const l = await this.findLeader();
    if (!l) return null;
    try {
      await axios.post(`${l.url}/admin/crash`, {}, { timeout: 1000 });
    } catch {
      // Connection reset is expected as the process exits mid-response.
    }
    return l.id;
  }
}

module.exports = { HttpDriver };