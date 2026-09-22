/**
 * Benchmark scenarios — driver-agnostic. Each scenario returns a
 * result object the report generator turns into a Markdown row.
 *
 * IMPORTANT: every scenario takes a `tag` string used as a unique
 * clientId prefix. Reusing (clientId, seq) across scenarios would
 * silently hit the dedupe fast-path — you'd measure 0ms commits and
 * inflated throughput because no Raft round-trip actually happens.
 * Bumping the tag per scenario keeps every measurement honest.
 */

const { summarize } = require("./stats");
const { sleep }     = require("../tests/integration/cluster");

const now = () => (typeof performance !== "undefined" ? performance.now() : Date.now());

// ─── throughput ────────────────────────────────────────────────────
async function throughput(driver, { tag, clients = 4, durationMs = 5000 } = {}) {
  const t0 = now();
  let totalCommitted = 0;

  async function client(clientId) {
    let seq = 1;
    while (now() - t0 < durationMs) {
      try {
        await driver.submitOne(clientId, seq);
        seq++;
        totalCommitted++;
      } catch {
        await sleep(5);
      }
    }
  }

  await Promise.all(
    Array.from({ length: clients }, (_, i) => client(`${tag}-tp-${i + 1}`))
  );
  const elapsedMs = now() - t0;
  return {
    name: "throughput",
    clients,
    durationMs: Math.round(elapsedMs),
    committed: totalCommitted,
    throughputPerSec: (totalCommitted / elapsedMs) * 1000,
  };
}

// ─── commit latency ────────────────────────────────────────────────
async function commitLatency(driver, { tag, samples = 200 } = {}) {
  const latencies = [];
  const clientId = `${tag}-lat`;
  for (let i = 1; i <= samples; i++) {
    const t0 = now();
    await driver.submitOne(clientId, i);
    latencies.push(now() - t0);
  }
  return {
    name: "commit-latency-ms",
    samples,
    ...summarize(latencies),
  };
}

// ─── time to re-elect ──────────────────────────────────────────────
async function timeToReelect(driver, { trials = 5, waitBetweenMs = 800 } = {}) {
  const gaps = [];
  for (let i = 0; i < trials; i++) {
    await driver.waitForLeader(5000);
    await sleep(waitBetweenMs);

    await driver.killLeader();
    const t0 = now();

    let gapMs = null;
    for (let poll = 0; poll < 200; poll++) {
      const l = await (driver.findLeader
        ? driver.findLeader()
        : Promise.resolve(driver.currentLeader?.()));
      if (l) { gapMs = now() - t0; break; }
      await sleep(20);
    }
    if (gapMs === null) throw new Error("no new leader after kill in trial " + i);
    gaps.push(gapMs);
  }
  return {
    name: "time-to-reelect-ms",
    trials,
    ...summarize(gaps),
  };
}

module.exports = { throughput, commitLatency, timeToReelect };