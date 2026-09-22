/**
 * Convergence — Jepsen-lite chaos test.
 *
 * The single strongest invariant of a Raft cluster: every live node
 * agrees on the same prefix of the committed log. This test hammers a
 * 5-node cluster with:
 *   • concurrent writes from multiple clients (retrying same seq on error)
 *   • random partitions healed after random durations
 *   • per-link latency noise
 * Then it heals everything, does one post-recovery confirmation write
 * to prove the cluster is functional, and verifies all live nodes
 * converge on IDENTICAL committed strokes.
 *
 * Passing this reliably is what separates "I wrote Raft" from "I wrote
 * Raft and it actually works under stress."
 */

const { Cluster, sleep } = require("./cluster");

const FAST = { raftConfig: {
  heartbeatMs: 30, electionMinMs: 100, electionMaxMs: 160, rpcTimeoutMs: 200,
}};

function randChoice(arr) { return arr[Math.floor(Math.random() * arr.length)]; }
function randInt(max)    { return Math.floor(Math.random() * max); }

describe("Convergence under chaos", () => {
  let cluster;
  afterEach(async () => { if (cluster) await cluster.stop(); cluster = null; });

  test("5-node cluster + random chaos + concurrent writes → all nodes converge", async () => {
    cluster = await new Cluster({ n: 5, ...FAST }).start();
    await cluster.waitForLeader();

    const CHAOS_DURATION_MS = 3000;
    const CLIENTS = 3;
    const t0 = Date.now();

    async function runClient(clientId) {
      let seq = 1;
      while (Date.now() - t0 < CHAOS_DURATION_MS) {
        const stroke = { clientId, seq, x1: seq, y1: seq, x2: seq + 1, y2: seq + 1 };
        try {
          const leader = cluster.live().find((r) => r.node.role === "leader");
          if (!leader) { await sleep(30); continue; }
          await leader.node.submit({ type: "stroke", payload: stroke });
          seq++;
        } catch { await sleep(20); /* retry same seq */ }
      }
      return seq - 1;
    }

    async function runChaos() {
      while (Date.now() - t0 < CHAOS_DURATION_MS) {
        const live = cluster.live();
        if (live.length === 0) return;
        const action = randChoice(["isolate", "heal", "latency", "latency"]);
        switch (action) {
          case "isolate": {
            const victim = randChoice(live);
            cluster.isolate(victim.id);
            await sleep(200 + randInt(300));
            cluster.heal();
            break;
          }
          case "heal":
            cluster.heal();
            await sleep(200);
            break;
          case "latency": {
            const a = randChoice(live), b = randChoice(live);
            if (a.id !== b.id) cluster.setLinkLatency(a.id, b.id, 50 + randInt(150));
            await sleep(200);
            break;
          }
        }
      }
    }

    const clientPromises = Array.from({ length: CLIENTS }, (_, i) => runClient(`c${i + 1}`));
    const chaosPromise = runChaos();
    const submitCounts = await Promise.all(clientPromises);
    await chaosPromise;

    // Recovery window: heal, let cluster stabilize, wait for all live
    // nodes to catch up to whatever the leader has committed.
    cluster.heal();
    await cluster.waitForLeader(5000);
    await sleep(3000);

    // Post-recovery confirmation write — proves the cluster is actually
    // functional again. Retry against whatever the current leader is
    // (like a real client would); no matter which node ends up leader,
    // at least one submit path must succeed for the cluster to be alive.
    let confirmed = false;
    for (let attempt = 0; attempt < 10 && !confirmed; attempt++) {
      const l = cluster.live().find((r) => r.node.role === "leader");
      if (!l) { await sleep(100); continue; }
      try {
        await l.node.submit({
          type: "stroke",
          payload: { clientId: "recovery", seq: 1, x1: 999, y1: 999, x2: 1000, y2: 1000 },
        });
        confirmed = true;
      } catch { await sleep(100); }
    }
    expect(confirmed).toBe(true);

    // Wait for all live nodes to catch up to the leader's commitIndex.
    const leader = cluster.live().find((r) => r.node.role === "leader");
    await cluster.waitUntil(
      () => cluster.live().every((r) => r.node.commitIndex >= leader.node.commitIndex),
      8000,
    );

    // THE invariant: every live node has identical committed strokes.
    cluster.assertConvergence();

    const totalSubmitted = submitCounts.reduce((a, b) => a + b, 0);
    const finalStrokeCount = leader.stateMachine.strokes.length;
    expect(finalStrokeCount).toBeGreaterThan(0);

    console.log(
      `[convergence] ${CLIENTS} clients submitted ${totalSubmitted} chaos-phase strokes ` +
      `over ${CHAOS_DURATION_MS}ms of chaos; final committed strokes: ${finalStrokeCount}; ` +
      `all ${cluster.live().length} live nodes converged.`,
    );
  }, 60000);
});