/**
 * Failover integration tests — exactly-once semantics.
 *
 * The bug idempotency was designed to fix: a client submit that ACTUALLY
 * committed but whose response was lost, then retried, appears as a
 * duplicate stroke in the canvas.
 *
 * Here we simulate that by submitting concurrent duplicate requests
 * with the same (clientId, seq) — some may hit different leaders after
 * failover — and asserting the final state has exactly ONE stroke per
 * unique key on EVERY live node.
 */

const { Cluster, sleep } = require("./cluster");

const FAST = { raftConfig: {
  heartbeatMs: 30, electionMinMs: 100, electionMaxMs: 160, rpcTimeoutMs: 200,
}};

describe("Failover & idempotency", () => {
  let cluster;
  afterEach(async () => { if (cluster) await cluster.stop(); cluster = null; });

  test("retried submit with same (clientId, seq) is deduped, not duplicated", async () => {
    cluster = await new Cluster({ n: 3, ...FAST }).start();
    await cluster.waitForLeader();
    const s = { clientId: "cA", seq: 1, x1: 0, y1: 0, x2: 5, y2: 5, color: "#000", size: 3 };
    const r1 = await cluster.submit(s);
    const r2 = await cluster.submit(s);
    const r3 = await cluster.submit(s);
    expect(r1.index).toBe(r2.index);
    expect(r2.index).toBe(r3.index);
    expect(r2.deduped).toBe(true);
    expect(r3.deduped).toBe(true);
    await cluster.waitUntil(() =>
      cluster.live().every((r) => r.stateMachine.strokes.length === 1)
    );
    cluster.assertConvergence();
  });

  test("kill leader mid-write: no duplicate strokes on any live node", async () => {
    cluster = await new Cluster({ n: 3, ...FAST }).start();
    const oldLeader = await cluster.waitForLeader();

    const N = 20;
    const submits = [];
    for (let i = 1; i <= N; i++) {
      const stroke = { clientId: "cA", seq: i, x1: i, y1: i, x2: i+1, y2: i+1 };
      submits.push(
        (async () => {
          for (let attempt = 0; attempt < 5; attempt++) {
            const leader = cluster.live().find((r) => r.node.role === "leader");
            if (!leader) { await sleep(50); continue; }
            try {
              return await leader.node.submit({ type: "stroke", payload: stroke });
            } catch (err) {
              await sleep(50);
            }
          }
          throw new Error("gave up submitting seq=" + i);
        })()
      );
      if (i === Math.floor(N / 2)) cluster.killNode(oldLeader.id);
    }

    await Promise.all(submits);

    await cluster.waitForLeader();
    await sleep(300);

    // Exactly-once check: each live node should have exactly N strokes.
    for (const r of cluster.live()) {
      expect(r.stateMachine.strokes.length).toBe(N);
    }
    cluster.assertConvergence();
  }, 20000);
});