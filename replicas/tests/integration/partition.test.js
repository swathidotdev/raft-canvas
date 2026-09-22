/**
 * Partition integration tests — split-brain safety.
 *
 * The single most important property: an isolated minority can NEVER
 * commit new entries, even if it has an old leader still thinking it's
 * in charge. The majority partition keeps making progress. When the
 * partition heals, the stale leader steps down cleanly.
 */

const { Cluster, sleep } = require("./cluster");

const FAST = { raftConfig: {
  heartbeatMs: 30, electionMinMs: 100, electionMaxMs: 160, rpcTimeoutMs: 200,
}};

describe("Network partitions", () => {
  let cluster;
  afterEach(async () => { if (cluster) await cluster.stop(); cluster = null; });

  test("isolated minority (1 node) cannot commit; majority can", async () => {
    cluster = await new Cluster({ n: 3, ...FAST }).start();
    const oldLeader = await cluster.waitForLeader();

    cluster.isolate(oldLeader.id);

    let newLeader;
    for (let i = 0; i < 100; i++) {
      newLeader = cluster.live().find(
        (r) => r.node.role === "leader" && r.id !== oldLeader.id
      );
      if (newLeader) break;
      await sleep(20);
    }
    expect(newLeader).toBeDefined();

    // The majority leader commits a write.
    const written = await newLeader.node.submit({
      type: "stroke",
      payload: { clientId: "c1", seq: 1, x1: 0, y1: 0, x2: 5, y2: 5 },
    });
    expect(written.index).toBeGreaterThanOrEqual(0);

    // The isolated old leader's attempt to commit must FAIL — no majority.
    let isolatedFailed = false;
    try {
      await oldLeader.node.submit({
        type: "stroke",
        payload: { clientId: "c2", seq: 1, x1: 9, y1: 9, x2: 9, y2: 9 },
      });
    } catch (err) {
      isolatedFailed = true;
      expect(["COMMIT_TIMEOUT", "LOST_LEADERSHIP", "NOT_LEADER"]).toContain(err.code);
    }
    expect(isolatedFailed).toBe(true);
  }, 15000);

  test("post-heal: stale leader steps down when it sees higher term", async () => {
    cluster = await new Cluster({ n: 3, ...FAST }).start();
    const oldLeader = await cluster.waitForLeader();
    const oldTerm   = oldLeader.node.currentTerm;

    cluster.isolate(oldLeader.id);

    await cluster.waitUntil(() =>
      cluster.live().some((r) => r.node.role === "leader" && r.id !== oldLeader.id),
      2500,
    );
    const newLeaderTerm = cluster.live().find(
      (r) => r.node.role === "leader" && r.id !== oldLeader.id
    ).node.currentTerm;
    expect(newLeaderTerm).toBeGreaterThan(oldTerm);

    cluster.heal();

    await cluster.waitUntil(() => oldLeader.node.role === "follower", 2500);
    expect(oldLeader.node.currentTerm).toBeGreaterThanOrEqual(newLeaderTerm);
  }, 15000);
});