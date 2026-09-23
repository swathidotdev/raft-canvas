const { Cluster } = require("./cluster");

const FAST = { raftConfig: {
  heartbeatMs: 30, electionMinMs: 100, electionMaxMs: 160, rpcTimeoutMs: 200,
}};

async function submitStrokes(cluster, count) {
  for (let seq = 1; seq <= count; seq++) {
    await cluster.submit({
      clientId: "recovery-test",
      seq,
      x1: seq,
      y1: seq,
      x2: seq + 1,
      y2: seq + 1,
    });
  }
}

describe("snapshot, restart, and linearizable reads", () => {
  let cluster;
  afterEach(async () => { if (cluster) await cluster.stop(); cluster = null; });

  test("snapshot compaction preserves the applied canvas state", async () => {
    cluster = await new Cluster({ n: 3, snapshotThreshold: 3, ...FAST }).start();
    const leader = await cluster.waitForLeader();

    await submitStrokes(cluster, 8);
    await cluster.waitUntil(() =>
      cluster.live().every((r) => r.stateMachine.strokes.length === 8)
    );

    expect(leader.node.snapshotCount).toBeGreaterThan(0);
    expect(leader.persistence.firstIndex()).toBeGreaterThan(0);
    cluster.assertConvergence();
  }, 15000);

  test("lagging follower catches up through InstallSnapshot", async () => {
    cluster = await new Cluster({ n: 3, snapshotThreshold: 3, ...FAST }).start();
    const leader = await cluster.waitForLeader();
    const lagging = cluster.live().find((r) => r.id !== leader.id);
    cluster.isolate(lagging.id);

    await submitStrokes(cluster, 8);
    await cluster.waitUntil(() => leader.node.snapshotCount > 0 && leader.persistence.firstIndex() > 0);

    cluster.heal();
    await cluster.waitUntil(() => lagging.stateMachine.strokes.length === 8, 5000);

    expect(lagging.persistence.loadLatestSnapshot().lastIncludedIndex).toBeGreaterThan(0);
    expect(lagging.stateMachine.strokes).toEqual(leader.stateMachine.strokes);
  }, 15000);

  test("restarted node catches up from its existing SQLite file", async () => {
    cluster = await new Cluster({ n: 3, snapshotThreshold: 100, ...FAST }).start();
    const leader = await cluster.waitForLeader();
    await submitStrokes(cluster, 3);

    const original = cluster.live().find((r) => r.id !== leader.id);
    const persistedLastIndex = original.persistence.lastIndex();
    cluster.killNode(original.id);
    const restarted = await cluster.restartNode(original.id);

    expect(restarted.persistence.lastIndex()).toBe(persistedLastIndex);
    await cluster.waitUntil(() => restarted.stateMachine.strokes.length === 3, 5000);
    expect(restarted.stateMachine.strokes).toEqual(cluster.live().find((r) => r.id !== restarted.id).stateMachine.strokes);
  }, 15000);

  test("ReadIndex confirms the leader's committed prefix", async () => {
    cluster = await new Cluster({ n: 3, ...FAST }).start();
    const leader = await cluster.waitForLeader();
    await submitStrokes(cluster, 1);

    const result = await leader.node.readIndex();
    expect(result.readIndex).toBe(leader.node.commitIndex);
    expect(result.term).toBe(leader.node.currentTerm);

    const follower = cluster.live().find((r) => r.id !== leader.id);
    await expect(follower.node.readIndex()).rejects.toMatchObject({ code: "NOT_LEADER" });
  }, 15000);
});