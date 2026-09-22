/**
 * RaftNode — unit tests for the consensus invariants.
 *
 * These use the Cluster harness so we're exercising the REAL state-
 * transition logic, but with in-memory transport for speed. Each test
 * is isolated in its own temp data dir.
 */

const { Cluster, sleep } = require("../integration/cluster");

// Faster timers so tests don't take forever. Election window shrunk 5x,
// heartbeat 3x. Real system uses 150ms hb / 500-800ms election.
const FAST = {
  raftConfig: {
    heartbeatMs: 30, electionMinMs: 100, electionMaxMs: 160,
    rpcTimeoutMs: 200,
  },
};

describe("RaftNode invariants", () => {
  let cluster;
  afterEach(async () => { if (cluster) await cluster.stop(); cluster = null; });

  test("election safety: at most one leader per term", async () => {
    cluster = await new Cluster({ n: 5, ...FAST }).start();
    await cluster.waitForLeader();
    // Sample over 300ms; at every observation moment, at most one leader
    // per term.
    for (let i = 0; i < 15; i++) {
      const leaders = cluster.live().filter((r) => r.node.role === "leader");
      const termsSeen = new Set();
      for (const l of leaders) {
        expect(termsSeen.has(l.node.currentTerm)).toBe(false);
        termsSeen.add(l.node.currentTerm);
      }
      await sleep(20);
    }
  });

  test("no-op-on-election commits immediately, unlocking §5.4.2 rule", async () => {
    cluster = await new Cluster({ n: 3, ...FAST }).start();
    const leader = await cluster.waitForLeader();
    await cluster.waitUntil(() => leader.node.commitIndex >= 0, 1000);
    const entry0 = leader.persistence.getEntry(0);
    expect(entry0.type).toBe("noop");
    expect(entry0.term).toBe(leader.node.currentTerm);
  });

  test("stroke submitted after election commits + applies on all nodes", async () => {
    cluster = await new Cluster({ n: 3, ...FAST }).start();
    await cluster.waitForLeader();
    const res = await cluster.submit({ clientId: "c1", seq: 1, x1: 0, y1: 0, x2: 5, y2: 5 });
    expect(res.deduped).toBeFalsy();
    await cluster.waitUntil(() =>
      cluster.live().every((r) => r.stateMachine.strokes.length === 1)
    );
    cluster.assertConvergence();
  });

  test("log matching: conflicting entries are rejected by the follower", async () => {
    cluster = await new Cluster({ n: 3, ...FAST }).start();
    const leader = await cluster.waitForLeader();
    const follower = cluster.live().find((r) => r.node.role === "follower");
    const reply = follower.node.handleAppendEntries({
      term: leader.node.currentTerm,
      leaderId: leader.id,
      prevLogIndex: 0,
      prevLogTerm: 999,   // wrong term for a real prev entry
      entries: [{ index: 1, term: leader.node.currentTerm, type: "stroke", payload: { clientId: "x", seq: 1 } }],
      leaderCommit: -1,
    });
    expect(reply.success).toBe(false);
  });

  test("higher-term RPC forces step-down (term monotonicity)", async () => {
    cluster = await new Cluster({ n: 3, ...FAST }).start();
    const leader = await cluster.waitForLeader();
    const term0 = leader.node.currentTerm;

    const reply = leader.node.handleAppendEntries({
      term: term0 + 5,
      leaderId: "ghost",
      prevLogIndex: -1, prevLogTerm: -1,
      entries: [], leaderCommit: -1,
    });
    expect(reply.success).toBe(true);
    expect(leader.node.role).toBe("follower");
    expect(leader.node.currentTerm).toBe(term0 + 5);
  });
});