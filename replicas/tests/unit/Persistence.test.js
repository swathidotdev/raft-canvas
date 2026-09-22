/**
 * Persistence — the fsync-before-ack layer.
 *
 * We can't test power-loss safety directly in a test suite, but we CAN
 * test the durability contract: whatever was written before close() is
 * still there after reopen(). If this ever regresses, election safety
 * (a restarted node must not vote twice in the same term) breaks.
 */

const fs   = require("fs");
const path = require("path");
const os   = require("os");

const { Persistence } = require("../../raft/Persistence");
const { ENTRY_TYPE }  = require("../../raft/types");

let dataDir;
beforeEach(() => { dataDir = fs.mkdtempSync(path.join(os.tmpdir(), "raft-p-")); });
afterEach(()  => { fs.rmSync(dataDir, { recursive: true, force: true }); });

function open(id = "node") {
  return new Persistence({ dataDir, replicaId: id, logger: console });
}

describe("Persistence", () => {
  test("fresh store: empty log, term 0, no vote", () => {
    const p = open();
    expect(p.loadMeta()).toEqual({
      currentTerm: 0, votedFor: null,
      lastSnapshotIndex: -1, lastSnapshotTerm: -1,
    });
    expect(p.lastIndex()).toBe(-1);
    expect(p.firstIndex()).toBe(0);
    expect(p.count()).toBe(0);
    p.close();
  });

  test("term and vote survive close+reopen (election safety)", () => {
    const p1 = open();
    p1.saveTermAndVote(5, "n2");
    p1.close();

    const p2 = open();
    const meta = p2.loadMeta();
    expect(meta.currentTerm).toBe(5);
    expect(meta.votedFor).toBe("n2");
    p2.close();
  });

  test("log entries persist across reopen", () => {
    const p1 = open();
    p1.appendEntries([
      { index: 0, term: 1, type: ENTRY_TYPE.NOOP,   payload: null },
      { index: 1, term: 1, type: ENTRY_TYPE.STROKE, payload: { clientId: "c", seq: 1, x1: 0, y1: 0 } },
    ]);
    p1.close();

    const p2 = open();
    expect(p2.lastIndex()).toBe(1);
    expect(p2.getEntry(1).payload.seq).toBe(1);
    p2.close();
  });

  test("truncateFrom removes conflicting entries and everything after", () => {
    const p = open();
    p.appendEntries([
      { index: 0, term: 1, type: ENTRY_TYPE.NOOP,   payload: null },
      { index: 1, term: 1, type: ENTRY_TYPE.STROKE, payload: { a: 1 } },
      { index: 2, term: 1, type: ENTRY_TYPE.STROKE, payload: { a: 2 } },
      { index: 3, term: 1, type: ENTRY_TYPE.STROKE, payload: { a: 3 } },
    ]);
    p.truncateFrom(2);
    expect(p.lastIndex()).toBe(1);
    expect(p.getEntry(2)).toBeNull();
    expect(p.getEntry(3)).toBeNull();
    p.close();
  });

  test("snapshot round-trips and updates firstIndex after compaction", () => {
    const p = open();
    for (let i = 0; i <= 10; i++) {
      p.appendEntries([{ index: i, term: 1, type: ENTRY_TYPE.STROKE, payload: { i } }]);
    }
    p.saveSnapshot(5, 1, { strokes: [{ i: "x" }], dedupe: [] });
    p.truncateUpTo(5);

    expect(p.firstIndex()).toBe(6);
    expect(p.lastIndex()).toBe(10);
    const snap = p.loadLatestSnapshot();
    expect(snap.lastIncludedIndex).toBe(5);
    expect(snap.state.strokes).toEqual([{ i: "x" }]);
    p.close();

    const p2 = open();
    expect(p2.loadMeta().lastSnapshotIndex).toBe(5);
    p2.close();
  });

  test("truncateAll wipes log but keeps snapshot", () => {
    const p = open();
    p.appendEntries([{ index: 0, term: 1, type: ENTRY_TYPE.NOOP, payload: null }]);
    p.saveSnapshot(0, 1, { strokes: [], dedupe: [] });
    p.truncateAll();
    expect(p.lastIndex()).toBe(0);
    expect(p.count()).toBe(0);
    expect(p.loadLatestSnapshot().lastIncludedIndex).toBe(0);
    p.close();
  });
});