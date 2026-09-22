/**
 * StateMachine — dedupe correctness and snapshot round-trip.
 *
 * The core promise: same log, same state, everywhere. That means:
 *   (a) apply() is idempotent w.r.t. re-application (crash-recovery replay safe)
 *   (b) dedupe drops (client, seq<=prev) entries deterministically
 *   (c) snapshot serialize/restore preserves both strokes AND dedupe
 */

const { StateMachine } = require("../../raft/StateMachine");
const { ENTRY_TYPE }   = require("../../raft/types");

function makeStroke(index, clientId, seq, extra = {}) {
  return {
    index, term: 1, type: ENTRY_TYPE.STROKE,
    payload: { clientId, seq, x1: 0, y1: 0, x2: 1, y2: 1, ...extra },
  };
}

describe("StateMachine", () => {
  test("applies strokes in order and tracks lastApplied", () => {
    const sm = new StateMachine({ logger: console });
    sm.apply(makeStroke(0, "c", 1));
    sm.apply(makeStroke(1, "c", 2));
    expect(sm.strokes).toHaveLength(2);
    expect(sm.lastApplied).toBe(1);
  });

  test("apply is idempotent (safe replay)", () => {
    const sm = new StateMachine({ logger: console });
    const e = makeStroke(0, "c", 1);
    sm.apply(e);
    sm.apply(e);
    sm.apply(e);
    expect(sm.strokes).toHaveLength(1);
  });

  test("dedupe: drops duplicate (clientId, seq) at apply time", () => {
    const sm = new StateMachine({ logger: console });
    sm.apply(makeStroke(0, "c1", 1));
    sm.apply(makeStroke(1, "c1", 1));
    expect(sm.strokes).toHaveLength(1);
    expect(sm.lastApplied).toBe(1);
  });

  test("dedupe: drops out-of-order lower seq (delayed retry)", () => {
    const sm = new StateMachine({ logger: console });
    sm.apply(makeStroke(0, "c1", 5));
    sm.apply(makeStroke(1, "c1", 3));
    expect(sm.strokes).toHaveLength(1);
    expect(sm.strokes[0].index).toBe(0);
  });

  test("dedupe: different clients don't interfere", () => {
    const sm = new StateMachine({ logger: console });
    sm.apply(makeStroke(0, "c1", 1));
    sm.apply(makeStroke(1, "c2", 1));
    sm.apply(makeStroke(2, "c1", 2));
    expect(sm.strokes).toHaveLength(3);
  });

  test("getDedupeIndex returns cached commit index for retries", () => {
    const sm = new StateMachine({ logger: console });
    sm.apply(makeStroke(7, "c1", 3));
    expect(sm.getDedupeIndex("c1", 3)).toBe(7);
    expect(sm.getDedupeIndex("c1", 2)).toBe(7);
    expect(sm.getDedupeIndex("c1", 4)).toBeNull();
    expect(sm.getDedupeIndex("c2", 3)).toBeNull();
  });

  test("dedupe LRU eviction is deterministic across replicas", () => {
    const a = new StateMachine({ logger: console, dedupeMaxClients: 3 });
    const b = new StateMachine({ logger: console, dedupeMaxClients: 3 });
    for (let i = 0; i < 5; i++) {
      const e = makeStroke(i, `c${i}`, 1);
      a.apply(e); b.apply(e);
    }
    expect(a._dedupe.size).toBe(3);
    expect(Array.from(a._dedupe.keys())).toEqual(Array.from(b._dedupe.keys()));
  });

  test("snapshot round-trip preserves strokes AND dedupe", () => {
    const a = new StateMachine({ logger: console });
    a.apply(makeStroke(0, "c1", 1));
    a.apply(makeStroke(1, "c2", 1));

    const snap = a.getSnapshot();
    const b = new StateMachine({ logger: console });
    b.restoreSnapshot(snap, 1);

    expect(b.strokes).toEqual(a.strokes);
    expect(b.lastApplied).toBe(1);
    expect(b.getDedupeIndex("c1", 1)).toBe(0);
    expect(b.getDedupeIndex("c2", 1)).toBe(1);
  });

  test("NOOP entries advance lastApplied but not strokes", () => {
    const sm = new StateMachine({ logger: console });
    sm.apply({ index: 0, term: 1, type: ENTRY_TYPE.NOOP, payload: null });
    sm.apply(makeStroke(1, "c1", 1));
    expect(sm.strokes).toHaveLength(1);
    expect(sm.lastApplied).toBe(1);
  });
});