/**
 * StateMachine — the application layer that sits above Raft.
 *
 * Raft's job is: get a consistent, ordered log of entries onto every node.
 * This class's job is: turn that log into the actual thing users see
 * (the canvas state), and know how far along it's gotten.
 *
 * lastApplied is intentionally SEPARATE from Raft's commitIndex:
 *   commitIndex = "safe to apply" (Raft has committed it)
 *   lastApplied = "actually applied to the state machine"
 * The gap between them matters once we add snapshotting in stage 2 —
 * we snapshot state at lastApplied, not at commitIndex.
 *
 * The dedupe table is stubbed here; stage 2 makes strokes idempotent
 * across leader failovers by including (clientId, seq) in the log entry
 * and dropping duplicates at apply time.
 */

const { ENTRY_TYPE } = require("./types");

class StateMachine {
  constructor({ logger }) {
    this.logger = logger;
    this.strokes = [];           // ordered committed strokes (the canvas)
    this.lastApplied = -1;       // highest log index applied so far
    this._dedupe = new Map();    // clientId → highest applied seq (stage 2)
  }

  /**
   * Apply a single committed entry. Idempotent w.r.t. re-application:
   * safe to call again with the same entry (won't double-apply). This
   * matters during crash recovery when we replay the log.
   */
  apply(entry) {
    if (entry.index <= this.lastApplied) return;   // already applied

    if (entry.type === ENTRY_TYPE.STROKE) {
      // Stage 2 will consult this._dedupe here before pushing.
      this.strokes.push({ index: entry.index, ...entry.payload });
    }
    // NOOP entries advance lastApplied but produce no visible effect.

    this.lastApplied = entry.index;
  }

  /** Committed strokes with index >= fromIndex. Used by clients on reconnect. */
  getStrokesFrom(fromIndex) {
    if (fromIndex <= 0) return this.strokes.slice();
    return this.strokes.filter((s) => s.index >= fromIndex);
  }

  /** Serializable snapshot of state (for stage 2 log compaction). */
  getSnapshot() {
    return {
      strokes: this.strokes,
      dedupe:  Array.from(this._dedupe.entries()),
    };
  }

  /** Restore from a snapshot loaded off disk. */
  restoreSnapshot(snap, lastIncludedIndex) {
    this.strokes = snap.strokes || [];
    this._dedupe = new Map(snap.dedupe || []);
    this.lastApplied = lastIncludedIndex;
  }
}

module.exports = { StateMachine };
