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
  constructor({ logger, dedupeMaxClients = 10000 }) {
    this.logger = logger;
    this.strokes = [];
    this.lastApplied = -1;
    this._dedupe = new Map();
    this.dedupeMaxClients = dedupeMaxClients;
  }

  /** Non-destructive lookup used by leader's fast-path check. */
  getDedupeIndex(clientId, seq) {
    const entry = this._dedupe.get(clientId);
    if (!entry) return null;
    return entry.seq >= seq ? entry.index : null;
  }

  _touchDedupe(clientId, seq, index) {
    this._dedupe.delete(clientId);
    this._dedupe.set(clientId, { seq, index });
    if (this._dedupe.size > this.dedupeMaxClients) {
      const oldest = this._dedupe.keys().next().value;
      this._dedupe.delete(oldest);
    }
  }

  /**
   * Apply a single committed entry. Idempotent w.r.t. re-application:
   * safe to call again with the same entry (won't double-apply). This
   * matters during crash recovery when we replay the log.
   */
  apply(entry) {
    if (entry.index <= this.lastApplied) return;

    if (entry.type === ENTRY_TYPE.STROKE) {
      const payload = entry.payload || {};
      const clientId = payload.clientId;
      const seq = payload.seq;

      if (clientId != null && seq != null) {
        const prev = this._dedupe.get(clientId);
        if (prev && prev.seq >= seq) {
          this.lastApplied = entry.index;
          return;
        }
        this._touchDedupe(clientId, seq, entry.index);
      }

      const { clientId: _clientId, seq: _seq, ...canvasFields } = payload;
      this.strokes.push({ index: entry.index, ...canvasFields });
    }

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
