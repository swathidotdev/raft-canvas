/**
 * StateMachine — the application layer above Raft.
 *
 * Raft delivers an ordered log of entries; this class turns them into
 * canvas state that clients actually see.
 *
 * IDEMPOTENCY (stage 2):
 *   Each stroke carries a client-generated (clientId, seq). The dedupe
 *   table lives HERE — inside the state machine's state — so it's part
 *   of every snapshot and gets replayed identically on every replica.
 *   That's what makes idempotency survive leader failover: the new
 *   leader inherits the same dedupe view via the log.
 *
 *   Eviction: we cap the table at `dedupeMaxClients` and evict the
 *   least-recently-touched entry when full. Because Map iteration order
 *   is insertion order and we delete-then-set on every touch, the
 *   iteration head is always the LRU candidate. Same log → same
 *   evictions on every replica.
 */

const { ENTRY_TYPE } = require("./types");

class StateMachine {
  constructor({ logger, dedupeMaxClients = 10000 }) {
    this.logger = logger;
    this.strokes = [];
    this.lastApplied = -1;
    this._dedupe = new Map();          // clientId → { seq, index }
    this.dedupeMaxClients = dedupeMaxClients;
  }

  /** Non-destructive lookup used by leader's fast-path check. */
  getDedupeIndex(clientId, seq) {
    const e = this._dedupe.get(clientId);
    if (!e) return null;
    return e.seq >= seq ? e.index : null;
  }

  _touchDedupe(clientId, seq, index) {
    // Delete-then-set keeps insertion order == recency order, so the
    // first-inserted (map head) is always the eviction target. Same
    // log order on every replica ⇒ identical eviction decisions ⇒
    // the state machine stays deterministic.
    this._dedupe.delete(clientId);
    this._dedupe.set(clientId, { seq, index });
    if (this._dedupe.size > this.dedupeMaxClients) {
      const oldest = this._dedupe.keys().next().value;
      this._dedupe.delete(oldest);
    }
  }

  /**
   * Apply a committed entry. Idempotent w.r.t. re-application
   * (safe during crash-recovery replay).
   */
  apply(entry) {
    if (entry.index <= this.lastApplied) return;

    if (entry.type === ENTRY_TYPE.STROKE) {
      const p = entry.payload || {};
      const cid = p.clientId;
      const seq = p.seq;

      // Apply-time dedupe check — the AUTHORITATIVE gate. Catches:
      //   • concurrent duplicate submits that both got into the log
      //   • post-failover duplicates the new leader didn't fast-path
      if (cid != null && seq != null) {
        const prev = this._dedupe.get(cid);
        if (prev && prev.seq >= seq) {
          // Already applied this (client, seq) — drop the stroke, but
          // still advance lastApplied so log compaction can proceed.
          this.lastApplied = entry.index;
          return;
        }
        this._touchDedupe(cid, seq, entry.index);
      }

      // Strip metadata from what we hand to the canvas.
      const { clientId, seq: _s, ...canvasFields } = p;
      this.strokes.push({ index: entry.index, ...canvasFields });
    }

    this.lastApplied = entry.index;
  }

  getStrokesFrom(fromIndex) {
    if (fromIndex <= 0) return this.strokes.slice();
    return this.strokes.filter((s) => s.index >= fromIndex);
  }

  /** Serializable snapshot — strokes + dedupe. Deterministic ordering. */
  getSnapshot() {
    return {
      strokes: this.strokes,
      dedupe:  Array.from(this._dedupe.entries()),
    };
  }

  restoreSnapshot(snap, lastIncludedIndex) {
    this.strokes = snap.strokes || [];
    this._dedupe = new Map(snap.dedupe || []);
    this.lastApplied = lastIncludedIndex;
  }
}

module.exports = { StateMachine };