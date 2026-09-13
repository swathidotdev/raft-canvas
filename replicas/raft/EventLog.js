/**
 * EventLog — bounded ring buffer of recent notable events on this node.
 *
 * Read by the dashboard via /metrics. Not persistent — this is a live
 * view, not an audit log. Cheap: O(1) push, O(N) read.
 */

class EventLog {
  constructor({ capacity = 100 } = {}) {
    this.capacity = capacity;
    this._buf = [];
  }

  add(type, detail) {
    this._buf.push({ ts: Date.now(), type, detail: detail ?? null });
    if (this._buf.length > this.capacity) {
      this._buf.splice(0, this._buf.length - this.capacity);
    }
  }

  /** Most recent first. */
  list() {
    return this._buf.slice().reverse();
  }
}

module.exports = { EventLog };