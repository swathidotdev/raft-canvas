/**
 * Shared constants for the Raft module.
 * Keeping these in one place avoids typo-driven bugs across files.
 */

const ROLE = Object.freeze({
  FOLLOWER:  "follower",
  CANDIDATE: "candidate",
  LEADER:    "leader",
});

/**
 * Log entries carry one of these types.
 *   STROKE — a client-submitted drawing command (the "application" data)
 *   NOOP   — a blank entry appended by a new leader on election win.
 *            Committing a NOOP in the current term is what lets the leader
 *            safely conclude that all prior-term entries are also committed
 *            (Raft paper §5.4.2 — "committing entries from previous terms").
 */
const ENTRY_TYPE = Object.freeze({
  STROKE: "stroke",
  NOOP:   "noop",
});

module.exports = { ROLE, ENTRY_TYPE };
