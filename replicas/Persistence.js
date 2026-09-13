/**
 * Persistence — SQLite-backed durable Raft state.
 *
 * WHAT MUST BE ON DISK BEFORE WE ACK A PEER (Raft §5, "Persistent state"):
 *   • currentTerm  — the latest term this node has seen
 *   • votedFor     — candidateId that received our vote in currentTerm (or NULL)
 *   • log[]        — every entry we've accepted (index, term, type, payload)
 *
 * WAL mode + synchronous=FULL gives us the fsync semantics Raft requires;
 * a "committed" SQLite transaction survives power loss.
 */

const Database = require("better-sqlite3");
const path     = require("path");
const fs       = require("fs");

const { ENTRY_TYPE } = require("./types");

const SCHEMA = `
CREATE TABLE IF NOT EXISTS raft_meta (
  id                   INTEGER PRIMARY KEY CHECK (id = 1),
  current_term         INTEGER NOT NULL DEFAULT 0,
  voted_for            TEXT,
  last_snapshot_index  INTEGER NOT NULL DEFAULT -1,
  last_snapshot_term   INTEGER NOT NULL DEFAULT -1
);

CREATE TABLE IF NOT EXISTS log_entries (
  idx        INTEGER PRIMARY KEY,
  term       INTEGER NOT NULL,
  entry_type TEXT    NOT NULL,
  payload    TEXT
);

CREATE TABLE IF NOT EXISTS snapshots (
  id                   INTEGER PRIMARY KEY AUTOINCREMENT,
  last_included_index  INTEGER NOT NULL,
  last_included_term   INTEGER NOT NULL,
  state_blob           TEXT    NOT NULL,
  created_at           INTEGER NOT NULL
);
`;

class Persistence {
  constructor({ dataDir, replicaId, logger }) {
    this.logger = logger;
    if (!fs.existsSync(dataDir)) fs.mkdirSync(dataDir, { recursive: true });
    const dbPath = path.join(dataDir, `${replicaId}.db`);

    this.db = new Database(dbPath);
    this.db.pragma("journal_mode = WAL");
    this.db.pragma("synchronous = FULL");
    this.db.exec(SCHEMA);

    this.db.prepare(`
      INSERT OR IGNORE INTO raft_meta (id, current_term, voted_for)
      VALUES (1, 0, NULL)
    `).run();

    this._stmts = {
      getMeta:        this.db.prepare(`SELECT * FROM raft_meta WHERE id = 1`),
      setTermAndVote: this.db.prepare(`UPDATE raft_meta SET current_term = ?, voted_for = ? WHERE id = 1`),
      setSnapMeta:    this.db.prepare(`UPDATE raft_meta SET last_snapshot_index = ?, last_snapshot_term = ? WHERE id = 1`),

      insertEntry:    this.db.prepare(`INSERT INTO log_entries (idx, term, entry_type, payload) VALUES (?, ?, ?, ?)`),
      replaceEntry:   this.db.prepare(`INSERT OR REPLACE INTO log_entries (idx, term, entry_type, payload) VALUES (?, ?, ?, ?)`),
      deleteFrom:     this.db.prepare(`DELETE FROM log_entries WHERE idx >= ?`),
      deleteUpTo:     this.db.prepare(`DELETE FROM log_entries WHERE idx <= ?`),
      deleteAll:      this.db.prepare(`DELETE FROM log_entries`),
      getEntry:       this.db.prepare(`SELECT idx, term, entry_type, payload FROM log_entries WHERE idx = ?`),
      getRange:       this.db.prepare(`SELECT idx, term, entry_type, payload FROM log_entries WHERE idx >= ? AND idx <= ? ORDER BY idx ASC`),
      getLast:        this.db.prepare(`SELECT idx, term FROM log_entries ORDER BY idx DESC LIMIT 1`),
      getFirst:       this.db.prepare(`SELECT idx, term FROM log_entries ORDER BY idx ASC LIMIT 1`),
      countEntries:   this.db.prepare(`SELECT COUNT(*) AS n FROM log_entries`),

      insertSnapshot: this.db.prepare(`INSERT INTO snapshots (last_included_index, last_included_term, state_blob, created_at) VALUES (?, ?, ?, ?)`),
      getLastSnap:    this.db.prepare(`SELECT * FROM snapshots ORDER BY id DESC LIMIT 1`),
      deleteOldSnaps: this.db.prepare(`DELETE FROM snapshots WHERE id < ?`),
    };
  }

  loadMeta() {
    const row = this._stmts.getMeta.get();
    return {
      currentTerm:       row.current_term,
      votedFor:          row.voted_for,
      lastSnapshotIndex: row.last_snapshot_index,
      lastSnapshotTerm:  row.last_snapshot_term,
    };
  }

  saveTermAndVote(term, votedFor) {
    this._stmts.setTermAndVote.run(term, votedFor);
  }

  saveSnapshotMeta(index, term) {
    this._stmts.setSnapMeta.run(index, term);
  }

  lastIndex() {
    const row = this._stmts.getLast.get();
    if (row) return row.idx;
    // Empty log — could be truly empty, or fully compacted into a snapshot.
    const meta = this._stmts.getMeta.get();
    return meta.last_snapshot_index;   // -1 if never snapshotted
  }

  lastTerm() {
    const row = this._stmts.getLast.get();
    if (row) return row.term;
    const meta = this._stmts.getMeta.get();
    return meta.last_snapshot_term;
  }

  /** Lowest index still available in the log tail (after compaction). */
  firstIndex() {
    const row = this._stmts.getFirst.get();
    if (row) return row.idx;
    const meta = this._stmts.getMeta.get();
    // If nothing in log, next needed index would be one past the snapshot.
    return meta.last_snapshot_index + 1;
  }

  count() {
    return this._stmts.countEntries.get().n;
  }

  getEntry(index) {
    const row = this._stmts.getEntry.get(index);
    return row ? _row2entry(row) : null;
  }

  getRange(fromIdx, toIdx) {
    if (fromIdx > toIdx) return [];
    return this._stmts.getRange.all(fromIdx, toIdx).map(_row2entry);
  }

  appendEntries(entries) {
    if (!entries || entries.length === 0) return;
    const tx = this.db.transaction((batch) => {
      for (const e of batch) {
        const payload = e.payload === undefined || e.payload === null
          ? null
          : JSON.stringify(e.payload);
        this._stmts.replaceEntry.run(e.index, e.term, e.type, payload);
      }
    });
    tx(entries);
  }

  truncateFrom(fromIdx) {
    this._stmts.deleteFrom.run(fromIdx);
  }

  /** Compaction: delete all log entries at or below `toIdx`. */
  truncateUpTo(toIdx) {
    this._stmts.deleteUpTo.run(toIdx);
  }

  /** Nuke the entire log — used when installing a snapshot that conflicts. */
  truncateAll() {
    this._stmts.deleteAll.run();
  }

  saveSnapshot(lastIncludedIndex, lastIncludedTerm, stateBlob) {
    const tx = this.db.transaction(() => {
      const info = this._stmts.insertSnapshot.run(
        lastIncludedIndex,
        lastIncludedTerm,
        JSON.stringify(stateBlob),
        Date.now(),
      );
      this._stmts.setSnapMeta.run(lastIncludedIndex, lastIncludedTerm);
      // Keep only the newest snapshot to bound disk usage.
      this._stmts.deleteOldSnaps.run(info.lastInsertRowid);
    });
    tx();
  }

  loadLatestSnapshot() {
    const row = this._stmts.getLastSnap.get();
    if (!row) return null;
    return {
      lastIncludedIndex: row.last_included_index,
      lastIncludedTerm:  row.last_included_term,
      state:             JSON.parse(row.state_blob),
      createdAt:         row.created_at,
    };
  }

  close() { this.db.close(); }
}

function _row2entry(row) {
  return {
    index:   row.idx,
    term:    row.term,
    type:    row.entry_type,
    payload: row.payload === null ? null : JSON.parse(row.payload),
  };
}

module.exports = { Persistence, ENTRY_TYPE };