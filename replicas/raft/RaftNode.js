/**
 * RaftNode — consensus core.
 *
 * Stage 1 correctness properties: persistence-before-ack, current-term
 * commit rule, no-op-on-election, per-peer nextIndex/matchIndex, safe
 * commitIndex clamping, ReadIndex for linearizable reads.
 *
 * Stage 2 additions: submit() fast-path dedupe, _maybeSnapshot() +
 * log compaction, InstallSnapshot for followers below the compaction
 * horizon.
 *
 * Stage 4 hardening: _stopped flag checked after every await that
 * precedes a persistence access, so in-flight async ops abort cleanly
 * when the node is shut down (SIGTERM, test teardown, etc.) instead
 * of crashing on a closed SQLite handle.
 */

const EventEmitter = require("events");

const { ROLE, ENTRY_TYPE } = require("./types");

const DEFAULT_CONFIG = {
  heartbeatMs:        150,
  electionMinMs:      500,
  electionMaxMs:      800,
  rpcTimeoutMs:       300,
  readIndexTimeoutMs: 500,
  snapshotThreshold:  100,   // take snapshot after this many applied entries
  installSnapshotTimeoutMs: 2000,
};

class RaftNode extends EventEmitter {
  constructor({ replicaId, peers, persistence, transport, stateMachine, logger, config }) {
    super();
    this.replicaId    = replicaId;
    this.peers        = peers.slice();
    this.persistence  = persistence;
    this.transport    = transport;
    this.stateMachine = stateMachine;
    this.logger       = logger;
    this.config       = { ...DEFAULT_CONFIG, ...(config || {}) };

    this.role        = ROLE.FOLLOWER;
    this.leaderId    = null;
    this.commitIndex = -1;

    this.currentTerm = 0;
    this.votedFor    = null;

    this.nextIndex  = new Map();
    this.matchIndex = new Map();

    this._electionTimer  = null;
    this._heartbeatTimer = null;
    this._stopped        = false;

    this.restartCount = 0;
    this.startedAt    = Date.now();
    this.snapshotCount = 0;
  }

  // ─── lifecycle ────────────────────────────────────────────────────
  async start() {
    const meta = this.persistence.loadMeta();
    this.currentTerm = meta.currentTerm;
    this.votedFor    = meta.votedFor;

    const snap = this.persistence.loadLatestSnapshot();
    if (snap) {
      this.stateMachine.restoreSnapshot(snap.state, snap.lastIncludedIndex);
      this.commitIndex = snap.lastIncludedIndex;
      this.logger.info("Restored snapshot", {
        lastIncludedIndex: snap.lastIncludedIndex,
        lastIncludedTerm:  snap.lastIncludedTerm,
      });
    }

    this.logger.info("Recovered from disk", {
      term:       this.currentTerm,
      votedFor:   this.votedFor,
      firstIndex: this.persistence.firstIndex(),
      lastIndex:  this.persistence.lastIndex(),
      commitIdx:  this.commitIndex,
    });

    this.restartCount += 1;
    this._becomeFollower(this.currentTerm);
  }

  stop() {
    this._stopped = true;
    this._clearElectionTimer();
    this._clearHeartbeatTimer();
  }

  // ─── role transitions ─────────────────────────────────────────────
  _becomeFollower(term, leaderId = null) {
    this._clearHeartbeatTimer();
    this.role = ROLE.FOLLOWER;

    if (term > this.currentTerm) {
      this.currentTerm = term;
      this.votedFor    = null;
      this.persistence.saveTermAndVote(this.currentTerm, this.votedFor);
    }
    if (leaderId) this.leaderId = leaderId;

    this.logger.info("→ FOLLOWER", leaderId ? `leader=${leaderId}` : "");
    this._resetElectionTimer();
    this.emit("role", this.role);
  }

  _becomeCandidate() {
    this.role = ROLE.CANDIDATE;
    this.currentTerm += 1;
    this.votedFor = this.replicaId;
    this.persistence.saveTermAndVote(this.currentTerm, this.votedFor);
    this.leaderId = null;
    this.logger.info("→ CANDIDATE", `requesting votes from ${this.peers.length} peers`);
    this.emit("role", this.role);
  }

  _becomeLeader() {
    this._clearElectionTimer();
    this.role = ROLE.LEADER;
    this.leaderId = this.replicaId;

    const last = this.persistence.lastIndex();
    this.nextIndex.clear();
    this.matchIndex.clear();
    for (const peer of this.peers) {
      this.nextIndex.set(peer, last + 1);
      this.matchIndex.set(peer, -1);
    }

    this.logger.info("→ LEADER 🏆", `won election, lastIndex=${last}`);
    this.emit("role", this.role);

    // NOOP for §5.4.2 + doubles as first heartbeat.
    this._appendLocal({ type: ENTRY_TYPE.NOOP, payload: null });

    this._sendAppendEntriesToAll();
    this._heartbeatTimer = setInterval(
      () => this._sendAppendEntriesToAll(),
      this.config.heartbeatMs,
    );
  }

  // ─── timers ───────────────────────────────────────────────────────
  _clearElectionTimer() {
    if (this._electionTimer) { clearTimeout(this._electionTimer); this._electionTimer = null; }
  }
  _clearHeartbeatTimer() {
    if (this._heartbeatTimer) { clearInterval(this._heartbeatTimer); this._heartbeatTimer = null; }
  }
  _resetElectionTimer() {
    this._clearElectionTimer();
    const { electionMinMs, electionMaxMs } = this.config;
    const t = electionMinMs + Math.floor(Math.random() * (electionMaxMs - electionMinMs));
    this._electionTimer = setTimeout(() => this._startElection(), t);
  }

  // ─── election ─────────────────────────────────────────────────────
  async _startElection() {
    this._becomeCandidate();
    this._resetElectionTimer();

    const lastLogIndex = this.persistence.lastIndex();
    const lastLogTerm  = this.persistence.lastTerm();
    const termAtStart  = this.currentTerm;

    let votes = 1;
    const needed = Math.floor((this.peers.length + 1) / 2) + 1;

    const results = await this.transport.broadcast(
      this.peers, "/request-vote",
      { term: termAtStart, candidateId: this.replicaId, lastLogIndex, lastLogTerm },
      this.config.rpcTimeoutMs,
    );

    // Post-await guard: we may have stopped, stepped down, or moved
    // on to a newer election while the broadcast was in flight.
    if (this._stopped || this.role !== ROLE.CANDIDATE || this.currentTerm !== termAtStart) return;

    for (const r of results) {
      if (!r.ok) continue;
      const body = r.response.data;
      if (body.term > this.currentTerm) { this._becomeFollower(body.term); return; }
      if (body.voteGranted) votes++;
    }

    if (votes >= needed) this._becomeLeader();
    else this.logger.info("Lost election", `votes=${votes}/${this.peers.length + 1} needed=${needed}`);
  }

  // ─── replication (leader side) ────────────────────────────────────
  _appendLocal({ type, payload }) {
    const index = this.persistence.lastIndex() + 1;
    const entry = { index, term: this.currentTerm, type, payload };
    this.persistence.appendEntries([entry]);
    return entry;
  }

  /**
   * Client write path. Fast-paths retries via the state machine's dedupe
   * table; otherwise appends + replicates + awaits commit.
   *
   * Return shape: { index, term, deduped? }
   */
  async submit({ type = ENTRY_TYPE.STROKE, payload }) {
    if (this.role !== ROLE.LEADER) {
      const err = new Error("not leader");
      err.code = "NOT_LEADER"; err.leaderId = this.leaderId;
      throw err;
    }

    const cid = payload?.clientId;
    const seq = payload?.seq;

    // Fast path — if already applied, return the cached index with
    // zero Raft traffic. Correctness gate is at apply-time on every
    // replica; this is only an optimization.
    if (cid != null && seq != null) {
      const cached = this.stateMachine.getDedupeIndex(cid, seq);
      if (cached != null) {
        this.logger.debug("Dedupe fast-path hit", { clientId: cid, seq, index: cached });
        return { index: cached, term: this.currentTerm, deduped: true };
      }
    }

    const entry = this._appendLocal({ type, payload });
    this._sendAppendEntriesToAll();
    const result = await this._awaitCommit(entry.index, entry.term);

    // Post-commit: if a concurrent duplicate slipped in and won the
    // dedupe race at apply-time, report the authoritative index.
    if (cid != null && seq != null) {
      const authoritative = this.stateMachine.getDedupeIndex(cid, seq);
      if (authoritative != null && authoritative !== result.index) {
        return { index: authoritative, term: result.term, deduped: true };
      }
    }
    return result;
  }

  _awaitCommit(index, termAtSubmit) {
    return new Promise((resolve, reject) => {
      const check = () => {
        if (this.commitIndex >= index) { resolve({ index, term: termAtSubmit }); return true; }
        if (this.role !== ROLE.LEADER || this.currentTerm !== termAtSubmit) {
          const err = new Error("lost leadership before commit");
          err.code = "LOST_LEADERSHIP"; reject(err); return true;
        }
        return false;
      };
      if (check()) return;
      const onCommit = () => { if (check()) this.off("commit", onCommit); };
      this.on("commit", onCommit);
      setTimeout(() => {
        this.off("commit", onCommit);
        if (this.commitIndex < index) {
          const err = new Error("commit timeout"); err.code = "COMMIT_TIMEOUT"; reject(err);
        }
      }, 5000);
    });
  }

  _sendAppendEntriesToAll() {
    if (this.role !== ROLE.LEADER) return;
    for (const peer of this.peers) this._sendAppendEntriesTo(peer);
  }

  async _sendAppendEntriesTo(peer) {
    // Pre-await guard: stopped nodes stay silent.
    if (this._stopped || this.role !== ROLE.LEADER) return;
    const next = this.nextIndex.get(peer) ?? 0;

    // Follower is behind our compaction horizon — send a snapshot instead.
    if (next < this.persistence.firstIndex()) {
      return this._sendInstallSnapshotTo(peer);
    }

    const prevLogIndex = next - 1;
    let prevLogTerm = -1;
    if (prevLogIndex >= 0) {
      const meta = this.persistence.loadMeta();
      if (prevLogIndex === meta.lastSnapshotIndex) {
        // Boundary case: prev entry lives in the snapshot metadata, not in the log.
        prevLogTerm = meta.lastSnapshotTerm;
      } else {
        prevLogTerm = this.persistence.getEntry(prevLogIndex)?.term ?? -1;
      }
    }

    const lastIdx = this.persistence.lastIndex();
    const entries = next <= lastIdx ? this.persistence.getRange(next, lastIdx) : [];

    const req = {
      term:         this.currentTerm,
      leaderId:     this.replicaId,
      prevLogIndex, prevLogTerm, entries,
      leaderCommit: this.commitIndex,
    };

    let res;
    try { res = await this.transport.call(peer, "/append-entries", req, this.config.rpcTimeoutMs); }
    catch { return; }

    // Post-await guard: node may have been stopped or stepped down
    // while the RPC was in flight — don't touch persistence if so.
    if (this._stopped || this.role !== ROLE.LEADER) return;
    const body = res.data;
    if (body.term > this.currentTerm) { this._becomeFollower(body.term); return; }

    if (body.success) {
      const newMatch = entries.length > 0 ? entries[entries.length - 1].index : prevLogIndex;
      this.matchIndex.set(peer, newMatch);
      this.nextIndex.set(peer, newMatch + 1);
      this._maybeAdvanceCommitIndex();
    } else {
      const backoff = Math.max(0, next - 1);
      this.nextIndex.set(peer, backoff);
      this.logger.debug("AppendEntries rejected — backing off", { peer, next: backoff });
    }
  }

  async _sendInstallSnapshotTo(peer) {
    const snap = this.persistence.loadLatestSnapshot();
    if (!snap) return;
    const req = {
      term:              this.currentTerm,
      leaderId:          this.replicaId,
      lastIncludedIndex: snap.lastIncludedIndex,
      lastIncludedTerm:  snap.lastIncludedTerm,
      data:              snap.state,
    };
    let res;
    try {
      res = await this.transport.call(peer, "/install-snapshot", req, this.config.installSnapshotTimeoutMs);
    } catch { return; }
    if (this._stopped || this.role !== ROLE.LEADER) return;
    const body = res.data;
    if (body.term > this.currentTerm) { this._becomeFollower(body.term); return; }
    this.matchIndex.set(peer, snap.lastIncludedIndex);
    this.nextIndex.set(peer, snap.lastIncludedIndex + 1);
    this.logger.info("Sent InstallSnapshot", { peer, lastIncludedIndex: snap.lastIncludedIndex });
  }

  /**
   * Leader commit rule (paper §5.4.2):
   *   commitIndex := max N such that
   *     N > commitIndex,
   *     log[N].term == currentTerm,
   *     AND a majority of matchIndex[i] >= N (counting self).
   *
   * The current-term restriction is the non-obvious part. Without it,
   * a leader can commit an old entry that a later leader will overwrite
   * — the classic "Figure 8" scenario. NOOP-on-election is what makes
   * this rule not permanently stall.
   */
  _maybeAdvanceCommitIndex() {
    if (this.role !== ROLE.LEADER) return;
    const lastIdx = this.persistence.lastIndex();
    for (let n = lastIdx; n > this.commitIndex; n--) {
      const e = this.persistence.getEntry(n);
      if (!e || e.term !== this.currentTerm) continue;
      let count = 1;
      for (const peer of this.peers) {
        if ((this.matchIndex.get(peer) ?? -1) >= n) count++;
      }
      const majority = Math.floor((this.peers.length + 1) / 2) + 1;
      if (count >= majority) {
        this.commitIndex = n;
        this.logger.info("Advanced commitIndex", { commitIndex: n });
        this._applyCommitted();
        this.emit("commit", n);
        return;
      }
    }
  }

  _applyCommitted() {
    while (this.stateMachine.lastApplied < this.commitIndex) {
      const next = this.stateMachine.lastApplied + 1;
      const entry = this.persistence.getEntry(next);
      if (!entry) break;
      this.stateMachine.apply(entry);
      this.emit("applied", entry);
    }
    this._maybeSnapshot();
  }

  /**
   * Compact if applied-entries-since-last-snapshot exceeds threshold.
   * Runs on every replica independently — no coordination needed since
   * snapshot content is a pure function of applied log entries.
   */
  _maybeSnapshot() {
    const meta = this.persistence.loadMeta();
    const sinceSnap = this.stateMachine.lastApplied - meta.lastSnapshotIndex;
    if (sinceSnap < this.config.snapshotThreshold) return;
    if (this.stateMachine.lastApplied < 0) return;

    const appliedIdx   = this.stateMachine.lastApplied;
    const appliedEntry = this.persistence.getEntry(appliedIdx);
    if (!appliedEntry) return;

    const blob = this.stateMachine.getSnapshot();
    this.persistence.saveSnapshot(appliedIdx, appliedEntry.term, blob);
    this.persistence.truncateUpTo(appliedIdx);
    this.snapshotCount += 1;
    this.logger.info("Snapshot taken + log compacted", {
      lastIncludedIndex: appliedIdx,
      lastIncludedTerm:  appliedEntry.term,
      strokes:           blob.strokes.length,
      dedupeSize:        blob.dedupe.length,
    });
  }

  // ─── RPC handlers ─────────────────────────────────────────────────
  handleRequestVote({ term, candidateId, lastLogIndex, lastLogTerm }) {
    if (term > this.currentTerm) this._becomeFollower(term);
    if (term < this.currentTerm) return { term: this.currentTerm, voteGranted: false };
    if (this.votedFor !== null && this.votedFor !== candidateId) {
      return { term: this.currentTerm, voteGranted: false };
    }
    const myLastIdx  = this.persistence.lastIndex();
    const myLastTerm = this.persistence.lastTerm();
    const upToDate =
      lastLogTerm > myLastTerm ||
      (lastLogTerm === myLastTerm && lastLogIndex >= myLastIdx);
    if (!upToDate) return { term: this.currentTerm, voteGranted: false };

    this.votedFor = candidateId;
    this.persistence.saveTermAndVote(this.currentTerm, this.votedFor);
    this._resetElectionTimer();
    this.logger.info("Voted for", candidateId);
    return { term: this.currentTerm, voteGranted: true };
  }

  handleAppendEntries({ term, leaderId, prevLogIndex, prevLogTerm, entries, leaderCommit }) {
    if (term < this.currentTerm) return { term: this.currentTerm, success: false };
    if (term > this.currentTerm || this.role !== ROLE.FOLLOWER) {
      this._becomeFollower(term, leaderId);
    } else {
      this.leaderId = leaderId;
      this._resetElectionTimer();
    }

    if (prevLogIndex >= 0) {
      const meta = this.persistence.loadMeta();
      let matched;
      if (prevLogIndex === meta.lastSnapshotIndex) {
        matched = prevLogTerm === meta.lastSnapshotTerm;
      } else if (prevLogIndex < meta.lastSnapshotIndex) {
        // Prev entry is inside a snapshot we've already installed;
        // trust it (we couldn't have installed the snapshot without
        // the leader's authority).
        matched = true;
      } else {
        const prev = this.persistence.getEntry(prevLogIndex);
        matched = prev && prev.term === prevLogTerm;
      }
      if (!matched) {
        return {
          term:      this.currentTerm,
          success:   false,
          logLength: this.persistence.lastIndex() + 1,
        };
      }
    }

    if (entries && entries.length > 0) {
      let writeStart = -1;
      for (const e of entries) {
        const existing = this.persistence.getEntry(e.index);
        if (existing && existing.term === e.term) continue;
        writeStart = e.index; break;
      }
      if (writeStart >= 0) {
        this.persistence.truncateFrom(writeStart);
        const toAppend = entries.filter((e) => e.index >= writeStart);
        this.persistence.appendEntries(toAppend);
      }
    }

    if (leaderCommit > this.commitIndex) {
      const myLast = this.persistence.lastIndex();
      this.commitIndex = Math.min(leaderCommit, myLast);
      this._applyCommitted();
    }

    return { term: this.currentTerm, success: true };
  }

  /**
   * InstallSnapshot handler. Follower-side of log compaction.
   * Never accepts a snapshot older than what we already have.
   */
  handleInstallSnapshot({ term, leaderId, lastIncludedIndex, lastIncludedTerm, data }) {
    if (term < this.currentTerm) return { term: this.currentTerm };
    if (term > this.currentTerm || this.role !== ROLE.FOLLOWER) {
      this._becomeFollower(term, leaderId);
    } else {
      this.leaderId = leaderId;
      this._resetElectionTimer();
    }

    const meta = this.persistence.loadMeta();
    if (lastIncludedIndex <= meta.lastSnapshotIndex) {
      // We already have this snapshot or newer — no-op.
      return { term: this.currentTerm };
    }

    // If our log contains lastIncludedIndex with matching term, KEEP the
    // entries after it (they're already correct). Otherwise wipe the log.
    const existing = this.persistence.getEntry(lastIncludedIndex);
    if (existing && existing.term === lastIncludedTerm) {
      this.persistence.truncateUpTo(lastIncludedIndex);
    } else {
      this.persistence.truncateAll();
    }

    this.persistence.saveSnapshot(lastIncludedIndex, lastIncludedTerm, data);
    this.stateMachine.restoreSnapshot(data, lastIncludedIndex);
    this.commitIndex = Math.max(this.commitIndex, lastIncludedIndex);
    this.logger.info("Installed snapshot", { lastIncludedIndex, lastIncludedTerm });
    return { term: this.currentTerm };
  }

  // ─── ReadIndex ────────────────────────────────────────────────────
  async readIndex() {
    if (this.role !== ROLE.LEADER) {
      const err = new Error("not leader");
      err.code = "NOT_LEADER"; err.leaderId = this.leaderId; throw err;
    }
    const commitEntry = this.commitIndex >= 0 ? this.persistence.getEntry(this.commitIndex) : null;
    const commitInSnapshot =
      this.commitIndex >= 0 && this.commitIndex <= this.persistence.loadMeta().lastSnapshotIndex;
    if (!commitInSnapshot && (!commitEntry || commitEntry.term !== this.currentTerm)) {
      const err = new Error("leader has not yet committed a current-term entry");
      err.code = "LEADER_WARMING_UP"; throw err;
    }

    const readIdx = this.commitIndex;
    const termAtStart = this.currentTerm;
    let acks = 1;
    const results = await this.transport.broadcast(
      this.peers, "/append-entries",
      {
        term: this.currentTerm, leaderId: this.replicaId,
        prevLogIndex: this.persistence.lastIndex(),
        prevLogTerm:  this.persistence.lastTerm(),
        entries: [], leaderCommit: this.commitIndex,
      },
      this.config.readIndexTimeoutMs,
    );
    for (const r of results) {
      if (!r.ok) continue;
      if (r.response.data.term > this.currentTerm) {
        this._becomeFollower(r.response.data.term);
        const err = new Error("stepped down during read");
        err.code = "LOST_LEADERSHIP"; throw err;
      }
      if (r.response.data.success) acks++;
    }
    const majority = Math.floor((this.peers.length + 1) / 2) + 1;
    if (acks < majority) {
      const err = new Error("could not confirm leadership");
      err.code = "LEADERSHIP_UNCONFIRMED"; throw err;
    }
    if (this.role !== ROLE.LEADER || this.currentTerm !== termAtStart) {
      const err = new Error("lost leadership mid-read");
      err.code = "LOST_LEADERSHIP"; throw err;
    }
    while (this.stateMachine.lastApplied < readIdx) {
      await new Promise((r) => setTimeout(r, 5));
    }
    return { readIndex: readIdx, term: termAtStart };
  }

  getStatus() {
    const meta = this.persistence.loadMeta();
    return {
      replicaId:    this.replicaId,
      role:         this.role,
      term:         this.currentTerm,
      leaderId:     this.leaderId,
      votedFor:     this.votedFor,
      firstIndex:   this.persistence.firstIndex(),
      lastIndex:    this.persistence.lastIndex(),
      commitIndex:  this.commitIndex,
      lastApplied:  this.stateMachine.lastApplied,
      peers:        this.peers,
      matchIndex:   Object.fromEntries(this.matchIndex),
      nextIndex:    Object.fromEntries(this.nextIndex),
      restartCount: this.restartCount,
      uptimeMs:     Date.now() - this.startedAt,
      snapshotCount: this.snapshotCount,
      lastSnapshotIndex: meta.lastSnapshotIndex,
      lastSnapshotTerm:  meta.lastSnapshotTerm,
      dedupeSize:   this.stateMachine._dedupe.size,
    };
  }
}

module.exports = { RaftNode, ROLE, ENTRY_TYPE };