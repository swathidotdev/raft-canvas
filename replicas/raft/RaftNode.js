/**
 * RaftNode — the consensus core.
 *
 * This file OWNS the invariants. All the I/O it does is via injected
 * `persistence` and `transport` objects, so unit tests (stage 4) can
 * swap those for fakes and drive election/replication scenarios in
 * milliseconds without opening a socket.
 *
 * KEY CORRECTNESS PROPERTIES ENFORCED HERE (vs. the original code):
 *
 *   1. Persistence-before-ack.
 *      Any state change that can influence future elections
 *      (currentTerm, votedFor, log entries) is written to disk in a
 *      single SQLite transaction BEFORE we respond to the RPC that
 *      caused it. Without this, a reboot-mid-vote can violate
 *      election safety (two leaders in one term).
 *
 *   2. Current-term commit rule (paper §5.4.2).
 *      The leader only counts replication toward `commitIndex` for
 *      entries of the current term. Older entries piggyback: once
 *      the leader gets its own current-term entry committed, every
 *      entry before it is transitively committed too. The mechanism
 *      that makes this work in practice is …
 *
 *   3. No-op-on-election.
 *      The instant a candidate wins, it appends a NOOP entry in its
 *      new term and replicates it. Committing that NOOP is what
 *      "unlocks" prior-term entries. Without this, a newly-elected
 *      leader can sit forever unable to commit anything if no new
 *      client writes arrive.
 *
 *   4. Per-peer nextIndex / matchIndex.
 *      Instead of the ad-hoc "if the follower says logLength=N, dump
 *      slice(N) at it" recovery, we track exactly where each follower
 *      is and back off one entry at a time on rejection. Standard
 *      Raft, and it composes cleanly with snapshots (stage 2).
 *
 *   5. Safe commitIndex clamping.
 *      Followers clamp leaderCommit against their own lastIndex(),
 *      and never advance commitIndex past a gap.
 */

const EventEmitter = require("events");

const { ROLE, ENTRY_TYPE } = require("./types");

const DEFAULT_CONFIG = {
  heartbeatMs:      150,
  electionMinMs:    500,
  electionMaxMs:    800,
  rpcTimeoutMs:     300,
  readIndexTimeoutMs: 500,
};

class RaftNode extends EventEmitter {
  constructor({ replicaId, peers, persistence, transport, stateMachine, logger, config }) {
    super();
    this.replicaId    = replicaId;
    this.peers        = peers.slice();  // array of peer URLs
    this.persistence  = persistence;
    this.transport    = transport;
    this.stateMachine = stateMachine;
    this.logger       = logger;
    this.config       = { ...DEFAULT_CONFIG, ...(config || {}) };

    // Volatile state (recomputed each start).
    this.role        = ROLE.FOLLOWER;
    this.leaderId    = null;
    this.commitIndex = -1;

    // Persistent state (loaded in start()).
    this.currentTerm = 0;
    this.votedFor    = null;

    // Leader-only volatile state (per-peer). Initialised on election win.
    this.nextIndex  = new Map();  // peerUrl → next log index to send
    this.matchIndex = new Map();  // peerUrl → highest log index known to be replicated

    // Timers.
    this._electionTimer  = null;
    this._heartbeatTimer = null;

    // Bookkeeping the dashboard will read (stage 3).
    this.restartCount = 0;
    this.startedAt    = Date.now();
  }

  // ─── lifecycle ────────────────────────────────────────────────────
  async start() {
    // 1. Load persisted term/vote — this is what saves us from
    //    "restarted node votes twice in the same term."
    const meta = this.persistence.loadMeta();
    this.currentTerm = meta.currentTerm;
    this.votedFor    = meta.votedFor;

    // 2. Restore snapshot if present, then replay any tail log entries
    //    into the state machine so it matches what commitIndex should be
    //    (we don't persist commitIndex — we reconstruct it from log +
    //    snapshot boundary, and it may briefly lag until the next
    //    heartbeat from a live leader).
    const snap = this.persistence.loadLatestSnapshot();
    if (snap) {
      this.stateMachine.restoreSnapshot(snap.state, snap.lastIncludedIndex);
      this.commitIndex = snap.lastIncludedIndex;
      this.logger.info("Restored snapshot", {
        lastIncludedIndex: snap.lastIncludedIndex,
        lastIncludedTerm:  snap.lastIncludedTerm,
      });
    }

    // We can't safely advance commitIndex on our own at boot — only a
    // leader tells us what's committed. But entries we already applied
    // pre-crash are still applied (via snapshot); the log tail beyond
    // that will get committed once we hear from a leader.

    this.logger.info("Recovered from disk", {
      term:      this.currentTerm,
      votedFor:  this.votedFor,
      lastIndex: this.persistence.lastIndex(),
      commitIdx: this.commitIndex,
    });

    this.restartCount += 1;
    this._becomeFollower(this.currentTerm);
  }

  stop() {
    this._clearElectionTimer();
    this._clearHeartbeatTimer();
  }

  // ─── role transitions ─────────────────────────────────────────────
  _becomeFollower(term, leaderId = null) {
    const termChanged = term !== this.currentTerm || this.votedFor !== null && term > this.currentTerm;
    this._clearHeartbeatTimer();
    this.role = ROLE.FOLLOWER;

    if (term > this.currentTerm) {
      // New term ⇒ clear vote AND persist BOTH before doing anything else.
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
    // Persist term+vote atomically BEFORE sending any RequestVotes.
    this.persistence.saveTermAndVote(this.currentTerm, this.votedFor);
    this.leaderId = null;
    this.logger.info("→ CANDIDATE", `requesting votes from ${this.peers.length} peers`);
    this.emit("role", this.role);
  }

  _becomeLeader() {
    this._clearElectionTimer();
    this.role = ROLE.LEADER;
    this.leaderId = this.replicaId;

    // Initialise per-peer replication trackers.
    const last = this.persistence.lastIndex();
    this.nextIndex.clear();
    this.matchIndex.clear();
    for (const peer of this.peers) {
      this.nextIndex.set(peer, last + 1);
      this.matchIndex.set(peer, -1);
    }

    this.logger.info("→ LEADER 🏆", `won election, lastIndex=${last}`);
    this.emit("role", this.role);

    // Fire the no-op immediately. This does two things at once:
    //  (a) counts as our first current-term entry so the commit rule
    //      lets us commit any prior-term entries left in the log;
    //  (b) doubles as the initial heartbeat, since it goes out on the
    //      normal AppendEntries path.
    this._appendLocal({
      type: ENTRY_TYPE.NOOP,
      payload: null,
    });

    // Start the heartbeat loop.
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
    this._resetElectionTimer();  // election also has a timeout

    const lastLogIndex = this.persistence.lastIndex();
    const lastLogTerm  = this.persistence.lastTerm();
    const termAtStart  = this.currentTerm;

    let votes = 1;
    const needed = Math.floor((this.peers.length + 1) / 2) + 1;

    const results = await this.transport.broadcast(
      this.peers,
      "/request-vote",
      { term: termAtStart, candidateId: this.replicaId, lastLogIndex, lastLogTerm },
      this.config.rpcTimeoutMs,
    );

    // If we've moved on (stepped down, higher term seen, new election),
    // ignore stale votes.
    if (this.role !== ROLE.CANDIDATE || this.currentTerm !== termAtStart) return;

    for (const r of results) {
      if (!r.ok) continue;
      const body = r.response.data;
      if (body.term > this.currentTerm) {
        this._becomeFollower(body.term);
        return;
      }
      if (body.voteGranted) votes++;
    }

    if (votes >= needed) {
      this._becomeLeader();
    } else {
      this.logger.info("Lost election", `votes=${votes}/${this.peers.length + 1} needed=${needed}`);
      // Stay candidate until next election timeout retries. (Original code
      // stepped down to follower here, which is also valid — we just don't
      // need to bump the term again immediately.)
    }
  }

  // ─── replication (leader side) ────────────────────────────────────
  /** Append an entry locally as leader, at index = lastIndex()+1. */
  _appendLocal({ type, payload }) {
    const index = this.persistence.lastIndex() + 1;
    const entry = { index, term: this.currentTerm, type, payload };
    this.persistence.appendEntries([entry]);
    // matchIndex for self = our own last index (implicitly, we track by
    // reading persistence.lastIndex() when computing commits).
    return entry;
  }

  /**
   * Public: client-facing write path. Leader only. Appends locally,
   * replicates, resolves when the entry is committed OR rejects on
   * failure/step-down.
   *
   * Returns { index, term } on commit.
   */
  async submit({ type = ENTRY_TYPE.STROKE, payload }) {
    if (this.role !== ROLE.LEADER) {
      const err = new Error("not leader");
      err.code = "NOT_LEADER";
      err.leaderId = this.leaderId;
      throw err;
    }
    const entry = this._appendLocal({ type, payload });
    // Kick a fan-out immediately rather than waiting for the next heartbeat.
    this._sendAppendEntriesToAll();
    // Wait until it commits (or we lose leadership).
    return this._awaitCommit(entry.index, entry.term);
  }

  _awaitCommit(index, termAtSubmit) {
    return new Promise((resolve, reject) => {
      const check = () => {
        if (this.commitIndex >= index) {
          resolve({ index, term: termAtSubmit });
          return true;
        }
        if (this.role !== ROLE.LEADER || this.currentTerm !== termAtSubmit) {
          const err = new Error("lost leadership before commit");
          err.code = "LOST_LEADERSHIP";
          reject(err);
          return true;
        }
        return false;
      };
      if (check()) return;
      // We could poll, but eventing off commitIndex changes is cleaner.
      const onCommit = () => { if (check()) this.off("commit", onCommit); };
      this.on("commit", onCommit);
      // Hard ceiling so a wedged cluster doesn't leak promises.
      setTimeout(() => {
        this.off("commit", onCommit);
        if (this.commitIndex < index) {
          const err = new Error("commit timeout");
          err.code = "COMMIT_TIMEOUT";
          reject(err);
        }
      }, 5000);
    });
  }

  /** Fire an AppendEntries RPC to every peer, one message per peer. */
  _sendAppendEntriesToAll() {
    if (this.role !== ROLE.LEADER) return;
    for (const peer of this.peers) this._sendAppendEntriesTo(peer);
  }

  async _sendAppendEntriesTo(peer) {
    if (this.role !== ROLE.LEADER) return;

    const next = this.nextIndex.get(peer) ?? 0;
    const prevLogIndex = next - 1;
    const prevLogTerm  = prevLogIndex >= 0
      ? (this.persistence.getEntry(prevLogIndex)?.term ?? -1)
      : -1;

    // Send everything from `next` up to our last index. Heartbeats are
    // AppendEntries with an empty `entries` array.
    const lastIdx = this.persistence.lastIndex();
    const entries = next <= lastIdx
      ? this.persistence.getRange(next, lastIdx)
      : [];

    const req = {
      term:         this.currentTerm,
      leaderId:     this.replicaId,
      prevLogIndex,
      prevLogTerm,
      entries,
      leaderCommit: this.commitIndex,
    };

    let res;
    try {
      res = await this.transport.call(peer, "/append-entries", req, this.config.rpcTimeoutMs);
    } catch {
      return; // peer unreachable; try again next heartbeat
    }

    // Response processing MUST re-check state — we might have stepped
    // down while the RPC was in flight.
    if (this.role !== ROLE.LEADER) return;
    const body = res.data;
    if (body.term > this.currentTerm) {
      this._becomeFollower(body.term);
      return;
    }
    if (body.success) {
      // Advance trackers to the entry after the batch we just sent.
      const newMatch = entries.length > 0 ? entries[entries.length - 1].index : prevLogIndex;
      this.matchIndex.set(peer, newMatch);
      this.nextIndex.set(peer, newMatch + 1);
      this._maybeAdvanceCommitIndex();
    } else {
      // Consistency check failed — back off one entry and retry next round.
      // (A production impl uses ConflictIndex/ConflictTerm hints to skip
      // whole terms at a time; simple decrement is correct, just slower.)
      const backoff = Math.max(0, next - 1);
      this.nextIndex.set(peer, backoff);
      this.logger.debug("AppendEntries rejected — backing off", { peer, next: backoff });
    }
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
    // We already hold every index locally (self counts as matched), so
    // walk downward from lastIdx and pick the first N that qualifies.
    for (let n = lastIdx; n > this.commitIndex; n--) {
      const e = this.persistence.getEntry(n);
      if (!e || e.term !== this.currentTerm) continue;   // §5.4.2 gate
      let count = 1; // self
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

  /** Apply any newly-committed entries to the state machine. */
  _applyCommitted() {
    while (this.stateMachine.lastApplied < this.commitIndex) {
      const next = this.stateMachine.lastApplied + 1;
      const entry = this.persistence.getEntry(next);
      if (!entry) break;
      this.stateMachine.apply(entry);
      this.emit("applied", entry);
    }
  }

  // ─── RPC handlers (called by replicaServer.js route handlers) ─────

  /**
   * Handle a RequestVote RPC. Returns the reply body synchronously.
   * All persistence happens BEFORE returning.
   */
  handleRequestVote({ term, candidateId, lastLogIndex, lastLogTerm }) {
    if (term > this.currentTerm) this._becomeFollower(term);

    if (term < this.currentTerm) {
      return { term: this.currentTerm, voteGranted: false };
    }
    if (this.votedFor !== null && this.votedFor !== candidateId) {
      return { term: this.currentTerm, voteGranted: false };
    }
    // Candidate log must be at least as up-to-date as ours.
    const myLastIdx  = this.persistence.lastIndex();
    const myLastTerm = this.persistence.lastTerm();
    const upToDate =
      lastLogTerm > myLastTerm ||
      (lastLogTerm === myLastTerm && lastLogIndex >= myLastIdx);
    if (!upToDate) {
      return { term: this.currentTerm, voteGranted: false };
    }

    this.votedFor = candidateId;
    this.persistence.saveTermAndVote(this.currentTerm, this.votedFor);  // persist BEFORE ack
    this._resetElectionTimer();
    this.logger.info("Voted for", candidateId);
    return { term: this.currentTerm, voteGranted: true };
  }

  /**
   * Handle an AppendEntries RPC (also used for heartbeats — empty entries).
   */
  handleAppendEntries({ term, leaderId, prevLogIndex, prevLogTerm, entries, leaderCommit }) {
    if (term < this.currentTerm) {
      return { term: this.currentTerm, success: false };
    }
    if (term > this.currentTerm || this.role !== ROLE.FOLLOWER) {
      this._becomeFollower(term, leaderId);
    } else {
      this.leaderId = leaderId;
      this._resetElectionTimer();
    }

    // Log-matching check.
    if (prevLogIndex >= 0) {
      const prev = this.persistence.getEntry(prevLogIndex);
      if (!prev || prev.term !== prevLogTerm) {
        return {
          term:      this.currentTerm,
          success:   false,
          // Hint so the leader can back off in bigger steps later (stage 4-ish).
          logLength: this.persistence.lastIndex() + 1,
        };
      }
    }

    // Append / overwrite. Walk in order; on the first conflicting entry,
    // truncate everything from that index and append the rest.
    if (entries && entries.length > 0) {
      let writeStart = -1;
      for (const e of entries) {
        const existing = this.persistence.getEntry(e.index);
        if (existing && existing.term === e.term) continue; // already have it
        writeStart = e.index;
        break;
      }
      if (writeStart >= 0) {
        this.persistence.truncateFrom(writeStart);
        const toAppend = entries.filter((e) => e.index >= writeStart);
        this.persistence.appendEntries(toAppend);           // persist BEFORE ack
      }
    }

    // Safe commit-index update: never advance past what we actually hold.
    if (leaderCommit > this.commitIndex) {
      const myLast = this.persistence.lastIndex();
      this.commitIndex = Math.min(leaderCommit, myLast);
      this._applyCommitted();
    }

    return { term: this.currentTerm, success: true };
  }

  // ─── linearizable read (ReadIndex) ────────────────────────────────
  /**
   * Serves a linearizable read.
   *   1. Snapshot commitIndex as readIndex.
   *   2. Confirm we're still leader via a fresh heartbeat majority.
   *   3. Wait until lastApplied >= readIndex.
   *   4. Caller can now safely read from state machine.
   *
   * Only safe once a current-term entry has committed — i.e. after the
   * election NOOP commits. Callers get NOT_LEADER until then.
   */
  async readIndex() {
    if (this.role !== ROLE.LEADER) {
      const err = new Error("not leader");
      err.code = "NOT_LEADER";
      err.leaderId = this.leaderId;
      throw err;
    }

    // Have we committed anything in this term yet? If not, our commit
    // index may reflect a previous leader's entries and reading it is
    // not linearizable.
    const commitEntry = this.commitIndex >= 0
      ? this.persistence.getEntry(this.commitIndex)
      : null;
    if (!commitEntry || commitEntry.term !== this.currentTerm) {
      const err = new Error("leader has not yet committed a current-term entry");
      err.code = "LEADER_WARMING_UP";
      throw err;
    }

    const readIdx  = this.commitIndex;
    const termAtStart = this.currentTerm;

    // Step 2: fresh heartbeat round, must see majority still recognizing us.
    let acks = 1;
    const results = await this.transport.broadcast(
      this.peers,
      "/append-entries",
      {
        term: this.currentTerm,
        leaderId: this.replicaId,
        prevLogIndex: this.persistence.lastIndex(),
        prevLogTerm:  this.persistence.lastTerm(),
        entries: [],
        leaderCommit: this.commitIndex,
      },
      this.config.readIndexTimeoutMs,
    );
    for (const r of results) {
      if (!r.ok) continue;
      if (r.response.data.term > this.currentTerm) {
        this._becomeFollower(r.response.data.term);
        const err = new Error("stepped down during read");
        err.code = "LOST_LEADERSHIP";
        throw err;
      }
      if (r.response.data.success) acks++;
    }
    const majority = Math.floor((this.peers.length + 1) / 2) + 1;
    if (acks < majority) {
      const err = new Error("could not confirm leadership");
      err.code = "LEADERSHIP_UNCONFIRMED";
      throw err;
    }
    if (this.role !== ROLE.LEADER || this.currentTerm !== termAtStart) {
      const err = new Error("lost leadership mid-read");
      err.code = "LOST_LEADERSHIP";
      throw err;
    }

    // Step 3: wait for the state machine to catch up to readIdx.
    while (this.stateMachine.lastApplied < readIdx) {
      await new Promise((r) => setTimeout(r, 5));
    }
    return { readIndex: readIdx, term: termAtStart };
  }

  // ─── observability ────────────────────────────────────────────────
  getStatus() {
    return {
      replicaId:   this.replicaId,
      role:        this.role,
      term:        this.currentTerm,
      leaderId:    this.leaderId,
      votedFor:    this.votedFor,
      lastIndex:   this.persistence.lastIndex(),
      firstIndex:  this.persistence.firstIndex(),
      commitIndex: this.commitIndex,
      lastApplied: this.stateMachine.lastApplied,
      peers:       this.peers,
      matchIndex:  Object.fromEntries(this.matchIndex),
      nextIndex:   Object.fromEntries(this.nextIndex),
      restartCount: this.restartCount,
      uptimeMs:    Date.now() - this.startedAt,
    };
  }
}

module.exports = { RaftNode, ROLE, ENTRY_TYPE };
