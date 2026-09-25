# Raft Canvas

**A collaborative drawing board built on a from-scratch Raft consensus implementation, with persistent logs, snapshotting, chaos engineering, and a live cluster dashboard.**

> **Live dashboard preview:** run the stack with the quickstart below, then open `http://localhost:3000/dashboard/`. A repository screenshot can be added at `docs/dashboard.png` when a captured image is available.

## Why This Project

This started as a basic Raft demo and was deliberately extended to explore production-grade distributed-systems concerns: durable recovery, compaction, idempotent retries, linearizable reads, and observable failure handling. The canvas is the workload; the project is about keeping replicated state correct while nodes fail, recover, and communicate over unreliable links.

## Distributed-Systems Features

- **Consensus and replication:** leader election, majority-committed log replication, ordered state-machine application, and linearizable reads through Raft `ReadIndex`.
- **Fault tolerance and recovery:** SQLite-backed write-ahead persistence for terms, votes, logs, and snapshots; crash recovery; log compaction; and follower catch-up through `InstallSnapshot`.
- **Idempotency:** client `(clientId, seq)` deduplication is replicated and survives lost responses and leader failover, preventing duplicate strokes on retry.
- **Chaos engineering:** per-link and global partition, latency, and message-drop injection through the admin API, with live role, fault, RPC, and recovery events in the dashboard.
- **Testing:** 30 Jest tests, including election safety, log matching, failover idempotency, snapshot recovery, partition behavior, ReadIndex, and a Jepsen-style randomized convergence checker.
- **Benchmarking:** measured HTTP throughput, commit latency percentiles, injected-fault behavior, and leader re-election time; the checked-in run is from Node 20 on Linux.

## Architecture

```text
Client (canvas.js) -> Gateway (WebSocket fanout) -> Raft Cluster (3 nodes)
                                                       |- replica1 (SQLite WAL)
                                                       |- replica2 (SQLite WAL)
                                                       `- replica3 (SQLite WAL)
```

The gateway discovers the current leader, forwards strokes, retries failed requests, and broadcasts committed strokes. Each replica runs the Raft state machine and exposes status, history, metrics, and fault-injection endpoints.

## Quickstart

Prerequisite: Docker Desktop with Docker Compose.

```bash
cd docker && docker compose up --build
```

Open:

- Dashboard: <http://localhost:3000/dashboard/>
- Canvas: <http://localhost:8080>

The canvas is served separately because it is a static client:

```bash
cd frontend
python -m http.server 8080
```

The Compose stack publishes the gateway on port `3000` and replicas on ports `4001`, `4002`, and `4003`. Stop it with `docker compose down`; use `docker compose down -v` when you intentionally want to erase persisted replica data.

## Benchmark Results

These values come from [the checked-in HTTP benchmark report](replicas/bench/results/2026-09-23T18-17-26-248Z-http.md), using four concurrent clients for throughput and serial submissions for latency.

| Scenario | Throughput | p50 latency | p99 latency |
|---|---:|---:|---:|
| Steady state | 98.3 strokes/s | 5.88 ms | 8.25 ms |
| +50 ms injected RPC latency | 86.1 strokes/s | 5.37 ms | 8.49 ms |
| 5% message drop | 132 strokes/s | 4.83 ms | 8.71 ms |

Leader re-election after a kill: **184 ms average**, **3.90 ms p50**, **906 ms p95** across five trials.

Run the HTTP benchmark against the running stack:

```bash
cd replicas
npm install
npm run bench:http
```

## Testing

The test suite covers:

- election safety and term monotonicity
- split-brain prevention and majority commit behavior
- log matching and ordered state-machine application
- exactly-once effects from client retries under leader failover
- SQLite term, vote, log, and snapshot persistence
- snapshot compaction and `InstallSnapshot` catch-up
- linearizable reads through `ReadIndex`
- convergence under randomized partitions, latency, concurrent writes, and recovery

Run it with:

```bash
cd replicas
npm install
npm test
```

The Jepsen-style convergence test is a five-node in-process stress scenario that retries concurrent writes during randomized chaos and verifies identical committed state on every live node.

## Limitations

- The single gateway remains a service and WebSocket fanout single point of failure.
- Membership is static: there is no joint consensus, dynamic reconfiguration, or automatic scale-out.
- There is no multi-Raft sharding, authentication, authorization, TLS, rate limiting, or durable gateway queue.
- The canvas workload is append-only; clearing the local canvas is not a replicated command.
- The deduplication index is bounded to 10,000 client IDs, and clients must preserve stable IDs and monotonically increasing sequence numbers.

## Tech Stack

Node.js, Express, `better-sqlite3`, vanilla JavaScript, Docker Compose, and Jest.
