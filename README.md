# Raft Canvas - Distributed Drawing Board on a From-Scratch Raft Cluster

A collaborative drawing board built on a from-scratch Raft consensus implementation — with persistent logs, snapshotting, chaos engineering, and a live cluster dashboard.

<!-- Live preview GIF/screenshot goes here -->

## Why this project

This started as a small Raft demo and was deliberately extended into a production-style distributed systems exercise. The goal was not to build a polished drawing app, but to stress the core invariants that actually matter in a replicated system: leader election, log safety, crash recovery, idempotent writes, and safe failover under chaos.

## What it demonstrates

- Consensus & replication: leader election, log replication, and linearizable reads via a ReadIndex-style read path.
- Fault tolerance & recovery: persistent SQLite WAL storage, crash recovery, snapshotting, and log compaction.
- Idempotency: client deduplication survives leader failover and retried writes without duplicating strokes.
- Chaos engineering: partition, latency, and drop injection through the admin API and live dashboard.
- Testing: a Jest suite covering election safety, split-brain prevention, failover semantics, convergence, and restart recovery.
- Benchmarking: measured throughput and latency under steady state and injected faults, plus re-election timing after a leader kill.

## Architecture

```text
Client (browser canvas)
    ↓
Gateway (WS fanout + leader routing)
    ↓
Raft cluster (3 nodes)
    ├─ replica1 (SQLite WAL + state machine)
    ├─ replica2 (SQLite WAL + state machine)
    └─ replica3 (SQLite WAL + state machine)
```

## Quickstart

Prerequisite: Docker Desktop must be running on your machine.

From the project root:

```bash
cd docker
docker compose up --build
```

Then open:

- Dashboard: http://localhost:3000/dashboard
- Gateway status: http://localhost:3000/status
- Replica statuses:
  - http://localhost:4001/status
  - http://localhost:4002/status
  - http://localhost:4003/status

To stop the cluster:

```bash
docker compose down
```

To reset persisted Raft data:

```bash
docker compose down -v
```

Run the frontend in a second terminal:

```bash
cd frontend
python -m http.server 8080
```

Then open http://localhost:8080.

## Benchmark results

Actual measurements from the project’s HTTP benchmark run in `replicas/bench/results/2026-09-23T18-17-26-248Z-http.md`.

| Scenario | Throughput | p50 latency | p99 latency |
|---|---:|---:|---:|
| Steady state | 98.3 strokes/s | 5.88ms | 8.25ms |
| +50ms injected latency | 86.1 strokes/s | 5.37ms | 8.49ms |
| 5% message drop | 132 strokes/s | 4.83ms | 8.71ms |

Leader re-election after an intentional kill averaged 184ms, with a p95 of 906ms across 5 trials.

## Testing

The project includes a focused Jest suite covering the invariants that matter in a Raft cluster:

- election safety and term monotonicity
- no-op-on-election commit behavior
- split-brain prevention during minority partitions
- stale leader step-down after recovery
- exactly-once write semantics across leader failover
- snapshot compaction, restart recovery, and linearizable reads
- randomized chaos convergence checks across a live cluster

This is the part of the project that proves the Raft logic is not just implemented, but stress-tested under failure conditions.

## What I’d do differently

- The single gateway is still a SPOF for client-facing traffic.
- Membership is static rather than dynamically reconfigurable.
- The system is intentionally limited to one Raft group and does not explore sharding or multi-cluster partitioning.
- There is no external auth, multi-tenant isolation, or production observability stack beyond the local dashboard and metrics.

These are real engineering boundaries, not excuses — they mark where the project is deliberately scoped to a single distributed systems exercise.

## Tech stack

Node.js, Express, better-sqlite3, vanilla JavaScript, Docker Compose, and Jest.
