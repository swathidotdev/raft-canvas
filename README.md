# Mini-RAFT Distributed Drawing Board

Mini-RAFT is a small, observable Raft cluster behind a collaborative HTML5 canvas. The project is designed to make consensus behavior visible: clients submit strokes, a leader commits them through a majority, followers apply the same ordered entries, and the dashboard can inject failures while the cluster recovers.

## Architecture

```text
Browser
  | WebSocket for live stroke updates
  v
Gateway :3000
  | HTTP POST /stroke, leader discovery, retry
  v
Replica 1 :4001 <--> Replica 2 :4002 <--> Replica 3 :4003
  | RaftNode: elections, replication, ReadIndex, snapshots
  | SQLite: WAL persistence for term, vote, log tail, and snapshots
  v
State machine: ordered, deduplicated canvas strokes
```

### Components

- **Frontend** (`frontend/`): static canvas client. It connects to the gateway WebSocket, replays history from `/log`, and tags every stroke with `(clientId, seq)`.
- **Gateway** (`gateway/`): Express and WebSocket edge service. It discovers the leader, forwards `POST /stroke`, retries the same payload, and broadcasts the leader's committed stroke to connected clients.
- **Replicas** (`replicas/raft/`): three Raft nodes using SQLite-backed persistence and an in-process state machine. The leader is the only replica that broadcasts applied strokes.
- **Dashboard** (`gateway/dashboard/`): cluster status, Raft timeline, and fault injection UI at `/dashboard/`.

The Docker Compose network uses service names internally and publishes the gateway on `localhost:3000` and replicas on `localhost:4001`, `:4002`, and `:4003`.

## Guarantees

- A successful stroke acknowledgement means the leader committed the entry according to the Raft majority rule. The response includes its `logIndex`.
- Replicas apply committed entries in log order and converge on the same deterministic state after communication is restored.
- Retrying a stroke with the same `(clientId, seq)` is deduplicated by the state machine, including retries after a lost HTTP response or leader change.
- Current term, vote, accepted log entries, and snapshots are persisted in SQLite with WAL mode and `synchronous=FULL`.
- A lagging follower can catch up from the leader's snapshot through `InstallSnapshot`, then receive the remaining log tail.
- `ReadIndex` confirms a leader's authority with a majority before returning the committed read index.

## Failure Model

The cluster tolerates one failed or partitioned replica in the default three-node deployment. A majority can elect a leader and commit new strokes; a minority cannot safely commit writes. When a partition heals, Raft repairs the follower from normal entries or a snapshot.

The Docker services use `restart: on-failure`, so the dashboard's Crash action demonstrates process restart and SQLite recovery. Gateway retries are bounded. A retry is safe only when the client preserves the original `(clientId, seq)` pair.

## Known Limitations

- Membership is fixed at three replicas. There is no joint consensus, membership change, or automatic scale-out.
- A majority is required for writes and `ReadIndex`; an isolated minority becomes unavailable for those operations.
- The gateway's leader record and connected WebSocket clients are in memory. Restarting the gateway requires clients to reconnect and causes leader discovery to run again.
- There is no authentication, authorization, TLS, rate limiting, or durable gateway queue. This is a local/demo system, not an internet-facing service.
- The dedupe table is bounded to 10,000 client IDs and is part of the replicated state. Clients must use stable IDs and monotonically increasing sequence numbers.
- The drawing model is append-only strokes. The frontend's Clear Canvas button clears only that browser's visible canvas; it is not a replicated command.
- The frontend assumes the gateway and replicas are reachable on the same host and default ports. Change the URLs in `frontend/canvas.js` for another topology.

## Run The Stack

### Prerequisites

- Docker Desktop with Docker Compose
- Node.js 20 or later for local replica tooling; `better-sqlite3` is a native dependency
- A modern browser

Start the full stack from the repository root:

```bash
cd docker
docker compose up -d --build
```

Serve the static frontend from another terminal:

```bash
cd frontend
python -m http.server 8080
```

Open <http://localhost:8080>. The gateway WebSocket is on port `3000`.

Useful endpoints:

| Service | Endpoint | Purpose |
|---|---|---|
| Gateway | `GET /status` | Gateway and current leader status |
| Gateway | `POST /stroke` | Forward a stroke and return the commit acknowledgement |
| Gateway | `WS /` | Live client updates |
| Gateway | `POST /broadcast` | Leader-to-gateway committed-stroke notification |
| Replica | `GET /status` | Raft role, term, commit, snapshot, and log metadata |
| Replica | `GET /log?from=0` | Committed history, including snapshot-compacted strokes |
| Replica | `GET /read-linearizable` | Leader-confirmed read of the current state |
| Replica | `GET /metrics` | Status, RPC metrics, faults, and event timeline |

Stop the stack with `docker compose down`. Add `-v` when you intentionally want to erase the named SQLite volumes and start with empty replicas.

## Tests

Install replica dependencies and run the complete suite:

```bash
cd replicas
npm install
npm test
```

Useful narrower commands:

```bash
npm run test:unit
npm run test:int
```

The integration suite covers convergence, partitions, failover/deduplication, history replay after compaction, snapshot compaction, `InstallSnapshot` catch-up, SQLite restart recovery, and `ReadIndex`. `node smoketest.js` runs a smaller three-node recovery check without HTTP or Docker.

If the local Node version cannot build `better-sqlite3`, run the tests in the Node 20 replica image instead:

```bash
cd docker
docker compose exec replica1 npm install --include=dev
docker compose exec replica1 npm test -- --silent
```

## Chaos Demo

Open <http://localhost:3000/dashboard/> while the stack is running. The dashboard polls every replica and can inject:

- partitions and full-node isolation
- per-peer or global latency
- message drops
- replica crashes with automatic Docker restart
- clearing faults on one node or every node

The same controls are HTTP endpoints on each replica. For example:

```bash
curl -X POST http://localhost:4001/admin/fault/partition \
  -H "Content-Type: application/json" \
  -d '{"peer":"replica2"}'

curl -X POST http://localhost:4001/admin/fault/clear
```

Watch `/status`, `/metrics`, the dashboard timeline, and the canvas while the leader changes. A minority partition should stop committing; after healing, followers should converge again.

## Benchmark

The benchmark has two modes:

- `npm run bench`: in-process Raft with fake transport. This isolates consensus and SQLite behavior from HTTP and Docker overhead.
- `npm run bench:http`: real Axios and Express traffic through the gateway. The default host configuration uses `localhost:3000` and replicas on `localhost:4001-4003`.

Run the HTTP benchmark against a running stack:

```bash
cd replicas
npm run bench:http
```

It measures steady-state throughput and commit latency, then repeats throughput/latency under `+50ms` RPC latency and `5%` message drop, and finally measures five leader re-elections. The leader-kill scenario must run outside the replica being killed. A Docker-safe runner is:

```bash
docker run --rm --network docker_raft-net \
  -e GATEWAY_URL=http://gateway:3000 \
  -e REPLICA_URLS=http://replica1:4001,http://replica2:4002,http://replica3:4003 \
  -v "$PWD/../replicas/bench/results:/app/bench/results" \
  docker-replica1 node bench/bench.js --mode http
```

Reports are printed to stdout and written under `replicas/bench/results/`. Tune the run with `--duration`, `--clients`, `--latency-samples`, `--reelect-trials`, `--skip-faults`, or `--skip-reelect`. The checked-in report is [2026-09-23T18-17-26-248Z-http.md](replicas/bench/results/2026-09-23T18-17-26-248Z-http.md).
