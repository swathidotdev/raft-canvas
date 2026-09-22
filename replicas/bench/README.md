# Benchmark suite

Two modes; same scenarios; different truths.

## Modes

### `npm run bench` — in-process
Spins up 3 RaftNodes in one Node process with fake transport. Fast
(~15s), no docker needed, isolates the CONSENSUS overhead from HTTP
noise. Use this while tuning heartbeat intervals, commit-loop batching,
persistence pragmas, etc.

### `npm run bench:http` — real HTTP
Assumes `docker compose up` is already running the full stack.
Submits via the gateway (http://localhost:3000/stroke), hits real
axios + Express + fsync overhead. Slower to run (~45s), but the
numbers are what a real client would experience — that's what to
quote on a resume.

## Scenarios

Each mode runs the same five scenarios:

1. **Steady-state throughput** — N concurrent clients hammering submit
2. **Steady-state commit latency** — serial submits, distribution of round-trip time
3. **Throughput + latency under +50ms per-RPC latency**
4. **Throughput + latency under 5% message drop rate**
5. **Time to re-elect after leader kill** (multiple trials for the distribution)

Each scenario uses a unique clientId prefix so its measurements never
hit the dedupe fast-path from a prior scenario — otherwise you'd be
measuring cache lookups instead of commits.

## Output

Reports print to stdout AND land in `bench/results/YYYY-MM-DD-<mode>.md`.

## Flags

| flag | default | notes |
|---|---|---|
| `--mode inprocess\|http` | `inprocess` | which driver |
| `--duration MS` | `5000` | throughput window per scenario |
| `--clients N` | `4` | concurrent client count |
| `--latency-samples N` | `200` | serial-submit sample size |
| `--reelect-trials N` | `5` | leader-kill iterations |
| `--skip-faults` | off | skip the injected-fault scenarios |
| `--skip-reelect` | off | skip the leader-kill scenarios |