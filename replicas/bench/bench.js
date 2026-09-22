#!/usr/bin/env node
/**
 * Mini-RAFT benchmark suite.
 *
 * Usage:
 *   node bench/bench.js --mode inprocess     (fast, no docker needed)
 *   node bench/bench.js --mode http          (real HTTP against running cluster)
 *
 * Optional:
 *   --duration 5000     throughput window in ms   (default 5000)
 *   --clients 4         throughput concurrency    (default 4)
 *   --latency-samples 200
 *   --reelect-trials 5
 *   --skip-faults       skip the injected-fault scenarios
 *   --skip-reelect      skip the leader-kill scenarios
 *
 * Each run appends a Markdown report under bench/results/.
 */

const args = process.argv.slice(2);
function arg(name, def) {
  const i = args.indexOf(`--${name}`);
  return i === -1 ? def : args[i + 1];
}
function flag(name) { return args.includes(`--${name}`); }

const MODE            = arg("mode", "inprocess");
const DURATION        = parseInt(arg("duration", "5000"));
const CLIENTS         = parseInt(arg("clients", "4"));
const LATENCY_SAMPLES = parseInt(arg("latency-samples", "200"));
const REELECT_TRIALS  = parseInt(arg("reelect-trials", "5"));
const SKIP_FAULTS     = flag("skip-faults");
const SKIP_REELECT    = flag("skip-reelect");

const { InProcessDriver } = require("./inprocess");
const { HttpDriver }      = require("./http");
const { throughput, commitLatency, timeToReelect } = require("./scenarios");
const { writeReport }     = require("./report");
const { sleep }           = require("../tests/integration/cluster");

function makeDriver(mode) {
  if (mode === "inprocess") return new InProcessDriver();
  if (mode === "http")      return new HttpDriver();
  throw new Error(`unknown mode: ${mode}`);
}

async function main() {
  console.log(`[bench] mode=${MODE} clients=${CLIENTS} duration=${DURATION}ms`);

  const driver = makeDriver(MODE);
  await driver.setup();

  const sections = [];

  console.log("\n[bench] scenario 1/5: throughput (steady state)");
  const tpBase = await throughput(driver, { tag: "s1", clients: CLIENTS, durationMs: DURATION });

  console.log("[bench] scenario 2/5: commit latency (steady state)");
  const latBase = await commitLatency(driver, { tag: "s2", samples: LATENCY_SAMPLES });

  sections.push(["Steady state (no faults)", [tpBase, latBase]]);

  if (!SKIP_FAULTS) {
    console.log("\n[bench] scenario 3/5: throughput + latency under +50ms per RPC");
    await driver.setGlobalLatency(50);
    await sleep(300);
    const tpLat = await throughput(driver, { tag: "s3", clients: CLIENTS, durationMs: DURATION });
    const latLat = await commitLatency(driver, { tag: "s4", samples: Math.min(100, LATENCY_SAMPLES) });
    await driver.clearFaults();
    await sleep(300);
    sections.push(["Under injected +50ms RPC latency", [tpLat, latLat]]);

    console.log("[bench] scenario 4/5: throughput + latency under 5% message drop");
    await driver.setGlobalDrop(0.05);
    await sleep(300);
    const tpDrop = await throughput(driver, { tag: "s5", clients: CLIENTS, durationMs: DURATION });
    const latDrop = await commitLatency(driver, { tag: "s6", samples: Math.min(100, LATENCY_SAMPLES) });
    await driver.clearFaults();
    await sleep(300);
    sections.push(["Under 5% message drop rate", [tpDrop, latDrop]]);
  }

  if (!SKIP_REELECT) {
    console.log(`\n[bench] scenario 5/5: time-to-reelect (${REELECT_TRIALS} trials)`);
    // In HTTP mode this crashes real processes; docker restart:on-failure
    // brings them back. In-process mode, killNode doesn't restart, so the
    // cluster shrinks — we only do 1 trial before we'd lose quorum.
    const trials = MODE === "inprocess"
      ? Math.min(REELECT_TRIALS, 1)
      : REELECT_TRIALS;
    const reelect = await timeToReelect(driver, { trials });
    sections.push(["Recovery: leader kill", [reelect]]);
  }

  await driver.teardown();

  writeReport(MODE, sections);
  process.exit(0);
}

main().catch((err) => {
  console.error("[bench] FAILED:", err.message);
  console.error(err.stack);
  process.exit(1);
});