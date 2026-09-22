/**
 * Markdown report writer — dumps results to stdout AND to a dated
 * file under bench/results/. Files are easy to commit if you want
 * to track numbers over time.
 */

const fs   = require("fs");
const path = require("path");

function fmtNum(n) {
  if (n === undefined || n === null || Number.isNaN(n)) return "—";
  if (n >= 100)  return n.toFixed(0);
  if (n >= 10)   return n.toFixed(1);
  return n.toFixed(2);
}

function renderResult(r) {
  const lines = [];
  if (r.name === "throughput") {
    lines.push(`### Throughput (${r.clients} concurrent clients)`);
    lines.push("");
    lines.push("| metric | value |");
    lines.push("|---|---|");
    lines.push(`| duration | ${fmtNum(r.durationMs)} ms |`);
    lines.push(`| committed | ${r.committed} strokes |`);
    lines.push(`| **throughput** | **${fmtNum(r.throughputPerSec)} strokes/s** |`);
  } else if (r.name === "commit-latency-ms") {
    lines.push(`### Commit latency (${r.samples} serial submits)`);
    lines.push("");
    lines.push("| p50 | p95 | p99 | avg | min | max |");
    lines.push("|---|---|---|---|---|---|");
    lines.push(`| ${fmtNum(r.p50)}ms | ${fmtNum(r.p95)}ms | ${fmtNum(r.p99)}ms | ${fmtNum(r.avg)}ms | ${fmtNum(r.min)}ms | ${fmtNum(r.max)}ms |`);
  } else if (r.name === "time-to-reelect-ms") {
    lines.push(`### Time to re-elect after leader kill (${r.trials} trials)`);
    lines.push("");
    lines.push("| p50 | p95 | avg | min | max |");
    lines.push("|---|---|---|---|---|");
    lines.push(`| ${fmtNum(r.p50)}ms | ${fmtNum(r.p95)}ms | ${fmtNum(r.avg)}ms | ${fmtNum(r.min)}ms | ${fmtNum(r.max)}ms |`);
  } else {
    lines.push("```json"); lines.push(JSON.stringify(r, null, 2)); lines.push("```");
  }
  return lines.join("\n");
}

function renderSection(title, results) {
  return [`## ${title}`, "", ...results.map(renderResult).map((s) => s + "\n")].join("\n");
}

function writeReport(mode, sections) {
  const stamp = new Date().toISOString().replace(/[:.]/g, "-");
  const header = [
    `# Mini-RAFT benchmark — ${mode} mode`,
    "",
    `Run: ${new Date().toISOString()}`,
    `Node: ${process.version}`,
    `Platform: ${process.platform}/${process.arch}`,
    "",
  ].join("\n");
  const body = sections.map(([title, results]) => renderSection(title, results)).join("\n");
  const doc  = header + body;

  console.log("\n" + doc);

  const outDir = path.join(__dirname, "results");
  if (!fs.existsSync(outDir)) fs.mkdirSync(outDir, { recursive: true });
  const outPath = path.join(outDir, `${stamp}-${mode}.md`);
  fs.writeFileSync(outPath, doc);
  console.log(`\n[wrote] ${outPath}`);
}

module.exports = { writeReport };