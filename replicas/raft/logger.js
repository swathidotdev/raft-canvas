/**
 * Structured logger.
 *
 * Prints one JSON-ish line per event, prefixed with a bracketed context header
 * so `docker compose logs -f replica1` remains grep-able the way the original
 * code was, while giving us machine-parseable fields for the dashboard.
 */

function createLogger(replicaId, getContext) {
  function stamp() {
    return new Date().toISOString().slice(11, 23);
  }

  function fmt(level, msg, detail) {
    const ctx = getContext ? getContext() : {};
    const term = ctx.term ?? 0;
    const role = (ctx.role ?? "follower").toUpperCase();
    const base = `[${stamp()}] [${replicaId}] [term:${term}] [${role}] ${msg}`;
    if (detail === undefined || detail === null || detail === "") return base;
    if (typeof detail === "string") return `${base} | ${detail}`;
    return `${base} | ${JSON.stringify(detail)}`;
  }

  return {
    info:  (msg, detail) => console.log (fmt("info",  msg, detail)),
    warn:  (msg, detail) => console.warn(fmt("warn",  msg, detail)),
    error: (msg, detail) => console.error(fmt("error", msg, detail)),
    // Reserve debug() so we can silence chatty per-heartbeat noise later.
    debug: (msg, detail) => {
      if (process.env.RAFT_DEBUG === "1") console.log(fmt("debug", msg, detail));
    },
  };
}

module.exports = { createLogger };
