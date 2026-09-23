const fs = require("fs");
const os = require("os");
const path = require("path");

const { Persistence } = require("../../raft/Persistence");
const { StateMachine } = require("../../raft/StateMachine");
const { ENTRY_TYPE } = require("../../raft/types");
const { getLogEntries } = require("../../logHistory");

describe("history replay", () => {
  test("returns compacted history from the state machine", () => {
    const dataDir = fs.mkdtempSync(path.join(os.tmpdir(), "raft-history-"));
    const persistence = new Persistence({ dataDir, replicaId: "n1", logger: console });
    const stateMachine = new StateMachine({ logger: console });

    try {
      const entries = Array.from({ length: 250 }, (_, index) => ({
        index,
        term: 1,
        type: ENTRY_TYPE.STROKE,
        payload: { clientId: "history", seq: index + 1, x1: index, y1: index },
      }));
      persistence.appendEntries(entries);
      for (const entry of entries) stateMachine.apply(entry);

      const snapshot = stateMachine.getSnapshot();
      persistence.saveSnapshot(249, 1, snapshot);
      persistence.truncateUpTo(249);

      const result = getLogEntries({
        fromIndex: 0,
        toIndex: 249,
        persistence,
        stateMachine,
      });

      expect(result).toHaveLength(250);
      expect(result[0]).toEqual({ index: 0, term: null, stroke: { x1: 0, y1: 0 } });
      expect(result[249]).toEqual({ index: 249, term: null, stroke: { x1: 249, y1: 249 } });
      expect(persistence.firstIndex()).toBe(250);
    } finally {
      persistence.close();
      fs.rmSync(dataDir, { recursive: true, force: true });
    }
  });
});