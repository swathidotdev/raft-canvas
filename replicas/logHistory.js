function getLogEntries({ fromIndex, toIndex, persistence, stateMachine }) {
  return stateMachine
    .getStrokesFrom(fromIndex)
    .filter((stroke) => stroke.index <= toIndex)
    .map((stroke) => {
      const entry = persistence.getEntry(stroke.index);
      const { index, ...strokePayload } = stroke;
      return {
        index,
        term: entry?.term ?? null,
        stroke: strokePayload,
      };
    });
}

module.exports = { getLogEntries };