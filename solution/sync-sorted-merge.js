"use strict";

const { MinPriorityQueue } = require("@datastructures-js/priority-queue");

function mergeAndPrintLogs(sources, printer) {
  const queue = new MinPriorityQueue((item) => item.log.date.getTime());
  sources.forEach((source, index) => {
    const log = source.pop();
    if (log) {
      queue.push({ log, indexOfSource: index });
    }
  });
  while (!queue.isEmpty()) {
    const entry = queue.pop();
    const log = entry.log;
    const indexOfSource = entry.indexOfSource;
    const source = sources[indexOfSource];

    printer.print(log);

    if (!source.drained) {
      const nextLog = source.pop();
      if (nextLog) {
        entry.log = nextLog;
        queue.push(entry);
      }
    }
  }
}

module.exports = (sources, printer) => {
  mergeAndPrintLogs(sources, printer);
  printer.done();
  return console.log("Sync sort complete.");
};
