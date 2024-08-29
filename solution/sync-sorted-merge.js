"use strict";

/**
 * Time Complexity: O(N log K), where N is the total number of log entries and K is the number of sources
 * - Each log entry is pushed to and popped from the priority queue once, which takes O(log K) time
 * - We process all N log entries, so the total time complexity is O(N log K)
 *
 * Space Complexity: O(K), where K is the number of sources
 * - The priority queue stores at most one entry per source at any given time
 * - Additional space is used for a constant number of variables, which doesn't affect the overall complexity
 */

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
