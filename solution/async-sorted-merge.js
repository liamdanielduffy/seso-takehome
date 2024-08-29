"use strict";

/**
 * Time Complexity: O(N log K), where N is the total number of log entries and K is the number of sources
 * - Each log entry is processed once, involving a push and pop operation on the priority queue (O(log K) each)
 * - We process all N log entries, so the total time complexity is O(N log K)
 *
 * Space Complexity: O(K), where K is the number of sources
 * - The priority queue stores at most one entry per source at any given time
 * - We maintain a fixed number of additional data structures (subjects, arrays) with size proportional to K
 * - The space used for processing individual log entries is constant and doesn't affect the overall complexity
 *
 * Performance Notes:
 * - This solution waits for all sources to have an entry in the queue
 *   before printing, which means printing is only as fast as the slowest source
 * - If some sources produce entries much faster than others, their entries may accumulate in the queue,
 *   potentially leading to high memory usage.
 */

const { Subject, merge } = require("rxjs");
const { scan } = require("rxjs/operators");
const { MinPriorityQueue } = require("@datastructures-js/priority-queue");

function mergeAndPrintLogs(sources, printer) {
  const logSourceStreams = sources.map(() => new Subject());
  const sourceCount = sources.length;
  const allLogsStream = merge(...logSourceStreams);

  const processedLogsStream = allLogsStream.pipe(
    scan(
      (acc, log) => {
        if (!log.data) {
          acc.numUndrainedSources--;
        } else {
          acc.queue.push(log);
          if (!acc.sourcesInQueue[log.sourceIndex]) {
            acc.sourcesInQueue[log.sourceIndex] = true;
            acc.numSourcesInQueue++;
          }
        }
        return acc;
      },
      {
        queue: new MinPriorityQueue((log) => log.data.date.getTime()),
        sourcesInQueue: new Array(sourceCount).fill(false),
        numSourcesInQueue: 0,
        numUndrainedSources: sourceCount,
      }
    )
  );

  return new Promise((resolve, reject) => {
    let lastAcc;
    processedLogsStream.subscribe({
      next: (acc) => {
        lastAcc = acc;
        while (
          acc.numSourcesInQueue === acc.numUndrainedSources &&
          !acc.queue.isEmpty()
        ) {
          const log = acc.queue.pop();
          printer.print(log.data);
          acc.sourcesInQueue[log.sourceIndex] = false;
          acc.numSourcesInQueue--;
        }
      },
      error: reject,
      complete: () => {
        while (lastAcc && !lastAcc.queue.isEmpty()) {
          const log = lastAcc.queue.pop();
          printer.print(log.data);
        }
        printer.done();
        console.log("Async sort complete.");
        resolve();
      },
    });
    for (let i = 0; i < sources.length; i++) {
      fetchLogs(sources[i], i, logSourceStreams[i])();
    }
  });
}

function fetchLogs(source, index, sourceLogStream) {
  return async function () {
    while (true) {
      const log = await source.popAsync();
      if (!log) {
        sourceLogStream.next({ data: null, sourceIndex: null });
        sourceLogStream.complete();
        break;
      }
      sourceLogStream.next({ data: log, sourceIndex: index });
    }
  };
}

module.exports = mergeAndPrintLogs;
