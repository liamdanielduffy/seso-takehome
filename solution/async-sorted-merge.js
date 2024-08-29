"use strict";

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
          acc.queue.enqueue(log);
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
          const log = acc.queue.dequeue();
          printer.print(log.data);
          acc.sourcesInQueue[log.sourceIndex] = false;
          acc.numSourcesInQueue--;
        }
      },
      error: reject,
      complete: () => {
        while (lastAcc && !lastAcc.queue.isEmpty()) {
          const log = lastAcc.queue.dequeue();
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
