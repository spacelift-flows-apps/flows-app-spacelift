import { AppBlock, events, kv, timers } from "@slflows/sdk/v1";
import { executeSpaceliftQuery, extractCredentials } from "../../client";

const TRIGGER_RUN_BATCH_MUTATION = `
  mutation RunTriggerBatch($input: RunTriggerBatchInput!) {
    runTriggerBatch(input: $input) {
      runs {
        runId
        run {
          id
          state
          stackId
          updatedAt
        }
      }
    }
  }
`;

// Run states that mean the run will not change any further.
const TERMINAL_STATES = [
  "CANCELED",
  "FAILED",
  "FINISHED",
  "DISCARDED",
  "STOPPED",
  "SKIPPED",
];

const ALL_RUN_STATES = [
  "UNKNOWN",
  "QUEUED",
  "CANCELED",
  "INITIALIZING",
  "PLANNING",
  "FAILED",
  "FINISHED",
  "UNCONFIRMED",
  "DISCARDED",
  "CONFIRMED",
  "APPLYING",
  "PERFORMING",
  "STOPPED",
  "DESTROYING",
  "PREPARING",
  "PREPARING_APPLY",
  "SKIPPED",
  "REPLAN_REQUESTED",
  "PENDING", // DEPRECATED
  "READY",
  "PREPARING_REPLAN",
  "PENDING_REVIEW",
];

// Flush at most one delta event per this many seconds.
const FLUSH_INTERVAL_SECONDS = 10;

// Batches and their per-run state live in block KV for 30 days, after which
// the cleanup schedule reaps anything that never reached completion.
const TTL_30_DAYS = 30 * 24 * 60 * 60;

interface BatchMeta {
  pendingEventId: string;
  parentEventId: string;
  total: number;
  createdAt: number;
  // Whether a flush timer is currently armed. Used to debounce: the first
  // change in a window arms the timer, subsequent changes just accumulate.
  timerArmed: boolean;
}

interface RunState {
  stackSlug: string;
  // Spacelift run updatedAt as a nanosecond epoch timestamp. Used to drop
  // out-of-order webhooks.
  runUpdatedAt: number;
  state: string;
}

interface Delta {
  stackSlug: string;
  oldState: string;
  newState: string;
  runUpdatedAt: number;
}

// Recompute the {STATE: count} summary by listing every tracked run for the
// batch. O(runs) per flush, not per state change — that is what keeps the
// whole block linear rather than quadratic.
async function computeSummary(
  batchId: string,
): Promise<{ summary: Record<string, number>; terminalCount: number }> {
  const summary: Record<string, number> = {};
  let terminalCount = 0;
  let startingKey: string | undefined;

  do {
    const page = await kv.block.list({
      keyPrefix: `run:${batchId}:`,
      startingKey,
    });
    for (const { value } of page.pairs) {
      const state = (value as RunState).state;
      summary[state] = (summary[state] || 0) + 1;
      if (TERMINAL_STATES.includes(state)) {
        terminalCount++;
      }
    }
    startingKey = page.nextStartingKey;
  } while (startingKey);

  return { summary, terminalCount };
}

async function listAll(
  keyPrefix: string,
): Promise<Array<{ key: string; value?: any }>> {
  const out: Array<{ key: string; value?: any }> = [];
  let startingKey: string | undefined;
  do {
    const page = await kv.block.list({ keyPrefix, startingKey });
    out.push(...page.pairs);
    startingKey = page.nextStartingKey;
  } while (startingKey);
  return out;
}

async function cleanupBatch(batchId: string): Promise<void> {
  const runKeys = (await listAll(`run:${batchId}:`)).map((p) => p.key);
  const deltaKeys = (await listAll(`delta:${batchId}:`)).map((p) => p.key);
  const appRunKeys = runKeys.map(
    (k) => `triggerRunBatch:${k.substring(`run:${batchId}:`.length)}`,
  );

  if (runKeys.length || deltaKeys.length) {
    await kv.block.delete([...runKeys, ...deltaKeys, `batch:${batchId}`]);
  } else {
    await kv.block.delete([`batch:${batchId}`]);
  }
  if (appRunKeys.length) {
    await kv.app.delete(appRunKeys);
  }
}

const runStateValueSchema = {
  type: "object" as const,
  properties: {
    stackSlug: { type: "string" as const },
    oldState: { type: "string" as const, enum: ALL_RUN_STATES },
    newState: { type: "string" as const, enum: ALL_RUN_STATES },
    runUpdatedAt: { type: "number" as const },
  },
  required: ["stackSlug", "oldState", "newState", "runUpdatedAt"],
};

const stateSummarySchema = {
  type: "object" as const,
  description:
    "Count of runs currently in each state across the whole batch. Always sums to the total number of runs.",
  additionalProperties: { type: "integer" as const },
};

export const triggerRunBatch: AppBlock = {
  name: "Trigger Run Batch",
  description:
    "Trigger runs on many Spacelift stacks at once and track them as a single batch, emitting debounced state-change deltas and a final summary once every run is terminal.",
  category: "Runs",
  inputs: {
    default: {
      config: {
        stacks: {
          name: "Stacks",
          description:
            "The stacks to trigger runs for. Each entry needs a stack slug and may optionally specify a commit SHA.",
          type: {
            type: "array",
            items: {
              type: "object",
              properties: {
                stack: {
                  type: "string",
                  description: "The stack slug",
                },
                commitSha: {
                  type: "string",
                  description:
                    "Commit SHA to use. Defaults to the commit currently tracked by the stack, or the HEAD of the tracked branch.",
                },
              },
              required: ["stack"],
            },
          },
          required: true,
        },
        forceApply: {
          name: "Force Apply",
          description:
            "Controls behavior when a run's plan shows zero changes. SINGLE force-applies only this run; CASCADE force-applies this run and all triggered dependents. Leave unset for normal behavior (runs with no changes stop at the plan stage).",
          type: {
            enum: ["SINGLE", "CASCADE"],
          },
          required: false,
        },
      },
      onEvent: async (input) => {
        const { stacks, forceApply } = input.event.inputConfig;
        const credentials = extractCredentials(input.app.config);

        const result = await executeSpaceliftQuery(
          credentials,
          TRIGGER_RUN_BATCH_MUTATION,
          {
            input: {
              stacks: (stacks as Array<Record<string, any>>).map((entry) => ({
                stack: entry.stack,
                ...(entry.commitSha ? { commitSha: entry.commitSha } : {}),
              })),
              ...(forceApply ? { forceApply } : {}),
            },
          },
        );

        const runs = result.runTriggerBatch.runs as Array<{
          runId: string;
          run: {
            id: string;
            state: string;
            stackId: string;
            updatedAt: number;
          };
        }>;

        // A batch ID scopes all KV keys for this trigger so multiple batches
        // tracked by the same block instance never collide.
        const batchId = input.event.id;

        const pendingEventId = await events.createPending({
          event: { batchId, total: runs.length },
          outputKey: "completed",
          statusDescription: `Batch of ${runs.length} run(s) triggered`,
        });

        // Persist per-run state and the app-level routing keys the shared
        // webhook handler uses to deliver each run's webhooks back to us.
        // We don't know the stack slug for runs we can't read (dependency
        // triggered on inaccessible stacks); fall back to the runId.
        for (const r of runs) {
          const stackSlug = r.run.stackId;
          const state = r.run.state;
          const runUpdatedAt = r.run.updatedAt;

          await kv.block.set({
            key: `run:${batchId}:${r.runId}`,
            value: { stackSlug, state, runUpdatedAt } satisfies RunState,
            ttl: TTL_30_DAYS,
          });

          await kv.app.set({
            key: `triggerRunBatch:${r.runId}`,
            value: {
              blockId: input.block.id,
              batchId,
              pendingEventId,
              parentEventId: input.event.id,
            },
            ttl: TTL_30_DAYS,
          });
        }

        const { summary } = await computeSummary(batchId);

        await kv.block.set({
          key: `batch:${batchId}`,
          value: {
            pendingEventId,
            parentEventId: input.event.id,
            total: runs.length,
            createdAt: Date.now(),
            timerArmed: false,
          } satisfies BatchMeta,
          ttl: TTL_30_DAYS,
        });

        // Emit an initial baseline: every run as a delta from "" to its
        // starting state, so downstream consumers see the full set once up front.
        const baseline: Record<string, Delta> = {};
        for (const r of runs) {
          baseline[r.runId] = {
            stackSlug: r.run?.stackId,
            oldState: "UNKNOWN",
            newState: r.run.state,
            runUpdatedAt: r.run.updatedAt,
          };
        }

        await events.emit(
          { batchId, stateChanges: baseline, stateSummary: summary },
          { outputKey: "stateChanged", parentEventId: input.event.id },
        );
      },
    },
  },
  onInternalMessage: async (input) => {
    const { payload, batchId, pendingEventId, parentEventId } =
      input.message.body;

    if (!payload?.run?.id || !batchId) {
      return;
    }

    const runId = payload.run.id;
    const newState = payload.state;
    const stackSlug = payload.stack?.id || runId;
    const runUpdatedAt = payload.run.updated_at;

    const { value: current } = await kv.block.get(`run:${batchId}:${runId}`);
    if (!current) {
      // Run isn't part of a batch we're tracking (or already cleaned up).
      return;
    }

    const oldState = (current as RunState).state;
    if (oldState === newState) {
      return;
    }
    // Ignore out-of-order webhooks: never let an older event overwrite a
    // newer one we've already recorded.
    if (runUpdatedAt < (current as RunState).runUpdatedAt) {
      return;
    }

    // Update the run's current state (feeds the summary recomputed on flush).
    await kv.block.set({
      key: `run:${batchId}:${runId}`,
      value: { stackSlug, state: newState, runUpdatedAt } satisfies RunState,
      ttl: TTL_30_DAYS,
    });

    // Record the delta to be flushed. If several webhooks for the same run
    // arrive within one window we keep the earliest oldState already stored.
    const { value: existingDelta } = await kv.block.get(
      `delta:${batchId}:${runId}`,
    );
    await kv.block.set({
      key: `delta:${batchId}:${runId}`,
      value: {
        stackSlug,
        oldState: (existingDelta as Delta | undefined)?.oldState ?? oldState,
        newState,
        runUpdatedAt,
      } satisfies Delta,
      ttl: TTL_30_DAYS,
    });

    // Debounce: arm a single flush timer per window. Subsequent changes in the
    // same window just accumulate deltas and ride the already-armed timer.
    const { value: meta } = await kv.block.get(`batch:${batchId}`);
    if (meta && !(meta as BatchMeta).timerArmed) {
      await timers.block.set(FLUSH_INTERVAL_SECONDS, {
        inputPayload: { batchId, pendingEventId, parentEventId },
        description: `Flush state changes for run batch ${batchId}`,
      });
      await kv.block.set({
        key: `batch:${batchId}`,
        value: { ...(meta as BatchMeta), timerArmed: true },
        ttl: TTL_30_DAYS,
      });
    }
  },
  onTimer: async (input) => {
    const { batchId, pendingEventId, parentEventId } = input.timer.payload as {
      batchId: string;
      pendingEventId: string;
      parentEventId: string;
    };

    const { value: meta } = await kv.block.get(`batch:${batchId}`);
    if (!meta) {
      return; // Batch already completed/cleaned up.
    }

    // Drain and clear the accumulated deltas for this window.
    const deltaPairs = await listAll(`delta:${batchId}:`);
    const stateChanges: Record<string, Delta> = {};
    for (const { key, value } of deltaPairs) {
      const runId = key.substring(`delta:${batchId}:`.length);
      stateChanges[runId] = value as Delta;
    }

    const { summary, terminalCount } = await computeSummary(batchId);

    // Disarm the timer; if changes arrived while we were flushing the next
    // onInternalMessage will re-arm it.
    await kv.block.set({
      key: `batch:${batchId}`,
      value: { ...(meta as BatchMeta), timerArmed: false },
      ttl: TTL_30_DAYS,
    });

    if (deltaPairs.length > 0) {
      await kv.block.delete(deltaPairs.map((p) => p.key));

      await events.emit(
        { batchId, stateChanges, stateSummary: summary },
        { outputKey: "stateChanged", parentEventId },
      );

      await events.updatePending(pendingEventId, {
        statusDescription: `Batch ${terminalCount}/${(meta as BatchMeta).total} run(s) terminal`,
      });
    }

    if (terminalCount === (meta as BatchMeta).total) {
      await emitCompleted(batchId, pendingEventId, parentEventId, summary);
    }
  },
  schedules: {
    cleanup: {
      definition: {
        type: "cron",
        cron: {
          expression: "0 * * * *",
          location: "UTC",
        },
      },
      onTrigger: async () => {
        const staleThreshold = Date.now() - TTL_30_DAYS * 1000;

        const batches = await listAll("batch:");
        for (const { key, value } of batches) {
          if ((value as BatchMeta).createdAt >= staleThreshold) {
            continue;
          }
          const batchId = key.substring("batch:".length);

          await events.cancelPending(
            (value as BatchMeta).pendingEventId,
            `Run batch ${batchId} did not complete within 30 days - cleaning up stale pending event`,
          );

          await cleanupBatch(batchId);
        }
      },
    },
  },
  outputs: {
    completed: {
      default: true,
      name: "Batch Completed",
      description:
        "Emitted once every run in the batch has reached a terminal state",
      type: {
        type: "object",
        properties: {
          batchId: { type: "string" },
          stateSummary: stateSummarySchema,
          runs: {
            type: "array",
            description: "Final state of every run in the batch.",
            items: {
              type: "object",
              properties: {
                runId: { type: "string" },
                stackSlug: { type: "string" },
                finalState: { type: "string", enum: TERMINAL_STATES },
              },
              required: ["runId", "stackSlug", "finalState"],
            },
          },
        },
        required: ["batchId", "stateSummary", "runs"],
      },
    },
    stateChanged: {
      secondary: true,
      name: "State Changed",
      description:
        "Emitted at most once per 10 seconds with only the runs whose state changed in that window, plus a full summary of how many runs are in each state.",
      type: {
        type: "object",
        properties: {
          batchId: { type: "string" },
          stateChanges: {
            type: "object",
            description:
              "Sparse map of runId -> the change for runs that changed state in this window. Unchanged runs are omitted. On the first (baseline) emission every run is present with an empty oldState.",
            additionalProperties: runStateValueSchema,
          },
          stateSummary: stateSummarySchema,
        },
        required: ["batchId", "stateChanges", "stateSummary"],
      },
    },
  },
};

async function emitCompleted(
  batchId: string,
  pendingEventId: string,
  parentEventId: string,
  summary: Record<string, number>,
): Promise<void> {
  const runPairs = await listAll(`run:${batchId}:`);
  const runs = runPairs.map(({ key, value }) => ({
    runId: key.substring(`run:${batchId}:`.length),
    stackSlug: (value as RunState).stackSlug,
    finalState: (value as RunState).state,
  }));

  await events.emit(
    { batchId, stateSummary: summary, runs },
    { outputKey: "completed", complete: pendingEventId, parentEventId },
  );

  await cleanupBatch(batchId);
}
