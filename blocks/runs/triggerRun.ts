import { AppBlock, events, kv } from "@slflows/sdk/v1";
import { executeSpaceliftQuery, extractCredentials } from "../../client";
import {
  defineSpaceliftInputConfig,
  mapInputConfig,
  mapInputsToGraphQLVariables,
} from "../../utils";

const inputConfig = {
  stackId: defineSpaceliftInputConfig({
    name: "Stack ID",
    description: "The ID of the stack to start a run for",
    type: "string",
    required: true,
    graphqlFieldKey: "stack",
  }),
  commitSha: defineSpaceliftInputConfig({
    name: "Commit SHA",
    description: "Specific commit SHA to run (optional)",
    type: "string",
    required: false,
    graphqlFieldKey: "commitSha",
  }),
};

const START_RUN_MUTATION = `
  mutation StartRun($stack: ID!, $runType: RunType, $commitSha: String) {
    runTrigger(stack: $stack, runType: $runType, commitSha: $commitSha) {
      id
      createdAt
      updatedAt
      state
      type
      commit {
        hash
      }
    }
  }
`;

const TERMINAL_STATES = [
  "CANCELED",
  "FAILED",
  "FINISHED",
  "DISCARDED",
  "STOPPED",
  "SKIPPED",
];

export const triggerRun: AppBlock = {
  name: "Trigger Run",
  description:
    "Start a new run on a Spacelift stack and emit events for state changes",
  category: "Runs",
  inputs: {
    default: {
      config: mapInputConfig(inputConfig),
      onEvent: async (input) => {
        const variables = mapInputsToGraphQLVariables(
          inputConfig,
          input.event.inputConfig,
        );
        const credentials = extractCredentials(input.app.config);

        const result = await executeSpaceliftQuery(
          credentials,
          START_RUN_MUTATION,
          variables,
        );

        const pendingEventId = await events.createPending({
          event: {
            runId: result.runTrigger.id,
            stackId: variables.stack,
          },
          outputKey: "completed",
          statusDescription: `Run ${result.runTrigger.id} started`,
        });

        await kv.app.set({
          key: `run:${result.runTrigger.id}`,
          value: {
            blockId: input.block.id,
            parentEventId: input.event.id,
            pendingEventId,
          },
        });
      },
    },
  },
  onInternalMessage: async (input) => {
    const { parentEventId, payload, pendingEventId } = input.message.body;

    if (!payload?.run) {
      return;
    }

    const { run, stack, state, stateVersion, account } = payload;

    await events.updatePending(pendingEventId, {
      statusDescription: `Run ${run.id} is ${state.toLowerCase()}`,
    });

    await events.emit(
      { run, stack, state, stateVersion, account },
      { outputKey: "stateChanged", parentEventId },
    );

    if (TERMINAL_STATES.includes(state)) {
      await events.emit(
        { run, stack, state, stateVersion, account },
        { outputKey: "completed", complete: pendingEventId, parentEventId },
      );

      await kv.app.delete([`run:${run.id}`]);
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
        const staleThreshold = Date.now() - 24 * 60 * 60 * 1000;

        const pendingEvents = await kv.block.list({
          keyPrefix: "pending:",
        });

        for (const item of pendingEvents.pairs) {
          if (item.value.createdAt < staleThreshold) {
            const runId = item.value.runId;

            await events.cancelPending(
              item.value.pendingEventId,
              `Run ${runId} did not complete within 24 hours - cleaning up stale pending event`,
            );

            await kv.block.delete([item.key]);
          }
        }
      },
    },
  },
  outputs: {
    completed: {
      default: true,
      name: "Run Completed",
      description: "Emitted when the run reaches a terminal state",
      type: {
        type: "object",
        properties: {
          run: {
            type: "object",
            properties: {
              id: { type: "string" },
              branch: { type: "string" },
              command: {
                anyOf: [{ type: "string" }, { type: "null" }],
              },
              commit: {
                type: "object",
                properties: {
                  authorLogin: {
                    anyOf: [{ type: "string" }, { type: "null" }],
                  },
                  authorName: { type: "string" },
                  hash: { type: "string" },
                  message: { type: "string" },
                  timestamp: { type: "number" },
                  url: { type: "string" },
                },
                required: ["authorName", "hash", "message", "timestamp", "url"],
              },
              createdAt: { type: "number" },
              delta: {
                anyOf: [
                  {
                    type: "object",
                    properties: {
                      added: { type: "number" },
                      changed: { type: "number" },
                      deleted: { type: "number" },
                      resources: { type: "number" },
                    },
                    required: ["added", "changed", "deleted", "resources"],
                  },
                  { type: "null" },
                ],
              },
              driftDetection: { type: "boolean" },
              triggeredBy: {
                anyOf: [{ type: "string" }, { type: "null" }],
              },
              type: { type: "string" },
              url: { type: "string" },
            },
            required: [
              "id",
              "branch",
              "commit",
              "createdAt",
              "driftDetection",
              "type",
              "url",
            ],
          },
          stack: {
            type: "object",
            properties: {
              id: { type: "string" },
              name: { type: "string" },
              description: {
                anyOf: [{ type: "string" }, { type: "null" }],
              },
              labels: {
                type: "array",
                items: { type: "string" },
              },
              repository: { type: "string" },
              url: { type: "string" },
              vcs: { type: "string" },
            },
            required: ["id", "name", "labels", "repository", "url", "vcs"],
          },
          state: {
            type: "string",
            enum: TERMINAL_STATES,
          },
          stateVersion: { type: "number" },
          account: { type: "object" },
          note: {
            anyOf: [{ type: "string" }, { type: "null" }],
          },
        },
        required: ["run", "stack", "state"],
      },
    },
    stateChanged: {
      secondary: true,
      name: "State Changed",
      description: "Emitted when the run state changes",
      type: {
        type: "object",
        properties: {
          run: {
            type: "object",
            properties: {
              id: { type: "string" },
              branch: { type: "string" },
              command: {
                anyOf: [{ type: "string" }, { type: "null" }],
              },
              commit: {
                type: "object",
                properties: {
                  authorLogin: {
                    anyOf: [{ type: "string" }, { type: "null" }],
                  },
                  authorName: { type: "string" },
                  hash: { type: "string" },
                  message: { type: "string" },
                  timestamp: { type: "number" },
                  url: { type: "string" },
                },
                required: ["authorName", "hash", "message", "timestamp", "url"],
              },
              createdAt: { type: "number" },
              delta: {
                anyOf: [
                  {
                    type: "object",
                    properties: {
                      added: { type: "number" },
                      changed: { type: "number" },
                      deleted: { type: "number" },
                      resources: { type: "number" },
                    },
                    required: ["added", "changed", "deleted", "resources"],
                  },
                  { type: "null" },
                ],
              },
              driftDetection: { type: "boolean" },
              triggeredBy: {
                anyOf: [{ type: "string" }, { type: "null" }],
              },
              type: { type: "string" },
              url: { type: "string" },
            },
            required: [
              "id",
              "branch",
              "commit",
              "createdAt",
              "driftDetection",
              "type",
              "url",
            ],
          },
          stack: {
            type: "object",
            properties: {
              id: { type: "string" },
              name: { type: "string" },
              description: {
                anyOf: [{ type: "string" }, { type: "null" }],
              },
              labels: {
                type: "array",
                items: { type: "string" },
              },
              repository: { type: "string" },
              url: { type: "string" },
              vcs: { type: "string" },
            },
            required: ["id", "name", "labels", "repository", "url", "vcs"],
          },
          state: {
            type: "string",
            enum: [
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
              "READY",
              "PREPARING_REPLAN",
              "PENDING_REVIEW",
            ],
          },
          stateVersion: { type: "number" },
          account: { type: "object" },
          note: {
            anyOf: [{ type: "string" }, { type: "null" }],
          },
        },
        required: ["run", "stack", "state"],
      },
    },
  },
};
