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
    description: "The ID of the stack to run the task on",
    type: "string",
    required: true,
    graphqlFieldKey: "stack",
  }),
  command: defineSpaceliftInputConfig({
    name: "Command",
    description: "The command to execute",
    type: "string",
    required: true,
    graphqlFieldKey: "command",
  }),
  skipInitialization: defineSpaceliftInputConfig({
    name: "Skip Initialization",
    description:
      "Initialize the workspace before running this task. This means executing all the pre-initialization hooks as well as running the vendor-specific initialization procedure. Toggle this off only if you want to explicitly skip this step.",
    type: "boolean",
    required: false,
    graphqlFieldKey: "skipInitialization",
  }),
};

const CREATE_TASK_MUTATION = `
  mutation CreateTask($stack: ID!, $command: String!, $skipInitialization: Boolean) {
    taskCreate(stack: $stack, command: $command, skipInitialization: $skipInitialization) {
      id
      createdAt
      state
      command
    }
  }
`;

const TERMINAL_STATES = [
  "CANCELED",
  "FAILED",
  "FINISHED",
  "STOPPED",
  "SKIPPED",
];

export const performTask: AppBlock = {
  name: "Perform Task",
  description:
    "Execute a task on a Spacelift stack and emit events for state changes",
  category: "Tasks",
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
          CREATE_TASK_MUTATION,
          variables,
        );

        const pendingEventId = await events.createPending({
          event: {
            taskId: result.taskCreate.id,
            stackId: variables.stack,
            command: result.taskCreate.command,
          },
          outputKey: "completed",
          statusDescription: `Task ${result.taskCreate.id} started`,
        });

        await kv.app.set({
          key: `run:${result.taskCreate.id}`,
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
      statusDescription: `Task ${run.id} is ${state.toLowerCase()}`,
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

      await kv.app.delete([`task:${run.id}`]);
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
            const taskId = item.value.taskId;

            await events.cancelPending(
              item.value.pendingEventId,
              `Task ${taskId} did not complete within 24 hours - cleaning up stale pending event`,
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
          account: { type: "string" },
          note: {
            anyOf: [{ type: "string" }, { type: "null" }],
          },
        },
        required: ["run", "stack", "state", "stateVersion", "account", "note"],
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
          account: { type: "string" },
          note: {
            anyOf: [{ type: "string" }, { type: "null" }],
          },
        },
        required: ["run", "stack", "state", "stateVersion", "account", "note"],
      },
    },
  },
};
