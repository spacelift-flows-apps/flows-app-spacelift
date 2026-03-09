import { AppBlock, events } from "@slflows/sdk/v1";

function shouldProcessEvent(payload: any, config: any) {
  const { stackIds, runStates, runTypes } = config;

  if (stackIds && stackIds.length > 0 && !stackIds.includes(payload.stack.id)) {
    return false;
  }

  if (runStates && runStates.length > 0 && !runStates.includes(payload.state)) {
    return false;
  }

  if (runTypes && runTypes.length > 0 && !runTypes.includes(payload.run.type)) {
    return false;
  }

  return true;
}

export const onRunStateChange: AppBlock = {
  name: "On Run State Change",
  description:
    "Subscribe to run state changes from Spacelift with configurable filtering",
  category: "Runs",
  config: {
    stackIds: {
      name: "Stack IDs",
      description:
        "List of stack IDs to monitor for run events. Leave empty to monitor all stacks.",
      type: {
        type: "array",
        items: { type: "string" },
      },
      required: false,
    },
    runStates: {
      name: "Run States",
      description:
        "List of run states to emit events for. Leave empty to monitor all states.",
      type: {
        type: "array",
        items: {
          enum: [
            "QUEUED",
            "INITIALIZING",
            "PLANNING",
            "UNCONFIRMED",
            "CONFIRMED",
            "APPLYING",
            "FINISHED",
            "FAILED",
            "CANCELED",
            "DISCARDED",
            "STOPPED",
          ],
        },
      },
      required: false,
    },
    runTypes: {
      name: "Run Types",
      description:
        "List of run types to emit events for. Leave empty to monitor all types.",
      type: {
        type: "array",
        items: {
          enum: ["PROPOSED", "TRACKED", "TASK", "TESTING"],
        },
      },
      required: false,
    },
  },
  onInternalMessage: async (input) => {
    try {
      const { payload } = input.message.body;

      if (!payload?.run || !payload?.stack) {
        return;
      }

      if (!shouldProcessEvent(payload, input.block.config)) {
        return;
      }

      await events.emit(payload, { outputKey: "runStateChanges" });
    } catch (error) {
      console.error("Error processing run event message:", error);
    }
  },
  outputs: {
    runStateChanges: {
      name: "Run State Changes",
      description: "All run state changes that match the configured filters",
      type: {
        type: "object",
        properties: {
          account: { type: "string" },
          state: {
            enum: [
              "QUEUED",
              "INITIALIZING",
              "PLANNING",
              "UNCONFIRMED",
              "CONFIRMED",
              "APPLYING",
              "FINISHED",
              "FAILED",
              "CANCELED",
              "DISCARDED",
              "STOPPED",
            ],
          },
          stateVersion: { type: "integer" },
          timestamp: { type: "integer" },
          timestamp_millis: { type: "integer" },
          note: { type: "string" },
          event_source: { type: "string" },
          event_type: { type: "string" },
          run: {
            type: "object",
            properties: {
              id: { type: "string" },
              branch: { type: "string" },
              command: { type: "string" },
              createdAt: { type: "integer" },
              driftDetection: { type: "boolean" },
              triggeredBy: { type: "string" },
              type: {
                enum: ["PROPOSED", "TRACKED", "TASK", "TESTING"],
              },
              url: { type: "string" },
              commit: {
                type: "object",
                properties: {
                  authorLogin: { type: "string" },
                  authorName: { type: "string" },
                  hash: { type: "string" },
                  message: { type: "string" },
                  timestamp: { type: "integer" },
                  url: { type: "string" },
                },
                required: ["authorName", "hash", "message", "timestamp", "url"],
              },
              delta: {
                type: "object",
                properties: {
                  added: { type: "integer" },
                  changed: { type: "integer" },
                  deleted: { type: "integer" },
                  resources: { type: "integer" },
                },
                required: ["added", "changed", "deleted", "resources"],
              },
            },
            required: [
              "id",
              "branch",
              "createdAt",
              "driftDetection",
              "type",
              "url",
              "commit",
            ],
          },
          stack: {
            type: "object",
            properties: {
              id: { type: "string" },
              name: { type: "string" },
              description: { type: "string" },
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
          workerPool: {
            type: "object",
            properties: {
              id: { type: "string" },
              name: { type: "string" },
            },
          },
        },
        required: [
          "account",
          "state",
          "stateVersion",
          "timestamp",
          "timestamp_millis",
          "event_source",
          "event_type",
          "run",
          "stack",
        ],
      },
    },
  },
};
