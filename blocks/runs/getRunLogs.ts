import { AppBlock, events } from "@slflows/sdk/v1";
import { executeSpaceliftQuery, extractCredentials } from "../../client";
import {
  defineSpaceliftInputConfig,
  mapInputConfig,
  mapInputsToGraphQLVariables,
} from "../../utils";

const inputConfig = {
  stackId: defineSpaceliftInputConfig({
    name: "Stack ID",
    description: "The ID of the stack containing the run",
    type: "string",
    required: true,
    graphqlFieldKey: "stackId",
  }),
  runId: defineSpaceliftInputConfig({
    name: "Run ID",
    description: "The ID of the run to get logs for",
    type: "string",
    required: true,
    graphqlFieldKey: "id",
  }),
  state: defineSpaceliftInputConfig({
    name: "Run State",
    description: "The state for which to retrieve logs",
    type: {
      enum: [
        "QUEUED",
        "INITIALIZING",
        "PLANNING",
        "UNCONFIRMED",
        "CONFIRMED",
        "APPLYING",
        "PERFORMING",
        "PREPARING",
        "PREPARING_APPLY",
        "PREPARING_REPLAN",
        "READY",
        "FINISHED",
        "FAILED",
        "STOPPED",
        "CANCELED",
        "DISCARDED",
        "DESTROYING",
        "SKIPPED",
        "REPLAN_REQUESTED",
        "PENDING_REVIEW",
      ],
    },
    required: true,
    graphqlFieldKey: "state",
  }),
  stateVersion: defineSpaceliftInputConfig({
    name: "State Version",
    description: "The state version for which to retrieve logs",
    type: "number",
    required: false,
    graphqlFieldKey: "stateVersion",
  }),
};

const GET_RUN_LOGS_QUERY = `
  query GetRunLogs($stackId: ID!, $id: ID!, $state: RunState!, $stateVersion: Int) {
    stack(id: $stackId) {
      id
      run(id: $id) {
        id
        logs(state: $state, stateVersion: $stateVersion) {
          exists
          expired
          finished
          hasMore
          nextToken
          messages {
            timestamp
            message
          }
        }
      }
    }
  }
`;

export const getRunLogs: AppBlock = {
  name: "Get Run Logs",
  description: "Retrieve logs for a specific run and state from a stack",
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
          GET_RUN_LOGS_QUERY,
          variables,
        );

        const logs = result.stack.run.logs.messages.map((message: any) => ({
          timestamp: message.timestamp,
          message: message.message,
        }));

        await events.emit({
          stackId: result.stack.id,
          runId: result.stack.run.id,
          logs,
          logInfo: {
            exists: result.stack.run.logs.exists,
            expired: result.stack.run.logs.expired,
            finished: result.stack.run.logs.finished,
            hasMore: result.stack.run.logs.hasMore,
            nextToken: result.stack.run.logs.nextToken,
          },
        });
      },
    },
  },
  outputs: {
    default: {
      type: {
        type: "object",
        properties: {
          stackId: { type: "string" },
          runId: { type: "string" },
          logs: {
            type: "array",
            items: {
              type: "object",
              properties: {
                timestamp: { type: "number" },
                message: { type: "string" },
              },
              required: ["timestamp", "message"],
            },
          },
          logInfo: {
            type: "object",
            properties: {
              exists: { type: "boolean" },
              expired: { type: "boolean" },
              finished: { type: "boolean" },
              hasMore: { type: "boolean" },
              nextToken: { type: "string" },
            },
            required: ["exists", "expired", "finished", "hasMore"],
          },
        },
        required: ["stackId", "runId", "logs", "logInfo"],
      },
    },
  },
};
