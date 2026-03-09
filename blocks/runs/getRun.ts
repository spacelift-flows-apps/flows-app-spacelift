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
    description: "The ID of the stack that contains the run",
    type: "string",
    required: true,
    graphqlFieldKey: "stackId",
  }),
  runId: defineSpaceliftInputConfig({
    name: "Run ID",
    description: "The ID of the run to retrieve",
    type: "string",
    required: true,
    graphqlFieldKey: "runId",
  }),
};

const GET_RUN_QUERY = `
  query GetRun($stackId: ID!, $runId: ID!) {
    stack(id: $stackId) {
      id
      name
      run(id: $runId) {
        id
        createdAt
        updatedAt
        state
        type
        commit {
          hash
        }
        triggeredBy
        delta {
          addCount
          deleteCount
          changeCount
        }
      }
    }
  }
`;

export const getRun: AppBlock = {
  name: "Get Run",
  description: "Retrieve details of a specific run",
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
          GET_RUN_QUERY,
          variables,
        );

        await events.emit({
          runId: result.stack.run.id,
          stackId: result.stack.id,
          stackName: result.stack.name,
          state: result.stack.run.state,
          type: result.stack.run.type,
          commitSha: result.stack.run.commit?.hash || null,
          triggeredBy: result.stack.run.triggeredBy,
          createdAt: result.stack.run.createdAt,
          updatedAt: result.stack.run.updatedAt,
          delta: result.stack.run.delta,
        });
      },
    },
  },
  outputs: {
    default: {
      type: {
        type: "object",
        properties: {
          runId: { type: "string" },
          stackId: { type: "string" },
          stackName: { type: "string" },
          state: { type: "string" },
          type: { type: "string" },
          commitSha: { oneOf: [{ type: "string" }, { type: "null" }] },
          triggeredBy: { oneOf: [{ type: "string" }, { type: "null" }] },
          createdAt: { type: "string" },
          updatedAt: { type: "string" },
          delta: {
            oneOf: [{ type: "object" }, { type: "null" }],
            properties: {
              addCount: { type: "number" },
              deleteCount: { type: "number" },
              changeCount: { type: "number" },
              importCount: { type: "number" },
              moveCount: { type: "number" },
              resources: { type: "number" },
              forgetCount: { type: "number" },
              okCount: { type: "number" },
              skipCount: { type: "number" },
              rescueCount: { type: "number" },
              ignoreCount: { type: "number" },
              unreachableCount: { type: "number" },
              failedCount: { type: "number" },
            },
          },
        },
        required: [
          "runId",
          "stackId",
          "stackName",
          "state",
          "type",
          "createdAt",
          "updatedAt",
        ],
      },
    },
  },
};
