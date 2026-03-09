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
    graphqlFieldKey: "stack",
  }),
  runId: defineSpaceliftInputConfig({
    name: "Run ID",
    description: "The ID of the PR run to retry",
    type: "string",
    required: true,
    graphqlFieldKey: "run",
  }),
};

const RETRY_RUN_MUTATION = `
  mutation RetryRun($stack: ID!, $run: ID!) {
    runRetry(stack: $stack, run: $run) {
      id
      state
      title
      type
      createdAt
      updatedAt
    }
  }
`;

export const retryRun: AppBlock = {
  name: "Retry Run",
  description: "Retry a PR run that has finished",
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
          RETRY_RUN_MUTATION,
          variables,
        );

        await events.emit({
          runId: result.runRetry.id,
          state: result.runRetry.state,
          title: result.runRetry.title,
          type: result.runRetry.type,
          createdAt: result.runRetry.createdAt,
          updatedAt: result.runRetry.updatedAt,
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
          state: { type: "string" },
          title: { type: "string" },
          type: { type: "string" },
          createdAt: { type: "number" },
          updatedAt: { type: "number" },
        },
        required: ["runId", "state", "title", "type", "createdAt", "updatedAt"],
      },
    },
  },
};
