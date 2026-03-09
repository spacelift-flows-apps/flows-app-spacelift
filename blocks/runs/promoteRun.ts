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
    description: "The ID of the proposed run to promote",
    type: "string",
    required: true,
    graphqlFieldKey: "run",
  }),
};

const PROMOTE_RUN_MUTATION = `
  mutation PromoteRun($stack: ID!, $run: ID!) {
    runPromote(stack: $stack, run: $run) {
      id
      state
      title
      type
      createdAt
      updatedAt
    }
  }
`;

export const promoteRun: AppBlock = {
  name: "Promote Run",
  description: "Promote a proposed run to a tracked one",
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
          PROMOTE_RUN_MUTATION,
          variables,
        );

        await events.emit({
          runId: result.runPromote.id,
          state: result.runPromote.state,
          title: result.runPromote.title,
          type: result.runPromote.type,
          createdAt: result.runPromote.createdAt,
          updatedAt: result.runPromote.updatedAt,
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
