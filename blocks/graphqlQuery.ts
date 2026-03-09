import { AppBlock, events } from "@slflows/sdk/v1";
import { executeSpaceliftQuery, extractCredentials } from "../client";
import {
  defineSpaceliftInputConfig,
  mapInputConfig,
  mapInputsToGraphQLVariables,
} from "../utils";

const inputConfig = {
  query: defineSpaceliftInputConfig({
    name: "Query",
    description:
      "The GraphQL query or mutation to execute against the Spacelift API",
    type: "string",
    required: true,
  }),
  variables: defineSpaceliftInputConfig({
    name: "Variables",
    description: "Variables to pass to the GraphQL query as a JSON object",
    type: {
      type: "object",
      additionalProperties: true,
    },
    required: false,
  }),
};

export const graphqlQuery: AppBlock = {
  name: "GraphQL Query",
  description:
    "Execute an authenticated GraphQL query or mutation against the Spacelift API. This is an escape hatch block for operations not covered by dedicated blocks.",
  category: "API",
  inputs: {
    default: {
      config: mapInputConfig(inputConfig),
      onEvent: async (input) => {
        const mapped = mapInputsToGraphQLVariables(
          inputConfig,
          input.event.inputConfig,
        );

        const credentials = extractCredentials(input.app.config);

        const result = await executeSpaceliftQuery(
          credentials,
          mapped.query,
          mapped.variables,
        );

        await events.emit(result);
      },
    },
  },
  outputs: {
    default: {
      name: "Query Result",
      description: "The data returned by the GraphQL query",
      type: {
        type: "object",
        additionalProperties: true,
      },
    },
  },
};
