import { AppBlock, events } from "@slflows/sdk/v1";
import { executeSpaceliftQuery, extractCredentials } from "../../client";
import {
  defineSpaceliftInputConfig,
  mapInputConfig,
  mapInputsToGraphQLVariables,
} from "../../utils";

const inputConfig = {
  name: defineSpaceliftInputConfig({
    name: "Context Name",
    description: "Name of the context",
    type: "string",
    required: true,
    graphqlFieldKey: "name",
  }),
  description: defineSpaceliftInputConfig({
    name: "Description",
    description: "Context description",
    type: "string",
    required: false,
    graphqlFieldKey: "description",
  }),
  labels: defineSpaceliftInputConfig({
    name: "Labels",
    description: "List of labels for the context",
    type: {
      type: "array",
      items: { type: "string" },
    },
    required: false,
    graphqlFieldKey: "labels",
  }),
  space: defineSpaceliftInputConfig({
    name: "Space",
    description: "ID of the space this context belongs to",
    type: "string",
    required: true,
    graphqlFieldKey: "space",
  }),
};

const CREATE_CONTEXT_MUTATION = `
  mutation CreateContext($input: ContextInput!) {
    contextCreateV2(input: $input) {
      id
      createdAt
    }
  }
`;

export const createContext: AppBlock = {
  name: "Create Context",
  description: "Create a new context",
  category: "Contexts",
  inputs: {
    default: {
      config: mapInputConfig(inputConfig),
      onEvent: async (input) => {
        const graphqlVariables = mapInputsToGraphQLVariables(
          inputConfig,
          input.event.inputConfig,
        );

        const credentials = extractCredentials(input.app.config);

        const result = await executeSpaceliftQuery(
          credentials,
          CREATE_CONTEXT_MUTATION,
          {
            input: graphqlVariables,
          },
        );

        await events.emit({
          id: result.contextCreateV2.id,
          createdAt: result.contextCreateV2.createdAt,
        });
      },
    },
  },
  outputs: {
    default: {
      type: {
        type: "object",
        properties: {
          id: { type: "string" },
          createdAt: { type: "string" },
        },
        required: ["contextId", "createdAt"],
      },
    },
  },
};
