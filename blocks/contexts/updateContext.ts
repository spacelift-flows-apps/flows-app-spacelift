import { AppBlock, events } from "@slflows/sdk/v1";
import { executeSpaceliftQuery, extractCredentials } from "../../client";
import {
  defineSpaceliftInputConfig,
  mapInputConfig,
  mapInputsToGraphQLVariables,
} from "../../utils";

const inputConfig = {
  id: defineSpaceliftInputConfig({
    name: "Context ID",
    description: "The ID of the context to update",
    type: "string",
    required: true,
    graphqlFieldKey: "id",
  }),
  name: defineSpaceliftInputConfig({
    name: "Name",
    description: "New name for the context",
    type: "string",
    required: false,
    graphqlFieldKey: "name",
  }),
  description: defineSpaceliftInputConfig({
    name: "Description",
    description: "New description for the context",
    type: "string",
    required: false,
    graphqlFieldKey: "description",
  }),
  labels: defineSpaceliftInputConfig({
    name: "Labels",
    description:
      "List of labels for the context (will replace all existing labels)",
    type: {
      type: "array",
      items: { type: "string" },
    },
    required: false,
    graphqlFieldKey: "labels",
  }),
};

const UPDATE_CONTEXT_MUTATION = `
  mutation UpdateContext($id: ID!, $input: ContextInput!) {
    contextUpdateV2(id: $id, input: $input) {
      id
      updatedAt
    }
  }
`;

export const updateContext: AppBlock = {
  name: "Update Context",
  description: "Update an existing context.",
  category: "Contexts",
  inputs: {
    default: {
      config: mapInputConfig(inputConfig),
      onEvent: async (input) => {
        const { id, ...updateInput } = mapInputsToGraphQLVariables(
          inputConfig,
          input.event.inputConfig,
        );

        const credentials = extractCredentials(input.app.config);
        const result = await executeSpaceliftQuery(
          credentials,
          UPDATE_CONTEXT_MUTATION,
          {
            id,
            input: updateInput,
          },
        );

        await events.emit({
          id: result.contextUpdateV2.id,
          updatedAt: result.contextUpdateV2.updatedAt,
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
          updatedAt: { type: "string" },
        },
        required: ["id", "updatedAt"],
      },
    },
  },
};
