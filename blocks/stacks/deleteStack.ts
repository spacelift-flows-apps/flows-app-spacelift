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
    description: "The ID of the stack to delete",
    type: "string",
    required: true,
    graphqlFieldKey: "id",
  }),
  destroyResources: defineSpaceliftInputConfig({
    name: "Destroy Resources",
    description: "Whether to destroy the stack's resources before deletion",
    type: "boolean",
    required: false,
    graphqlFieldKey: "destroyResources",
  }),
};

const DELETE_STACK_MUTATION = `
  mutation DeleteStack($id: ID!, $destroyResources: Boolean) {
    stackDelete(id: $id, destroyResources: $destroyResources) {
      id
      name
    }
  }
`;

export const deleteStack: AppBlock = {
  name: "Delete Stack",
  description: "Delete a stack",
  category: "Stacks",
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
          DELETE_STACK_MUTATION,
          variables,
        );

        await events.emit({
          stackId: result.stackDelete.id,
          stackName: result.stackDelete.name,
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
          stackName: { type: "string" },
        },
        required: ["stackId", "stackName"],
      },
    },
  },
};
