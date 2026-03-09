import { AppBlock, events } from "@slflows/sdk/v1";
import { executeSpaceliftQuery, extractCredentials } from "../../client";
import {
  defineSpaceliftInputConfig,
  mapInputConfig,
  mapInputsToGraphQLVariables,
} from "../../utils";

const inputConfig = {
  contextId: defineSpaceliftInputConfig({
    name: "Context ID",
    description: "The ID of the context to delete",
    type: "string",
    required: true,
    graphqlFieldKey: "id",
  }),
  force: defineSpaceliftInputConfig({
    name: "Force Delete",
    description: "Force deletion even if the context has dependencies",
    type: "boolean",
    required: false,
    graphqlFieldKey: "force",
  }),
};

const DELETE_CONTEXT_MUTATION = `
  mutation DeleteContext($id: ID!, $force: Boolean) {
    contextDelete(id: $id, force: $force) {
      id
    }
  }
`;

export const deleteContext: AppBlock = {
  name: "Delete Context",
  description: "Delete a context",
  category: "Contexts",
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
          DELETE_CONTEXT_MUTATION,
          variables,
        );

        await events.emit({
          id: result.contextDelete.id,
        });
      },
    },
  },
  outputs: {
    default: {
      type: {
        type: "object",
        properties: {
          contextId: { type: "string" },
        },
        required: ["id"],
      },
    },
  },
};
