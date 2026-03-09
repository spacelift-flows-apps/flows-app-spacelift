import { AppBlock, events } from "@slflows/sdk/v1";
import { executeSpaceliftQuery, extractCredentials } from "../../client";
import {
  defineSpaceliftInputConfig,
  mapInputConfig,
  mapInputsToGraphQLVariables,
} from "../../utils";

const inputConfig = {
  id: defineSpaceliftInputConfig({
    name: "Module ID",
    description: "The ID of the module to delete",
    type: "string",
    required: true,
    graphqlFieldKey: "id",
  }),
};

const DELETE_MODULE_MUTATION = `
  mutation DeleteModule($id: ID!) {
    moduleDelete(id: $id) {
      id
    }
  }
`;

export const deleteModule: AppBlock = {
  name: "Delete Module",
  description: "Delete a module",
  category: "Modules",
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
          DELETE_MODULE_MUTATION,
          variables,
        );

        await events.emit({
          moduleId: result.moduleDelete.id,
        });
      },
    },
  },
  outputs: {
    default: {
      type: {
        type: "object",
        properties: {
          moduleId: { type: "string" },
        },
        required: ["moduleId"],
      },
    },
  },
};
