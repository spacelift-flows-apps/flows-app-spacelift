import { AppBlock, events } from "@slflows/sdk/v1";
import { executeSpaceliftQuery, extractCredentials } from "../../client";
import {
  defineSpaceliftInputConfig,
  mapInputConfig,
  mapInputsToGraphQLVariables,
} from "../../utils";

const inputConfig = {
  id: defineSpaceliftInputConfig({
    name: "Policy ID",
    description: "The ID of the policy to delete",
    type: "string",
    required: true,
    graphqlFieldKey: "id",
  }),
  force: defineSpaceliftInputConfig({
    name: "Force Delete",
    description: "Force delete the policy even if it has attached stacks",
    type: "boolean",
    required: false,
    graphqlFieldKey: "force",
  }),
};

const DELETE_POLICY_MUTATION = `
  mutation DeletePolicy($id: ID!, $force: Boolean) {
    policyDelete(id: $id, force: $force) {
      id
    }
  }
`;

export const deletePolicy: AppBlock = {
  name: "Delete Policy",
  description: "Delete a policy",
  category: "Policies",
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
          DELETE_POLICY_MUTATION,
          variables,
        );

        await events.emit({
          id: result.policyDelete.id,
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
        },
        required: ["id"],
      },
    },
  },
};
