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
    description: "The ID of the policy to retrieve",
    type: "string",
    required: true,
    graphqlFieldKey: "id",
  }),
};

const GET_POLICY_QUERY = `
  query GetPolicy($id: ID!) {
    policy(id: $id) {
      id
      name
      type
      body
      labels
      createdAt
      updatedAt
    }
  }
`;

export const getPolicy: AppBlock = {
  name: "Get Policy",
  description: "Retrieve details of a specific policy",
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
          GET_POLICY_QUERY,
          variables,
        );

        await events.emit({
          id: result.policy.id,
          name: result.policy.name,
          type: result.policy.type,
          body: result.policy.body,
          labels: result.policy.labels,
          createdAt: result.policy.createdAt,
          updatedAt: result.policy.updatedAt,
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
          name: { type: "string" },
          type: { type: "string" },
          body: { type: "string" },
          labels: {
            type: "array",
            items: { type: "string" },
          },
          createdAt: { type: "string" },
          updatedAt: { type: "string" },
        },
        required: [
          "id",
          "name",
          "type",
          "body",
          "labels",
          "createdAt",
          "updatedAt",
        ],
      },
    },
  },
};
