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
    description: "The ID of the policy to update",
    type: "string",
    required: true,
    graphqlFieldKey: "id",
  }),
  name: defineSpaceliftInputConfig({
    name: "Name",
    description: "New name for the policy",
    type: "string",
    required: false,
    graphqlFieldKey: "name",
  }),
  body: defineSpaceliftInputConfig({
    name: "Body",
    description: "New policy body (Rego code)",
    type: "string",
    required: false,
    graphqlFieldKey: "body",
  }),
  labels: defineSpaceliftInputConfig({
    name: "Labels",
    description: "List of labels for the policy",
    type: {
      type: "array",
      items: { type: "string" },
    },
    required: false,
    graphqlFieldKey: "labels",
  }),
  space: defineSpaceliftInputConfig({
    name: "Space ID",
    description: "ID of the space this policy belongs to",
    type: "string",
    required: false,
    graphqlFieldKey: "space",
  }),
  description: defineSpaceliftInputConfig({
    name: "Description",
    description: "Description of the policy",
    type: "string",
    required: false,
    graphqlFieldKey: "description",
  }),
};

const UPDATE_POLICY_MUTATION = `
  mutation UpdatePolicy($id: ID!, $input: PolicyUpdateInput!) {
    policyUpdatev2(id: $id, input: $input) {
      id
      name
      type
      body
      labels
      space
      description
      updatedAt
    }
  }
`;

export const updatePolicy: AppBlock = {
  name: "Update Policy",
  description: "Update an existing policy",
  category: "Policies",
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
          UPDATE_POLICY_MUTATION,
          {
            id,
            input: updateInput,
          },
        );

        await events.emit({
          id: result.policyUpdatev2.id,
          name: result.policyUpdatev2.name,
          type: result.policyUpdatev2.type,
          body: result.policyUpdatev2.body,
          labels: result.policyUpdatev2.labels,
          space: result.policyUpdatev2.space,
          description: result.policyUpdatev2.description,
          updatedAt: result.policyUpdatev2.updatedAt,
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
          space: { oneOf: [{ type: "string" }, { type: "null" }] },
          description: { oneOf: [{ type: "string" }, { type: "null" }] },
          updatedAt: { type: "string" },
        },
        required: ["id", "name", "type", "body", "labels", "updatedAt"],
      },
    },
  },
};
