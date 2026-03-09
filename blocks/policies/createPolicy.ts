import { AppBlock, events } from "@slflows/sdk/v1";
import { executeSpaceliftQuery, extractCredentials } from "../../client";
import {
  defineSpaceliftInputConfig,
  mapInputConfig,
  mapInputsToGraphQLVariables,
} from "../../utils";

const inputConfig = {
  name: defineSpaceliftInputConfig({
    name: "Name",
    description: "Name of the policy",
    type: "string",
    required: true,
    graphqlFieldKey: "name",
  }),
  type: defineSpaceliftInputConfig({
    name: "Type",
    description: "Type of policy",
    type: {
      enum: [
        "ACCESS",
        "APPROVAL",
        "GIT_PUSH",
        "LOGIN",
        "PLAN",
        "TRIGGER",
        "NOTIFICATION",
      ],
    },
    required: true,
    graphqlFieldKey: "type",
  }),
  body: defineSpaceliftInputConfig({
    name: "Body",
    description: "Policy body (Rego code)",
    type: "string",
    required: true,
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
    description: "ID of the space this policy will belong to",
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

const CREATE_POLICY_MUTATION = `
  mutation CreatePolicy($input: PolicyCreateInput!) {
    policyCreatev2(input: $input) {
      id
      name
      type
      body
      labels
      space
      description
      createdAt
    }
  }
`;

export const createPolicy: AppBlock = {
  name: "Create Policy",
  description: "Create a new policy",
  category: "Policies",
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
          CREATE_POLICY_MUTATION,
          {
            input: graphqlVariables,
          },
        );

        await events.emit({
          id: result.policyCreatev2.id,
          name: result.policyCreatev2.name,
          type: result.policyCreatev2.type,
          body: result.policyCreatev2.body,
          labels: result.policyCreatev2.labels,
          space: result.policyCreatev2.space,
          description: result.policyCreatev2.description,
          createdAt: result.policyCreatev2.createdAt,
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
          createdAt: { type: "string" },
        },
        required: ["id", "name", "type", "body", "labels", "createdAt"],
      },
    },
  },
};
