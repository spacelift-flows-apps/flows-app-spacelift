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
    description: "The ID of the stack",
    type: "string",
    required: true,
    graphqlFieldKey: "stackId",
  }),
  policyId: defineSpaceliftInputConfig({
    name: "Policy ID",
    description: "The ID of the policy to attach",
    type: "string",
    required: true,
    graphqlFieldKey: "policyId",
  }),
};

const ATTACH_POLICY_MUTATION = `
  mutation AttachPolicy($stackId: ID!, $policyId: ID!) {
    policyAttach(id: $policyId, stack: $stackId) {
      id
      stackId
      stackName
      isModule
    }
  }
`;

export const attachPolicyToStack: AppBlock = {
  name: "Attach Policy to Stack",
  description: "Attach a policy to a stack",
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
          ATTACH_POLICY_MUTATION,
          variables,
        );

        await events.emit({
          attachmentId: result.policyAttach.id,
          policyId: variables.policyId,
          stackId: result.policyAttach.stackId,
          stackName: result.policyAttach.stackName,
          isModule: result.policyAttach.isModule,
        });
      },
    },
  },
  outputs: {
    default: {
      type: {
        type: "object",
        properties: {
          attachmentId: { type: "string" },
          policyId: { type: "string" },
          stackId: { type: "string" },
          stackName: { type: "string" },
          isModule: { type: "boolean" },
        },
        required: [
          "attachmentId",
          "policyId",
          "stackId",
          "stackName",
          "isModule",
        ],
      },
    },
  },
};
