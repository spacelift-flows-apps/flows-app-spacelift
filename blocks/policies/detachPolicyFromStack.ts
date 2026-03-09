import { AppBlock, events } from "@slflows/sdk/v1";
import { executeSpaceliftQuery, extractCredentials } from "../../client";
import {
  defineSpaceliftInputConfig,
  mapInputConfig,
  mapInputsToGraphQLVariables,
} from "../../utils";

const inputConfig = {
  attachmentId: defineSpaceliftInputConfig({
    name: "Attachment ID",
    description: "The ID of the policy attachment to detach",
    type: "string",
    required: true,
    graphqlFieldKey: "attachmentId",
  }),
};

const DETACH_POLICY_MUTATION = `
  mutation DetachPolicy($attachmentId: ID!) {
    policyDetach(id: $attachmentId) {
      id
      stackId
      stackName
      isModule
    }
  }
`;

export const detachPolicyFromStack: AppBlock = {
  name: "Detach Policy from Stack",
  description: "Detach a policy from a stack using the attachment ID",
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
          DETACH_POLICY_MUTATION,
          variables,
        );

        await events.emit({
          attachmentId: result.policyDetach.id,
          stackId: result.policyDetach.stackId,
          stackName: result.policyDetach.stackName,
          isModule: result.policyDetach.isModule,
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
          stackId: { type: "string" },
          stackName: { type: "string" },
          isModule: { type: "boolean" },
        },
        required: ["attachmentId", "stackId", "stackName", "isModule"],
      },
    },
  },
};
