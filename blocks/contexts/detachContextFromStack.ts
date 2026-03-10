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
    description:
      "The ID of the context attachment to detach (get this from the attach context output or stack query)",
    type: "string",
    required: true,
    graphqlFieldKey: "attachmentId",
  }),
};

const DETACH_CONTEXT_MUTATION = `
  mutation DetachContext($attachmentId: ID!) {
    contextDetach(id: $attachmentId) {
      id
    }
  }
`;

export const detachContextFromStack: AppBlock = {
  name: "Detach Context from Stack",
  description: "Detach a context from a stack using the attachment ID",
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
          DETACH_CONTEXT_MUTATION,
          variables,
        );

        await events.emit({
          attachmentId: result.contextDetach.id,
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
        },
        required: ["attachmentId"],
      },
    },
  },
};
