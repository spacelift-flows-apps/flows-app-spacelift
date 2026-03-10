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
  contextId: defineSpaceliftInputConfig({
    name: "Context ID",
    description: "The ID of the context to attach",
    type: "string",
    required: true,
    graphqlFieldKey: "contextId",
  }),
  priority: defineSpaceliftInputConfig({
    name: "Priority",
    description:
      "Priority of the context attachment (higher number = higher priority)",
    type: "number",
    required: true,
    graphqlFieldKey: "priority",
  }),
};

const ATTACH_CONTEXT_MUTATION = `
  mutation AttachContext($stackId: ID!, $contextId: ID!, $priority: Int!) {
    contextAttach(id: $contextId, stack: $stackId, priority: $priority) {
      id
      priority
    }
  }
`;

export const attachContextToStack: AppBlock = {
  name: "Attach Context to Stack",
  description: "Attach a context to a stack",
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
          ATTACH_CONTEXT_MUTATION,
          variables,
        );

        await events.emit({
          attachmentId: result.contextAttach.id,
          contextId: variables.contextId,
          stackId: variables.stackId,
          priority: result.contextAttach.priority,
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
          contextId: { type: "string" },
          stackId: { type: "string" },
          priority: { type: "number" },
        },
        required: ["attachmentId", "contextId", "stackId", "priority"],
      },
    },
  },
};
