import { AppBlock, events } from "@slflows/sdk/v1";
import { executeSpaceliftQuery, extractCredentials } from "../../client";
import { defineSpaceliftInputConfig, mapInputConfig } from "../../utils";

const inputConfig = {
  targetType: defineSpaceliftInputConfig({
    name: "Target Type",
    description: "Type of entity to delete the environment variable from",
    type: {
      type: "string",
      enum: ["context", "stack"],
    },
    required: true,
  }),
  targetId: defineSpaceliftInputConfig({
    name: "Target ID",
    description: "ID of the context or stack",
    type: "string",
    required: true,
  }),
  key: defineSpaceliftInputConfig({
    name: "Key",
    description: "Environment variable name to delete",
    type: "string",
    required: true,
  }),
};

const DELETE_CONTEXT_ENV_VAR_MUTATION = `
  mutation DeleteContextEnvironmentVariable($context: ID!, $id: ID!) {
    contextConfigDelete(context: $context, id: $id) {
      id
    }
  }
`;

const DELETE_STACK_ENV_VAR_MUTATION = `
  mutation DeleteStackEnvironmentVariable($stack: ID!, $id: ID!) {
    stackConfigDelete(stack: $stack, id: $id) {
      id
    }
  }
`;

export const deleteEnvironmentVariable: AppBlock = {
  name: "Delete Environment Variable",
  description: "Delete an environment variable from a stack or context",
  category: "Environment Variables",
  inputs: {
    default: {
      config: mapInputConfig(inputConfig),
      onEvent: async (input) => {
        const { targetType, targetId, key } = input.event.inputConfig;
        const credentials = extractCredentials(input.app.config);

        let result;
        if (targetType === "context") {
          result = await executeSpaceliftQuery(
            credentials,
            DELETE_CONTEXT_ENV_VAR_MUTATION,
            {
              context: targetId,
              id: key,
            },
          );
          result = result.contextConfigDelete;
        } else if (targetType === "stack") {
          result = await executeSpaceliftQuery(
            credentials,
            DELETE_STACK_ENV_VAR_MUTATION,
            {
              stack: targetId,
              id: key,
            },
          );
          result = result.stackConfigDelete;
        } else {
          throw new Error(`Unsupported target type: ${targetType}`);
        }

        await events.emit({
          targetType,
          targetId,
          id: result.id,
        });
      },
    },
  },
  outputs: {
    default: {
      type: {
        type: "object",
        properties: {
          targetType: { type: "string" },
          targetId: { type: "string" },
          id: { type: "string" },
        },
        required: ["targetType", "targetId", "id"],
      },
    },
  },
};
