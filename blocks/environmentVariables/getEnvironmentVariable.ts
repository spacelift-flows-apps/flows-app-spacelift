import { AppBlock, events } from "@slflows/sdk/v1";
import { executeSpaceliftQuery, extractCredentials } from "../../client";
import { defineSpaceliftInputConfig, mapInputConfig } from "../../utils";

const inputConfig = {
  targetType: defineSpaceliftInputConfig({
    name: "Target Type",
    description: "Type of entity to get the environment variable from",
    type: {
      type: "string",
      enum: ["context", "stack", "module"],
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
    description: "Environment variable name to retrieve",
    type: "string",
    required: true,
  }),
};

const GET_CONTEXT_QUERY = `
  query GetContextEnvironmentVariable($id: ID!, $key: ID!) {
    context(id: $id) {
      configElement(id: $key) {
        id
        type
        value
        checksum
        writeOnly
        description
        createdAt
      }
    }
  }
`;

const GET_STACK_QUERY = `
  query GetStackEnvironmentVariable($id: ID!, $key: ID!) {
    stack(id: $id) {
      configElement(id: $key) {
        id
        type
        value
        checksum
        writeOnly
        description
        createdAt
      }
    }
  }
`;

const GET_MODULE_QUERY = `
  query GetModuleEnvironmentVariable($id: ID!, $key: ID!) {
    module(id: $id) {
      configElement(id: $key) {
        id
        type
        value
        checksum
        writeOnly
        description
        createdAt
      }
    }
  }
`;

export const getEnvironmentVariable: AppBlock = {
  name: "Get Environment Variable",
  description: "Retrieve an environment variable from a stack or context",
  category: "Environment Variables",
  inputs: {
    default: {
      config: mapInputConfig(inputConfig),
      onEvent: async (input) => {
        const { targetType, targetId, key } = input.event.inputConfig;
        const credentials = extractCredentials(input.app.config);

        let result;
        if (targetType === "context") {
          const response = await executeSpaceliftQuery(
            credentials,
            GET_CONTEXT_QUERY,
            {
              id: targetId,
              key,
            },
          );
          result = response.context?.configElement;
        } else if (targetType === "stack") {
          const response = await executeSpaceliftQuery(
            credentials,
            GET_STACK_QUERY,
            {
              id: targetId,
              key,
            },
          );
          result = response.stack?.configElement;
        } else if (targetType === "module") {
          const response = await executeSpaceliftQuery(
            credentials,
            GET_MODULE_QUERY,
            {
              id: targetId,
              key,
            },
          );
          result = response.module?.configElement;
        } else {
          throw new Error(`Unsupported target type: ${targetType}`);
        }

        if (!result) {
          throw new Error(
            `Environment variable '${key}' not found in ${targetType} '${targetId}'`,
          );
        }

        if (result.type !== "ENVIRONMENT_VARIABLE") {
          throw new Error(
            `Config element '${key}' is not an environment variable`,
          );
        }

        await events.emit({
          targetType,
          targetId,
          key: result.id,
          value: result.writeOnly ? null : result.value,
          checksum: result.checksum,
          writeOnly: result.writeOnly,
          description: result.description,
          createdAt: result.createdAt,
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
          key: { type: "string" },
          value: { type: "string" },
          checksum: { type: "string" },
          writeOnly: { type: "boolean" },
          description: { type: "string" },
          createdAt: { type: "integer" },
        },
        required: ["targetType", "targetId", "key", "checksum", "writeOnly"],
      },
    },
  },
};
