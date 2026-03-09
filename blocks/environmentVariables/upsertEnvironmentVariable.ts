import { AppBlock, events } from "@slflows/sdk/v1";
import { executeSpaceliftQuery, extractCredentials } from "../../client";
import {
  defineSpaceliftInputConfig,
  mapInputConfig,
  mapInputsToGraphQLVariables,
} from "../../utils";

const inputConfig = {
  targetType: defineSpaceliftInputConfig({
    name: "Target Type",
    description:
      "Type of entity to attach the environment variable to (use 'stack' for both stacks and modules)",
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
    description: "Environment variable name",
    type: "string",
    required: true,
    graphqlFieldKey: "id",
  }),
  value: defineSpaceliftInputConfig({
    name: "Value",
    description: "Environment variable value",
    type: "string",
    required: true,
    graphqlFieldKey: "value",
  }),
  description: defineSpaceliftInputConfig({
    name: "Description",
    description: "Optional description for the environment variable",
    type: "string",
    required: false,
    graphqlFieldKey: "description",
  }),
  writeOnly: defineSpaceliftInputConfig({
    name: "Write Only",
    description:
      "When marked as write-only, the value is only accessible to the Run, but not in the API or UI",
    type: "boolean",
    required: false,
    default: false,
    graphqlFieldKey: "writeOnly",
  }),
};

const UPSERT_CONTEXT_ENV_VAR_MUTATION = `
  mutation UpsertContextEnvironmentVariable($context: ID!, $config: ConfigInput!) {
    contextConfigAdd(context: $context, config: $config) {
      id
      type
      checksum
      writeOnly
      description
    }
  }
`;

const UPSERT_STACK_ENV_VAR_MUTATION = `
  mutation UpsertStackEnvironmentVariable($stack: ID!, $config: ConfigInput!) {
    stackConfigAdd(stack: $stack, config: $config) {
      id
      type
      checksum
      writeOnly
      description
    }
  }
`;

export const upsertEnvironmentVariable: AppBlock = {
  name: "Upsert Environment Variable",
  description:
    "Create a new environment variable or update an existing one for a stack or context",
  category: "Environment Variables",
  inputs: {
    default: {
      config: mapInputConfig(inputConfig),
      onEvent: async (input) => {
        const inputs = input.event.inputConfig;
        const targetType = inputs.targetType;
        const targetId = inputs.targetId;

        const configInput = mapInputsToGraphQLVariables(inputConfig, inputs);
        configInput.type = "ENVIRONMENT_VARIABLE";
        delete configInput.targetType;
        delete configInput.targetId;

        const credentials = extractCredentials(input.app.config);

        let result;
        if (targetType === "context") {
          result = await executeSpaceliftQuery(
            credentials,
            UPSERT_CONTEXT_ENV_VAR_MUTATION,
            {
              context: targetId,
              config: configInput,
            },
          );
          result = result.contextConfigAdd;
        } else if (targetType === "stack") {
          result = await executeSpaceliftQuery(
            credentials,
            UPSERT_STACK_ENV_VAR_MUTATION,
            {
              stack: targetId,
              config: configInput,
            },
          );
          result = result.stackConfigAdd;
        } else {
          throw new Error(`Unsupported target type: ${targetType}`);
        }

        await events.emit({
          targetType,
          targetId,
          key: result.id,
          type: result.type,
          checksum: result.checksum,
          writeOnly: result.writeOnly,
          description: result.description,
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
          type: { type: "string" },
          checksum: { type: "string" },
          writeOnly: { type: "boolean" },
          description: { type: "string" },
        },
        required: ["targetType", "targetId", "key", "type", "checksum"],
      },
    },
  },
};
