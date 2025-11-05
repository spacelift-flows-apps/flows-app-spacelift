import { AppBlock, events } from "@slflows/sdk/v1";
import { executeSpaceliftQuery, extractCredentials } from "../../client";
import {
  defineSpaceliftInputConfig,
  mapInputConfig,
  mapInputsToGraphQLVariables,
} from "../../utils";

const inputConfig = {
  blueprintId: defineSpaceliftInputConfig({
    name: "Blueprint ID",
    description: "ID of the blueprint to use for stack creation",
    type: "string",
    required: true,
    graphqlFieldKey: "id",
  }),
  inputs: defineSpaceliftInputConfig({
    name: "Inputs",
    description: "Key-value pairs for blueprint template inputs",
    type: {
      type: "object",
      additionalProperties: { type: "string" },
    },
    required: false,
    graphqlFieldKey: "templateInputs",
  }),
};

const CREATE_STACK_FROM_BLUEPRINT_MUTATION = `
  mutation CreateStackFromBlueprint($id: ID!, $input: BlueprintStackCreateInput!) {
    blueprintCreateStack(id: $id, input: $input) {
      stackIds
      runIds
    }
  }
`;

export const createStackFromBlueprint: AppBlock = {
  name: "Create stack from blueprint",
  description: "Create a new Spacelift stack from a blueprint template",
  category: "Stacks",
  inputs: {
    default: {
      config: mapInputConfig(inputConfig),
      onEvent: async (input) => {
        const mappedInputs = mapInputsToGraphQLVariables(
          inputConfig,
          input.event.inputConfig,
        );

        // Transform the inputs object into an array of BlueprintStackCreateInputPair
        const templateInputs = mappedInputs.templateInputs
          ? Object.entries(mappedInputs.templateInputs).map(([id, value]) => ({
              id,
              value: String(value),
            }))
          : [];

        const createVariables = {
          id: mappedInputs.id,
          input: {
            templateInputs,
          },
        };

        const credentials = extractCredentials(input.app.config);

        const result = await executeSpaceliftQuery(
          credentials,
          CREATE_STACK_FROM_BLUEPRINT_MUTATION,
          createVariables,
        );

        await events.emit({
          stackIds: result.blueprintCreateStack.stackIds,
          runIds: result.blueprintCreateStack.runIds,
        });
      },
    },
  },
  outputs: {
    default: {
      type: {
        type: "object",
        properties: {
          stackIds: {
            type: "array",
            items: { type: "string" },
          },
          runIds: {
            type: "array",
            items: { type: "string" },
          },
        },
        required: ["stackIds", "runIds"],
      },
    },
  },
};
