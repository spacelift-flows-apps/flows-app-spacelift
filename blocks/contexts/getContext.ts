import { AppBlock, events } from "@slflows/sdk/v1";
import { executeSpaceliftQuery, extractCredentials } from "../../client";
import {
  defineSpaceliftInputConfig,
  mapInputConfig,
  mapInputsToGraphQLVariables,
} from "../../utils";

const inputConfig = {
  contextId: defineSpaceliftInputConfig({
    name: "Context ID",
    description: "The ID of the context to retrieve",
    type: "string",
    required: true,
    graphqlFieldKey: "id",
  }),
};

const GET_CONTEXT_QUERY = `
  query GetContext($id: ID!) {
    context(id: $id) {
      id
      name
      description
      labels
      createdAt
      updatedAt
      space
      attachedStacks {
        id
        stackId
        stackName
        priority
        isModule
      }
      config {
        id
        type
        checksum
        value
        description
        writeOnly
      }
    }
  }
`;

export const getContext: AppBlock = {
  name: "Get Context",
  description: "Retrieve details of a specific context",
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
          GET_CONTEXT_QUERY,
          variables,
        );

        await events.emit({
          contextId: result.context.id,
          name: result.context.name,
          description: result.context.description,
          labels: result.context.labels,
          createdAt: result.context.createdAt,
          updatedAt: result.context.updatedAt,
          space: result.context.space,
          attachedStacks: result.context.attachedStacks,
          configElements: result.context.config,
        });
      },
    },
  },
  outputs: {
    default: {
      type: {
        type: "object",
        properties: {
          contextId: { type: "string" },
          name: { type: "string" },
          description: { oneOf: [{ type: "string" }, { type: "null" }] },
          labels: {
            type: "array",
            items: { type: "string" },
          },
          createdAt: { type: "string" },
          updatedAt: { type: "string" },
          space: { oneOf: [{ type: "string" }, { type: "null" }] },
          attachedStacks: {
            type: "array",
            items: {
              type: "object",
              properties: {
                id: { type: "string" },
                stackId: { type: "string" },
                stackName: { type: "string" },
                priority: { type: "number" },
                isModule: { type: "boolean" },
              },
              required: ["id", "stackId", "stackName", "priority", "isModule"],
            },
          },
          configElements: {
            type: "array",
            items: {
              type: "object",
              properties: {
                id: { type: "string" },
                type: { type: "string" },
                checksum: { type: "string" },
                value: { oneOf: [{ type: "string" }, { type: "null" }] },
                description: { type: "string" },
                writeOnly: { type: "boolean" },
              },
              required: ["id", "type", "checksum", "description", "writeOnly"],
            },
          },
        },
        required: ["contextId", "name", "labels", "createdAt", "updatedAt"],
      },
    },
  },
};
