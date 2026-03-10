import { AppBlock, kv } from "@slflows/sdk/v1";
import { executeSpaceliftQuery, extractCredentials } from "../../client";

const CREATE_CONTEXT_MUTATION = `
  mutation CreateContext($input: ContextInput!) {
    contextCreateV2(input: $input) {
      id
      name
      description
    }
  }
`;

const UPDATE_CONTEXT_MUTATION = `
  mutation UpdateContext($id: ID!, $input: ContextInput!) {
    contextUpdateV2(id: $id, input: $input, replaceConfigElements: true) {
      id
      name
      description
    }
  }
`;

const DELETE_CONTEXT_MUTATION = `
  mutation DeleteContext($id: ID!) {
    contextDelete(id: $id) {
      id
    }
  }
`;

export const contextResource: AppBlock = {
  name: "Context Resource",
  description: "Manages a Spacelift context resource lifecycle",
  category: "Contexts",
  config: {
    space: {
      name: "Space ID",
      description: "ID of the space this context belongs to",
      type: "string",
      required: true,
    },
    environmentVariables: {
      name: "Environment Variables",
      description:
        "Environment variables to attach to the context (key-value pairs)",
      type: {
        type: "object",
        additionalProperties: {
          type: "string",
        },
      },
      required: false,
    },
  },
  signals: {
    contextId: {
      name: "Context ID",
      description: "The ID of the managed context",
    },
  },
  onSync: async (input) => {
    const { name, description } = input.block;
    const { space, environmentVariables } = input.block.config;
    const credentials = extractCredentials(input.app.config);

    const contextId = await kv.block.get("contextId");

    try {
      const configAttachments = Object.entries(environmentVariables || {}).map(
        ([id, value]) => ({
          id,
          type: "ENVIRONMENT_VARIABLE",
          value,
          writeOnly: false,
        }),
      );

      if (contextId.value) {
        const result = await executeSpaceliftQuery(
          credentials,
          UPDATE_CONTEXT_MUTATION,
          {
            id: contextId.value,
            input: {
              name,
              description: description || null,
              space,
              configAttachments,
            },
          },
        );

        return {
          signalUpdates: {
            contextId: result.contextUpdateV2.id,
          },
          newStatus: "ready",
        };
      } else {
        const result = await executeSpaceliftQuery(
          credentials,
          CREATE_CONTEXT_MUTATION,
          {
            input: {
              name,
              description: description || null,
              space,
              configAttachments,
            },
          },
        );

        await kv.block.set({
          key: "contextId",
          value: result.contextCreateV2.id,
        });

        return {
          signalUpdates: {
            contextId: result.contextCreateV2.id,
          },
          newStatus: "ready",
        };
      }
    } catch {
      return {
        newStatus: "failed",
        customStatusDescription: "Failed to sync context",
      };
    }
  },

  onDrain: async (input) => {
    const credentials = extractCredentials(input.app.config);

    const contextId = await kv.block.get("contextId");

    if (!contextId.value) {
      return { newStatus: "drained" };
    }

    try {
      await executeSpaceliftQuery(credentials, DELETE_CONTEXT_MUTATION, {
        id: contextId.value,
      });

      await kv.block.delete(["contextId"]);

      return { newStatus: "drained" };
    } catch {
      return {
        newStatus: "draining_failed",
        customStatusDescription: "Failed to delete context",
      };
    }
  },
};
