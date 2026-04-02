import { AppBlock, events, http, kv } from "@slflows/sdk/v1";
import { executeSpaceliftQuery, extractCredentials } from "../../client";

const CREATE_WEBHOOK_MUTATION = `
  mutation CreateWebhook($stack: ID!, $input: WebhooksIntegrationInput!) {
    webhooksIntegrationCreate(stack: $stack, input: $input) {
      id
    }
  }
`;

const UPDATE_WEBHOOK_MUTATION = `
  mutation UpdateWebhook($stack: ID!, $id: ID!, $input: WebhooksIntegrationInput!) {
    webhooksIntegrationUpdate(stack: $stack, id: $id, input: $input) {
      id
    }
  }
`;

const DELETE_WEBHOOK_MUTATION = `
  mutation DeleteWebhook($stack: ID!, $id: ID!) {
    webhooksIntegrationDelete(stack: $stack, id: $id) {
      id
    }
  }
`;

export const webhook: AppBlock = {
  name: "Stack Webhook",
  description: "Manages webhook integration for a Spacelift stack",
  category: "Stacks",
  entrypoint: true,
  config: {
    stackId: {
      name: "Stack ID",
      description: "The ID of the Spacelift stack",
      type: "string",
      required: true,
    },
    enabled: {
      name: "Enable Webhook",
      description: "Whether the webhook should be enabled",
      type: "boolean",
      required: false,
      default: true,
    },
  },
  signals: {
    webhookId: {
      name: "Webhook Integration ID",
      description: "The ID of the webhook integration",
    },
  },
  onSync: async (input) => {
    const { stackId, enabled = true } = input.block.config;
    const credentials = extractCredentials(input.app.config);

    if (!input.block.http?.url) {
      return {
        newStatus: "failed",
        customStatusDescription: "Internal error",
      };
    }

    const webhookEndpoint = input.block.http.url;

    const webhookIdKV = await kv.block.get("webhookId");

    try {
      if (webhookIdKV.value) {
        const result = await executeSpaceliftQuery(
          credentials,
          UPDATE_WEBHOOK_MUTATION,
          {
            stack: stackId,
            id: webhookIdKV.value,
            input: {
              endpoint: webhookEndpoint,
              enabled,
            },
          },
        );

        return {
          signalUpdates: {
            webhookId: result.webhooksIntegrationUpdate.id,
          },
          newStatus: "ready",
        };
      } else {
        const result = await executeSpaceliftQuery(
          credentials,
          CREATE_WEBHOOK_MUTATION,
          {
            stack: stackId,
            input: {
              endpoint: webhookEndpoint,
              enabled,
            },
          },
        );

        await kv.block.set({
          key: "webhookId",
          value: result.webhooksIntegrationCreate.id,
        });

        return {
          signalUpdates: {
            webhookId: result.webhooksIntegrationCreate.id,
          },
          newStatus: "ready",
        };
      }
    } catch (error) {
      console.error(error);

      return {
        newStatus: "failed",
        customStatusDescription: "Failed to sync webhook",
      };
    }
  },

  onDrain: async (input) => {
    const { stackId } = input.block.config;
    const credentials = extractCredentials(input.app.config);

    const webhookIdKV = await kv.block.get("webhookId");

    if (!webhookIdKV.value) {
      return { newStatus: "drained" };
    }

    try {
      await executeSpaceliftQuery(credentials, DELETE_WEBHOOK_MUTATION, {
        stack: stackId,
        id: webhookIdKV.value,
      });

      await kv.block.delete(["webhookId"]);

      return { newStatus: "drained" };
    } catch (error) {
      console.error(error);

      return {
        newStatus: "draining_failed",
        customStatusDescription: "Failed to delete webhook",
      };
    }
  },

  http: {
    onRequest: async (input) => {
      const webhookData = input.request.body;

      await events.emit({
        payload: webhookData,
      });

      await http.respond(input.request.requestId, {
        statusCode: 200,
        body: { message: "Webhook received successfully" },
      });
    },
  },

  outputs: {
    default: {
      name: "Spacelift Webhook Event",
      description: "Emitted when a Spacelift webhook is received",
      type: {
        type: "object",
        properties: {
          payload: {
            type: "object",
            description: "The webhook payload from Spacelift",
          },
        },
        required: ["payload"],
      },
    },
  },
};
