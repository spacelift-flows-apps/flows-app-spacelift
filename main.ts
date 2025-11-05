import { defineApp, http, kv, messaging, blocks } from "@slflows/sdk/v1";
import { createHmac, timingSafeEqual, randomBytes } from "node:crypto";
import { executeSpaceliftQuery, extractCredentials } from "./client";
import { webhook } from "./blocks/stacks/webhook";
import { getStack } from "./blocks/stacks/getStack";
import { lockStack } from "./blocks/stacks/lockStack";
import { unlockStack } from "./blocks/stacks/unlockStack";
import { getContext } from "./blocks/contexts/getContext";
import { getRun } from "./blocks/runs/getRun";
import { getRunLogs } from "./blocks/runs/getRunLogs";
import { startRun } from "./blocks/runs/startRun";
import { attachContextToStack } from "./blocks/contexts/attachContextToStack";
import { detachContextFromStack } from "./blocks/contexts/detachContextFromStack";
import { createPolicy } from "./blocks/policies/createPolicy";
import { deletePolicy } from "./blocks/policies/deletePolicy";
import { updatePolicy } from "./blocks/policies/updatePolicy";
import { getPolicy } from "./blocks/policies/getPolicy";
import { simulatePolicy } from "./blocks/policies/simulatePolicy";
import { getPolicyEvaluations } from "./blocks/policies/getPolicyEvaluations";
import { attachPolicyToStack } from "./blocks/policies/attachPolicyToStack";
import { detachPolicyFromStack } from "./blocks/policies/detachPolicyFromStack";
import { markExternalDependency } from "./blocks/dependencies/markExternalDependency";
import { createContext } from "./blocks/contexts/createContext";
import { updateContext } from "./blocks/contexts/updateContext";
import { contextResource } from "./blocks/contexts/contextResource";
import { deleteContext } from "./blocks/contexts/deleteContext";
import { getEnvironmentVariable } from "./blocks/environmentVariables/getEnvironmentVariable";
import { upsertEnvironmentVariable } from "./blocks/environmentVariables/upsertEnvironmentVariable";
import { deleteEnvironmentVariable } from "./blocks/environmentVariables/deleteEnvironmentVariable";
import { getModule } from "./blocks/modules/getModule";
import { updateModule } from "./blocks/modules/updateModule";
import { deleteModule } from "./blocks/modules/deleteModule";
import { createModule } from "./blocks/modules/createModule";
import { createStack } from "./blocks/stacks/createStack";
import { createStackFromBlueprint } from "./blocks/stacks/createStackFromBlueprint";
import { updateStack } from "./blocks/stacks/updateStack";
import { deleteStack } from "./blocks/stacks/deleteStack";
import { addComment } from "./blocks/runs/addComment";
import { discardRun } from "./blocks/runs/discardRun";
import { retryRun } from "./blocks/runs/retryRun";
import { stopRun } from "./blocks/runs/stopRun";
import { confirmRun } from "./blocks/runs/confirmRun";
import { approveRun } from "./blocks/runs/approveRun";
import { rejectRun } from "./blocks/runs/rejectRun";
import { promoteRun } from "./blocks/runs/promoteRun";
import { triggerRun } from "./blocks/runs/triggerRun";
import { onRunStateChange } from "./blocks/runs/subscribeToRunEvents";
import { performTask } from "./blocks/tasks/performTask";

export const app = defineApp({
  name: "Spacelift",
  installationInstructions:
    "To connect your Spacelift account:\n1. **Get API Key**: Log in to your Spacelift account and generate an API key\n2. **Configure**: Enter your API Key ID, API Key Secret, and Spacelift endpoint (e.g., 'your-account.app.spacelift.io')\n3. **Confirm**: Click 'Confirm' to complete the installation",
  config: {
    apiKeyId: {
      name: "API Key ID",
      description: "Your Spacelift API key ID",
      type: "string",
      required: true,
    },
    apiKeySecret: {
      name: "API Key Secret",
      description: "Your Spacelift API key secret",
      type: "string",
      required: true,
      sensitive: true,
    },
    endpoint: {
      name: "Spacelift Endpoint",
      description:
        "Your Spacelift endpoint (e.g., 'your-account.app.spacelift.io')",
      type: "string",
      required: true,
    },
    spaceId: {
      name: "Space ID",
      description:
        "The Spacelift space where notification policy and webhook will be created",
      type: "string",
      required: true,
    },
  },
  signals: {
    webhookId: {
      name: "Webhook ID",
      description:
        "The ID of the managed Spacelift webhook used for receiving notifications",
    },
    webhookSecret: {
      name: "Webhook Secret",
      description: "The secret used to verify Spacelift webhook requests",
      sensitive: true,
    },
    notificationPolicyId: {
      name: "Notification Policy ID",
      description:
        "The ID of the managed Spacelift notification policy used for routing events to Flows",
    },
  },
  blocks: {
    // Context operations
    attachContextToStack,
    contextResource,
    createContext,
    deleteContext,
    detachContextFromStack,
    getContext,
    updateContext,

    // Environment variable operations
    upsertEnvironmentVariable,
    deleteEnvironmentVariable,
    getEnvironmentVariable,

    // External operations
    markExternalDependency,

    // Module operations
    createModule,
    deleteModule,
    getModule,
    updateModule,

    // Policy operations
    attachPolicyToStack,
    detachPolicyFromStack,
    createPolicy,
    deletePolicy,
    getPolicy,
    getPolicyEvaluations,
    simulatePolicy,
    updatePolicy,

    // Run operations
    approveRun,
    rejectRun,
    confirmRun,
    startRun,
    stopRun,
    retryRun,
    discardRun,
    addComment,
    promoteRun,
    triggerRun,
    getRun,
    getRunLogs,
    onRunStateChange,

    // Stack operations
    getStack,
    webhook,
    createStack,
    createStackFromBlueprint,
    deleteStack,
    lockStack,
    unlockStack,
    updateStack,

    // Task operations
    performTask,
  },
  http: {
    onRequest: async (input) => {
      const { request } = input;
      const signature256 = request.headers["X-Signature-256"];

      if (request.method !== "POST" || !signature256) {
        await http.respond(request.requestId, {
          statusCode: 400,
          body: { error: "Invalid request" },
        });

        return;
      }

      const webhookPayload = request.body;

      try {
        const webhookSecret = input.app.signals.webhookSecret;

        if (!webhookSecret) {
          console.log("No webhook secret configured");

          await http.respond(request.requestId, {
            statusCode: 400,
            body: { error: "No webhook secret configured" },
          });
          return;
        }

        const expectedSignature =
          "sha256=" +
          createHmac("sha256", webhookSecret)
            .update(request.rawBody)
            .digest("hex");

        const providedSignature = Buffer.from(signature256);
        const computedSignature = Buffer.from(expectedSignature);

        if (!timingSafeEqual(providedSignature, computedSignature)) {
          await http.respond(request.requestId, {
            statusCode: 401,
            body: { error: "Invalid webhook signature" },
          });
          return;
        }
      } catch (error) {
        console.error("Error verifying webhook signature:", error);

        await http.respond(request.requestId, {
          statusCode: 500,
          body: { error: "Internal server error" },
        });
        return;
      }

      await http.respond(request.requestId, {
        statusCode: 200,
        body: { message: "Webhook received" },
      });

      if (webhookPayload.run && webhookPayload.stack) {
        const subscriptionBlocks = await blocks.list({
          typeIds: ["onRunStateChange"],
        });

        await messaging.sendToBlocks({
          body: {
            payload: webhookPayload,
          },
          blockIds: subscriptionBlocks.blocks.map((block) => block.id),
        });
      }

      const { value } = await kv.app.get(`run:${webhookPayload.run.id}`);
      if (value) {
        const { blockId, pendingEventId, parentEventId } = value;
        await messaging.sendToBlocks({
          body: {
            payload: webhookPayload,
            pendingEventId,
            parentEventId,
          },
          blockIds: [blockId],
        });
      }
    },
  },
  async onSync(input) {
    const { spaceId } = input.app.config;

    if (!spaceId) {
      return {
        newStatus: "failed",
        customStatusDescription: "Missing space ID",
      };
    }

    const signalUpdates: Record<string, any> = {};
    let { webhookId, notificationPolicyId } = input.app.signals;

    try {
      const credentials = extractCredentials(input.app.config);

      if (!webhookId) {
        const webhookSecret = randomBytes(32).toString("hex");

        const webhookResult = await executeSpaceliftQuery(
          credentials,
          `
          mutation CreateNamedWebhook($input: NamedWebhooksIntegrationInput!) {
            namedWebhooksIntegrationCreate(input: $input) {
              id
              name
              endpoint
              enabled
              }
              }
              `,
          {
            input: {
              space: spaceId,
              name: `Flows Webhook (${input.app.installationUrl})`,
              endpoint: input.app.http.url,
              enabled: true,
              labels: ["flows"],
              secret: webhookSecret,
            },
          },
        );

        webhookId = webhookResult.namedWebhooksIntegrationCreate.id;
        signalUpdates.webhookSecret = webhookSecret;
        signalUpdates.webhookId = webhookId;
      }

      if (!notificationPolicyId) {
        const policyResult = await executeSpaceliftQuery(
          credentials,
          `
              mutation CreatePolicy($input: PolicyCreateInput!) {
                policyCreatev2(input: $input) {
                  id
                  name
                  body
                  type
                }
              }
            `,
          {
            input: {
              name: `Flows Notification Policy (${input.app.installationUrl})`,
              body: `package spacelift

# Default notification policy that routes all notifications to Flows webhook
webhook[{"endpoint_id": endpoint.id}] {
  endpoint := input.webhook_endpoints[_]
  endpoint.id == "${webhookId}"
}`,
              type: "NOTIFICATION",
              space: spaceId,
              labels: ["flows"],
              description:
                "Notification policy for routing Spacelift events to Flows webhook",
            },
          },
        );

        signalUpdates.notificationPolicyId = policyResult.policyCreatev2.id;
      }

      return { newStatus: "ready", signalUpdates };
    } catch (error) {
      console.error("Error during sync:", error);

      return {
        newStatus: "failed",
        signalUpdates,
        customStatusDescription: `Setup failed, see logs`,
      };
    }
  },
});
