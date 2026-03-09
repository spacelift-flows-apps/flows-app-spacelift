import { AppBlock, events, kv } from "@slflows/sdk/v1";
import { executeSpaceliftQuery, extractCredentials } from "../../client";
import {
  defineSpaceliftInputConfig,
  mapInputConfig,
  mapInputsToGraphQLVariables,
} from "../../utils";

const inputConfig = {
  templateVersionId: defineSpaceliftInputConfig({
    name: "Template Version ID",
    description:
      "The ID of the template version to deploy. You can get it from the URL when viewing a template deployment version in the template workbench.",
    type: "string",
    required: true,
    graphqlFieldKey: "id",
  }),
  space: defineSpaceliftInputConfig({
    name: "Space",
    description: "The space where the deployment will be created",
    type: "string",
    required: true,
  }),
  name: defineSpaceliftInputConfig({
    name: "Deployment Name",
    description: "A name for the deployment",
    type: "string",
    required: true,
  }),
  description: defineSpaceliftInputConfig({
    name: "Description",
    description: "A description for the deployment",
    type: "string",
    required: false,
  }),
  inputs: defineSpaceliftInputConfig({
    name: "Inputs",
    description: "Key-value pairs for template deployment inputs",
    type: {
      type: "object",
      additionalProperties: { type: "string" },
    },
    required: false,
  }),
};

const DEPLOY_TEMPLATE_MUTATION = `
  mutation BlueprintDeploymentCreate($id: ID!, $input: BlueprintDeploymentCreateInput!) {
    blueprintDeploymentCreate(id: $id, input: $input) {
      id
      name
      description
      state
      blueprint { id }
      blueprintVersion { id version }
      stacks { id name state outputs { id value sensitive } }
      space { id }
    }
  }
`;

const GET_DEPLOYMENT_QUERY = `
  query BlueprintDeploymentGet($id: ID!, $blueprintID: ID!) {
    blueprintDeployment(id: $id, blueprintID: $blueprintID) {
      id
      name
      state
      blueprint { id }
      blueprintVersion { id version }
      stacks { id name state outputs { id value sensitive } }
      space { id }
    }
  }
`;

const TERMINAL_STATES = [
  "FINISHED",
  "FAILURE",
  "DESTROYING_FAILED",
  "DISCARDED",
];

const TTL_7_DAYS = 7 * 24 * 60 * 60;

interface DeploymentStack {
  id: string;
  name: string;
  state: string;
  outputs: Array<{ id: string; value: string | null; sensitive: boolean }>;
}

function collectOutputs(stacks: DeploymentStack[]): Record<string, unknown> {
  const outputs: Record<string, unknown> = {};
  for (const stack of stacks) {
    for (const output of stack.outputs) {
      if (!output.sensitive && output.value != null) {
        try {
          outputs[output.id] = JSON.parse(output.value);
        } catch {
          outputs[output.id] = output.value;
        }
      }
    }
  }
  return outputs;
}

function buildEventData(deployment: any) {
  return {
    deploymentId: deployment.id,
    name: deployment.name,
    state: deployment.state,
    template: deployment.blueprint,
    templateVersion: deployment.blueprintVersion,
    outputs: collectOutputs(deployment.stacks),
    space: deployment.space,
  };
}

const deploymentOutputSchema = {
  type: "object" as const,
  properties: {
    deploymentId: { type: "string" as const },
    name: { type: "string" as const },
    state: {
      type: "string" as const,
      enum: [
        "NONE",
        "FINISHED",
        "IN_PROGRESS",
        "FAILURE",
        "DESTROYING",
        "DESTROYING_FAILED",
        "UNCONFIRMED",
        "DISCARDED",
      ],
    },
    template: {
      type: "object" as const,
      properties: { id: { type: "string" as const } },
      required: ["id"],
    },
    templateVersion: {
      type: "object" as const,
      properties: {
        id: { type: "string" as const },
        version: {
          anyOf: [{ type: "string" as const }, { type: "null" as const }],
        },
      },
      required: ["id"],
    },
    outputs: {
      type: "object" as const,
      description:
        "Collected outputs from all stacks in the deployment (non-sensitive only, JSON-parsed)",
      additionalProperties: true,
    },
    space: {
      type: "object" as const,
      properties: { id: { type: "string" as const } },
      required: ["id"],
    },
  },
  required: ["deploymentId", "name", "state", "outputs"],
};

export const deployTemplate: AppBlock = {
  name: "Deploy Template",
  description:
    "Deploy a Spacelift template and track deployment status via webhook notifications",
  category: "Templates",
  inputs: {
    default: {
      config: mapInputConfig(inputConfig),
      onEvent: async (input) => {
        const mappedInputs = mapInputsToGraphQLVariables(
          inputConfig,
          input.event.inputConfig,
        );

        const deploymentInputs = mappedInputs.inputs
          ? Object.entries(mappedInputs.inputs).map(([id, value]) => ({
              id,
              value: String(value),
            }))
          : [];

        const createVariables = {
          id: mappedInputs.id,
          input: {
            space: mappedInputs.space,
            name: mappedInputs.name,
            description: mappedInputs.description,
            inputs: deploymentInputs,
          },
        };

        const credentials = extractCredentials(input.app.config);

        const createResult = await executeSpaceliftQuery(
          credentials,
          DEPLOY_TEMPLATE_MUTATION,
          createVariables,
        );

        // Poll until the deployment has stacks and a non-NONE state,
        // since the API returns empty stacks and NONE state initially.
        let deployment = createResult.blueprintDeploymentCreate;
        for (let attempt = 0; attempt < 30; attempt++) {
          if (deployment.stacks.length > 0 && deployment.state !== "NONE") {
            break;
          }

          await new Promise((resolve) => setTimeout(resolve, 2000));

          const pollResult = await executeSpaceliftQuery(
            credentials,
            GET_DEPLOYMENT_QUERY,
            {
              id: deployment.id,
              blueprintID: deployment.blueprint.id,
            },
          );

          if (pollResult.blueprintDeployment) {
            deployment = pollResult.blueprintDeployment;
          }
        }

        const pendingEventId = await events.createPending({
          event: buildEventData(deployment),
          outputKey: "completed",
          statusDescription: `Deployment ${deployment.name} created (${deployment.state})`,
        });

        // Emit initial status event
        await events.emit(buildEventData(deployment), {
          outputKey: "stateChanged",
          parentEventId: input.event.id,
        });

        for (const stack of deployment.stacks) {
          await kv.app.set({
            key: `template-stack:${stack.id}`,
            value: {
              blockId: input.block.id,
              parentEventId: input.event.id,
              pendingEventId,
              deploymentId: deployment.id,
              blueprintId: deployment.blueprint.id,
            },
            ttl: TTL_7_DAYS,
          });
        }

        await kv.app.set({
          key: `template-deployment:${deployment.id}`,
          value: {
            pendingEventId,
            blueprintId: deployment.blueprint.id,
            lastState: deployment.state,
            stackIds: deployment.stacks.map((s: { id: string }) => s.id),
            createdAt: Date.now(),
          },
          ttl: TTL_7_DAYS,
        });
      },
    },
  },
  onInternalMessage: async (input) => {
    const { parentEventId, pendingEventId, deploymentId, blueprintId } =
      input.message.body;

    const credentials = extractCredentials(input.app.config);

    const result = await executeSpaceliftQuery(
      credentials,
      GET_DEPLOYMENT_QUERY,
      {
        id: deploymentId,
        blueprintID: blueprintId,
      },
    );

    const deployment = result.blueprintDeployment;
    if (!deployment) {
      return;
    }

    const { value: deploymentMeta } = await kv.app.get(
      `template-deployment:${deploymentId}`,
    );

    if (!deploymentMeta) {
      return;
    }

    if (deployment.state === deploymentMeta.lastState) {
      return;
    }

    await kv.app.set({
      key: `template-deployment:${deploymentId}`,
      value: {
        ...deploymentMeta,
        lastState: deployment.state,
      },
      ttl: TTL_7_DAYS,
    });

    const eventData = buildEventData(deployment);

    await events.updatePending(pendingEventId, {
      statusDescription: `Deployment ${deployment.name} is ${deployment.state.toLowerCase().replace(/_/g, " ")}`,
    });

    await events.emit(eventData, { outputKey: "stateChanged", parentEventId });

    if (TERMINAL_STATES.includes(deployment.state)) {
      await events.emit(eventData, {
        outputKey: "completed",
        complete: pendingEventId,
        parentEventId,
      });

      const stackIds = deploymentMeta.stackIds as string[];
      await kv.app.delete(
        stackIds
          .map((id: string) => `template-stack:${id}`)
          .concat(`template-deployment:${deploymentId}`),
      );
    }
  },
  schedules: {
    cleanup: {
      definition: {
        type: "cron",
        cron: {
          expression: "0 * * * *",
          location: "UTC",
        },
      },
      onTrigger: async () => {
        const staleThreshold = Date.now() - 7 * 24 * 60 * 60 * 1000;

        const trackedDeployments = await kv.app.list({
          keyPrefix: "template-deployment:",
        });

        for (const item of trackedDeployments.pairs) {
          const { pendingEventId, createdAt } = item.value;
          if (createdAt < staleThreshold) {
            await events.cancelPending(
              pendingEventId,
              `Deployment ${item.key.replace("template-deployment:", "")} did not complete within 7 days - cleaning up stale pending event`,
            );
          }
        }
      },
    },
  },
  outputs: {
    completed: {
      default: true,
      name: "Deployment Completed",
      description:
        "Emitted when the template deployment reaches a terminal state",
      type: deploymentOutputSchema,
    },
    stateChanged: {
      secondary: true,
      name: "State Changed",
      description: "Emitted when the template deployment status changes",
      type: deploymentOutputSchema,
    },
  },
};
