import { AppBlock, events } from "@slflows/sdk/v1";
import { executeSpaceliftQuery, extractCredentials } from "../../client";

const MARK_EXTERNAL_DEPENDENCY_COMPLETED_MUTATION = `
  mutation MarkExternalDependencyCompleted($dependency: ID!, $status: RunExternalDependencyStatus!) {
    runExternalDependencyMarkAsCompleted(dependency: $dependency, status: $status) {
      phantom
    }
  }
`;

export const markExternalDependency: AppBlock = {
  name: "Mark External Dependency",
  description: "Mark an external dependency as finished or failed.",
  category: "Dependencies",
  inputs: {
    default: {
      config: {
        dependencyId: {
          name: "Dependency ID",
          description: "ID of the external dependency to mark as completed",
          type: "string",
          required: true,
        },
        status: {
          name: "Status",
          description: "Status to set for the dependency (FINISHED or FAILED)",
          type: {
            type: "string",
            enum: ["FINISHED", "FAILED"],
          },
          required: true,
        },
      },
      onEvent: async (input) => {
        const credentials = extractCredentials(input.app.config);

        await executeSpaceliftQuery(
          credentials,
          MARK_EXTERNAL_DEPENDENCY_COMPLETED_MUTATION,
          {
            dependency: input.event.inputConfig.dependencyId,
            status: input.event.inputConfig.status,
          },
        );

        await events.emit({
          dependencyId: input.event.inputConfig.dependencyId,
          status: input.event.inputConfig.status,
        });
      },
    },
  },
  outputs: {
    default: {
      type: {
        type: "object",
        properties: {
          dependencyId: { type: "string" },
          status: { type: "string" },
        },
        required: ["dependencyId", "status"],
      },
    },
  },
};
