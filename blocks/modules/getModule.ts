import { AppBlock, events } from "@slflows/sdk/v1";
import { executeSpaceliftQuery, extractCredentials } from "../../client";
import {
  defineSpaceliftInputConfig,
  mapInputConfig,
  mapInputsToGraphQLVariables,
} from "../../utils";

const inputConfig = {
  moduleId: defineSpaceliftInputConfig({
    name: "Module ID",
    description: "The ID of the module to retrieve",
    type: "string",
    required: true,
    graphqlFieldKey: "id",
  }),
};

const GET_MODULE_QUERY = `
  query GetModule($id: ID!) {
    module(id: $id) {
      id
      name
      description
      repository
      branch
      projectRoot
      public
      createdAt
      administrative
      githubActionDeploy
      isDisabled
      labels
      localPreviewEnabled
      moduleSource
      namespace
      ownerSubdomain
      protectFromDeletion
      provider
      repositoryURL
      sharedAccounts
      terraformProvider
      workflowTool
      runnerImage
      space
      spaceDetails {
        id
        name
        description
        inheritEntities
        parentSpace
      }
      workerPool {
        id
        name
        description
      }
      vcsIntegration {
        id
        name
        description
        isDefault
        provider
      }
    }
  }
`;

export const getModule: AppBlock = {
  name: "Get Module",
  description: "Retrieve details of a specific module",
  category: "Modules",
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
          GET_MODULE_QUERY,
          variables,
        );

        await events.emit({
          // Core fields
          moduleId: result.module.id,
          name: result.module.name,
          description: result.module.description,

          // Repository fields
          repository: result.module.repository,
          repositoryUrl: result.module.repositoryURL,
          branch: result.module.branch,
          projectRoot: result.module.projectRoot,
          namespace: result.module.namespace,
          provider: result.module.provider,

          // Configuration fields
          administrative: result.module.administrative,
          githubActionDeploy: result.module.githubActionDeploy,
          isDisabled: result.module.isDisabled,
          labels: result.module.labels,
          localPreviewEnabled: result.module.localPreviewEnabled,
          protectFromDeletion: result.module.protectFromDeletion,

          // Module-specific fields
          public: result.module.public,
          moduleSource: result.module.moduleSource,
          ownerSubdomain: result.module.ownerSubdomain,
          sharedAccounts: result.module.sharedAccounts,
          terraformProvider: result.module.terraformProvider,
          workflowTool: result.module.workflowTool,
          runnerImage: result.module.runnerImage,

          // Space information
          spaceId: result.module.space,
          spaceDetails: result.module.spaceDetails,

          // Worker pool information
          workerPool: result.module.workerPool,

          // VCS integration information
          vcsIntegration: result.module.vcsIntegration,

          // Timestamps
          createdAt: result.module.createdAt,
        });
      },
    },
  },
  outputs: {
    default: {
      type: {
        type: "object",
        properties: {
          // Core fields
          moduleId: { type: "string" },
          name: { type: "string" },
          description: { oneOf: [{ type: "string" }, { type: "null" }] },

          // Repository fields
          repository: { type: "string" },
          repositoryUrl: { oneOf: [{ type: "string" }, { type: "null" }] },
          branch: { type: "string" },
          projectRoot: { oneOf: [{ type: "string" }, { type: "null" }] },
          namespace: { type: "string" },
          provider: { type: "string" },

          // Configuration fields
          administrative: { type: "boolean" },
          githubActionDeploy: { type: "boolean" },
          isDisabled: { type: "boolean" },
          labels: { type: "array", items: { type: "string" } },
          localPreviewEnabled: { type: "boolean" },
          protectFromDeletion: { type: "boolean" },

          // Module-specific fields
          public: { type: "boolean" },
          moduleSource: { type: "string" },
          ownerSubdomain: { type: "string" },
          sharedAccounts: { type: "array", items: { type: "string" } },
          terraformProvider: { oneOf: [{ type: "string" }, { type: "null" }] },
          workflowTool: { type: "string" },
          runnerImage: { oneOf: [{ type: "string" }, { type: "null" }] },

          // Space information
          spaceId: { type: "string" },
          spaceDetails: {
            oneOf: [
              {
                type: "object",
                properties: {
                  id: { type: "string" },
                  name: { type: "string" },
                  description: { type: "string" },
                  inheritEntities: { type: "boolean" },
                  parentSpace: {
                    oneOf: [{ type: "string" }, { type: "null" }],
                  },
                },
                required: ["id", "name", "description", "inheritEntities"],
              },
              { type: "null" },
            ],
          },

          // Worker pool information
          workerPool: {
            oneOf: [
              {
                type: "object",
                properties: {
                  id: { type: "string" },
                  name: { type: "string" },
                  description: {
                    oneOf: [{ type: "string" }, { type: "null" }],
                  },
                },
                required: ["id", "name"],
              },
              { type: "null" },
            ],
          },

          // VCS integration information
          vcsIntegration: {
            oneOf: [
              {
                type: "object",
                properties: {
                  id: { type: "string" },
                  name: { type: "string" },
                  description: { type: "string" },
                  isDefault: { type: "boolean" },
                  provider: { type: "string" },
                },
                required: [
                  "id",
                  "name",
                  "description",
                  "isDefault",
                  "provider",
                ],
              },
              { type: "null" },
            ],
          },

          // Timestamps
          createdAt: { type: "number" },
        },
        required: [
          "moduleId",
          "name",
          "repository",
          "branch",
          "namespace",
          "provider",
          "administrative",
          "githubActionDeploy",
          "isDisabled",
          "labels",
          "localPreviewEnabled",
          "protectFromDeletion",
          "public",
          "moduleSource",
          "ownerSubdomain",
          "sharedAccounts",
          "workflowTool",
          "spaceId",
          "createdAt",
        ],
      },
    },
  },
};
