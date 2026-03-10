import { AppBlock, events } from "@slflows/sdk/v1";
import { executeSpaceliftQuery, extractCredentials } from "../../client";
import {
  defineSpaceliftInputConfig,
  mapInputConfig,
  mapInputsToGraphQLVariables,
} from "../../utils";

const inputConfig = {
  id: defineSpaceliftInputConfig({
    name: "Module ID",
    description: "The ID of the module to update",
    type: "string",
    required: true,
    graphqlFieldKey: "id",
  }),
  administrative: defineSpaceliftInputConfig({
    name: "Administrative",
    description: "Whether the module is administrative",
    type: "boolean",
    required: false,
    graphqlFieldKey: "administrative",
  }),
  branch: defineSpaceliftInputConfig({
    name: "Branch",
    description: "Git branch to track",
    type: "string",
    required: false,
    graphqlFieldKey: "branch",
  }),
  description: defineSpaceliftInputConfig({
    name: "Description",
    description: "Module description (supports Markdown)",
    type: "string",
    required: false,
    graphqlFieldKey: "description",
  }),
  repository: defineSpaceliftInputConfig({
    name: "Repository",
    description: "Name of the repository to track",
    type: "string",
    required: false,
    graphqlFieldKey: "repository",
  }),
  namespace: defineSpaceliftInputConfig({
    name: "Namespace",
    description: "Repository namespace (defaults to GitHub account name)",
    type: "string",
    required: false,
    graphqlFieldKey: "namespace",
  }),
  provider: defineSpaceliftInputConfig({
    name: "VCS Provider",
    description: "VCS provider (GITHUB, GITLAB, BITBUCKET_CLOUD, etc.)",
    type: "string",
    required: false,
    graphqlFieldKey: "provider",
  }),
  repositoryUrl: defineSpaceliftInputConfig({
    name: "Repository URL",
    description: "Optional URL of the repository",
    type: "string",
    required: false,
    graphqlFieldKey: "repositoryURL",
  }),
  githubActionDeploy: defineSpaceliftInputConfig({
    name: "GitHub Action Deploy",
    description: "Enable GitHub Checks 'Deploy' action",
    type: "boolean",
    required: false,
    graphqlFieldKey: "githubActionDeploy",
  }),
  labels: defineSpaceliftInputConfig({
    name: "Labels",
    description: "Module labels",
    type: {
      type: "array",
      items: { type: "string" },
    },
    required: false,
    graphqlFieldKey: "labels",
  }),
  localPreviewEnabled: defineSpaceliftInputConfig({
    name: "Enable Local Preview",
    description: "Enable local workspace based versions",
    type: "boolean",
    required: false,
    graphqlFieldKey: "localPreviewEnabled",
  }),
  projectRoot: defineSpaceliftInputConfig({
    name: "Project Root",
    description: "Directory relative to workspace root containing module root",
    type: "string",
    required: false,
    graphqlFieldKey: "projectRoot",
  }),
  protectFromDeletion: defineSpaceliftInputConfig({
    name: "Protect from Deletion",
    description: "Enable deletion protection",
    type: "boolean",
    required: false,
    graphqlFieldKey: "protectFromDeletion",
  }),
  sharedAccounts: defineSpaceliftInputConfig({
    name: "Shared Accounts",
    description: "List of account subdomains with access to the module",
    type: {
      type: "array",
      items: { type: "string" },
    },
    required: false,
    graphqlFieldKey: "sharedAccounts",
  }),
  space: defineSpaceliftInputConfig({
    name: "Space ID",
    description: "ID of the space the module should be in",
    type: "string",
    required: false,
    graphqlFieldKey: "space",
  }),
  vcsIntegrationId: defineSpaceliftInputConfig({
    name: "VCS Integration ID",
    description: "ID of VCS integration (for space-level integrations)",
    type: "string",
    required: false,
    graphqlFieldKey: "vcsIntegrationId",
  }),
  workerPool: defineSpaceliftInputConfig({
    name: "Worker Pool ID",
    description: "ID of the worker pool to use",
    type: "string",
    required: false,
    graphqlFieldKey: "workerPool",
  }),
  workflowTool: defineSpaceliftInputConfig({
    name: "Workflow Tool",
    description: "Workflow tool (TERRAFORM_FOSS, OPEN_TOFU, CUSTOM)",
    type: "string",
    required: false,
    graphqlFieldKey: "workflowTool",
  }),
  runnerImage: defineSpaceliftInputConfig({
    name: "Runner Image",
    description: "Custom runner image for parsing runs",
    type: "string",
    required: false,
    graphqlFieldKey: "runnerImage",
  }),
};

const GET_MODULE_QUERY = `
  query GetModule($id: ID!) {
    module(id: $id) {
      id
      administrative
      branch
      description
      repository
      namespace
      provider
      repositoryURL
      githubActionDeploy
      labels
      localPreviewEnabled
      projectRoot
      protectFromDeletion
      sharedAccounts
      space
      vcsIntegration {
        id
      }
      workerPool {
        id
      }
      workflowTool
      runnerImage
    }
  }
`;

const UPDATE_MODULE_V2_MUTATION = `
  mutation UpdateModuleV2($id: ID!, $input: ModuleUpdateV2Input!) {
    moduleUpdateV2(id: $id, input: $input) {
      id
    }
  }
`;

export const updateModule: AppBlock = {
  name: "Update Module",
  description: "Update an existing module configuration",
  category: "Modules",
  inputs: {
    default: {
      config: mapInputConfig(inputConfig),
      onEvent: async (input) => {
        const { id, ...userUpdates } = mapInputsToGraphQLVariables(
          inputConfig,
          input.event.inputConfig,
        );
        const credentials = extractCredentials(input.app.config);

        const currentModule = await executeSpaceliftQuery(
          credentials,
          GET_MODULE_QUERY,
          { id },
        );

        const updateInput: any = {
          administrative: currentModule.module.administrative,
          branch: currentModule.module.branch,
          description: currentModule.module.description,
          repository: currentModule.module.repository,
          namespace: currentModule.module.namespace,
          provider: currentModule.module.provider,
          repositoryURL: currentModule.module.repositoryURL,
          githubActionDeploy: currentModule.module.githubActionDeploy,
          labels: currentModule.module.labels || [],
          localPreviewEnabled: currentModule.module.localPreviewEnabled,
          projectRoot: currentModule.module.projectRoot,
          protectFromDeletion: currentModule.module.protectFromDeletion,
          sharedAccounts: currentModule.module.sharedAccounts || [],
          space: currentModule.module.space,
          workerPool: currentModule.module.workerPool?.id || null,
          workflowTool: currentModule.module.workflowTool,
          runnerImage: currentModule.module.runnerImage,
        };

        if (currentModule.module.vcsIntegration?.id) {
          updateInput.vcsIntegrationId = currentModule.module.vcsIntegration.id;
        }

        Object.keys(userUpdates).forEach((key) => {
          if (userUpdates[key] !== undefined) {
            updateInput[key] = userUpdates[key];
          }
        });

        const result = await executeSpaceliftQuery(
          credentials,
          UPDATE_MODULE_V2_MUTATION,
          {
            id,
            input: updateInput,
          },
        );

        await events.emit({
          id: result.moduleUpdateV2.id,
        });
      },
    },
  },
  outputs: {
    default: {
      type: {
        type: "object",
        properties: {
          id: { type: "string" },
        },
        required: ["id"],
      },
    },
  },
};
