import { AppBlock, events } from "@slflows/sdk/v1";
import { executeSpaceliftQuery, extractCredentials } from "../../client";
import {
  defineSpaceliftInputConfig,
  mapInputConfig,
  mapInputsToGraphQLVariables,
} from "../../utils";

const inputConfig = {
  name: defineSpaceliftInputConfig({
    name: "Module Name",
    description: "Name of the module",
    type: "string",
    required: true,
    graphqlFieldKey: "name",
  }),
  namespace: defineSpaceliftInputConfig({
    name: "Namespace",
    description: "Repository namespace (e.g., GitHub username or organization)",
    type: "string",
    required: true,
    graphqlFieldKey: "namespace",
  }),
  repository: defineSpaceliftInputConfig({
    name: "Repository",
    description: "Repository name",
    type: "string",
    required: true,
    graphqlFieldKey: "repository",
  }),
  branch: defineSpaceliftInputConfig({
    name: "Branch",
    description: "Git branch to track",
    type: "string",
    required: true,
    graphqlFieldKey: "branch",
  }),
  administrative: defineSpaceliftInputConfig({
    name: "Administrative",
    description: "Whether the module has administrative privileges",
    type: "boolean",
    required: true,
    graphqlFieldKey: "administrative",
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
  // Configuration fields
  description: defineSpaceliftInputConfig({
    name: "Description",
    description: "Module description (supports Markdown)",
    type: "string",
    required: false,
    graphqlFieldKey: "description",
  }),
  projectRoot: defineSpaceliftInputConfig({
    name: "Project Root",
    description: "Directory relative to workspace root containing module root",
    type: "string",
    required: false,
    graphqlFieldKey: "projectRoot",
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
  terraformProvider: defineSpaceliftInputConfig({
    name: "Terraform Provider",
    description: "Terraform provider for which this module is designed",
    type: "string",
    required: false,
    graphqlFieldKey: "terraformProvider",
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

const CREATE_MODULE_MUTATION = `
  mutation CreateModule($input: ModuleCreateInput!) {
    moduleCreate(input: $input) {
      id
    }
  }
`;

export const createModule: AppBlock = {
  name: "Create Module",
  description: "Create a new Spacelift module",
  category: "Modules",
  inputs: {
    default: {
      config: mapInputConfig(inputConfig),
      onEvent: async (input) => {
        const mappedInputs = mapInputsToGraphQLVariables(
          inputConfig,
          input.event.inputConfig,
        );

        if (!mappedInputs.name) {
          throw new Error("Module name is required");
        }
        if (!mappedInputs.repository) {
          throw new Error("Repository is required");
        }
        if (!mappedInputs.namespace) {
          throw new Error("Namespace is required");
        }

        const createInput: any = {
          name: mappedInputs.name,
          labels: mappedInputs.labels || [],
          description: mappedInputs.description || "",
          terraformProvider: mappedInputs.terraformProvider || "default",
          provider: mappedInputs.provider || "GITHUB",
          repository: mappedInputs.repository,
          namespace: mappedInputs.namespace,
          branch: mappedInputs.branch || "main",
          space: mappedInputs.space || "root",
          projectRoot: mappedInputs.projectRoot || "",

          updateInput: {
            workerPool: mappedInputs.workerPool || null,
            workflowTool: mappedInputs.workflowTool || "TERRAFORM_FOSS",
            administrative:
              mappedInputs.administrative !== undefined
                ? mappedInputs.administrative
                : false,
            localPreviewEnabled: mappedInputs.localPreviewEnabled || false,
            protectFromDeletion: mappedInputs.protectFromDeletion || false,
            runnerImage: mappedInputs.runnerImage || null,
            provider: mappedInputs.provider || "GITHUB",
            repository: mappedInputs.repository,
            namespace: mappedInputs.namespace,
            branch: mappedInputs.branch || "main",
            space: mappedInputs.space || "root",
            projectRoot: mappedInputs.projectRoot || "",
            name: mappedInputs.name,
            labels: mappedInputs.labels || [],
            description: mappedInputs.description || "",
            terraformProvider: "",
            githubActionDeploy:
              mappedInputs.githubActionDeploy !== undefined
                ? mappedInputs.githubActionDeploy
                : true,
            sharedAccounts: mappedInputs.sharedAccounts || [],
          },
        };

        if (mappedInputs.repositoryURL !== undefined) {
          createInput.repositoryURL = mappedInputs.repositoryURL;
        }
        if (mappedInputs.vcsIntegrationId !== undefined) {
          createInput.vcsIntegrationId = mappedInputs.vcsIntegrationId;
        }

        const credentials = extractCredentials(input.app.config);

        const result = await executeSpaceliftQuery(
          credentials,
          CREATE_MODULE_MUTATION,
          {
            input: createInput,
          },
        );

        await events.emit({
          id: result.moduleCreate.id,
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
