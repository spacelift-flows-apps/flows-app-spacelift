import { AppBlock, events } from "@slflows/sdk/v1";
import { executeSpaceliftQuery, extractCredentials } from "../../client";
import {
  defineSpaceliftInputConfig,
  mapInputConfig,
  mapInputsToGraphQLVariables,
} from "../../utils";

const inputConfig = {
  stackId: defineSpaceliftInputConfig({
    name: "Stack ID",
    description: "The ID of the stack to update",
    type: "string",
    required: true,
    graphqlFieldKey: "id",
  }),
  name: defineSpaceliftInputConfig({
    name: "Name",
    description: "New name for the stack",
    type: "string",
    required: false,
    graphqlFieldKey: "name",
  }),
  description: defineSpaceliftInputConfig({
    name: "Description",
    description: "Stack description (supports Markdown)",
    type: "string",
    required: false,
    graphqlFieldKey: "description",
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
  repository: defineSpaceliftInputConfig({
    name: "Repository",
    description: "Repository name",
    type: "string",
    required: false,
    graphqlFieldKey: "repository",
  }),
  repositoryURL: defineSpaceliftInputConfig({
    name: "Repository URL",
    description: "URL of the repository",
    type: "string",
    required: false,
    graphqlFieldKey: "repositoryURL",
  }),
  branch: defineSpaceliftInputConfig({
    name: "Branch",
    description: "Git branch to track",
    type: "string",
    required: false,
    graphqlFieldKey: "branch",
  }),
  administrative: defineSpaceliftInputConfig({
    name: "Administrative",
    description: "Whether this is an administrative stack",
    type: "boolean",
    required: false,
    graphqlFieldKey: "administrative",
  }),
  autodeploy: defineSpaceliftInputConfig({
    name: "Auto Deploy",
    description: "Enable automatic deployments for tracked branches",
    type: "boolean",
    required: false,
    graphqlFieldKey: "autodeploy",
  }),
  autoretry: defineSpaceliftInputConfig({
    name: "Autoretry",
    description: "Automatically retry invalidated Pull Request runs",
    type: "boolean",
    required: false,
    graphqlFieldKey: "autoretry",
  }),
  localPreviewEnabled: defineSpaceliftInputConfig({
    name: "Enable Local Preview",
    description: "Enable local workspace based runs",
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
  projectRoot: defineSpaceliftInputConfig({
    name: "Project Root",
    description:
      "Directory relative to workspace root containing stack entrypoint",
    type: "string",
    required: false,
    graphqlFieldKey: "projectRoot",
  }),
  additionalProjectGlobs: defineSpaceliftInputConfig({
    name: "Additional Project Globs",
    description: "List of globs that trigger runs when changed",
    type: {
      type: "array",
      items: { type: "string" },
    },
    required: false,
    graphqlFieldKey: "additionalProjectGlobs",
  }),
  terraformVersion: defineSpaceliftInputConfig({
    name: "Terraform Version",
    description: "Terraform version to use (can be a version range)",
    type: "string",
    required: false,
    graphqlFieldKey: "terraformVersion",
  }),
  runnerImage: defineSpaceliftInputConfig({
    name: "Runner Image",
    description: "Custom runner image for processing runs",
    type: "string",
    required: false,
    graphqlFieldKey: "runnerImage",
  }),
  space: defineSpaceliftInputConfig({
    name: "Space ID",
    description: "ID of the space the stack should be in",
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
  githubActionDeploy: defineSpaceliftInputConfig({
    name: "GitHub Action Deploy",
    description: "Enable GitHub Checks 'Deploy' action",
    type: "boolean",
    required: false,
    graphqlFieldKey: "githubActionDeploy",
  }),
  enableWellKnownSecretMasking: defineSpaceliftInputConfig({
    name: "Enable Secret Masking",
    description: "Enable masking for well-known secrets",
    type: "boolean",
    required: false,
    graphqlFieldKey: "enableWellKnownSecretMasking",
  }),
  enableSensitiveOutputUpload: defineSpaceliftInputConfig({
    name: "Enable Sensitive Output Upload",
    description: "Upload sensitive outputs for stack dependencies",
    type: "boolean",
    required: false,
    graphqlFieldKey: "enableSensitiveOutputUpload",
  }),
  labels: defineSpaceliftInputConfig({
    name: "Labels",
    description: "Stack labels",
    type: {
      type: "array",
      items: { type: "string" },
    },
    required: false,
    graphqlFieldKey: "labels",
  }),
  afterApply: defineSpaceliftInputConfig({
    name: "After Apply Scripts",
    description: "Scripts to run after apply",
    type: {
      type: "array",
      items: { type: "string" },
    },
    required: false,
    graphqlFieldKey: "afterApply",
  }),
  beforeApply: defineSpaceliftInputConfig({
    name: "Before Apply Scripts",
    description: "Scripts to run before apply",
    type: {
      type: "array",
      items: { type: "string" },
    },
    required: false,
    graphqlFieldKey: "beforeApply",
  }),
  afterDestroy: defineSpaceliftInputConfig({
    name: "After Destroy Scripts",
    description: "Scripts to run after destroy",
    type: {
      type: "array",
      items: { type: "string" },
    },
    required: false,
    graphqlFieldKey: "afterDestroy",
  }),
  beforeDestroy: defineSpaceliftInputConfig({
    name: "Before Destroy Scripts",
    description: "Scripts to run before destroy",
    type: {
      type: "array",
      items: { type: "string" },
    },
    required: false,
    graphqlFieldKey: "beforeDestroy",
  }),
  afterInit: defineSpaceliftInputConfig({
    name: "After Init Scripts",
    description: "Scripts to run after init",
    type: {
      type: "array",
      items: { type: "string" },
    },
    required: false,
    graphqlFieldKey: "afterInit",
  }),
  beforeInit: defineSpaceliftInputConfig({
    name: "Before Init Scripts",
    description: "Scripts to run before init",
    type: {
      type: "array",
      items: { type: "string" },
    },
    required: false,
    graphqlFieldKey: "beforeInit",
  }),
  afterPlan: defineSpaceliftInputConfig({
    name: "After Plan Scripts",
    description: "Scripts to run after plan",
    type: {
      type: "array",
      items: { type: "string" },
    },
    required: false,
    graphqlFieldKey: "afterPlan",
  }),
  beforePlan: defineSpaceliftInputConfig({
    name: "Before Plan Scripts",
    description: "Scripts to run before plan",
    type: {
      type: "array",
      items: { type: "string" },
    },
    required: false,
    graphqlFieldKey: "beforePlan",
  }),
  afterPerform: defineSpaceliftInputConfig({
    name: "After Perform Scripts",
    description: "Scripts to run after perform",
    type: {
      type: "array",
      items: { type: "string" },
    },
    required: false,
    graphqlFieldKey: "afterPerform",
  }),
  beforePerform: defineSpaceliftInputConfig({
    name: "Before Perform Scripts",
    description: "Scripts to run before perform",
    type: {
      type: "array",
      items: { type: "string" },
    },
    required: false,
    graphqlFieldKey: "beforePerform",
  }),
  afterRun: defineSpaceliftInputConfig({
    name: "After Run Scripts",
    description: "Scripts to run after any run",
    type: {
      type: "array",
      items: { type: "string" },
    },
    required: false,
    graphqlFieldKey: "afterRun",
  }),
};

const GET_STACK_QUERY = `
  query GetStack($id: ID!) {
    stack(id: $id) {
      id
      name
      description
      namespace
      provider
      repository
      repositoryURL
      branch
      administrative
      autodeploy
      autoretry
      localPreviewEnabled
      protectFromDeletion
      projectRoot
      additionalProjectGlobs
      terraformVersion
      runnerImage
      space
      vcsIntegration {
        id
      }
      workerPool {
        id
      }
      githubActionDeploy
      isDisabled
      enableWellKnownSecretMasking
      enableSensitiveOutputUpload
      labels
      afterApply
      beforeApply
      afterDestroy
      beforeDestroy
      afterInit
      beforeInit
      afterPlan
      beforePlan
      afterPerform
      beforePerform
      afterRun
    }
  }
`;

const UPDATE_STACK_MUTATION = `
  mutation UpdateStack($id: ID!, $input: StackInput!) {
    stackUpdate(id: $id, input: $input) {
      id
    }
  }
`;

export const updateStack: AppBlock = {
  name: "Update Stack",
  description: "Update an existing stack configuration",
  category: "Stacks",
  inputs: {
    default: {
      config: mapInputConfig(inputConfig),
      onEvent: async (input) => {
        const { id, ...userUpdates } = mapInputsToGraphQLVariables(
          inputConfig,
          input.event.inputConfig,
        );
        const credentials = extractCredentials(input.app.config);

        const currentStack = await executeSpaceliftQuery(
          credentials,
          GET_STACK_QUERY,
          { id },
        );

        const updateInput: any = {
          administrative: currentStack.stack.administrative,
          branch: currentStack.stack.branch,
          name: currentStack.stack.name,
          repository: currentStack.stack.repository,
          autodeploy: currentStack.stack.autodeploy,
          autoretry: currentStack.stack.autoretry,
          description: currentStack.stack.description,
          githubActionDeploy: currentStack.stack.githubActionDeploy,
          isDisabled: currentStack.stack.isDisabled,
          labels: currentStack.stack.labels || [],
          localPreviewEnabled: currentStack.stack.localPreviewEnabled,
          namespace: currentStack.stack.namespace,
          projectRoot: currentStack.stack.projectRoot,
          additionalProjectGlobs:
            currentStack.stack.additionalProjectGlobs || [],
          protectFromDeletion: currentStack.stack.protectFromDeletion,
          provider: currentStack.stack.provider,
          repositoryURL: currentStack.stack.repositoryURL,
          runnerImage: currentStack.stack.runnerImage,
          space: currentStack.stack.space,
          terraformVersion: currentStack.stack.terraformVersion,
          workerPool: currentStack.stack.workerPool?.id || null,
          enableWellKnownSecretMasking:
            currentStack.stack.enableWellKnownSecretMasking,
          enableSensitiveOutputUpload:
            currentStack.stack.enableSensitiveOutputUpload,

          afterApply: currentStack.stack.afterApply || [],
          beforeApply: currentStack.stack.beforeApply || [],
          afterDestroy: currentStack.stack.afterDestroy || [],
          beforeDestroy: currentStack.stack.beforeDestroy || [],
          afterInit: currentStack.stack.afterInit || [],
          beforeInit: currentStack.stack.beforeInit || [],
          afterPlan: currentStack.stack.afterPlan || [],
          beforePlan: currentStack.stack.beforePlan || [],
          afterPerform: currentStack.stack.afterPerform || [],
          beforePerform: currentStack.stack.beforePerform || [],
          afterRun: currentStack.stack.afterRun || [],
        };

        if (currentStack.stack.vcsIntegration?.id) {
          updateInput.vcsIntegrationId = currentStack.stack.vcsIntegration.id;
        }

        Object.keys(userUpdates).forEach((key) => {
          if (userUpdates[key] !== undefined) {
            updateInput[key] = userUpdates[key];
          }
        });

        const result = await executeSpaceliftQuery(
          credentials,
          UPDATE_STACK_MUTATION,
          {
            id,
            input: updateInput,
          },
        );

        await events.emit({
          id: result.stackUpdate.id,
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
