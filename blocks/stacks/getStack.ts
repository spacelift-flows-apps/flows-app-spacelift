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
    description: "The ID of the stack to retrieve",
    type: "string",
    required: true,
    graphqlFieldKey: "id",
  }),
};

const GET_STACK_QUERY = `
  query GetStack($id: ID!) {
    stack(id: $id) {
      id
      name
      description
      space
      spaceDetails {
        id
        name
        description
        inheritEntities
        parentSpace
      }
      
      repository
      repositoryURL
      branch
      namespace
      provider
      projectRoot
      additionalProjectGlobs
      repositoryTags
      vcsDetached
      vcsIntegration {
        id
        name
        description
        isDefault
        provider
      }
      
      administrative
      autodeploy
      autoretry
      githubActionDeploy
      isDisabled
      localPreviewEnabled
      protectFromDeletion
      managesStateFile
      enableWellKnownSecretMasking
      enableSensitiveOutputUpload
      labels
      runnerImage
      
      terraformVersion
      effectiveTerraformVersion
      
      vendorConfig {
        ... on StackConfigVendorTerraform {
          version
          workspace
          useSmartSanitization
          externalStateAccessEnabled
          workflowTool
        }
        ... on StackConfigVendorTerragrunt {
          terraformVersion
          terragruntVersion
          useRunAll
          useSmartSanitization
          tool
        }
        ... on StackConfigVendorPulumi {
          loginURL
          stackName
        }
        ... on StackConfigVendorCloudFormation {
          entryTemplateFile
          templateBucket
          stackName
          region
        }
        ... on StackConfigVendorKubernetes {
          namespace
          kubectlVersion
          kubernetesWorkflowTool
        }
        ... on StackConfigVendorAnsible {
          playbook
        }
      }
      
      workerPool {
        id
        name
        description
      }
      
      state
      stateIsTerminal
      stateSetAt
      stateSetBy
      lastApplyAt
      blocked
      blocker {
        id
        state
        type
        title
      }
      isDrifted
      entityCount
      needsApproval
      deleted
      deleting
      
      lockedAt
      lockedBy
      lockNote
      
      trackedCommit {
        hash
        tag
        message
        authorName
        timestamp
        url
      }
      trackedBranchHead {
        hash
        tag
        message
        authorName
        timestamp
        url
      }
      trackedCommitSetBy
      
      config {
        id
        checksum
        createdAt
        runtime
        type
        value
        description
        writeOnly
        fileMode
      }
      
      hooks {
        afterApply
        beforeApply
        afterInit
        beforeInit
        afterPlan
        beforePlan
        afterPerform
        beforePerform
        afterDestroy
        beforeDestroy
        afterRun
      }
      
      integrations {
        aws {
          activated
          assumedRoleArn
          assumeRoleLink
          assumeRolePolicyStatement
          durationSeconds
          externalID
          generateCredentialsInWorker
          region
        }
        awsV2 {
          id
          integrationId
          name
          read
          write
          roleArn
          durationSeconds
          externalId
          generateCredentialsInWorker
          legacy
        }
        azure {
          id
          integrationId
          integrationName
          integrationDisplayName
          read
          write
          subscriptionId
          defaultSubscriptionId
        }
        gcp {
          activated
          serviceAccountEmail
          tokenScopes
        }
        driftDetection {
          deleted
          reconcile
          ignoreState
          timezone
          schedule
          nextSchedule
        }
        slack {
          activated
          channelNames
        }
        webhooks {
          id
          deleted
          enabled
          endpoint
        }
      }
      
      attachedContexts {
        id
        contextId
        contextName
        contextDescription
        contextLabels
        priority
        isAutoattached
      }
      attachedPolicies {
        id
        policyId
        policyName
        policyType
        policyLabels
        isAutoattached
      }
      
      dependsOn {
        id
        stackId
        dependsOnStackId
        referenceCount
      }
      isDependedOnBy {
        id
        stackId
        dependsOnStackId
        referenceCount
      }
      
      scheduledRuns {
        id
        name
        timezone
        cronSchedule
        timestampSchedule
        nextSchedule
      }
      scheduledDeletes {
        id
        shouldDeleteResources
        timezone
        cronSchedule
        timestampSchedule
        nextSchedule
      }
      scheduledTasks {
        id
        command
        timezone
        cronSchedule
        timestampSchedule
        nextSchedule
      }
      
      blueprint {
        id
        name
        description
        state
      }
      
      outputs {
        id
        value
        sensitive
        description
      }
      
      canWrite
      starred
      notificationCount
      createdAt
      apiHost
      isStateRollback
    }
  }
`;

export const getStack: AppBlock = {
  name: "Get Stack",
  description: "Retrieve details of a specific stack",
  category: "Stacks",
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
          GET_STACK_QUERY,
          variables,
        );

        await events.emit({
          stackId: result.stack.id,
          name: result.stack.name,
          description: result.stack.description,
          spaceId: result.stack.space,
          spaceDetails: result.stack.spaceDetails,
          repository: result.stack.repository,
          repositoryUrl: result.stack.repositoryURL,
          branch: result.stack.branch,
          namespace: result.stack.namespace,
          provider: result.stack.provider,
          projectRoot: result.stack.projectRoot,
          additionalProjectGlobs: result.stack.additionalProjectGlobs,
          repositoryTags: result.stack.repositoryTags,
          vcsDetached: result.stack.vcsDetached,
          vcsIntegration: result.stack.vcsIntegration,
          administrative: result.stack.administrative,
          autodeploy: result.stack.autodeploy,
          autoretry: result.stack.autoretry,
          githubActionDeploy: result.stack.githubActionDeploy,
          isDisabled: result.stack.isDisabled,
          localPreviewEnabled: result.stack.localPreviewEnabled,
          protectFromDeletion: result.stack.protectFromDeletion,
          managesStateFile: result.stack.managesStateFile,
          enableWellKnownSecretMasking:
            result.stack.enableWellKnownSecretMasking,
          enableSensitiveOutputUpload: result.stack.enableSensitiveOutputUpload,
          labels: result.stack.labels,
          runnerImage: result.stack.runnerImage,
          terraformVersion: result.stack.terraformVersion,
          effectiveTerraformVersion: result.stack.effectiveTerraformVersion,
          vendorConfig: result.stack.vendorConfig,
          workerPool: result.stack.workerPool,
          state: result.stack.state,
          stateIsTerminal: result.stack.stateIsTerminal,
          stateSetAt: result.stack.stateSetAt,
          stateSetBy: result.stack.stateSetBy,
          lastApplyAt: result.stack.lastApplyAt,
          blocked: result.stack.blocked,
          blocker: result.stack.blocker,
          isDrifted: result.stack.isDrifted,
          entityCount: result.stack.entityCount,
          needsApproval: result.stack.needsApproval,
          deleted: result.stack.deleted,
          deleting: result.stack.deleting,
          lockedAt: result.stack.lockedAt,
          lockedBy: result.stack.lockedBy,
          lockNote: result.stack.lockNote,
          trackedCommit: result.stack.trackedCommit,
          trackedBranchHead: result.stack.trackedBranchHead,
          trackedCommitSetBy: result.stack.trackedCommitSetBy,
          config: result.stack.config,
          hooks: result.stack.hooks,
          integrations: result.stack.integrations,
          attachedContexts: result.stack.attachedContexts,
          attachedPolicies: result.stack.attachedPolicies,
          dependsOn: result.stack.dependsOn,
          isDependedOnBy: result.stack.isDependedOnBy,
          scheduledRuns: result.stack.scheduledRuns,
          scheduledDeletes: result.stack.scheduledDeletes,
          scheduledTasks: result.stack.scheduledTasks,
          blueprint: result.stack.blueprint,
          outputs: result.stack.outputs,
          canWrite: result.stack.canWrite,
          starred: result.stack.starred,
          notificationCount: result.stack.notificationCount,
          createdAt: result.stack.createdAt,
          apiHost: result.stack.apiHost,
          isStateRollback: result.stack.isStateRollback,
        });
      },
    },
  },
  outputs: {
    default: {
      type: {
        type: "object",
        properties: {
          stackId: { type: "string" },
          name: { type: "string" },
          description: { oneOf: [{ type: "string" }, { type: "null" }] },
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
          repository: { type: "string" },
          repositoryUrl: { oneOf: [{ type: "string" }, { type: "null" }] },
          branch: { type: "string" },
          namespace: { type: "string" },
          provider: { type: "string" },
          projectRoot: { oneOf: [{ type: "string" }, { type: "null" }] },
          additionalProjectGlobs: { type: "array", items: { type: "string" } },
          repositoryTags: { type: "array", items: { type: "string" } },
          vcsDetached: { type: "boolean" },
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
          administrative: { type: "boolean" },
          autodeploy: { type: "boolean" },
          autoretry: { type: "boolean" },
          githubActionDeploy: { type: "boolean" },
          isDisabled: { type: "boolean" },
          localPreviewEnabled: { type: "boolean" },
          protectFromDeletion: { type: "boolean" },
          managesStateFile: { type: "boolean" },
          enableWellKnownSecretMasking: { type: "boolean" },
          enableSensitiveOutputUpload: { type: "boolean" },
          labels: { type: "array", items: { type: "string" } },
          runnerImage: { oneOf: [{ type: "string" }, { type: "null" }] },
          terraformVersion: { oneOf: [{ type: "string" }, { type: "null" }] },
          effectiveTerraformVersion: {
            oneOf: [{ type: "string" }, { type: "null" }],
          },
          vendorConfig: {
            oneOf: [
              {
                type: "object",
                additionalProperties: true,
              },
              { type: "null" },
            ],
          },
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
          state: { type: "string" },
          stateIsTerminal: { type: "boolean" },
          stateSetAt: { oneOf: [{ type: "number" }, { type: "null" }] },
          stateSetBy: { oneOf: [{ type: "string" }, { type: "null" }] },
          lastApplyAt: { oneOf: [{ type: "number" }, { type: "null" }] },
          blocked: { type: "boolean" },
          blocker: {
            oneOf: [
              {
                type: "object",
                properties: {
                  id: { type: "string" },
                  state: { type: "string" },
                  type: { type: "string" },
                  title: { type: "string" },
                },
                required: ["id", "state", "type", "title"],
              },
              { type: "null" },
            ],
          },
          isDrifted: { type: "boolean" },
          entityCount: { type: "number" },
          needsApproval: { type: "boolean" },
          deleted: { type: "boolean" },
          deleting: { type: "boolean" },
          lockedAt: { oneOf: [{ type: "number" }, { type: "null" }] },
          lockedBy: { oneOf: [{ type: "string" }, { type: "null" }] },
          lockNote: { oneOf: [{ type: "string" }, { type: "null" }] },
          trackedCommit: {
            oneOf: [
              {
                type: "object",
                properties: {
                  hash: { type: "string" },
                  tag: { type: "string" },
                  message: { type: "string" },
                  authorName: { type: "string" },
                  timestamp: { type: "number" },
                  url: { type: "string" },
                },
                required: [
                  "hash",
                  "tag",
                  "message",
                  "authorName",
                  "timestamp",
                  "url",
                ],
              },
              { type: "null" },
            ],
          },
          trackedBranchHead: {
            oneOf: [
              {
                type: "object",
                properties: {
                  hash: { type: "string" },
                  tag: { type: "string" },
                  message: { type: "string" },
                  authorName: { type: "string" },
                  timestamp: { type: "number" },
                  url: { type: "string" },
                },
                required: [
                  "hash",
                  "tag",
                  "message",
                  "authorName",
                  "timestamp",
                  "url",
                ],
              },
              { type: "null" },
            ],
          },
          trackedCommitSetBy: { type: "string" },
          config: {
            type: "array",
            items: {
              type: "object",
              properties: {
                id: { type: "string" },
                checksum: { type: "string" },
                createdAt: { oneOf: [{ type: "number" }, { type: "null" }] },
                runtime: { type: "boolean" },
                type: { type: "string" },
                value: { oneOf: [{ type: "string" }, { type: "null" }] },
                description: { type: "string" },
                writeOnly: { type: "boolean" },
                fileMode: { oneOf: [{ type: "string" }, { type: "null" }] },
              },
              required: [
                "id",
                "checksum",
                "runtime",
                "type",
                "description",
                "writeOnly",
              ],
            },
          },
          hooks: {
            type: "object",
            properties: {
              afterApply: { type: "array", items: { type: "string" } },
              beforeApply: { type: "array", items: { type: "string" } },
              afterInit: { type: "array", items: { type: "string" } },
              beforeInit: { type: "array", items: { type: "string" } },
              afterPlan: { type: "array", items: { type: "string" } },
              beforePlan: { type: "array", items: { type: "string" } },
              afterPerform: { type: "array", items: { type: "string" } },
              beforePerform: { type: "array", items: { type: "string" } },
              afterDestroy: { type: "array", items: { type: "string" } },
              beforeDestroy: { type: "array", items: { type: "string" } },
              afterRun: { type: "array", items: { type: "string" } },
            },
            required: [
              "afterApply",
              "beforeApply",
              "afterInit",
              "beforeInit",
              "afterPlan",
              "beforePlan",
              "afterPerform",
              "beforePerform",
              "afterDestroy",
              "beforeDestroy",
              "afterRun",
            ],
          },
          integrations: {
            type: "object",
            properties: {
              aws: {
                type: "object",
                properties: {
                  activated: { type: "boolean" },
                  assumedRoleArn: {
                    oneOf: [{ type: "string" }, { type: "null" }],
                  },
                  assumeRoleLink: {
                    oneOf: [{ type: "string" }, { type: "null" }],
                  },
                  assumeRolePolicyStatement: {
                    oneOf: [{ type: "string" }, { type: "null" }],
                  },
                  durationSeconds: {
                    oneOf: [{ type: "number" }, { type: "null" }],
                  },
                  externalID: { oneOf: [{ type: "string" }, { type: "null" }] },
                  generateCredentialsInWorker: { type: "boolean" },
                  region: { oneOf: [{ type: "string" }, { type: "null" }] },
                },
                required: ["activated", "generateCredentialsInWorker"],
              },
              awsV2: {
                type: "array",
                items: {
                  type: "object",
                  properties: {
                    id: { type: "string" },
                    integrationId: { type: "string" },
                    name: { type: "string" },
                    read: { type: "boolean" },
                    write: { type: "boolean" },
                    roleArn: { type: "string" },
                    durationSeconds: { type: "number" },
                    externalId: { type: "string" },
                    generateCredentialsInWorker: { type: "boolean" },
                    legacy: { type: "boolean" },
                  },
                  required: [
                    "id",
                    "integrationId",
                    "name",
                    "read",
                    "write",
                    "roleArn",
                    "durationSeconds",
                    "externalId",
                    "generateCredentialsInWorker",
                    "legacy",
                  ],
                },
              },
              azure: {
                type: "array",
                items: {
                  type: "object",
                  properties: {
                    id: { type: "string" },
                    integrationId: { type: "string" },
                    integrationName: { type: "string" },
                    integrationDisplayName: { type: "string" },
                    read: { type: "boolean" },
                    write: { type: "boolean" },
                    subscriptionId: {
                      oneOf: [{ type: "string" }, { type: "null" }],
                    },
                    defaultSubscriptionId: {
                      oneOf: [{ type: "string" }, { type: "null" }],
                    },
                  },
                  required: [
                    "id",
                    "integrationId",
                    "integrationName",
                    "integrationDisplayName",
                    "read",
                    "write",
                  ],
                },
              },
              gcp: {
                type: "object",
                properties: {
                  activated: { type: "boolean" },
                  serviceAccountEmail: {
                    oneOf: [{ type: "string" }, { type: "null" }],
                  },
                  tokenScopes: { type: "array", items: { type: "string" } },
                },
                required: ["activated", "tokenScopes"],
              },
              driftDetection: {
                oneOf: [
                  {
                    type: "object",
                    properties: {
                      deleted: { type: "boolean" },
                      reconcile: { type: "boolean" },
                      ignoreState: { type: "boolean" },
                      timezone: { type: "string" },
                      schedule: { type: "array", items: { type: "string" } },
                      nextSchedule: {
                        oneOf: [{ type: "number" }, { type: "null" }],
                      },
                    },
                    required: [
                      "deleted",
                      "reconcile",
                      "ignoreState",
                      "timezone",
                      "schedule",
                    ],
                  },
                  { type: "null" },
                ],
              },
              slack: {
                type: "object",
                properties: {
                  activated: { type: "boolean" },
                  channelNames: { type: "array", items: { type: "string" } },
                },
                required: ["activated", "channelNames"],
              },
              webhooks: {
                type: "array",
                items: {
                  type: "object",
                  properties: {
                    id: { type: "string" },
                    deleted: { type: "boolean" },
                    enabled: { type: "boolean" },
                    endpoint: { type: "string" },
                  },
                  required: ["id", "deleted", "enabled", "endpoint"],
                },
              },
            },
            required: ["aws", "awsV2", "azure", "gcp", "slack", "webhooks"],
          },
          attachedContexts: {
            type: "array",
            items: {
              type: "object",
              properties: {
                id: { type: "string" },
                contextId: { type: "string" },
                contextName: { type: "string" },
                contextDescription: {
                  oneOf: [{ type: "string" }, { type: "null" }],
                },
                contextLabels: { type: "array", items: { type: "string" } },
                priority: { type: "number" },
                isAutoattached: { type: "boolean" },
              },
              required: [
                "id",
                "contextId",
                "contextName",
                "contextLabels",
                "priority",
                "isAutoattached",
              ],
            },
          },
          attachedPolicies: {
            type: "array",
            items: {
              type: "object",
              properties: {
                id: { type: "string" },
                policyId: { type: "string" },
                policyName: { type: "string" },
                policyType: { type: "string" },
                policyLabels: { type: "array", items: { type: "string" } },
                isAutoattached: { type: "boolean" },
              },
              required: [
                "id",
                "policyId",
                "policyName",
                "policyType",
                "policyLabels",
                "isAutoattached",
              ],
            },
          },
          dependsOn: {
            type: "array",
            items: {
              type: "object",
              properties: {
                id: { type: "string" },
                stackId: { type: "string" },
                dependsOnStackId: { type: "string" },
                referenceCount: { type: "number" },
              },
              required: ["id", "stackId", "dependsOnStackId", "referenceCount"],
            },
          },
          isDependedOnBy: {
            type: "array",
            items: {
              type: "object",
              properties: {
                id: { type: "string" },
                stackId: { type: "string" },
                dependsOnStackId: { type: "string" },
                referenceCount: { type: "number" },
              },
              required: ["id", "stackId", "dependsOnStackId", "referenceCount"],
            },
          },
          scheduledRuns: {
            type: "array",
            items: {
              type: "object",
              properties: {
                id: { type: "string" },
                name: { type: "string" },
                timezone: { oneOf: [{ type: "string" }, { type: "null" }] },
                cronSchedule: {
                  oneOf: [
                    { type: "array", items: { type: "string" } },
                    { type: "null" },
                  ],
                },
                timestampSchedule: {
                  oneOf: [{ type: "number" }, { type: "null" }],
                },
                nextSchedule: { oneOf: [{ type: "number" }, { type: "null" }] },
              },
              required: ["id", "name"],
            },
          },
          scheduledDeletes: {
            type: "array",
            items: {
              type: "object",
              properties: {
                id: { type: "string" },
                shouldDeleteResources: { type: "boolean" },
                timezone: { oneOf: [{ type: "string" }, { type: "null" }] },
                cronSchedule: {
                  oneOf: [
                    { type: "array", items: { type: "string" } },
                    { type: "null" },
                  ],
                },
                timestampSchedule: {
                  oneOf: [{ type: "number" }, { type: "null" }],
                },
                nextSchedule: { oneOf: [{ type: "number" }, { type: "null" }] },
              },
              required: ["id", "shouldDeleteResources"],
            },
          },
          scheduledTasks: {
            type: "array",
            items: {
              type: "object",
              properties: {
                id: { type: "string" },
                command: { type: "string" },
                timezone: { oneOf: [{ type: "string" }, { type: "null" }] },
                cronSchedule: {
                  oneOf: [
                    { type: "array", items: { type: "string" } },
                    { type: "null" },
                  ],
                },
                timestampSchedule: {
                  oneOf: [{ type: "number" }, { type: "null" }],
                },
                nextSchedule: { oneOf: [{ type: "number" }, { type: "null" }] },
              },
              required: ["id", "command"],
            },
          },
          blueprint: {
            oneOf: [
              {
                type: "object",
                properties: {
                  id: { type: "string" },
                  name: { type: "string" },
                  description: {
                    oneOf: [{ type: "string" }, { type: "null" }],
                  },
                  state: { type: "string" },
                },
                required: ["id", "name", "state"],
              },
              { type: "null" },
            ],
          },
          outputs: {
            type: "array",
            items: {
              type: "object",
              properties: {
                id: { type: "string" },
                value: { oneOf: [{ type: "string" }, { type: "null" }] },
                sensitive: { type: "boolean" },
                description: { oneOf: [{ type: "string" }, { type: "null" }] },
              },
              required: ["id", "sensitive"],
            },
          },
          canWrite: { type: "boolean" },
          starred: { type: "boolean" },
          notificationCount: { type: "number" },
          createdAt: { type: "number" },
          apiHost: { oneOf: [{ type: "string" }, { type: "null" }] },
          isStateRollback: { type: "boolean" },
        },
        required: [
          "stackId",
          "name",
          "spaceId",
          "spaceDetails",
          "repository",
          "branch",
          "namespace",
          "provider",
          "additionalProjectGlobs",
          "repositoryTags",
          "vcsDetached",
          "administrative",
          "autodeploy",
          "autoretry",
          "githubActionDeploy",
          "isDisabled",
          "localPreviewEnabled",
          "protectFromDeletion",
          "managesStateFile",
          "enableWellKnownSecretMasking",
          "enableSensitiveOutputUpload",
          "labels",
          "state",
          "stateIsTerminal",
          "blocked",
          "isDrifted",
          "entityCount",
          "needsApproval",
          "deleted",
          "deleting",
          "trackedCommitSetBy",
          "config",
          "hooks",
          "integrations",
          "attachedContexts",
          "attachedPolicies",
          "dependsOn",
          "isDependedOnBy",
          "scheduledRuns",
          "scheduledDeletes",
          "scheduledTasks",
          "outputs",
          "canWrite",
          "starred",
          "notificationCount",
          "createdAt",
          "isStateRollback",
        ],
      },
    },
  },
};
