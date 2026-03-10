import { AppBlock, events } from "@slflows/sdk/v1";
import { executeSpaceliftQuery, extractCredentials } from "../../client";
import {
  defineSpaceliftInputConfig,
  mapInputConfig,
  mapInputsToGraphQLVariables,
} from "../../utils";

const inputConfig = {
  id: defineSpaceliftInputConfig({
    name: "Policy ID",
    description: "The ID of the policy to simulate",
    type: "string",
    required: true,
    graphqlFieldKey: "id",
  }),
  input: defineSpaceliftInputConfig({
    name: "Input",
    description: "The input data for the policy simulation (JSON)",
    type: {
      type: "object",
      additionalProperties: true,
    },
    required: true,
    graphqlFieldKey: "input",
  }),
};

const GET_POLICY_QUERY = `
  query GetPolicy($id: ID!) {
    policy(id: $id) {
      id
      body
      type
    }
  }
`;

const SIMULATE_POLICY_MUTATION = `
  mutation SimulatePolicy($body: String!, $input: String!, $type: PolicyType!) {
    policySimulate(body: $body, input: $input, type: $type)
  }
`;

export const simulatePolicy: AppBlock = {
  name: "Simulate Policy",
  description: "Test a policy with sample input data",
  category: "Policies",
  inputs: {
    default: {
      config: mapInputConfig(inputConfig),
      onEvent: async (input) => {
        const variables = mapInputsToGraphQLVariables(
          inputConfig,
          input.event.inputConfig,
        );
        const credentials = extractCredentials(input.app.config);

        const policyResult = await executeSpaceliftQuery(
          credentials,
          GET_POLICY_QUERY,
          { id: variables.id },
        );

        if (!policyResult.policy) {
          throw new Error(`Policy with ID ${variables.id} not found`);
        }

        let inputString: string;
        if (typeof variables.input === "string") {
          inputString = variables.input;
        } else {
          inputString = JSON.stringify(variables.input);
        }

        const simulateVariables = {
          body: policyResult.policy.body,
          input: inputString,
          type: policyResult.policy.type,
        };

        const result = await executeSpaceliftQuery(
          credentials,
          SIMULATE_POLICY_MUTATION,
          simulateVariables,
        );

        try {
          const parsed = JSON.parse(result.policySimulate);

          await events.emit({
            result: parsed,
            type: policyResult.policy.type,
          });
        } catch {
          throw new Error("Failed to parse simulation result");
        }
      },
    },
  },
  outputs: {
    default: {
      type: {
        type: "object",
        properties: {
          type: {
            type: "string",
            enum: [
              "LOGIN",
              "ACCESS",
              "INITIALIZATION",
              "PLAN",
              "TASK",
              "PUSH",
              "TRIGGER",
              "NOTIFICATION",
            ],
          },
          result: {
            type: "object",
            properties: {
              // Base decision properties (inherited by all)
              flag: { type: "array", items: { type: "string" } },
              sample: { type: "boolean" },

              // Approval policy
              approve: { type: "boolean" },
              reject: { type: "boolean" },

              // Login policy
              access_level: {
                type: "string",
                enum: ["deny", "allow", "admin"],
              },
              teams: { type: "array", items: { type: "string" } },
              space_admin: { type: "array", items: { type: "string" } },
              space_write: { type: "array", items: { type: "string" } },
              space_read: { type: "array", items: { type: "string" } },
              roles: {
                type: "object",
                additionalProperties: true,
              },

              // Stack Access policy
              level: { type: "string", enum: ["deny", "read", "write"] },

              // Rejection policy (Initialization & Task policies)
              violations: { type: "array", items: { type: "string" } },

              // Plan policy
              deny: { type: "array", items: { type: "string" } },
              warn: { type: "array", items: { type: "string" } },

              // Push policy
              action: { type: "string", enum: ["ignore", "propose", "track"] },
              ignore_reason: { type: "number" },
              allow_fork: { type: "boolean" },
              cancel: { type: "array", items: { type: "string" } },
              external_dependencies: {
                type: "array",
                items: { type: "string" },
              },
              fail: { type: "boolean" },
              messages: { type: "array", items: { type: "string" } },
              module_version: { type: "string" },
              notify: { type: "boolean" },
              no_trigger: { type: "boolean" },
              prioritize: { type: "boolean" },
              lock: { type: "string" },
              unlock: { type: "array", items: { type: "string" } },

              // Trigger policy
              stack_slugs: { type: "array", items: { type: "string" } },

              // Notification policy
              inbox: {
                type: "array",
                items: {
                  type: "object",
                  properties: {
                    title: { type: "string" },
                    body: { type: "string" },
                    severity: { type: "string" },
                  },
                },
              },
              slack: {
                type: "array",
                items: {
                  type: "object",
                  properties: {
                    channel_id: { type: "string" },
                    message: { type: "string" },
                    mention_users: { type: "array", items: { type: "string" } },
                    mention_groups: {
                      type: "array",
                      items: { type: "string" },
                    },
                  },
                  required: ["channel_id"],
                },
              },
              webhook: {
                type: "array",
                items: {
                  type: "object",
                  properties: {
                    endpoint_id: { type: "string" },
                    headers: {
                      type: "object",
                      additionalProperties: true,
                    },
                    method: { type: "string" },
                    payload: { type: "object", additionalProperties: true },
                  },
                  required: ["endpoint_id"],
                },
              },
              pull_request: {
                type: "array",
                items: {
                  type: "object",
                  properties: {
                    id: { type: "number" },
                    branch: { type: "string" },
                    commit: { type: "string" },
                    body: { type: "string" },
                    truncate_body: { type: "boolean" },
                    deduplication_key: { type: "string" },
                  },
                },
              },
            },
            required: ["flag", "sample"],
          },
        },
        required: ["type", "result"],
      },
    },
  },
};
