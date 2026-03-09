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
    description: "The ID of the policy to get evaluations for",
    type: "string",
    required: true,
    graphqlFieldKey: "id",
  }),
};

const GET_POLICY_EVALUATIONS_QUERY = `
  query GetPolicyEvaluations($id: ID!) {
    policy(id: $id) {
      evaluationRecords {
        key
        outcome
        timestamp
      }
    }
  }
`;

export const getPolicyEvaluations: AppBlock = {
  name: "Get Policy Evaluations",
  description: "Retrieve evaluations for a policy",
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

        const result = await executeSpaceliftQuery(
          credentials,
          GET_POLICY_EVALUATIONS_QUERY,
          variables,
        );

        const evaluations = result.policy.evaluationRecords.map(
          (record: any) => ({
            key: record.key,
            outcome: record.outcome,
            timestamp: record.timestamp,
          }),
        );

        await events.emit({
          totalCount: result.policy.evaluationRecords.length,
          evaluations,
        });
      },
    },
  },
  outputs: {
    default: {
      type: {
        type: "object",
        properties: {
          totalCount: { type: "number" },
          evaluations: {
            type: "array",
            items: {
              type: "object",
              properties: {
                key: { type: "string" },
                outcome: { type: "string" },
                timestamp: { type: "number" },
              },
              required: ["key", "outcome", "timestamp"],
            },
          },
        },
        required: ["totalCount", "evaluations"],
      },
    },
  },
};
