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
    description: "The ID of the stack to start a run for",
    type: "string",
    required: true,
    graphqlFieldKey: "stack",
  }),
  runType: defineSpaceliftInputConfig({
    name: "Run Type",
    description: "Type of run to start",
    type: {
      enum: ["PROPOSED", "TRACKED", "DESTROY"],
    },
    required: false,
    graphqlFieldKey: "runType",
  }),
  commitSha: defineSpaceliftInputConfig({
    name: "Commit SHA",
    description: "Specific commit SHA to run (optional)",
    type: "string",
    required: false,
    graphqlFieldKey: "commitSha",
  }),
};

const START_RUN_MUTATION = `
  mutation StartRun($stack: ID!, $runType: RunType, $commitSha: String) {
    runTrigger(stack: $stack, runType: $runType, commitSha: $commitSha) {
      id
      createdAt
      updatedAt
      state
      type
      commit {
        hash
      }
    }
  }
`;

export const startRun: AppBlock = {
  name: "Start Run",
  description: "Start a new run on a Spacelift stack",
  category: "Runs",
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
          START_RUN_MUTATION,
          variables,
        );

        await events.emit({
          runId: result.runTrigger.id,
          state: result.runTrigger.state,
          type: result.runTrigger.type,
          commitSha: result.runTrigger.commit?.hash || null,
          createdAt: result.runTrigger.createdAt,
          updatedAt: result.runTrigger.updatedAt,
        });
      },
    },
  },
  outputs: {
    default: {
      type: {
        type: "object",
        properties: {
          runId: { type: "string" },
          state: { type: "string" },
          type: { type: "string" },
          commitSha: { oneOf: [{ type: "string" }, { type: "null" }] },
          createdAt: { type: "string" },
          updatedAt: { type: "string" },
        },
        required: ["runId", "state", "type", "createdAt", "updatedAt"],
      },
    },
  },
};
