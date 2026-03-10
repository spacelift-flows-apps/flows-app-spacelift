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
    description: "The ID of the stack containing the run",
    type: "string",
    required: true,
    graphqlFieldKey: "stack",
  }),
  runId: defineSpaceliftInputConfig({
    name: "Run ID",
    description: "The ID of the run to approve",
    type: "string",
    required: false,
    graphqlFieldKey: "run",
  }),
  note: defineSpaceliftInputConfig({
    name: "Note",
    description: "Optional note for the approval",
    type: "string",
    required: false,
    graphqlFieldKey: "note",
  }),
};

const APPROVE_RUN_MUTATION = `
  mutation ApproveRun($stack: ID!, $run: ID, $note: String) {
    runReview(stack: $stack, run: $run, decision: APPROVE, note: $note) {
      id
      decision
      note
      timestamp
      author
    }
  }
`;

export const approveRun: AppBlock = {
  name: "Approve Run",
  description: "Approve a run that requires approval",
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
          APPROVE_RUN_MUTATION,
          variables,
        );

        await events.emit({
          reviewId: result.runReview.id,
          decision: result.runReview.decision,
          note: result.runReview.note,
          timestamp: result.runReview.timestamp,
          author: result.runReview.author,
        });
      },
    },
  },
  outputs: {
    default: {
      type: {
        type: "object",
        properties: {
          reviewId: { type: "string" },
          decision: { type: "string" },
          note: { type: "string" },
          timestamp: { type: "number" },
          author: { type: "string" },
        },
        required: ["reviewId", "decision", "timestamp", "author"],
      },
    },
  },
};
