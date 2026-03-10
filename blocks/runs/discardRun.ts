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
    description:
      "The ID of the run to discard (optional, defaults to current blocking run)",
    type: "string",
    required: false,
    graphqlFieldKey: "run",
  }),
  note: defineSpaceliftInputConfig({
    name: "Note",
    description: "Optional note for the discard action",
    type: "string",
    required: false,
    graphqlFieldKey: "note",
  }),
};

const DISCARD_RUN_MUTATION = `
  mutation DiscardRun($stack: ID!, $run: ID, $note: String) {
    runDiscard(stack: $stack, run: $run, note: $note) {
      id
      state
      title
      createdAt
      updatedAt
    }
  }
`;

export const discardRun: AppBlock = {
  name: "Discard Run",
  description: "Discard a run that's waiting to be applied",
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
          DISCARD_RUN_MUTATION,
          variables,
        );

        await events.emit({
          runId: result.runDiscard.id,
          state: result.runDiscard.state,
          title: result.runDiscard.title,
          createdAt: result.runDiscard.createdAt,
          updatedAt: result.runDiscard.updatedAt,
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
          title: { type: "string" },
          createdAt: { type: "number" },
          updatedAt: { type: "number" },
        },
        required: ["runId", "state", "title", "createdAt", "updatedAt"],
      },
    },
  },
};
