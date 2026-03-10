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
      "The ID of the run to confirm (optional, defaults to current blocking run)",
    type: "string",
    required: false,
    graphqlFieldKey: "run",
  }),
  note: defineSpaceliftInputConfig({
    name: "Note",
    description: "Optional note for the confirmation",
    type: "string",
    required: false,
    graphqlFieldKey: "note",
  }),
};

const CONFIRM_RUN_MUTATION = `
  mutation ConfirmRun($stack: ID!, $run: ID, $note: String) {
    runConfirm(stack: $stack, run: $run, note: $note) {
      id
      state
      title
      createdAt
      updatedAt
    }
  }
`;

export const confirmRun: AppBlock = {
  name: "Confirm Run",
  description: "Confirm a run that is waiting for confirmation",
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
          CONFIRM_RUN_MUTATION,
          variables,
        );

        await events.emit({
          runId: result.runConfirm.id,
          state: result.runConfirm.state,
          title: result.runConfirm.title,
          createdAt: result.runConfirm.createdAt,
          updatedAt: result.runConfirm.updatedAt,
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
