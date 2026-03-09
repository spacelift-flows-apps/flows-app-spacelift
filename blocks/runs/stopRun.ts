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
    description: "The ID of the run to stop",
    type: "string",
    required: true,
    graphqlFieldKey: "run",
  }),
  note: defineSpaceliftInputConfig({
    name: "Note",
    description: "Optional note for stopping the run",
    type: "string",
    required: false,
    graphqlFieldKey: "note",
  }),
};

const STOP_RUN_MUTATION = `
  mutation StopRun($stack: ID!, $run: ID!, $note: String) {
    runStop(stack: $stack, run: $run, note: $note) {
      id
      state
      title
      createdAt
      updatedAt
    }
  }
`;

export const stopRun: AppBlock = {
  name: "Stop Run",
  description: "Stop a run that's currently in progress",
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
          STOP_RUN_MUTATION,
          variables,
        );

        await events.emit({
          runId: result.runStop.id,
          state: result.runStop.state,
          title: result.runStop.title,
          createdAt: result.runStop.createdAt,
          updatedAt: result.runStop.updatedAt,
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
