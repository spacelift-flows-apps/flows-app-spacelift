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
    description: "The ID of the stack to lock",
    type: "string",
    required: true,
    graphqlFieldKey: "id",
  }),
  note: defineSpaceliftInputConfig({
    name: "Lock Note",
    description: "Note for locking the stack",
    type: "string",
    required: false,
    graphqlFieldKey: "note",
  }),
};

const LOCK_STACK_MUTATION = `
  mutation LockStack($id: ID!, $note: String) {
    stackLock(id: $id, note: $note) {
      id
      lockedAt
      lockNote
    }
  }
`;

export const lockStack: AppBlock = {
  name: "Lock Stack",
  description: "Lock the stack for exclusive use.",
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
          LOCK_STACK_MUTATION,
          variables,
        );

        await events.emit({
          stackId: result.stackLock.id,
          locked: !!result.stackLock.lockedAt,
          lockNote: result.stackLock.lockNote,
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
          locked: { type: "boolean" },
          lockNote: { oneOf: [{ type: "string" }, { type: "null" }] },
        },
        required: ["stackId", "locked"],
      },
    },
  },
};
