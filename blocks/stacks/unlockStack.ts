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
    description: "The ID of the stack to unlock",
    type: "string",
    required: true,
    graphqlFieldKey: "id",
  }),
};

const UNLOCK_STACK_MUTATION = `
  mutation UnlockStack($id: ID!) {
    stackUnlock(id: $id) {
      id
      lockedAt
    }
  }
`;

export const unlockStack: AppBlock = {
  name: "Unlock Stack",
  description: "Unlock a stack to allow runs",
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
          UNLOCK_STACK_MUTATION,
          variables,
        );

        await events.emit({
          stackId: result.stackUnlock.id,
          locked: !!result.stackUnlock.lockedAt,
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
        },
        required: ["stackId", "locked"],
      },
    },
  },
};
