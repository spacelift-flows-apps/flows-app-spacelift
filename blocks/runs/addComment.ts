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
    description: "The ID of the run to comment on",
    type: "string",
    required: true,
    graphqlFieldKey: "run",
  }),
  body: defineSpaceliftInputConfig({
    name: "Comment",
    description: "The comment body (supports Markdown)",
    type: "string",
    required: true,
    graphqlFieldKey: "body",
  }),
};

const ADD_COMMENT_MUTATION = `
  mutation AddComment($stack: ID!, $run: ID!, $body: String!) {
    runComment(stack: $stack, run: $run, body: $body) {
      body
      createdAt
      username
    }
  }
`;

export const addComment: AppBlock = {
  name: "Add Comment to Run",
  description: "Add a comment to a run",
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
          ADD_COMMENT_MUTATION,
          variables,
        );

        await events.emit({
          body: result.runComment.body,
          createdAt: result.runComment.createdAt,
          username: result.runComment.username,
        });
      },
    },
  },
  outputs: {
    default: {
      type: {
        type: "object",
        properties: {
          body: { type: "string" },
          createdAt: { type: "number" },
          username: { type: "string" },
        },
        required: ["body", "createdAt", "username"],
      },
    },
  },
};
