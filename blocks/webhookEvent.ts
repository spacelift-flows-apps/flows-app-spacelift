import { AppBlock, events } from "@slflows/sdk/v1";

export const webhookEvent: AppBlock = {
  name: "On notification policy event",
  description:
    "Receive all events from the Spacelift notification policy. This is an escape hatch block that outputs the raw payload without any filtering.",
  category: "Events",
  onInternalMessage: async (input) => {
    const { payload } = input.message.body;
    if (!payload) {
      return;
    }

    await events.emit(payload);
  },
  outputs: {
    default: {
      name: "Webhook event",
      description: "Raw webhook payload from Spacelift notification policy",
      type: {
        type: "object",
        additionalProperties: true,
      },
    },
  },
};
