import { AppBlock, events } from "@slflows/sdk/v1";

export const webhookEvent: AppBlock = {
  name: "On Notification Policy Event",
  description:
    "Receive all events from the Spacelift notification policy. This is an escape hatch block that outputs the raw payload without any filtering.",
  category: "Events",
  entrypoint: true,
  onInternalMessage: async (input) => {
    const { payload } = input.message.body;
    if (!payload) {
      return;
    }

    await events.emit(payload);
  },
  outputs: {
    default: {
      name: "Webhook Event",
      description: "Raw webhook payload from Spacelift notification policy",
      type: {
        type: "object",
        additionalProperties: true,
      },
    },
  },
};
