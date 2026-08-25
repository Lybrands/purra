import { Agent, type ModelGateway, type ToolDefinition } from "purra";

let round = 0;
const model: ModelGateway = {
  async invoke() {
    round += 1;
    if (round === 1) {
      return {
        message: {
          role: "assistant",
          content: "",
          toolCalls: [{ id: "lookup-1", name: "lookup", arguments: { key: "status" } }],
        },
        finishReason: "tool_calls",
      };
    }
    return {
      message: { role: "assistant", content: "PurrA is ready." },
      finishReason: "stop",
    };
  },
};

const lookup = {
  name: "lookup",
  description: "Look up one local value.",
  inputSchema: {
    type: "object",
    properties: { key: { type: "string" } },
    required: ["key"],
    additionalProperties: false,
  },
  policy: { mode: "read", title: "Look up" },
  run(input) {
    const { key } = input as { readonly key: string };
    return { content: { key, value: "ready" }, effectState: "not_started" };
  },
} satisfies ToolDefinition;

const handle = await new Agent({ model, tools: [lookup] }).submit({
  messages: [{ role: "user", content: "Check the local status." }],
});
const result = await handle.result;
const eventKinds: string[] = [];
for await (const event of handle.events()) eventKinds.push(event.kind);

if (result.output !== "PurrA is ready.") throw new Error("unexpected example result");
console.log("events:", eventKinds.join(", "));
console.log("result:", result.output);
