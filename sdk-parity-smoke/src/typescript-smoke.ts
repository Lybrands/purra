import { Agent, type ModelGateway, type ToolDefinition } from "purra";
import packageMetadata from "purra/package.json" with { type: "json" };

const scenario = "local-tool-roundtrip";
const toolCalls: Array<{
  name: string;
  arguments: { key: string };
  result: { key: string; value: string };
}> = [];
let modelCalls = 0;

const model: ModelGateway = {
  async invoke(request) {
    modelCalls += 1;
    if (modelCalls === 1) {
      return {
        message: {
          role: "assistant",
          content: "",
          toolCalls: [{
            id: "lookup-1",
            name: "lookup",
            arguments: { key: "status" },
          }],
        },
        finishReason: "tool_calls",
      };
    }
    if (request.tools.length === 0) {
      if (
        !request.messages.some((message) => message.content === "private TypeScript candidate")
        || request.messages.at(-1)?.role !== "developer"
      ) {
        throw new Error("public presentation context is incomplete");
      }
      return {
        message: { role: "assistant", content: "PurrA is ready." },
        finishReason: "stop",
      };
    }
    if (request.messages.at(-1)?.role !== "tool") {
      throw new Error("model did not receive the tool result");
    }
    return {
      message: { role: "assistant", content: "private TypeScript candidate" },
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
    const arguments_ = input as { key: string };
    const result = { key: arguments_.key, value: "ready" };
    toolCalls.push({ name: "lookup", arguments: arguments_, result });
    return { content: result, effectState: "not_started" };
  },
} satisfies ToolDefinition;

const handle = await new Agent({ model, tools: [lookup] }).submit({
  messages: [{ role: "user", content: "Check the local status." }],
}, {
  budgets: { maxRunOutputTokens: null },
});
const result = await handle.result;
const eventKinds: string[] = [];
for await (const event of handle.events()) eventKinds.push(event.kind);

for (const required of ["run.started", "tool.started", "tool.completed", "final", "run.completed"]) {
  if (!eventKinds.includes(required)) throw new Error(`missing event: ${required}`);
}
if (result.output !== "PurrA is ready." || result.rounds !== 3) {
  throw new Error("unexpected TypeScript Agent result");
}
if (JSON.stringify(result.messages).includes("private TypeScript candidate")) {
  throw new Error("private presentation candidate leaked into public messages");
}

console.log(JSON.stringify({
  runtime: "typescript",
  install: "npm-tarball",
  version: packageMetadata.version,
  scenario,
  status: "completed",
  output: result.output,
  modelCalls,
  toolCalls,
}));
