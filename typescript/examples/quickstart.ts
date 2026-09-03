import { Agent, RetrieverTool, type ModelGateway, type Retriever } from "purra";

let round = 0;
const model: ModelGateway = {
  async invoke() {
    round += 1;
    if (round === 1) {
      return {
        message: {
          role: "assistant",
          content: "",
          toolCalls: [{ id: "lookup-1", name: "searchKnowledge", arguments: { query: "status" } }],
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

const localRetriever: Retriever = {
  async retrieve() {
    return [{
      id: "status",
      content: "PurrA is ready.",
      source: "local-example",
      untrusted: true,
      metadata: {},
    }];
  },
};
const retrieval = new RetrieverTool({
  retriever: localRetriever,
  name: "searchKnowledge",
  description: "Search the configured local knowledge source.",
  scope: { namespace: "example.quickstart" },
});

const handle = await new Agent({ model, tools: [retrieval.definition] }).submit({
  messages: [{ role: "user", content: "Check the local status." }],
}, {
  budgets: { maxRunOutputTokens: null },
});
const result = await handle.result;
const eventKinds: string[] = [];
for await (const event of handle.events()) eventKinds.push(event.kind);

if (result.output !== "PurrA is ready.") throw new Error("unexpected example result");
console.log("events:", eventKinds.join(", "));
console.log("result:", result.output);
