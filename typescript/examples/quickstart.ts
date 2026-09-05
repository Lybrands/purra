import { Agent, RetrieverTool, type ModelCapabilitySnapshot, type ModelGateway, type Retriever } from "purra";

const capabilities: ModelCapabilitySnapshot = {
  schemaVersion: 2,
  profileId: "example:quickstart",
  providerProtocol: "custom",
  contextWindowTokens: 16_000,
  maxGenerationTokens: 512,
  thinkingTokenAccounting: "unknown",
  protocol: {
    reasoningControl: "selectable", reasoningReplay: "ignored", toolCalling: "supported",
    requiredToolChoice: "supported", parallelToolCalls: "supported", streaming: "unavailable",
    cancellation: "supported", assistantContentWithToolCalls: "optional", jsonSchemaLevel: "unknown",
    streamFinishSemantics: "normalized", usageSemantics: "normalized",
  },
};

let round = 0;
const model: ModelGateway = {
  capabilities,
  async invoke(request) {
    round += 1;
    if (round === 1) {
      return {
        message: {
          role: "assistant",
          content: "",
          toolCalls: [{ id: "lookup-1", name: "searchKnowledge", arguments: { query: "status" } }],
        },
        finishReason: "tool_calls",
        appliedGenerationLimit: request.outputBudget.maxGenerationTokens,
      };
    }
    return {
      message: { role: "assistant", content: "PurrA is ready." },
      finishReason: "stop",
      appliedGenerationLimit: request.outputBudget.maxGenerationTokens,
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
  budgets: { maxRunGenerationTokens: null },
});
const result = await handle.result;
const eventKinds: string[] = [];
for await (const event of handle.events()) eventKinds.push(event.kind);

if (result.output !== "PurrA is ready.") throw new Error("unexpected example result");
console.log("events:", eventKinds.join(", "));
console.log("result:", result.output);
