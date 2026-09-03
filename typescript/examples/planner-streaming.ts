/** Deterministic fixture only: real Adapters must forward actual Provider records. */
import {
  Agent, InMemoryAgentAdapters, ModelWorkPlanner,
  type ModelGateway, type ModelCapabilitySnapshot, type OutputEvent,
} from "purra";

const capabilities: ModelCapabilitySnapshot = {
  schemaVersion: 1, profileId: "example:planner", providerProtocol: "custom",
  contextWindowTokens: 32768, maxCallOutputTokens: 512, thinkingTokenAccounting: "unknown",
  protocol: { reasoningControl: "selectable", reasoningReplay: "ignored", toolCalling: "supported",
    requiredToolChoice: "supported", parallelToolCalls: "supported", streaming: "supported", cancellation: "supported",
    assistantContentWithToolCalls: "optional", jsonSchemaLevel: "unknown", streamFinishSemantics: "normalized", usageSemantics: "normalized" },
};
function gate() {
  let resolve!: () => void;
  const promise = new Promise<void>((done) => { resolve = done; });
  return { promise, resolve };
}
function require(value: unknown): asserts value { if (!value) throw new Error("Example assertion failed"); }
let release = gate();
const model: ModelGateway = {
  capabilities,
  async invoke() { throw new Error("This example uses managed streams"); },
  stream(request, signal) {
    const planning = request.messages.some((message) => message.attributes?.planningContract);
    return { ...(request.outputLimit === undefined ? {} : { appliedOutputLimit: request.outputLimit.maxTokens }),
      async *[Symbol.asyncIterator]() {
        if (planning) {
          yield { contentDelta: JSON.stringify({ v: 1, type: "progress", text: "I will check the request's scope." }) + "\n" };
          let cancel!: () => void;
          try {
            const stopped = new Promise<void>((resolve) => { cancel = resolve; signal?.addEventListener("abort", cancel, { once: true }); });
            await Promise.race([release.promise, stopped]);
            if (signal?.aborted) return;
          } finally { signal?.removeEventListener("abort", cancel); }
          yield { contentDelta: JSON.stringify({ v: 1, type: "plan", plan: { workPlan: {
            title: "Answer", steps: [{ id: "answer", title: "Answer", type: "review", executor: "model" }],
          } } }) + "\n" };
        } else yield { contentDelta: "The answer is ready." };
        yield { finishReason: "stop" as const };
      },
    };
  },
};

const storage = new InMemoryAgentAdapters(); // Replace runs for durable canonical storage.
const agent = new Agent({ model, runRepository: storage.runs, outputPublisher: storage.outputs,
  planning: { plannerFactory: (tasks) => new ModelWorkPlanner(tasks) },
});
const input = { messages: [{ role: "user" as const, content: "Give a concise answer." }], planningMode: "planned" as const };
const options = { budgets: { maxRunOutputTokens: null } };
const handle = await agent.submit(input, options);
const live: OutputEvent[] = [];
for await (const event of handle.events()) {
  live.push(event);
  if (event.kind === "planning.progress") {
    require((await storage.runs.listEvents(handle.runId, 0)).some((stored) => stored.eventId === event.eventId));
    release.resolve();
  }
}
require((await handle.result).output === "The answer is ready.");
const replay: OutputEvent[] = [];
for await (const event of handle.events()) replay.push(event);
require(JSON.stringify(live) === JSON.stringify(replay));
require(live.some((event) => event.kind === "plan.updated")); // Admitted summary; private plan remains in storage.

release = gate();
const canceled = await agent.submit(input, options);
const result = canceled.result.catch(() => undefined);
for await (const event of canceled.events()) {
  if (event.kind === "planning.progress") { await canceled.cancel(); break; }
}
await result;
require((await canceled.snapshot()).status === "canceled");
const before = await storage.runs.listEvents(canceled.runId, 0);
release.resolve();
require(JSON.stringify(before) === JSON.stringify(await storage.runs.listEvents(canceled.runId, 0)));
