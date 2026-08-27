import {
  Agent,
  InMemoryAgentAdapters,
  type ModelGateway,
  type ToolDefinition,
} from "purra";
import packageMetadata from "purra/package.json" with { type: "json" };

const scenario = "canonical-child-agent";
const parentSecret = "parent-private-secret";
const childInstruction = "Inspect child evidence.";
const toolCalls: Array<{
  name: string;
  arguments: { key: string };
  result: { key: string; value: string };
}> = [];
let rootModelCalls = 0;
let childModelCalls = 0;

const model: ModelGateway = {
  async invoke(request) {
    const child = request.messages.some((message) => message.content === childInstruction);
    const serializedMessages = JSON.stringify(request.messages);
    const hasToolResult = request.messages.some((message) => message.role === "tool");
    if (child) {
      childModelCalls += 1;
      if (serializedMessages.includes(parentSecret)) {
        throw new Error("child Agent received the parent's private prompt");
      }
    } else {
      rootModelCalls += 1;
    }

    if (request.messages.at(-1)?.attributes?.publicPresentation === true) {
      throw new Error("Agent Tree unexpectedly entered public presentation");
    }

    if (child) {
      if (request.tools.map((tool) => tool.name).join(",") !== "lookup") {
        throw new Error("child Agent did not receive exactly the allowed read tool");
      }
      if (!hasToolResult) {
        return {
          message: {
            role: "assistant",
            content: "",
            toolCalls: [{
              id: "child-lookup",
              name: "lookup",
              arguments: { key: "child-status" },
            }],
          },
          finishReason: "tool_calls",
        };
      }
      return {
        message: { role: "assistant", content: "child evidence ready" },
        finishReason: "stop",
      };
    }

    if (!hasToolResult) {
      return {
        message: {
          role: "assistant",
          content: "",
          toolCalls: [{
            id: "delegate-child",
            name: "delegateToAgents",
            arguments: {
              delegations: [{
                agentName: "evidence-reader",
                title: "Evidence reader",
                instruction: childInstruction,
                objective: "Read and report the child status.",
              }],
            },
          }],
        },
        finishReason: "tool_calls",
      };
    }
    if (!serializedMessages.includes("child evidence ready")) {
      throw new Error("parent Agent did not receive the child Agent result");
    }
    return {
      message: {
        role: "assistant",
        content: "Parent received: child evidence ready.",
      },
      finishReason: "stop",
    };
  },
};

const lookup = {
  name: "lookup",
  description: "Read one local child value.",
  inputSchema: {
    type: "object",
    properties: { key: { type: "string" } },
    required: ["key"],
    additionalProperties: false,
  },
  policy: { mode: "read", title: "Lookup" },
  run(input) {
    const arguments_ = input as { key: string };
    const result = { key: arguments_.key, value: "ready" };
    toolCalls.push({ name: "lookup", arguments: arguments_, result });
    return { content: result, effectState: "not_started" };
  },
} satisfies ToolDefinition;

const adapters = new InMemoryAgentAdapters();
const handle = await new Agent({
  model,
  tools: [lookup],
  runRepository: adapters.runs,
  outputPublisher: adapters.outputs,
  agentTree: {
    repository: adapters.runTree,
    rootAgentId: "sdk-parity-root-agent",
  },
}).submit({
  messages: [{ role: "user", content: parentSecret }],
  enabledTools: ["delegateToAgents"],
}, {
  budgets: { maxRunOutputTokens: null },
});
const result = await handle.result;
const descendants = await adapters.runTree.listDescendants(handle.runId);
const journal = await adapters.runs.listRootEvents(handle.runId, 0);

if (result.output !== "Parent received: child evidence ready." || result.rounds !== 2) {
  throw new Error("unexpected TypeScript parent Agent result");
}
if (
  descendants.length !== 1
  || descendants[0]?.status !== "done"
  || descendants[0].parentRunId !== handle.runId
  || descendants[0].runId === handle.runId
) {
  throw new Error("TypeScript child Agent did not complete as a canonical Child Run");
}
if (
  journal.some((event, index) => event.rootSequence !== index + 1)
  || !journal.some((event) => event.runId === descendants[0]?.runId)
) {
  throw new Error("TypeScript child Agent journal attribution is invalid");
}
if (rootModelCalls !== 2 || childModelCalls !== 2 || toolCalls.length !== 1) {
  throw new Error("unexpected TypeScript child Agent execution counts");
}

console.log(JSON.stringify({
  runtime: "typescript",
  install: "npm-tarball",
  version: packageMetadata.version,
  scenario,
  status: "completed",
  output: result.output,
  modelCalls: rootModelCalls + childModelCalls,
  toolCalls,
  childRuns: descendants.length,
}));
