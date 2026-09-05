// Deterministic recovery audit; shared with the regression tests.
// Run: npm --prefix typescript run build && node scripts/audit_retrieval_evidence.mjs
// Uses deterministic gateways and in-memory repositories; no external services.
import assert from "node:assert/strict";
import { pathToFileURL } from "node:url";
import {
  Agent, AgentCapabilityGrant, InMemoryAgentAdapters, RetrieverTool,
  estimateMessagesTokens, prepareContext,
} from "../typescript/dist/index.js";

const OPTIONS = { budgets: { maxRunGenerationTokens: null }, deadlineAt: null };
const CHILD = {
  name: "evidence-worker", title: "Evidence worker",
  instruction: "Inspect audit evidence.", objective: "Retrieve and report.",
};
const FACT = "CONTEXT_EVIDENCE_AUDIT_MARKER";
const HIT = {
  id: "audit-hit", source: "audit-source", version: 3,
  content: "Tool retrieval evidence.", untrusted: true,
  metadata: { evidenceId: "retrieval:audit-source:audit-hit:3" },
};
const CAPABILITIES = {
  schemaVersion: 2, profileId: "audit:model", providerProtocol: "custom",
  contextWindowTokens: 16_000, maxGenerationTokens: 512,
  thinkingTokenAccounting: "unknown",
  protocol: {
    reasoningControl: "selectable", reasoningReplay: "ignored",
    toolCalling: "supported", requiredToolChoice: "supported",
    parallelToolCalls: "supported", streaming: "unavailable", cancellation: "supported",
    assistantContentWithToolCalls: "optional", jsonSchemaLevel: "unknown",
    streamFinishSemantics: "normalized", usageSemantics: "normalized",
  },
};

function contextOptions(counts, compression = true) {
  return {
    provider: {
      describeContextDemands() { return [{ name: "facts", desiredTokens: 256 }]; },
      buildContext() {
        counts.provider += 1;
        return { blocks: [{ name: "facts", content: FACT, untrusted: true,
          evidence: [{ evidenceId: "context-fact", source: "audit-source", itemId: "fact", version: "3" }],
        }] };
      },
    },
    ...(compression ? { compression: {
      compress(request) {
        counts.compression += 1;
        return { messages: request.messages };
      },
    } } : {}),
  };
}

function makeAgent(adapters, counts, invoke, options = {}) {
  const retrieval = new RetrieverTool({
    name: "searchKnowledge", description: "Search public audit data.",
    retriever: { async retrieve() { counts.retrieval += 1; return [HIT]; } },
  });
  return new Agent({
    model: { capabilities: options.capabilities ?? CAPABILITIES, invoke }, tools: [retrieval.definition],
    context: options.context?.(counts) ?? contextOptions(counts, options.compression), runRepository: adapters.runs,
    outputPublisher: adapters.outputs,
    agentTree: { repository: adapters.runTree, rootAgentId: "audit-root-agent" },
    ...(options.evidenceValidator === undefined
      ? {}
      : { evidenceValidator: options.evidenceValidator }),
  });
}

function turn(request, content, toolCalls) {
  return {
    message: { role: "assistant", content, ...(toolCalls === undefined ? {} : { toolCalls }) },
    finishReason: toolCalls === undefined ? "stop" : "tool_calls",
    appliedGenerationLimit: request.outputBudget.maxGenerationTokens,
  };
}

export async function captureAutomaticCheckpoint(options = {}) {
  const adapters = new InMemoryAgentAdapters();
  const counts = { provider: 0, compression: 0, retrieval: 0 };
  const childRequests = [];
  const agent = makeAgent(adapters, counts, async (request) => {
    const isChild = request.messages.some((message) => message.content === CHILD.instruction);
    if (isChild) childRequests.push(request);
    if (request.messages.some((message) => message.role === "tool")) return turn(request, "done");
    return turn(request, "", isChild
      ? [{ id: "audit-search", name: "searchKnowledge", arguments: { query: "evidence" } }]
      : [{ id: "audit-child", name: "delegateToAgents", arguments: { children: [{
        name: CHILD.name, title: CHILD.title, instruction: CHILD.instruction, objective: CHILD.objective,
      }] } }]);
  }, options);
  const handle = await agent.submit({ messages: [{ role: "user", content: "Audit retrieval." }] }, OPTIONS);
  await handle.result;
  const [child] = await adapters.runTree.listDescendants(handle.runId);
  const childSnapshot = await adapters.runs.get(child.runId);
  const rootSnapshot = await adapters.runs.get(handle.runId);
  const checkpoint = childSnapshot.executionCheckpoint;
  assert.ok(checkpoint, "the real Tool round must produce a checkpoint");
  assert.equal(counts.retrieval, 1);
  assert.ok(childRequests.every((request) => JSON.stringify(request.messages).includes(FACT)));
  assert.deepEqual(checkpoint.messages.find((message) => message.role === "tool").content.hits, [HIT]);
  assert.deepEqual(checkpoint.contextEvidence, [{
    evidenceId: HIT.metadata.evidenceId,
    contextBlock: "searchKnowledge",
    source: HIT.source,
    itemId: HIT.id,
    version: String(HIT.version),
  }]);
  const events = await adapters.runs.listRootEvents(handle.runId, 0);
  const receipts = events.filter((event) => event.runId === child.runId && event.kind === "invocation.started")
    .map((event) => event.payload.receipt);
  return { checkpoint, childSnapshot, rootSnapshot, capabilities: CAPABILITIES, report: {
    normalCallsReceiveContext: true,
    toolHitSavedInCheckpoint: true,
    contextBodySavedInCheckpoint: JSON.stringify(checkpoint).includes(FACT),
    normalInvocationEvidenceCount: receipts[0].contextEvidence.length,
  } };
}

export async function replayCheckpoint(captured, options = {}) {
  // Materialize a committed model_ready checkpoint under an expired worker lease.
  // This exercises the real recovery API, not an OS-process crash or real database.
  let now = 100;
  const adapters = new InMemoryAgentAdapters({ agentTreeClockMs: () => now });
  const counts = { provider: 0, compression: 0, retrieval: 0 };
  const received = [];
  const agent = makeAgent(adapters, counts, async (request) => {
    received.push(request);
    return turn(request, "recovered");
  }, options);
  const rootId = "audit-replay-root";
  await adapters.runs.begin({
    requestedRunId: rootId, agentId: "audit-root-agent", preset: captured.rootSnapshot.preset,
    deadlineAt: null, budgets: captured.rootSnapshot.budgets, metadata: {},
  });
  await adapters.runTree.beginRoot({
    runId: rootId, agentId: "audit-root-agent", name: "root", title: "Root",
    instruction: "Own the audit.", objective: "Replay a checkpoint.",
    capabilityGrant: new AgentCapabilityGrant(captured.rootSnapshot.preset.agentTree.capabilityGrant),
    idempotencyKey: `begin:${rootId}`,
  });
  const [child] = (await adapters.runTree.spawnAgents({
    parentRunId: rootId, idempotencyKey: "audit-child", children: [{
      ...CHILD,
      capabilityGrant: new AgentCapabilityGrant(captured.childSnapshot.preset.agentTree.capabilityGrant),
    }],
  })).items;
  await adapters.runTree.markWaiting(rootId);
  const claim = await adapters.runTree.claimRun(child.run.runId, { ownerId: "old-worker", leaseDurationMs: 10 });
  const lease = { leaseOwnerId: claim.leaseOwnerId, leaseEpoch: claim.leaseEpoch };
  await adapters.runs.begin({
    requestedRunId: claim.runId, rootRunId: rootId, agentId: claim.agentId,
    parentRunId: rootId, ...lease, preset: captured.childSnapshot.preset,
    deadlineAt: null, budgets: captured.rootSnapshot.budgets, metadata: {},
  });
  const checkpoint = JSON.parse(JSON.stringify(captured.checkpoint));
  const messages = checkpoint.messages;
  if (options.oversized) {
    // A legal transcript containing an old complete turn; ordinary projection
    // can trim it, but recovery must not send it directly past the input budget.
    messages.splice(1, 0,
      { role: "user", content: "Old request " + "x".repeat(40_000) },
      { role: "assistant", content: "Old response." });
  }
  if (options.protectedInput) messages.push({ role: "user", content: "x".repeat(40_000) });
  options.editCheckpoint?.(checkpoint);
  await adapters.runs.saveExecutionCheckpoint(claim.runId, {
    ...checkpoint, runId: claim.runId, messages,
  }, lease);
  now += 10;
  await agent.recoverAgentTreeRoot(rootId, {
    messages: [{ role: "user", content: "Recover audit." }],
  }, OPTIONS);
  const snapshot = await adapters.runs.get(claim.runId);
  const events = await adapters.runs.listRootEvents(rootId, 0);
  const receipt = events.find((event) => event.runId === claim.runId && event.kind === "invocation.started")?.payload.receipt;
  const request = received[0];
  return { snapshot, request, receipt, report: {
    status: snapshot.status, errorCode: snapshot.errorCode ?? null,
    contextProviderCalls: counts.provider, compressionCalls: counts.compression,
    retrieverCalls: counts.retrieval, modelCalls: received.length,
    toolHitRestored: request?.messages.some((message) => message.role === "tool") ?? false,
    contextBodyRestored: request === undefined ? false : JSON.stringify(request.messages).includes(FACT),
    invocationEvidenceCount: receipt?.contextEvidence.length ?? 0,
    providerInputEstimate: request === undefined ? null : estimateMessagesTokens(request.messages),
    declaredWindow: (options.capabilities ?? CAPABILITIES).contextWindowTokens,
  } };
}

async function projectionControl(checkpoint) {
  const messages = structuredClone(checkpoint.messages);
  messages.unshift({ role: "user", content: "old " + "x".repeat(40_000) }, { role: "assistant", content: "old answer" });
  const original = JSON.stringify(messages);
  const context = await prepareContext({}, {
    request: { messages }, tools: [], windowTokens: CAPABILITIES.contextWindowTokens, outputReserveTokens: 512,
  });
  const projected = await context.project(messages);
  assert.equal(JSON.stringify(messages), original);
  assert.ok(projected.length < messages.length);
  return { sourceUnchanged: true, oldTurnTrimmed: true, toolHitStillPresent: projected.some((message) => message.role === "tool") };
}

if (process.argv[1] !== undefined && import.meta.url === pathToFileURL(process.argv[1]).href) {
  const captured = await captureAutomaticCheckpoint();
  console.log(JSON.stringify({
    source: "built TypeScript SDK, deterministic in-memory diagnostic",
    automaticCheckpoint: captured.report,
    projection: await projectionControl(captured.checkpoint),
    recovery: (await replayCheckpoint(captured)).report,
    oversizedRecovery: (await replayCheckpoint(captured, { oversized: true })).report,
  }, null, 2));
}
