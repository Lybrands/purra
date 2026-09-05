import assert from "node:assert/strict";
import test from "node:test";
import { AgentError, estimateMessagesTokens, prepareContext } from "purra";
import { restoreContext } from "../dist/context/coordinator.js";
import { captureAutomaticCheckpoint, replayCheckpoint } from "../../scripts/audit_retrieval_evidence.mjs";

test("Child Run recovery restores resolved context and receipts without requerying data", async () => {
  const captured = await captureAutomaticCheckpoint();
  assert.equal(captured.report.contextBodySavedInCheckpoint, true);
  const recovered = await replayCheckpoint(captured, {
    context(counts) {
      return {
        providerFactory() { throw new Error("Do not reopen the data source during recovery"); },
        compressionFactory() {
          return { compress(request) { counts.compression++; return { messages: request.messages }; } };
        },
      };
    },
  });
  assert.equal(recovered.report.status, "completed");
  assert.equal(recovered.report.contextBodyRestored, true);
  assert.equal(recovered.report.contextProviderCalls, 0);
  assert.equal(recovered.report.compressionCalls, 1);
  assert.equal(recovered.report.retrieverCalls, 0);
  assert.deepEqual(recovered.receipt.contextEvidence, [{
    evidenceId: "retrieval:audit-source:audit-hit:3", contextBlock: "searchKnowledge",
    source: "audit-source", itemId: "audit-hit", version: "3",
  }, {
    evidenceId: "context-fact", contextBlock: "facts", source: "audit-source", itemId: "fact", version: "3",
  }]);
  assert.equal(recovered.request.messages.find((m) => m.attributes?.contextBlockName === "facts").attributes.untrusted, true);
  assert.deepEqual(recovered.request.messages.find((m) => m.role === "tool"),
    captured.checkpoint.messages.find((m) => m.role === "tool"));
});

test("Child Run recovery revalidates checkpointed tool evidence before Provider dispatch", async () => {
  const captured = await captureAutomaticCheckpoint();
  const validations = [];
  const recovered = await replayCheckpoint(captured, {
    evidenceValidator: {
      validateEvidence(receipts) {
        validations.push(receipts);
        throw new AgentError("external_evidence_stale", "stale evidence");
      },
    },
  });

  assert.equal(recovered.report.status, "failed");
  assert.equal(recovered.report.errorCode, "external_evidence_stale");
  assert.equal(recovered.report.modelCalls, 0);
  assert.equal(validations.length, 1);
  assert.equal(
    validations[0].some((receipt) => (
      receipt.evidenceId === "retrieval:audit-source:audit-hit:3"
    )),
    true,
  );
});

test("recovered context rejects an oversized custom compression result before invoking the model", async () => {
  const recovered = await replayCheckpoint(await captureAutomaticCheckpoint(), { oversized: true });
  assert.equal(recovered.report.status, "failed");
  assert.equal(recovered.report.errorCode, "context_compression_result_exceeds_budget");
  assert.equal(recovered.report.modelCalls, 0);
  assert.equal(recovered.report.compressionCalls, 1);
  assert.equal(recovered.report.contextProviderCalls, 0);
});

test("recovered default projection trims only old turns and protects the current tool exchange", async () => {
  const captured = await captureAutomaticCheckpoint({ compression: false });
  const recovered = await replayCheckpoint(captured, { oversized: true, compression: false });
  assert.equal(recovered.report.status, "completed");
  assert.equal(recovered.report.contextBodyRestored, true);
  assert.equal(recovered.request.messages.some((m) => typeof m.content === "string" && m.content.startsWith("Old request")), false);
  assert.equal(recovered.snapshot.executionCheckpoint.messages.some((m) => typeof m.content === "string" && m.content.startsWith("Old request")), true);
  assert.deepEqual(recovered.request.messages.find((m) => m.role === "tool"),
    captured.checkpoint.messages.find((m) => m.role === "tool"));
  assert.ok(estimateMessagesTokens(recovered.request.messages) < recovered.report.declaredWindow - 512);
  const protectedRun = await replayCheckpoint(captured, { protectedInput: true, compression: false });
  assert.equal(protectedRun.report.errorCode, "protected_messages_exceed_compression_budget");
  assert.equal(protectedRun.report.modelCalls, 0);
});

test("context snapshot roundtrip copies evidence and preserves the spent compaction budget", async () => {
  const block = { name: "facts", content: "original", untrusted: true,
    evidence: [{ evidenceId: "fact", source: "data", version: "1" }] };
  const options = {
    provider: { buildContext() { return { blocks: [block] }; } },
    triggerRatio: 0.001, maxCompactions: 1,
  };
  const input = { request: { messages: [{ role: "user", content: "current" }] },
    tools: [], windowTokens: 16_000, outputReserveTokens: 512 };
  const prepared = await prepareContext(options, input);
  await prepared.project(input.request.messages);
  const snapshot = JSON.parse(JSON.stringify(prepared.snapshot()));
  block.content = "updated source";
  block.evidence[0].version = "2";
  assert.equal(snapshot.blocks[0].content, "original");
  assert.equal(snapshot.blocks[0].evidence[0].version, "1");
  assert.equal(snapshot.compactions, 1);
  const restored = restoreContext(options, input, snapshot);
  snapshot.blocks[0].content = "mutated snapshot";
  assert.equal(restored.snapshot().blocks[0].content, "original");
  await assert.rejects(restored.project(input.request.messages), { code: "context_compaction_budget_exceeded" });
});

test("recovery validates snapshot shape and fails closed when resolved context is absent", async () => {
  const captured = await captureAutomaticCheckpoint();
  for (const editCheckpoint of [
    (c) => { c.schemaVersion = 1; },
    (c) => { delete c.context; },
    (c) => { delete c.context.summary; },
    (c) => { c.context.summary = { name: "summary", content: 7 }; },
    (c) => { c.context.summary = { name: "facts", content: "overwrite" }; },
    (c) => { c.context.compactions = -1; },
    (c) => { c.context.contextAllocations.facts = -1; },
    (c) => { c.context.blocks[0].content = 7; },
    (c) => { c.context.blocks[0].untrusted = "false"; },
    (c) => { c.context.blocks[0].evidence.push(c.context.blocks[0].evidence[0]); },
  ]) {
    await assert.rejects(replayCheckpoint(captured, { editCheckpoint }), TypeError);
  }
  const absent = await replayCheckpoint(captured, { editCheckpoint(c) { c.context = null; } });
  assert.equal(absent.report.errorCode, "agent_execution_checkpoint_conflict");
  assert.equal(absent.report.contextProviderCalls, 0);
  assert.equal(absent.report.modelCalls, 0);
});

test("recovery recomputes its input budget using the rebound model and output reserve", async () => {
  const captured = await captureAutomaticCheckpoint({ compression: false });
  const recovered = await replayCheckpoint(captured, {
    compression: false,
    capabilities: { ...captured.capabilities, maxGenerationTokens: 8_000 },
  });
  assert.equal(recovered.report.errorCode, "minimum_context_demand_exceeds_pool");
  assert.equal(recovered.report.modelCalls, 0);
  assert.equal(recovered.report.contextProviderCalls, 0);
});

test("custom summary and provenance survive recovery until explicitly replaced or cleared", async () => {
  const summary = { name: "summary", content: "SAVED_SUMMARY", untrusted: true,
    evidence: [{ evidenceId: "summary-fact", source: "archive", itemId: "fact", version: "3" }] };
  const captured = await captureAutomaticCheckpoint({ context() {
    return {
      provider: { buildContext() { return { blocks: [{ name: "facts", content: "CONTEXT_EVIDENCE_AUDIT_MARKER" }] }; } },
      compression: { compress(request) { return { messages: request.messages, summary }; } },
    };
  } });
  assert.equal(captured.checkpoint.context.summary?.content, "SAVED_SUMMARY");
  const recovered = await replayCheckpoint(captured, { context(counts) {
    return { compression: { compress(request) {
      counts.compression++;
      assert.equal(request.previousSummary.content, "SAVED_SUMMARY");
      return { messages: request.messages };
    } } };
  } });
  assert.equal(recovered.report.status, "completed");
  assert.equal(recovered.request.messages.filter((m) => m.attributes?.contextBlockName === "summary").length, 1);
  assert.equal(
    recovered.receipt.contextEvidence.some((item) => item.evidenceId === "summary-fact"),
    true,
  );
  assert.equal(recovered.report.retrieverCalls, 0);
  assert.equal(recovered.report.contextProviderCalls, 0);
  const cleared = await replayCheckpoint(captured, { context() {
    return { compression: { compress(request) { return { messages: request.messages, summary: null }; } } };
  } });
  assert.equal(cleared.report.status, "completed");
  assert.equal(cleared.request.messages.some((m) => m.attributes?.contextBlockName === "summary"), false);
  assert.deepEqual(
    cleared.receipt.contextEvidence.map((item) => item.evidenceId),
    ["retrieval:audit-source:audit-hit:3"],
  );
});

test("summary updates stay untrusted and commit only after protocol, identity and budget validation", async () => {
  const first = { name: "summary", content: "original", untrusted: false,
    evidence: [{ evidenceId: "summary-1", source: "archive", version: "1" }] };
  let next = first;
  const context = await prepareContext({
    provider: { buildContext() { return { blocks: [{ name: "facts", content: "fact",
      evidence: [{ evidenceId: "fact-1", source: "archive" }],
    }] }; } },
    compression: { compress(request) { return { messages: request.messages, ...(next === undefined ? {} : { summary: next }) }; } },
  }, { request: { messages: [{ role: "user", content: "read" }] }, tools: [], windowTokens: 16_000, outputReserveTokens: 512 });
  const messages = [{ role: "user", content: "read" }];
  await context.project(messages);
  assert.equal(context.snapshot().summary.untrusted, true);
  first.content = "source changed";
  next = undefined;
  assert.match((await context.project(messages)).find((m) => m.attributes?.contextBlockName === "summary").content, /original/);
  for (const invalid of [
    { name: "facts", content: "replace fixed facts" },
    { name: "summary", content: "duplicate identity", evidence: [{ evidenceId: "fact-1", source: "other" }] },
    { name: "summary", content: "x".repeat(40_000) },
  ]) {
    next = invalid;
    await assert.rejects(context.project(messages));
    assert.equal(context.snapshot().summary.content, "original");
    assert.equal(context.evidence.at(-1).evidenceId, "summary-1");
  }
  next = { name: "summary", content: "replacement", evidence: [{ evidenceId: "summary-2", source: "archive", version: "2" }] };
  await context.project(messages);
  assert.deepEqual(context.evidence.map((e) => e.evidenceId), ["fact-1", "summary-2"]);
  next = null;
  await context.project(messages);
  assert.equal(context.snapshot().summary, null);
  assert.deepEqual(context.evidence.map((e) => e.evidenceId), ["fact-1"]);
});
