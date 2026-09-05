import assert from "node:assert/strict";
import { mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { Agent, AgentCapabilityGrant, UserInputRequired } from "purra";
import { SqliteAgentAdapters } from "../dist/index.js";
import { testGateway } from "../../../../typescript/test/support/model-gateway.mjs";

const { cases } = JSON.parse(readFileSync(new URL("../../../../conformance/fixtures/run_resume.json", import.meta.url), "utf8"));
const request = { messages: [{ role: "user", content: "Look up the value" }], planningMode: "reactive" };
const options = { budgets: { maxRunGenerationTokens: null } };

function host(path, { revision = "1", leased = true, pause = false, tree = false, firstSnapshot } = {}) {
  const storage = new SqliteAgentAdapters(path, { scope: "resume" });
  const counts = { model: 0, tool: 0 };
  const model = testGateway({
    async invoke(input) {
      counts.model++;
      if (input.messages.some(m => m.role === "tool") || input.tools.length === 0) {
        return { message: { role: "assistant", content: "42" }, finishReason: "stop" };
      }
      return { message: { role: "assistant", content: "", toolCalls: [{ id: "lookup-1", name: "lookup", arguments: {} }] }, finishReason: "tool_calls" };
    },
  });
  const repository = new Proxy(storage.runs, {
    get(target, key) {
      if (key === "executeOwned" && !leased) return undefined;
      if (key === "get" && firstSnapshot !== undefined) return async id => {
        assert.equal(id, firstSnapshot.runId);
        const saved = firstSnapshot; firstSnapshot = undefined;
        return saved;
      };
      return target[key];
    },
  });
  const agent = new Agent({
    model, preset: { id: "resume", revision }, runRepository: repository,
    outputPublisher: storage.publisher,
    ...(tree ? { agentTree: { repository: storage.runTree } } : {}),
    tools: [{ name: "lookup", description: "Read the value",
      inputSchema: { type: "object", properties: {} }, policy: { mode: "read", title: "Lookup" },
      async run() { counts.tool++; return { content: "42" }; },
    }],
    ...(pause ? { checkpointHandler: async checkpoint => { throw new UserInputRequired(checkpoint.runId, "pause"); } } : {}),
  });
  return { storage, counts, agent };
}

async function paused(path) {
  const first = host(path, { pause: true });
  try {
    const handle = await first.agent.submit(request, options);
    await assert.rejects(handle.result, UserInputRequired);
    assert.deepEqual(first.counts, { model: 1, tool: 1 });
    return handle.runId;
  } finally { first.storage.close(); }
}

for (const scenario of cases) test(`public resume: ${scenario.id} preserves journal without Provider calls`, async () => {
  const dir = mkdtempSync(join(tmpdir(), "purra-resume-"));
  const path = join(dir, "agent.db");
  let current;
  let releaseOwner;
  let owner;
  try {
    let id = await paused(path);
    current = host(path, { revision: scenario.id === "preset-mismatch" ? "2" : "1", leased: scenario.id !== "lease-required" });
    const { storage, agent, counts } = current;
    if (scenario.id === "checkpoint-missing") {
      const { preset, budgets, deadlineAt, metadata } = await storage.runs.get(id);
      const started = await storage.runs.begin({ preset, budgets, deadlineAt, metadata });
      id = started.snapshot.runId;
    } else if (scenario.id === "terminal") {
      await (await agent.resume(id, request)).result;
    } else if (scenario.id === "lease-conflict") {
      let entered;
      const ready = new Promise(resolve => { entered = resolve; });
      const gate = new Promise(resolve => { releaseOwner = resolve; });
      owner = storage.runs.executeOwned(id, async () => { entered(); await gate; });
      await ready;
    } else if (scenario.id === "unreconciled-attempt") {
      await storage.runs.openInvocation(id, {
        schemaVersion: 2, runId: id, invocationId: "unfinished-attempt",
        messageFingerprint: "messages", toolFingerprint: "tools", requestFingerprint: "request",
        evidenceFingerprint: "evidence", contextEvidence: [], capabilityProfileId: null, outputBudget: null,
      });
    }
    const before = await storage.runs.get(id);
    const events = await storage.runs.listEvents(id, 0);
    const previousCounts = { ...counts };
    await assert.rejects(async () => { await (await agent.resume(id, request)).result; }, { code: scenario.errorCode });
    assert.deepEqual(counts, previousCounts);
    assert.deepEqual(await storage.runs.get(id), before);
    assert.deepEqual(await storage.runs.listEvents(id, 0), events);
  } finally {
    releaseOwner?.();
    await owner;
    current?.storage.close();
    rmSync(dir, { recursive: true });
  }
});

test("public resume after reopen preserves committed tool result, budget and journal prefix", async () => {
  const dir = mkdtempSync(join(tmpdir(), "purra-resume-"));
  let current;
  try {
    const path = join(dir, "agent.db");
    const id = await paused(path);
    current = host(path);
    const { storage, agent, counts } = current;
    const before = await storage.runs.get(id);
    const events = await storage.runs.listEvents(id, 0);
    const result = await (await agent.resume(id, request)).result;
    assert.equal(result.output, "42");
    assert.ok(counts.model > 0);
    assert.equal(counts.tool, 0);
    const after = await storage.runs.get(id);
    assert.equal(after.status, "completed");
    assert.equal(after.deadlineAt, before.deadlineAt);
    assert.deepEqual(after.budgets, before.budgets);
    assert.ok(after.usage.modelAttempts > before.usage.modelAttempts);
    assert.deepEqual((await storage.runs.listEvents(id, 0)).slice(0, events.length), events);
  } finally { current?.storage.close(); rmSync(dir, { recursive: true }); }
});


test("Child resume requires the Root scheduler before execution", async () => {
  const dir = mkdtempSync(join(tmpdir(), "purra-child-resume-"));
  const current = host(join(dir, "agent.db"), { tree: true });
  try {
    const tree = current.storage.runTree;
    await tree.beginRoot({ runId: "root", agentId: "root-agent", name: "root", title: "Root",
      instruction: "Own the task", objective: "Read the value", idempotencyKey: "begin",
      capabilityGrant: new AgentCapabilityGrant({ canSpawnAgents: true }),
    });
    const receipt = await tree.spawnAgents({ parentRunId: "root", idempotencyKey: "spawn",
      children: [{ name: "child", title: "Child", instruction: "Read", objective: "Read" }],
    });
    const child = receipt.items[0].run;
    await assert.rejects(current.agent.resume(child.runId, request), { code: "child_run_resume_requires_scheduler" });
    assert.deepEqual(current.counts, { model: 0, tool: 0 });
    assert.deepEqual(await tree.getRun(child.runId), child);
  } finally { current.storage.close(); rmSync(dir, { recursive: true }); }
});

test("resume rejects a checkpoint changed since the initial repository read", async () => {
  const dir = mkdtempSync(join(tmpdir(), "purra-checkpoint-race-"));
  let current;
  try {
    const path = join(dir, "agent.db");
    const id = await paused(path);
    current = host(path);
    const stale = await current.storage.runs.get(id);
    await current.storage.runs.saveExecutionCheckpoint(id, {
      ...stale.executionCheckpoint, nextRound: stale.executionCheckpoint.nextRound + 1,
    });
    current.storage.close();
    current = host(path, { firstSnapshot: stale });
    const before = await current.storage.runs.get(id);
    const events = await current.storage.runs.listEvents(id, 0);
    await assert.rejects(current.agent.resume(id, request), { code: "agent_execution_checkpoint_conflict" });
    await assert.rejects(current.storage.runs.executeOwned(id, async () => {
      assert.fail("a stale checkpoint must not acquire execution");
    }, stale.executionCheckpoint), { code: "agent_execution_checkpoint_conflict" });
    assert.deepEqual(current.counts, { model: 0, tool: 0 });
    assert.deepEqual(await current.storage.runs.get(id), before);
    assert.deepEqual(await current.storage.runs.listEvents(id, 0), events);
  } finally { current?.storage.close(); rmSync(dir, { recursive: true }); }
});
