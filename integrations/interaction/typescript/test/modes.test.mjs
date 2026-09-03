import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { Agent, UserInputRequired } from "purra";
import { SqliteAgentAdapters } from "purra-sqlite";
import { SqliteClarification } from "../dist/index.js";

const QUESTION = { questions: [{ id: "detail", prompt: "Choose a detail", choices: ["yes", "no"], allowFreeform: false }] };
const answered = messages => messages.some(m => typeof m.content === "string" && m.content.includes("Answers to requested"));
const plan = (ask = true, suffix = "") => ({ workPlan: { title: "Clarify and finish", steps: [
  ...(ask ? [{ id: "ask" + suffix, title: "Ask", type: "read", executor: "tool", capabilityNames: ["request_user_input"] }] : []),
  { id: "finish" + suffix, title: "Finish", type: "review", executor: "model", dependsOn: ask ? ["ask" + suffix] : [] },
] } });
function planner(ask = true) { return { calls: 0, revisions: [], createPlan() { this.calls++; return plan(ask); }, revisePlan(_request, _caps, turn) { this.revisions.push(turn.revision); return plan(false); } }; }
function compose(path, script, planner, tree = false, replan = false) {
  const storage = new SqliteAgentAdapters(path, { scope: "modes" });
  const interaction = new SqliteClarification(storage);
  const calls = [];
  const agent = new Agent({ runRepository: storage.runs, outputPublisher: storage.publisher,
    preset: { id: "modes", revision: "1" }, tools: [replan ? { ...interaction.tool, run: input => ({ ...interaction.tool.run(input), planningDisposition: "replan", planningReason: "Use the answer for the remaining work" }) } : interaction.tool], checkpointHandler: interaction.checkpointHandler,
    ...(planner ? { planning: { planner } } : {}),
    ...(tree ? { agentTree: { repository: storage.runTree, policy: { allowRecursiveDelegation: true } } } : {}),
    model: { async invoke(request) {
      calls.push(request);
      const output = await script(request.messages, calls.length);
      return Array.isArray(output)
        ? { message: { role: "assistant", content: "", toolCalls: [{ id: `call-${request.messages.flatMap(m => m.toolCalls ?? []).length + 1}`, name: output[0], arguments: output[1] }] }, finishReason: "tool_calls", usage: { inputTokens: 2, outputTokens: 2 } }
        : { message: { role: "assistant", content: output }, finishReason: "stop", usage: { inputTokens: 2, outputTokens: 2 } };
    } },
  });
  return { storage, interaction, agent, calls };
}

for (const mode of ["auto", "planned", "promoted", "remaining"]) test(`${mode} restart preserves plan and Auto activation`, async () => {
  const dir = mkdtempSync(join(tmpdir(), "purra-modes-"));
  const path = join(dir, "agent.db");
  const planning = mode === "auto" ? undefined : planner(mode !== "remaining");
  let host = compose(path, (_messages, count) => mode === "promoted" && count === 1 ? ["request_plan", {}] : ["request_user_input", QUESTION], planning);
  try {
    const handle = await host.agent.submit({ messages: [{ role: "user", content: "Root task" }], planningMode: mode === "planned" ? "planned" : "auto" }, { budgets: { maxRunOutputTokens: 1000 } });
    let id;
    await assert.rejects(handle.result, error => { id = error.requestId; if (!(error instanceof UserInputRequired)) throw error; return true; });
    const checkpoint = (await handle.snapshot()).executionCheckpoint;
    assert.equal(checkpoint.executionProfile, ["planned", "promoted"].includes(mode) ? "planned" : "auto");
    if (checkpoint.planning) assert.equal(checkpoint.planning.plan.steps[0].status, "done");
    host.storage.close();
    const rebound = planning ? planner(false) : undefined;
    host = compose(path, (messages, count) => { assert(answered(messages)); return mode === "remaining" && count === 1 ? ["request_remaining_plan", {}] : "Finished"; }, rebound);
    await host.interaction.answer(id, { revision: 1, key: "answer", answers: { detail: "yes" } });
    const resumed = await host.interaction.resume(host.agent, id);
    assert.equal(resumed.runId, handle.runId);
    assert.equal((await resumed.result).output, "Finished");
    if (rebound) assert.equal(rebound.calls, mode === "remaining" ? 1 : 0);
  } finally { host.storage.close(); rmSync(dir, { recursive: true }); }
});

function treeScript(messages) {
  const instruction = messages.find(m => m.role === "system" && m.attributes?.agentId)?.content ?? "root";
  const delegated = messages.filter(m => m.role === "tool").map(m => typeof m.content === "string" ? JSON.parse(m.content) : m.content).filter(m => m?.pendingRunIds);
  if (["root", "middle"].includes(instruction)) {
    if (!delegated.length) return ["delegateToAgents", { delegations: (instruction === "root" ? ["middle", "sibling"] : ["leaf"]).map(name => ({ agentName: name, title: name, instruction: name, objective: name })) }];
    for (const result of delegated) assert.deepEqual(result.pendingRunIds, []);
    return "Tree finished";
  }
  return answered(messages) ? "Child finished" : ["request_user_input", QUESTION];
}
for (const cancel of [false, true]) test(`nested tree restart, multiple questions, cancellation=${cancel}`, async () => {
  const dir = mkdtempSync(join(tmpdir(), "purra-tree-input-"));
  const path = join(dir, "agent.db");
  let host = compose(path, treeScript, undefined, true);
  try {
    const handle = await host.agent.submit({ messages: [{ role: "user", content: "Root task" }] }, { budgets: { maxRunOutputTokens: 1000 } });
    await assert.rejects(handle.result, error => { if (!(error instanceof UserInputRequired)) throw error; return true; });
    const pending = await host.interaction.listWaiting();
    assert.equal(pending.length, 2);
    const descendants = await host.storage.runTree.listDescendants(handle.runId);
    assert.equal(descendants.length, 3);
    assert(descendants.every(r => r.status === "waiting" && r.leaseOwnerId === null));
    host.storage.close(); host = compose(path, treeScript, undefined, true);
    if (cancel) {
      assert.equal(await host.interaction.cancel(pending[0].id), true);
      assert.equal((await host.storage.runs.get(handle.runId)).status, "canceled");
      assert((await host.storage.runTree.listDescendants(handle.runId)).every(r => r.status === "canceled"));
      assert.deepEqual(await host.interaction.listWaiting(), []);
    } else {
      await host.interaction.answer(pending[0].id, { revision: 1, key: "a", answers: { detail: "yes" } });
      await assert.rejects(host.interaction.resume(host.agent, pending[0].id), /answers_incomplete/);
      await host.interaction.answer(pending[1].id, { revision: 1, key: "a", answers: { detail: "yes" } });
      const resumed = await host.interaction.resume(host.agent, pending[0].id);
      assert.equal(resumed.runId, handle.runId);
      assert.equal((await resumed.result).output, "Tree finished");
      assert((await host.storage.runTree.listDescendants(handle.runId)).every(r => r.status === "done"));
      assert.deepEqual(await host.interaction.listPending(), []);
    }
  } finally { host.storage.close(); rmSync(dir, { recursive: true }); }
});


test("replanning revision survives two input waits", async () => {
  const dir = mkdtempSync(join(tmpdir(), "purra-revisions-"));
  const path = join(dir, "agent.db");
  const makePlanner = () => ({ ...planner(), revisePlan(_request, _caps, turn) {
    this.revisions.push(turn.revision); return plan(turn.revision === 1, String(turn.revision));
  } });
  const script = messages => messages.filter(m => typeof m.content === "string" && m.content.includes("Answers to requested")).length < 2
    ? ["request_user_input", QUESTION] : "Finished";
  let planning = makePlanner(), host = compose(path, script, planning, false, true);
  try {
    let handle = await host.agent.submit({ messages: [{ role: "user", content: "Root task" }], planningMode: "planned" }, { budgets: { maxRunOutputTokens: 1000 } });
    for (const revision of [0, 1]) {
      let id;
      await assert.rejects(handle.result, error => { if (!(error instanceof UserInputRequired)) throw error; id = error.requestId; return true; });
      const checkpoint = (await handle.snapshot()).executionCheckpoint;
      assert(checkpoint.pendingReplan);
      assert.equal(checkpoint.planning.revision, revision);
      assert.deepEqual(planning.revisions, revision === 0 ? [] : [1]);
      host.storage.close(); planning = makePlanner(); host = compose(path, script, planning, false, true);
      await host.interaction.answer(id, { revision: 1, key: "answer", answers: { detail: "yes" } });
      handle = await host.interaction.resume(host.agent, id);
    }
    assert.equal((await handle.result).output, "Finished");
    assert.equal(planning.calls, 0); assert.deepEqual(planning.revisions, [2]);
  } finally { host.storage.close(); rmSync(dir, { recursive: true }); }
});

test("Planned Root and Auto-promoted children resume together", async () => {
  const dir = mkdtempSync(join(tmpdir(), "purra-planned-tree-"));
  const path = join(dir, "agent.db");
  const makePlanner = () => ({ ...planner(), createPlan(request) {
    this.calls++;
    const instruction = request.messages.find(m => m.role === "system" && m.attributes?.agentId)?.content ?? "root";
    return ["root", "middle"].includes(instruction) ? { workPlan: { title: "Delegate", steps: [
      { id: "delegate", title: "Delegate", type: "write", executor: "tool", capabilityNames: ["delegateToAgents"] },
      { id: "finish", title: "Finish", type: "review", executor: "model", dependsOn: ["delegate"] },
    ] } } : plan();
  } });
  const makeScript = () => {
    const seen = new Set();
    return messages => {
      const instruction = messages.find(m => m.role === "system" && m.attributes?.agentId)?.content ?? "root";
      if (instruction !== "root" && !seen.has(instruction) && !answered(messages) && !messages.some(m => m.role === "tool")) {
        seen.add(instruction); return ["request_plan", {}];
      }
      return treeScript(messages);
    };
  };
  let planning = makePlanner(), host = compose(path, makeScript(), planning, true);
  try {
    const handle = await host.agent.submit({ messages: [{ role: "user", content: "Root task" }], planningMode: "planned" }, { budgets: { maxRunOutputTokens: 1000, maxModelAttempts: 32 } });
    await assert.rejects(handle.result, error => { if (!(error instanceof UserInputRequired)) throw error; return true; });
    const pending = await host.interaction.listWaiting();
    assert.equal(pending.length, 2); assert.equal(planning.calls, 4);
    host.storage.close(); planning = makePlanner(); host = compose(path, makeScript(), planning, true);
    for (const row of pending) await host.interaction.answer(row.id, { revision: 1, key: "a", answers: { detail: "yes" } });
    const resumed = await host.interaction.resume(host.agent, pending[0].id);
    assert.equal((await resumed.result).output, "Tree finished");
    assert.equal(planning.calls, 0);
    assert((await host.storage.runTree.listDescendants(handle.runId)).every(run => run.status === "done"));
  } finally { host.storage.close(); rmSync(dir, { recursive: true }); }
});


test("duplicate Root resume cannot settle another owner's Agent tree", async () => {
  const dir = mkdtempSync(join(tmpdir(), "purra-tree-lease-"));
  let release, entered;
  const gate = new Promise(resolve => { release = resolve; });
  const started = new Promise(resolve => { entered = resolve; });
  const host = compose(join(dir, "agent.db"), async messages => {
    if (answered(messages)) { entered(); await gate; }
    return treeScript(messages);
  }, undefined, true);
  try {
    const handle = await host.agent.submit({ messages: [{ role: "user", content: "Root task" }] }, { budgets: { maxRunOutputTokens: 1000 } });
    await assert.rejects(handle.result, UserInputRequired);
    const pending = await host.interaction.listWaiting();
    for (const row of pending) await host.interaction.answer(row.id, { revision: 1, key: "a", answers: { detail: "yes" } });
    const first = await host.interaction.resume(host.agent, pending[0].id);
    await started;
    const duplicate = await host.interaction.resume(host.agent, pending[0].id);
    await assert.rejects(duplicate.result, /lease/);
    assert.equal((await host.storage.runs.get(handle.runId)).status, "running");
    assert(["running", "waiting"].includes((await host.storage.runTree.getRun(handle.runId)).status));
    release();
    assert.equal((await first.result).output, "Tree finished");
    assert.equal((await host.storage.runTree.getRun(handle.runId)).status, "done");
  } finally { release(); host.storage.close(); rmSync(dir, { recursive: true }); }
});
