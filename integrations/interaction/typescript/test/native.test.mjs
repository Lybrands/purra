import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { Agent, UserInputRequired, assertRunRepositoryConforms, assertArtifactRepositoryConforms, assertLongTaskRepositoryConforms } from "purra";
import { SqliteAgentAdapters } from "purra-sqlite";
import { SqliteClarification } from "../dist/index.js";
import { TEST_MODEL_CAPABILITIES } from "./model-capabilities.mjs";

function compose(path, beforeAnswered = async () => {}) {
  const storage = new SqliteAgentAdapters(path, { scope: "owner/project" });
  const interaction = new SqliteClarification(storage);
  const calls = [];
  const agent = new Agent({ runRepository: storage.runs, outputPublisher: storage.publisher,
    preset: { id: "sqlite", revision: "1" }, tools: [interaction.tool], checkpointHandler: interaction.checkpointHandler,
    model: { capabilities: TEST_MODEL_CAPABILITIES, async invoke(request) {
      calls.push(request);
      const answered = request.messages.some(m => typeof m.content === "string" && m.content.includes("Answers to requested"));
      if (answered) await beforeAnswered();
      return answered ? { message: { role: "assistant", content: "Finished with the answer" }, finishReason: "stop", appliedGenerationLimit: request.outputBudget.maxGenerationTokens, usage: { inputTokens: 5, generationTokens: 7 } }
        : { message: { role: "assistant", content: "", toolCalls: [{ id: "ask-1", name: "request_user_input", arguments: { questions: [{ id: "length", prompt: "篇幅？", choices: ["短", "长"], allowFreeform: false }] } }] }, finishReason: "tool_calls", appliedGenerationLimit: request.outputBudget.maxGenerationTokens, usage: { inputTokens: 10, generationTokens: 20 } };
    } },
  });
  return { storage, interaction, calls, agent };
}

test("question restart answer resume retains Run id, budget and canonical output", async () => {
  const dir = mkdtempSync(join(tmpdir(), "purra-interaction-"));
  const path = join(dir, "agent.db");
  let host = compose(path);
  try {
    const handle = await host.agent.submit({ messages: [{ role: "user", content: "Write" }], planningMode: "reactive" }, { budgets: { maxRunGenerationTokens: 100 } });
    let requestId;
    await assert.rejects(handle.result, error => { assert(error instanceof UserInputRequired); requestId = error.requestId; return true; });
    const before = await host.storage.runs.listEvents(handle.runId, 0);
    assert.equal(host.calls.length, 1);
    assert.equal((await host.interaction.get(requestId)).state, "waiting");
    host.storage.close(); host = compose(path);
    await assert.rejects(host.interaction.answer(requestId, { revision: 1, key: "bad", answers: { length: "invalid" } }));
    const ready = await host.interaction.answer(requestId, { revision: 1, key: "answer", answers: { length: "短" } });
    assert.deepEqual(await host.interaction.answer(requestId, { revision: 1, key: "answer", answers: { length: "短" } }), ready);
    const resumed = await host.interaction.resume(host.agent, requestId);
    assert.equal(resumed.runId, handle.runId);
    assert.equal((await resumed.result).output, "Finished with the answer");
    const saved = await resumed.snapshot();
    assert.equal(saved.usage.modelAttempts, 3);
    assert.equal(saved.usage.generationTokens, 34);
    const events = await host.storage.runs.listEvents(handle.runId, 0);
    assert.deepEqual(events.slice(0, before.length), before);
    assert.equal(events.filter(e => e.kind === "input.required").length, 1);
  } finally { host.storage.close(); rmSync(dir, { recursive: true }); }
});

test("persistent cancellation and cumulative attempt limits", async () => {
  const dir = mkdtempSync(join(tmpdir(), "purra-limits-"));
  try {
    for (const cancel of [false, true]) {
      const host = compose(join(dir, `${cancel}.db`));
      try {
        const handle = await host.agent.submit({ messages: [{ role: "user", content: "Write" }], planningMode: "reactive" }, { budgets: { maxRunGenerationTokens: 100, maxModelAttempts: 1 } });
        let id;
        await assert.rejects(handle.result, error => { id = error.requestId; return error instanceof UserInputRequired; });
        if (cancel) {
          assert.equal(await host.interaction.cancel(id), true);
          assert.equal(await host.interaction.cancel(id), false);
          assert.equal((await host.interaction.get(id)).state, "canceled");
          assert.deepEqual(await host.interaction.listWaiting(), []);
        } else {
          await host.interaction.answer(id, { revision: 1, key: "a", answers: { length: "短" } });
          const resumed = await host.interaction.resume(host.agent, id);
          await assert.rejects(resumed.result);
          assert.equal(host.calls.length, 1);
        }
      } finally { host.storage.close(); }
    }
  } finally { rmSync(dir, { recursive: true }); }
});

test("concurrent resume cannot borrow another execution's lease", async () => {
  const dir = mkdtempSync(join(tmpdir(), "purra-owners-"));
  let enter, release;
  const entered = new Promise(resolve => { enter = resolve; });
  const barrier = new Promise(resolve => { release = resolve; });
  const host = compose(join(dir, "agent.db"), async () => { enter(); await barrier; });
  let first;
  try {
    const initial = await host.agent.submit({ messages: [{ role: "user", content: "Write" }], planningMode: "reactive" }, { budgets: { maxRunGenerationTokens: 100 } });
    let id;
    await assert.rejects(initial.result, error => { id = error.requestId; return error instanceof UserInputRequired; });
    await host.interaction.answer(id, { revision: 1, key: "a", answers: { length: "短" } });
    first = await host.interaction.resume(host.agent, id);
    await entered;
    const second = await host.interaction.resume(host.agent, id);
    await assert.rejects(second.result, { code: "run_lease_conflict" });
    release();
    assert.equal((await first.result).output, "Finished with the answer");
    assert.equal(host.calls.length, 3);
  } finally {
    release();
    if (first) await first.result.catch(() => {});
    host.storage.close();
    rmSync(dir, { recursive: true });
  }
});
