// Worker for the Python verify_load.py orchestrator. Uses only temporary synthetic data.
import assert from "node:assert/strict";
import { readFileSync, writeFileSync } from "node:fs";
import { createInterface } from "node:readline/promises";
import { DatabaseSync } from "node:sqlite";
import { InMemoryRunRepository, assertRunRepositoryConforms } from "purra";
import { SqliteAgentAdapters } from "../dist/index.js";

const [, , flag, mode, raw] = process.argv;
assert.equal(flag, "--worker");
const config = JSON.parse(raw);
const storage = new SqliteAgentAdapters(config.path, { scope: "load" });
const draft = (key) => ({ sourceKey: key, kind: "model.diagnostics", channel: "model", visibility: "private", payload: { text: "x".repeat(128) } });
const waitForKill = () => new Promise(() => { setInterval(() => {}, 1000); });
try {
  if (mode === "seed") {
    const reference = new InMemoryRunRepository();
    await assertRunRepositoryConforms(reference);
    const { preset, budgets, executionCheckpoint } = await reference.get("conformance-run-1");
    await storage.transaction(async ({ runs }) => {
      const ids = [];
      for (let root = 0; root < config.roots; root++) {
        const rootId = `root:${root}`;
        for (let child = 0; child <= config.children; child++) {
          const runId = child === 0 ? rootId : `${rootId}:child:${child}`;
          await runs.begin({ preset, budgets, requestedRunId: runId, deadlineAt: null, metadata: {},
            ...(child === 0 ? {} : { rootRunId: rootId, parentRunId: rootId }) });
          ids.push(runId);
        }
        await runs.saveExecutionCheckpoint(rootId, { ...executionCheckpoint, runId: rootId, nextRound: 2,
          messages: [{ role: "user", content: "x".repeat(config.checkpoint_chars) }] });
      }
      for (let i = 0; i < config.history; i++) await runs.appendEvent(ids[i % ids.length], draft(`history:${i}`));
    });
    console.log(JSON.stringify({ runs: config.roots * (config.children + 1) }));
  } else if (mode === "write") {
    const input = createInterface({ input: process.stdin });
    console.log('{"ready":true}');
    for await (const line of input) { assert.equal(line, "start"); break; }
    input.close();
    const ms = [];
    for (let i = 0; i < config.writes; i++) {
      const item = draft(`worker:${config.worker}:${i}`);
      const start = performance.now();
      const event = await storage.runs.appendEvent("root:0", item);
      ms.push(performance.now() - start);
      if (i % 5 === 0) assert.deepEqual(await storage.runs.appendEvent("root:0", item), event);
    }
    console.log(JSON.stringify({ ms }));
  } else if (mode === "crash") {
    const db = new DatabaseSync(config.path);
    db.exec("BEGIN IMMEDIATE; UPDATE purra_state SET body='partial' WHERE scope='load' AND sdk='typescript'; DELETE FROM purra_output_events WHERE scope='load' AND sdk='typescript' AND run_id='root:0' AND sequence=1;");
    console.log('{"ready":true}');
    await waitForKill();
  } else if (mode === "tool-crash") {
    await storage.idempotency.executeOnce("crash-tool", async () => {
      writeFileSync(config.effect, "executed-once");
      console.log('{"ready":true}');
      await waitForKill();
      return { content: "done" };
    });
  } else if (mode === "recover") {
    let effects = 0;
    const forbidden = async () => { effects++; return { content: "unexpected" }; };
    await assert.rejects(storage.idempotency.executeOnce("crash-tool", forbidden), /tool_effect_unknown/);
    await storage.reconcileTool("crash-tool", { result: { content: "done" } });
    assert.equal((await storage.idempotency.executeOnce("crash-tool", forbidden)).content, "done");
    assert.equal(effects, 0);
    assert.equal(readFileSync(config.effect, "utf8"), "executed-once");
    for (let root = 0; root < config.roots; root++) {
      const saved = await storage.runs.get(`root:${root}`);
      assert.equal(saved.executionCheckpoint.nextRound, 2);
      assert.equal(saved.executionCheckpoint.messages[0].content.length, config.checkpoint_chars);
    }
    console.log('{"recovered":true}');
  } else throw new Error("unsupported worker mode");
} finally { storage.close(); }
