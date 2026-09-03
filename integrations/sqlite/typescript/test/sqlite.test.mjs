import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { Agent, UserInputRequired, assertRunRepositoryConforms, assertArtifactRepositoryConforms, assertLongTaskRepositoryConforms, assertDelegationRepositoryConforms } from "purra";
import { SqliteAgentAdapters } from "../dist/index.js";

test("canonical storage conformance survives reopen and rejects terminal writes", async () => {
  const dir = mkdtempSync(join(tmpdir(), "purra-sqlite-"));
  let storage = new SqliteAgentAdapters(join(dir, "agent.db"), { scope: "conformance" });
  try {
    await assertRunRepositoryConforms(storage.runs);
    await assertArtifactRepositoryConforms(storage.artifacts);
    await assertLongTaskRepositoryConforms(storage.longTasks);
    await assertDelegationRepositoryConforms(storage.delegations);
    const events = await storage.runs.listEvents("conformance-run-1", 0);
    storage.close(); storage = new SqliteAgentAdapters(join(dir, "agent.db"), { scope: "conformance" });
    assert.deepEqual(await storage.runs.listEvents("conformance-run-1", 0), events);
    assert.equal((await storage.runs.get("conformance-run-1")).status, "completed");
    await assert.rejects(storage.runs.appendEvent("conformance-run-1", { sourceKey: "late", kind: "commentary", channel: "commentary", visibility: "public", payload: { text: "late" } }));
  } finally { storage.close(); rmSync(dir, { recursive: true }); }
});

