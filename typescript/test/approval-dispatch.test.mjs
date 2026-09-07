import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";
import { AgentCanceledError, AgentError } from "purra";
import { ToolCatalog } from "../dist/tools/catalog.js";

const { cases } = JSON.parse(readFileSync(new URL("../../conformance/fixtures/approval_dispatch.json", import.meta.url)));

for (const fixture of cases) test(`approval dispatch: ${fixture.name}`, async () => {
  const controller = new AbortController();
  let approvals = 0, dispatches = 0, claims = 0, scopeChecks = 0;
  const catalog = new ToolCatalog([{
    name: "write", description: "Write fixture",
    inputSchema: { type: "object", properties: { target: { type: "string" } }, required: ["target"] },
    policy: { mode: "confirm", title: "Write fixture", riskLevel: "write" },
    scope: input => {
      scopeChecks++;
      assert.equal(input.target, "fixture");
      if (approvals) {
        if (fixture.name === "revoked") return false;
        if (fixture.name === "scope_error") throw new Error("PRIVATE_AUTHORITY_DETAILS");
        if (fixture.name === "canceled_after_scope") controller.abort();
        if (fixture.name === "control_error") throw new AgentError("run_lease_conflict", "Execution ownership changed");
      }
    },
    run: () => { dispatches++; return { content: "done", effectState: "committed" }; },
  }], {
    approval: { request: () => {
      approvals++;
      if (fixture.name === "canceled") controller.abort();
      return fixture.name === "rejected" ? "rejected" : "approved";
    } },
    idempotency: { executeOnce: async (_, operation) => { claims++; return operation(); } },
  });
  const running = catalog.executeBatch([{ id: "call", name: "write", arguments: { target: "fixture" } }], {
    executionKey: "run", runId: "run", signal: controller.signal,
  });
  if (fixture.name === "unchanged") {
    assert.equal((await running).messages[0].content, "done");
    assert.ok(scopeChecks >= 2);
  } else if (["canceled", "canceled_after_scope"].includes(fixture.name)) {
    await assert.rejects(running, AgentCanceledError);
  } else {
    await assert.rejects(running, error => {
      assert.equal(error.code, fixture.error ?? "tool_approval_rejected");
      assert.ok(!error.message.includes("PRIVATE_AUTHORITY_DETAILS"));
      return true;
    });
  }
  assert.equal(approvals, 1);
  assert.equal(dispatches, fixture.dispatches);
  assert.equal(claims, fixture.claims);
});
