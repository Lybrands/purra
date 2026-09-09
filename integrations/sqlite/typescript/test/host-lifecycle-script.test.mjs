import assert from "node:assert/strict";
import test from "node:test";
import { spawnSync } from "node:child_process";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { HostRegistry } from "../scripts/check-host-lifecycle.mjs";

test("host lifecycle script reconstructs request state and stops worker", t => {
  const directory = mkdtempSync(join(tmpdir(), "purra-host-lifecycle-test-"));
  t.after(() => rmSync(directory, { recursive: true }));
  const result = spawnSync(process.execPath, [new URL("../scripts/check-host-lifecycle.mjs", import.meta.url).pathname, directory],
    { encoding: "utf8", timeout: 20_000 });
  assert.equal(result.status, 0, result.stderr);
  const report = JSON.parse(result.stdout);
  assert.deepEqual(report, {
    schemaVersion: 1, runStatus: "completed", toolCalls: 1, modelCalls: 2,
    workerScans: report.workerScans, workerStopped: true,
    hostConfigurationReconstructed: true, syntheticOnly: true,
  });
  assert.ok(report.workerScans >= 2);
});

test("host registry rejects a changed current binding", t => {
  const directory = mkdtempSync(join(tmpdir(), "purra-host-registry-test-"));
  t.after(() => rmSync(directory, { recursive: true }));
  const path = join(directory, "host-runs.json"), registry = new HostRegistry(path);
  registry.save("run", 123);
  const saved = JSON.parse(readFileSync(path, "utf8"));
  saved.run.bindingRevision = "2";
  writeFileSync(path, JSON.stringify(saved));
  assert.throws(() => registry.resolve("run"), /host_run_configuration_mismatch/);
});
