/** Synthetic host lifecycle check; no Provider, MCP service, or business data. */
import { mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { RecoveryWorker } from "purra";
import { createApprovalHost, paused, approve, request } from "../test/approval-host.mjs";

export class HostRegistry {
  constructor(path) { this.path = path; }
  save(runId, expiresAtMs) {
    writeFileSync(this.path, JSON.stringify({ [runId]: {
      requestProfile: "fixture-v1", presetRevision: "1", bindingRevision: "1",
      scopeRevision: "1", expiresAtMs,
    } }));
  }
  resolve(runId) {
    const row = JSON.parse(readFileSync(this.path, "utf8"))[runId];
    const keys = ["bindingRevision", "expiresAtMs", "presetRevision", "requestProfile", "scopeRevision"];
    if (row === null || typeof row !== "object" || Array.isArray(row)
      || Object.keys(row).sort().join() !== keys.join()
      || row.requestProfile !== "fixture-v1" || row.presetRevision !== "1"
      || row.bindingRevision !== "1" || row.scopeRevision !== "1"
      || !Number.isSafeInteger(row.expiresAtMs)) throw new Error("host_run_configuration_mismatch");
    return row;
  }
}

export async function check(directory) {
  mkdirSync(directory, { recursive: true });
  const path = join(directory, "agent.db"), registry = new HostRegistry(join(directory, "host-runs.json"));
  const first = createApprovalHost(path, { expiry: Date.now() + 60_000 });
  let runId;
  try {
    runId = await paused(first);
    const [record] = await first.approvals.listPending({ runId });
    registry.save(runId, record.expiresAtMs);
  } finally { first.storage.close(); }

  const resolved = registry.resolve(runId);
  const restored = createApprovalHost(path, { expiry: resolved.expiresAtMs });
  const cursor = restored.storage.recoveryCursor("example-host", { pageSize: 10 });
  const schedule = restored.storage.recoverySchedule({ intervalMs: 20, maxBackoffMs: 100 });
  const stop = new AbortController(), scans = [];
  const worker = new RecoveryWorker({
    discover: () => cursor.discover(), acknowledge: ids => cursor.acknowledge(ids),
    inspect: id => restored.storage.inspectRecovery(id), schedule,
    resume: async id => {
      registry.resolve(id);
      const result = await (await restored.agent.resume(id, request)).result;
      if (result.output !== "done") throw new Error("host resume did not complete");
    },
  });
  const service = worker.run({ signal: stop.signal, pollIntervalMs: 20, maxBackoffMs: 100,
    onScan: async report => scans.push(report.map(row => row.action)) });
  try {
    await new Promise(resolve => setTimeout(resolve, 50));
    await approve(restored, runId);
    worker.wake();
    const deadline = Date.now() + 2_000;
    while ((await restored.storage.runs.get(runId)).status !== "completed") {
      if (Date.now() >= deadline) throw new Error("worker did not complete approved Run");
      await new Promise(resolve => setTimeout(resolve, 10));
    }
  } finally {
    stop.abort(); worker.wake(); await service; restored.storage.close();
  }

  const final = createApprovalHost(path);
  try {
    if ((await final.storage.runs.get(runId)).status !== "completed") throw new Error("canonical Run is not complete");
    await final.storage.transaction(async (_, extra) => {
      const receipts = Object.values(extra.tools);
      if (receipts.length !== 1 || receipts[0].state !== "complete") throw new Error("execution receipt is incomplete");
      if (Object.values(extra.leases).some(row => row.owner !== null)) throw new Error("active lease remains");
    });
  } finally { final.storage.close(); }
  return { schemaVersion: 1, runStatus: "completed", toolCalls: restored.counts.tool,
    modelCalls: restored.counts.model, workerScans: scans.length,
    workerStopped: !worker.diagnostics().serving, hostConfigurationReconstructed: true,
    syntheticOnly: true };
}

if (process.argv[1] === new URL(import.meta.url).pathname) {
  const supplied = process.argv[2];
  const directory = supplied ?? mkdtempSync(join(tmpdir(), "purra-host-lifecycle-"));
  try { console.log(JSON.stringify(await check(directory), null, 2)); }
  finally { if (supplied === undefined) rmSync(directory, { recursive: true }); }
}
