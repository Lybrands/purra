import assert from 'node:assert/strict';
import test from 'node:test';
import { spawn } from 'node:child_process';
import { once } from 'node:events';
import { mkdtempSync, rmSync, readFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createInterface } from 'node:readline';
import { createApprovalHost, paused, approve } from './approval-host.mjs';

async function bounded(promise) {
  let timer;
  try { return await Promise.race([promise, new Promise((_, reject) => { timer = setTimeout(() => reject(new Error('child timed out')), 15000); })]); }
  finally { clearTimeout(timer); }
}
async function setup(t) {
  const dir = mkdtempSync(join(tmpdir(), 'purra-worker-process-')), path = join(dir, 'db'), effect = join(dir, 'effects'), children = [];
  t.after(async () => {
    try { for (const child of children) { if (child.process.exitCode === null && child.process.signalCode === null) child.process.kill('SIGKILL'); await bounded(child.exit); } }
    finally { rmSync(dir, { recursive: true }); }
  });
  const expiry = Date.now() + 600000, host = createApprovalHost(path, { expiry });
  let id, record;
  try { id = await paused(host); record = await approve(host, id); } finally { host.storage.close(); }
  function start(mode) {
    const process = spawn(globalThis.process.execPath, ['--disable-warning=ExperimentalWarning', new URL('./worker-process-child.mjs', import.meta.url).pathname, path, effect, String(expiry), mode], { stdio: ['pipe', 'pipe', 'pipe'] });
    const exit = once(process, 'exit');
    const lines = createInterface({ input: process.stdout })[Symbol.asyncIterator]();
    let stderr = ''; process.stderr.setEncoding('utf8').on('data', value => stderr += value);
    const child = { process, exit,
      async event(stage) { const line = await bounded(lines.next()); assert.equal(line.done, false, stderr); const value = JSON.parse(line.value); assert.equal(value.stage, stage); return value; },
      release() { process.stdin.write('continue\n'); },
      async finish(code = 0) { assert.deepEqual(await bounded(exit), [code, null], stderr); assert.equal(stderr, ''); },
    };
    children.push(child); return child;
  }
  return { path, effect, id, record, start };
}

test('independent competitor with stale inspection cannot repeat an approved write', async t => {
  const { start, effect, id } = await setup(t);
  const competitor = start('competitor');
  assert.deepEqual((await competitor.event('discovered')).ids, [id]);
  assert.deepEqual((await competitor.event('inspected')).blockers, []);
  const owner = start('owner');
  await owner.event('discovered'); await owner.event('effect');
  competitor.release();
  const rejected = await competitor.event('result');
  assert.deepEqual(rejected.actions, ['failed']); assert.deepEqual(rejected.errors, ['run_lease_conflict']);
  assert.equal(rejected.tool, 0); assert.equal(rejected.model, 0); await competitor.finish();
  owner.release(); assert.deepEqual((await owner.event('result')).actions, ['settled']); await owner.finish();
  assert.equal(readFileSync(effect, 'utf8'), 'write\n');
});

test('exit before cursor acknowledgement rediscovers terminal Run without executing', async t => {
  const { start, effect, id, path } = await setup(t);
  const first = start('exit_before_ack'); await first.event('discovered');
  const before = await first.event('before_ack'); assert.deepEqual(before.ids, [id]); assert.equal(before.tools, 1);
  await first.finish(73);
  const host = createApprovalHost(path);
  try {
    assert.equal((await host.storage.runs.get(id)).status, 'completed');
    await host.storage.transaction((_, extra) => { assert.equal(extra.recoveryCursors?.worker, undefined); assert.equal(Object.values(extra.tools)[0].state, 'complete'); });
  } finally { host.storage.close(); }
  const restarted = start('restart'); assert.deepEqual((await restarted.event('discovered')).ids, [id]);
  const result = await restarted.event('result'); assert.deepEqual(result.actions, ['blocked']); assert.equal(result.tool, 0); assert.equal(result.model, 0);
  await restarted.finish(); assert.equal(readFileSync(effect, 'utf8'), 'write\n');
  const final = createApprovalHost(path);
  try { await final.storage.transaction((_, extra) => assert.equal(extra.recoveryCursors.worker.revision, 1)); }
  finally { final.storage.close(); }
});

test('effect exit keeps claim after real lease expiry until explicit reconciliation', async t => {
  const { start, effect, id, path } = await setup(t);
  const crashed = start('exit_after_effect');
  await crashed.event('discovered'); await crashed.event('effect'); await crashed.finish(74);
  const host = createApprovalHost(path);
  let key, claim;
  try {
    const expires = await host.storage.transaction((_, extra) => {
      [key] = Object.keys(extra.tools); claim = structuredClone(extra.tools[key]);
      assert.equal(claim.state, 'claimed'); assert.equal(claim.result, undefined);
      assert.ok(extra.leases[id].owner); return extra.leases[id].expires;
    });
    await assert.rejects(host.storage.reconcileTool(key, { result: { content: 'written', effectState: 'committed' } }), { code: 'run_lease_conflict' });
    const delay = Math.max(0, expires - Date.now()) + 50;
    assert.ok(delay < 35000);
    // Actual persisted expiry, without editing leases or replacing the clock.
    await new Promise(resolve => setTimeout(resolve, delay));
    assert.ok(Date.now() > expires);
  } finally { host.storage.close(); }
  const blocked = start('restart'); await blocked.event('discovered');
  const report = await blocked.event('result'); await blocked.finish();
  assert.deepEqual(report.actions, ['blocked']); assert.ok(report.reasons[0].includes('tool_effect_unknown'));
  assert.equal(report.tool, 0); assert.equal(report.model, 0);
  const forced = start('force_resume'); await forced.event('discovered');
  const rejected = await forced.event('result'); await forced.finish();
  assert.deepEqual(rejected.errors, ['run_recovery_requires_reconciliation']);
  assert.equal(rejected.tool, 0); assert.equal(rejected.model, 0);
  const reconciler = createApprovalHost(path);
  try {
    await reconciler.storage.transaction((_, extra) => assert.deepEqual(extra.tools[key], claim));
    assert.equal(readFileSync(effect, 'utf8'), 'write\n');
    await reconciler.storage.reconcileTool(key, { result: { content: 'written', effectState: 'committed' } });
  } finally { reconciler.storage.close(); }
  const recovered = start('restart'); await recovered.event('discovered');
  const result = await recovered.event('result'); await recovered.finish();
  assert.deepEqual(result.actions, ['settled']); assert.equal(result.tool, 0);
  assert.equal(readFileSync(effect, 'utf8'), 'write\n');
  const final = createApprovalHost(path);
  try {
    assert.equal((await final.storage.runs.get(id)).status, 'completed');
    await final.storage.transaction((_, extra) => assert.equal(extra.tools[key].state, 'complete'));
  } finally { final.storage.close(); }
});

test('three services drain waves and restart without duplicate effects', async t => {
  const waves = Number(process.env.PURRA_WORKER_LOAD_WAVES ?? 3), gap = Number(process.env.PURRA_WORKER_LOAD_GAP_MS ?? 100);
  assert.ok(Number.isInteger(waves) && waves >= 3 && waves <= 30 && Number.isInteger(gap) && gap >= 0 && gap <= 10000);
  const { start, effect, id, path } = await setup(t), ids = [id], active = [], reports = [], started = performance.now();
  async function launch(index) { const child = start(`service-${index}`); await child.event('ready'); child.release(); return child; }
  async function stop(child) {
    child.process.stdin.write('stop\n'); const result = await child.event('result'); await child.finish();
    assert.ok(result.scans > 0); assert.equal(result.diagnostics.phase, 'idle'); assert.equal(result.diagnostics.serving, false);
    assert.equal(result.failed, result.errors.length, JSON.stringify(result));
    assert.ok(result.errors.every(code => ['run_lease_conflict', 'run_terminal', 'run_recovery_requires_reconciliation', 'tool_effect_unknown'].includes(code)), JSON.stringify(result));
    reports.push(result);
  }
  for (let i = 0; i < 3; i++) active.push(await launch(i));
  for (let wave = 0; wave < waves; wave++) {
    for (let i = 0; i < (wave === 0 ? 3 : 4); i++) {
      const seed = createApprovalHost(path);
      try { const id = await paused(seed); await approve(seed, id); ids.push(id); }
      finally { seed.storage.close(); }
    }
    const reader = createApprovalHost(path);
    try {
      const deadline = performance.now() + 30000;
      while (true) {
        const statuses = await Promise.all(ids.map(async id => (await reader.storage.runs.get(id)).status));
        assert.ok(statuses.every(status => ['running', 'completed'].includes(status)), JSON.stringify(statuses));
        if (statuses.every(status => status === 'completed')) break;
        assert.ok(performance.now() < deadline, 'wave did not drain');
        await new Promise(resolve => setTimeout(resolve, 50));
      }
    } finally { reader.storage.close(); }
    if (wave === Math.floor(waves / 2)) { await stop(active[0]); active[0] = await launch(0); }
    await new Promise(resolve => setTimeout(resolve, gap));
  }
  for (const child of active) await stop(child);
  const effects = readFileSync(effect, 'utf8').trim().split('\n').map(JSON.parse);
  assert.deepEqual(effects.map(row => row.runId).sort(), ids.sort());
  assert.equal(reports.reduce((total, row) => total + row.tool, 0), ids.length);
  assert.ok(new Set(effects.map(row => row.worker)).size >= 2);
  const overlaps = effects.reduce((total, a, i) => total + effects.slice(i+1).filter(b => a.worker !== b.worker && BigInt(a.startNs) < BigInt(b.endNs) && BigInt(b.startNs) < BigInt(a.endNs)).length, 0);
  const reader = createApprovalHost(path);
  try { await reader.storage.transaction((_, extra) => {
    assert.equal(Object.keys(extra.tools).length, ids.length);
    assert.ok(Object.values(extra.tools).every(row => row.state === 'complete'));
    assert.ok(Object.values(extra.leases).every(row => row.owner === null));
    for (let i = 0; i < 3; i++) assert.ok(extra.recoveryCursors[`service-${i}`].revision > 0);
  }); } finally { reader.storage.close(); }
  console.log(JSON.stringify({ sdk: 'typescript', runs: ids.length, waves, elapsedSeconds: (performance.now()-started)/1000, overlappingHandlerPairs: overlaps, workerReports: reports }));
});
