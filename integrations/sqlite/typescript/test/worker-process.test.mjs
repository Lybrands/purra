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
