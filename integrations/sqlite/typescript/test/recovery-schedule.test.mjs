import assert from 'node:assert/strict';
import test from 'node:test';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { RecoveryWorker, buildRecoveryInspection } from 'purra';
import { SqliteAgentAdapters } from '../dist/index.js';

function fixture(t) {
  const dir = mkdtempSync(join(tmpdir(), 'purra-schedule-')), stores = [];
  t.after(() => { stores.forEach(s => s.close()); rmSync(dir, { recursive: true }); });
  return scope => { const store = new SqliteAgentAdapters(join(dir, 'db'), { scope }); stores.push(store); return store; };
}

test('persisted backoff, scope isolation and stale settlement after wake', async t => {
  const open = fixture(t), first = open('one');
  let now = 100;
  const options = { intervalMs: 10, maxBackoffMs: 40, clockMs: () => now };
  const initial = first.recoverySchedule(options);
  assert.equal(await initial.ready('run'), 0);
  assert.equal(await initial.settle('run', 0, true), true);
  const restored = open('one'), other = open('one'), isolated = open('two');
  const schedule = restored.recoverySchedule(options);
  assert.equal(await schedule.ready('run'), null);
  assert.equal(await isolated.recoverySchedule().ready('run'), 0);
  now = 120;
  const revision = await schedule.ready('run');
  assert.equal(await schedule.settle('run', revision, true), true);
  now = 159;
  assert.equal(await schedule.ready('run'), null);
  await other.recoverySchedule().wake('run');
  const token = await schedule.ready('run');
  assert.notEqual(token, null);
  assert.equal(await schedule.settle('run', revision, true), false);
  assert.equal(await schedule.ready('run'), token);
  assert.equal(await schedule.settle('run', token, false), true);
  now = 169;
  assert.notEqual(await schedule.ready('run'), null);
});

test('worker preserves a wake received during a failing callback', async t => {
  const schedule = fixture(t)('one').recoverySchedule({ clockMs: () => 100 }), calls = [];
  const worker = new RecoveryWorker({ schedule, discover: async () => ['one', 'two'],
    inspect: async () => buildRecoveryInspection({}),
    resume: async id => { calls.push(id); if (id === 'one') { await schedule.wake(id); throw new Error('synthetic'); } },
  });
  await worker.runOnce();
  assert.notEqual(await schedule.ready('one'), null);
  assert.equal(await schedule.ready('two'), null);
  const report = await worker.runOnce();
  assert.deepEqual(calls, ['one', 'two', 'one']);
  assert.deepEqual(report[1].reasons, ['retry_not_due']);
});
