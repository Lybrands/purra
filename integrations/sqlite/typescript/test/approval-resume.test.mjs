import assert from 'node:assert/strict';
import test from 'node:test';
import { mkdtempSync, rmSync, readFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import { Agent, ApprovalIntent, ApprovalRequired, jsonIdentityDigest } from 'purra';
import { SqliteAgentAdapters } from '../dist/index.js';
import { testGateway } from '../../../../typescript/test/support/model-gateway.mjs';

const request = { messages: [{ role: 'user', content: 'Write the fixture' }], planningMode: 'reactive' };
const options = { budgets: { maxRunGenerationTokens: null } };
function setup(t) {
  const dir = mkdtempSync(join(tmpdir(), 'purra-tool-resume-')), stores = [];
  t.after(() => { stores.forEach(store => store.close()); rmSync(dir, { recursive: true }); });
  return (settings = {}) => {
    const storage = new SqliteAgentAdapters(join(dir, 'db'), { scope: 'approval' }); stores.push(storage);
    const counts = { model: 0, tool: 0 }, expiry = settings.expiry ?? Date.now() + 600000;
    const approvals = storage.approvalStore({ authorize: () => true, ...(settings.clockMs ? { clockMs: settings.clockMs } : {}) });
    const repository = new Proxy(storage.runs, { get(target, key) {
      if (key === 'saveExecutionCheckpoint' && settings.stopAfterTool) return async () => { throw new ApprovalRequired('fixture', 'stop-after-receipt'); };
      return target[key];
    } });
    const agent = new Agent({
      runRepository: repository, outputPublisher: storage.publisher, preset: { id: 'approval', revision: '1' },
      ...(settings.planner ? { planning: { planner: settings.planner } } : {}),
      ...(settings.tree ? { agentTree: { repository: storage.runTree } } : {}),
      toolCheckpointNames: ['write'],
      model: testGateway({ async invoke(input) {
        counts.model++;
        if (settings.script) return settings.script(input, counts.model);
        if (input.messages.some(message => message.role === 'tool') || input.tools.length === 0) return { message: { role: 'assistant', content: 'done' }, finishReason: 'stop' };
        return { message: { role: 'assistant', content: '', reasoning: 'private fixture reasoning', providerData: { fixture: 'opaque replay' },
          toolCalls: [{ id: 'write-1', name: 'write', arguments: { value: 42 } }] }, finishReason: 'tool_calls' };
      } }),
      tools: [{ name: 'write', description: 'Write a synthetic value', inputSchema: { type: 'object', properties: { value: { type: 'integer' } }, required: ['value'] },
        policy: { mode: 'confirm', title: 'Write', riskLevel: 'write' }, scope: settings.scope ?? (() => true),
        async run() { counts.tool++; if (settings.run) return settings.run(); return { content: 'written', effectState: 'committed' }; },
      }],
      approval: approvals.gateway(), idempotency: storage.idempotency,
      ...(settings.noHandler ? {} : { toolCheckpointHandler: async checkpoint => {
        const call = checkpoint.assistant.toolCalls[0], snapshot = await storage.runs.get(checkpoint.runId);
        const intent = await ApprovalIntent.create({ schemaVersion: 1, runId: checkpoint.runId, rootRunId: checkpoint.runId,
          toolCallId: call.id, toolName: call.name, arguments: call.arguments, presetFingerprint: await jsonIdentityDigest(snapshot.preset),
          bindingId: 'fixture', bindingRevision: '1', scopeId: 'fixture-only', scopeRevision: '1', effect: 'write' });
        const [existing] = await approvals.listPending({ runId: checkpoint.runId });
        await approvals.prepare(checkpoint, intent, { expiresAtMs: existing?.expiresAtMs ?? expiry });
      } }),
    });
    return { storage, approvals, agent, counts, path: join(dir, 'db') };
  };
}
async function paused(host) {
  await host.storage.enableApprovals();
  const handle = await host.agent.submit(request, options);
  await assert.rejects(handle.result, ApprovalRequired);
  assert.deepEqual(host.counts, { model: 1, tool: 0 });
  return handle.runId;
}
async function approve(host, id) {
  const records = await host.approvals.listPending({ runId: id });
  const record = records.find(record => record.status === "pending") ?? records[0];
  await host.approvals.decide({ approvalId: record.approvalId, expectedRevision: record.revision, intentDigest: record.intentDigest,
    commandKey: 'approve', decision: 'approve' }, { principalId: 'host' });
  return record;
}

test('Reactive Root waits, reopens, stays pending without model work, then dispatches once', async t => {
  const open = setup(t), first = open(), id = await paused(first), second = open();
  const before = await second.storage.runs.get(id);
  assert.equal(before.executionCheckpoint, undefined);
  assert.equal(before.toolExecutionCheckpoint.phase, 'tool_ready');
  assert.equal(before.toolExecutionCheckpoint.assistant.providerData.fixture, 'opaque replay');
  await assert.rejects((await second.agent.resume(id, request)).result, ApprovalRequired);
  assert.deepEqual(second.counts, { model: 0, tool: 0 });
  assert.deepEqual((await second.storage.runs.get(id)).usage, before.usage);
  await approve(second, id);
  const result = await (await second.agent.resume(id, request)).result;
  assert.equal(result.output, 'done');
  assert.equal(second.counts.tool, 1);
  const final = await second.storage.runs.get(id);
  assert.equal(final.status, 'completed'); assert.equal(final.toolExecutionCheckpoint, undefined);
  assert.equal(final.usage.modelAttempts, 3);
  const events = await second.storage.runs.listEvents(id, 0);
  assert.equal(events.filter(event => event.kind === 'approval.required').length, 1);
  assert.equal(events.filter(event => event.kind === 'tool.started').length, 1);
  assert.equal(events.find(event => event.kind === 'approval.required').visibility, 'private');
});

test('missing handler and legacy ownership cannot bypass a pending tool checkpoint', async t => {
  const open = setup(t), first = open(), id = await paused(first), second = open({ noHandler: true });
  await assert.rejects(second.agent.resume(id, request), { code: 'approval_runtime_required' });
  await assert.rejects(second.storage.runs.executeOwned(id, async () => assert.fail('executed')), { code: 'approval_runtime_required' });
  assert.deepEqual(second.counts, { model: 0, tool: 0 });
});

test('competing recovery cannot dispatch while the first owner holds the tool claim', async t => {
  const open = setup(t), first = open(), id = await paused(first);
  let enter, release; const entered = new Promise(resolve => enter = resolve), gate = new Promise(resolve => release = resolve);
  const owner = open({ run: async () => { enter(); await gate; return { content: 'written', effectState: 'committed' }; } }), competitor = open();
  await approve(owner, id);
  const running = (await owner.agent.resume(id, request)).result;
  await entered;
  try { await assert.rejects((await competitor.agent.resume(id, request)).result, { code: 'run_lease_conflict' }); }
  finally { release(); }
  await running;
  assert.equal(owner.counts.tool, 1); assert.deepEqual(competitor.counts, { model: 0, tool: 0 });
});


test('current scope denies approved dispatch without another model attempt', async t => {
  const open = setup(t), first = open(), id = await paused(first), denied = open({ scope: () => false });
  await approve(denied, id);
  await assert.rejects((await denied.agent.resume(id, request)).result);
  assert.deepEqual(denied.counts, { model: 0, tool: 0 });
});

test('unknown effect keeps its claim and rejects unknown reconciliation', async t => {
  const open = setup(t), first = open(), id = await paused(first);
  const owner = open({ run: () => ({ content: 'uncertain', effectState: 'unknown' }) });
  await approve(owner, id);
  await assert.rejects((await owner.agent.resume(id, request)).result, { code: 'tool_effect_unknown' });
  const report=await owner.storage.inspectRecovery(id);
  assert.equal(report.observations.approvalUnknownReceipts,1);assert.ok(report.blockers.includes('tool_effect_unknown'));
  const key = await owner.storage.transaction(async (_, extra) => {
    const [[key, entry]] = Object.entries(extra.tools);
    assert.equal(entry.state, 'claimed'); assert.equal(entry.result, undefined); assert.equal(entry.runId, id);
    return key;
  });
  await assert.rejects(owner.storage.reconcileTool(key, { result: { content: 'uncertain', effectState: 'unknown', errorCode: 'fixture_unknown' } }), { code: 'tool_effect_unknown' });
  await assert.rejects(owner.agent.resume(id, request), { code: 'run_terminal' });
  assert.equal(owner.counts.tool, 1);
  const other=await owner.agent.submit(request,options);
  await assert.rejects(other.result,ApprovalRequired);
  const otherReport=await owner.storage.inspectRecovery(other.runId);
  assert.equal(otherReport.observations.approvalUnknownReceipts,0);
  assert.equal(otherReport.blockers.includes('tool_effect_unknown'),false);
  assert.ok(otherReport.cautions.includes('unattributed_tool_effect_unknown'));
});

test('receipt failure after the effect survives reopen and blocks redispatch', async t => {
  const open = setup(t), first = open(), id = await paused(first), db = new DatabaseSync(first.path);
  t.after(() => db.close());
  const owner = open({ run: () => {
    db.exec("CREATE TRIGGER fixture_receipt_failure BEFORE UPDATE ON purra_state BEGIN SELECT RAISE(ABORT, 'fixture receipt failure'); END");
    return { content: 'written', effectState: 'committed' };
  } });
  await approve(owner, id);
  await assert.rejects((await owner.agent.resume(id, request)).result);
  db.exec('DROP TRIGGER fixture_receipt_failure');
  await owner.storage.transaction(async (_, extra) => { extra.leases[id].expires = 0; });
  const restored = open();
  await assert.rejects((await restored.agent.resume(id, request)).result, { code: 'run_recovery_requires_reconciliation' });
  assert.deepEqual(restored.counts, { model: 0, tool: 0 }); assert.equal(owner.counts.tool, 1);
  await restored.storage.transaction(async (_, extra) => assert.equal(Object.values(extra.tools)[0].state, 'claimed'));
});

test('completed receipt replays after expiry without repeating the effect', async t => {
  const open = setup(t), first = open(), id = await paused(first), owner = open({ stopAfterTool: true });
  const record = await approve(owner, id);
  await assert.rejects((await owner.agent.resume(id, request)).result, ApprovalRequired);
  assert.equal(owner.counts.tool, 1);
  const restored = open({ expiry: record.expiresAtMs, clockMs: () => record.expiresAtMs + 1 });
  assert.equal((await restored.approvals.refresh(record.approvalId)).status, 'expired');
  const report=await restored.storage.inspectRecovery(id);
  assert.equal(report.observations.approvalReceipt,'complete');assert.equal(report.blockers.includes('approval_expired'),false);
  assert.ok(report.cautions.includes('approval_terminal_receipt_present'));
  assert.equal((await (await restored.agent.resume(id, request)).result).output, 'done');
  assert.equal(restored.counts.tool, 0);
});

test('atomic prepare rejects an unrelated invocation and v4 writes roll back', async t => {
  const open = setup(t), first = open(), id = await paused(first);
  const checkpoint = (await first.storage.runs.get(id)).toolExecutionCheckpoint;
  const [record] = await first.approvals.listPending({ runId: id });
  const intent = await ApprovalIntent.create(record.intent);
  await assert.rejects(first.approvals.prepare(checkpoint, intent, { expiresAtMs: record.expiresAtMs }), { code: 'run_lease_lost' });
  const before = await first.storage.runs.listEvents(id, 0);
  await first.storage.runs.executeToolOwned(id, async () => {
    await assert.rejects(first.approvals.prepare({ ...checkpoint, invocationId: 'invented' }, intent, { expiresAtMs: record.expiresAtMs }), { code: 'agent_execution_checkpoint_conflict' });
  }, checkpoint);
  assert.deepEqual(await first.storage.runs.listEvents(id, 0), before);
  const dir = mkdtempSync(join(tmpdir(), 'purra-v4-tool-')), legacy = new SqliteAgentAdapters(join(dir, 'db'), { scope: 'legacy' });
  try {
    const snapshot = await first.storage.runs.get(id);
    await legacy.runs.begin({ requestedRunId: id, preset: snapshot.preset, budgets: snapshot.budgets, deadlineAt: null, metadata: {} });
    const [started] = (await first.storage.runs.listEvents(id, 0)).filter(event => event.kind === 'invocation.started');
    const { attempt, openedAt, ...receipt } = started.payload.receipt;
    await legacy.runs.openInvocation(id, receipt);
    await legacy.runs.settleInvocation(id, { invocationId: receipt.invocationId, status: 'completed' });
    await assert.rejects(legacy.runs.saveToolExecutionCheckpoint(id, checkpoint), { code: 'approval_storage_not_enabled' });
    assert.equal((await legacy.runs.get(id)).toolExecutionCheckpoint, undefined);
  } finally { legacy.close(); rmSync(dir, { recursive: true }); }
});

test('expiry after gateway approval is revalidated inside the actual claim transaction', async t => {
  const open = setup(t), first = open(), id = await paused(first), record = await approve(first, id);
  let now = Date.now();
  const owner = open({ clockMs: () => now }), checkpoint = (await owner.storage.runs.get(id)).toolExecutionCheckpoint;
  const call = checkpoint.assistant.toolCalls[0];
  await owner.storage.runs.executeToolOwned(id, async () => {
    assert.equal(await owner.approvals.gateway().request({ call, title: 'Write', riskLevel: 'write', summary: '', dispatch: { runId: id, call } }), 'approved');
    now = record.expiresAtMs + 1;
    await assert.rejects(owner.storage.idempotency.executeOnce('opaque-host-key', async () => assert.fail('expired dispatch'), { runId: id, call }), { code: 'approval_expired' });
  }, checkpoint);
  assert.equal((await owner.approvals.get(record.approvalId)).status, 'expired');
  await owner.storage.transaction(async (_, extra) => assert.equal(Object.keys(extra.tools).length, 0));
});

test('receipt identity uses explicit Run and call context and cannot move to another opaque key', async t => {
  const open = setup(t), first = open(), id = await paused(first), record = await approve(first, id);
  const checkpoint = (await first.storage.runs.get(id)).toolExecutionCheckpoint, call = checkpoint.assistant.toolCalls[0];
  await first.storage.runs.executeToolOwned(id, async () => {
    const receipt = await first.storage.idempotency.executeOnce('an opaque key without a Run prefix', async () => ({ content: 'written', effectState: 'committed' }), { runId: id, call });
    assert.equal(receipt.effectState, 'committed');
    await assert.rejects(first.storage.idempotency.executeOnce('different-key', async () => assert.fail('duplicate effect'), { runId: id, call }), { code: 'approval_intent_conflict' });
    await assert.rejects(first.storage.idempotency.executeOnce('an opaque key without a Run prefix', async () => assert.fail('changed arguments'), { runId: id, call: { ...call, arguments: { value: 43 } } }), { code: 'approval_intent_conflict' });
  }, checkpoint);
  await first.storage.transaction(async (_, extra) => {
    const entry = Object.values(extra.tools)[0];
    assert.equal(entry.approvalId, record.approvalId); assert.equal(entry.intentDigest, record.intentDigest);
    assert.equal(entry.approvalRevision, 2); assert.ok(entry.leaseEpoch >= 2);
  });
});


function writePlan(suffix = '') { return { workPlan: { title: 'Write and finish', steps: [
  { id: 'prepare' + suffix, title: 'Prepare', type: 'review', executor: 'model' },
  { id: 'write' + suffix, title: 'Write', type: 'write', executor: 'tool', capabilityNames: ['write'], dependsOn: ['prepare' + suffix] },
  { id: 'finish' + suffix, title: 'Finish', type: 'review', executor: 'model', dependsOn: ['write' + suffix] },
] } }; }
function writePlanner() { return { calls: 0, revisions: [], createPlan() { this.calls++; return writePlan(); },
  revisePlan(_request, _capabilities, turn) { this.revisions.push(turn.revision); return writePlan(String(turn.revision)); } }; }
const modeCases = JSON.parse(readFileSync(new URL('../../../../conformance/fixtures/approval_runtime_modes.json', import.meta.url), 'utf8')).cases;
for (const scenario of modeCases) test(`${scenario.id} approval restart preserves the active tool step and does not rerun planning`, async t => {
  const mode = scenario.id;
  const open = setup(t), planner = writePlanner();
  const first = open({ planner, ...(mode === 'promoted' ? { script: (_input, count) => ({ message: { role: 'assistant', content: '', toolCalls: [
    count === 1 ? { id: 'plan', name: 'request_plan', arguments: {} } : { id: 'write-1', name: 'write', arguments: { value: 42 } },
  ] }, finishReason: 'tool_calls' }) } : {}) });
  await first.storage.enableApprovals();
  const original = { ...request, planningMode: scenario.requestPlanningMode };
  const handle = await first.agent.submit(original, options);
  await assert.rejects(handle.result, ApprovalRequired);
  const saved = await first.storage.runs.get(handle.runId), checkpoint = saved.toolExecutionCheckpoint;
  assert.equal(checkpoint.executionProfile, scenario.checkpointProfile);
  if (checkpoint.planning) {
    assert.equal(checkpoint.planning.plan.steps[0].status, 'done');
    assert.equal(checkpoint.planning.plan.steps[1].status, 'running');
    assert.equal(checkpoint.planning.plan.steps[2].status, 'pending');
  }
  assert.equal(planner.calls, scenario.plannerCreationsBeforeWait);
  const restoredPlanner = writePlanner(), restored = open({ planner: restoredPlanner });
  await assert.rejects((await restored.agent.resume(handle.runId, original)).result, ApprovalRequired);
  assert.deepEqual(restored.counts, { model: 0, tool: 0 }); assert.equal(restoredPlanner.calls, 0);
  assert.deepEqual((await restored.storage.runs.get(handle.runId)).usage, saved.usage);
  await approve(restored, handle.runId);
  assert.equal((await (await restored.agent.resume(handle.runId, original)).result).output, 'done');
  assert.equal(restored.counts.tool, 1); assert.equal(restoredPlanner.calls, 0); assert.deepEqual(restoredPlanner.revisions, []);
});


test('replanning followed by another approval preserves revision and completed write receipts', async t => {
  const open = setup(t), original = { ...request, planningMode: 'planned' };
  const script = input => {
    const writes = input.messages.filter(message => message.role === 'tool').length;
    return writes >= 2 || input.tools.length === 0 ? { message: { role: 'assistant', content: 'done' }, finishReason: 'stop' }
      : { message: { role: 'assistant', content: '', toolCalls: [{ id: `write-${writes + 1}`, name: 'write', arguments: { value: 42 } }] }, finishReason: 'tool_calls' };
  };
  const first = open({ planner: writePlanner(), script }); await first.storage.enableApprovals();
  let handle = await first.agent.submit(original, options);
  await assert.rejects(handle.result, ApprovalRequired); await approve(first, handle.runId);
  const secondPlanner = writePlanner(), second = open({ planner: secondPlanner, script,
    run: () => ({ content: 'written', effectState: 'committed', planningDisposition: 'replan', planningReason: 'Continue with the second fixture' }) });
  handle = await second.agent.resume(handle.runId, original);
  await assert.rejects(handle.result, ApprovalRequired);
  const checkpoint = (await second.storage.runs.get(handle.runId)).toolExecutionCheckpoint;
  assert.equal(checkpoint.planning.revision, 1); assert.equal(checkpoint.assistant.toolCalls[0].id, 'write-2');
  assert.deepEqual(secondPlanner.revisions, [1]); assert.equal(second.counts.tool, 1);
  const lastPlanner = writePlanner(), last = open({ planner: lastPlanner, script });
  await approve(last, handle.runId);
  assert.equal((await (await last.agent.resume(handle.runId, original)).result).output, 'done');
  assert.equal(last.counts.tool, 1); assert.equal(lastPlanner.calls, 0); assert.deepEqual(lastPlanner.revisions, []);
  await last.storage.transaction(async (_, extra) => assert.equal(Object.values(extra.tools).filter(entry => entry.state === 'complete').length, 2));
});


for (const mode of ['reactive', 'planned']) test(`Agent Tree ${mode} Root waits for approval after a read-only Child without repeating the Child`, async t => {
  const open = setup(t), planner = mode === 'planned' ? { createPlan: () => ({ workPlan: { title: 'Delegate, write, finish', steps: [
    { id: 'delegate', title: 'Delegate', type: 'read', executor: 'tool', capabilityNames: ['delegateToAgents'] },
    { id: 'write', title: 'Write', type: 'write', executor: 'tool', capabilityNames: ['write'], dependsOn: ['delegate'] },
    { id: 'finish', title: 'Finish', type: 'review', executor: 'model', dependsOn: ['write'] },
  ] } }), revisePlan: () => assert.fail('unexpected replanning') } : undefined;
  const script = input => {
    if (input.messages.some(message => message.attributes?.agentId)) {
      assert.equal(input.tools.some(tool => tool.name === 'write'), false);
      return { message: { role: 'assistant', content: 'child result' }, finishReason: 'stop' };
    }
    const calls = input.messages.flatMap(message => message.toolCalls ?? []);
    return calls.some(call => call.name === 'write') || input.tools.length === 0
      ? { message: { role: 'assistant', content: 'done' }, finishReason: 'stop' }
      : { message: { role: 'assistant', content: '', toolCalls: [calls.some(call => call.name === 'delegateToAgents')
        ? { id: 'write-1', name: 'write', arguments: { value: 42 } }
        : { id: 'delegate', name: 'delegateToAgents', arguments: { children: [{ name: 'reader', title: 'Read', instruction: 'Read only', objective: 'Report' }] } },
      ] }, finishReason: 'tool_calls' };
  };
  const first = open({ tree: true, planner, script }); await first.storage.enableApprovals();
  const original = { ...request, planningMode: mode }, handle = await first.agent.submit(original, options);
  await assert.rejects(handle.result, ApprovalRequired);
  const children = await first.storage.runTree.listDescendants(handle.runId);
  assert.equal(children.length, 1); assert.equal(children[0].status, 'done');
  const restored = open({ tree: true, planner, script });
  await assert.rejects((await restored.agent.resume(handle.runId, original)).result, ApprovalRequired);
  assert.deepEqual(restored.counts, { model: 0, tool: 0 });
  await approve(restored, handle.runId);
  assert.equal((await (await restored.agent.resume(handle.runId, original)).result).output, 'done');
  assert.equal(restored.counts.tool, 1);
  assert.deepEqual(await restored.storage.runTree.listDescendants(handle.runId), children);
  assert.equal((await restored.storage.runTree.getRun(handle.runId)).status, 'done');
});

test('Auto can request a remaining plan after the first approved write and suspend at the second', async t => {
  const open = setup(t), original = { ...request, planningMode: 'auto' }, first = open({ planner: writePlanner() });
  await first.storage.enableApprovals();
  const handle = await first.agent.submit(original, options);
  await assert.rejects(handle.result, ApprovalRequired);
  assert.equal((await first.storage.runs.get(handle.runId)).toolExecutionCheckpoint.initialPlanningOpen, true);
  await approve(first, handle.runId);
  const planner = writePlanner();
  const second = open({ planner, script: (input, count) => ({ message: { role: 'assistant', content: '', toolCalls: [
    count === 1 ? { id: 'remaining-plan', name: 'request_remaining_plan', arguments: {} }
      : { id: 'write-2', name: 'write', arguments: { value: 42 } },
  ] }, finishReason: 'tool_calls' }) });
  await assert.rejects((await second.agent.resume(handle.runId, original)).result, ApprovalRequired);
  assert.equal(second.counts.tool, 1); assert.equal(planner.calls, 1);
  const checkpoint = (await second.storage.runs.get(handle.runId)).toolExecutionCheckpoint;
  assert.equal(checkpoint.executionProfile, 'planned'); assert.equal(checkpoint.initialPlanningOpen, false);
  const rebound = writePlanner(), last = open({ planner: rebound }); await approve(last, handle.runId);
  assert.equal((await (await last.agent.resume(handle.runId, original)).result).output, 'done');
  assert.equal(last.counts.tool, 1); assert.equal(rebound.calls, 0);
});

for (const status of ['pending','approved','expired']) test(`approval inspection observes ${status} without mutation`,async t => {
  let now=Date.now(); const open=setup(t),host=open({clockMs:()=>now,expiry:now+60000}),id=await paused(host);
  if(status==='approved') await approve(host,id);
  if(status==='expired') now+=60000;
  const db=new DatabaseSync(host.path);t.after(()=>db.close());
  const snapshot=()=>JSON.stringify(db.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name").all().map(({name})=>[name,db.prepare(`SELECT * FROM ${name}`).all()]));
  const before=snapshot(),report=await host.storage.inspectRecovery(id);
  assert.equal(snapshot(),before);assert.equal(report.observations.approvalState,status);
  assert.equal(report.observations.approvalCheckpointIntent,'matched');assert.equal(report.observations.approvalRecords,1);
  assert.equal(report.observations.approvalReceipt,'absent');assert.equal(report.observations.attemptsAfterCheckpoint,0);
  assert.deepEqual(host.counts,{model:1,tool:0});
  const [record]=await host.approvals.listPending({runId:id});
  assert.equal(JSON.stringify(report).includes(record.approvalId),false);assert.equal(JSON.stringify(report).includes(record.intentDigest),false);
});
