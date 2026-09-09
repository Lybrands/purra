import assert from 'node:assert/strict';
import test from 'node:test';
import { mkdtempSync, rmSync, readFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import { ApprovalIntent, ApprovalRequired } from 'purra';
import { SqliteAgentAdapters } from '../dist/index.js';

import { createApprovalHost, request, paused, approve } from './approval-host.mjs';
const options = { budgets: { maxRunGenerationTokens: null } };
function setup(t) {
  const dir = mkdtempSync(join(tmpdir(), 'purra-tool-resume-')), stores = [];
  t.after(() => { stores.forEach(store => store.close()); rmSync(dir, { recursive: true }); });
  return (settings = {}) => {
    const host = createApprovalHost(join(dir, 'db'), settings);
    stores.push(host.storage);
    return host;
  };
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

for (const fault of ['expired','replaced-owner','advanced-epoch']) test(`late approved result is fenced after ${fault}`,{timeout:5000},async t=>{
  const open=setup(t),first=open(),id=await paused(first);
  let enter,release;const entered=new Promise(r=>enter=r),gate=new Promise(r=>release=r);
  const owner=open({run:async()=>{enter();await gate;return {content:'late committed',effectState:'committed'};}}),other=open();
  await approve(owner,id);
  const handle=await owner.agent.resume(id,request);
  // Attach before inducing failure so delayed rejection is always observed.
  const outcome=handle.result.then(value=>({value}),error=>({error}));
  await entered;
  let replacement,claim;
  await other.storage.transaction(async(_,extra)=>{
    replacement=fault==='expired'?{...extra.leases[id],expires:0}:{...extra.leases[id],
      owner:fault==='replaced-owner'?'replacement-owner':extra.leases[id].owner,
      epoch:extra.leases[id].epoch+1,expires:Date.now()+60000};
    extra.leases[id]=replacement;claim=structuredClone(Object.values(extra.tools)[0]);
  });
  try {
    const before=await other.storage.runs.get(id);
    const events=await other.storage.runs.listEvents(id,0);
    if(fault==='expired'||fault==='advanced-epoch') await new Promise(r=>setTimeout(r,1200));
    release();const result=await outcome;assert.ok(result.error);
    await other.storage.transaction(async(_,extra)=>{
      assert.deepEqual(Object.values(extra.tools),[claim]);
      if(fault!=='expired') assert.deepEqual(extra.leases[id],replacement);
    });
    assert.deepEqual(await other.storage.runs.get(id),before);
    assert.deepEqual(await other.storage.runs.listEvents(id,0),events);
    const reopened=open();await assert.rejects(async()=>{const resumed=await reopened.agent.resume(id,request);await resumed.result;});
    assert.deepEqual(reopened.counts,{model:0,tool:0});assert.equal(owner.counts.tool,1);
    assert.ok((await reopened.storage.inspectRecovery(id)).blockers.includes('tool_effect_unknown'));
  } finally {release();await outcome;}
});

for(const field of ['intentDigest','runId','callId','approvalRevision','approvalId','missingApprovalId'])test(`reconciliation rejects corrupted approval claim ${field}`,async t=>{
 const open=setup(t),first=open(),id=await paused(first),owner=open({run:()=>({content:'uncertain',effectState:'unknown'})});
 await approve(owner,id);await assert.rejects((await owner.agent.resume(id,request)).result,{code:'tool_effect_unknown'});
 const key=await owner.storage.transaction(async(_,extra)=>{
  const [key,claim]=Object.entries(extra.tools)[0];if(field==='missingApprovalId')delete claim.approvalId;else claim[field]=field==='approvalRevision'?999:'corrupt';return key;
 });
 const before=await owner.storage.transaction(async(_,extra)=>structuredClone(extra.tools));
 await assert.rejects(owner.storage.reconcileTool(key,{result:{content:'verified',effectState:'committed'}}),{code:'approval_claim_conflict'});
 assert.deepEqual(await owner.storage.transaction(async(_,extra)=>structuredClone(extra.tools)),before);
});
for(const proof of [{result:{content:'verified',effectState:'committed'}},{notExecuted:true}])test(`host reconciliation retains terminal run: ${'result' in proof?'completed':'not executed'}`,async t=>{
 const open=setup(t),first=open(),id=await paused(first),owner=open({run:()=>({content:'uncertain',effectState:'unknown'})});
 await approve(owner,id);await assert.rejects((await owner.agent.resume(id,request)).result,{code:'tool_effect_unknown'});
 const key=await owner.storage.transaction(async(_,extra)=>Object.keys(extra.tools)[0]);
 await owner.storage.reconcileTool(key,proof);
 const reopened=open();
 await reopened.storage.transaction(async(_,extra)=>{
  if('result' in proof){assert.equal(extra.tools[key].state,'complete');assert.equal(extra.tools[key].result.effectState,'committed');}
  else assert.equal(extra.tools[key],undefined);
 });
 await assert.rejects(reopened.agent.resume(id,request),{code:'run_terminal'});
 assert.deepEqual(reopened.counts,{model:0,tool:0});
});

for(const completed of [true,false])for(const gate of ['allowed','revoked','expired'])test(`running tool checkpoint resumes after host reconciliation: ${completed?'completed':'not executed'} ${gate}`,async t=>{
 const open=setup(t),first=open(),id=await paused(first),db=new DatabaseSync(first.path);t.after(()=>db.close());
 let effects=0;
 const owner=open({run:()=>{
  db.exec("CREATE TRIGGER fixture_receipt_failure BEFORE UPDATE ON purra_state BEGIN SELECT RAISE(ABORT, 'fixture receipt failure'); END");
  if(!completed)throw Error('synthetic handler stopped before effect');
  effects++;return {content:'written',effectState:'committed'};
 }});
 const record=await approve(owner,id);
 await assert.rejects((await owner.agent.resume(id,request)).result);
 assert.equal(effects,Number(completed));db.exec('DROP TRIGGER fixture_receipt_failure');
 await owner.storage.transaction(async(_,extra)=>{extra.leases[id].expires=0;});
 const restored=open({expiry:record.expiresAtMs});assert.equal((await restored.storage.runs.get(id)).status,'running');
 const key=await restored.storage.transaction(async(_,extra)=>Object.keys(extra.tools)[0]);
 const schedule=restored.storage.recoverySchedule({clockMs:()=>100});
 await schedule.wake(id);await schedule.settle(id,await schedule.ready(id),true);
 const proof=completed?{result:{content:'written',effectState:'committed'}}:{notExecuted:true};
 db.exec("CREATE TRIGGER fixture_reconcile_failure BEFORE UPDATE ON purra_state BEGIN SELECT RAISE(ABORT, 'fixture rollback'); END");
 await assert.rejects(restored.storage.reconcileTool(key,proof));
 db.exec('DROP TRIGGER fixture_reconcile_failure');
 assert.equal(await schedule.ready(id),null);
 assert.equal(await restored.storage.transaction(async(_,extra)=>extra.tools[key].state),'claimed');
 await restored.storage.reconcileTool(key,proof);
 assert.notEqual(await schedule.ready(id),null);
 const final=open({expiry:record.expiresAtMs,...(gate==='expired'?{clockMs:()=>record.expiresAtMs+1}:{}),scope:()=>gate!=='revoked',
  script:input=>{assert.ok(input.messages.some(m=>m.role==='tool')||input.tools.length===0);return {message:{role:'assistant',content:'done'},finishReason:'stop'};},
  run:()=>{effects++;return {content:'written',effectState:'committed'};}});
 if(gate==='revoked'||(gate==='expired'&&!completed)){
  await assert.rejects(async()=>{await (await final.agent.resume(id,request)).result;});
  assert.deepEqual(final.counts,{model:0,tool:0});assert.equal(effects,Number(completed));return;
 }
 assert.equal((await (await final.agent.resume(id,request)).result).output,'done');
 assert.equal(final.counts.tool,completed?0:1);assert.equal(effects,1);
});

test('recovery worker rediscovers approval after reopen and never polls the model while waiting', async t => {
  const { RecoveryWorker } = await import('purra');
  const open = setup(t), first = open(), id = await paused(first), host = open();
  const worker = new RecoveryWorker({
    discover: () => host.storage.listRunning(),
    inspect: id => host.storage.inspectRecovery(id),
    resume: async id => (await host.agent.resume(id, request)).result,
  });
  for (let i = 0; i < 2; i++) {
    const [result] = await worker.runOnce();
    assert.equal(result.action, 'blocked');
    assert.ok(result.reasons.includes('approval_required'));
  }
  assert.deepEqual(host.counts, { model: 0, tool: 0 });
  await approve(host, id);
  assert.deepEqual(await worker.runOnce(), [{ runId: id, action: 'settled', reasons: [] }]);
  assert.equal(host.counts.tool, 1);
  assert.deepEqual(await worker.runOnce(), []);
});

test('worker clean diagnosis cannot bypass scope changed before public resume', async t => {
  const { RecoveryWorker } = await import('purra');
  const open = setup(t), first = open(), id = await paused(first);
  let allowed = true;
  const host = open({ scope: () => allowed });
  await approve(host, id);
  const worker = new RecoveryWorker({
    discover: () => host.storage.listRunning(),
    inspect: async id => {
      const report = await host.storage.inspectRecovery(id);
      assert.deepEqual(report.blockers, []);
      allowed = false;
      return report;
    },
    resume: async id => (await host.agent.resume(id, request)).result,
  });
  assert.deepEqual(await worker.runOnce(), [{ runId: id, action: 'failed', reasons: ['resume_failed'] }]);
  assert.deepEqual(host.counts, { model: 0, tool: 0 });
});

test('worker service wakes after committed approval', { timeout: 5000 }, async t => {
  const { RecoveryWorker } = await import('purra');
  const open = setup(t), host = open(), id = await paused(host);
  const stop = new AbortController(), reports = [];
  const schedule = host.storage.recoverySchedule();
  const cursor = host.storage.recoveryCursor('service');
  const worker = new RecoveryWorker({ schedule, discover: () => cursor.discover(), acknowledge: ids => cursor.acknowledge(ids),
    inspect: id => host.storage.inspectRecovery(id),
    resume: async id => (await host.agent.resume(id, request)).result,
  });
  try {
    await worker.run({ signal: stop.signal, pollIntervalMs: 60000, maxBackoffMs: 60000,
      onScan: async report => {
        reports.push(report);
        if (reports.length === 1) {
          assert.equal(report[0].action, 'blocked');
          assert.deepEqual(host.counts, { model: 1, tool: 0 });
          await approve(host, id);
          worker.wake();
        } else stop.abort();
      },
    });
    assert.equal(reports.length, 2); assert.equal(reports[1][0].action, 'settled');
    assert.equal(host.counts.tool, 1);
    assert.deepEqual(await host.storage.listRunning(), []);
  } finally { stop.abort(); }
});

for (const decision of ['approve', 'reject']) test(`decision and registered wake commit atomically: ${decision}`, async t => {
  const open = setup(t), host = open(), id = await paused(host);
  const record = (await host.approvals.listPending({ runId: id }))[0];
  const schedule = host.storage.recoverySchedule({ clockMs: () => 100, maxFailures: 1 });
  await schedule.wake(id); await schedule.settle(id, await schedule.ready(id), true);
  assert.equal((await schedule.check(id)).reason, "retry_exhausted");
  const command = { approvalId: record.approvalId, expectedRevision: record.revision, intentDigest: record.intentDigest, commandKey: 'atomic', decision };
  const db = new DatabaseSync(host.path); t.after(() => db.close());
  db.exec("CREATE TRIGGER fixture_decision_failure BEFORE UPDATE ON purra_state BEGIN SELECT RAISE(ABORT, 'fixture rollback'); END");
  await assert.rejects(host.approvals.decide(command, { principalId: 'host' }));
  db.exec('DROP TRIGGER fixture_decision_failure');
  assert.equal((await host.approvals.get(record.approvalId)).status, 'pending');
  assert.equal(await schedule.ready(id), null);
  const receipt = await host.approvals.decide(command, { principalId: 'host' });
  const token = await schedule.ready(id); assert.notEqual(token, null);
  await schedule.settle(id, token, true);
  assert.deepEqual(await host.approvals.decide(command, { principalId: 'host' }), receipt);
  assert.equal(await schedule.ready(id), null);
});

test('terminal schedule pruning fences old scans before and after reopen', async t => {
  const open = setup(t), host = open(), id = await paused(host);
  await approve(host, id);
  const schedule = host.storage.recoverySchedule({ clockMs: () => 100 });
  await schedule.wake(id); const stale = await schedule.ready(id);
  assert.deepEqual(await host.storage.pruneRecoverySchedule([id]), []);
  await (await host.agent.resume(id, request)).result;
  assert.deepEqual(await host.storage.pruneRecoverySchedule([id, id]), [id]);
  assert.equal(await schedule.settle(id, stale, true), false);
  const reopened = open(), next = reopened.storage.recoverySchedule({ clockMs: () => 100 });
  await next.wake(id);
  assert.equal(await next.settle(id, stale, true), false);
  assert.equal((await reopened.storage.runs.get(id)).status, 'completed');
});

test('candidate page can feed worker without resuming a terminal Run', async t => {
  const { RecoveryWorker } = await import('purra');
  const open = setup(t), host = open(), id = await paused(host);
  await approve(host, id); await (await host.agent.resume(id, request)).result;
  assert.deepEqual((await host.storage.inspectRecoveryMany([id]))[id], await host.storage.inspectRecovery(id));
  assert.deepEqual((await host.storage.listRunCandidates({ limit: 1 })).runIds, [id]);
  const calls = [];
  const worker = new RecoveryWorker({
    discover: async () => (await host.storage.listRunCandidates({ limit: 1 })).runIds,
    inspect: id => host.storage.inspectRecovery(id), resume: async id => { calls.push(id); },
  });
  const [result] = await worker.runOnce();
  assert.equal(result.action, 'blocked'); assert.ok(result.reasons.includes('run_terminal'));
  assert.deepEqual(calls, []);
});
