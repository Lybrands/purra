import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';
import { buildRecoveryInspection, inspectRecovery, checkIntegration, assertModelGatewayConforms } from '../dist/index.js';
import { testGateway } from './support/model-gateway.mjs';
const fixture = JSON.parse(readFileSync(new URL('../../conformance/fixtures/recovery_inspection.json', import.meta.url)));
for (const item of fixture.cases) test(`recovery: ${item.id}`, () => {
  const report = buildRecoveryInspection({...item.state, content:'credential-secret', privateReasoning:'credential-secret'});
  assert.deepEqual(report.blockers,item.blockers);
  assert.deepEqual(report.cautions,item.cautions ?? []);
  assert.equal(report.authority,'diagnosis_only');
  assert.equal(JSON.stringify(report).includes('credential-secret'),false);
  assert.equal(report.suggestedActions.at(-1),'revalidate_execution');
  assert.ok(report.unknown.includes('externalToolEffects'));
  assert.ok(report.unknown.includes('agentTreeOwnership'));
  if (!Object.keys(item.state).length) {
    assert.equal(report.observations.attemptsAfterCheckpoint,null);
    assert.ok(report.unknown.includes('usage'));
  }
});
for (const state of fixture.invalid) test(`invalid recovery: ${JSON.stringify(state)}`,()=>assert.throws(()=>buildRecoveryInspection(state),TypeError));
test('generic inspection only reads get',async()=>{
  let calls=0;
  const repository=new Proxy({get:async id=>{assert.equal(id,'run');calls++;return {status:'running',executionCheckpoint:{}};}},{get(t,k){assert.equal(k,'get');return t[k];}});
  const report=await inspectRecovery(repository,'run');
  assert.equal(calls,1);assert.equal(report.observations.checkpoint,'present');assert.equal(report.observations.lease,'unknown');assert.equal(report.observations.unknownToolReceipts,null);
});
test('report wraps actual conformance assertions, gates external probes and redacts errors',async()=>{
  const calls=[];
  const gateway=testGateway({invoke:async()=>{calls.push('good');return {message:{role:'assistant',content:'fixture'},finishReason:'stop'};}});
  const checks=[{capability:'gateway',category:'deterministic',probe:()=>assertModelGatewayConforms({gateway})},
    {capability:'storage',category:'deterministic',probe:async()=>{calls.push('bad');throw Error('credential-secret');}},
    {capability:'gateway',category:'real_provider_mcp',probe:async()=>{calls.push('live');}}];
  const report=await checkIntegration({component:'host',version:'1',checks,declaredCapabilities:{gateway:'supported'}});
  assert.deepEqual(calls,['good','bad']);
  const row=(capability,category)=>report.checks.find(r=>r.capability===capability&&r.category===category);
  assert.equal(row('storage','deterministic').errorCode,'storage_nonconforming');
  assert.equal(row('gateway','deterministic').status,'passed');
  assert.equal(row('gateway','real_provider_mcp').status,'not_run');
  assert.equal(JSON.stringify(report).includes('credential-secret'),false);
  await checkIntegration({component:'host',version:'1',checks,enabledCategories:['real_provider_mcp']});
  assert.equal(calls.at(-1),'live');
});
test('report preflight and cancellation',async()=>{
  let calls=0;
  const check={capability:'gateway',category:'deterministic',probe:async()=>{calls++;throw new DOMException('cancel','AbortError');}};
  await assert.rejects(checkIntegration({component:'host',version:'1',checks:[check,check]}),TypeError);assert.equal(calls,0);
  await assert.rejects(checkIntegration({component:'host',version:'1',checks:[check]}),{name:'AbortError'});
});
