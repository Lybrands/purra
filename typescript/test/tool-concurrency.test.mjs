import assert from 'node:assert/strict';
import test from 'node:test';
import {readFileSync} from 'node:fs';
import {setImmediate as tick} from 'node:timers/promises';
import {AgentCanceledError,AgentError} from 'purra';
import {ToolCatalog} from '../dist/tools/catalog.js';

const fixture=JSON.parse(readFileSync(new URL('../../conformance/fixtures/tool_concurrency.json',import.meta.url)));
const deferred=()=>{let resolve;const promise=new Promise(r=>{resolve=r;});return {promise,resolve};};
function harness({concurrency=2,safe=true,failing,scope,observer}={}) {
  const gates=fixture.calls.map(deferred),started=fixture.calls.map(deferred);
  const state={active:0,peak:0,starts:[],scopes:[],events:[]};
  const controller=new AbortController();
  const catalog=new ToolCatalog([{name:'read',description:'Read',inputSchema:{type:'object',properties:{index:{type:'integer'}},required:['index'],additionalProperties:false},
    concurrencySafe:safe,policy:{mode:'read',title:'Read'},scope:async args=>{state.scopes.push(args.index);return scope?.(args.index,state.scopes.filter(i=>i===args.index).length);},
    run:async (args,context)=>{
      const i=args.index;state.starts.push(i);state.active++;state.peak=Math.max(state.peak,state.active);started[i].resolve();
      let abort;
      try {
        await Promise.race([gates[i].promise,new Promise((_,reject)=>{abort=()=>reject(new AgentCanceledError());context.signal?.addEventListener('abort',abort,{once:true});})]);
        return {content:i,effectState:'not_started',...(i===failing?{errorCode:'read_failed'}:{})};
      } finally {state.active--;context.signal?.removeEventListener('abort',abort);}
    }}],{limits:{maxConcurrency:concurrency}});
  const run=()=>catalog.executeBatch(fixture.calls.map(i=>({id:String(i),name:'read',arguments:{index:i}})),{executionKey:'test',signal:controller.signal,
    onEvent:async event=>{state.events.push(event);await observer?.(event);}});
  return {...state,state,catalog,run,controller,wait:i=>started[i].promise,release:i=>gates[i].resolve()};
}
for(const value of fixture.invalidLimits)test(`invalid concurrency ${JSON.stringify(value)}`,()=>{
  assert.throws(()=>new ToolCatalog([],{limits:{maxConcurrency:value}}),TypeError);
});
for(const [concurrency,safe,expected] of [[1,true,1],[2,false,1],[2,true,2]])test(`opt in ${concurrency}/${safe}, bounded and ordered`,{timeout:3000},async()=>{
  const h=harness({concurrency,safe}),running=h.run();await h.wait(0);
  if(expected===2){await h.wait(1);assert.deepEqual(h.scopes.slice(0,4),fixture.calls);h.release(1);await h.wait(2);h.release(2);await h.wait(3);h.release(3);h.release(0);}
  else {assert.deepEqual(h.starts,[0]);for(const i of fixture.calls){await h.wait(i);h.release(i);}}
  const result=await running;
  assert.equal(h.state.peak,expected);assert.equal(h.state.active,0);
  assert.deepEqual(result.messages.map(m=>m.toolCallId),fixture.calls.map(String));
  assert.deepEqual(result.messages.map(m=>m.content),fixture.calls);
  const completed=h.events.filter(e=>e.type==='tool_completed');
  assert.deepEqual(completed.map(e=>e.toolCallId).sort(),fixture.calls.map(String));
  if(expected===2)assert.equal(completed[0].toolCallId,'1');
});
test('first failure closes queue and keeps in-flight success',{timeout:3000},async()=>{
  const h=harness({failing:1}),running=h.run();await h.wait(0);await h.wait(1);h.release(1);
  while(!h.events.some(e=>e.type==='tool_completed'))await tick();
  h.release(0);const result=await running;
  assert.deepEqual(h.starts,[0,1]);assert.equal(h.state.active,0);
  assert.equal(result.messages[0].content,0);
  assert.deepEqual(result.messages.slice(1).map(m=>m.content.error.code),['read_failed',fixture.skippedError,fixture.skippedError]);
});
test('last admission scope rejects before any handler',async()=>{
  const h=harness({scope:i=>i===3?'denied':undefined});
  await assert.rejects(h.run(),e=>e.code==='tool_scope_violation');
  assert.deepEqual(h.starts,[]);assert.deepEqual(h.scopes,fixture.calls);
});
test('dispatch revalidates current scope',{timeout:3000},async()=>{
  const h=harness({scope:(i,count)=>i===2&&count>1?'revoked':undefined}),running=h.run();
  await h.wait(0);await h.wait(1);h.release(1);
  while(h.scopes.filter(i=>i===2).length<2)await tick();
  h.release(0);const result=await running;
  assert.equal(result.messages[0].content,0);
  assert.equal(result.messages[2].content.error.code,'tool_scope_violation');
  assert.deepEqual(h.starts,[0,1]);assert.equal(h.state.active,0);
});
test('cancellation drains active handlers and closes queue',{timeout:3000},async()=>{
  const h=harness(),running=h.run();const canceled=assert.rejects(running,AgentCanceledError);
  await h.wait(0);await h.wait(1);h.controller.abort();await canceled;
  assert.deepEqual(h.starts,[0,1]);assert.equal(h.state.active,0);
  const count=h.events.length;await tick();assert.equal(h.events.length,count);
});
test('observer failure remains fatal and joins sibling cleanup',{timeout:3000},async()=>{
  const h=harness({observer:event=>{if(event.type==='tool_completed')throw new Error('observer failed');}}),running=h.run();
  const failed=assert.rejects(running,/observer failed/);await h.wait(0);await h.wait(1);h.release(1);await failed;
  assert.deepEqual(h.starts,[0,1]);assert.equal(h.state.active,0);
});
test('lost lease at start fence prevents dispatch and propagates',{timeout:3000},async()=>{
  const h=harness({observer:event=>{if(event.type==='tool_started'&&event.toolCallId==='1')throw new AgentError('agent_run_lease_lost','stale lease');}});
  await assert.rejects(h.run(),e=>e.code==='agent_run_lease_lost');
  assert.ok(!h.starts.includes(1));assert.ok(!h.starts.includes(2));assert.equal(h.state.active,0);
});

test('execution identity binds concurrency declarations and limits',()=>{
  const serial=harness({concurrency:1,safe:true}).catalog;
  const parallel=harness({concurrency:2,safe:true}).catalog;
  const unsafe=harness({concurrency:2,safe:false}).catalog;
  assert.notDeepEqual(serial.executionSnapshotFor(),parallel.executionSnapshotFor());
  assert.notDeepEqual(parallel.executionSnapshotFor(),unsafe.executionSnapshotFor());
});

test('handler cleanup is joined after cancellation even when completion is delayed',{timeout:3000},async()=>{
  const entered=deferred(),cleanup=deferred(),release=deferred();let active=0;
  const catalog=new ToolCatalog([{name:'read',description:'Read',inputSchema:{type:'object'},concurrencySafe:true,policy:{mode:'read',title:'Read'},
    run:async (_,context)=>{active++;entered.resolve();try{await new Promise(resolve=>context.signal.addEventListener('abort',resolve,{once:true}));cleanup.resolve();await release.promise;return {content:'late',effectState:'not_started'};}finally{active--;}}}],{limits:{maxConcurrency:2}});
  const controller=new AbortController();let settled=false;
  const running=catalog.executeBatch([0,1,2].map(i=>({id:String(i),name:'read',arguments:{}})),{executionKey:'cleanup',signal:controller.signal});
  const failed=assert.rejects(running,AgentCanceledError).then(()=>{settled=true;});
  await entered.promise;await tick();controller.abort();await cleanup.promise;await tick();
  assert.equal(settled,false);assert.equal(active,2);release.resolve();await failed;assert.equal(active,0);
});

test('persisted Agent composition binds the configured concurrency limit',async()=>{
  const {Agent}=await import('purra');
  const {testGateway}=await import('./support/model-gateway.mjs');
  const fingerprints=[];
  for(const maxConcurrency of [1,2]){
    const agent=new Agent({toolLimits:{maxConcurrency},model:testGateway({async invoke(){return {message:{role:'assistant',content:'done'},finishReason:'stop'};}})});
    const handle=await agent.submit({messages:[{role:'user',content:'run'}]},{budgets:{maxRunGenerationTokens:null}});
    await handle.result;
    fingerprints.push((await handle.snapshot()).preset.compositionFingerprint);
  }
  assert.notEqual(fingerprints[0],fingerprints[1]);
});
