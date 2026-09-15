import assert from 'node:assert/strict';
import test from 'node:test';
import { setTimeout as delay } from 'node:timers/promises';
import { deliverAgentResults } from '../dist/agent-tree-delivery.js';
import { Agent, AgentTreePolicy, AgentTreeRunSupervisor, InMemoryAgentAdapters } from '../dist/index.js';
import { RunSession, allowAllOutput } from '../dist/run/session.js';
import { treeTestGateway as testGateway } from './support/model-gateway.mjs';
const deferred = () => { let resolve; const promise = new Promise(r => resolve = r); return {promise, resolve}; };
const aggregate = (...ids) => ({state:'ready',pendingRunIds:[],requiredFailures:[],results:ids.map(runId=>({runId,status:'done'}))});

test('per-result dispatch delivers without waiting for slow Child and never repeats results', {timeout:3000}, async () => {
  const delivered = deferred(), calls = [];
  await deliverAgentResults(async notify => {
    await delay(20); assert.equal(calls.length,0);
    notify(aggregate('a'));
    await delivered.promise;
    notify(aggregate('a','b'));
    return aggregate('a','b');
  }, async rows => {calls.push(rows.map(r=>r.runId));delivered.resolve();});
  assert.deepEqual(calls,[['a'],['b']]);
});

test('results arriving during output stay outside the frozen selection', {timeout:3000}, async () => {
  const started=deferred(), arrived=deferred(), calls=[];
  await deliverAgentResults(async notify => {
    notify(aggregate('a')); await started.promise;
    notify(aggregate('a','b')); arrived.resolve(); return aggregate('a','b');
  }, async rows => {calls.push(rows.map(r=>r.runId)); if(calls.length===1){started.resolve();await arrived.promise;assert.equal(rows.length,1);}});
  assert.deepEqual(calls,[['a'],['b']]);
});

for (const presentResults of [false, true]) test(`main Agent receives results with host presentation ${presentResults}`,  {timeout:3000}, async () => {
  const slow = deferred();
  const adapters = new InMemoryAgentAdapters();
  const observed = [];
  const model = testGateway({async invoke(request) {
    const texts = request.messages.map(m => m.content);
    if (request.messages.at(-1)?.attributes?.publicPresentation) return {message:{role:'assistant',content:'Main Agent feedback'},finishReason:'stop'};
    if (texts.includes('CHILD_FAST') || texts.includes('CHILD_SLOW')) {
      if (texts.includes('CHILD_SLOW')) await slow.promise;
      return {message:{role:'assistant',content:'private child result'},finishReason:'stop'};
    }
    const receipt = request.messages.filter(m => m.role === 'tool').at(-1);
    if (receipt === undefined) return {message:{role:'assistant',content:'',toolCalls:[{
      id:'delegate',name:'delegateToAgents',arguments:{children:[
        {name:'fast',title:'fast',instruction:'CHILD_FAST',objective:'Analyze'},
        {name:'slow',title:'slow',instruction:'CHILD_SLOW',objective:'Analyze'},
      ]},
    }]},finishReason:'tool_calls'};
    const data = typeof receipt.content === 'string' ? JSON.parse(receipt.content) : receipt.content;
    observed.push(data);
    if (data.pendingRunIds.length) {
      slow.resolve();
      return {message:{role:'assistant',content:'',toolCalls:[{
        id:'receive',name:'receiveAgentResults',arguments:{runIds:data.runIds,afterRunIds:data.results.map(r => r.runId)},
      }]},finishReason:'tool_calls'};
    }
    return {message:{role:'assistant',content:'Unified final answer'},finishReason:'stop'};
  }});
  const agent = new Agent({model,runRepository:adapters.runs,outputPublisher:adapters.outputs,
    agentTree:{repository:adapters.runTree,policy:{resultPresentationInstruction:presentResults ? "Explain this result." : null}}});
  const handle = await agent.submit({messages:[{role:'user',content:'Analyze'}],enabledTools:['delegateToAgents','receiveAgentResults']},
    {budgets:{maxRunGenerationTokens:null,maxModelCalls:20}});
  try {
    assert.equal((await handle.result).output,'Unified final answer');
    assert.ok(observed[0].pendingRunIds.length);
    assert.equal(observed.at(-1).pendingRunIds.length,0);
    const events = await adapters.runs.listEvents(handle.runId,0);
    assert.deepEqual(events.filter(e => e.payload.schemaVersion === 'purra.parent-delivery/v1').map(e => e.payload.state), presentResults ? ['started','completed','started','completed'] : []);
    if (!presentResults) {
      await assert.rejects(agent.reportAgentResults(handle.runId, [], new AbortController().signal), {code:'agent_feedback_policy_required'});
      assert.deepEqual(await adapters.runs.listEvents(handle.runId,0), events);
    }
  } finally { slow.resolve(); await handle.result.catch(() => {}); }
});

test('cancellation during parent output cleans producer and presenter', {timeout:3000}, async () => {
  const started=deferred(), controller=new AbortController();
  let producerClosed=false, presentationClosed=false;
  const run=deliverAgentResults(async(notify,signal)=>{
    try{notify(aggregate('a'));await delay(5000,undefined,{signal});return aggregate('a');}
    finally{producerClosed=true;}
  },async(rows,signal)=>{
    try{started.resolve();await delay(5000,undefined,{signal});}finally{presentationClosed=true;}
  },controller.signal);
  const rejected=assert.rejects(run);
  await started.promise;controller.abort();await rejected;
  assert.equal(producerClosed,true);assert.equal(presentationClosed,true);
});


test('Agent scheduling needs no public presentation configuration', () => {
  const adapters = new InMemoryAgentAdapters();
  new AgentTreeRunSupervisor({repository:adapters.runTree,executor:{async execute(){assert.fail('must not execute');}}});
});
