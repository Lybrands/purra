import assert from 'node:assert/strict';
import test from 'node:test';
import { AgentCapabilityGrant, AgentTreeRunSupervisor, RunCommandService, InMemoryRunTreeRepository } from '../dist/index.js';
const deferred = () => { let resolve; const promise = new Promise(r => {resolve=r;}); return {resolve,promise}; };
const spec = (name, dependsOn=[]) => ({name,title:name,instruction:name,objective:name,dependsOn});
async function setup() {
  const repository = new InMemoryRunTreeRepository();
  await repository.beginRoot({runId:'root',agentId:'root-agent',name:'root',title:'Root',instruction:'Own',objective:'Analyze',
    capabilityGrant:new AgentCapabilityGrant({canSpawnAgents:true}),idempotencyKey:'root'});
  const command = {parentRunId:'root',idempotencyKey:'plan',children:[spec('analysis',['read']),spec('read'),spec('slow')]};
  const receipt = await repository.spawnAgents(command);
  const restored = new InMemoryRunTreeRepository();
  restored.importState(repository.exportState());
  for (const item of receipt.items) assert.deepEqual(await restored.getRun(item.run.runId), item.run);
  return {repository:restored,command,ids:Object.fromEntries(receipt.items.map(i=>[i.agent.name,i.run.runId]))};
}

test('dependencies and Root delivery use the same scheduler without waiting for slow work', {timeout:2000}, async () => {
  const {repository,command,ids} = await setup();
  const analysis=deferred(), slow=deferred(), calls=[], deliveries=[];
  const supervisor=new AgentTreeRunSupervisor({repository,executor:{async execute(run,agent) {
      calls.push(agent.name);
      if(agent.name==='analysis') {assert.equal((await repository.getRun(ids.read)).status,'done');await analysis.promise;}
      if(agent.name==='slow') await slow.promise;
      return {status:'done',result:agent.name,contentRef:agent.name,fingerprint:agent.name};
    }},
    async deliverResults(root,results) {
      const selected=results.map(r=>r.runId); deliveries.push(selected);
      if(selected.includes(ids.read)) {assert.equal((await repository.getRun(ids.slow)).status,'running');analysis.resolve();}
      if(selected.includes(ids.analysis)) slow.resolve();
    },
  });
  assert.equal(await repository.claimRun(ids.analysis),undefined);
  await assert.rejects(supervisor.executeAndJoin('root',[ids.analysis]),{code:'agent_dependency_join_incomplete'});
  assert.equal((await repository.getRun('root')).status,'running');
  const commands = new RunCommandService(repository, supervisor);
  let result;
  const received = [];
  try {
    for (;;) {
      result = await commands.receiveRuns('root', Object.values(ids), undefined, {}, received);
      const selected = result.results.map(r => r.runId);
      received.push(...selected);

      if (!result.pendingRunIds.length) break;
    }
    await commands.waitResultFeedback('root');
  } finally { analysis.resolve(); slow.resolve(); await commands.closeReceivers(); }
  assert.equal(result.state,'ready');
  assert.ok(calls.indexOf('read')<calls.indexOf('analysis'));
  assert.deepEqual(deliveries,[[ids.read],[ids.analysis],[ids.slow]]);
  assert.equal((await repository.spawnAgents(command)).replayed,true);
  await assert.rejects(repository.spawnAgents({...command,children:[spec('analysis'),spec('read'),spec('slow')]}),{code:'child_spawn_idempotency_conflict'});
});

test('failed dependency blocks execution of its consumer', async () => {
  const {repository,ids}=await setup(), calls=[];
  const supervisor=new AgentTreeRunSupervisor({repository,executor:{async execute(run,agent) {
      calls.push(agent.name);
      if(agent.name==='read') throw Error('read failed');
      return {status:'done',result:agent.name,contentRef:agent.name,fingerprint:agent.name};
    }},async deliverResults(){},
  });
  assert.equal((await supervisor.executeAndJoin('root',Object.values(ids))).state,'blocked');
  assert.equal(calls.includes('analysis'),false);
  assert.equal((await repository.getRun(ids.analysis)).errorCode,'agent_dependency_failed');
});

for(const children of [[spec('a',['missing'])],[spec('a',['a'])],[spec('a',['b']),spec('b',['a'])]]) {
  test(`invalid dependency graph rejected atomically: ${JSON.stringify(children)}`,async()=>{
    const {repository}=await setup(); const before=repository.exportState();
    await assert.rejects(repository.spawnAgents({parentRunId:'root',idempotencyKey:'invalid',children}),TypeError);
    assert.equal(repository.exportState(),before);
  });
}

test('suspended dependency leaves the consumer pending without stalling', async()=>{
  const {UserInputRequired}=await import('../dist/interaction.js');
  const {repository,ids}=await setup(),calls=[];
  const supervisor=new AgentTreeRunSupervisor({repository,executor:{async execute(run,agent) {
      calls.push(agent.name);
      if(agent.name==='read') throw new UserInputRequired(run.runId,'input');
      return {status:'done',result:agent.name,contentRef:agent.name,fingerprint:agent.name};
    }},async deliverResults(){},
  });
  const result=await supervisor.executeAndJoin('root',Object.values(ids));
  assert.equal(result.state,'pending');
  assert.deepEqual(new Set(result.pendingRunIds),new Set([ids.read,ids.analysis]));
  assert.equal(calls.includes('analysis'),false);
});

test('cancellation stops active and dependency-blocked Runs', {timeout:2000},async()=>{
  const {repository,ids}=await setup(),started=deferred(),controller=new AbortController(),calls=[];
  const supervisor=new AgentTreeRunSupervisor({repository,executor:{async execute(run,agent) {calls.push(agent.name);started.resolve();await new Promise(()=>{});}},
    async deliverResults(){assert.fail('No completed result');},
  });
  const execution=supervisor.executeAndJoin('root',Object.values(ids),controller.signal);
  await started.promise; controller.abort();
  await assert.rejects(execution,{code:'child_run_join_canceled'});
  assert.equal(calls.includes('analysis'),false);
  for(const id of Object.values(ids)) assert.equal((await repository.getRun(id)).status,'canceled');
});
