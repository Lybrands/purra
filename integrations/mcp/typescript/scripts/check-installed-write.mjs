// Installed-consumer validation against a separate Python MCP writer process.
import assert from 'node:assert/strict';
import { mkdtempSync, readFileSync, existsSync, rmSync } from 'node:fs';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { Client } from '@modelcontextprotocol/sdk/client/index.js';
import { StdioClientTransport } from '@modelcontextprotocol/sdk/client/stdio.js';
import { ToolListChangedNotificationSchema } from '@modelcontextprotocol/sdk/types.js';
import { Agent, ApprovalIntent, ApprovalRequired, jsonIdentityDigest } from 'purra';
import { SqliteAgentAdapters } from 'purra-sqlite';
import { McpCatalogMonitor, discoverMcpWriteTools } from 'purra-mcp';

const capabilities={schemaVersion:2,profileId:'fixture',providerProtocol:'custom',contextWindowTokens:65536,maxGenerationTokens:128,
  thinkingTokenAccounting:'unknown',protocol:{reasoningControl:'selectable',reasoningReplay:'ignored',toolCalling:'supported',requiredToolChoice:'supported',
    parallelToolCalls:'supported',streaming:'unknown',cancellation:'supported',publicProgress:'supported',assistantContentWithToolCalls:'optional',
    jsonSchemaLevel:'unknown',streamFinishSemantics:'normalized',usageSemantics:'normalized'}};
const request={messages:[{role:'user',content:'Write the synthetic value'}],planningMode:'reactive'};
const [server,python]=process.argv.slice(2);assert.ok(server&&python,'Provide fixture server path and its Python interpreter');
for(const name of ['purra','purra-sqlite','purra-mcp']) assert.ok(import.meta.resolve(name).includes('/node_modules/'),`Install ${name} before running this consumer`);

async function check(mode){
  const dir=mkdtempSync(join(tmpdir(),'purra-stdio-write-')),monitor=new McpCatalogMonitor();
  const transport=new StdioClientTransport({command:python,args:['-I',server,dir,mode],stderr:'pipe'});
  transport.setProtocolVersion=version=>monitor.acceptProtocolVersion(version);
  const client=new Client({name:'installed-fixture-host',version:'1'});
  client.setNotificationHandler(ToolListChangedNotificationSchema,n=>monitor.onNotification(n));client.onclose=()=>monitor.close();
  // Drain only fixture diagnostics; protocol output and persisted evidence are checked separately.
  const ledger=()=>readFileSync(join(dir,'remote.jsonl'),'utf8').trim().split('\n').map(line=>JSON.parse(line));
  let host;
  try {
    await client.connect(transport);transport.stderr?.resume();
    const catalog=await discoverMcpWriteTools(client,'synthetic',{write:{localName:'write',policy:{mode:'confirm',title:'Write fixture',riskLevel:'write'},scope:()=>true,
      bindingId:'synthetic-writer',bindingRevision:'1',scopeId:'temporary-fixture',scopeRevision:'1',effect:'write'}},{monitor,limits:{timeoutMs:2000}});
    const expiry=Date.now()+60000;
    const open=()=>{
      const storage=new SqliteAgentAdapters(join(dir,'run.db'),{scope:'stdio-write'}),approvals=storage.approvalStore({authorize:principal=>principal==='fixture-host'});
      const counts={model:0};
      const agent=new Agent({runRepository:storage.runs,outputPublisher:storage.publisher,preset:{id:'stdio-write',revision:'1'},
        model:{capabilities,async invoke(input){
          counts.model++;
          return {...(input.messages.some(m=>m.role==='tool')||input.tools.length===0?{message:{role:'assistant',content:'done'},finishReason:'stop'}:
            {message:{role:'assistant',content:'',toolCalls:[{id:'write-1',name:'write',arguments:{value:42}}]},finishReason:'tool_calls'}),appliedGenerationLimit:input.outputBudget.maxGenerationTokens};
        }},tools:catalog.registrations,approval:approvals.gateway(),idempotency:storage.idempotency,toolCheckpointNames:['write'],
        toolCheckpointHandler:async checkpoint=>{
          const call=checkpoint.assistant.toolCalls[0],run=await storage.runs.get(checkpoint.runId);
          const intent=await ApprovalIntent.create({schemaVersion:1,runId:checkpoint.runId,rootRunId:checkpoint.runId,toolCallId:call.id,toolName:call.name,
            arguments:call.arguments,presetFingerprint:await jsonIdentityDigest(run.preset),...catalog.registrations[0].approvalBinding});
          await approvals.prepare(checkpoint,intent,{expiresAtMs:expiry});
        }});
      return {storage,approvals,counts,agent};
    };
    host=open();await host.storage.enableApprovals();
    const handle=await host.agent.submit(request,{budgets:{maxRunGenerationTokens:null}});
    await assert.rejects(handle.result,ApprovalRequired);assert.equal(host.counts.model,1);assert.deepEqual(ledger().map(r=>r.event),['started']);
    host.storage.close();host=open();
    await assert.rejects((await host.agent.resume(handle.runId,request)).result,ApprovalRequired);
    assert.equal(host.counts.model,0);assert.deepEqual(ledger().map(r=>r.event),['started']);
    const [record]=await host.approvals.listPending({runId:handle.runId});
    await host.approvals.decide({approvalId:record.approvalId,expectedRevision:record.revision,intentDigest:record.intentDigest,commandKey:'approve',decision:'approve'},{principalId:'fixture-host'});
    const resumed=await host.agent.resume(handle.runId,request);
    if(mode==='success')assert.equal((await resumed.result).output,'done');
    else await assert.rejects(resumed.result,{code:'tool_effect_unknown'});
    await host.storage.transaction(async(_,extra)=>{
      const entries=Object.values(extra.tools);assert.equal(entries.length,1);assert.equal(entries[0].state,mode==='success'?'complete':'claimed');
      if(mode==='success')assert.equal(entries[0].result.effectState,'committed');else assert.equal(entries[0].result,undefined);
    });
    host.storage.close();host=open();const before=ledger();
    await assert.rejects(async()=>{const retry=await host.agent.resume(handle.runId,request);await retry.result;},{code:"run_terminal"});
    assert.equal(host.counts.model,0);assert.deepEqual(ledger(),before);
    await client.close();monitor.close();
    const rows=ledger(),pid=rows[0].pid;
    assert.throws(()=>process.kill(pid,0),error=>error.code==='ESRCH');
    const events=rows.map(r=>r.event),writes=mode==='exit-before'?0:1;
    assert.equal(events.filter(event=>event==='received').length,1);assert.equal(events.filter(event=>event==='written').length,writes);
    assert.equal(existsSync(join(dir,'value.txt')),Boolean(writes));if(writes)assert.equal(readFileSync(join(dir,'value.txt'),'utf8'),'42');
    return {scenario:mode,protocol:monitor.protocolVersion,remoteCalls:1,remoteWrites:writes,effect:mode==='success'?'committed':'unknown',duplicateDispatch:false,resumeBlocker:"run_terminal",serverExited:true,events};
  } finally {await client.close();monitor.close();host?.storage.close();rmSync(dir,{recursive:true,force:true});}
}
const checks=[];for(const mode of ['success','response-loss','exit-before','exit-after','error-after'])checks.push(await check(mode));
console.log(JSON.stringify({sdk:'typescript',model:'scripted fixture; no real Provider',checks},null,2));
