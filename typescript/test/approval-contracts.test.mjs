import assert from 'node:assert/strict';
import test from 'node:test';
import {readFileSync} from 'node:fs';
import {ApprovalIntent,copyApprovalRecord,copyApprovalDecisionCommand} from 'purra';
const fixture=JSON.parse(readFileSync(new URL('../../conformance/fixtures/approval_records.json',import.meta.url)));

test('shared intent digest binds every field and snapshots before awaiting',async()=>{
  const input=structuredClone(fixture.intent),pending=ApprovalIntent.create(input);
  input.arguments.value.a=99;
  const intent=await pending;
  assert.equal(intent.digest,fixture.intentDigest);
  assert.equal(intent.value.arguments.value.a,1);
  assert.throws(()=>{intent.value.arguments.value.a=3;},TypeError);
  assert.equal((await ApprovalIntent.create({...fixture.intent,arguments:{value:{a:1,b:2},target:'fixture/你好'}})).digest,intent.digest);
  for(const key of ['runId','rootRunId','toolCallId','toolName','presetFingerprint','bindingId','bindingRevision','scopeId','scopeRevision']) {
    assert.notEqual((await ApprovalIntent.create({...fixture.intent,[key]:fixture.intent[key]+'-changed'})).digest,intent.digest);
  }
  assert.notEqual((await ApprovalIntent.create({...fixture.intent,effect:'destructive'})).digest,intent.digest);
  assert.notEqual((await ApprovalIntent.create({...fixture.intent,arguments:{value:2}})).digest,intent.digest);
});
for(const [field,value] of [['schemaVersion',true],['schemaVersion',2],['effect','read'],['scopeRevision',''],['runId',' run'],['arguments',[]],['extra',true]]) {
  test(`invalid intent ${field}/${JSON.stringify(value)}`,async()=>{
    await assert.rejects(ApprovalIntent.create({...fixture.intent,[field]:value}));
  });
}
test('decision audit is required and bound to the record',async()=>{
  const pending={approvalId:'approval',intent:fixture.intent,intentDigest:fixture.intentDigest,revision:1,status:'pending',createdAtMs:1000,expiresAtMs:2000,decisionAudit:{}};
  await assert.rejects(copyApprovalRecord({...pending,status:'approved'}));
  const command={approvalId:'approval',expectedRevision:1,intentDigest:fixture.intentDigest,commandKey:'command',decision:'approve'};
  const approved={...pending,status:'approved',revision:2,decisionAudit:{command,principalId:'host-user',decidedAtMs:1200,revision:2}};
  assert.deepEqual(await copyApprovalRecord(approved),approved);
  for(const [key,value] of [['approvalId','other'],['intentDigest','forged'],['revision',1],['status','rejected']]) {
    await assert.rejects(copyApprovalRecord({...approved,[key]:value}));
  }
  assert.deepEqual((await copyApprovalRecord({...approved,status:'expired',revision:3})).decisionAudit,approved.decisionAudit);
});
for(const expectedRevision of [true,0,-1,1.5,9007199254740992]) test(`invalid decision revision ${expectedRevision}`,()=>{
  assert.throws(()=>copyApprovalDecisionCommand({approvalId:'id',expectedRevision,intentDigest:'digest',commandKey:'key',decision:'approve'}));
});
