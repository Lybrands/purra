import assert from 'node:assert/strict';
import test from 'node:test';
import { mkdtempSync, rmSync, readFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import { ApprovalIntent, jsonIdentityDigest, InMemoryRunRepository, assertRunRepositoryConforms } from 'purra';
import { SqliteAgentAdapters } from '../dist/index.js';
const fixture=JSON.parse(readFileSync(new URL('../../../../conformance/fixtures/approval_records.json',import.meta.url)));
const source=new InMemoryRunRepository();
await assertRunRepositoryConforms(source);
const {preset,budgets}=await source.get('conformance-run-1');
async function begin(storage){await storage.runs.begin({requestedRunId:'run-1',preset,budgets,deadlineAt:null,metadata:{}});}
async function intent(){return ApprovalIntent.create({...fixture.intent,presetFingerprint:await jsonIdentityDigest(preset)});}
function command(record,decision='approve',commandKey='decision'){return {approvalId:record.approvalId,intentDigest:record.intentDigest,expectedRevision:record.revision,decision,commandKey};}
function setup(t){const dir=mkdtempSync(join(tmpdir(),'purra-approval-'));const path=join(dir,'db');let stores=[];t.after(()=>{for(const s of stores)s.close();rmSync(dir,{recursive:true});});return {path,open(scope='fixture'){const s=new SqliteAgentAdapters(path,{scope});stores.push(s);return s;}};}

test('explicit activation preserves history and rejects preopened legacy writes',async t=>{
 const f=setup(t),s=f.open();await assertRunRepositoryConforms(s.runs);
 const db=new DatabaseSync(f.path);t.after(()=>db.close());
 const before=db.prepare("SELECT body FROM purra_state WHERE sdk='typescript'").get().body;
 const events=await s.runs.listEvents("conformance-run-1",0);
 await s.enableApprovals();await s.enableApprovals();
 assert.deepEqual(await s.runs.listEvents("conformance-run-1",0),events);
 await s.publisher.publishCommitted(events[0]);
 assert.equal(db.prepare("SELECT body FROM purra_state WHERE sdk='typescript'").get().body,before);
 assert.ok(db.prepare('SELECT 1 FROM purra_state WHERE version != 4').get());
 assert.throws(()=>db.exec("INSERT INTO purra_state VALUES('new','typescript',4,'{}')"),/unsupported SQLite storage version/);
 assert.throws(()=>db.exec("UPDATE purra_state SET version=4"),/unsupported SQLite storage version/);
 assert.equal((await f.open().runs.get('conformance-run-1')).status,'completed');
});
for(const blocker of ['active','claim','foreign'])test(`activation rejects ${blocker} in another scope without mutation`,async t=>{
 const f=setup(t),s=f.open(),other=f.open('other'),db=new DatabaseSync(f.path);t.after(()=>db.close());
 if(blocker==='active')await begin(other);
 else if(blocker==='claim')await other.transaction(async(_,extra)=>{extra.tools={unknown:{state:'claimed'}};});
 else db.exec("INSERT INTO purra_state VALUES('foreign','python',4,'{}')");
 const before=db.prepare('SELECT * FROM purra_state').all();
 await assert.rejects(s.enableApprovals(),/approval_activation_/);
 assert.deepEqual(db.prepare('SELECT * FROM purra_state').all(),before);
 assert.equal(db.prepare("SELECT 1 FROM sqlite_master WHERE name='purra_approvals'").get(),undefined);
});
test('reopen, immutable command replay, read-only projection and sticky expiration',async t=>{
 const f=setup(t),s=f.open();let now=1000;const options={authorize:()=>true,clockMs:()=>now};let store=s.approvalStore(options);
 await assert.rejects(store.create(await intent(),{expiresAtMs:2000}),{code:'approval_storage_not_enabled'});
 await s.enableApprovals();await begin(s);
 const record=await store.create(await intent(),{expiresAtMs:2000});
 now=1100;assert.deepEqual(await store.create(await intent(),{expiresAtMs:2000}),record);
 const cmd=command(record),receipt=await store.decide(cmd,{principalId:'host'});
 store=f.open().approvalStore(options);
 assert.deepEqual(await store.decide(cmd,{principalId:'host'}),receipt);
 await assert.rejects(store.decide({...cmd,decision:'reject'},{principalId:'host'}),{code:'approval_command_conflict'});
 await assert.rejects(store.decide(cmd,{principalId:'other'}),{code:'approval_command_conflict'});
 now=2000;assert.equal((await store.get(record.approvalId)).status,'approved');
 assert.equal((await store.listPending()).length,1);
 assert.equal((await store.refresh(record.approvalId)).status,'expired');
 now=1200;assert.equal((await store.refresh(record.approvalId)).status,'expired');
 assert.deepEqual(await store.decide(cmd,{principalId:'host'}),receipt);
});
for(const change of ['expire','cancel'])test(`authorization releases writer lock and rechecks ${change}`,async t=>{
 const f=setup(t),s=f.open(),other=f.open();await s.enableApprovals();await begin(s);let now=1000;
 let enter,release;const entered=new Promise(r=>enter=r),released=new Promise(r=>release=r);
 const store=s.approvalStore({clockMs:()=>now,authorize:async()=>{enter();await released;return true;}});
 const record=await store.create(await intent(),{expiresAtMs:2000});
 const decision=store.decide(command(record),{principalId:'host'});
 const rejected=assert.rejects(decision,{code:change==='expire'?'approval_expired':'approval_canceled'});
 await entered;await other.transaction(async(_,extra)=>{extra.unlocked=true;});
 if(change==='expire')now=2000;else await other.runs.cancel('run-1');
 release();await rejected;
 assert.equal((await store.get(record.approvalId)).status,change==='expire'?'expired':'canceled');
});
test('competing decisions commit one revision and truthy authorization is rejected',async t=>{
 const f=setup(t),s=f.open();await s.enableApprovals();await begin(s);
 const a=s.approvalStore({authorize:()=>true,clockMs:()=>1000}),b=f.open().approvalStore({authorize:()=>true,clockMs:()=>1000});
 const record=await a.create(await intent(),{expiresAtMs:2000});
 const denied=s.approvalStore({authorize:()=> 'yes',clockMs:()=>1000});
 await assert.rejects(denied.decide(command(record),{principalId:'host'}),{code:'approval_authorization_denied'});
 const outcomes=await Promise.allSettled([a.decide(command(record),{principalId:'host'}),b.decide(command(record,'reject','other'),{principalId:'host'})]);
 assert.equal(outcomes.filter(x=>x.status==='fulfilled').length,1);
 assert.equal(outcomes.find(x=>x.status==='rejected').reason.code,'approval_revision_conflict');
 assert.equal((await a.get(record.approvalId)).revision,2);
});
test('failed decision persistence rolls back and scope boundaries isolate records',async t=>{
 const f=setup(t),s=f.open();await s.enableApprovals();await begin(s);
 const store=s.approvalStore({authorize:()=>true,clockMs:()=>1000}),record=await store.create(await intent(),{expiresAtMs:2000});
 await assert.rejects(f.open('other').approvalStore({authorize:()=>true}).get(record.approvalId),{code:'approval_not_found'});
 const db=new DatabaseSync(f.path);t.after(()=>db.close());
 db.exec("CREATE TRIGGER fail_approval_update BEFORE UPDATE ON purra_approvals BEGIN SELECT RAISE(ABORT, 'fixture persistence failure'); END");
 await assert.rejects(store.decide(command(record),{principalId:'host'}),/fixture persistence failure/);
 assert.deepEqual(await store.get(record.approvalId),record);
 db.exec('DROP TRIGGER fail_approval_update');
 assert.equal((await store.decide(command(record),{principalId:'host'})).revision,2);
});
test('creation checks root, configuration and deadline; authorization errors stay private',async t=>{
 const f=setup(t),s=f.open();await s.enableApprovals();
 await s.runs.begin({requestedRunId:'run-1',preset,budgets,deadlineAt:new Date(1500).toISOString(),metadata:{}});
 const store=s.approvalStore({authorize:()=>{throw new Error('private authorization failure');},clockMs:()=>1000});
 const original=await intent();
 for(const [change,code] of [[{rootRunId:'other'},'approval_run_conflict'],[{presetFingerprint:'changed'},'approval_configuration_mismatch']]){
  await assert.rejects(store.create(await ApprovalIntent.create({...original.value,...change}),{expiresAtMs:2000}),{code});
 }
 const record=await store.create(original,{expiresAtMs:2000});assert.equal(record.expiresAtMs,1500);
 await assert.rejects(store.decide(command(record),{principalId:'host'}),e=>e.code==='approval_authorization_failed'&&!e.message.includes('private'));
 assert.deepEqual(await store.get(record.approvalId),record);
});
test('creation replay distinguishes JSON boolean and number arguments',async t=>{
 const f=setup(t),s=f.open();await s.enableApprovals();await begin(s);
 const store=s.approvalStore({authorize:()=>true,clockMs:()=>1000}),base=await intent();
 await store.create(await ApprovalIntent.create({...base.value,arguments:{value:1}}),{expiresAtMs:2000});
 await assert.rejects(store.create(await ApprovalIntent.create({...base.value,arguments:{value:true}}),{expiresAtMs:2000}),{code:'approval_intent_conflict'});
});
