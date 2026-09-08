import assert from 'node:assert/strict';
import test from 'node:test';
import { mkdtempSync,rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import { RecoveryWorker, InMemoryRunRepository, assertRunRepositoryConforms } from 'purra';
import { SqliteAgentAdapters } from '../dist/index.js';
async function fixture(t,empty=false){
 const dir=mkdtempSync(join(tmpdir(),'purra-cursor-')),path=join(dir,'db'),stores=[];
 const open=()=>{const s=new SqliteAgentAdapters(path,{scope:'cursor'});stores.push(s);return s;};
 t.after(()=>{stores.forEach(s=>s.close());rmSync(dir,{recursive:true});});
 const store=open();
 const seed=async()=>{const reference=new InMemoryRunRepository();await assertRunRepositoryConforms(reference);const {preset,budgets}=await reference.get('conformance-run-1');for(const requestedRunId of ['a','b','c'])await store.runs.begin({preset,budgets,requestedRunId,deadlineAt:null,metadata:{}});};
 if(!empty)await seed();return {store,open,path,seed};
}
test('cursor prefix, stale acknowledgement, reopen and wrap',async t=>{
 const {store,open}=await fixture(t),first=store.recoveryCursor('worker',{pageSize:2}),other=store.recoveryCursor('worker',{pageSize:2});
 assert.deepEqual(await first.discover(),['a','b']);assert.deepEqual(await other.discover(),['a','b']);
 await assert.rejects(first.acknowledge(['b']));await first.acknowledge(['a']);
 await assert.rejects(other.acknowledge(['a','b']),/recovery_cursor_conflict/);
 const restored=open().recoveryCursor('worker',{pageSize:2});assert.deepEqual(await restored.discover(),['b','c']);
 const replay=open().recoveryCursor('worker',{pageSize:2});assert.deepEqual(await replay.discover(),['b','c']);await replay.acknowledge(['b','c']);
 assert.deepEqual(await replay.discover(),['a','b']);assert.deepEqual(await store.recoveryCursor('other',{pageSize:1}).discover(),['a']);
});
test('worker preserves stopped and unsettled candidates, including a batch limit',async t=>{
 const {store}=await fixture(t),cursor=store.recoveryCursor('worker'),stop=new AbortController(),seen=[];
 const resume=async id=>{seen.push(id);};
 const worker=new RecoveryWorker({discover:()=>cursor.discover(),acknowledge:ids=>cursor.acknowledge(ids),
 inspect:async id=>{if(id==='b')stop.abort();return {blockers:[]};},resume});
 await worker.run({signal:stop.signal});assert.deepEqual(seen,['a']);
 assert.deepEqual(await store.recoveryCursor('worker').discover(),['b','c']);
 const next=store.recoveryCursor('worker');
 const failing=new RecoveryWorker({discover:()=>next.discover(),acknowledge:ids=>next.acknowledge(ids),inspect:async()=>({blockers:[]}),resume,
 schedule:{ready:async()=>0,settle:async()=>{throw new Error('persist failed');}}});
 await assert.rejects(failing.runOnce(),/persist failed/);assert.deepEqual(await store.recoveryCursor('worker').discover(),['b','c']);
 const bounded=store.recoveryCursor('worker');
 await new RecoveryWorker({discover:()=>bounded.discover(),acknowledge:ids=>bounded.acknowledge(ids),maxRunsPerScan:1,inspect:async()=>({blockers:[]}),resume}).runOnce();
 assert.deepEqual(await store.recoveryCursor('worker').discover(),['c']);
});
test('empty cursor and failed acknowledgement do not skip work',async t=>{
 const {store,path,seed}=await fixture(t,true),cursor=store.recoveryCursor('worker');
 assert.deepEqual(await cursor.discover(),[]);await cursor.acknowledge([]);await seed();assert.deepEqual(await cursor.discover(),['a','b','c']);
 const db=new DatabaseSync(path);t.after(()=>db.close());db.exec("CREATE TRIGGER fail_cursor BEFORE UPDATE ON purra_state BEGIN SELECT RAISE(ABORT, 'fixture'); END");
 await assert.rejects(cursor.acknowledge(['a','b','c']));db.exec('DROP TRIGGER fail_cursor');assert.deepEqual(await store.recoveryCursor('worker').discover(),['a','b','c']);
});
