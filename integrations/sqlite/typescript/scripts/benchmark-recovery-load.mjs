// Build Core/SQLite first. Isolated large journal and independent-process writer probe.
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { spawn } from 'node:child_process';
import { once } from 'node:events';
import { createInterface } from 'node:readline';
import { fileURLToPath } from 'node:url';
import { InMemoryRunRepository, assertRunRepositoryConforms } from 'purra';
import { SqliteAgentAdapters } from '../dist/index.js';
const now = () => Number(process.hrtime.bigint() / 1000n); // common monotonic microseconds
const event = key => ({ sourceKey:key, kind:'model.diagnostics', channel:'model', visibility:'private', payload:{text:'x'.repeat(128)} });
const median = values => { const v=[...values].sort((a,b)=>a-b); return Number(((v[Math.floor((v.length-1)/2)]+v[Math.floor(v.length/2)])/2).toFixed(3)); };
if (process.argv[2] === '--writer') {
  const storage = new SqliteAgentAdapters(process.argv[3], {scope:'load'});
  try {
    const lines = createInterface({input:process.stdin});
    const start = once(lines, 'line'); console.log('ready');
    assert.equal((await start)[0], 'start'); lines.close();
    const intervals=[];
    for(let i=0;i<20;i++){const a=now();await storage.runs.appendEvent('run-00',event(`writer:${i}`));intervals.push([a,now()]);}
    console.log(JSON.stringify(intervals));
  } finally { storage.close(); }
} else {
  const historyCount=Number(process.env.PURRA_BENCHMARK_EVENTS??5000);
  assert.ok(Number.isSafeInteger(historyCount)&&historyCount>=1&&historyCount<=100000);
  const directory=mkdtempSync(join(tmpdir(),'purra-recovery-load-')), path=join(directory,'db');
  const storage=new SqliteAgentAdapters(path,{scope:'load'});
  let child, watchdog;
  try {
    const reference=new InMemoryRunRepository();await assertRunRepositoryConforms(reference);
    const {preset,budgets}=await reference.get('conformance-run-1');
    await storage.transaction(async({runs})=>{
      for(let i=0;i<20;i++)await runs.begin({preset,budgets,requestedRunId:`run-${String(i).padStart(2,'0')}`,deadlineAt:null,metadata:{}});
      for(let i=0;i<historyCount;i++)await runs.appendEvent('run-19',event(`history:${i}`));
    });
    const selected=['run-00','run-01','run-02','run-03','run-04'];
    const individual=async()=>{const result=Object.create(null);for(const id of selected)result[id]=await storage.inspectRecovery(id);return result;};
    const batch=()=>storage.inspectRecoveryMany(selected),expected=await individual();
    for(const [operation,run] of [['individual',individual],['batch',batch]]){
      const samples=[];
      for(let i=0;i<7;i++){const start=now();assert.deepEqual(await run(),expected);samples.push((now()-start)/1000);}
      console.log(JSON.stringify({sdk:'typescript',scenario:'unrelated_journal',runs:20,selected:5,unrelated_events:historyCount,operation,median_ms:median(samples.slice(2)),samples:5}));
    }
    child=spawn(process.execPath,[fileURLToPath(import.meta.url),'--writer',path],{stdio:['pipe','pipe','pipe']});
    const exited=once(child,'exit');
    watchdog=setTimeout(()=>child.kill(),30000);
    let stderr='';child.stderr.on('data',chunk=>stderr+=chunk);
    const lines=createInterface({input:child.stdout})[Symbol.asyncIterator]();
    assert.equal((await lines.next()).value,'ready');
    const writesResult=lines.next();child.stdin.end('start\n');
    const reads=[];
    for(let i=0;i<20;i++){const a=now();assert.deepEqual(await batch(),expected);reads.push([a,now()]);}
    const writes=JSON.parse((await writesResult).value);
    const [code]=await exited;assert.equal(code,0,stderr);
    const overlaps=writes.filter(([a,b])=>reads.some(([c,d])=>a<d&&c<b)).length;
    const committed=(await storage.runs.listEvents('run-00',0)).filter(e=>e.sourceKey.startsWith('writer:'));
    assert.equal(committed.length,20);assert.equal(new Set(committed.map(e=>e.sourceKey)).size,20);assert.ok(overlaps>0,'probe did not overlap');
    console.log(JSON.stringify({sdk:'typescript',scenario:'mixed_processes',reads:20,writes:20,observed_overlapping_writes:overlaps,reader_median_ms:median(reads.map(([a,b])=>(b-a)/1000)),writer_median_ms:median(writes.map(([a,b])=>(b-a)/1000)),committed_events:20,diagnosis_unchanged:true}));
  } finally {
    clearTimeout(watchdog);
    if(child&&child.exitCode===null&&child.signalCode===null){child.kill();await once(child,'exit');}
    storage.close();rmSync(directory,{recursive:true});
  }
}
