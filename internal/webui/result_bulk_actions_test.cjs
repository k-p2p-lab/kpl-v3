const test = require('node:test');
const assert = require('node:assert/strict');
const {prepare, createAdmissions} = require('./static/result-bulk-actions.js');

const run = (id, state='completed', extra={}) => ({id,name:id,state,iteration:1,repetitions:1,...extra});
const started = '2026-11-07T00:00:00Z';
const queued = id => ({id:'analysis-'+id,runId:id,state:'queued'});

test('analysis selection expands complete batches, de-duplicates children, and reserves running runs',async()=>{
  const results=[run('done','completed',{batchId:'b'}),run('old','failed',{batchId:'b',superseded:true}),
    run('live','running',{batchId:'b',active:true}),run('unreadable','unreadable',{batchId:'b'})];
  const plan=prepare('analysis',['batch:b','run:done','batch:b'],results);
  assert.deepEqual(plan.entries.map(e=>e.id),['done','old','live']);
  assert.equal(plan.skipped.length,1);
  const calls=[];const report=await createAdmissions({api:async(path,options)=>{
    calls.push([path,options.method]);return queued(decodeURIComponent(path.split('/').at(-1)));
  }}).execute(plan);
  assert.equal(report.accepted.length,3);
  assert.deepEqual(calls,[['/api/v1/analysis-jobs/done','POST'],['/api/v1/analysis-jobs/old','POST'],['/api/v1/analysis-jobs/live','POST']]);
  assert.ok(calls.every(([path])=>!path.includes('refresh=')));
});

test('retry selection targets current failed batches and includes unfinished iterations while preserving previous runs',()=>{
  const results=[run('old','failed',{batchId:'b',superseded:true}),
    run('done','completed',{batchId:'b',repetitions:3,iteration:1}),
    run('failed','failed',{batchId:'b',repetitions:3,iteration:2,previousRunIds:['old']}),
    run('canceled','canceled',{batchId:'b',repetitions:3,iteration:3})];
  const before=JSON.stringify(results);
  const plan=prepare('retry',['batch:b','run:failed','run:old'],results);
  assert.equal(plan.entries.length,1);
  assert.deepEqual(plan.entries[0].runIds,['failed','canceled']);
  assert.equal(plan.entries[0].runCount,2);
  assert.equal(JSON.stringify(results),before);
  assert.equal(prepare('retry',['run:old'],results).entries.length,0);
  assert.equal(prepare('retry',['run:done'],results).entries.length,0);
});

test('retry includes canceled-only and interrupted batches even when they never started',()=>{
  const results=[run('canceled','canceled',{batchId:'c'}),run('interrupted','interrupted',{batchId:'i',startedAt:started}),
    run('unstarted','interrupted',{batchId:'u',startedAt:'0001-01-01T00:00:00Z'}),run('running','running',{batchId:'r'}),run('solo','failed')];
  const plan=prepare('retry',['batch:c','batch:i','batch:u','batch:r','run:solo'],results);
  assert.deepEqual(plan.entries.map(e=>e.id),['c','i','u']);
  assert.deepEqual(plan.entries.map(e=>e.runIds),[['canceled'],['interrupted'],['unstarted']]);
  assert.equal(plan.skipped.length,2);
});

test('a selected canceled or interrupted Run retries its current unfinished batch and retains completed and prior attempts',()=>{
  const results=[
    run('previous','canceled',{batchId:'b',superseded:true}),
    run('done','completed',{batchId:'b',iteration:1,repetitions:3}),
    run('cancel','canceled',{batchId:'b',iteration:2,repetitions:3,previousRunIds:['previous'],startedAt:'0001-01-01T00:00:00Z'}),
    run('interrupt','interrupted',{batchId:'b',iteration:3,repetitions:3}),
  ];
  const original=JSON.stringify(results);
  for(const key of ['run:cancel','run:interrupt']) {
    const plan=prepare('retry',[key],results);
    assert.equal(plan.entries.length,1);
    assert.deepEqual(plan.entries[0].runIds,['cancel','interrupt']);
    assert.deepEqual(plan.entries[0].knownRunIds,['previous','done','cancel','interrupt']);
  }
  for(const key of ['run:done','run:previous']) assert.equal(prepare('retry',[key],results).entries.length,0);
  assert.equal(JSON.stringify(results),original);
  results[2].active=true;
  assert.equal(prepare('retry',['batch:b'],results).entries.length,0,'cleanup or execution ownership still blocks a retry');
});

test('canceled-only retries keep the guarded attempt snapshot and do not replay confirmed admissions',async()=>{
  const results=[run('cancel','canceled',{batchId:'b',startedAt:'0001-01-01T00:00:00Z'})];
  const plan=prepare('retry',['batch:b'],results);let calls=0;
  results[0].state='completed';
  const admissions=createAdmissions({api:async(path,options)=>{
    calls++;
    assert.equal(path,'/api/v1/result-batches/b/retry');
    assert.deepEqual(JSON.parse(options.body),{expectedRunIds:['cancel']});
    return {id:'fresh-canceled-retry',batchId:'b',state:'queued'};
  }});
  const report=await admissions.execute(plan);
  assert.equal(report.accepted.length,1);assert.equal(report.accepted[0].reused,false);
  await admissions.execute(plan);assert.equal(calls,1);
});

test('invalid or stale selections reject before sending anything',()=>{
  assert.throws(()=>prepare('analysis',['run:missing'],[run('known')]),/no longer listed/);
  assert.throws(()=>prepare('retry',[],[]),/Select/);
  assert.throws(()=>prepare('other',['run:a'],[run('a')]),/Unknown/);
});

test('admission is sequential, recognizes reuse, and confirmed jobs survive rendering failures',async()=>{
  const plan=prepare('analysis',['run:a','run:b'],[run('a','completed',{analysis:{id:'already',state:'completed'}}),run('b')]);
  let inflight=0,max=0;const calls=[];
  const admissions=createAdmissions({api:async path=>{
    inflight++;max=Math.max(max,inflight);calls.push(path);await new Promise(setImmediate);inflight--;
    return path.endsWith('/a')?{id:'already',runId:'a',state:'completed'}:queued('b');
  }});
  const report=await admissions.execute(plan,{onAccepted(){throw Error('render failure')}});
  assert.equal(max,1);assert.equal(report.accepted.length,2);assert.equal(report.accepted[0].reused,true);
  await admissions.execute(plan);assert.equal(calls.length,2,'confirmed work must not be submitted again');
});

test('capacity failure stops remaining admissions and manual retry skips all confirmed work',async()=>{
  const plan=prepare('analysis',['run:a','run:b','run:c'],[run('a'),run('b'),run('c')]);
  let full=true;const calls=[];
  const admissions=createAdmissions({api:async path=>{
    const id=path.split('/').at(-1);calls.push(id);
    if(id==='b'&&full)throw Object.assign(Error('analysis queue is full'),{status:503});
    return queued(id);
  }});
  const first=await admissions.execute(plan);
  assert.deepEqual(calls,['a','b']);assert.equal(first.accepted.length,1);assert.equal(first.failed.length,1);
  assert.deepEqual(first.remaining.map(e=>e.id),['c']);
  full=false;const second=await admissions.execute(plan);
  assert.deepEqual(calls,['a','b','b','c']);assert.equal(second.accepted.length,3);assert.equal(second.failed.length,0);
});

test('unknown transport outcomes stop admission and cannot replay the same plan',async()=>{
  const plan=prepare('retry',['batch:a','batch:b'],[run('a','failed',{batchId:'a'}),run('b','failed',{batchId:'b'})]);
  let calls=0;const admissions=createAdmissions({api:async()=>{calls++;throw Error('connection lost')}});
  const report=await admissions.execute(plan);
  assert.equal(calls,1);assert.equal(report.unknown.length,1);assert.equal(report.remaining.length,1);
  await assert.rejects(admissions.execute(plan),/Outcome unknown/);assert.equal(calls,1);
});

test('a malformed success response or explicit accepted error is unknown rather than safely retryable',async()=>{
  for(const response of [null,{id:'wrong',runId:'different',state:'queued'},{id:'job',runId:'a'},
    {id:'job',runId:'a',state:'failed'},{id:'job',runId:'a',state:'canceled'}]){
    const plan=prepare('analysis',['run:a'],[run('a')]);
    const report=await createAdmissions({api:async()=>response}).execute(plan);
    assert.equal(report.unknown.length,1);assert.equal(report.failed.length,0);
  }
  const plan=prepare('analysis',['run:a'],[run('a')]);
  const report=await createAdmissions({api:async()=>{throw Object.assign(Error('durability uncertain'),{status:500,accepted:true})}}).execute(plan);
  assert.equal(report.unknown.length,1);
});

test('retry success must identify a fresh queued attempt and transient HTTP errors remain unknown',async()=>{
  for(const response of [{id:'old',batchId:'b',state:'queued'},{id:'new',batchId:'b',state:'failed'},
    {id:'new',batchId:'other',state:'queued'},{id:'new',batchId:'b'}]){
    const plan=prepare('retry',['batch:b'],[run('old','failed',{batchId:'b'})]);
    const report=await createAdmissions({api:async()=>response}).execute(plan);
    assert.equal(report.unknown.length,1);assert.equal(report.accepted.length,0);
  }
  for(const status of [408,500,502,503,504]){
    const plan=prepare('retry',['batch:b'],[run('old','failed',{batchId:'b'})]);
    const report=await createAdmissions({api:async()=>{throw Object.assign(Error('temporary server error'),{status})}}).execute(plan);
    assert.equal(report.unknown.length,1);
  }
  const plan=prepare('retry',['batch:b'],[run('old','failed',{batchId:'b'})]);
  const report=await createAdmissions({api:async()=>({id:'fresh',batchId:'b',state:'queued'})}).execute(plan);
  assert.equal(report.accepted.length,1);assert.equal(report.accepted[0].reused,false);
});

test('retry sends an immutable unfinished-attempt precondition and stale attempts stop as unknown',async()=>{
  const original=[run('done','completed',{batchId:'b',iteration:1,repetitions:2}),run('failed','failed',{batchId:'b',iteration:2,repetitions:2})];
  const plan=prepare('retry',['batch:b'],original);original[1].id='later';
  let calls=0;
  const admissions=createAdmissions({api:async(path,options)=>{
    calls++;assert.equal(options.headers['Content-Type'],'application/json');
    assert.deepEqual(JSON.parse(options.body),{expectedRunIds:['failed']});
    throw Object.assign(Error('batch attempts changed'),{status:409,code:'batch-attempt-changed'});
  }});
  const report=await admissions.execute(plan);assert.equal(report.unknown.length,1);assert.equal(report.skipped.length,0);
  await assert.rejects(admissions.execute(plan),/Outcome unknown/);assert.equal(calls,1);
  assert.equal(Object.isFrozen(plan.entries[0].runIds),true);
});

test('bulk analysis binds the observed job ID and state, stopping changed attempts without replay',async()=>{
  const results=[run('a','failed',{analysis:{id:'old',state:'running'}}),run('b')];
  const plan=prepare('analysis',['run:a','run:b'],results);
  results[0].analysis={id:'later',state:'failed'};
  let calls=0;const admissions=createAdmissions({api:async(path,options)=>{
    calls++;assert.equal(options.headers['Content-Type'],'application/json');
    assert.deepEqual(JSON.parse(options.body),{expectedAnalysis:{id:'old',state:'running'}});
    throw Object.assign(Error('analysis attempt changed'),{status:409,code:'analysis-attempt-changed'});
  }});
  const report=await admissions.execute(plan);
  assert.equal(report.unknown.length,1);assert.equal(report.skipped.length,0);assert.equal(report.remaining.length,1);
  await assert.rejects(admissions.execute(plan),/Outcome unknown/);assert.equal(calls,1);
});

test('busy or deleted items are skipped while independent later items can be queued',async()=>{
  const plan=prepare('analysis',['run:a','run:b','run:c'],[run('a'),run('b'),run('c')]);
  const report=await createAdmissions({api:async path=>{
    const id=path.split('/').at(-1);
    if(id!=='c')throw Object.assign(Error(id==='a'?'busy':'deleted'),{status:id==='a'?409:404});
    return queued(id);
  }}).execute(plan);
  assert.equal(report.skipped.length,2);assert.equal(report.accepted.length,1);assert.equal(report.remaining.length,0);
});

test('stopping between requests leaves undispatched items and stopping in flight records unknown',async()=>{
  const plan=prepare('analysis',['run:a','run:b'],[run('a'),run('b')]);
  const cancel=new AbortController();let calls=0;
  const report=await createAdmissions({api:async()=>{calls++;return queued('a')}}).execute(plan,{
    signal:cancel.signal,onAccepted(){cancel.abort()}
  });
  assert.equal(calls,1);assert.equal(report.unknown.length,0);assert.deepEqual(report.remaining.map(e=>e.id),['b']);
  const interrupted=new AbortController();
  const inflight=await createAdmissions({api:async(path,{signal})=>new Promise((resolve,reject)=>{
    signal.addEventListener('abort',()=>reject(new DOMException('stopped','AbortError')),{once:true});
    interrupted.abort();
  })}).execute(prepare('analysis',['run:a'],[run('a')]),{signal:interrupted.signal});
  assert.equal(inflight.unknown.length,1);
});
