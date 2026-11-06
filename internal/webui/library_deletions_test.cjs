const test=require('node:test');
const assert=require('node:assert/strict');
const {prepareDeletion,createDeletions}=require('./static/library-deletions.js');

const items=[{key:'scenario:a',name:'<Alpha>'},{key:'scenario:b',name:'Beta'},{key:'batch:group',name:'Group'},{key:'run:solo',name:'Solo'}];
const results=[{id:'current',batchId:'group'},{id:'previous',batchId:'group'},{id:'solo'}];
const plan=(scope,keys)=>prepareDeletion(scope,keys,items,results);

test('deletion plans freeze selected units, preserve batch scope and reject unknown or mixed keys',()=>{
  const p=plan('results',['batch:group','run:solo','batch:group']);
  assert.deepEqual(p.entries.map(entry=>entry.url),['/api/v1/result-batches/group','/api/v1/results/solo']);
  assert.deepEqual(p.entries[0].runIDs,['current','previous']);
  assert.ok(Object.isFrozen(p.entries));
  for(const keys of [[],['unknown:key'],['scenario:a'],Array.from({length:2001},(_,i)=>'run:'+i)])assert.throws(()=>plan('results',keys));
  assert.throws(()=>plan('scenarios',['run:solo']));
  assert.equal(plan('scenarios',['scenario:a']).entries[0].url,'/api/v1/scenarios/a');
});

test('confirmed deletion uses existing APIs sequentially and reports server-confirmed batch IDs',async()=>{
  const calls=[],removed=[],progress=[];
  const executor=createDeletions({api:async(path,options)=>{
    calls.push([path,options.method]);
    return path.includes('result-batches')?{deletedIds:['current','previous','new-member']}:null;
  }});
  const r=await executor.execute(plan('results',['batch:group','run:solo']),{onDeleted:(entry,ids)=>removed.push([entry.key,ids]),onProgress:p=>progress.push(p.index)});
  assert.deepEqual(calls,[['/api/v1/result-batches/group','DELETE'],['/api/v1/results/solo','DELETE']]);
  assert.deepEqual(removed,[['batch:group',['current','previous','new-member']],['run:solo',['solo']]]);
  assert.deepEqual(r.deleted,['batch:group','run:solo']);
  assert.equal(r.failed.length,0);assert.equal(r.remaining.length,0);
  assert.deepEqual(progress,[0,1]);
});

test('busy items remain retryable while unrelated selected items are deleted',async()=>{
  let busy=true;const calls=[];
  const executor=createDeletions({api:async path=>{
    calls.push(path);
    if(path.includes('result-batches')&&busy)throw Object.assign(Error('active'),{status:409});
    return {deletedIds:['current','previous']};
  }});
  const p=plan('results',['batch:group','run:solo']);
  const r=await executor.execute(p);
  assert.deepEqual(r.deleted,['run:solo']);assert.equal(r.failed[0].entry.key,'batch:group');
  busy=false;
  await executor.execute({scope:'results',entries:r.failed.map(f=>f.entry)});
  assert.equal(calls.filter(path=>path.endsWith('/solo')).length,1);
});

test('an unknown response stops later requests and 404 confirms a safe retry',async()=>{
  let uncertain=true;const calls=[];
  const executor=createDeletions({api:async path=>{
    calls.push(path);
    if(path.endsWith('/a')){
      if(uncertain){uncertain=false;throw Error('Response lost after deletion');}
      throw Object.assign(Error('gone'),{status:404});
    }
    return null;
  }});
  const p=plan('scenarios',['scenario:a','scenario:b']);
  const first=await executor.execute(p);
  assert.equal(first.deleted.length,0);assert.equal(first.failed.length,1);
  assert.equal(first.remaining[0].key,'scenario:b');assert.equal(calls.length,1);
  const second=await executor.execute(p);
  assert.deepEqual(second.deleted,['scenario:a','scenario:b']);
});

test('timeouts and partial batch failures keep unresolved targets and release the executor',async()=>{
  let fail=true;
  const executor=createDeletions({timeoutMs:5,api:async(path,options)=>{
    if(fail)return new Promise((resolve,reject)=>options.signal.addEventListener('abort',()=>reject(Object.assign(Error('timeout'),{name:'AbortError'})),{once:true}));
    return {deletedIds:[]};
  }});
  const p=plan('results',['batch:group','run:solo']);
  const r=await executor.execute(p);
  assert.match(r.failed[0].message,/may still finish/);assert.equal(r.remaining.length,1);
  fail=false;assert.equal((await executor.execute(p)).deleted.length,2);
});

test('malformed batch responses never imply success and duplicate executions are blocked',async()=>{
  let release;const held=new Promise(resolve=>release=resolve);
  const executor=createDeletions({api:async()=>{await held;return {};}});
  const p=plan('results',['batch:group','run:solo']);
  const first=executor.execute(p);
  await assert.rejects(executor.execute(p),/already in progress/);
  release();const report=await first;
  assert.equal(report.deleted.length,0);assert.equal(report.remaining.length,1);
  assert.match(report.failed[0].message,/Unexpected batch deletion response/);
});

test('rendering errors cannot turn confirmed deletions into failures',async()=>{
  const executor=createDeletions({api:async()=>null});
  const r=await executor.execute(plan('scenarios',['scenario:a','scenario:b']),{onDeleted(){throw Error('render');}});
  assert.equal(r.deleted.length,2);assert.equal(r.failed.length,0);
});
