const test = require('node:test');
const assert = require('node:assert/strict');
const {createQueue} = require('./static/scenario-run-queue.js');

const scenarios = [{id:'a',name:'Alpha'},{id:'b',name:'Beta'},{id:'c',name:'Gamma'}];
function memory() {
  const values = new Map();
  return {values,getItem:key=>values.get(key),setItem:(key,value)=>values.set(key,value),removeItem:key=>values.delete(key)};
}
function fixture(options={}) {
  const calls=[], receipts=new Map(), storage=options.storage || memory();let count=0;
  const request=async(path,config,kind)=>{
    calls.push({path,config,kind});
    if (!config) return {id:path.split('/').at(-1),yaml:'version: 4\nname: original\n'};
    const key=config.headers['Idempotency-Key'];
    if (receipts.has(key)) {
      assert.equal(config.body,receipts.get(key).payload,'retries must reuse the exact saved YAML');
      return receipts.get(key).run;
    }
    const run={id:'run-'+receipts.size};
    receipts.set(key,{payload:config.body,run});
    if(options.afterAccept) await options.afterAccept(JSON.parse(config.body));
    return run;
  };
  const config={request:options.request || request,storage,createKey:()=> 'key-'+(++count)};
  return {queue:createQueue(config),config,calls,receipts,storage};
}

test('each selected scenario gets its own repeated submission in selection snapshot order',async()=>{
  const h=fixture(),progress=[];
  const result=await h.queue.submit(scenarios,3,value=>progress.push(value));
  assert.deepEqual(result,{scenarios:3,runs:9});
  assert.deepEqual(h.calls.map(call=>call.path),['/api/v1/scenarios/a','/api/v1/experiments','/api/v1/scenarios/b','/api/v1/experiments','/api/v1/scenarios/c','/api/v1/experiments']);
  const posts=h.calls.filter(call=>call.config);
  assert.deepEqual(posts.map(call=>JSON.parse(call.config.body).scenarioId),['a','b','c']);
  assert.ok(posts.every(call=>JSON.parse(call.config.body).repetitions===3 && call.kind==='run'));
  assert.equal(new Set(posts.map(call=>call.config.headers['Idempotency-Key'])).size,3);
  assert.deepEqual(progress.map(item=>item.accepted),[0,1,2]);
  assert.equal(h.storage.values.size,0);
  await h.queue.submit(scenarios,3);
  assert.equal(h.receipts.size,6,'a fully acknowledged new click starts a new set');
});

test('partial ambiguous failures resume without duplicating confirmed or accepted runs, even after reload',async()=>{
  let fail=true;
  const h=fixture({afterAccept:async payload=>{if(payload.scenarioId==='b'&&fail){fail=false;throw new Error('Response lost');}}});
  await assert.rejects(h.queue.submit(scenarios,2),/Confirmed 1\/3 scenarios \(2 runs\).*Beta: Response lost.*retry/);
  assert.equal(h.receipts.size,2,'the unknown response was already accepted by the server');
  h.calls.length=0;
  const restored=createQueue(h.config);
  assert.deepEqual(await restored.submit([...scenarios].reverse(),2),{scenarios:3,runs:6});
  assert.deepEqual(h.calls.map(call=>call.path),['/api/v1/experiments','/api/v1/scenarios/c','/api/v1/experiments']);
  assert.equal(h.receipts.size,3);
});

test('saved YAML is retained across uncertain responses even if the saved record changes or disappears',async()=>{
  const storage=memory();let reads=0,posts=0;const bodies=[],keys=[];
  const queue=createQueue({storage,createKey:()=> 'stable',request:async(path,options)=>{
    if(!options){reads++;return {id:'a',yaml:'version: 4\nname: first snapshot\n'};}
    bodies.push(options.body);keys.push(options.headers['Idempotency-Key']);
    if(++posts===1)throw new Error('Timed out');
    return {id:'accepted'};
  }});
  await assert.rejects(queue.submit([scenarios[0]],4),/Timed out/);
  await queue.submit([scenarios[0]],4);
  assert.equal(reads,1);
  assert.equal(bodies[0],bodies[1]);
  assert.deepEqual(keys,['stable','stable']);
});

test('a missing saved scenario stops registration and retry keeps earlier submissions',async()=>{
  const calls=[];let missing=true;
  const queue=createQueue({storage:memory(),createKey:()=> String(calls.length),request:async(path,options)=>{
    calls.push(path);
    if(path==='/api/v1/scenarios/b'&&missing)throw new Error('Not found');
    return options?{id:'run'}:{id:path.split('/').at(-1),yaml:'saved'};
  }});
  await assert.rejects(queue.submit(scenarios,1),/Confirmed 1\/3.*Beta: Not found/);
  assert.equal(calls.filter(path=>path==='/api/v1/experiments').length,1);
  missing=false;
  await queue.submit(scenarios,1);
  assert.equal(calls.filter(path=>path==='/api/v1/experiments').length,3);
});

test('selection and repeat count are frozen and duplicate invocations are blocked while registering',async()=>{
  let release;const held=new Promise(resolve=>release=resolve);const posts=[];
  const queue=createQueue({createKey:()=> 'key-'+posts.length,request:async(path,options)=>{
    if(!options){await held;return {id:path.split('/').at(-1),yaml:'saved'};}
    posts.push(JSON.parse(options.body));return {id:'run'};
  }});
  const selected=scenarios.map(item=>({...item}));
  const first=queue.submit(selected,7);
  selected[0].id='changed';selected.pop();
  await assert.rejects(queue.submit(scenarios,7),/already in progress/);
  release();await first;
  assert.deepEqual(posts.map(item=>item.scenarioId),['a','b','c']);
  assert.ok(posts.every(item=>item.repetitions===7));
});

test('malformed responses and invalid input cannot admit later scenarios',async()=>{
  for(const value of [null,[],[scenarios[0],scenarios[0]],Array(2001).fill(scenarios[0])]) {
    await assert.rejects(fixture().queue.submit(value,1));
  }
  for(const repeats of [0,101,1.5,NaN])await assert.rejects(fixture().queue.submit(scenarios,repeats));
  let posts=0;
  const malformed=fixture({request:async(path,options)=>{if(options){posts++;return {}; }return {id:'a',yaml:'saved'};}});
  await assert.rejects(malformed.queue.submit(scenarios,1),/Unexpected experiment response/);
  assert.equal(posts,1);
  for(const reply of [{id:'wrong',yaml:'saved'},{id:'a',yaml:''},{id:'a'}]) {
    const bad=fixture({request:async()=>reply});
    await assert.rejects(bad.queue.submit(scenarios,1),/Unexpected saved scenario response/);
  }
});

test('in-memory retries work when browser storage is unavailable',async()=>{
  const storage={getItem(){throw Error('blocked');},setItem(){throw Error('quota');},removeItem(){throw Error('blocked');}};
  let fail=true;
  const h=fixture({storage,afterAccept:async()=>{if(fail){fail=false;throw Error('lost');}}});
  await assert.rejects(h.queue.submit(scenarios,1));
  await h.queue.submit(scenarios,1);
  assert.equal(h.receipts.size,3);
});

test('corrupt browser journals are ignored and different Runs start distinct submissions',async()=>{
  const storage=memory();storage.setItem('kpl.pending-scenario-runs','{broken');
  let fail=true;
  const h=fixture({storage,afterAccept:async()=>{if(fail){fail=false;throw Error('lost');}}});
  await assert.rejects(h.queue.submit(scenarios,1));
  await h.queue.submit(scenarios,2);
  assert.equal(h.receipts.size,4);
});

test('a malformed saved payload or repeated receipt key cannot replace a fresh registration',async()=>{
  for(const corrupt of [value=>value.payload='null',value=>value.entries[1].key=value.entries[0].key,
    value=>value.next=-1,value=>value.entries[0].id='other',value=>value.payload='{"scenario":"wrong","scenarioId":"b","repetitions":1}']) {
    const storage=memory();
    const failed=fixture({storage,request:async(path,options)=>{
      if(!options)return {id:'a',yaml:'original'};
      throw Error('lost');
    }});
    await assert.rejects(failed.queue.submit(scenarios,1));
    const value=JSON.parse(storage.getItem('kpl.pending-scenario-runs'));corrupt(value);
    storage.setItem('kpl.pending-scenario-runs',JSON.stringify(value));
    const restored=fixture({storage});
    await restored.queue.submit(scenarios,1);
    assert.equal(restored.receipts.size,3);
    assert.equal(restored.calls[0].path,'/api/v1/scenarios/a');
  }
});
