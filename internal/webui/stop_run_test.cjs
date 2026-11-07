const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const {queueMarkup} = require('./static/work-queue.js');
const source = fs.readFileSync(path.join(__dirname, 'static/app.js'), 'utf8');

function fixture(runs) {
  const elements = new Map(), requests = [], toasts = [];
  const element = selector => {
    if (!elements.has(selector)) elements.set(selector, { innerHTML:'', textContent:'', hidden:false });
    return elements.get(selector);
  };
  const sandbox = {
    KPLWorkQueue:{update(items,details){element('#workQueueList').innerHTML=queueMarkup(items,new Set(),details);}},
    Date, Intl, Headers, document:{querySelector:element}, localStorage:{getItem:()=>null},
    setTimeout:()=>0, clearTimeout(){},
    fetch:(url, options)=>new Promise(resolve=>requests.push({url, options, resolve})),
  };
  vm.createContext(sandbox);
  vm.runInContext(source.slice(0, source.indexOf('const defaultScenario =')) +
    source.slice(source.indexOf('function escapeHTML('), source.indexOf('$("#scenarioText").value = defaultScenario;')), sandbox);
  const state = vm.runInContext('state', sandbox);
  sandbox.showToast = message => toasts.push(message);
  const render = next => {
    const groups=new Map();
    for(const run of next){const key=sandbox.runStopKey(run);if(!groups.has(key))groups.set(key,[]);groups.get(key).push(run);}
    const workQueue=[...groups].filter(([,members])=>members.some(sandbox.isPendingRun)).map(([id,members],position)=>({id,kind:'experiment',name:members[0].name,runId:members[0].id,state:members.some(run=>run.state==='running')?'running':'queued',position}));
    state.snapshot={experiments:next,workQueue};sandbox.renderRuns(next);
  };
  const respond = (index, status=202, body={runId:runs[0].id,status:'stopping'}) => {
    requests[index].resolve(new Response(JSON.stringify(body), {status, headers:{'Content-Type':'application/json'}}));
  };
  render(runs);
  return {api:sandbox,state,requests,toasts,element,render,respond};
}

const batch = () => [
  {id:'one',batchId:'series',name:'Test series',state:'running',iteration:1,repetitions:3},
  {id:'two',batchId:'series',name:'Test series',state:'queued',iteration:2,repetitions:3},
  {id:'three',batchId:'series',name:'Test series',state:'queued',iteration:3,repetitions:3},
];

test('one batch stop sends one request and stays disabled after acceptance until all members finish',async()=>{
  const runs=batch();
  const f=fixture(runs);
  const stop=f.api.requestRunStop('two');
  assert.equal(f.requests.length,1);
  assert.equal(f.requests[0].url,'/api/v1/experiments/two/stop');
  assert.equal(f.requests[0].options.method,'POST');
  assert.equal(f.requests[0].options.headers.get('X-KPL-Request'),'dashboard');
  assert.equal((f.element('#workQueueList').innerHTML.match(/disabled>Stopping…/g)||[]).length,1);
  await f.api.requestRunStop('one');
  assert.equal(f.requests.length,1,'another batch member sent a duplicate request');
  f.respond(0,202,{runId:'two',status:'stopping'});
  await stop;
  assert.equal(f.state.pendingStops.has('series'),true);
  assert.deepEqual(f.toasts,['Batch stop requested. Remaining queued runs will be canceled.']);
  f.render(runs.map(run=>({...run})));
  await f.api.requestRunStop('three');
  assert.equal(f.requests.length,1,'cleanup in progress allowed another request');
  assert.equal((f.element('#workQueueList').innerHTML.match(/disabled>Stopping…/g)||[]).length,1);
  f.render(runs.map(run=>({...run,state:run.id==='one'?'canceled':run.state})));
  assert.equal(f.state.pendingStops.has('series'),true,'ended current run unlocked its queued siblings');
  f.render(runs.map(run=>({...run,state:'canceled'})));
  assert.equal(f.state.pendingStops.size,0);
  assert.doesNotMatch(f.element('#workQueueList').innerHTML,/data-work-cancel=/);
});

test('a single experiment stop waits for the final snapshot and does not affect other runs',async()=>{
  const runs=[{id:'single',name:'Single',state:'running'},{id:'other',name:'Other',state:'running'}];
  const f=fixture(runs);
  const stop=f.api.requestRunStop('single');
  f.respond(0);
  await stop;
  assert.equal(f.state.pendingStops.has('single'),true);
  assert.match(f.element('#workQueueList').innerHTML,/data-work-cancel="single"[^>]*disabled>Stopping…/);
  assert.match(f.element('#workQueueList').innerHTML,/data-work-cancel="other"[^>]*>Stop<\/button>/);
  assert.deepEqual(f.toasts,['Stop requested. Cleaning up Peers…']);
  f.render([{...runs[0],state:'canceled'},runs[1]]);
  assert.equal(f.state.pendingStops.size,0);
});

test('a genuine stop rejection reports the error and permits retry',async()=>{
  const f=fixture(batch());
  const first=f.api.requestRunStop('one');
  f.respond(0,503,{error:'Controller temporarily unavailable'});
  await first;
  assert.deepEqual(f.toasts,['Controller temporarily unavailable']);
  assert.equal(f.state.pendingStops.size,0);
  assert.doesNotMatch(f.element('#workQueueList').innerHTML,/disabled>Stopping…/);
  const retry=f.api.requestRunStop('one');
  assert.equal(f.requests.length,2);
  f.respond(1);
  await retry;
  assert.equal(f.state.pendingStops.has('series'),true);
});

test('a terminal snapshot arriving before acknowledgement cannot leave a pending stop behind',async()=>{
  const runs=batch();
  const f=fixture(runs);
  const stop=f.api.requestRunStop('one');
  f.render(runs.map(run=>({...run,state:'canceled'})));
  f.respond(0);
  await stop;
  assert.equal(f.state.pendingStops.size,0);
  await f.api.requestRunStop('one');
  assert.equal(f.requests.length,1,'stale control retried an already finished batch');
});

test('failed, interrupted and removed runs clear stopping controls',async()=>{
  for(const terminal of ['failed','interrupted','removed']) {
    const runs=[{id:'single',name:'Single',state:'running'}];
    const f=fixture(runs);
    const stop=f.api.requestRunStop('single');
    f.respond(0);await stop;
    f.render(terminal==='removed'?[]:[{...runs[0],state:terminal}]);
    assert.equal(f.state.pendingStops.size,0,terminal);
  }
});

test('stopping state and request token belong to the execution, including a reused run ID',async()=>{
 const old=batch().map(run=>({...run,executionId:'execution-old'})),f=fixture(old);
 const pending=f.api.requestRunStop('two');
 assert.equal(f.requests[0].options.headers.get('X-KPL-Execution'),'execution-old');
 f.respond(0);await pending;
 const next=old.map(run=>({...run,executionId:'execution-new'}));f.render(next);
 assert.equal(f.state.pendingStops.size,0,'old stopping state leaked into new execution');
 assert.doesNotMatch(f.element('#workQueueList').innerHTML,/disabled>Stopping/);
 const stop=f.api.requestRunStop('two');assert.equal(f.requests[1].options.headers.get('X-KPL-Execution'),'execution-new');f.respond(1);await stop;
 f.state.pendingStops.clear();f.render(next.map(run=>({...run,stopRequested:true})));
 assert.match(f.element('#workQueueList').innerHTML,/disabled>Stopping/,'refresh forgot the server cancellation');
});
