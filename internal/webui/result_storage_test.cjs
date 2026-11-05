const test = require('node:test');
const assert = require('node:assert/strict');
const {describe} = require('./static/result-storage.js');
const ready = {availableBytes: 8 * 1024 ** 3, minFreeBytes: 1024 ** 3, lastCheckedAt: '2026-09-25T10:00:00Z'};
test('storage status distinguishes local space, archive discovery and archive errors', () => {
  assert.deepEqual(describe(ready), {text:'Local result space: 8 GiB free. Archive connected.', warning:false});
  assert.match(describe({...ready,lastCheckedAt:'0001-01-01T00:00:00Z'}).text,/Discovering saved archives/);
  const offline=describe({...ready,error:'mount unavailable'});
  assert.equal(offline.warning,true);assert.match(offline.text,/results remain local/);
});
test('low storage reports a pause before the next run and supports a disabled guard', () => {
  const low=describe({...ready,availableBytes:512 * 1024 ** 2});
  assert.equal(low.warning,true);assert.match(low.text,/512 MiB free/);assert.match(low.text,/before starting the next run/);
  assert.equal(describe({...ready,availableBytes:0,minFreeBytes:0}).warning,false);
});
test('only server-reported lack of progress warns, independently of client clock', () => {
  const transferring = {...ready, checking: true, phase: 'copying', checkStartedAt: '2020-01-01T00:00:00Z', lastProgressAt: '2020-01-01T00:01:00Z', stalled: false};
  const healthy = describe(transferring);
  assert.equal(healthy.warning, false);
  assert.match(healthy.text, /Copying results to archive/);
  const stalled = describe({...transferring, stalled: true});
  assert.equal(stalled.warning, true);
  assert.match(stalled.text, /Archive progress is delayed/);
  assert.match(stalled.text, /local recording remains available/);
  assert.doesNotMatch(stalled.text, /Archive connected/);
});

test('copying, verification, discovery, deletion and deliberate pauses stay neutral', () => {
  const labels = {copying: /Copying/, verifying: /Verifying/, discovering: /Discovering/, deleting: /Removing/, publishing: /Finalizing/, preparing: /Preparing/, paused: /paused while experiments/};
  for (const [phase, label] of Object.entries(labels)) {
    const status = describe({...ready, checking: true, phase, checkStartedAt: '2020-01-01T00:00:00Z', stalled: false});
    assert.equal(status.warning, false, phase);
    assert.match(status.text, label);
  }
  assert.match(describe({...ready, phase: 'paused', checking: false}).text, /paused while experiments/);
  assert.equal(describe({...ready, checking: true, phase: 'paused', stalled: true}).warning, false);
});

test('old Controller status does not interpret total cycle time as a NAS timeout', () => {
  const status = describe({...ready, checking: true, checkStartedAt: '2020-01-01T00:00:00Z'});
  assert.equal(status.warning, false);
  assert.match(status.text, /Archive work in progress/);
});

test('storage polling notifies result refresh and catches changes after returning to the page', async () => {
  const fs = require('node:fs'), vm = require('node:vm');
  const listeners = {}, timers = new Map(), seen = [];
  const document = {hidden:false, querySelector:selector=>selector === '#resultStorageStatus' ? {dataset:{}} : null, addEventListener:(name,fn)=>listeners[name]=fn};
  const context = {document, AbortController, Intl, setTimeout:(fn,delay)=>{timers.set(delay,fn);return delay;}, clearTimeout:id=>timers.delete(id), addEventListener:(name,fn)=>listeners[name]=fn};
  vm.createContext(context);
  vm.runInContext(fs.readFileSync(`${__dirname}/static/result-storage.js`,'utf8'),context);
  let revision='local', polls=0;
  context.KPLResultStorage.init({api:async()=>{polls++;return {...ready,resultsRevision:revision};},onStatus:status=>seen.push(status.resultsRevision)});
  await new Promise(setImmediate);
  assert.deepEqual(seen,['local']);
  revision='archived'; timers.get(15000)(); await new Promise(setImmediate);
  assert.deepEqual(seen,['local','archived']);
  document.hidden=true; listeners.visibilitychange();
  assert.equal(timers.has(15000),false);
  revision='late-data'; document.hidden=false; listeners.visibilitychange(); await new Promise(setImmediate);
  assert.equal(seen.at(-1),'late-data');
  listeners.pagehide(); assert.equal(timers.has(15000),false);
  const stoppedPolls=polls; listeners.pageshow(); await new Promise(setImmediate);
  assert.equal(polls,stoppedPolls+1);
});

test('archive access, maintenance and local inspection errors show distinct causes', () => {
  const unavailable = describe({...ready,errorKind:'unavailable',error:'archive storage identity changed'});
  assert.match(unavailable.text,/Archive unavailable/);
  assert.match(unavailable.text,/archive storage identity changed/);
  const maintenance = describe({...ready,errorKind:'maintenance',error:'run-a: invalid manifest'});
  assert.match(maintenance.text,/Some archive work is deferred/);
  assert.match(maintenance.text,/run-a: invalid manifest/);
  assert.doesNotMatch(maintenance.text,/Archive unavailable/);
  const local = describe({...ready,availableBytes:0,errorKind:'local',error:'permission denied'});
  assert.equal(local.warning,true);
  assert.match(local.text,/unknown free/);
  assert.match(local.text,/Local result storage could not be inspected/);
  assert.doesNotMatch(local.text,/Archive unavailable|Waiting for space/);
  assert.doesNotMatch(describe({...ready,error:'legacy failure'}).text,/Archive unavailable/);
});

test('local manifest errors preserve measured space and its admission guard', () => {
  const local={...ready,localSpaceKnown:true,errorKind:'local',error:'invalid local archive identity'};
  const measured=describe(local);
  assert.match(measured.text,/8 GiB free/);
  assert.match(measured.text,/Local result storage needs attention/);
  assert.doesNotMatch(measured.text,/unknown free|could not be inspected/);
  const low=describe({...local,availableBytes:0});
  assert.equal(low.warning,true);
  assert.match(low.text,/0 B free/);
  assert.match(low.text,/Waiting for space before starting the next run/);
  const unknown=describe({...local,localSpaceKnown:false,availableBytes:0});
  assert.match(unknown.text,/unknown free/);
  assert.match(unknown.text,/could not be inspected/);
  assert.doesNotMatch(unknown.text,/Waiting for space/);
});

test('reconnection progress and failure preserve the actual verification status', () => {
  for (const reconnectState of ['queued','running']) {
    const status = describe({...ready,errorKind:'unavailable',error:'old identity error',reconnectState});
    assert.doesNotMatch(status.text,/old identity error|Archive unavailable|Archive connected/);
    assert.equal(status.warning,false);
  }
  assert.match(describe({...ready,reconnectState:'running',phase:'paused'}).text,/waiting for running experiments/);
  const failed = describe({...ready,errorKind:'unavailable',error:'archived event checksum mismatch',reconnectState:'failed',reconnectError:'archived event checksum mismatch'});
  assert.equal(failed.warning,true);
  assert.match(failed.text,/checksum mismatch/);
  assert.doesNotMatch(failed.text,/Archive connected/);
  assert.match(describe({...ready,reconnectState:'completed'}).text,/Archive connected/);
});

function reconnectFixture(api) {
  const fs = require('node:fs'), vm = require('node:vm');
  const elements = new Map(), listeners = {}, timers = new Map();
  for (const id of ['resultStorageStatus','reconnectArchive','archiveReconnectPrompt','confirmArchiveReconnect','cancelArchiveReconnect']) {
    elements.set(`#${id}`, {hidden:id!=='resultStorageStatus',disabled:false,dataset:{},attributes:{},listeners:{},textContent:'',
      setAttribute(name,value){this.attributes[name]=value;},addEventListener(name,fn){this.listeners[name]=fn;},focus(){this.focused=true;}});
  }
  const document = {hidden:false,querySelector:selector=>elements.get(selector),addEventListener:(name,fn)=>listeners[name]=fn};
  let nextTimer=0;
  const context={document,AbortController,Intl,setTimeout:(fn,delay)=>{const id=++nextTimer;timers.set(id,{fn,delay});return id;},clearTimeout:id=>timers.delete(id),addEventListener:(name,fn)=>listeners[name]=fn};
  vm.createContext(context);
  vm.runInContext(fs.readFileSync(`${__dirname}/static/result-storage.js`,'utf8'),context);
  context.KPLResultStorage.init({api});
  return {elements,listeners,timers};
}

test('archive reconnection needs explicit confirmation and coalesces repeated clicks', async () => {
  let status={...ready,errorKind:'unavailable',error:'archive identity changed'}, release;
  const requests=[];
  const fixture=reconnectFixture(async (path,options)=>{
    requests.push({path,options});
    if(options?.method==='POST') return new Promise(resolve=>{release=()=>{status={...status,reconnectState:'queued'};resolve({state:'queued'});};});
    return status;
  });
  await new Promise(setImmediate);
  const get=id=>fixture.elements.get(`#${id}`);
  assert.equal(get('reconnectArchive').hidden,false);
  assert.equal(requests.length,1,'status polling must not reconnect automatically');
  assert.match(get('resultStorageStatus').textContent,/archive identity changed/);
  get('reconnectArchive').listeners.click();
  assert.equal(get('archiveReconnectPrompt').hidden,false);
  get('cancelArchiveReconnect').listeners.click();
  assert.equal(get('archiveReconnectPrompt').hidden,true);
  assert.equal(requests.length,1);
  get('reconnectArchive').listeners.click();
  const pending=get('confirmArchiveReconnect').listeners.click();
  await get('confirmArchiveReconnect').listeners.click();
  assert.equal(requests.filter(r=>r.options?.method==='POST').length,1);
  const post=requests.find(r=>r.options?.method==='POST');
  assert.equal(post.path,'/api/v1/result-storage/reconnect');
  assert.deepEqual(JSON.parse(post.options.body),{confirmCurrentArchive:true,allowMissingRuns:true});
  assert.equal(get('reconnectArchive').disabled,true);
  release();await pending;await new Promise(setImmediate);
  assert.match(get('resultStorageStatus').textContent,/reconnection queued/);
  status={...ready,reconnectState:'completed'};
  fixture.listeners.visibilitychange();await new Promise(setImmediate);
  assert.equal(get('reconnectArchive').hidden,true);
  assert.match(get('resultStorageStatus').textContent,/Archive connected/);
  fixture.listeners.pagehide();
});

test('the storage tooltip keeps the full reconnect permission diagnostic and current error', async () => {
  const permission='confirm archive reconnect identity: '+ 'archive/path/'.repeat(24) +'.kpl-store-id: permission denied (controller uid=100 gid=1000)';
  const status={...ready,errorKind:'unavailable',error:'archive storage identity is missing',reconnectState:'failed',reconnectError:permission};
  const fixture=reconnectFixture(async()=>status);
  await new Promise(setImmediate);
  const element=fixture.elements.get('#resultStorageStatus');
  assert.match(element.textContent,/Last reconnect attempt failed/);
  assert.doesNotMatch(element.textContent,/uid=100/,'the inline status remains bounded');
  assert.match(element.title,/permission denied \(controller uid=100 gid=1000\)/);
  assert.match(element.title,/Current status: archive storage identity is missing/);
});

test('reconnect verification failure stays visible and permits a deliberate retry', async () => {
  let status={...ready,errorKind:'unavailable',error:'identity changed'};
  const fixture=reconnectFixture(async (_path,options)=>{
    if(options?.method==='POST') {status={...status,reconnectState:'failed',reconnectError:'cannot verify archived object'};return {state:'queued'};}
    return status;
  });
  await new Promise(setImmediate);
  const get=id=>fixture.elements.get(`#${id}`);
  get('reconnectArchive').listeners.click();
  await get('confirmArchiveReconnect').listeners.click();
  await new Promise(setImmediate);
  assert.match(get('resultStorageStatus').textContent,/cannot verify archived object/);
  assert.equal(get('reconnectArchive').hidden,false);
  assert.equal(get('reconnectArchive').disabled,false);
  assert.equal(get('archiveReconnectPrompt').hidden,true);
  fixture.listeners.pagehide();
});

test('reconnect invalidates in-flight status and defers polling until admission settles', async () => {
  const unavailable={...ready,errorKind:'unavailable',error:'identity changed'};
  let status=unavailable, getCount=0, staleResolve, postResolve;
  const fixture=reconnectFixture(async (_path,options)=>{
    if(options?.method==='POST') return new Promise(resolve=>{postResolve=resolve;});
    if(++getCount===2) return new Promise(resolve=>{staleResolve=resolve;});
    return status;
  });
  await new Promise(setImmediate);
  const get=id=>fixture.elements.get(`#${id}`);
  fixture.listeners.visibilitychange();
  get('reconnectArchive').listeners.click();
  const post=get('confirmArchiveReconnect').listeners.click();
  fixture.listeners.pageshow();
  fixture.listeners.visibilitychange();
  assert.equal(getCount,2,'POST must not overlap a new status poll');
  status={...unavailable,reconnectState:'queued'};
  postResolve({state:'queued'});await post;
  assert.match(get('resultStorageStatus').textContent,/reconnection queued/);
  staleResolve(unavailable);await new Promise(setImmediate);
  assert.equal(getCount,3,'invalidated GET must trigger fresh reconciliation without waiting 15 seconds');
  assert.match(get('resultStorageStatus').textContent,/reconnection queued/);
  assert.equal(get('reconnectArchive').disabled,true);
  fixture.listeners.pagehide();
});

test('reconnect blocks status polls started after the POST begins', async () => {
  let getCount=0, postResolve;
  const unavailable={...ready,errorKind:'unavailable',error:'identity changed'};
  const fixture=reconnectFixture(async (_path,options)=>{
    if(options?.method==='POST') return new Promise(resolve=>{postResolve=resolve;});
    getCount++;
    return {...unavailable,reconnectState:getCount>1?'queued':undefined};
  });
  await new Promise(setImmediate);
  const get=id=>fixture.elements.get(`#${id}`);
  get('reconnectArchive').listeners.click();
  const post=get('confirmArchiveReconnect').listeners.click();
  fixture.listeners.pageshow();
  fixture.listeners.visibilitychange();
  await new Promise(setImmediate);
  assert.equal(getCount,1,'a status response captured during POST could overwrite its accepted result');
  postResolve({state:'queued'});await post;await new Promise(setImmediate);
  assert.equal(getCount,2);
  assert.match(get('resultStorageStatus').textContent,/reconnection queued/);
  assert.equal(get('reconnectArchive').disabled,true);
  fixture.listeners.pagehide();
});

test('ordinary archive recovery supersedes a failed reconnect request without another POST', async () => {
  const history={reconnectState:'failed',reconnectError:'cannot verify archived object'};
  let status={...ready,...history,errorKind:'unavailable',error:'identity changed'}, posts=0;
  const fixture=reconnectFixture(async (_path,options)=>{if(options?.method==='POST') posts++;return status;});
  await new Promise(setImmediate);
  const get=id=>fixture.elements.get(`#${id}`);
  assert.equal(get('reconnectArchive').hidden,false);
  assert.equal(get('resultStorageStatus').dataset.warning,'true');
  // This also covers the small interval after reconnect failed but before the
  // worker has published its final error: checking cannot claim connection.
  status={...ready,...history,checking:true,phase:'verifying'};
  fixture.listeners.visibilitychange();await new Promise(setImmediate);
  assert.match(get('resultStorageStatus').textContent,/Verifying archived results/);
  assert.match(get('resultStorageStatus').textContent,/Last reconnect attempt failed/);
  assert.doesNotMatch(get('resultStorageStatus').textContent,/Archive connected/);
  status={...ready,...history,checking:true,phase:'copying'};
  fixture.listeners.visibilitychange();await new Promise(setImmediate);
  assert.match(get('resultStorageStatus').textContent,/Copying results to archive/);
  assert.equal(get('resultStorageStatus').dataset.warning,'false');
  status={...ready,...history,checking:false,phase:'idle'};
  fixture.listeners.visibilitychange();await new Promise(setImmediate);
  assert.match(get('resultStorageStatus').textContent,/Archive connected/);
  assert.doesNotMatch(get('resultStorageStatus').textContent,/failed|cannot verify/);
  assert.doesNotMatch(get('resultStorageStatus').title,/cannot verify/);
  assert.equal(get('resultStorageStatus').dataset.warning,'false');
  assert.equal(get('reconnectArchive').hidden,true);
  assert.equal(posts,0);
  fixture.listeners.pagehide();
});

test('older Controller without reconnect support keeps the action hidden', async () => {
  const fixture=reconnectFixture(async()=>({...ready,error:'legacy maintenance failure'}));
  await new Promise(setImmediate);
  assert.equal(fixture.elements.get('#reconnectArchive').hidden,true);
  assert.match(fixture.elements.get('#resultStorageStatus').textContent,/Archiving needs attention/);
  fixture.listeners.pagehide();
});
