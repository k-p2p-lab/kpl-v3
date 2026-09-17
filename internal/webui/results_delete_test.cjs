const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

const source = fs.readFileSync(path.join(__dirname, 'static/app.js'), 'utf8');
const functions = source.slice(source.indexOf('function escapeHTML('), source.indexOf('$("#scenarioText").value = defaultScenario;'));
function fixture(extra = {}) {
  const elements = new Map();
  const element = selector => {
    if (!elements.has(selector)) elements.set(selector, {
      textContent:'', innerHTML:'', value:'', disabled:false, hidden:false, open:false,
      querySelectorAll(){return [];}, contains(){return false;},
      classList:{toggle(){}}, setAttribute(){},
      showModal(){this.open=true;}, close(){this.open=false;},
    });
    return elements.get(selector);
  };
  const api = {Intl, Date, AbortController, setTimeout, clearTimeout,
    document:{querySelector:element}, localStorage:{getItem:()=>null,setItem(){}}, ...extra};
  vm.createContext(api);
  vm.runInContext(source.slice(0, source.indexOf('const defaultScenario =')) + functions, api);
  const state = vm.runInContext('state', api);
  state.savedResults = [{id:'saved-run', name:'Saved run', state:'completed'}];
  api.showToast = () => {};
  return {api, state, element};
}
test('opening deletion requires no background size request', () => {
  let calls=0;
  const {api,state,element}=fixture({fetch:()=>{calls++;throw new Error('unexpected request');}});
  state.savedResults[0].sourceBytes=1536;
  api.renderSavedResults();
  api.requestResultDeletion('saved-run');
  assert.equal(element('#deleteResultDialog').open,true);
  assert.equal(calls,0);
});

test('deletion finishes without waiting for a slow list refresh', async () => {
  for (const status of [204,404]) {
    let deletions=0, refreshes=0;
    const {api,state,element}=fixture();
    api.api = async (_url,options)=>{
      assert.equal(options.method,'DELETE');assert.ok(options.signal);deletions++;
      if (status===404) throw Object.assign(new Error('already deleted'),{status});
    };
    api.refreshSavedResults=()=>{refreshes++;return new Promise(()=>{});};
    api.requestResultDeletion('saved-run');
    await api.confirmResultDeletion();
    assert.equal(deletions,1);assert.equal(refreshes,1);
    assert.equal(state.deletingResultId,null);
    assert.equal(element('#deleteResultDialog').open,false);
    assert.equal(state.savedResults.length,0);
    assert.equal(state.deletedResultIDs.has('saved-run'),true);
    assert.equal(element('#confirmDeleteResult').disabled,false);
    api.renderRuns([{id:'saved-run',state:'completed'}]);
    assert.match(element('#runList').innerHTML,/No experiments yet/);
  }
});

test('timed-out deletion releases controls and preserves a retryable result', async () => {
  const timers = new Map(); let next=1;
  const {api,state,element} = fixture({
    setTimeout:(fn,delay)=>{const id=next++;timers.set(id,{fn,delay});return id;},
    clearTimeout:id=>timers.delete(id),
  });
  api.api = (_url,options)=>new Promise((_resolve,reject)=>{
    options.signal.addEventListener('abort',()=>reject(Object.assign(new Error('timeout'),{name:'AbortError'})));
  });
  api.refreshSavedResults = ()=>new Promise(()=>{});
  api.requestResultDeletion('saved-run');
  const deleting = api.confirmResultDeletion();
  assert.equal(element('#cancelDeleteResult').disabled,true);
  assert.equal(timers.size,1);
  const timer=[...timers.values()][0];
  assert.equal(timer.delay,30000);
  timer.fn(); await deleting;
  assert.equal(timers.size,0);
  assert.equal(state.deletingResultId,null);
  assert.equal(state.savedResults.length,1);
  assert.equal(state.deletedResultIDs.size,0);
  assert.equal(element('#confirmDeleteResult').disabled,false);
  assert.equal(element('#cancelDeleteResult').disabled,false);
  assert.match(element('#deleteResultError').textContent,/timed out; it may still finish/);
});

test('real download conflicts stay visible and active results cannot be deleted', async () => {
  const {api,state,element} = fixture();
  let calls=0;
  api.api = async ()=>{calls++;throw Object.assign(new Error('downloading'),{status:409});};
  api.refreshSavedResults = ()=>new Promise(()=>{});
  api.requestResultDeletion('saved-run');
  await api.confirmResultDeletion();
  assert.equal(calls,1);
  assert.equal(state.deletedResultIDs.size,0);
  assert.equal(element('#deleteResultDialog').open,true);
  assert.equal(element('#confirmDeleteResult').disabled,false);
  assert.match(element('#deleteResultError').textContent,/being downloaded/);
  state.savedResults[0].state='running';
  await api.confirmResultDeletion();
  assert.equal(calls,1,'active result was sent to the deletion API');
  assert.match(element('#deleteResultError').textContent,/active/);
});

function groupFixture(extra = {}) {
  const result = fixture(extra);
  result.state.savedResults = [
    {id:'run-2',name:'Same name',batchId:'batch',iteration:2,repetitions:2,state:'failed'},
    {id:'run-1',name:'Same name',batchId:'batch',iteration:1,repetitions:2,state:'completed'},
    {id:'unrelated',name:'Same name',batchId:'other',iteration:1,repetitions:2,state:'completed'},
  ];
  result.api.refreshSavedResults = () => new Promise(() => {});
  return result;
}

test('group deletion confirms its scope and preserves individual deletion wording', () => {
  const {api,state,element} = groupFixture();
  api.renderSavedResults();
  const html = element('#savedResultsRows').innerHTML;
  assert.match(html, /<summary[\s\S]*data-delete-batch="batch"[\s\S]*Delete group[\s\S]*<\/summary>/);
  assert.doesNotMatch(html, /<details[^>]*\bopen/);
  api.requestResultDeletion('batch',true);
  assert.equal(state.pendingDelete.isBatch,true);
  assert.equal(element('#deleteResultHeading').textContent,'Delete result group?');
  assert.match(element('#deleteResultID').textContent,/Batch batch · 2 saved runs/);
  assert.match(element('#deleteResultHelp').textContent,/all 2 saved runs.*group mean analysis/);
  assert.equal(element('#confirmDeleteResult').textContent,'Delete group');
  api.requestResultDeletion('run-1');
  assert.equal(element('#deleteResultHeading').textContent,'Delete saved result?');
  assert.doesNotMatch(element('#deleteResultHelp').textContent,/group mean/);
  assert.equal(element('#confirmDeleteResult').textContent,'Delete result');
});

test('one group request removes returned members but preserves same-name groups', async () => {
  for (const status of [200,404]) {
    const removed = [], calls = [];
    const {api,state,element} = groupFixture({KPLResultImages:{remove:id=>removed.push(id)}});
    api.api = async (url,options) => {
      calls.push(url);
      assert.equal(options.method,'DELETE'); assert.ok(options.signal);
      if (status === 404) throw Object.assign(new Error('already gone'),{status});
      return {deletedIds:['run-1','run-2','new-member']};
    };
    api.requestResultDeletion('batch',true);
    await api.confirmResultDeletion();
    assert.deepEqual(calls,['/api/v1/result-batches/batch']);
    assert.deepEqual(Array.from(state.savedResults,run=>run.id),status===200?['unrelated']:['run-2','run-1','unrelated']);
    assert.equal(state.deletedResultIDs.has('run-1'),status===200); assert.equal(state.deletedResultIDs.has('run-2'),status===200);
    assert.equal(state.deletedResultIDs.has('new-member'),status===200);
    assert.ok(removed.includes('batch')); assert.equal(removed.includes('run-1'),status===200);
    assert.equal(element('#deleteResultDialog').open,false);
    assert.equal(state.deletingResultId,null);
    api.renderRuns([{id:'run-1',state:'completed'},{id:'run-2',state:'failed'}]);
    if (status===200) assert.match(element('#runList').innerHTML,/No experiments yet/);
  }
});

test('stale group membership never hides a run that the server did not delete', async () => {
  const {api,state} = groupFixture();
  api.api=async()=>({deletedIds:['run-1']});
  api.requestResultDeletion('batch',true);
  await api.confirmResultDeletion();
  assert.deepEqual(Array.from(state.savedResults,run=>run.id),['run-2','unrelated']);
  assert.equal(state.deletedResultIDs.has('run-2'),false);
});

test('a queued or live member blocks the whole group before opening and again before confirmation', async () => {
  const {api,state,element} = groupFixture();
  let calls=0; api.api=async()=>{calls++;};
  state.snapshot={experiments:[{id:'new-member',batchId:'batch',state:'queued'}]};
  api.renderSavedResults();
  assert.match(element('#savedResultsRows').innerHTML,/data-delete-batch="batch"[^>]*disabled/);
  api.requestResultDeletion('batch',true);
  assert.equal(element('#deleteResultDialog').open,false);
  state.snapshot=null;
  api.requestResultDeletion('batch',true);
  state.savedResults[0]={...state.savedResults[0],state:'running'};
  await api.confirmResultDeletion();
  assert.equal(calls,0);
  assert.match(element('#deleteResultError').textContent,/active/);
  assert.equal(state.deletedResultIDs.size,0);
});

test('group conflicts and partial storage failures remain visible and retryable', async () => {
  for (const status of [401,409,500]) {
    const {api,state,element} = groupFixture();
    api.api=async()=>{throw Object.assign(new Error('some results may already be deleted'),{status});};
    api.requestResultDeletion('batch',true);
    await api.confirmResultDeletion();
    assert.equal(element('#deleteResultDialog').open,true);
    assert.equal(element('#cancelDeleteResult').disabled,false);
    assert.equal(element('#confirmDeleteResult').textContent,'Delete group');
    assert.equal(state.deletedResultIDs.size,0);
    assert.equal(state.savedResults.length,3);
    assert.match(element('#deleteResultError').textContent,status===409?/being downloaded/:/some results may already be deleted/);
    api.api=async()=>({deletedIds:['run-1','run-2']});
    await api.confirmResultDeletion();
    assert.equal(element('#deleteResultDialog').open,false);
    assert.equal(state.savedResults.length,1);
  }
});

test('group timeout releases controls and prevents duplicate submissions while pending', async () => {
  const timers=new Map(); let next=1,calls=0;
  const {api,state,element}=groupFixture({setTimeout:(fn,delay)=>{const id=next++;timers.set(id,{fn,delay});return id;},clearTimeout:id=>timers.delete(id)});
  api.api=(_url,options)=>{calls++;return new Promise((_resolve,reject)=>options.signal.addEventListener('abort',()=>reject(Object.assign(new Error('timeout'),{name:'AbortError'}))));};
  api.requestResultDeletion('batch',true);
  const pending=api.confirmResultDeletion();
  await api.confirmResultDeletion();
  api.requestResultDeletion('other',true);
  assert.equal(calls,1); assert.equal(state.pendingDelete.id,'batch');
  assert.equal(element('#cancelDeleteResult').disabled,true);
  [...timers.values()].find(timer=>timer.delay===30000).fn();
  await pending;
  assert.equal(element('#cancelDeleteResult').disabled,false);
  assert.equal(element('#confirmDeleteResult').disabled,false);
  assert.equal(state.deletingResultId,null);
  assert.match(element('#deleteResultError').textContent,/timed out; it may still finish/);
  assert.equal(state.savedResults.length,3);
});
