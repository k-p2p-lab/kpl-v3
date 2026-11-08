const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),vm=require('node:vm');
const source=fs.readFileSync(__dirname+'/static/app.js','utf8');
const functions=source.slice(source.indexOf('function resultBulkSummary('),source.indexOf('function renderLibraryDeletionPlan('));
function fixture(api){
  const elements=new Map();
  const element=selector=>{
    if(!elements.has(selector))elements.set(selector,{textContent:'',hidden:false,disabled:false,open:false,
      showModal(){this.open=true},close(){this.open=false},focus(){this.focused=true}});
    return elements.get(selector);
  };
  const state={savedResults:[{id:'a',batchId:'b',name:'<long-name>',state:'failed',iteration:1,repetitions:1}],snapshot:{experiments:[]},pendingResumes:new Set(),pendingAppends:new Set()};
  const app={state,$:element,api,AbortController,Date,Number,KPLResultBulkActions:require('./static/result-bulk-actions.js'),
    renderSavedScenarios(){},renderSavedResults(){},renderResultViews(){},refreshSavedResults:async()=>{}};
  vm.createContext(app);vm.runInContext(functions,app);
  return {app,state,element};
}

test('bulk preview snapshots selection and cancellation sends no request',async()=>{
  let calls=0;const f=fixture(async()=>{calls++});const keys=['batch:b'];
  const pending=f.app.requestResultBulkAction('retry',keys);keys.length=0;
  assert.equal(calls,0);assert.equal(f.element('#resultBulkDialog').open,true);
  assert.equal(f.element('#cancelResultBulk').focused,true);
  assert.match(f.element('#resultBulkDescription').textContent,/1 batch with 1 failed, canceled or interrupted/);
  assert.equal(f.state.pendingResultBulk.plan.entries[0].id,'b');
  f.app.closeResultBulkAction();assert.equal(calls,0);
  f.state.pendingResultBulk.resolve('Canceled');await pending;
});

test('a canceled-only selection has an enabled retry preview and names unfinished states',async()=>{
  let calls=0;const f=fixture(async()=>{calls++});
  f.state.savedResults[0].state='canceled';f.state.savedResults[0].startedAt='0001-01-01T00:00:00Z';
  const pending=f.app.requestResultBulkAction('retry',['batch:b']);
  assert.equal(f.element('#resultBulkHeading').textContent,'Retry selected unfinished runs');
  assert.equal(f.element('#confirmResultBulk').disabled,false);
  assert.equal(f.state.pendingResultBulk.plan.entries[0].runIds[0],'a');
  assert.equal(calls,0);f.state.pendingResultBulk.resolve('Canceled');await pending;
});

test('retry acceptance adds a new attempt and preserves all previous result states',async()=>{
  const f=fixture(async()=>({id:'fresh',batchId:'b',state:'queued',iteration:1,repetitions:1}));
  const pending=f.app.requestResultBulkAction('retry',['batch:b']);
  await f.app.confirmResultBulkAction();
  assert.equal(f.state.savedResults.find(run=>run.id==='a').state,'failed');
  assert.equal(f.state.savedResults.find(run=>run.id==='fresh').state,'queued');
  assert.equal(f.state.snapshot.experiments[0].id,'fresh');
  assert.match(f.element('#resultBulkProgress').textContent,/Confirmed 1 retry batch\./);
  assert.equal(f.element('#confirmResultBulk').disabled,true);
  f.state.pendingResultBulk.resolve('Done');await pending;
});

test('unknown bulk outcome disables repeat submission and preserves plain feedback',async()=>{
  let calls=0;const f=fixture(async()=>{calls++;throw Error('<script>lost connection</script>')});
  const pending=f.app.requestResultBulkAction('retry',['batch:b']);
  await f.app.confirmResultBulkAction();await f.app.confirmResultBulkAction();
  assert.equal(calls,1);assert.equal(f.element('#confirmResultBulk').disabled,true);
  assert.match(f.element('#resultBulkError').textContent,/Outcome unknown/);
  assert.match(f.element('#resultBulkReport').textContent,/<script>lost connection<\/script>/);
  assert.equal(f.state.savedResults[0].state,'failed');
  f.state.pendingResultBulk.resolve('Unknown');await pending;
});

test('a newer known analysis survives late accepted responses even when the analysis ID differs',()=>{
  const f=fixture(async()=>{});
  const known={id:'newer',runId:'a',state:'completed',updatedAt:'2026-11-07T02:00:00Z'};
  f.state.savedResults[0].analysis=known;
  f.app.applyResultBulkAccepted('analysis',{entry:{id:'a'},response:{id:'older',runId:'a',state:'queued',updatedAt:'2026-11-07T01:00:00Z'}});
  assert.equal(f.state.savedResults[0].analysis,known);
  f.app.applyResultBulkAccepted('analysis',{entry:{id:'a'},response:{id:'missing-time',runId:'a',state:'queued'}});
  assert.equal(f.state.savedResults[0].analysis,known);
  f.app.applyResultBulkAccepted('analysis',{entry:{id:'a'},response:{id:'latest',runId:'a',state:'queued',updatedAt:'2026-11-07T03:00:00Z'}});
  assert.equal(f.state.savedResults[0].analysis.id,'latest');
});

test('individual retry also snapshots unfinished IDs and treats a stale response as unknown',async()=>{
  const run={id:'failed',batchId:'b',state:'failed',iteration:1};
  const state={savedResults:[run],pendingResumes:new Set(),snapshot:{experiments:[]}};
  const toasts=[],tabs=[];let refreshed=0;
  const app={state,Number,JSON,savedResultBatches:()=>[{id:'b',runs:[run]}],retryBatchRuns:()=>[run],remainingBatchRuns:()=>[],
    renderSavedResults(){},renderResultViews(){},refreshSavedResults:async()=>{refreshed++},
    showToast:text=>toasts.push(text),KPLDashboardTabs:{show:tab=>tabs.push(tab)},
    api:async(path,options)=>{
      assert.equal(path,'/api/v1/result-batches/b/retry');assert.deepEqual(JSON.parse(options.body),{expectedRunIds:['failed']});
      throw Object.assign(Error('attempts changed'),{status:409,code:'batch-attempt-changed'});
    }};
  vm.createContext(app);vm.runInContext(source.slice(source.indexOf('async function resumeSavedBatch('),source.indexOf('function savedResultTable(')),app);
  await app.resumeSavedBatch('b',true);
  assert.match(toasts[0],/Retry outcome unknown/);assert.deepEqual(tabs,['experiments']);assert.equal(refreshed,1);
  assert.equal(run.state,'failed');assert.equal(state.pendingResumes.size,0);
});

test('analysis update ordering preserves RFC3339 nanoseconds within the same millisecond',()=>{
  const f=fixture(async()=>{});
  const known={id:'newer',runId:'a',state:'completed',updatedAt:'2026-11-07T02:00:00.123900Z'};
  f.state.savedResults[0].analysis=known;
  for(const id of ['older','newer']){
    f.app.applyResultBulkAccepted('analysis',{entry:{id:'a'},response:{id,runId:'a',state:'queued',updatedAt:'2026-11-07T02:00:00.123100Z'}});
    assert.equal(f.state.savedResults[0].analysis,known);
  }
  f.app.applyResultBulkAccepted('analysis',{entry:{id:'a'},response:{id:'same-time-different-id',runId:'a',state:'queued',updatedAt:known.updatedAt}});
  assert.equal(f.state.savedResults[0].analysis,known);
  f.app.applyResultBulkAccepted('analysis',{entry:{id:'a'},response:{id:'latest',runId:'a',state:'queued',updatedAt:'2026-11-07T02:00:00.123901Z'}});
  assert.equal(f.state.savedResults[0].analysis.id,'latest');
});
