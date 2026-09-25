const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

const source = fs.readFileSync(path.join(__dirname, 'static/app.js'), 'utf8');
const functions = source.slice(source.indexOf('function escapeHTML('), source.indexOf('$("#scenarioText").value = defaultScenario;'));
function fixture(results) {
  const elements = new Map(), timers = new Map();
  let nextTimer = 0, resolve, reject;
  const element = selector => {
    if (!elements.has(selector)) elements.set(selector, {
      textContent: '', html: '', writes: 0, hidden: false, disabled: false, attributes: {},
      get innerHTML() { return this.html; },
      set innerHTML(value) { this.html = value; this.writes++; },
      querySelectorAll() { return []; }, contains() { return false; },
      classList: { toggle() {} },
      setAttribute(name, value) { this.attributes[name] = value; },
    });
    return elements.get(selector);
  };
  const api = {
    document: { querySelector: element }, localStorage: { getItem: () => null },
    Intl, Date, AbortController,
    setTimeout: (fn, delay) => { const id = ++nextTimer; timers.set(id, { fn, delay }); return id; },
    clearTimeout: id => timers.delete(id),
  };
  vm.createContext(api);
  vm.runInContext(source.slice(0, source.indexOf('const defaultScenario =')) + functions, api);
  const state = vm.runInContext('state', api);
  state.savedResults = results;
  api.api = () => new Promise((yes, no) => { resolve = yes; reject = no; });
  api.renderSavedResults();
  return { api, state, element, timers, resolve: value => resolve(value), reject: error => reject(error) };
}

test('analysis polling preserves the visible list and refresh label while the request is pending', async () => {
  const run = { id: 'analyzing', state: 'completed', sourceBytes: 1024, analysis: { state: 'running', progress: 9 } };
  const { api, state, element, timers, resolve } = fixture([run]);
  const rows = element('#savedResultsRows'), writes = rows.writes;
  const refreshing = api.refreshSavedResults();
  assert.equal(state.resultsLoading, true);
  assert.equal(element('#savedResultsStatus').hidden, true);
  assert.equal(element('#savedResultsTable').hidden, false);
  assert.equal(element('#savedResultsTable').attributes['aria-busy'], 'true');
  assert.equal(element('#refreshResults').textContent, 'Refresh');
  assert.equal(rows.writes, writes);
  resolve([{ ...run }]);
  await refreshing;
  assert.equal(element('#savedResultsStatus').hidden, true);
  assert.equal(element('#savedResultsTable').attributes['aria-busy'], 'false');
  assert.equal(element('#refreshResults').textContent, 'Refresh');
  assert.equal(rows.writes, writes, 'an unchanged poll rebuilt the result rows');
  assert.equal([...timers.values()].filter(timer => timer.delay === 3000).length, 1);
  state.savedResults[0].analysis = { state: 'running', progress: 10 };
  api.renderSavedResults();
  assert.match(rows.innerHTML, /Images · 10% read/);
});

test('only initial loading shows loading text; refreshing an empty list preserves its empty state', async () => {
  for (const initial of [null, []]) {
    const { api, element, resolve } = fixture(initial);
    const refreshing = api.refreshSavedResults();
    assert.equal(element('#savedResultsStatus').textContent, initial === null ? 'Loading saved results…' : 'No saved results yet.');
    assert.equal(element('#savedResultsTable').hidden, true);
    resolve([]);
    await refreshing;
    assert.equal(element('#savedResultsStatus').textContent, 'No saved results yet.');
    assert.equal(element('#savedResultsStatus').hidden, false);
  }
});

test('a failed refresh retains the loaded results and reports the error', async () => {
  const { api, element, reject } = fixture([{ id: 'saved', state: 'running' }]);
  const rows = element('#savedResultsRows'), writes = rows.writes;
  const refreshing = api.refreshSavedResults();
  reject(new Error('Controller unavailable'));
  await refreshing;
  assert.equal(rows.writes, writes);
  assert.equal(element('#savedResultsTable').hidden, false);
  assert.equal(element('#savedResultsStatus').hidden, false);
  assert.equal(element('#savedResultsStatus').attributes.role, 'alert');
  assert.match(element('#savedResultsStatus').textContent, /Controller unavailable.*Showing the last loaded list/);
  assert.equal(element('#refreshResults').disabled, false);
});


test('Saved results groups only one repetition batch and preserves individual actions', () => {
  const runs = [
    { id: 'a', name: 'Same name', batchId: 'one', repetitions: 3, state: 'completed' },
    { id: 'b', name: 'Same name', batchId: 'one', repetitions: 3, state: 'completed' },
    { id: 'c', name: 'Same name', batchId: 'one', repetitions: 3, state: 'running' },
    { id: 'd', name: 'Same name', batchId: 'two', repetitions: 2, state: 'completed' },
  ];
  const { api, element } = fixture(runs);
  const groups = api.savedResultBatches(runs);
  assert.equal(groups.length, 2);
  assert.equal(groups[0].completed, 2);
  assert.equal(groups[0].active, true);
  assert.match(element('#savedResultsRows').innerHTML, /data-batch-images="one"[^>]+disabled/);
  assert.match(element('#savedResultsRows').innerHTML, /data-result-images="a"/);
  runs[2].state = 'completed';
  api.renderSavedResults();
  assert.doesNotMatch(element('#savedResultsRows').innerHTML, /data-batch-images="one"[^>]+disabled/);
  assert.match(element('#savedResultsRows').innerHTML, /data-batch-images="two"[^>]+disabled/);
});


test('repeated series keep archive group order and show individual results in execution order', () => {
  const runs = [
    { id: 'first', name: 'Independent', state: 'completed' },
    { id: 'a3', name: 'Same name', batchId: 'alpha', repetitions: 3, iteration: 3, state: 'completed' },
    { id: 'b2', name: 'Same name', batchId: 'beta', repetitions: 2, iteration: 2, state: 'completed' },
    { id: 'a2', name: 'Same name', batchId: 'alpha', repetitions: 3, iteration: 2, state: 'completed' },
    { id: 'middle', state: 'completed' },
    { id: 'a1', name: 'Same name', batchId: 'alpha', repetitions: 3, iteration: 1, state: 'completed' },
    { id: 'b1', name: 'Same name', batchId: 'beta', repetitions: 2, iteration: 1, state: 'completed' },
    { id: 'single', batchId: 'one-run', repetitions: 1, iteration: 1, state: 'completed' },
  ];
  const { element } = fixture(runs);
  const html = element('#savedResultsRows').innerHTML;
  assert.deepEqual([...html.matchAll(/<details[^>]+data-result-batch="([^"]+)"/g)].map(match => match[1]), ['alpha', 'beta']);
  assert.doesNotMatch(html, /<details[^>]*\bopen(?:[\s=>])/);
  const groups = [...html.matchAll(/<details[^>]+data-result-batch="([^"]+)"[\s\S]*?<\/details>/g)];
  assert.match(groups[0][0], /Run 1 of 3[\s\S]*Run 2 of 3[\s\S]*Run 3 of 3/);
  assert.match(groups[1][0], /Run 1 of 2[\s\S]*Run 2 of 2/);
  for (const run of runs) {
    for (const action of ['images', 'download']) assert.equal(html.split(`data-result-${action}="${run.id}"`).length - 1, 1);
    assert.equal(html.split(`data-delete-result="${run.id}"`).length - 1, 1);
  }
  const singleRows = html.replace(/<details[\s\S]*?<\/details>/g, '');
  assert.match(singleRows, /data-result-images="first"[\s\S]*data-result-images="middle"[\s\S]*data-result-images="single"/);
  assert.doesNotMatch(singleRows, /data-result-images="[ab][123]"/);
  assert.ok(html.indexOf('data-result-images="first"') < html.indexOf('data-result-batch="alpha"'));
});

test('series retain incomplete metadata members and remain grouped until their last saved run is deleted', () => {
  const id = 'batch"<&', name = '<img src=x onerror=alert(1)>';
  const runs = [
    { id: 'metadata-partial', batchId: id, name, state: 'unreadable' },
    { id: 'remaining', batchId: id, name, repetitions: 3, iteration: 2, state: 'completed' },
    { id: 'unrelated', name, repetitions: 3, iteration: 1, state: 'completed' },
  ];
  const { api, state, element } = fixture(runs);
  let html = element('#savedResultsRows').innerHTML;
  assert.match(html, /data-result-batch="batch&quot;&lt;&amp;"/);
  assert.match(html, /&lt;img src=x onerror=alert\(1\)&gt;/);
  assert.doesNotMatch(html, /<img/);
  assert.match(html, /<details[\s\S]*data-result-images="remaining"[\s\S]*data-result-images="metadata-partial"[\s\S]*<\/details>/);
  assert.match(html, /2 runs · 1 \/ 3 completed · 1 excluded · 1 missing\/unreadable/);
  state.savedResults = runs.slice(1);
  api.renderSavedResults();
  html = element('#savedResultsRows').innerHTML;
  assert.match(html, /<details/);
  assert.match(html, /1 run · 1 \/ 3 completed/);
  assert.match(html, /data-batch-images="batch&quot;&lt;&amp;"[^>]+disabled/);
  state.savedResults = runs.slice(2);
  api.renderSavedResults();
  html = element('#savedResultsRows').innerHTML;
  assert.doesNotMatch(html, /<details/);
  assert.match(html, /data-result-images="unrelated"/);
});

function resumableRuns() {
  return [
    {id:'first',name:'Series',batchId:'batch',iteration:1,repetitions:4,state:'completed',startedAt:'2026-09-01T00:00:00Z'},
    {id:'failed',name:'Series',batchId:'batch',iteration:2,repetitions:4,state:'failed',startedAt:'2026-09-01T00:01:00Z'},
    {id:'third',name:'Series',batchId:'batch',iteration:3,repetitions:4,state:'canceled',startedAt:'0001-01-01T00:00:00Z'},
    {id:'fourth',name:'Series',batchId:'batch',iteration:4,repetitions:4,state:'canceled',startedAt:'0001-01-01T00:00:00Z'},
  ];
}

test('Continue remaining counts only unstarted runs after failure and preserves the batch', () => {
  const {api,state,element}=fixture(resumableRuns());
  const batch=api.savedResultBatches(state.savedResults)[0];
  assert.deepEqual(Array.from(api.remainingBatchRuns(batch),run=>run.id),['third','fourth']);
  assert.match(element('#savedResultsRows').innerHTML,/data-resume-batch="batch"[^>]*>Continue remaining \(2\)/);
  state.savedResults[2].state='interrupted';
  state.savedResults[2].startedAt='2026-09-01T00:02:00Z';
  api.renderSavedResults();
  assert.match(element('#savedResultsRows').innerHTML,/Continue remaining \(1\)/);
  state.savedResults[3].state='running';
  api.renderSavedResults();
  assert.doesNotMatch(element('#savedResultsRows').innerHTML,/data-resume-batch=/);
});

test('Continue remaining is absent for deliberate stop without failure and for exhausted batches',()=>{
  const {api,state,element}=fixture(resumableRuns());
  state.savedResults[1].state='canceled';
  api.renderSavedResults();
  assert.doesNotMatch(element('#savedResultsRows').innerHTML,/data-resume-batch=/);
  state.savedResults[1].state='failed';
  state.savedResults[2].state='completed';
  state.savedResults[3].state='failed';
  api.renderSavedResults();
  assert.doesNotMatch(element('#savedResultsRows').innerHTML,/data-resume-batch=/);
});

test('Continue remaining sends one POST, preserves attempted runs, and refreshes saved results',async()=>{
  const {api,state,element}=fixture(resumableRuns());
  let resolve,calls=0,refreshes=0,toast='';
  api.api=(path,options)=>{
    calls++;
    assert.equal(path,'/api/v1/result-batches/batch/resume');
    assert.equal(options.method,'POST');
    return new Promise(yes=>{resolve=yes;});
  };
  api.refreshSavedResults=async()=>{refreshes++;};
  api.showToast=message=>{toast=message;};
  const resume=api.resumeSavedBatch('batch');
  assert.match(element('#savedResultsRows').innerHTML,/data-resume-batch="batch"[^>]*disabled[^>]*>Continuing…/);
  await api.resumeSavedBatch('batch');
  assert.equal(calls,1);
  resolve({id:'third',state:'queued'});
  await resume;
  assert.deepEqual(Array.from(state.savedResults,run=>run.state),['completed','failed','queued','queued']);
  assert.deepEqual(Array.from(state.savedResults,run=>run.iteration),[1,2,3,4]);
  assert.equal(refreshes,1);
  assert.equal(state.pendingResumes.size,0);
  assert.match(toast,/Continuing 2 remaining runs/);
  assert.doesNotMatch(element('#savedResultsRows').innerHTML,/data-resume-batch=/);
});

test('a rejected continuation restores the button and leaves the remainder unchanged',async()=>{
  const {api,state,element}=fixture(resumableRuns());
  let toast='';
  api.api=async()=>{throw new Error('Batch is still being finalized');};
  api.showToast=message=>{toast=message;};
  await api.resumeSavedBatch('batch');
  assert.equal(state.pendingResumes.size,0);
  assert.equal(toast,'Batch is still being finalized');
  assert.deepEqual(Array.from(state.savedResults,run=>run.state),['completed','failed','canceled','canceled']);
  assert.match(element('#savedResultsRows').innerHTML,/data-resume-batch="batch"[^>]*>Continue remaining \(2\)/);
});

test('Retry from run includes failed attempts and all unfinished iterations but preserves completed runs',()=>{
  const {api,state,element}=fixture(resumableRuns());
  let batch=api.savedResultBatches(state.savedResults)[0];
  assert.deepEqual(Array.from(api.retryBatchRuns(batch),run=>run.id),['failed','third','fourth']);
  assert.match(element('#savedResultsRows').innerHTML,/data-retry-batch="batch"[^>]*>Retry from run 2 \(3\)/);
  assert.match(element('#savedResultsRows').innerHTML,/Continue remaining \(2\)/);
  for (const status of ['interrupted','canceled']) {
    state.savedResults[1].state=status;
    state.savedResults[1].startedAt='0001-01-01T00:00:00Z';
    api.renderSavedResults();
    assert.match(element('#savedResultsRows').innerHTML,/Retry from run 2 \(3\)/);
  }
  state.savedResults[2].state='completed';
  state.savedResults[3].state='completed';
  api.renderSavedResults();
  assert.match(element('#savedResultsRows').innerHTML,/Retry from run 2 \(1\)/);
  state.savedResults[1].state='completed';
  api.renderSavedResults();
  assert.doesNotMatch(element('#savedResultsRows').innerHTML,/data-retry-batch=/);
});

test('Retry experiment is available for a saved single failure and an interrupted last iteration',()=>{
  const {api,state,element}=fixture([{id:'single',batchId:'single',iteration:1,repetitions:1,state:'failed'}]);
  assert.match(element('#savedResultsRows').innerHTML,/data-retry-batch="single"[^>]*>Retry experiment/);
  state.savedResults=resumableRuns().map(run=>({...run,state:run.iteration===4?'interrupted':'completed'}));
  api.renderSavedResults();
  assert.match(element('#savedResultsRows').innerHTML,/Retry from run 4 \(1\)/);
  assert.doesNotMatch(element('#savedResultsRows').innerHTML,/data-resume-batch=/);
});

test('Retry prevents duplicate or competing continuation requests and keeps old attempts immutable',async()=>{
  const {api,state,element}=fixture(resumableRuns());
  const before=JSON.stringify(state.savedResults);
  let resolve,calls=0,toast='';
  api.api=(path,options)=>{
    calls++;
    assert.equal(path,'/api/v1/result-batches/batch/retry');
    assert.equal(options.method,'POST');
    return new Promise(yes=>{resolve=yes;});
  };
  api.refreshSavedResults=async()=>{};
  api.showToast=message=>{toast=message;};
  const retry=api.resumeSavedBatch('batch',true);
  assert.match(element('#savedResultsRows').innerHTML,/data-retry-batch="batch"[^>]*disabled[^>]*>Continuing…/);
  await api.resumeSavedBatch('batch',true);
  await api.resumeSavedBatch('batch');
  assert.equal(calls,1);
  resolve({id:'new-second',name:'Series',batchId:'batch',iteration:2,repetitions:4,state:'queued',previousRunIds:['failed']});
  await retry;
  assert.equal(JSON.stringify(state.savedResults.slice(0,4)),before);
  assert.equal(state.savedResults[4].id,'new-second');
  assert.equal(api.savedResultBatches(state.savedResults)[0].active,true);
  assert.doesNotMatch(element('#savedResultsRows').innerHTML,/data-retry-batch=/);
  assert.match(toast,/Queued 3 unfinished runs from run 2/);
});

test('Retry failure restores controls and historical attempts do not inflate batch counts or repeat targets',async()=>{
  const {api,state,element}=fixture(resumableRuns());
  const before=JSON.stringify(state.savedResults);
  let toast='';
  api.api=async()=>{throw new Error('Agent unavailable');};
  api.showToast=message=>{toast=message;};
  await api.resumeSavedBatch('batch',true);
  assert.equal(toast,'Agent unavailable');
  assert.equal(JSON.stringify(state.savedResults),before);
  assert.equal(state.pendingResumes.size,0);
  assert.match(element('#savedResultsRows').innerHTML,/Retry from run 2 \(3\)/);
  state.savedResults.push({id:'new-second',name:'Series',batchId:'batch',iteration:2,repetitions:4,state:'completed',previousRunIds:['failed']});
  api.renderSavedResults();
  const batch=api.savedResultBatches(state.savedResults)[0];
  assert.equal(batch.runs.length,4);
  assert.equal(batch.completed,2);
  assert.deepEqual(Array.from(batch.previousRuns,run=>run.id),['failed']);
  assert.deepEqual(Array.from(api.retryBatchRuns(batch),run=>run.id),['third','fourth']);
  const html=element('#savedResultsRows').innerHTML;
  assert.match(html,/4 runs · 2 \/ 4 completed/);
  assert.match(html,/Previous attempts · 1 saved record/);
  for(const run of state.savedResults) assert.equal(html.split(`data-result-download="${run.id}"`).length-1,1);
});

test('Retry is blocked while work is active or metadata is invalid',()=>{
  for (const status of ['queued','running','unreadable']) {
    const runs=resumableRuns(); runs[2].state=status;
    const {element}=fixture(runs);
    assert.doesNotMatch(element('#savedResultsRows').innerHTML,/data-retry-batch=/);
  }
  const runs=resumableRuns(); runs[2].iteration=2;
  const {element}=fixture(runs);
  assert.doesNotMatch(element('#savedResultsRows').innerHTML,/data-retry-batch=/);
});


test('Retry after a Controller restart accepts an empty live snapshot',async()=>{
  const {api,state}=fixture(resumableRuns());
  state.snapshot={experiments:null};
  let toast='';
  api.api=async()=>({id:'retry',batchId:'batch',iteration:2,repetitions:4,state:'queued',previousRunIds:['failed']});
  api.refreshSavedResults=async()=>{};
  api.renderResultViews=()=>{};
  api.showToast=message=>{toast=message;};
  await api.resumeSavedBatch('batch',true);
  assert.equal(state.snapshot.experiments.length,1);
  assert.equal(state.snapshot.experiments[0].id,'retry');
  assert.match(toast,/Queued 3 unfinished runs/);
});

test('queued, running and retried batch rows keep numeric execution order across refresh', () => {
  const runs = [
    {id:'old-2',batchId:'batch',iteration:2,repetitions:12,state:'failed'},
    {id:'run-10',batchId:'batch',iteration:10,repetitions:12,state:'queued'},
    {id:'retry-2',batchId:'batch',iteration:2,repetitions:12,state:'queued',previousRunIds:['old-2']},
    {id:'run-3',batchId:'batch',iteration:3,repetitions:12,state:'queued'},
    {id:'run-1',batchId:'batch',iteration:1,repetitions:12,state:'completed'},
  ];
  const {api,state,element} = fixture(runs);
  const original = JSON.stringify(runs);
  const assertOrder = () => {
    const html = element('#savedResultsRows').innerHTML;
    const current = html.split('Previous attempts')[0];
    assert.deepEqual([...current.matchAll(/data-result-images="([^"]+)"/g)].map(match=>match[1]),['run-1','retry-2','run-3','run-10']);
    assert.equal((html.match(/data-result-images="old-2"/g)||[]).length,1);
    assert.doesNotMatch(current,/data-result-images="old-2"/);
  };
  assertOrder();
  assert.equal(JSON.stringify(runs),original,'rendering mutated the received archive order');
  state.savedResults = [...runs].reverse().map(run=>run.id==='retry-2'?{...run,state:'running',startedAt:'2026-09-23T00:00:00Z'}:run);
  api.renderSavedResults();
  assertOrder();
});

test('result search keeps full matching batches and treats search text literally', () => {
  const runs = [
    {id:'mesh-1',batchId:'mesh',name:'Churn Study',iteration:1,repetitions:2,state:'completed'},
    {id:'mesh-2',batchId:'mesh',name:'Churn Study',iteration:2,repetitions:2,state:'queued'},
    {id:'baseline',name:'Literal .* example',state:'completed'},
  ];
  const {api} = fixture(runs);
  const ids = result => Array.from(result,run=>run.id);
  for (const query of [' MESH-2 ', 'churn study', 'mesh']) {
    assert.deepEqual(ids(api.filterSavedResults(runs,query,'all')),['mesh-1','mesh-2']);
  }
  assert.deepEqual(ids(api.filterSavedResults(runs,'.*','all')),['baseline']);
  assert.deepEqual(ids(api.filterSavedResults(runs,'not present','all')),[]);
  assert.deepEqual(ids(runs),['mesh-1','mesh-2','baseline']);
});

test('result status filters use current attempts and require a complete batch for Completed', () => {
  const runs = [
    {id:'old-failure',batchId:'recovered',iteration:1,repetitions:1,state:'failed'},
    {id:'recovered-run',batchId:'recovered',iteration:1,repetitions:1,state:'completed',previousRunIds:['old-failure']},
    {id:'active-done',batchId:'active',iteration:1,repetitions:2,state:'completed'},
    {id:'active-next',batchId:'active',iteration:2,repetitions:2,state:'queued'},
    {id:'failed-done',batchId:'failed',iteration:1,repetitions:2,state:'completed'},
    {id:'failed-run',batchId:'failed',iteration:2,repetitions:2,state:'interrupted'},
    {id:'unreadable',state:'unreadable'},
  ];
  const {api} = fixture(runs);
  const filter = (query,status) => Array.from(api.filterSavedResults(runs,query,status),run=>run.id);
  assert.deepEqual(filter('','completed'),['old-failure','recovered-run']);
  assert.deepEqual(filter('','active'),['active-done','active-next']);
  assert.deepEqual(filter('','attention'),['failed-done','failed-run','unreadable']);
  assert.deepEqual(filter('failed-run','attention'),['failed-done','failed-run']);
  assert.deepEqual(filter('failed-run','completed'),[]);
});

test('result filters survive refresh; clearing search preserves status and All preserves search', async () => {
  const runs = [{id:'one',name:'First',state:'completed'},{id:'two',name:'Second',state:'failed'}];
  const {api,state,element,resolve} = fixture(runs);
  state.resultQuery='missing';
  state.resultStatus='attention';
  api.renderSavedResults();
  assert.match(element('#savedResultsStatus').textContent,/No matching results/);
  assert.equal(element('#savedResultsTable').hidden,true);
  assert.equal(element('#clearResultSearch').hidden,false);
  assert.equal(element('#resultFilterSummary').textContent,'0 of 2 saved runs');
  state.resultQuery='second';
  const refreshing=api.refreshSavedResults();
  resolve(runs.map(run=>({...run})));
  await refreshing;
  assert.equal(state.resultQuery,'second');
  assert.equal(element('#resultFilterSummary').textContent,'1 of 2 saved runs');
  assert.doesNotMatch(element('#savedResultsRows').innerHTML,/data-result-images="one"/);
  assert.match(element('#savedResultsRows').innerHTML,/data-result-images="two"/);
  let focused=0;
  element('#resultSearch').focus=()=>focused++;
  api.clearResultSearch();
  assert.equal(element('#resultSearch').value,'');
  assert.equal(state.resultStatus,'attention');
  assert.equal(element('#resultStatus-attention').checked,true);
  assert.equal(element('#clearResultSearch').hidden,true);
  assert.equal(element('#resultFilterSummary').textContent,'1 of 2 saved runs');
  state.resultQuery='second';
  api.setResultStatus('all');
  assert.equal(state.resultQuery,'second');
  assert.equal(element('#resultStatus-all').checked,true);
  assert.equal(element('#resultStatus-attention').checked,false);
  assert.equal(element('#resultFilterSummary').textContent,'1 of 2 saved runs');
  api.clearResultSearch();
  assert.equal(element('#resultFilterSummary').textContent,'2 saved runs');
  assert.equal(state.savedResults.length,2);
  assert.equal(focused,2);
});

test('group note preview is independent of run notes and survives filtering and previous attempts', () => {
  const runs = [
    {id:'old', batchId:'group', name:'Repeated', repetitions:2, iteration:1, state:'failed', groupNote:{preview:'<script>group</script>'}},
    {id:'retry', batchId:'group', name:'Repeated', repetitions:2, iteration:1, state:'completed', previousRunIds:['old'], note:{preview:'Run observation'}},
    {id:'second', batchId:'group', name:'Repeated', repetitions:2, iteration:2, state:'completed'},
    {id:'unrelated', batchId:'other', name:'Other', repetitions:2, iteration:1, state:'completed'},
  ];
  const {api, state, element} = fixture(runs);
  state.resultQuery = 'retry'; state.resultStatus = 'completed'; api.renderSavedResults();
  const html = element('#savedResultsRows').innerHTML;
  assert.equal(html.split('data-group-note="group"').length - 1, 1);
  assert.match(html, /Edit group note/);
  assert.match(html, /&lt;script&gt;group&lt;\/script&gt;/);
  assert.ok(!html.includes('<script>'));
  assert.match(html, /Run observation/);
  assert.ok(!html.includes('data-group-note="other"'));
  assert.equal(api.savedResultBatches(runs)[0].note.preview, '<script>group</script>');
});

test('only complete current groups offer append and analysis or pending admission disables it', () => {
  const runs=[1,2].map(iteration=>({id:`run-${iteration}`,batchId:'group',name:'Completed group',iteration,repetitions:2,state:'completed'}));
  const {api,state,element}=fixture(runs);
  assert.match(element('#savedResultsRows').innerHTML,/data-append-batch="group"/);
  const batch=api.savedResultBatches(runs)[0];
  assert.equal(api.canAppendBatch(batch),true);
  assert.equal(api.canAppendBatch({...batch,expected:3}),false);
  assert.equal(api.canAppendBatch({...batch,runs:[runs[0],{...runs[1],state:'failed'}]}),false);
  state.pendingAppends.add('group');api.renderSavedResults();
  assert.match(element('#savedResultsRows').innerHTML,/data-append-batch="group"[^>]*disabled/);
  assert.match(element('#savedResultsRows').innerHTML,/data-delete-batch="group"[^>]*disabled/);
  state.pendingAppends.clear();runs[0].analysis={state:'running'};api.renderSavedResults();
  assert.match(element('#savedResultsRows').innerHTML,/data-append-batch="group"[^>]*disabled/);
});

test('retired attempts stay excluded after their replacement is deleted and stale analyses request an update',()=>{
 const old={id:'old',batchId:'group',iteration:1,repetitions:2,state:'failed',superseded:true};
 const current={id:'second',batchId:'group',iteration:2,repetitions:2,state:'completed',dataState:'incomplete',cleanupState:'failed',cleanupError:'Agent <offline>',batchAnalysis:{state:'idle',stale:true}};
 const f=fixture([old,current]),groups=f.api.savedResultBatches([old,current]);
 assert.equal(groups[0].runs.length,1);assert.equal(groups[0].previousRuns.length,1);
 const markup=f.element('#savedResultsRows').innerHTML;
 assert.match(markup,/Batch mean · Update/);assert.match(markup,/Peer cleanup failed/);assert.match(markup,/Experiment data may be incomplete/);assert.match(markup,/Agent &lt;offline&gt;/);
});


test('normal recording and cleanup use neutral activity, with no pending warning', () => {
  const cases = [
    { state: 'running', cleanupState: 'pending', dataState: 'collecting', label: 'Recording experiment data' },
    { state: 'running', cleanupState: 'running', dataState: 'collecting', label: 'Cleaning up Peers and collecting final logs' },
    { state: 'completed', cleanupState: 'retained', dataState: 'collecting', label: 'Peers retained · Recording experiment data' },
  ];
  for (const [i, run] of cases.entries()) {
    const f = fixture([{ id: `run-${i}`, name: 'Normal experiment', ...run }]);
    const markup = f.element('#savedResultsRows').innerHTML;
    assert.match(markup, /class="result-activity"/);
    assert.ok(markup.includes(run.label));
    assert.doesNotMatch(markup, /class="result-integrity"|Peer cleanup: pending|Data: collecting/);
  }
});

test('queued and fully collected runs omit extra status while interrupted collection remains unverified', () => {
  for (const run of [
    { state: 'queued', cleanupState: 'pending', dataState: 'pending' },
    { state: 'completed', cleanupState: 'complete', dataState: 'complete' },
  ]) {
    const f = fixture([{ id: 'quiet', name: 'Quiet experiment', ...run }]);
    assert.doesNotMatch(f.element('#savedResultsRows').innerHTML, /class="result-(?:integrity|activity)"/);
  }
  const f = fixture([{ id: 'interrupted', state: 'interrupted', cleanupState: 'pending', dataState: 'collecting' }]);
  const markup = f.element('#savedResultsRows').innerHTML;
  assert.match(markup, /class="result-integrity"/);
  assert.match(markup, /Peer cleanup has not been confirmed/);
  assert.match(markup, /Data completeness has not been verified/);
  assert.doesNotMatch(markup, /Recording experiment data/);
});

test('recording errors still warn during an active run and escape error text', () => {
  const f = fixture([{ id: 'broken', state: 'running', cleanupState: 'pending', dataState: 'collecting', integrityError: 'Write failed <disk>' }]);
  const markup = f.element('#savedResultsRows').innerHTML;
  assert.match(markup, /class="result-integrity"/);
  assert.match(markup, /Write failed &lt;disk&gt;/);
  assert.doesNotMatch(markup, /class="result-activity"|<disk>/);
});
