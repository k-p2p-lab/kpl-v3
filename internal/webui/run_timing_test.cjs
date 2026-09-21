const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const source = fs.readFileSync(path.join(__dirname, 'static/app.js'), 'utf8');
function fixture() {
  const elements = new Map();
  const element = selector => {
    if (!elements.has(selector)) elements.set(selector, {innerHTML:'',textContent:'',hidden:false});
    return elements.get(selector);
  };
  const api = {Date,Intl,document:{querySelector:element},localStorage:{getItem:()=>null},setTimeout:()=>0,clearTimeout(){}};
  vm.createContext(api);
  vm.runInContext(source.slice(0,source.indexOf('const defaultScenario ='))+
    source.slice(source.indexOf('function escapeHTML('),source.indexOf('$("#scenarioText").value = defaultScenario;')),api);
  return {api,state:vm.runInContext('state',api),element};
}
const timing = () => ({estimatedFinishAt:'2026-09-21T01:30:00Z',remainingSeconds:3600,basis:'observed-runs',observedRuns:2,batchEstimatedFinishAt:'2026-09-21T04:30:00Z',batchRemainingSeconds:14400});
test('each queued run gets its own finish and one group summary covers the series',()=>{
  const {api,element}=fixture();
  api.renderRuns([
    {id:'run-1',batchId:'group',name:'Series',iteration:1,repetitions:3,state:'completed'},
    {id:'run-2',batchId:'group',name:'Series',iteration:2,repetitions:3,state:'running',timing:timing()},
    {id:'run-3',batchId:'group',name:'Series',iteration:3,repetitions:3,state:'queued',timing:{...timing(),estimatedFinishAt:'2026-09-21T04:30:00Z'}},
  ]);
  const cards=element('#runList').innerHTML, summary=element('#runBatchEstimates').innerHTML;
  assert.equal((cards.match(/<span>Est\. finish<\/span>/g)||[]).length,2);
  assert.match(cards,/datetime="2026-09-21T01:30:00Z"/);
  assert.match(cards,/datetime="2026-09-21T04:30:00Z"/);
  assert.match(cards,/Based on 2 completed runs/);
  assert.equal((summary.match(/data-batch-estimate=/g)||[]).length,1);
  assert.match(summary,/1 \/ 3 completed/);
  assert.match(summary,/About 4h remaining/);
  assert.equal(element('#runBatchEstimates').hidden,false);
});
test('unknown and overdue runs do not claim they have finished',()=>{
  const {api}=fixture();
  assert.match(api.runTimingMarkup({state:'queued'},false),/Estimating…/);
  const delayed=api.runTimingMarkup({state:'running',timing:{...timing(),overdue:true,remainingSeconds:0}},false);
  assert.match(delayed,/Taking longer than estimated/);
  assert.doesNotMatch(delayed,/Less than 1 min/);
  assert.doesNotMatch(api.runTimingMarkup({state:'running',timing:timing()},true),/<time /);
  for(const state of ['completed','failed','canceled','interrupted']) assert.equal(api.runTimingMarkup({state,timing:timing()},false),'');
});
test('group summaries clear after cancellation and empty lists',()=>{
  const {api,state,element}=fixture();
  const run={id:'one',batchId:'group',name:'Series',repetitions:2,state:'running',timing:timing()};
  api.renderRuns([run]);
  state.pendingStops.add('group');
  api.renderRuns([run]);
  assert.match(element('#runBatchEstimates').innerHTML,/Stopping…/);
  assert.doesNotMatch(element('#runBatchEstimates').innerHTML,/<time /);
  api.renderRuns([{...run,state:'canceled'}]);
  assert.equal(element('#runBatchEstimates').hidden,true);
  assert.equal(element('#runBatchEstimates').innerHTML,'');
  api.renderRuns([]);
  assert.match(element('#runList').innerHTML,/No experiments yet/);
});
test('finish rendering escapes names and rejects invalid timestamps',()=>{
  const {api,element}=fixture();
  api.renderRuns([{id:'one',batchId:'<bad>',name:'<img src=x onerror=bad>',repetitions:2,state:'running',timing:timing()}]);
  assert.doesNotMatch(element('#runBatchEstimates').innerHTML,/<img/);
  assert.match(element('#runBatchEstimates').innerHTML,/&lt;img/);
  for(const value of [undefined,'bad','0001-01-01T00:00:00Z']) assert.equal(api.estimateFinishMarkup(value),'Estimating…');
  assert.match(api.formatEstimateRemaining(90061),/1d 1h 2m/);
  assert.equal(api.formatEstimateRemaining(NaN),'Estimating…');
});


test('continuation after restart includes saved successes once in the live batch summary',()=>{
  const {api,state,element}=fixture();
  const completed={id:'first',batchId:'group',iteration:1,repetitions:3,state:'completed'};
  state.savedResults=[completed,{id:'failed',batchId:'group',iteration:2,repetitions:3,state:'failed'}];
  const live=[
    {id:'retry',batchId:'group',iteration:2,repetitions:3,state:'running',previousRunIds:['failed'],timing:timing()},
    {id:'third',batchId:'group',iteration:3,repetitions:3,state:'queued',timing:timing()},
  ];
  api.renderRuns(live);
  assert.match(element('#runBatchEstimates').innerHTML,/1 \/ 3 completed/);
  api.renderRuns([completed,...live]);
  assert.match(element('#runBatchEstimates').innerHTML,/1 \/ 3 completed/);
  state.savedResults.push({...live[0],state:'completed'});
  api.renderRuns(live);
  assert.match(element('#runBatchEstimates').innerHTML,/1 \/ 3 completed/,'live state must override a stale saved result');
});
