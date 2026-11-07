const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const {queueMarkup} = require('./static/work-queue.js');
const source = fs.readFileSync(path.join(__dirname, 'static/app.js'), 'utf8');
function fixture() {
  const elements = new Map(),timers=[],cleared=[];
  const element = selector => {
    if (!elements.has(selector)) elements.set(selector, {innerHTML:'',textContent:'',hidden:false});
    return elements.get(selector);
  };
  const api = {Date,Intl,document:{querySelector:element},localStorage:{getItem:()=>null},setTimeout:(fn,delay)=>{timers.push({fn,delay});return timers.length;},clearTimeout:id=>cleared.push(id),
    KPLWorkQueue:{update(items,details){element('#workQueueList').innerHTML=queueMarkup(items,new Set(),details);}}};
  vm.createContext(api);
  vm.runInContext(source.slice(0,source.indexOf('const defaultScenario ='))+
    source.slice(source.indexOf('function escapeHTML('),source.indexOf('$("#scenarioText").value = defaultScenario;')),api);
  const state=vm.runInContext('state',api);
  function render(runs,queue) {
    if (!queue) {
      const groups=new Map();
      for(const run of runs) {
        const key=(run.batchId||run.id)+':'+(run.executionId||'');
        if(!groups.has(key))groups.set(key,[]);groups.get(key).push(run);
      }
      queue=[...groups].filter(([,members])=>members.some(api.isPendingRun)).map(([id,members],position)=>({id,kind:'experiment',name:members[0].name,runId:members[0].id,batchId:members[0].batchId,state:members.some(run=>run.state==='running')?'running':'queued',position}));
    }
    state.snapshot={experiments:runs,workQueue:queue};api.renderRuns(runs);
    return element('#workQueueList').innerHTML;
  }
  return {api,state,element,render,timers,cleared};
}
const timing = () => ({estimatedFinishAt:'2026-09-21T01:30:00Z',remainingSeconds:3600,basis:'observed-runs',observedRuns:2,batchEstimatedFinishAt:'2026-09-21T04:30:00Z',batchRemainingSeconds:14400});

test('queued submissions show FIFO position and cleanup waits without hiding canonical cancellation',()=>{
  const {render}=fixture();
  const cards=render([{id:'active',name:'Active',state:'running'},{id:'next',name:'Next',state:'queued'},{id:'last',name:'Last',state:'queued'}]);
  assert.match(cards,/1 task ahead/);assert.match(cards,/2 tasks ahead/);
  assert.ok(cards.indexOf('title="Next"')<cards.indexOf('title="Last"'));
  const waiting=render([{id:'next',name:'Next',state:'queued',phaseName:'Waiting for previous Peer cleanup: <offline>'}]);
  assert.match(waiting,/Waiting for previous Peer cleanup: &lt;offline&gt;/);assert.match(waiting,/data-work-cancel=/);
});

test('one batch card shows the current iteration and both individual and group forecasts',()=>{
  const {render}=fixture();
  const cards=render([
    {id:'run-1',batchId:'group',name:'Series',iteration:1,repetitions:3,state:'completed'},
    {id:'run-2',batchId:'group',name:'Series',iteration:2,repetitions:3,state:'running',timing:timing()},
    {id:'run-3',batchId:'group',name:'Series',iteration:3,repetitions:3,state:'queued',timing:{...timing(),estimatedFinishAt:'2026-09-21T04:30:00Z'}},
  ]);
  assert.equal((cards.match(/data-work-item=/g)||[]).length,1);
  assert.equal((cards.match(/<span>Est\. finish<\/span>/g)||[]).length,1);
  assert.match(cards,/Run 2 of 3/);assert.match(cards,/datetime="2026-09-21T01:30:00Z"/);assert.match(cards,/datetime="2026-09-21T04:30:00Z"/);
  assert.match(cards,/Based on 2 completed runs/);assert.match(cards,/1 \/ 3 completed/);assert.match(cards,/About 4h remaining/);
  assert.match(cards,/data-result-download="run-2"/);assert.match(cards,/>Stop batch<\/button>/);
});

test('unknown and overdue runs do not claim they have finished',()=>{
  const {api}=fixture();
  assert.match(api.runTimingMarkup({state:'queued'},false),/Estimating…/);
  const delayed=api.runTimingMarkup({state:'running',timing:{...timing(),overdue:true,remainingSeconds:0}},false);
  assert.match(delayed,/Taking longer than estimated/);assert.doesNotMatch(delayed,/Less than 1 min/);
  assert.doesNotMatch(api.runTimingMarkup({state:'running',timing:timing()},true),/<time /);
  for(const state of ['completed','failed','canceled','interrupted'])assert.equal(api.runTimingMarkup({state,timing:timing()},false),'');
});

test('stopping suppresses forecasts and completed history leaves the live queue',()=>{
  const {state,render}=fixture();
  const run={id:'one',batchId:'group',name:'Series',repetitions:2,state:'running',timing:timing()};
  render([run]);state.pendingStops.add('group');
  const cards=render([run]);assert.match(cards,/Stopping…/);assert.doesNotMatch(cards,/<time /);assert.match(cards,/disabled>Stopping…/);
  assert.equal(render([{...run,state:'canceled'}]),'');assert.equal(state.pendingStops.size,0);assert.equal(render([]),'');
});

test('finish rendering escapes names, errors and phases and rejects invalid timestamps',()=>{
  const {api,render}=fixture();
  const cards=render([{id:'one',batchId:'<bad>',name:'<img src=x onerror=bad>',repetitions:2,state:'running',phaseName:'<phase>',error:'<error>',timing:timing()}]);
  assert.doesNotMatch(cards,/<img|<error>|<phase>/);assert.match(cards,/&lt;img/);assert.match(cards,/&lt;error&gt;/);
  for(const value of [undefined,'bad','0001-01-01T00:00:00Z'])assert.equal(api.estimateFinishMarkup(value),'Estimating…');
  assert.match(api.formatEstimateRemaining(90061),/1d 1h 2m/);assert.equal(api.formatEstimateRemaining(NaN),'Estimating…');
});

test('restart merges saved successes once while live state overrides stale metadata',()=>{
  const {state,render}=fixture();
  const completed={id:'first',batchId:'group',iteration:1,repetitions:3,state:'completed'};
  state.savedResults=[completed,{id:'failed',batchId:'group',iteration:2,repetitions:3,state:'failed'}];
  const live=[{id:'retry',batchId:'group',iteration:2,repetitions:3,state:'running',previousRunIds:['failed'],timing:timing()}, {id:'third',batchId:'group',iteration:3,repetitions:3,state:'queued',timing:timing()}];
  assert.match(render(live),/1 \/ 3 completed/);assert.match(render([completed,...live]),/1 \/ 3 completed/);
  state.savedResults.push({...live[0],state:'completed'});assert.match(render(live),/1 \/ 3 completed/);
});

test('queue first-run anchors select current iteration inside their own execution',()=>{
  const {render}=fixture();
  const runs=[
    {id:'first',batchId:'group',executionId:'old',name:'Series',iteration:1,repetitions:4,state:'completed'},
    {id:'second',batchId:'group',executionId:'old',name:'Series',iteration:2,repetitions:4,state:'running',timing:timing()},
    {id:'append',batchId:'group',executionId:'append',name:'Series',iteration:3,repetitions:4,state:'queued',timing:{...timing(),estimatedFinishAt:'2026-09-21T06:30:00Z'}},
    {id:'append-last',batchId:'group',executionId:'append',name:'Series',iteration:4,repetitions:4,state:'queued'},
  ];
  const cards=render(runs);
  assert.equal((cards.match(/data-work-item=/g)||[]).length,2);assert.match(cards,/Run 2 of 4/);assert.match(cards,/Run 3 of 4/);
  assert.match(cards,/data-result-download="second"/);assert.match(cards,/data-result-download="append"/);assert.match(cards,/06:30:00Z/);
});

test('cleanup barrier remains a live slot with terminal diagnostics and exact snapshot identity',()=>{
  const {render}=fixture();
  const cards=render([{id:'first',name:'Finished',state:'failed',cleanupState:'failed',cleanupError:'<cleanup>',dataState:'incomplete',error:'agent delivery failed'}],
    [{id:'work-immutable',kind:'experiment',name:'Finished',runId:'first',state:'running',position:0,phase:'waiting-for-cleanup'}]);
  assert.match(cards,/Waiting for Peer cleanup/);assert.match(cards,/Peer cleanup failed/);assert.match(cards,/Experiment data may be incomplete/);assert.match(cards,/&lt;cleanup&gt;/);assert.match(cards,/agent delivery failed/);
  assert.match(cards,/data-work-cancel="work-immutable"/);assert.match(cards,/data-result-download="first"/);assert.doesNotMatch(cards,/<time /);
});

test('missing anchors never show or download a different execution in the same batch',()=>{
  const {render}=fixture();
  const cards=render([{id:'other',batchId:'group',executionId:'new',state:'running'}], [{id:'old-slot',kind:'experiment',runId:'missing',batchId:'group',state:'running'}]);
  assert.match(cards,/Experiment details will appear/);assert.doesNotMatch(cards,/data-result-download/);
});


test('a retiring stopped batch keeps the last started run rather than its canceled unstarted tail',()=>{
 const {render}=fixture();
 const cards=render([
  {id:'first',batchId:'group',executionId:'same',iteration:1,repetitions:3,state:'completed',startedAt:'2026-11-08T00:00:00Z'},
  {id:'active',batchId:'group',executionId:'same',iteration:2,repetitions:3,state:'canceled',startedAt:'2026-11-08T00:05:00Z',error:'current diagnostic'},
  {id:'tail',batchId:'group',executionId:'same',iteration:3,repetitions:3,state:'canceled',startedAt:'0001-01-01T00:00:00Z'},
 ],[{id:'retiring-slot',kind:'experiment',runId:'first',state:'running',phase:'Stopping'}]);
 assert.match(cards,/Run 2 of 3/);assert.match(cards,/current diagnostic/);assert.match(cards,/data-result-download="active"/);assert.doesNotMatch(cards,/data-result-download="tail"/);
});


test('terminal state changes still debounce Saved Results refresh without polling identical queue snapshots',()=>{
 const {render,timers,cleared}=fixture();
 const first={id:'run',name:'Run',state:'running'};render([first]);assert.equal(timers.length,0);
 render([{...first,phaseName:'Next phase'}]);assert.equal(timers.length,0);
 render([{...first,state:'completed'}]);assert.equal(timers.length,1);assert.equal(timers[0].delay,250);
 render([{...first,state:'completed'}]);assert.equal(timers.length,1);
 render([{...first,state:'completed'},{id:'next',state:'queued'}]);assert.equal(timers.length,2);assert.equal(cleared.at(-1),1);
});
