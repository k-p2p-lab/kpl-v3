const test=require('node:test');
const assert=require('node:assert/strict');
const {createWorkQueue,queueMarkup,phaseLabel}=require('./static/work-queue.js');
const turn=()=>new Promise(setImmediate);
const item=(id,kind='analysis',position=0)=>({id,kind,name:'Task '+id,runId:'run-'+id,batchId:'batch-'+id,state:position?'queued':'running',position});
function fixture(api=async()=>({})){
  let click,opens=[];
  const elements=new Map(['#workQueue','#workQueueList','#workQueueCount','#workQueueStatus','#workQueueEmpty','#workQueuePrevious','#workQueueNext'].map(key=>[key,{innerHTML:'',textContent:'',hidden:false,scrollLeft:0,clientWidth:0,scrollWidth:0,events:{},addEventListener(type,handler){this.events[type]=handler;},scrollBy(options){this.scrollLeft=Math.max(0,Math.min(this.scrollWidth-this.clientWidth,this.scrollLeft+options.left));this.events.scroll?.();},contains:()=>false,setAttribute(name,value){this[name]=value;}}]));
  const document={querySelector:key=>elements.get(key),addEventListener(type,handler){if(type==='click')click=handler;}};
  const view=createWorkQueue({document});view.init({api,onOpen:value=>opens.push(value)});
  function action(id,open=false){click({target:{closest:()=>({dataset:open?{workOpen:id}:{workCancel:id}})},preventDefault(){}});}
  return {view,elements,action,opens,document};
}

test('one queue preserves experiment and analysis order and escapes names and identifiers',()=>{
  const entries=[item('one','experiment'),item('two','analysis',1),item('three','batch-analysis',2)];
  entries[1].name='<img src=x onerror=bad>';entries[1].id='"><bad>';
  const html=queueMarkup(entries);
  assert.ok(html.indexOf('Experiment')<html.indexOf('Run analysis'));
  assert.ok(html.indexOf('Run analysis')<html.indexOf('Batch mean analysis'));
  assert.match(html,/1 task ahead/);assert.match(html,/2 tasks ahead/);
  assert.match(html,/&lt;img src=x onerror=bad&gt;/);assert.doesNotMatch(html,/<img|<bad>/);
  assert.equal((html.match(/data-work-cancel=/g)||[]).length,3);
  assert.equal((html.match(/data-work-open=/g)||[]).length,2);
});

test('queue view keeps the empty state and Saved Results entry point visible and does not rebuild unchanged or timestamp-only updates',()=>{
  const h=fixture(),list=h.elements.get('#workQueueList');let writes=0,value='';
  Object.defineProperty(list,'innerHTML',{get:()=>value,set:next=>{writes++;value=next;}});
  h.view.update([]);assert.equal(h.elements.get('#workQueue').hidden,false);assert.equal(h.elements.get('#workQueueEmpty').hidden,false);
  h.view.update([item('one')]);const before=writes;
  h.view.update([{...item('one'),updatedAt:'later'}]);assert.equal(writes,before);
  assert.equal(h.elements.get('#workQueueCount').textContent,'1');assert.equal(h.elements.get('#workQueueEmpty').hidden,true);
  h.view.update([]);assert.equal(h.elements.get('#workQueue').hidden,false);assert.equal(h.elements.get('#workQueueEmpty').hidden,false);assert.equal(value,'');
});

test('analysis progress is shown only when known and queued work never claims calculation progress',()=>{
  assert.doesNotMatch(queueMarkup([item('one')]),/%/);
  assert.match(queueMarkup([{...item('one'),progress:25.9,phase:'aggregating'}]),/Calculating · 25%/);
  assert.doesNotMatch(queueMarkup([{...item('two','analysis',1),progress:75}]),/75%/);
  assert.equal(phaseLabel({...item('one'),phase:'waiting-for-cleanup'}),'Waiting for Peer cleanup');
});

test('an unopened head explains integrated Run metric finalization without adding a queue item',()=>{
  const head={...item('next','experiment'),state:'queued',phase:'Finishing previous Run metrics'};
  assert.equal(phaseLabel(head),'Finishing previous Run metrics');
  assert.match(queueMarkup([head]),/Finishing previous Run metrics/);
  assert.equal(phaseLabel({...head,position:1}),'1 task ahead');
  assert.equal(phaseLabel({...head,phase:''}),'Waiting to start');
});

test('cancel uses an immutable ID once and keeps the slot pending until the live queue removes it',async()=>{
  let resolve;const calls=[];
  const h=fixture((path,options)=>{calls.push({path,options});return new Promise(yes=>resolve=yes);});
  h.view.update([item('a/b')]);h.action('a/b');h.action('a/b');
  assert.equal(calls.length,1);assert.equal(calls[0].path,'/api/v1/work-queue/a%2Fb/cancel');
  assert.equal(calls[0].options.method,'POST');assert.ok(calls[0].options.signal instanceof AbortSignal);
  assert.match(h.elements.get('#workQueueList').innerHTML,/canceling/);
  resolve({});await turn();h.action('a/b');assert.equal(calls.length,1);
  assert.match(h.elements.get('#workQueueStatus').textContent,/waits for cleanup/);
  h.view.update([item('replacement')]);h.action('a/b');assert.equal(calls.length,1);
});

test('failed cancellation stays retryable and a missing task is reported as already finished',async()=>{
  let calls=0;const h=fixture(async()=>{calls++;const error=new Error('offline <error>');if(calls>1)error.status=404;throw error;});
  h.view.update([item('one')]);h.action('one');await turn();
  assert.equal(h.elements.get('#workQueueStatus').role,'alert');
  assert.equal(h.elements.get('#workQueueStatus').textContent,'offline <error>');
  assert.doesNotMatch(h.elements.get('#workQueueList').innerHTML,/canceling/);
  h.action('one');await turn();assert.equal(calls,2);
  assert.match(h.elements.get('#workQueueStatus').textContent,/already finished/);
  assert.equal(h.elements.get('#workQueueStatus').role,'status');
});

test('opening a queued analysis preserves its run or batch identity without changing queue state',()=>{
  const h=fixture();h.view.update([item('one'),item('two','batch-analysis',1)]);
  h.action('two',true);assert.equal(h.opens.length,1);assert.equal(h.opens[0].batchId,'batch-two');
  assert.match(h.elements.get('#workQueueList').innerHTML,/1 task ahead/);
});

test('parallel analyses show their assigned workers in the shared queue', () => {
  const h = fixture();
  const local = {...item('local', 'analysis'), worker:'Controller'};
  const remote = {...item('remote', 'batch-analysis', 1), state:'running', worker:'Analysis worker 1'};
  h.view.update([local, remote, item('next', 'experiment', 2)]);
  const html = h.elements.get('#workQueueList').innerHTML;
  assert.match(html, /aria-label="Queue position 2">2/);
  assert.match(html, /Run analysis · Controller/);
  assert.match(html, /Batch mean analysis · Analysis worker 1/);
  assert.match(html, /2 tasks ahead/);
});

test('host reboot work has a distinct status view and cancel never claims to undo an accepted reboot',async()=>{
 const h=fixture();h.view.update([{...item('maintenance','host-reboot'),operationId:'host-reboot-operation',phase:'waiting-for-hosts',progress:75}]);
 const html=h.elements.get('#workQueueList').innerHTML;
 assert.match(html,/Host reboot/);assert.match(html,/Waiting for hosts to return/);assert.match(html,/Open host reboot status/);assert.doesNotMatch(html,/75%/);
 h.action('maintenance',true);assert.equal(h.opens[0].operationId,'host-reboot-operation');
 h.action('maintenance');await turn();assert.match(h.elements.get('#workQueueStatus').textContent,/already requested on a host cannot be canceled/);
});


test('slider navigation reflects the visible range and does not consume action button arrow keys',()=>{
  const h=fixture(),list=h.elements.get('#workQueueList'),previous=h.elements.get('#workQueuePrevious'),next=h.elements.get('#workQueueNext');
  list.clientWidth=400;list.scrollWidth=1200;h.view.update([item('one'),item('two','analysis',1)]);
  assert.equal(previous.disabled,true);assert.equal(next.disabled,false);
  next.events.click();assert.equal(list.scrollLeft,340);assert.equal(previous.disabled,false);
  let prevented=0;list.events.keydown({target:list,key:'ArrowRight',preventDefault(){prevented++;}});
  assert.equal(list.scrollLeft,680);assert.equal(prevented,1);
  list.events.keydown({target:{dataset:{workCancel:'two'}},key:'ArrowLeft',preventDefault(){prevented++;}});
  assert.equal(list.scrollLeft,680);assert.equal(prevented,1);
  next.events.click();assert.equal(list.scrollLeft,800);assert.equal(next.disabled,true);
  previous.events.click();assert.equal(list.scrollLeft,460);
  h.view.update([]);assert.equal(previous.disabled,true);assert.equal(next.disabled,true);
});

test('experiment detail renderers enrich the immutable slot without adding an unrelated View action',()=>{
 const h=fixture();
 h.view.update([item('batch-work','experiment')],()=>({repeated:true,bodyHTML:'<span>Run 2 of 3</span>',actionsHTML:'<a data-result-download="actual-current">Download snapshot</a>'}));
 const html=h.elements.get('#workQueueList').innerHTML;
 assert.match(html,/Run 2 of 3/);assert.match(html,/data-result-download="actual-current"/);assert.match(html,/>Stop batch<\/button>/);
 assert.match(html,/data-work-cancel="batch-work"/);assert.doesNotMatch(html,/data-work-open/);
 h.view.update([item('batch-work','experiment')],()=>({stopping:true}));
 assert.match(h.elements.get('#workQueueList').innerHTML,/disabled>Stopping…/);
});


test('removing the final focused task restores focus to the visible Refresh control',()=>{
 const h=fixture();h.view.update([item('one')]);
 let focused=false;h.elements.set('#refreshExperiments',{focus(options){focused=options.preventScroll;}});
 h.document.activeElement={dataset:{workDetails:'one'}};
 const list=h.elements.get('#workQueueList');list.contains=()=>true;list.querySelectorAll=()=>[];
 h.view.update([]);assert.equal(focused,true);
});
