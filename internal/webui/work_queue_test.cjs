const test=require('node:test');
const assert=require('node:assert/strict');
const {createWorkQueue,queueMarkup,phaseLabel}=require('./static/work-queue.js');
const turn=()=>new Promise(setImmediate);
const item=(id,kind='analysis',position=0)=>({id,kind,name:'Task '+id,runId:'run-'+id,batchId:'batch-'+id,state:position?'queued':'running',position});
function fixture(api=async()=>({})){
  let click,opens=[];
  const elements=new Map(['#workQueue','#workQueueList','#workQueueCount','#workQueueStatus'].map(key=>[key,{innerHTML:'',textContent:'',hidden:false,contains:()=>false,setAttribute(name,value){this[name]=value;}}]));
  const document={querySelector:key=>elements.get(key),addEventListener(type,handler){if(type==='click')click=handler;}};
  const view=createWorkQueue({document});view.init({api,onOpen:value=>opens.push(value)});
  function action(id,open=false){click({target:{closest:()=>({dataset:open?{workOpen:id}:{workCancel:id}})},preventDefault(){}});}
  return {view,elements,action,opens};
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

test('queue view hides when empty and does not rebuild unchanged or timestamp-only updates',()=>{
  const h=fixture(),list=h.elements.get('#workQueueList');let writes=0,value='';
  Object.defineProperty(list,'innerHTML',{get:()=>value,set:next=>{writes++;value=next;}});
  h.view.update([]);assert.equal(h.elements.get('#workQueue').hidden,true);
  h.view.update([item('one')]);const before=writes;
  h.view.update([{...item('one'),updatedAt:'later'}]);assert.equal(writes,before);
  assert.equal(h.elements.get('#workQueueCount').textContent,'1');
  h.view.update([]);assert.equal(h.elements.get('#workQueue').hidden,true);assert.equal(value,'');
});

test('analysis progress is shown only when known and queued work never claims calculation progress',()=>{
  assert.doesNotMatch(queueMarkup([item('one')]),/%/);
  assert.match(queueMarkup([{...item('one'),progress:25.9,phase:'aggregating'}]),/Calculating · 25%/);
  assert.doesNotMatch(queueMarkup([{...item('two','analysis',1),progress:75}]),/75%/);
  assert.equal(phaseLabel({...item('one'),phase:'waiting-for-cleanup'}),'Waiting for Peer cleanup');
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

test('host reboot work has a distinct status view and cancel never claims to undo an accepted reboot',async()=>{
 const h=fixture();h.view.update([{...item('maintenance','host-reboot'),operationId:'host-reboot-operation',phase:'waiting-for-hosts',progress:75}]);
 const html=h.elements.get('#workQueueList').innerHTML;
 assert.match(html,/Host reboot/);assert.match(html,/Waiting for hosts to return/);assert.match(html,/Open host reboot status/);assert.doesNotMatch(html,/75%/);
 h.action('maintenance',true);assert.equal(h.opens[0].operationId,'host-reboot-operation');
 h.action('maintenance');await turn();assert.match(h.elements.get('#workQueueStatus').textContent,/already requested on a host cannot be canceled/);
});
