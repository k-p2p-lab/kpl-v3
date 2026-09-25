const test = require('node:test'), assert = require('node:assert/strict');
const {createDialog} = require('./static/batch-append.js');
function fixture(api) {
 const elements=new Map(), busy=[], added=[], errors=[];
 function el(id) {
  if(!elements.has(id)) {
   const listeners=new Map();
   elements.set(id,{value:'',textContent:'',disabled:false,open:false,focus(){},select(){},
    addEventListener(name,fn){if(!listeners.has(name))listeners.set(name,[]);listeners.get(name).push(fn);},
    emit(name,event={preventDefault(){}}){for(const fn of listeners.get(name)||[])fn(event);},
    showModal(){this.open=true;},close(){this.open=false;this.emit('close');}
   });
  }
  return elements.get(id);
 }
 const ui=createDialog({document:{querySelector:el},api,onBusy:(...args)=>busy.push(args),onAdded:(...args)=>added.push(args),onError:error=>errors.push(error)});
 const count=value=>{el('#appendBatchCount').value=String(value);el('#appendBatchCount').emit('input');};
 return {ui,el,busy,added,errors,count};
}
const group={id:'group-a',name:'Completed group',expected:10};

test('append previews 10 + 10 = 20 and submits the observed total to prevent duplicates', async()=>{
 const calls=[];
 const f=fixture(async(path,options)=>{calls.push({path,options});return {id:'new-run',batchId:'group-a',iteration:11,repetitions:20};});
 f.ui.open(group);
 assert.equal(f.el('#appendBatchCount').value,'10');
 assert.match(f.el('#appendBatchSummary').textContent,/10 completed \+ 10 additional = 20 total.*Run 11 of 20/);
 await f.ui.save();
 assert.equal(calls[0].path,'/api/v1/result-batches/group-a/append');
 assert.deepEqual(JSON.parse(calls[0].options.body),{additionalRuns:10,expectedRepetitions:10});
 assert.deepEqual(f.busy,[['group-a',true],['group-a',false]]);
 assert.equal(f.added[0][1],10);assert.equal(f.el('#appendBatchDialog').open,false);
});

test('only positive whole additions within the group limit can start',()=>{
 const f=fixture(async()=>{throw Error('must not submit');});f.ui.open({...group,expected:98});
 assert.equal(f.el('#appendBatchCount').value,'2');assert.equal(f.el('#appendBatchCount').max,'2');
 for(const value of ['',0,-1,1.5,3,'no']){f.count(value);assert.equal(f.el('#confirmAppendBatch').disabled,true);}
 f.count(1);assert.equal(f.el('#confirmAppendBatch').disabled,false);assert.match(f.el('#appendBatchSummary').textContent,/99 total/);
});

test('duplicate submission and closing are blocked until admission finishes',async()=>{
 let finish,writes=0;const f=fixture(()=>{writes++;return new Promise(resolve=>finish=resolve);});f.ui.open(group);
 const pending=f.ui.save();await f.ui.save();assert.equal(writes,1);assert.equal(f.el('#cancelAppendBatch').disabled,true);
 let prevented=false;f.el('#appendBatchDialog').emit('cancel',{preventDefault(){prevented=true;}});assert.equal(prevented,true);
 f.el('#cancelAppendBatch').emit('click');assert.equal(f.el('#appendBatchDialog').open,true);
 finish({id:'new-run',batchId:'group-a',iteration:11,repetitions:20});await pending;
});

for(const kind of ['conflict','timeout'])test(`${kind} preserves the count and requires a fresh group before retry`,async()=>{
 const f=fixture(async()=>{throw Object.assign(Error('Group changed'),kind==='conflict'?{status:409}:{name:'AbortError'});});f.ui.open(group);f.count(5);await f.ui.save();
 assert.equal(f.el('#appendBatchDialog').open,true);assert.equal(f.el('#appendBatchCount').value,'5');assert.equal(f.el('#confirmAppendBatch').disabled,true);
 assert.equal(f.errors.length,1);assert.match(f.el('#appendBatchError').textContent,/Close this window/);assert.deepEqual(f.busy.at(-1),['group-a',false]);
});
