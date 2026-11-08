const test=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs');
const vm=require('node:vm');
const source=fs.readFileSync(__dirname+'/static/app.js','utf8');
const functions=source.slice(source.indexOf('function renderLibraryDeletionPlan('),source.indexOf('async function refreshAgents('));

function fixture(request) {
  const elements=new Map(),toasts=[],removed=[],selected=new Set(['scenario:a','scenario:b','batch:g','run:solo']);
  function element(id) {
    if(!elements.has(id))elements.set(id,{textContent:'',disabled:false,open:false,value:'retained draft',attributes:{},
      showModal(){this.open=true;},close(){this.open=false;},focus(){this.focused=true;},
      setAttribute(name,value){this.attributes[name]=value;},removeAttribute(name){delete this.attributes[name];}});
    return elements.get(id);
  }
  const state={savedScenarios:[{id:'a',name:'Alpha'},{id:'b',name:'Beta'}],selectedScenarioId:'a',
    savedResults:[{id:'one',batchId:'g',name:'Group'},{id:'old',batchId:'g',name:'Group'},{id:'solo',name:'Solo'}],deletedResultIDs:new Set()};
  const sandbox={state,$:element,api:request,document:{querySelector:()=>null},
    KPLLibraryDeletions:require('./static/library-deletions.js'),
    KPLLibraryGroups:{isBusy:()=>Boolean(state.libraryDeleting),forgetSelection:keys=>keys.forEach(key=>selected.delete(key)),refresh:async()=>{}},
    KPLResultImages:{remove:id=>removed.push(id)},showToast:text=>toasts.push(text),
    scenarioOperationBusy:()=>Boolean(state.libraryDeleting),renderSavedScenarios(){},renderSavedResults(){},renderResultViews(){},
    refreshSavedScenarios:async()=>{},refreshSavedResults:async()=>{},
    libraryGroupItems:()=>[...state.savedScenarios.map(item=>({key:'scenario:'+item.id,name:item.name})),
      ...Array.from(new Map(state.savedResults.map(run=>[run.batchId?'batch:'+run.batchId:'run:'+run.id,
        {key:run.batchId?'batch:'+run.batchId:'run:'+run.id,name:run.name}])).values())]};
  vm.createContext(sandbox);vm.runInContext(functions,sandbox);
  return {app:sandbox,state,element,toasts,removed,selected};
}

test('failed cleanup previews individual failed runs and never deletes a whole mixed batch',async()=>{
  const calls=[];const f=fixture(async(path,options)=>{calls.push([path,options.method]);return null});
  f.state.savedResults=[{id:'one',batchId:'g',state:'completed',name:'Success'},
    {id:'old',batchId:'g',state:'failed',name:'Old failure'},
    {id:'canceled',batchId:'g',state:'canceled',name:'Canceled'},
    {id:'solo',state:'failed',name:'Failure'}];
  f.app.requestLibraryDeletion('results',['batch:g','run:solo'],true);
  assert.equal(calls.length,0);assert.equal(f.element('#libraryDeleteDialog').open,true);
  assert.equal(f.element('#libraryDeleteHeading').textContent,'Delete 2 failed runs?');
  assert.match(f.element('#libraryDeleteDescription').textContent,/Completed and other runs are kept/);
  assert.equal(f.element('#cancelLibraryDelete').focused,true);
  await f.app.confirmLibraryDeletion();
  assert.deepEqual(calls,[['/api/v1/results/old?expectedState=failed','DELETE'],['/api/v1/results/solo?expectedState=failed','DELETE']]);
  assert.deepEqual(f.state.savedResults.map(run=>run.id),['one','canceled']);
  assert.equal(f.state.pendingLibraryDeletion,null);assert.equal(f.element('#libraryDeleteDialog').open,false);
});

test('failed cleanup retains its guarded plan and a changed successful run after a conflict',async()=>{
  const f=fixture(async()=>{throw Object.assign(new Error('State changed'),{status:409,code:'result-state-changed'})});
  f.state.savedResults=[{id:'solo',state:'failed',name:'Failure'}];
  f.app.requestLibraryDeletion('results',['run:solo'],true);
  f.state.savedResults[0].state='completed';
  await f.app.confirmLibraryDeletion();
  assert.equal(f.state.savedResults[0].state,'completed');assert.equal(f.state.deletedResultIDs.size,0);
  assert.equal(f.state.pendingLibraryDeletion.mode,'failed');
  assert.equal(f.state.pendingLibraryDeletion.entries[0].url,'/api/v1/results/solo?expectedState=failed');
  assert.match(f.element('#libraryDeleteError').textContent,/no longer failed/);
  assert.equal(f.element('#libraryDeleteDialog').open,true);
});

test('failed cleanup with no failed runs opens no confirmation and sends no request',()=>{
  let calls=0;const f=fixture(async()=>{calls++});
  f.state.savedResults=[{id:'solo',state:'canceled',name:'Canceled'}];
  assert.equal(f.app.requestLibraryDeletion('results',['run:solo'],true),false);
  assert.equal(calls,0);assert.equal(f.element('#libraryDeleteDialog').open,false);
  assert.match(f.toasts.at(-1),/No failed runs/);
});

test('bulk scenario deletion requires confirmation and preserves the loaded editor draft',async()=>{
  const calls=[];const f=fixture(async(path,options)=>{calls.push([path,options.method]);return null;});
  f.app.requestLibraryDeletion('scenarios',['scenario:a','scenario:b']);
  assert.equal(calls.length,0,'opening confirmation must not delete');
  assert.equal(f.element('#libraryDeleteDialog').open,true);
  assert.equal(f.element('#cancelLibraryDelete').focused,true);
  assert.match(f.element('#libraryDeleteDescription').textContent,/Experiment results.*retained/);
  await f.app.confirmLibraryDeletion();
  assert.deepEqual(calls,[['/api/v1/scenarios/a','DELETE'],['/api/v1/scenarios/b','DELETE']]);
  assert.equal(f.state.savedScenarios.length,0);assert.equal(f.state.selectedScenarioId,null);
  assert.equal(f.element('#scenarioText').value,'retained draft');
  assert.equal(f.state.savedResults.length,3,'scenario deletion must preserve results');
  assert.equal(f.element('#libraryDeleteDialog').open,false);
  assert.equal(f.selected.has('scenario:a'),false);
});

test('busy batches remain selected, successful units disappear, and retry deletes only the remainder',async()=>{
  let busy=true;const calls=[];
  const f=fixture(async path=>{
    calls.push(path);
    if(path.includes('result-batches')){
      if(busy)throw Object.assign(Error('active'),{status:409});
      return {deletedIds:['one','old']};
    }
    return null;
  });
  f.app.requestLibraryDeletion('results',['batch:g','run:solo']);
  assert.match(f.element('#libraryDeleteDescription').textContent,/all current and previous runs/);
  await f.app.confirmLibraryDeletion();
  assert.equal(f.element('#libraryDeleteDialog').open,true);
  assert.match(f.element('#libraryDeleteError').textContent,/1 deletion confirmed.*1 failed/);
  assert.equal(f.element('#confirmLibraryDelete').textContent,'Retry delete');
  assert.deepEqual(f.state.savedResults.map(run=>run.id),['one','old']);
  assert.equal(f.selected.has('batch:g'),true);assert.equal(f.selected.has('run:solo'),false);
  busy=false;await f.app.confirmLibraryDeletion();
  assert.equal(calls.filter(path=>path.endsWith('/solo')).length,1);
  assert.equal(f.state.savedResults.length,0);
  assert.ok(['one','old','solo','g'].every(id=>f.removed.includes(id)));
  assert.equal(f.element('#libraryDeleteDialog').open,false);
});

test('the active bulk delete locks controls and rejects duplicate confirmation',async()=>{
  let release,calls=0;const hold=new Promise(resolve=>release=resolve);
  const f=fixture(async()=>{calls++;await hold;return null;});
  f.app.requestLibraryDeletion('scenarios',['scenario:a']);
  const deleting=f.app.confirmLibraryDeletion();
  assert.equal(f.state.libraryDeleting,true);
  assert.equal(f.element('#cancelLibraryDelete').disabled,true);
  assert.equal(f.element('#confirmLibraryDelete').disabled,true);
  await f.app.confirmLibraryDeletion();assert.equal(calls,1);
  release();await deleting;
  assert.equal(f.state.libraryDeleting,false);assert.equal(f.state.deletingResultId,null);
  assert.equal(f.element('#cancelLibraryDelete').disabled,false);
});

test('confirmation snapshots survive selection changes and show names as plain text',async()=>{
  const calls=[];const f=fixture(async path=>{calls.push(path);return null;});
  f.state.savedScenarios[0].name='<script>Alpha</script>';
  f.app.requestLibraryDeletion('scenarios',['scenario:a']);
  assert.equal(f.element('#libraryDeleteNames').textContent,'<script>Alpha</script> · a');
  f.selected.clear();f.selected.add('scenario:b');
  await f.app.confirmLibraryDeletion();
  assert.deepEqual(calls,['/api/v1/scenarios/a']);
  assert.deepEqual(f.state.savedScenarios.map(item=>item.id),['b']);
});

test('focus returns to Select when the empty selection disables Actions',()=>{
  const f=fixture(async()=>null),actions=f.element('actions'),select=f.element('select');actions.disabled=true;
  f.app.document.querySelector=selector=>selector.includes('action-menu-toggle')?actions:select;
  f.app.focusLibrarySelectionTools('results');
  assert.equal(select.focused,true);assert.equal(actions.focused,undefined);
});
