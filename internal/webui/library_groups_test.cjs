const test = require('node:test');
const assert = require('node:assert/strict');
const {createLibraryGroups} = require('./static/library-groups.js');

const initial = () => ({revision:'1', groups:[{id:'g1',name:'Experiment A'},{id:'g2',name:'Experiment B'}], memberships:{'scenario:one':'g1','batch:one':'g1','run:single':'g2'}});
const nextTurn = () => new Promise(setImmediate);
const deferred = () => { let resolve,reject; const promise=new Promise((yes,no)=>{resolve=yes;reject=no;});return {promise,resolve,reject}; };

function harness(api, configuration={}) {
  const listeners = new Map();
  const document = {querySelector:()=>null,querySelectorAll:()=>[],addEventListener:(type,handler,capture)=>{
    const entries=listeners.get(type)||[];entries.push({handler,capture});listeners.set(type,entries);
  }};
  let changes=0;
  const groups=createLibraryGroups({document});
  const items=[{key:'scenario:one',name:'First scenario'},{key:'scenario:two',name:'Second scenario'},
    {key:'batch:one',name:'First batch'},{key:'run:single',name:'Single run'}];
  const initialized=groups.init({api,getItems:()=>items,onChanged:()=>changes++,...configuration});
  async function action(name, scope='scenarios') {
    const button={disabled:false,dataset:{libraryAction:name,libraryScope:scope}};
    const event={target:{closest:selector=>selector==='[data-library-action]'?button:null},preventDefault(){},stopPropagation(){}};
    for(const entry of listeners.get('click')||[])entry.handler(event);
    await nextTurn();
  }
  function check(key, checked) {
    const element={checked,dataset:{librarySelectKey:key},matches:selector=>selector==='[data-library-select-key]'};
    for(const entry of listeners.get('change')||[])entry.handler({target:element});
    return element.checked;
  }
  return {groups,initialized,items,action,check,changes:()=>changes,listeners};
}

test('scenario and result filters use one shared group snapshot', async()=>{
  const h=harness(async()=>initial());await h.initialized;
  assert.equal(h.groups.matches('scenario:two'),true);
  assert.equal(h.groups.getImportGroup(),'');
  h.groups.setFilter('g1');
  assert.equal(h.groups.isFiltered(),true);
  assert.equal(h.groups.getImportGroup(),'g1');
  assert.equal(h.groups.matches('scenario:one'),true);
  assert.equal(h.groups.matches('batch:one'),true);
  assert.equal(h.groups.matches('run:single'),false);
  h.groups.setFilter('');
  assert.equal(h.groups.matches('scenario:two'),true);
  assert.equal(h.groups.matches('scenario:one'),false);
  h.groups.setFilter('*');
  assert.equal(h.groups.isFiltered(),false);
  assert.equal(h.groups.getGroupName('g2'),'Experiment B');
});

test('a deleted group returns its items to Ungrouped without deleting source items',async()=>{
  let current=initial();const h=harness(async()=>current);await h.initialized;
  h.groups.setFilter('g1');
  current={revision:'2',groups:[{id:'g2',name:'Experiment B'}],memberships:{'run:single':'g2'}};
  await h.groups.refresh();
  assert.equal(h.groups.getImportGroup(),'');
  assert.equal(h.groups.matches('scenario:one'),true);
  assert.equal(h.groups.matches('batch:one'),true);
  assert.equal(h.groups.matches('run:single'),false);
  assert.equal(h.items.length,4);
});

test('failed refresh retains existing memberships, names, and filter',async()=>{
  let fail=false;const h=harness(async()=>{if(fail)throw new Error('offline');return initial();});await h.initialized;
  h.groups.setFilter('g2');fail=true;
  await assert.rejects(h.groups.refresh(),/offline/);
  assert.equal(h.groups.matches('run:single'),true);
  assert.equal(h.groups.matches('scenario:one'),false);
  assert.equal(h.groups.getGroupName('g2'),'Experiment B');
});

test('invalid responses cannot replace the last complete snapshot',async()=>{
  let current=initial();const h=harness(async()=>current);await h.initialized;
  current={revision:'2',groups:[{id:'g3',name:'Invalid replacement'}],memberships:{'scenario:one':'missing'}};
  await assert.rejects(h.groups.refresh(),/invalid/);
  assert.equal(h.groups.getGroupName('g1'),'Experiment A');
  assert.equal(h.groups.getGroupName('g3'),'');
});

test('moving items sends unique canonical keys and the current revision',async()=>{
  const calls=[];const h=harness(async(path,options)=>{
    calls.push({path,options});
    if(options.method)return {revision:'2',groups:initial().groups,memberships:{...initial().memberships,'scenario:two':'g2','batch:one':'g2'}};
    return initial();
  });await h.initialized;
  await h.groups.assign(['scenario:two','batch:one','batch:one'],'g2');
  const write=calls.find(call=>call.options.method);
  assert.equal(write.path,'/api/v1/library-groups/members');
  assert.equal(write.options.method,'PUT');
  assert.deepEqual(JSON.parse(write.options.body),{groupId:'g2',keys:['scenario:two','batch:one'],revision:'1'});
  assert.equal(write.options.signal instanceof AbortSignal,true);
  h.groups.setFilter('g2');
  assert.equal(h.groups.matches('scenario:two'),true);
  assert.equal(h.groups.matches('batch:one'),true);
});

test('moving to Ungrouped uses the empty group ID',async()=>{
  let body;const h=harness(async(path,options)=>{
    if(options.method){body=JSON.parse(options.body);return {...initial(),revision:'2',memberships:{}};}
    return initial();
  });await h.initialized;
  await h.groups.assign(['batch:one'],'');
  assert.equal(body.groupId,'');h.groups.setFilter('');assert.equal(h.groups.matches('batch:one'),true);
});

test('revision conflicts reload without automatically repeating a mutation',async()=>{
  let reads=0,writes=0;const h=harness(async(path,options)=>{
    if(options.method){writes++;const error=new Error('stale');error.status=409;throw error;}
    reads++;return {...initial(),revision:String(reads),memberships:reads===1?initial().memberships:{'scenario:two':'g2'}};
  });await h.initialized;
  await assert.rejects(h.groups.assign(['scenario:one'],'g2'),/Review.*try again/);
  assert.equal(writes,1);assert.equal(reads,2);
  h.groups.setFilter('g2');
  assert.equal(h.groups.matches('scenario:two'),true);assert.equal(h.groups.matches('scenario:one'),false);
});

test('an uncertain failed write requires a successful refresh before retrying',async()=>{
  let writes=0,revision='1';const h=harness(async(path,options)=>{
    if(options.method){writes++;throw new Error('connection interrupted');}
    return {...initial(),revision};
  });await h.initialized;
  await assert.rejects(h.groups.assign(['scenario:one'],'g2'),/connection interrupted/);
  await assert.rejects(h.groups.assign(['scenario:one'],'g2'),/Refresh groups/);
  assert.equal(writes,1);assert.equal(h.groups.getGroupName('g1'),'Experiment A');
  revision='2';await h.groups.refresh();
  await assert.rejects(h.groups.assign(['scenario:one'],'g2'),/connection interrupted/);
  assert.equal(writes,2);
});

test('an older GET cannot overwrite a newer mutation response',async()=>{
  const stale=deferred();let reads=0;
  const h=harness(async(path,options)=>{
    if(options.method)return {...initial(),revision:'2',memberships:{'scenario:two':'g2'}};
    if(++reads===1)return initial();return stale.promise;
  });await h.initialized;
  const reading=h.groups.refresh();
  await h.groups.assign(['scenario:two'],'g2');
  stale.resolve(initial());await reading;
  h.groups.setFilter('g2');
  assert.equal(h.groups.matches('scenario:two'),true);
  assert.equal(h.groups.matches('run:single'),false);
});

test('overlapping writes are rejected while an existing move is in progress',async()=>{
  const write=deferred();let writes=0;
  const h=harness(async(path,options)=>{if(options.method){writes++;return write.promise;}return initial();});await h.initialized;
  const moving=h.groups.assign(['scenario:one'],'g2');
  await assert.rejects(h.groups.assign(['batch:one'],'g2'),/still in progress/);
  write.resolve({...initial(),revision:'2'});await moving;
  assert.equal(writes,1);
});

test('selection is scoped and Select visible honors the parent search filter',async()=>{
  let moved;const h=harness(async(path,options)=>{
    if(options.method){moved=JSON.parse(options.body);return {...initial(),revision:'2',memberships:{}};}
    return initial();
  },{getVisibleKeys:()=>['scenario:two','run:single','not-a-key']});await h.initialized;
  await h.action('select');
  assert.match(h.groups.selectionMarkup('scenario:one','One'),/type="checkbox"/);
  assert.equal(h.groups.selectionMarkup('batch:one','Batch'),'');
  await h.action('select-visible');
  assert.match(h.groups.selectionMarkup('scenario:two','Two'),/ checked/);
  assert.doesNotMatch(h.groups.selectionMarkup('scenario:one','One'),/ checked/);
  await h.action('move');
  assert.deepEqual(moved.keys,['scenario:two']);
  assert.doesNotMatch(h.groups.selectionMarkup('scenario:two','Two'),/ checked/);
});

test('changing the group filter clears selection; list updates drop deleted items',async()=>{
  const h=harness(async()=>initial());await h.initialized;await h.action('select');
  h.check('scenario:one',true);
  assert.match(h.groups.selectionMarkup('scenario:one','One'),/ checked/);
  h.groups.setFilter('g1');
  assert.doesNotMatch(h.groups.selectionMarkup('scenario:one','One'),/ checked/);
  h.check('scenario:one',true);h.items.splice(0,1);h.groups.refreshUI();
  assert.doesNotMatch(h.groups.selectionMarkup('scenario:one','One'),/ checked/);
});

test('selection labels and group badges escape user-controlled names',async()=>{
  const h=harness(async()=>({...initial(),groups:[{id:'g1',name:'<img onerror="x">'},{id:'g2',name:'B'}]}));await h.initialized;
  await h.action('select');
  assert.doesNotMatch(h.groups.badgeMarkup('scenario:one'),/<img/);
  assert.match(h.groups.badgeMarkup('scenario:one'),/&lt;img/);
  assert.match(h.groups.selectionMarkup('scenario:one','"<script>'),/&quot;&lt;script&gt;/);
});

test('batch selection clicks do not toggle the surrounding summary',async()=>{
  const h=harness(async()=>initial());await h.initialized;
  let stopped=false,prevented=false;
  const capture=h.listeners.get('click').find(item=>item.capture).handler;
  capture({target:{closest:()=>({})},stopPropagation(){stopped=true;},preventDefault(){prevented=true;}});
  assert.equal(stopped,true);assert.equal(prevented,false);
});

test('bulk moves enforce the server limit and never send an invalid target group',async()=>{
  let writes=0;const h=harness(async(path,options)=>{if(options.method)writes++;return initial();});await h.initialized;
  await assert.rejects(h.groups.assign(Array.from({length:2001},(_,i)=>'scenario:'+i),'g1'),/2,000/);
  await assert.rejects(h.groups.assign(['scenario:one'],'missing'),/no longer exists/);
  await h.groups.assign([],'g1');assert.equal(writes,0);
});

test('refreshUI never invokes the parent render callback recursively',async()=>{
  const h=harness(async()=>initial());await h.initialized;
  const before=h.changes();h.groups.refreshUI();h.groups.refreshUI();assert.equal(h.changes(),before);
});

test('unchanged snapshots do not rebuild the filter controls after DOM attribute normalization',async()=>{
  let replacements=0,markup='';
  const bar={contains:()=>false,querySelector:()=>null,
    get innerHTML(){return markup.replace(/ selected/g,' selected=""').replace(/ disabled/g,' disabled=""');},
    set innerHTML(value){markup=value;replacements++;}};
  const document={querySelector:selector=>selector==='#scenarioGroupTools'?bar:null,querySelectorAll:()=>[],addEventListener(){}};
  const groups=createLibraryGroups({document});
  await groups.init({api:async()=>initial(),getItems:()=>[{key:'scenario:one'}]});
  const before=replacements;
  groups.refreshUI();groups.refreshUI();await groups.refresh();
  assert.equal(replacements,before);
});
