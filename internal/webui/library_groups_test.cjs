const test = require('node:test');
const assert = require('node:assert/strict');
const {createLibraryGroups} = require('./static/library-groups.js');

const initial = () => ({revision:'1', groups:[{id:'g1',name:'Experiment A'},{id:'g2',name:'Experiment B'}], memberships:{'scenario:one':'g1','batch:one':'g1','run:single':'g2'}});
const nextTurn = () => new Promise(setImmediate);
const deferred = () => { let resolve,reject; const promise=new Promise((yes,no)=>{resolve=yes;reject=no;});return {promise,resolve,reject}; };

function harness(api, configuration={}) {
  const listeners = new Map();
  const document = {querySelector:selector=>configuration.bars?.[selector]||null,querySelectorAll:()=>[],addEventListener:(type,handler,capture)=>{
    const entries=listeners.get(type)||[];entries.push({handler,capture});listeners.set(type,entries);
  }};
  let changes=0;
  const groups=createLibraryGroups({document,KPLActionMenus:configuration.actionMenus});
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
  function master(scope, checked) {
    const element={checked,dataset:{libraryScope:scope},matches:selector=>selector==='[data-library-select-visible]'};
    for(const entry of listeners.get('change')||[])entry.handler({target:element});
    return element.checked;
  }
  function filter(id, checked) {
    const element={checked,dataset:{libraryGroupFilter:id},matches:selector=>selector==='[data-library-group-filter]'};
    for(const entry of listeners.get('change')||[])entry.handler({target:element});
  }
  return {groups,initialized,items,action,check,master,filter,changes:()=>changes,listeners};
}

test('visible item checkboxes start selection immediately and the final uncheck restores the editor',async()=>{
  let changes=0;
  const h=harness(async()=>initial(),{onSelectionChanged:()=>changes++});await h.initialized;
  assert.match(h.groups.selectionMarkup('scenario:one','One'),/type="checkbox"/);
  h.check('scenario:one',true);
  assert.deepEqual(h.groups.getSelection('scenarios'),['scenario:one']);
  h.check('batch:one',true);
  assert.equal(h.groups.getSelection('scenarios'),null);
  assert.deepEqual(h.groups.getSelection('results'),['batch:one']);
  assert.doesNotMatch(h.groups.selectionMarkup('scenario:one','One'),/ checked/);
  h.check('batch:one',false);
  assert.equal(h.groups.getSelection('results'),null);
  assert.equal(changes,3);
});

test('master checkbox reflects partial selection and toggles only visible search matches',async()=>{
  const bar={innerHTML:'',contains:()=>false,querySelector:()=>null};
  let visible=['scenario:one','scenario:two'];
  const h=harness(async()=>initial(),{bars:{'#scenarioGroupTools':bar},getVisibleKeys:()=>visible});await h.initialized;
  h.check('scenario:one',true);
  assert.match(bar.innerHTML,/data-library-select-visible[^>]*data-library-mixed="true"/);
  h.master('scenarios',true);
  assert.deepEqual(h.groups.getSelection('scenarios'),['scenario:one','scenario:two']);
  assert.match(bar.innerHTML,/data-library-select-visible[^>]* checked/);
  visible=['scenario:two'];h.groups.refreshUI();
  assert.match(bar.innerHTML,/2 selected · 1 hidden/);
  h.master('scenarios',false);
  assert.deepEqual(h.groups.getSelection('scenarios'),['scenario:one']);
  assert.doesNotMatch(bar.innerHTML,/data-library-select-visible[^>]* checked/);
  h.master('scenarios',true);
  assert.deepEqual(h.groups.getSelection('scenarios'),['scenario:one','scenario:two']);
  await h.action('clear');
  assert.equal(h.groups.getSelection('scenarios'),null);
});

test('group checkbox menu combines groups and Ungrouped and clears hidden selections',async()=>{
  const bars=Object.fromEntries(['#scenarioGroupTools','#resultGroupTools'].map(id=>[id,{innerHTML:'',contains:()=>false,querySelector:()=>null}]));
  const h=harness(async()=>initial(),{bars});await h.initialized;
  h.check('scenario:one',true);
  h.filter('g2',false); // All groups minus Experiment B
  assert.equal(h.groups.getSelection('scenarios'),null);
  assert.equal(h.groups.matches('scenario:one'),true);
  assert.equal(h.groups.matches('scenario:two'),true);
  assert.equal(h.groups.matches('run:single'),false);
  assert.equal(h.groups.getImportGroup(),'');
  assert.match(bars['#resultGroupTools'].innerHTML,/data-library-group-filter="\*"[^>]*data-library-mixed/);
  h.filter('',false);
  assert.equal(h.groups.getImportGroup(),'g1');
  assert.equal(h.groups.matches('scenario:two'),false);
  h.filter('g2',true);
  assert.equal(h.groups.matches('run:single'),true);
  assert.equal(h.groups.getImportGroup(),'');
  h.filter('*',false);
  assert.equal(h.groups.matches('scenario:one'),false);
  assert.match(bars['#scenarioGroupTools'].innerHTML,/>No groups</);
  h.filter('*',true);
  assert.equal(h.groups.isFiltered(),false);
});

test('checkbox selection honors busy state, canonical keys and the 2000-item limit',async()=>{
  let busy=false;
  const h=harness(async()=>initial(),{isExternallyBusy:()=>busy});await h.initialized;
  h.check('scenario:one',true);busy=true;
  h.master('results',true);h.filter('g1',false);h.check('scenario:two',true);
  assert.deepEqual(h.groups.getSelection('scenarios'),['scenario:one']);
  assert.equal(h.groups.isFiltered(),false);
  busy=false;h.check('scenario:forged',true);
  assert.deepEqual(h.groups.getSelection('scenarios'),['scenario:one']);
  h.items.push(...Array.from({length:2000},(_,i)=>({key:'scenario:extra-'+i})));
  h.master('scenarios',true);
  assert.deepEqual(h.groups.getSelection('scenarios'),['scenario:one']);
  h.items.splice(0,h.items.length,...h.items.filter(item=>item.key.startsWith('scenario:')).slice(0,2000));h.master('scenarios',true);
  assert.equal(h.groups.getSelection('scenarios').length,2000);
});

test('selection Actions opens deletion confirmation for exactly the checked units',async()=>{
  const requests=[];let locked=false;
  const h=harness(async()=>initial(),{onDeleteSelected:(scope,keys)=>requests.push({scope,keys}),isExternallyBusy:()=>locked});await h.initialized;
  h.check('scenario:one',true);h.check('scenario:two',true);
  await h.action('delete-selected');
  assert.deepEqual(requests,[{scope:'scenarios',keys:['scenario:one','scenario:two']}]);
  locked=true;await h.action('delete-selected');assert.equal(requests.length,1);
  locked=false;h.groups.forgetSelection(['scenario:one']);
  assert.deepEqual(h.groups.getSelection('scenarios'),['scenario:two']);
});

test('scenario execution reads a selection snapshot and updates on individual checkbox changes',async()=>{
  let changes=0;
  const h=harness(async()=>initial(),{onSelectionChanged:()=>changes++});await h.initialized;
  assert.equal(h.groups.getSelection('scenarios'),null);

  assert.equal(h.groups.getSelection('scenarios'),null);
  h.check('scenario:one',true);
  const frozen=h.groups.getSelection('scenarios');
  h.check('scenario:two',true);
  assert.deepEqual(frozen,['scenario:one']);
  assert.deepEqual(h.groups.getSelection('scenarios'),['scenario:one','scenario:two']);
  assert.equal(h.groups.getSelection('results'),null);
  assert.equal(changes,2);
});

test('registration locks group mutations, filters, downloads and selection until it finishes',async()=>{
  let registering=false,downloads=0,writes=0;
  const h=harness(async(path,options)=>{if(options?.method)writes++;return initial();},
    {isExternallyBusy:()=>registering,download:async()=>downloads++});await h.initialized;
  h.check('scenario:one',true);
  registering=true;h.groups.refreshUI();
  assert.equal(h.groups.isBusy(),true);
  assert.match(h.groups.selectionMarkup('scenario:one','One'),/ disabled/);
  assert.equal(h.check('scenario:one',false),true);
  assert.equal(h.check('scenario:two',true),false);
  await h.action('clear');h.master('scenarios',true);await h.action('download');
  h.groups.setFilter('g2');
  assert.equal(h.groups.isFiltered(),false);
  assert.deepEqual(h.groups.getSelection('scenarios'),['scenario:one']);
  await assert.rejects(h.groups.assign(['scenario:one'],'g2'));
  assert.equal(writes,0);assert.equal(downloads,0);
  registering=false;
  assert.equal(h.groups.isBusy(),false);
  await h.action('clear');assert.equal(h.groups.getSelection('scenarios'),null);
});

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

test('fresh refresh reads memberships after an older pending GET completes',async()=>{
  const stale=deferred();let reads=0;
  const updated={...initial(),revision:'3',memberships:{...initial().memberships,'batch:new':'g1'}};
  const h=harness(async()=>{
    if(++reads===1)return initial();
    if(reads===2)return stale.promise;
    return updated;
  });await h.initialized;
  h.groups.setFilter('g1');
  const changes=h.changes();
  const older=h.groups.refresh();
  const fresh=h.groups.refresh({fresh:true});
  const duplicate=h.groups.refresh({fresh:true});
  stale.resolve({...initial(),revision:'2',memberships:{'scenario:one':'g2'}});
  await Promise.all([older,fresh,duplicate]);
  assert.equal(reads,3,'fresh callers should share one new read after the stale request');
  assert.equal(h.changes(),changes+1,'the stale snapshot must not be adopted');
  assert.equal(h.groups.matches('scenario:one'),true);
  assert.equal(h.groups.matches('batch:new'),true);
});

test('fresh refresh proceeds after a failed stale read and a pending local mutation',async()=>{
  const stale=deferred(),write=deferred();let reads=0;
  const updated={...initial(),revision:'3',memberships:{'scenario:one':'g2','batch:new':'g1'}};
  const h=harness(async(path,options)=>{
    if(options.method)return write.promise;
    if(++reads===1)return initial();
    if(reads===2)return stale.promise;
    return updated;
  });await h.initialized;
  const older=h.groups.refresh();
  const rejected=assert.rejects(older,/old read failed/);
  const moving=h.groups.assign(['scenario:one'],'g2');
  const fresh=h.groups.refresh({fresh:true});
  stale.reject(new Error('old read failed'));
  await rejected;await nextTurn();
  assert.equal(reads,2,'the new read must wait for the mutation response');
  write.resolve({...initial(),revision:'2',memberships:{'scenario:one':'g2'}});
  await Promise.all([moving,fresh]);
  assert.equal(reads,3);
  h.groups.setFilter('g1');assert.equal(h.groups.matches('batch:new'),true);
  h.groups.setFilter('g2');assert.equal(h.groups.matches('scenario:one'),true);
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

  assert.match(h.groups.selectionMarkup('scenario:one','One'),/type="checkbox"/);
  assert.match(h.groups.selectionMarkup('batch:one','Batch'),/type="checkbox"/);
  assert.doesNotMatch(h.groups.selectionMarkup('batch:one','Batch'),/ checked/);
  h.master('scenarios',true);
  assert.match(h.groups.selectionMarkup('scenario:two','Two'),/ checked/);
  assert.doesNotMatch(h.groups.selectionMarkup('scenario:one','One'),/ checked/);
  await h.action('move');
  assert.deepEqual(moved.keys,['scenario:two']);
  assert.doesNotMatch(h.groups.selectionMarkup('scenario:two','Two'),/ checked/);
});

test('changing the group filter clears selection; list updates drop deleted items',async()=>{
  const h=harness(async()=>initial());await h.initialized;
  h.check('scenario:one',true);
  assert.match(h.groups.selectionMarkup('scenario:one','One'),/ checked/);
  h.groups.setFilter('g1');
  assert.doesNotMatch(h.groups.selectionMarkup('scenario:one','One'),/ checked/);
  h.check('scenario:one',true);h.items.splice(0,1);h.groups.refreshUI();
  assert.doesNotMatch(h.groups.selectionMarkup('scenario:one','One'),/ checked/);
});

test('selection labels and group badges escape user-controlled names',async()=>{
  const h=harness(async()=>({...initial(),groups:[{id:'g1',name:'<img onerror="x">'},{id:'g2',name:'B'}]}));await h.initialized;

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
  stopped=false;
  capture({target:{closest:selector=>selector==='[data-library-select]'?{}:null},stopPropagation(){stopped=true;}});
  assert.equal(stopped,false,'ordinary checkbox clicks must reach outside-menu dismissal');
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

test('selection downloads preserve selection and suppress duplicate requests while preparing', async()=>{
  const pending=deferred(),downloads=[];
  const h=harness(async()=>initial(),{download:async(scope,keys)=>{downloads.push({scope,keys});return pending.promise}});
  await h.initialized;

  h.check('batch:one',true);
  await h.action('download','results');
  await h.action('download','results');
  assert.deepEqual(downloads,[{scope:'results',keys:['batch:one']}]);
  pending.resolve();await nextTurn();
  assert.match(h.groups.selectionMarkup('batch:one','One'),/ checked/);
  assert.doesNotMatch(h.groups.selectionMarkup('batch:one','One'),/ disabled/);
});

test('failed download preparation restores controls and leaves the selection retryable',async()=>{
  let attempts=0;
  const h=harness(async()=>initial(),{download:async()=>{attempts++;throw new Error('source unavailable')}});
  await h.initialized;h.check('scenario:one',true);
  await h.action('download');await h.action('download');
  assert.equal(attempts,2);
  assert.match(h.groups.selectionMarkup('scenario:one','One'),/ checked/);
  assert.doesNotMatch(h.groups.selectionMarkup('scenario:one','One'),/ disabled/);
});

test('CSV ZIP is limited to Saved Results selection and reports progress without changing selection',async()=>{
  const pending=deferred(),downloads=[];
  const bars=Object.fromEntries(['#scenarioGroupTools','#resultGroupTools'].map(id=>[id,{innerHTML:'',contains:()=>false,querySelector:()=>null}]));
  const h=harness(async()=>initial(),{bars,download:async()=>{},downloadCSV:(scope,keys,options)=>{downloads.push({scope,keys,options});return pending.promise}});
  await h.initialized;h.check('scenario:one',true);
  assert.doesNotMatch(bars['#scenarioGroupTools'].innerHTML,/download-csv/);
  await h.action('download-csv','scenarios');assert.equal(downloads.length,0);
  h.check('batch:one',true);
  assert.match(bars['#resultGroupTools'].innerHTML,/data-library-action="download-csv"[^>]*>CSV ZIP/);
  await h.action('download-csv','results');
  assert.equal(downloads.length,1);
  assert.deepEqual(downloads[0].keys,['batch:one']);
  downloads[0].options.onProgress('CSV ZIP 1/2 · <unsafe>');
  assert.match(bars['#resultGroupTools'].innerHTML,/CSV ZIP 1\/2 · &lt;unsafe&gt;/);
  assert.match(bars['#resultGroupTools'].innerHTML,/data-library-action="cancel-download"/);
  await h.action('download-csv','results');await h.action('clear','results');
  assert.equal(downloads.length,1);
  assert.match(h.groups.selectionMarkup('batch:one','One'),/ checked/);
  pending.resolve();await nextTurn();
  assert.match(bars['#resultGroupTools'].innerHTML,/Download requested/);
  assert.doesNotMatch(bars['#resultGroupTools'].innerHTML,/cancel-download/);
  assert.doesNotMatch(h.groups.selectionMarkup('batch:one','One'),/ disabled/);
});

test('canceling CSV ZIP unlocks selection and leaves accepted server analysis running',async()=>{
  let signal;
  const bar={innerHTML:'',contains:()=>false,querySelector:()=>null};
  const h=harness(async()=>initial(),{bars:{'#resultGroupTools':bar},downloadCSV:(scope,keys,options)=>{
    signal=options.signal;
    return new Promise((resolve,reject)=>signal.addEventListener('abort',()=>reject(new DOMException('Canceled','AbortError'))));
  }});
  await h.initialized;h.check('run:single',true);
  await h.action('download-csv','results');
  assert.equal(signal.aborted,false);
  await h.action('cancel-download','results');
  assert.equal(signal.aborted,true);
  assert.match(bar.innerHTML,/CSV download canceled.*continues on the server/);
  assert.match(h.groups.selectionMarkup('run:single','Single'),/ checked/);
  assert.doesNotMatch(h.groups.selectionMarkup('run:single','Single'),/ disabled/);
});

test('CSV export errors keep selected results retryable and preserve original ZIP action',async()=>{
  let csvAttempts=0,original=0;
  const h=harness(async()=>initial(),{download:async()=>{original++},downloadCSV:async()=>{csvAttempts++;throw new Error('analysis unavailable')}});
  await h.initialized;h.check('batch:one',true);
  await h.action('download-csv','results');await h.action('download-csv','results');
  assert.equal(csvAttempts,2);
  await h.action('download','results');assert.equal(original,1);
  assert.match(h.groups.selectionMarkup('batch:one','One'),/ checked/);
});

test('one Download dropdown offers the three result formats and only YAML for scenarios',async()=>{
  const bars=Object.fromEntries(['#scenarioGroupTools','#resultGroupTools'].map(id=>[id,{innerHTML:'',contains:()=>false,querySelector:()=>null}]));
  let refreshes=0;
  const h=harness(async()=>initial(),{bars,download:async()=>{},downloadCSV:async()=>{},downloadJSON:async()=>{},actionMenus:{refresh(){refreshes++}}});
  await h.initialized;
  let html=bars['#resultGroupTools'].innerHTML;
  assert.doesNotMatch(html,/data-action-menu-toggle="library-download:results"/);
  h.check('batch:one',true);html=bars['#resultGroupTools'].innerHTML;
  assert.match(html,/class="action-menu" data-action-menu="library-download:results"/);
  assert.equal((html.match(/data-action-menu-toggle="library-download:/g)||[]).length,1);
  assert.match(html,/data-action-menu-toggle="library-download:results" aria-expanded="false">Actions/);
  assert.match(html,/class="action-menu-items">[\s\S]*>Results ZIP<\/button>[\s\S]*>CSV ZIP<\/button>[\s\S]*>Analysis JSON ZIP<\/button>/);
  h.check('scenario:one',true);html=bars['#scenarioGroupTools'].innerHTML;
  assert.match(html,/data-action-menu-toggle="library-download:scenarios"/);
  assert.match(html,/>Scenarios ZIP<\/button>/);
  assert.doesNotMatch(html,/download-json|download-csv/);
  assert.ok(refreshes>0,'dynamic dropdown markup must refresh the shared menu controller');
});

test('JSON menu export supports progress and cancel, blocks all other formats and restores trigger focus',async()=>{
  const calls=[];let cancelFocus=0,triggerFocus=0;
  const bar={innerHTML:'',contains:()=>false,querySelector:selector=> /cancel-download|action-menu-toggle/.test(selector) ? {focus(){if(selector.includes('cancel-download'))cancelFocus++;else if(selector.includes('action-menu-toggle'))triggerFocus++}} : null};
  const h=harness(async()=>initial(),{bars:{'#resultGroupTools':bar},download:async()=>{calls.push('raw')},downloadCSV:async()=>{calls.push('csv')},downloadJSON:(scope,keys,options)=>{
    calls.push({scope,keys,options});
    return new Promise((resolve,reject)=>options.signal.addEventListener('abort',()=>reject(new DOMException('Canceled','AbortError'))));
  }});
  await h.initialized;h.check('scenario:one',true);
  await h.action('download-json','scenarios');assert.equal(calls.length,0);
  h.check('batch:one',true);
  await h.action('download-json','results');assert.equal(calls.length,1);
  assert.deepEqual(calls[0].keys,['batch:one']);assert.equal(calls[0].scope,'results');
  assert.equal(cancelFocus,1);
  calls[0].options.onProgress('JSON ZIP 1/2 · <unsafe>');
  assert.match(bar.innerHTML,/JSON ZIP 1\/2 · &lt;unsafe&gt;/);
  assert.match(bar.innerHTML,/data-action-menu-toggle="library-download:results"[^>]* disabled/);
  await h.action('download-json','results');await h.action('download-csv','results');await h.action('download','results');await h.action('clear','results');
  assert.equal(calls.length,1);assert.match(h.groups.selectionMarkup('batch:one','One'),/ checked/);
  await h.action('cancel-download','results');
  assert.equal(calls[0].options.signal.aborted,true);
  assert.match(bar.innerHTML,/JSON download canceled.*continues on the server/);
  assert.doesNotMatch(bar.innerHTML,/data-action-menu-toggle="library-download:results"[^>]* disabled/);
  assert.equal(triggerFocus,1);
  assert.match(h.groups.selectionMarkup('batch:one','One'),/ checked/);
});

test('JSON failure keeps selection retryable and leaves the existing download formats available',async()=>{
  let failures=0,csv=0,raw=0;
  const h=harness(async()=>initial(),{download:async()=>raw++,downloadCSV:async()=>csv++,downloadJSON:async()=>{failures++;throw new Error('analysis unavailable')}});
  await h.initialized;h.check('run:single',true);
  await h.action('download-json','results');await h.action('download-json','results');
  await h.action('download-csv','results');await h.action('download','results');
  assert.equal(failures,2);assert.equal(csv,1);assert.equal(raw,1);
  assert.match(h.groups.selectionMarkup('run:single','Single'),/ checked/);
});

test('selected result actions share the dropdown while scenario controls remain unchanged',async()=>{
  const resultBar={innerHTML:'',contains:()=>false,querySelector:()=>null};
  const scenarioBar={innerHTML:'',contains:()=>false,querySelector:()=>null};
  const h=harness(async()=>initial(),{bars:{'#resultGroupTools':resultBar,'#scenarioGroupTools':scenarioBar},
    onAnalyzeSelected:async()=>'',onRetrySelected:async()=>''});
  await h.initialized;h.check('scenario:one',true);
  assert.doesNotMatch(scenarioBar.innerHTML,/Start analysis|Retry failed runs/);
  h.check('batch:one',true);
  assert.match(resultBar.innerHTML,/data-library-action="analyze-selected"[^>]*>Start analysis/);
  assert.match(resultBar.innerHTML,/data-library-action="retry-selected"[^>]*>Retry failed runs/);
});

test('bulk work freezes selected keys, locks repeat clicks, and preserves the selection after submission',async()=>{
  const gate=deferred(),calls=[];
  const bar={innerHTML:'',contains:()=>false,querySelector:()=>null};
  const h=harness(async()=>initial(),{bars:{'#resultGroupTools':bar},onAnalyzeSelected:async keys=>{
    calls.push([...keys]);await gate.promise;return 'Queued 2 analyses · 1 skipped.';
  },onRetrySelected:async()=>{throw Error('second action must stay locked')}});
  await h.initialized;h.check('batch:one',true);h.check('run:single',true);
  await h.action('analyze-selected','results');
  assert.equal(h.groups.isBusy(),true);
  await h.action('analyze-selected','results');await h.action('retry-selected','results');
  await h.action('clear','results');
  assert.equal(calls.length,1);
  assert.deepEqual(calls[0],['batch:one','run:single']);
  assert.deepEqual(h.groups.getSelection('results'),['batch:one','run:single']);
  gate.resolve();await nextTurn();await nextTurn();
  assert.equal(h.groups.isBusy(),false);
  assert.match(bar.innerHTML,/Queued 2 analyses · 1 skipped/);
  assert.deepEqual(h.groups.getSelection('results'),['batch:one','run:single']);
});

test('bulk work failures retain selection and show plain escaped feedback without automatic retry',async()=>{
  let calls=0;const bar={innerHTML:'',contains:()=>false,querySelector:()=>null};
  const h=harness(async()=>initial(),{bars:{'#resultGroupTools':bar},onRetrySelected:async()=>{
    calls++;throw Error('<script>Outcome unknown</script>');
  }});
  await h.initialized;h.check('run:single',true);await h.action('retry-selected','results');
  assert.equal(calls,1);assert.equal(h.groups.isBusy(),false);
  assert.deepEqual(h.groups.getSelection('results'),['run:single']);
  assert.match(bar.innerHTML,/&lt;script&gt;Outcome unknown&lt;\/script&gt;/);
  assert.doesNotMatch(bar.innerHTML,/<script>/);
});
