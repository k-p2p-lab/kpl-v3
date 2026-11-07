const test=require('node:test');
const assert=require('node:assert/strict');
const {createAgentHosts,groupHosts,validStoredRequest}=require('./static/agent-hosts.js');
const turn=()=>new Promise(setImmediate);
const agent=(id='a',nodeId='node-a',bootId='boot-a')=>({id,name:'Agent '+id,hostname:'container-'+id,state:'online',startedAt:'2026-11-07T00:00:00Z',host:{nodeId,hostname:'Linux '+nodeId,bootId,rebootEnabled:true,rebootSupported:true}});
function job(payload,state='queued',hostState='queued'){
 return {id:payload.id,state,targets:payload.targets,hosts:payload.targets.map(target=>({...target,hostname:'Linux '+target.nodeId,state:hostState}))};
}
function fixture(handler,storage=new Map()){
 const elements=new Map(),listeners=new Map(),requests=[],timers=new Map(),inputs=[];let nextTimer=0,nextID=0;
 const element=id=>{if(!elements.has(id))elements.set(id,{id,textContent:'',innerHTML:'',hidden:false,disabled:false,open:false,checked:false,indeterminate:false,setAttribute(name,value){this[name]=value;},focus(){document.activeElement=this;},showModal(){this.open=true;},close(){this.open=false;},addEventListener(){}});return elements.get(id);};
 const ids=['selectAgentHosts','agentHostSelection','rebootAgentHosts','clearAgentHosts','viewHostReboot','agentHostStatus','agentHostDialog','agentHostTargets','agentHostDialogSummary','agentHostDialogStatus','confirmAgentHostReboot','closeAgentHostDialog','refreshHostReboot','agentHostDialogError'];
 ids.forEach(element);
 const document={querySelector:selector=>elements.get(selector.slice(1)),querySelectorAll:()=>inputs,addEventListener(type,handler){if(!listeners.has(type))listeners.set(type,[]);listeners.get(type).push(handler);}};
 const environment={document,crypto:{randomUUID:()=>String(++nextID).padStart(8,'0')+'-1122-4333-8444-555566667777'},sessionStorage:{getItem:key=>storage.get(key)||null,setItem:(key,value)=>storage.set(key,value)},setTimeout(callback,delay){const id=++nextTimer;timers.set(id,{callback,delay});return id;},clearTimeout:id=>timers.delete(id)};
 const api=async(path,options={})=>{requests.push({path,options});return handler?handler(path,options):job(JSON.parse(options.body));};
 const view=createAgentHosts(environment);view.init({api});
 const dispatch=(type,event)=>{for(const listener of listeners.get(type)||[])listener(event);};
 const action=id=>dispatch('click',{target:{closest:()=>element(id)},preventDefault(){}});
 const change=(nodeId,checked)=>dispatch('change',{target:{dataset:{hostSelect:nodeId},checked}});
 const master=checked=>{element('selectAgentHosts').checked=checked;dispatch('change',{target:element('selectAgentHosts')});};
 return {view,elements,element,requests,storage,document,inputs,action,change,master,timers,runPoll(){for(const [id,timer]of timers)if(timer.delay===3000){timers.delete(id);timer.callback();break;}}};
}

test('host identity groups duplicate Agents without using container hostnames or disable settings as identity',()=>{
 const first=agent(),second={...agent('b'),disabled:true};
 const groups=groupHosts([second,first,agent('c','node-c')]);
 assert.equal(groups.size,2);assert.equal(groups.get('node-a').agents.length,2);assert.equal(groups.get('node-a').agent.id,'a');
 assert.equal(groups.get('node-a').available,true);
 second.host.bootId='different-boot';assert.match(groupHosts([first,second]).get('node-a').reason,/boot identity changed/);
});

test('unavailable and unsupported hosts remain unselectable with an explicit reason',()=>{
 const values=[agent('offline','node-offline'),agent('disabled','node-disabled'),agent('pending','node-pending'),agent('old','node-old')];
 values[0].state='offline';values[1].host.rebootEnabled=false;values[2].host.rebootPending=true;values[3].host.rebootSupported=false;values[3].host.rebootReason='This manager host cannot be rebooted.';
 const h=fixture();h.view.update(values);
 for(const value of values){assert.match(h.view.checkbox(value),/ disabled/);h.change(value.host.nodeId,true);}
 assert.equal(h.element('agentHostSelection').textContent,'0 hosts selected');
 assert.equal(h.element('selectAgentHosts').disabled,true);
 assert.match(h.view.checkbox(values[3]),/This manager host cannot be rebooted/);
 assert.doesNotThrow(()=>groupHosts([{...agent(),startedAt:123}]));
});

test('an offline alias from an older boot cannot permanently block a recovered host',()=>{
 const current=agent('new-agent'),old=agent('old-agent');current.host.bootId='new-boot';old.state='offline';old.host.rebootPending=true;
 assert.equal(groupHosts([current,old]).get('node-a').available,true);
 old.host.bootId='new-boot';assert.match(groupHosts([current,old]).get('node-a').reason,/already pending/);
 old.state='online';old.host.bootId='conflicting-live-boot';assert.match(groupHosts([current,old]).get('node-a').reason,/boot identity changed/);
});

test('master selection counts each host once and preserves the mixed checkbox state',()=>{
 const h=fixture(),a=agent(),b=agent('b'),c=agent('c','node-c');h.view.update([a,b,c]);
 h.change('node-a',true);assert.equal(h.element('agentHostSelection').textContent,'1 host selected');
 assert.equal(h.element('selectAgentHosts').indeterminate,true);assert.equal(h.element('selectAgentHosts')['aria-checked'],'mixed');
 assert.match(h.view.checkbox(b),/ checked/);
 h.master(true);assert.equal(h.element('agentHostSelection').textContent,'2 hosts selected');assert.equal(h.element('selectAgentHosts').checked,true);
 h.master(false);assert.equal(h.element('agentHostSelection').textContent,'0 hosts selected');
 h.master(true);h.action('clearAgentHosts');assert.equal(h.element('agentHostSelection').textContent,'0 hosts selected');
});

test('confirmation shows escaped host identity and cancel never submits a reboot',()=>{
 const h=fixture(),value=agent();value.host.hostname='<img src=x onerror=bad>';value.host.nodeId='node-"<bad>';
 h.view.update([value]);h.change(value.host.nodeId,true);h.action('rebootAgentHosts');
 assert.equal(h.element('agentHostDialog').open,true);assert.equal(h.document.activeElement.id,'closeAgentHostDialog');
 assert.match(h.element('agentHostTargets').innerHTML,/&lt;img src=x onerror=bad&gt;/);assert.doesNotMatch(h.element('agentHostTargets').innerHTML,/<img|<bad>/);
 assert.equal(h.requests.length,0);h.action('closeAgentHostDialog');assert.equal(h.element('agentHostDialog').open,false);assert.equal(h.requests.length,0);assert.equal(h.document.activeElement.id,'rebootAgentHosts');
});

test('one confirmed request freezes target identity, deduplicates hosts and blocks double clicks',async()=>{
 let resolve;const h=fixture((path,options)=>new Promise(yes=>resolve=()=>yes(job(JSON.parse(options.body)))));
 h.view.update([agent(),agent('b'),agent('c','node-c')]);h.master(true);h.action('rebootAgentHosts');
 h.action('confirmAgentHostReboot');h.action('confirmAgentHostReboot');
 assert.equal(h.requests.length,1);const request=h.requests[0];
 assert.equal(request.path,'/api/v1/agent-hosts/reboot');assert.equal(request.options.method,'POST');
 assert.equal(request.options.headers['Content-Type'],'application/json');
 const payload=JSON.parse(request.options.body);assert.equal(payload.confirm,true);assert.equal(payload.targets.length,2);
 assert.deepEqual(payload.targets[0],{agentId:'a',nodeId:'node-a',bootId:'boot-a',agentStartedAt:'2026-11-07T00:00:00Z'});
 assert.equal(h.element('closeAgentHostDialog').disabled,true);
 assert.equal(validStoredRequest(JSON.parse([...h.storage.values()][0])),true);
 resolve();await turn();assert.equal(h.element('confirmAgentHostReboot').hidden,true);
 assert.equal(h.element('agentHostSelection').textContent,'0 hosts selected');assert.match(h.element('agentHostDialogStatus').textContent,/Queued behind/);
});

test('lost replies only retry the identical operation, target boot and Agent instance',async()=>{
 let calls=0;const h=fixture((path,options)=>{if(++calls===1)throw Error('connection lost');return job(JSON.parse(options.body));});
 h.view.update([agent()]);h.master(true);h.action('rebootAgentHosts');h.action('confirmAgentHostReboot');await turn();
 const original=h.requests[0].options.body;assert.match(h.element('agentHostDialogStatus').textContent,/exact request/);
 assert.equal(h.element('confirmAgentHostReboot').textContent,'Retry same request');
 const changed=agent();changed.startedAt='2026-11-07T01:00:00Z';changed.host.bootId='new-boot';h.view.update([changed]);
 h.action('confirmAgentHostReboot');await turn();assert.equal(h.requests.length,2);assert.equal(h.requests[1].options.body,original);
});

test('Agent instance timestamps retain Go nanosecond precision in the reboot proof',async()=>{
 const h=fixture(),value=agent();value.startedAt='2026-11-07T00:00:00.123456789Z';
 h.view.update([value]);h.master(true);h.action('rebootAgentHosts');h.action('confirmAgentHostReboot');await turn();
 assert.equal(JSON.parse(h.requests[0].options.body).targets[0].agentStartedAt,value.startedAt);
});

test('reload refreshes the saved request without automatically submitting it again',async()=>{
 const storage=new Map(),first=fixture(()=>{throw Error('connection lost');},storage);
 first.view.update([agent()]);first.master(true);first.action('rebootAgentHosts');first.action('confirmAgentHostReboot');await turn();
 const before=first.requests[0].options.body;
 const h=fixture((path,options)=>{if(options.method==='POST')return job(JSON.parse(options.body));const error=Error('not found');error.status=404;throw error;},storage);
 await turn();assert.equal(h.requests.length,1);assert.equal(h.requests[0].options.method,undefined);
 assert.match(h.requests[0].path,/\/api\/v1\/agent-hosts\/reboot\/host-reboot-/);
 h.action('viewHostReboot');await turn();h.action('confirmAgentHostReboot');await turn();
 const posts=h.requests.filter(request=>request.options.method==='POST');assert.equal(posts.length,1);assert.equal(posts[0].options.body,before);
});

test('explicit pre-admission rejection releases the request without claiming an unknown reboot',async()=>{
 const h=fixture(()=>{const error=Error('Host reboot not enabled');error.status=422;error.accepted=false;throw error;});
 h.view.update([agent()]);h.master(true);h.action('rebootAgentHosts');h.action('confirmAgentHostReboot');await turn();
 assert.match(h.element('agentHostStatus').textContent,/not admitted/);assert.equal(h.element('rebootAgentHosts').disabled,false);
 const original=JSON.parse(h.requests[0].options.body).id;h.action('confirmAgentHostReboot');await turn();
 assert.notEqual(JSON.parse(h.requests[1].options.body).id,original);
});

test('queued polling never resends a reboot and requested is distinct from confirmed',async()=>{
 let payload,polls=0;const h=fixture((path,options)=>{
  if(options.method==='POST'){payload=JSON.parse(options.body);return job(payload);}
  if(++polls===1)return job(payload,'running','requested');
  return job(payload,'failed','unknown');
 });
 h.view.update([agent()]);h.master(true);h.action('rebootAgentHosts');h.action('confirmAgentHostReboot');await turn();
 h.runPoll();await turn();assert.match(h.element('agentHostTargets').innerHTML,/Reboot requested/);assert.doesNotMatch(h.element('agentHostTargets').innerHTML,/Reboot confirmed/);
 h.runPoll();await turn();assert.match(h.element('agentHostTargets').innerHTML,/Outcome unknown/);
 assert.equal(h.requests.filter(request=>request.options.method==='POST').length,1);
 h.view.update([agent()]);assert.match(h.view.checkbox(agent()),/ disabled/);
 const changed=agent();changed.host.bootId='new-boot';h.view.update([changed]);assert.doesNotMatch(h.view.checkbox(changed),/ disabled/);
});

test('changed Agent or boot identity cannot silently alter selected targets',()=>{
 const h=fixture(),value=agent();h.view.update([value]);h.master(true);
 value.host.bootId='new-boot';h.view.update([value]);assert.equal(h.element('rebootAgentHosts').disabled,true);
 h.action('clearAgentHosts');h.master(true);assert.equal(h.element('rebootAgentHosts').disabled,false);
});

test('checkbox focus and table scroll survive Agent row replacement',()=>{
 const h=fixture(),value=agent(),table={scrollLeft:240,scrollTop:50};h.view.update([value]);
 const before={dataset:{hostSelect:'node-a',hostAgent:'a'},closest:()=>table};h.document.activeElement=before;h.view.beforeRender();
 const after={dataset:{hostSelect:'node-a',hostAgent:'a'},focus(){h.document.activeElement=this;table.scrollLeft=0;table.scrollTop=0;}};h.inputs.push(after);
 h.view.update([{...value,activeNodes:4}]);h.view.afterRender();
 assert.equal(h.document.activeElement,after);assert.equal(table.scrollLeft,240);assert.equal(table.scrollTop,50);
});

test('queue View can inspect another tab’s immutable operation and result errors are escaped',async()=>{
 const payload={id:'host-reboot-other-tab',targets:[{agentId:'a',nodeId:'node-a',bootId:'boot-a',agentStartedAt:'2026-11-07T00:00:00Z'}],confirm:true};
 const response=job(payload,'failed','rejected');response.hosts[0].error='<script>bad()</script>';
 const h=fixture(()=>response);await h.view.open(payload.id);
 assert.equal(h.element('agentHostDialog').open,true);assert.match(h.element('agentHostTargets').innerHTML,/&lt;script&gt;bad\(\)&lt;\/script&gt;/);
 assert.equal(h.element('confirmAgentHostReboot').hidden,true);assert.equal(h.requests[0].options.method,undefined);
});

test('inspecting another operation cannot overwrite an uncertain submission or enable its replay',async()=>{
 const otherPayload={id:'host-reboot-other-tab',targets:[{agentId:'other',nodeId:'other-node',bootId:'other-boot',agentStartedAt:'2026-11-07T00:00:00Z'}],confirm:true};
 let posts=0;const h=fixture((path,options)=>{
  if(options.method==='POST'){if(++posts===1)throw Error('response lost');return job(JSON.parse(options.body));}
  if(path.endsWith(otherPayload.id))return job(otherPayload,'completed','rebooted');
  const error=Error('not found');error.status=404;throw error;
 });
 h.view.update([agent()]);h.master(true);h.action('rebootAgentHosts');h.action('confirmAgentHostReboot');await turn();
 const original=h.requests[0].options.body,stored=[...h.storage.values()][0];
 await h.view.openJob(otherPayload.id);assert.equal([...h.storage.values()][0],stored);assert.equal(h.element('confirmAgentHostReboot').hidden,true);
 h.action('confirmAgentHostReboot');await turn();assert.equal(posts,1);
 h.action('closeAgentHostDialog');h.action('viewHostReboot');await turn();
 assert.equal(h.element('confirmAgentHostReboot').textContent,'Retry same request');h.action('confirmAgentHostReboot');await turn();
 const actual=h.requests.filter(request=>request.options.method==='POST');assert.equal(actual.length,2);assert.equal(actual[1].options.body,original);
});

test('readonly queue inspection does not create a durable reboot submission',async()=>{
 const payload={id:'host-reboot-readonly',targets:[],confirm:true};
 const h=fixture(()=>job(payload,'completed','rebooted'));await h.view.openJob(payload.id);
 assert.equal(h.storage.size,0);assert.equal(h.element('confirmAgentHostReboot').hidden,true);
 h.action('confirmAgentHostReboot');await turn();assert.equal(h.requests.filter(request=>request.options.method==='POST').length,0);
});

test('failed and interrupted operation errors remain visible as plain text',async()=>{
 for(const state of ['failed','interrupted']){
  const payload={id:'host-reboot-'+state,targets:[],confirm:true};
  const result=job(payload,state,'rejected');result.error='Controller <img src=x onerror=bad> restarted; no request will be replayed.';
  const h=fixture(()=>result);await h.view.openJob(payload.id);
  assert.ok(h.element('agentHostDialogStatus').textContent.includes(result.error));
  assert.equal(h.element('agentHostDialogStatus').innerHTML,'');
  assert.equal(h.element('confirmAgentHostReboot').hidden,true);
 }
});

test('invalid stored operations and an absent host UI never submit or throw',()=>{
 for(const value of [null,{version:2},{version:1,payload:{id:'bad',targets:[],confirm:true}}])assert.equal(validStoredRequest(value),false);
 const view=createAgentHosts({document:{querySelector:()=>null,querySelectorAll:()=>[],addEventListener(){}},sessionStorage:{getItem:()=>'{broken'}});
 assert.doesNotThrow(()=>{view.init({api:()=>{throw Error('unexpected request');}});view.update([]);view.beforeRender();view.afterRender();});
});
