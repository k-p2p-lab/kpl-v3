const test=require('node:test'),assert=require('node:assert/strict');
const fs=require('node:fs'),path=require('node:path'),vm=require('node:vm');
const resources=require('./static/agent-resources.js');
const now=Date.parse('2026-09-28T00:00:00Z');
const agent=(id='one',cores=1.5)=>({id,state:'online',resources:{sampledAt:new Date(now).toISOString(),cpuCores:cores,cpuCapacityCores:8,memoryUsageBytes:4096,memoryWorkingSetBytes:3072,containers:4,measuredContainers:4,complete:true}});
test('KPL CPU uses the whole host as 100% and shows memory working set',()=>{
 const a=agent();const d=resources.describe(a,now);
 assert.equal(d.cpu,'18.8%');assert.equal(d.memory,'3 KiB');assert.match(d.detail,/4\/4 containers/);assert.match(d.title,/4 KiB/);
 assert.match(resources.cell(a,'cpu',now),/18.8%/);
 a.resources.error='<script>';a.resources.complete=false;
 assert.doesNotMatch(resources.cell(a,'cpu',now),/<script>/);
});
test('KPL totals include partial values and exclude offline, stale and missing samples',()=>{
 const online=agent(),other=agent('two',2),offline={...agent('offline',99),state:'offline'},stale=agent('stale'),partial=agent('partial');
 stale.resources.sampledAt=new Date(now-31000).toISOString();partial.resources.measuredContainers=3;partial.resources.complete=false;
 const result=resources.aggregate([online,other,offline,stale,partial,{id:'old',state:'online'}],now);
 assert.deepEqual(result,{cpuPercent:5/24*100,cpuCores:5,cpuCapacityCores:24,cpuMeasured:3,memoryUsageBytes:12288,memoryWorkingSetBytes:9216,measured:3,total:6,partial:1,containers:12,measuredContainers:11});
 assert.equal(resources.describe(offline,now).cpu,'N/A');assert.match(resources.describe(stale,now).detail,/Stale/);
 for(const value of [null,undefined,NaN,Infinity,-1]){const a=agent();a.resources.cpuCores=value;assert.equal(resources.valid(a,now),false);}
});
test('CSV controls download server CSV and expose backend errors without navigating away',async()=>{
 const elements=new Map();const element=id=>{if(!elements.has(id))elements.set(id,{textContent:'',hidden:true,value:'1h',disabled:false});return elements.get(id)};
 const buttons=['current','samples','summary'].map(kind=>({dataset:{resourceExport:kind},addEventListener(type,fn){this.click=fn}}));
 let fail=false;const requests=[],downloads=[];
 const sandbox={module:{exports:{}},Date,URL:{createObjectURL(){return 'blob:download'},revokeObjectURL(){}},AbortController,setTimeout(fn,ms){return 1},clearTimeout(){},
  fetch:async url=>{requests.push(url);return fail?{ok:false,status:502,json:async()=>({error:'Prometheus unavailable'})}:{ok:true,status:200,headers:{get:()=> 'text/csv; charset=utf-8'},blob:async()=>({})}},
  document:{querySelector:id=>id==='#startResourceMeasurement'?null:element(id),querySelectorAll:()=>buttons,body:{append(){}},createElement(){return {click(){downloads.push(this.download)},remove(){}}}}
 };
 vm.createContext(sandbox);vm.runInContext(fs.readFileSync(path.join(__dirname,'static/agent-resources.js'),'utf8'),sandbox);
 sandbox.module.exports.init({api:async()=>{}});
 for(const b of buttons)await b.click();
 assert.equal(downloads.length,3);assert.match(requests[0],/resources\?format=csv$/);assert.match(requests[2],/history\?range=1h&kind=summary&format=csv$/);
 fail=true;await buttons[1].click();assert.match(element('#agentResourceExportStatus').textContent,/Prometheus unavailable/);assert.equal(downloads.length,3);assert.ok(buttons.every(b=>!b.disabled));
});
test('Grafana keeps process-only metrics separate from Agent plus Peer measurements',()=>{
 const d=JSON.parse(fs.readFileSync(path.join(__dirname,'../../monitoring/grafana/dashboards/kpl-experiments.json')));
 const cpu=d.panels.find(p=>p.title==='KPL CPU by Agent'),memory=d.panels.find(p=>p.title==='KPL memory by Agent');
 assert.match(cpu.targets[0].expr,/kpl_agent_cpu_usage_percent/);assert.equal(cpu.fieldConfig.defaults.unit,'percent');
 assert.match(memory.targets[0].expr,/kpl_agent_memory_working_set_bytes/);assert.equal(memory.fieldConfig.defaults.unit,'bytes');
 assert.ok(d.panels.some(p=>p.title==='Agent process CPU only'&&p.targets[0].expr.includes('process_cpu_seconds_total')));
 assert.ok(d.panels.some(p=>p.title==='Agent process resident memory only'));
 const ids=d.panels.map(p=>p.id);assert.equal(new Set(ids).size,ids.length);
 for(let i=0;i<d.panels.length;i++)for(let j=i+1;j<d.panels.length;j++){
  const a=d.panels[i].gridPos,b=d.panels[j].gridPos;
  assert.ok(a.x+a.w<=b.x||b.x+b.w<=a.x||a.y+a.h<=b.y||b.y+b.h<=a.y,`overlap ${d.panels[i].title} / ${d.panels[j].title}`);
 }
 const stack=fs.readFileSync(path.join(__dirname,'../../stack.swarm.yaml'),'utf8');
 assert.match(stack,/KPL_PROMETHEUS_URL: http:\/\/prometheus:9090/);assert.equal((stack.match(/grafana-dashboard-resources-v9/g)||[]).length,2);
});

function measurementUI(initial, handler) {
 const elements=new Map(),requests=[],downloads=[],intervals=[];
 function element(id){
  if(!elements.has(id))elements.set(id,{textContent:'',hidden:false,value:'',disabled:false,dataset:{},children:[],
   addEventListener(type,fn){this[type]=fn},replaceChildren(){this.children=[];this.value=''},append(child){this.children.push(child)}});
  return elements.get(id);
 }
 const buttons=['current','samples','summary','measurement-samples','measurement-summary'].map(kind=>Object.assign(element(kind),{dataset:{resourceExport:kind}}));
 element('#agentResourceRange').value='1h';
 const state={...initial},calls=[];
 const sandbox={module:{exports:{}},Date,AbortController,URL:{createObjectURL(){return 'blob:csv'},revokeObjectURL(){}},setTimeout(){return 1},clearTimeout(){},setInterval(fn){intervals.push(fn)},
  fetch:async url=>{requests.push(url);return {ok:true,status:200,headers:{get:()=> 'text/csv'},blob:async()=>({})}},
  document:{hidden:false,querySelector:element,querySelectorAll:()=>buttons,body:{append(){}},createElement(tag){return {value:'',textContent:'',click(){downloads.push(this.download)},remove(){}}}}
 };
 vm.createContext(sandbox);vm.runInContext(fs.readFileSync(path.join(__dirname,'static/agent-resources.js'),'utf8'),sandbox);
 const api=async(path,options)=>{calls.push({path,...options});return handler?handler(path,options,state):state};
 sandbox.module.exports.init({api});
 return {element,buttons,calls,requests,downloads,intervals,state};
}
const flush=()=>new Promise(resolve=>setImmediate(resolve));
const intervalRecord=(id='a'.repeat(32))=>({id,startedAt:'2026-09-28T00:00:00Z'});
const intervalList=measurements=>({generatedAt:'2026-09-28T00:01:00Z',maxDurationSeconds:86400,measurements});
test('measurement controls restore active state, stop the specific ID, and export fixed interval CSV',async()=>{
 const m=intervalRecord();
 const ui=measurementUI(intervalList([m]),async(path,options,state)=>{
  if(options.method==='POST'){assert.equal(path,`/api/v1/agents/resources/measurements/${m.id}/stop`);state.measurements=[{...m,endedAt:'2026-09-28T00:01:00Z',endReason:'stopped'}]}
  return state;
 });
 assert.equal(ui.element('#startResourceMeasurement').disabled,true);
 await flush();
 assert.equal(ui.element('#stopResourceMeasurement').disabled,false);
 assert.match(ui.element('#resourceMeasurementStatus').textContent,/Measuring · 00:01:00.*Sep.*UTC/);
 assert.equal(ui.buttons[3].disabled,true);
 await ui.element('#stopResourceMeasurement').click();
 assert.equal(ui.element('#stopResourceMeasurement').disabled,true);
 assert.equal(ui.element('#startResourceMeasurement').disabled,false);
 assert.equal(ui.element('#resourceMeasurementSelect').value,m.id);
 for(const button of ui.buttons.slice(3))await button.click();
 assert.equal(ui.downloads.length,2);
 assert.equal(ui.requests[0],`/api/v1/agents/resources/measurements/${m.id}/export?kind=samples&format=csv`);
 assert.equal(ui.requests[1],`/api/v1/agents/resources/measurements/${m.id}/export?kind=summary&format=csv`);
 assert.ok(ui.downloads.every(name=>name.includes(m.id)));
});
test('measurement starts suppress duplicate clicks and ignore older poll replies after stop',async()=>{
 const m=intervalRecord();let resolveStart,resolvePoll,pollDeferred=false;
 const ui=measurementUI(intervalList([]),async(path,options,state)=>{
  if(options.method==='GET'&&pollDeferred)return new Promise(resolve=>resolvePoll=resolve);
  if(options.method==='POST'&&path.endsWith('/measurements'))return new Promise(resolve=>resolveStart=()=>{state.measurements=[m];resolve({...state})});
  if(options.method==='POST'){state.measurements=[{...m,endedAt:'2026-09-28T00:01:00Z',endReason:'stopped'}]}
  return {...state};
 });
 await flush();
 const pending=ui.element('#startResourceMeasurement').click();
 await ui.element('#startResourceMeasurement').click();
 assert.equal(ui.calls.filter(c=>c.method==='POST').length,1);
 resolveStart();await pending;
 pollDeferred=true;const poll=ui.element('#refreshResourceMeasurements').click();
 await ui.element('#stopResourceMeasurement').click();
 resolvePoll(intervalList([m]));await poll;
 assert.equal(ui.element('#stopResourceMeasurement').disabled,true);
 assert.match(ui.element('#resourceMeasurementStatus').textContent,/Ready/);
 assert.equal(ui.buttons[3].disabled,false);
});
test('failed measurement state or mutation stays visible and can be recovered by refresh',async()=>{
 let fail=true;
 const ui=measurementUI(intervalList([]),async(path,options,state)=>{if(fail)throw Error('Local storage unavailable');return state});
 await flush();
 assert.equal(ui.element('#startResourceMeasurement').disabled,true);
 assert.match(ui.element('#resourceMeasurementError').textContent,/Local storage unavailable/);
 fail=false;await ui.element('#refreshResourceMeasurements').click();
 assert.equal(ui.element('#startResourceMeasurement').disabled,false);
 fail=true;await ui.element('#startResourceMeasurement').click();
 assert.equal(ui.element('#startResourceMeasurement').disabled,true);
 assert.match(ui.element('#resourceMeasurementError').textContent,/Refresh measurement status/);
});

test('46/47 partial containers remain visible with host-normalized CPU and weighted fleet totals',()=>{
 const partial=agent('partial',4);Object.assign(partial.resources,{cpuCapacityCores:16,containers:47,measuredContainers:46,complete:false});
 const full=agent('full',2);full.resources.cpuCapacityCores=4;
 const legacy=agent('legacy',100);delete legacy.resources.cpuCapacityCores;
 const d=resources.describe(partial,now);
 assert.equal(d.cpu,'25%');assert.equal(d.memory,'3 KiB');assert.match(d.detail,/Partial · 46\/47 containers/);assert.match(d.memoryDetail,/46\/47/);
 assert.doesNotMatch(resources.cell(partial,'cpu',now),/N\/A/);assert.doesNotMatch(resources.cell(partial,'memory',now),/N\/A/);
 assert.equal(resources.describe(full,now).cpu,'50%');
 const total=resources.aggregate([partial,full,legacy],now);assert.equal(total.cpuPercent,30);assert.equal(total.cpuCapacityCores,20);assert.equal(total.cpuMeasured,2);assert.equal(total.measured,3);
 assert.equal(resources.describe(legacy,now).cpu,'N/A');assert.equal(resources.describe(legacy,now).memory,'3 KiB');
 partial.resources.measuredContainers=0;assert.equal(resources.describe(partial,now).memory,'N/A');
 partial.resources.measuredContainers=48;assert.equal(resources.valid(partial,now),false);
});

test('deleting a completed interval protects the active one and ignores an older poll',async()=>{
 const active=intervalRecord('b'.repeat(32)),complete={...intervalRecord(),endedAt:'2026-09-28T00:01:00Z',endReason:'stopped'};
 let resolveDelete,resolvePoll,pollDeferred=false;
 const ui=measurementUI(intervalList([active,complete]),async(path,options,state)=>{
  if(options.method==='GET'&&pollDeferred)return new Promise(resolve=>resolvePoll=resolve);
  if(options.method==='DELETE'){
   assert.equal(path,`/api/v1/agents/resources/measurements/${complete.id}`);
   return new Promise(resolve=>resolveDelete=()=>{state.measurements=[active];resolve({...state})});
  }
  return {...state};
 });
 await flush();assert.equal(ui.element('#deleteResourceMeasurement').disabled,false);
 pollDeferred=true;const poll=ui.element('#refreshResourceMeasurements').click();
 const deletion=ui.element('#deleteResourceMeasurement').click();
 await ui.element('#deleteResourceMeasurement').click();
 assert.equal(ui.calls.filter(c=>c.method==='DELETE').length,1);
 resolveDelete();await deletion;resolvePoll(intervalList([active,complete]));await poll;
 assert.equal(ui.element('#resourceMeasurementSelect').value,'');
 assert.equal(ui.element('#deleteResourceMeasurement').disabled,true);
 assert.equal(ui.element('#stopResourceMeasurement').disabled,false);
 assert.match(ui.element('#resourceMeasurementError').textContent,/Interval deleted/);
});
test('disabled Agents remain visible as excluded but do not enter hardware totals',()=>{
 const active=agent('active',2),disabled={...agent('disabled',10),disabled:true};
 const result=resources.aggregate([active,disabled],now);
 assert.equal(result.total,1);assert.equal(result.measured,1);assert.equal(result.cpuCores,2);assert.equal(result.memoryWorkingSetBytes,3072);
 assert.equal(resources.describe(disabled,now).cpu,'Excluded');assert.equal(resources.describe(disabled,now).memory,'Excluded');
});
