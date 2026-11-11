const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const resources = require('./static/agent-resources.js');
global.KPLAgentResources=resources;
const services = require('./static/service-resources.js');
const now=Date.now();
const row=()=>({service:'controller',state:'running',nodeId:'node1',nodeName:'manager',reportedAt:new Date(now).toISOString(),resources:{sampledAt:new Date(now).toISOString(),containers:2,measuredContainers:1,complete:false,cpuCores:2,cpuCapacityCores:8,memoryUsageBytes:4096,memoryWorkingSetBytes:3072}});
test('service rows show host percentages, partial coverage, and safe names',()=>{
 const value=row(),d=services.describe(value,now);
 assert.equal(d.cpu,'25%');assert.equal(d.memory,'3 KiB');assert.match(d.coverage,/Partial · 1\/2/);
 value.nodeName='<img src=x onerror=alert(1)>';const html=services.tableRows([value],now);
 assert.match(html,/&lt;img/);assert.doesNotMatch(html,/<img/);
 value.resources.cpuCapacityCores=0;assert.equal(services.describe(value,now).cpu,'N/A');assert.equal(services.describe(value,now).memory,'3 KiB');
 for(const state of ['stale','unknown','not_running','awaiting']){value.state=state;assert.equal(services.describe(value,now).cpu,'N/A');assert.equal(services.describe(value,now).memory,'N/A');}
 value.state='running';assert.equal(services.describe(value,now+31000).state,'stale');assert.equal(services.describe(value,now+31000).memory,'N/A');
});
test('service host labels use Agent numbers while retaining escaped host identities and fallbacks',()=>{
 const value={...row(),nodeName:'manager <one>',nodeId:'node"1'},seen=[];
 const html=services.tableRows([value],now,entry=>{seen.push(entry);return 'Agent 7 & 9';});
 assert.deepEqual(seen,[value]);
 assert.match(html,/class="service-resource-host" title="manager &lt;one&gt; · node&quot;1">Agent 7 &amp; 9<\/td>/);
 assert.match(services.tableRows([value],now,()=>''),/title="manager &lt;one&gt; · node&quot;1">manager &lt;one&gt;<\/td>/);
 value.nodeName='';
 assert.match(services.tableRows([value],now,()=>null),/class="service-resource-host" title="node&quot;1">node&quot;1<\/td>/);
 value.nodeId='';
 assert.match(services.tableRows([value],now),/class="service-resource-host" title="">Host not reported<\/td>/);
});
test('service totals count a shared host once and weight distinct hosts by capacity',()=>{
 const controller=row(),prometheus={...row(),service:'prometheus'},other={...row(),nodeId:'node2'};
 prometheus.resources.cpuCores=1;
 other.resources.cpuCapacityCores=16;other.resources.cpuCores=4;
 const shared=services.aggregate([controller,prometheus],now);
 assert.equal(shared.cpuCapacityCores,8);assert.equal(shared.cpuPercent,37.5);
 const total=services.aggregate([controller,prometheus,other],now);
 assert.equal(total.cpuCapacityCores,24);assert.equal(total.cpuPercent,100*7/24);
 assert.equal(total.memoryWorkingSetBytes,9216);assert.equal(total.memoryUsageBytes,12288);
 assert.equal(total.hosts,2);assert.equal(total.measured,3);assert.equal(total.partial,3);
 assert.equal(total.containers,6);assert.equal(total.measuredContainers,3);
 const html=services.tableRows([controller,prometheus,other],now);
 assert.match(html,/^<tr class="resource-total-row resource-has-notice" data-resource-total="services">/);
 assert.match(html,/29.2%/);assert.match(html,/9 KiB/);assert.match(html,/Partial · 3\/6 containers/);
 assert.equal((html.match(/data-service=/g)||[]).length,3);
});
test('service totals retain partial memory while excluding stale and unidentifiable CPU samples',()=>{
 const partial=row(),missing=row(),stale=row(),noHost=row(),noCapacity=row(),stopped=row();
 missing.resources.measuredContainers=0;stale.reportedAt=new Date(now-31000).toISOString();
 noHost.nodeId='';noCapacity.resources.cpuCapacityCores=0;stopped.state='not_running';
 const total=services.aggregate([partial,missing,stale,noHost,noCapacity,stopped],now);
 assert.equal(total.measured,3);assert.equal(total.cpuMeasured,1);assert.equal(total.cpuPercent,25);
 assert.equal(total.memoryWorkingSetBytes,9216);assert.equal(total.containers,8);assert.equal(total.measuredContainers,3);
 const empty=services.aggregate([],now);assert.equal(empty.cpuPercent,null);assert.equal(empty.measured,0);
 for(const rows of [[],[stale,stopped],[missing]]) {
  const html=services.tableRows(rows,now);
  assert.match(html,/data-resource-total="services"/);assert.match(html,/N\/A/);
  assert.doesNotMatch(html,/>0%<|>0 B</);
 }
 assert.match(services.tableRows([partial,missing,stale,noHost,noCapacity,stopped],now),/1 \/ 6 services measured/);
});
test('service polling is bounded, pauses when hidden, and avoids overlapping requests',async()=>{
 const elements=new Map();const element=id=>{if(!elements.has(id))elements.set(id,{hidden:false,innerHTML:'',textContent:'',listeners:{},addEventListener(k,fn){this.listeners[k]=fn}});return elements.get(id)};
 const timers=[],listeners={},doc={hidden:false,querySelector:element,addEventListener(type,fn){listeners[type]=fn}};
 const sandbox={document:doc,KPLAgentResources:resources,setInterval:fn=>timers.push(fn),setTimeout,clearTimeout,AbortController,Date,console};
 vm.runInNewContext(fs.readFileSync(path.join(__dirname,'static/service-resources.js'),'utf8'),sandbox);
 let calls=0,resolve,hostLabel='';
 element('#services').hidden=true;
 sandbox.KPLServiceResources.init({api:async()=>{calls++;return new Promise(r=>resolve=r)},hostLabel:()=>hostLabel});
 assert.equal(calls,0);
 element('#agents').hidden=true;element('#services').hidden=false;
 listeners['dashboard:tab-change']({detail:{id:'services'}});timers[0]();assert.equal(calls,1);
 assert.equal(element('#refreshServices').disabled,true);
 resolve({generatedAt:new Date(now).toISOString(),controllerUptimeSeconds:60,services:[row()]});await new Promise(r=>setImmediate(r));
 assert.match(element('#serviceResourceRows').innerHTML,/25%/);assert.match(element('#serviceResourceStatus').textContent,/Included in interval/);
 assert.equal(element('#refreshServices').disabled,false);
 assert.match(element('#serviceResourceRows').innerHTML,/title="manager · node1">manager<\/td>/);
 hostLabel='Agent 7';timers[0]();
 assert.match(element('#serviceResourceRows').innerHTML,/title="manager · node1">Agent 7<\/td>/);
 assert.equal(calls,1,'A late Agent-number mapping updates the displayed label without another resource fetch');
 doc.hidden=true;timers[0]();assert.equal(calls,1);
 doc.hidden=false;element('#services').hidden=true;timers[0]();assert.equal(calls,1);
 element('#services').hidden=false;const refresh=element('#refreshServices').listeners.click();assert.equal(calls,2);
 resolve({controllerUptimeSeconds:90,services:[]});await refresh;
 assert.match(element('#serviceResourceRows').innerHTML,/No service resources/);
});

test('service measurement link opens the single shared controls and returns to Agent Status',()=>{
 const elements=new Map(),focused=[];
 const element=id=>{if(!elements.has(id))elements.set(id,{hidden:true,addEventListener(type,fn){this[type]=fn},querySelector(){return {focus(){focused.push('summary')}}}});return elements.get(id)};
 const shown=[],sandbox={document:{querySelector:element,hidden:false,addEventListener(){}},KPLAgentResources:resources,KPLDashboardTabs:{show:id=>shown.push(id)},setInterval(){},setTimeout,clearTimeout,AbortController,Date};
 vm.runInNewContext(fs.readFileSync(path.join(__dirname,'static/service-resources.js'),'utf8'),sandbox);
 sandbox.KPLServiceResources.init({api:async()=>assert.fail('Hidden service panel must not fetch')});
 let prevented=false;element('#openResourceTools').click({preventDefault(){prevented=true}});
 assert.equal(prevented,true);assert.equal(element('#resourceTools').open,true);
 assert.deepEqual(shown,['agents']);assert.deepEqual(focused,['summary']);
});
test('Swarm isolates service monitoring from Agent participation and Grafana uses service labels',()=>{
 const root=path.join(__dirname,'../..'),stack=fs.readFileSync(path.join(root,'stack.swarm.yaml'),'utf8');
 const monitor=stack.split('\n  resource-monitor:')[1].split('\n  prometheus:')[0];
 assert.match(monitor,/mode: global/);assert.doesNotMatch(monitor,/kpl.*\.agent ==/);assert.match(monitor,/docker.sock:ro/);
 const controller=stack.split('\n  controller:')[1].split('\n  agent:')[0];assert.doesNotMatch(controller,/docker.sock|user: '0:0'/);
 const manager=stack.split('\n  network-manager:')[1].split('\n  resource-monitor:')[0];
 assert.match(manager,/io.kpl.resource-monitor: 'true'/);assert.match(manager,/node.role == manager/);
 assert.equal((stack.match(/io.kpl.resource-monitor: 'true'/g)||[]).length,5);
 const panels=JSON.parse(fs.readFileSync(path.join(root,'monitoring/grafana/dashboards/kpl-experiments.json'))).panels;
 const service=panels.filter(p=>p.title.startsWith('Service '));assert.equal(service.length,6);
 for(const p of service){assert.match(p.targets[0].expr,/kpl_service_/);assert.doesNotMatch(p.targets[0].expr,/agent_id/);assert.match(p.targets[0].legendFormat,/node_id/);}
});

test('service notices share one footer instead of repeating inside resource cells',()=>{
 const html=services.tableRows([row()],now),rows=html.match(/<tr[\s\S]*?<\/tr>/g);
 assert.equal(rows.length,4);
 for(const index of [0,2]) {
  assert.doesNotMatch(rows[index],/>Partial|services measured<|class="agent-resource-partial"/);
  assert.match(rows[index+1],/class="resource-notice-row/);
  assert.match(rows[index+1],/colspan="6"/);
  assert.equal((rows[index+1].match(/Partial/g)||[]).length,1);
 }
 const complete=row();complete.resources.complete=true;complete.resources.measuredContainers=2;
 assert.doesNotMatch(services.tableRows([complete],now),/resource-notice-row/);
});
