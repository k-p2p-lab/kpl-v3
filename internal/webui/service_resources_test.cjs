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
 assert.match(html,/^<tr class="resource-total-row" data-resource-total="services">/);
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
 const timers=[],doc={hidden:false,querySelector:element,addEventListener(){}};
 const sandbox={document:doc,KPLAgentResources:resources,setInterval:fn=>timers.push(fn),setTimeout,clearTimeout,AbortController,Date,console};
 vm.runInNewContext(fs.readFileSync(path.join(__dirname,'static/service-resources.js'),'utf8'),sandbox);
 let calls=0,resolve;
 element('#agents').hidden=true;
 sandbox.KPLServiceResources.init({api:async()=>{calls++;return new Promise(r=>resolve=r)}});
 assert.equal(calls,0);
 element('#agents').hidden=false;timers[0]();timers[0]();assert.equal(calls,1);
 resolve({generatedAt:new Date(now).toISOString(),controllerUptimeSeconds:60,services:[row()]});await new Promise(r=>setImmediate(r));
 assert.match(element('#serviceResourceRows').innerHTML,/25%/);assert.match(element('#serviceResourceStatus').textContent,/Included in interval/);
 doc.hidden=true;timers[0]();assert.equal(calls,1);
});
test('Swarm isolates service monitoring from Agent participation and Grafana uses service labels',()=>{
 const root=path.join(__dirname,'../..'),stack=fs.readFileSync(path.join(root,'stack.swarm.yaml'),'utf8');
 const monitor=stack.split('\n  resource-monitor:')[1].split('\n  prometheus:')[0];
 assert.match(monitor,/mode: global/);assert.doesNotMatch(monitor,/kpl.*\.agent ==/);assert.match(monitor,/docker.sock:ro/);
 const controller=stack.split('\n  controller:')[1].split('\n  agent:')[0];assert.doesNotMatch(controller,/docker.sock|user: '0:0'/);
 assert.equal((stack.match(/io.kpl.resource-monitor: 'true'/g)||[]).length,4);
 const panels=JSON.parse(fs.readFileSync(path.join(root,'monitoring/grafana/dashboards/kpl-experiments.json'))).panels;
 const service=panels.filter(p=>p.title.startsWith('Service '));assert.equal(service.length,6);
 for(const p of service){assert.match(p.targets[0].expr,/kpl_service_/);assert.doesNotMatch(p.targets[0].expr,/agent_id/);assert.match(p.targets[0].legendFormat,/node_id/);}
});
