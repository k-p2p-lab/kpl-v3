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
test('service cards show host percentages, partial coverage, and safe names',()=>{
 const value=row(),d=services.describe(value,now);
 assert.equal(d.cpu,'25%');assert.equal(d.memory,'3 KiB');assert.match(d.coverage,/Partial · 1\/2/);
 value.nodeName='<img src=x onerror=alert(1)>';const html=services.cards([value],now);
 assert.match(html,/&lt;img/);assert.doesNotMatch(html,/<img/);
 value.resources.cpuCapacityCores=0;assert.equal(services.describe(value,now).cpu,'N/A');assert.equal(services.describe(value,now).memory,'3 KiB');
 for(const state of ['stale','unknown','not_running','awaiting']){value.state=state;assert.equal(services.describe(value,now).cpu,'N/A');assert.equal(services.describe(value,now).memory,'N/A');}
 value.state='running';assert.equal(services.describe(value,now+31000).state,'stale');assert.equal(services.describe(value,now+31000).memory,'N/A');
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
 assert.match(element('#serviceResourceCards').innerHTML,/25%/);assert.match(element('#serviceResourceStatus').textContent,/Included in interval/);
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
