const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const source = fs.readFileSync(path.join(__dirname,'static/app.js'),'utf8');
const functions = source.slice(source.indexOf('function escapeHTML('),source.indexOf('$("#scenarioText").value = defaultScenario;'));
const sandbox = {};
vm.createContext(sandbox);vm.runInContext(functions,sandbox);
const sample = overrides => ({sessions:2,finalizedSessions:0,sentBytes:2048,receivedBytes:1024,rejectedSamples:0,latestAt:'2026-09-09T00:00:00Z',currentRates:{available:true,sentBitsPerSecond:8000,receivedBitsPerSecond:16000,reportingSessions:2,staleSessions:0},...overrides});

test('bandwidth cards use explicit bit rates and binary cumulative byte units',()=>{
 const view=sandbox.bandwidthMetricView(sample());
 assert.equal(view.send,'8 kbit/s');assert.equal(view.receive,'16 kbit/s');
 assert.equal(view.sent,'Total sent: 2 KiB');assert.equal(view.received,'Total received: 1 KiB');
 assert.equal(view.quality,'Live');assert.match(view.sessions,/2 \/ 2 active sessions fresh/);
 for (const value of [null,undefined,NaN,Infinity,-1,'8000']) assert.equal(sandbox.formatBandwidthRate(value),'N/A');
 assert.equal(sandbox.formatBandwidthRate(0),'0 bit/s');assert.equal(sandbox.formatBandwidthRate(8e6),'8 Mbit/s');
});

test('missing and stale bandwidth stays unavailable while measured zero and final totals survive',()=>{
 for (const data of [undefined,null,{sessions:0,rejectedSamples:1}]) {
  const view=sandbox.bandwidthMetricView(data);assert.equal(view.send,'N/A');assert.equal(view.sent,'Total sent: N/A');
 }
 const zero=sandbox.bandwidthMetricView(sample({sentBytes:0,receivedBytes:0,currentRates:{available:true,sentBitsPerSecond:0,receivedBitsPerSecond:0,reportingSessions:2,staleSessions:0}}));
 assert.equal(zero.send,'0 bit/s');assert.equal(zero.sent,'Total sent: 0 B');
 const stale=sandbox.bandwidthMetricView(sample({currentRates:{available:false,sentBitsPerSecond:0,receivedBitsPerSecond:0,reportingSessions:0,staleSessions:2}}));
 assert.equal(stale.send,'N/A');assert.equal(stale.sent,'Total sent: 2 KiB');assert.equal(stale.quality,'Rate unavailable');assert.match(stale.sessions,/2 stale/);
 const final=sandbox.bandwidthMetricView(sample({finalizedSessions:2,currentRates:{available:true,sentBitsPerSecond:0,receivedBitsPerSecond:0,reportingSessions:0,staleSessions:0}}));
 assert.equal(final.send,'0 bit/s');assert.equal(final.sent,'Total sent: 2 KiB');assert.equal(final.quality,'Finalized');
 const old=sandbox.bandwidthMetricView(sample({currentRates:undefined}));assert.equal(old.send,'N/A');assert.equal(old.sent,'Total sent: 2 KiB');
});

test('partial coverage is visible and actual main rendering clears bandwidth on run changes',()=>{
 const elements=new Map();
 sandbox.$=selector=>{if(!elements.has(selector))elements.set(selector,{textContent:''});return elements.get(selector)};
 for(const name of ['rememberAgents','renderRuns','renderAgents','renderEvents','renderTopology'])sandbox[name]=()=>{};
 sandbox.filterTopologyEdges=()=>[];
 const bw=sample({rejectedSamples:3,currentRates:{available:true,sentBitsPerSecond:8000,receivedBitsPerSecond:16000,reportingSessions:1,staleSessions:1}});
 sandbox.render({generatedAt:'2026-09-09T00:00:00Z',experiments:[{id:'one',state:'running'}],metrics:{runId:'one',definition:'dispatch-cohort-v1',bandwidth:bw}});
 assert.equal(elements.get('#bandwidthSendMetric').textContent,'8 kbit/s');
 assert.equal(elements.get('#bandwidthQualityMetric').textContent,'Partial');
 assert.match(elements.get('#bandwidthSessionsMetric').textContent,/1 \/ 2 active sessions fresh/);
 assert.equal(elements.get('#bandwidthRejectedMetric').textContent,'Rejected samples: 3');
 assert.match(elements.get('#messageMetricsScope').textContent,/one/);
 sandbox.render({generatedAt:'2026-09-09T00:00:01Z',experiments:[{id:'two',state:'running'}],metrics:{runId:'two'}});
 assert.equal(elements.get('#bandwidthSendMetric').textContent,'N/A');
 assert.equal(elements.get('#bandwidthSendTotal').textContent,'Total sent: N/A');
 assert.equal(elements.get('#bandwidthQualityMetric').textContent,'N/A');
 assert.match(elements.get('#messageMetricsScope').textContent,/two/);
});
