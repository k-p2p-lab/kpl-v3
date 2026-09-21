const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const source = fs.readFileSync(path.join(__dirname, 'static/app.js'), 'utf8');
const functions = source.slice(source.indexOf('function escapeHTML('), source.indexOf('$("#scenarioText").value = defaultScenario;'));

function fixture() {
  const streams = [], timers = new Map(), documentEvents = {}, windowEvents = {};
  let timerId = 0;
  class EventSource {
    constructor(url) { this.url = url; this.listeners = {}; this.closed = 0; streams.push(this); }
    addEventListener(name, callback) { this.listeners[name] = callback; }
    close() { this.closed++; }
    receive(name, data) { this.listeners[name]({ data: JSON.stringify(data) }); }
  }
  const context = {
    state: { snapshot: null, stream: null, streamSnapshot: null, streamPageHidden: false, loginRedirecting: false, reconnectTimer: null, snapshotRenderTimer: null },
    AbortController, Math: Object.assign(Object.create(Math), { random: () => 0.5 }),
    EventSource, document: { hidden: false, addEventListener(name, callback) { documentEvents[name] = callback; } },
    window: { addEventListener(name, callback) { windowEvents[name] = callback; } },
    setTimeout(callback, delay) { const id = ++timerId; timers.set(id, { callback, delay }); return id; },
    clearTimeout(id) { timers.delete(id); },
  };
  vm.createContext(context);
  vm.runInContext(functions, context);
  const statuses = [], rendered = [];
  context.setConnection = (mode, label) => statuses.push({mode, label});
  context.render = snapshot => rendered.push(snapshot);
  context.api = async () => ({});
  context.setupStreamLifecycle();
  return { context, state: context.state, streams, timers, documentEvents, windowEvents, statuses, rendered };
}
function plain(value) { return JSON.parse(JSON.stringify(value)); }
function baseline() {
  return { generatedAt:'one', agents:[{id:'a',state:'online'}], nodes:[{id:'n1'},{id:'n2'}], experiments:[{id:'r1',state:'running'},{id:'r2',state:'queued'}], edges:[{source:'n1',target:'n2'}], events:[{type:'old'}], metrics:{runId:'r1'} };
}

test('one connection receives the initial snapshot and merges only changed rows, removal and ordering', () => {
  const {context,state,streams,timers,rendered} = fixture();
  context.connectStream(); context.connectStream();
  assert.equal(streams.length,1);
  assert.equal(streams[0].url,'/api/v1/stream?view=dashboard');
  const original = baseline();
  streams[0].receive('snapshot',original);
  // A control's optimistic update must not corrupt the server's delta base.
  state.snapshot.experiments[0].state = 'stopping';
  assert.equal(state.streamSnapshot.experiments[0].state,'running');
  streams[0].receive('snapshot_delta', {
    generatedAt:'two', nodes:{remove:['n1'],upsert:[{id:'n3'}],order:['n3','n2']},
    experiments:{upsert:[{id:'r2',state:'running'},{id:'r1',state:'completed'}],order:['r2','r1']},
    edges:[],events:[],metrics:{runId:'r2'},
  });
  assert.deepEqual(plain(state.snapshot.nodes),[{id:'n3'},{id:'n2'}]);
  assert.deepEqual(plain(state.snapshot.agents),original.agents);
  assert.deepEqual(plain(state.snapshot.experiments),[{id:'r2',state:'running'},{id:'r1',state:'completed'}]);
  assert.deepEqual(plain(state.snapshot.edges),[]);
  const renderTimers = [...timers.values()].filter(timer => timer.delay === 250);
  assert.equal(renderTimers.length,1,'burst updates should share one render');
  renderTimers[0].callback();
  assert.equal(rendered.length,1);
  assert.equal(rendered[0].generatedAt,'two');
  streams[0].receive('snapshot_delta',{nodes:{remove:['n2','n3'],order:[]}});
  assert.deepEqual(plain(state.snapshot.nodes),[]);
});

test('hidden mobile tabs close SSE and resume with a fresh baseline without duplicate connections', () => {
  const {context,state,streams,timers,documentEvents,windowEvents} = fixture();
  context.connectStream();
  streams[0].receive('snapshot',baseline());
  context.document.hidden=true; documentEvents.visibilitychange();
  assert.equal(streams[0].closed,1);
  assert.equal(state.stream,null); assert.equal(state.streamSnapshot,null);
  assert.equal(timers.size,0);
  streams[0].receive('snapshot_delta',{generatedAt:'stale'});
  assert.equal(state.snapshot.generatedAt,'one');
  context.connectStream(); assert.equal(streams.length,1);
  context.document.hidden=false; documentEvents.visibilitychange(); windowEvents.pageshow();
  assert.equal(streams.length,2);
  streams[1].receive('snapshot',{...baseline(),generatedAt:'resumed',nodes:[{id:'fresh'}]});
  assert.equal(state.snapshot.generatedAt,'resumed');
  assert.deepEqual(plain(state.snapshot.nodes),[{id:'fresh'}]);
  windowEvents.pagehide();
  assert.equal(streams[1].closed,1);
  context.connectStream(); assert.equal(streams.length,2);
  windowEvents.pageshow(); assert.equal(streams.length,3);
});

test('an in-flight authentication check cannot restart a hidden tab', async () => {
  const {context,state,streams,timers,documentEvents} = fixture();
  let resolve;
  context.api = () => new Promise(done => { resolve=done; });
  context.connectStream();
  const reconnecting = streams[0].onerror();
  context.document.hidden=true; documentEvents.visibilitychange();
  resolve({}); await reconnecting;
  assert.equal(state.stream,null);
  assert.equal(timers.size,0);
  context.document.hidden=false; documentEvents.visibilitychange();
  assert.equal(streams.length,2);
});

test('repeated errors schedule one reconnect and malformed/missing baselines force a fresh connection', async () => {
  const {context,state,streams,timers} = fixture();
  context.connectStream();
  await Promise.all([streams[0].onerror(), streams[0].onerror()]);
  assert.equal(streams[0].closed,1);
  assert.equal(timers.size,1);
  const reconnect=[...timers.values()][0];
  assert.equal(reconnect.delay,2000);
  reconnect.callback();
  assert.equal(streams.length,2);
  streams[1].receive('snapshot_delta',{nodes:{upsert:[{id:'unknown'}]}});
  await new Promise(resolve => setImmediate(resolve));
  assert.equal(streams[1].closed,1);
  assert.equal(state.stream,null);
  assert.equal(state.streamSnapshot,null);
});

test('logout redirect prevents stream reopening on visibility and bfcache restoration', () => {
  const {context,state,streams,documentEvents,windowEvents} = fixture();
  state.loginRedirecting=true;
  context.connectStream(); documentEvents.visibilitychange(); windowEvents.pageshow();
  assert.equal(streams.length,0);
});

function fireTimer(timers, delay) {
  const entry = [...timers.entries()].find(([, timer]) => timer.delay === delay);
  assert.ok(entry, `expected a ${delay} ms timer`);
  timers.delete(entry[0]);
  return entry[1].callback();
}
const settle = () => new Promise(resolve => setImmediate(resolve));

test('a session request that never responds is aborted and cannot strand reconnecting', async () => {
  const {context,state,streams,timers,statuses} = fixture();
  let signal;
  context.api = (url, options) => {
    assert.equal(url,'/api/v1/auth/session');
    signal = options.signal;
    return new Promise((resolve, reject) => signal.addEventListener('abort', () => reject(new Error('aborted')), {once:true}));
  };
  context.connectStream();
  const reconnect = streams[0].onerror();
  assert.equal(signal.aborted,false);
  fireTimer(timers,8000);
  await reconnect;
  assert.equal(signal.aborted,true);
  assert.equal(state.streamAuthAbort,null);
  assert.equal(state.stream,null);
  assert.equal(timers.size,1);
  fireTimer(timers,2000);
  assert.equal(streams.length,2);
  streams[1].receive('snapshot',baseline());
  assert.equal(state.streamFailures,0);
  assert.equal(statuses.at(-1).label,'Live');
});

test('watchdog recovers both a stalled connection attempt and a silently stalled open stream', async () => {
  const {context,state,streams,timers} = fixture();
  context.connectStream();
  fireTimer(timers,30000); await settle();
  assert.equal(streams[0].closed,1);
  fireTimer(timers,2000);
  streams[1].receive('snapshot',baseline());
  fireTimer(timers,250);
  fireTimer(timers,45000); await settle();
  assert.equal(streams[1].closed,1);
  assert.equal(state.stream,null);
  fireTimer(timers,2000);
  assert.equal(streams.length,3);
});

test('heartbeats keep an idle dashboard live without rerendering or accepting stale streams', () => {
  const {context,state,streams,timers,rendered,documentEvents} = fixture();
  context.connectStream();
  // A heartbeat cannot mask a missing initial snapshot.
  streams[0].receive('heartbeat',{});
  assert.equal([...timers.values()][0].delay,30000);
  streams[0].receive('snapshot',baseline());
  fireTimer(timers,250);
  for(let i=0;i<100;i++) streams[0].receive('heartbeat',{});
  assert.equal(timers.size,1);
  assert.equal([...timers.values()][0].delay,45000);
  assert.equal(rendered.length,1);
  context.document.hidden=true; documentEvents.visibilitychange();
  streams[0].receive('heartbeat',{});
  assert.equal(timers.size,0);
  assert.equal(state.stream,null);
});

test('repeated failures back off up to 30 seconds and successful snapshots reset the delay', async () => {
  const {context,state,streams,timers} = fixture();
  context.connectStream();
  for(const delay of [2000,4000,8000,16000,30000,30000]) {
    await streams.at(-1).onerror();
    assert.equal(timers.size,1);
    fireTimer(timers,delay);
  }
  streams.at(-1).receive('snapshot',baseline());
  assert.equal(state.streamFailures,0);
  fireTimer(timers,250);
  await streams.at(-1).onerror();
  fireTimer(timers,2000);
});

test('hiding a page cancels its session probe and network restoration opens just one fresh stream', async () => {
  const {context,state,streams,timers,documentEvents,windowEvents} = fixture();
  let signal;
  context.api = (url, options) => {
    signal = options.signal;
    return new Promise((resolve,reject) => signal.addEventListener('abort', () => reject(new Error('aborted')), {once:true}));
  };
  context.connectStream();
  const reconnect = streams[0].onerror();
  context.document.hidden=true; documentEvents.visibilitychange();
  await reconnect;
  assert.equal(signal.aborted,true);
  assert.equal(timers.size,0);
  windowEvents.online();
  assert.equal(streams.length,1,'background online event must not reopen SSE');
  context.document.hidden=false; documentEvents.visibilitychange();
  windowEvents.online();
  assert.equal(streams.length,3);
  assert.equal(streams[1].closed,1);
  assert.equal(state.stream,streams[2]);
  assert.equal(timers.size,1);
});
