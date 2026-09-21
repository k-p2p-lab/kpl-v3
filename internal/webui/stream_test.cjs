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
  assert.equal(timers.size,1,'burst updates should share one render');
  [...timers.values()][0].callback();
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
