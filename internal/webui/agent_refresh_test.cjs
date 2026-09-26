const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const source = fs.readFileSync(`${__dirname}/static/app.js`, 'utf8');
function fixture() {
  const elements = new Map(), requests = [], timers = new Map();
  const element = key => {
    if (!elements.has(key)) elements.set(key, {textContent:'', hidden:true, disabled:false, dataset:{}, attributes:{}, setAttribute(k,v){this.attributes[k]=v;}});
    return elements.get(key);
  };
  const api = {Date, Intl, URL, AbortController, document:{querySelector:element,querySelectorAll:()=>[]}, localStorage:{getItem:()=>null}, setTimeout:(fn, delay)=>{timers.set(delay,fn);return delay;}, clearTimeout:id=>timers.delete(id)};
  vm.createContext(api);
  vm.runInContext(source.slice(0, source.indexOf('const defaultScenario =')) + source.slice(source.indexOf('function escapeHTML('), source.indexOf('$("#scenarioText").value = defaultScenario;')), api);
  const state = vm.runInContext('state', api);
  state.snapshot = {agents:[{id:'a', name:'Worker A', state:'offline', lastSeen:'2026-09-26T01:00:00Z'}], nodes:[{id:'peer'}]};
  api.render = snapshot => { api.rendered = snapshot; };
  api.api = (url, options) => new Promise((resolve,reject) => requests.push({url,options,resolve,reject}));
  return {api,state,element,requests,timers};
}

test('Refresh Agents checks the server once, updates status and preserves other snapshot sections', async () => {
  const {api,state,element,requests,timers} = fixture();
  const refreshing = api.refreshAgents();
  await api.refreshAgents();
  assert.equal(requests.length,1);
  assert.equal(requests[0].url,'/api/v1/agents/refresh');
  assert.equal(requests[0].options.method,'POST');
  assert.equal(element('#refreshAgents').disabled,true);
  assert.equal(element('#refreshAgents').attributes['aria-busy'],'true');
  assert.equal(timers.has(20000),true);
  requests[0].resolve({agents:[{...state.snapshot.agents[0],state:'online',lastSeen:'2026-09-26T01:01:00Z'}], requested:1, refreshed:1, failures:[]});
  await refreshing;
  assert.equal(state.snapshot.agents[0].state,'online');
  assert.equal(state.snapshot.nodes[0].id,'peer');
  assert.equal(api.rendered,state.snapshot);
  assert.equal(element('#agentRefreshStatus').textContent,'Refreshed 1 of 1 Agents.');
  assert.equal(element('#refreshAgents').disabled,false);
  assert.equal(element('#refreshAgents').textContent,'Refresh Agents');
  assert.equal(timers.size,0);
});

test('partial refresh names unavailable Agents and preserves a newer SSE update', async () => {
  const {api,state,element,requests} = fixture();
  const refreshing = api.refreshAgents();
  state.snapshot.agents[0] = {...state.snapshot.agents[0], state:'online',lastSeen:'2026-09-26T01:02:00Z'};
  requests[0].resolve({agents:[{id:'a',state:'offline',lastSeen:'2026-09-26T01:00:00Z'}],requested:1,refreshed:0,failures:[{id:'a',name:'Worker <A>'}]});
  await refreshing;
  assert.equal(state.snapshot.agents[0].state,'online');
  assert.equal(element('#agentRefreshStatus').dataset.error,'true');
  assert.match(element('#agentRefreshStatus').textContent,/Could not refresh: Worker <A>/);
});

test('HTTP errors and timeout keep the current list and unlock refresh', async () => {
  for (const failure of [new Error('Controller unavailable'),Object.assign(new Error(),{name:'AbortError'})]) {
    const {api,state,element,requests,timers} = fixture();
    const original = state.snapshot;
    const refreshing = api.refreshAgents();
    timers.get(20000)();
    assert.equal(requests[0].options.signal.aborted,true);
    requests[0].reject(failure);
    await refreshing;
    assert.equal(state.snapshot,original);
    assert.match(element('#agentRefreshStatus').textContent,/Showing the last known status/);
    assert.equal(element('#refreshAgents').disabled,false);
    assert.equal(state.agentsRefreshing,false);
  }
});

test('an empty Controller inventory explains that Agents must register', async () => {
  const {api,state,element,requests} = fixture();
  const refreshing = api.refreshAgents();
  requests[0].resolve({agents:[],requested:0,refreshed:0,failures:[]});
  await refreshing;
  assert.match(element('#agentRefreshStatus').textContent,/No Agents are registered/);
  assert.equal(state.snapshot.agents.length,0);
});
