const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

const source = fs.readFileSync(path.join(__dirname, 'static/app.js'), 'utf8');
const functions = source.slice(source.indexOf('function escapeHTML('), source.indexOf('$("#scenarioText").value = defaultScenario;'));
const receiptKey = 'kpl.pending-submission';

function fixture(respond, storage = new Map()) {
  const elements = new Map(), requests = [], toasts = [], tabs = [], timers = new Set(), redirects = [];
  let generatedKeys = 0;
  const element = selector => {
    if (!elements.has(selector)) elements.set(selector, {value:'', textContent:'', open:false,
      close() { this.open = false; }, focus() {}});
    return elements.get(selector);
  };
  element('#runRepetitions').value = '1';
  element('#scenarioText').value = 'version: 4\nname: synthetic\nseed: 42\nphases: [{action: stop-all}]\n';
  element('#scenarioDialog').open = true;
  const context = {
    Headers, AbortController, state:{scenarioEditorVersion:1}, $:element,
    crypto:{randomUUID:()=>`synthetic-key-${++generatedKeys}`},
    sessionStorage:{getItem:key=>storage.get(key) ?? null, setItem:(key,value)=>storage.set(key,value), removeItem:key=>storage.delete(key)},
    location:{replace:value=>redirects.push(value)},
    setTimeout(callback, delay) { const timer = {callback, delay}; timers.add(timer); return timer; },
    clearTimeout(timer) { timers.delete(timer); },
    KPLDashboardTabs:{show:value=>tabs.push(value)},
    async fetch(url, options) {
      const request = {url, body:options.body, key:options.headers.get('Idempotency-Key'), options};
      requests.push(request);
      return respond(request, requests.length);
    },
  };
  vm.createContext(context);
  vm.runInContext(functions, context);
  context.renderSavedScenarios = () => {};
  context.showToast = message => toasts.push(message);
  return {context, storage, requests, toasts, tabs, timers, redirects, element, generatedKeys:()=>generatedKeys};
}

const accepted = () => new Response(JSON.stringify({id:'run-confirmed', state:'queued', name:'synthetic', queuePosition:0}), {status:202});

function assertUnconfirmed(fixture) {
  assert.ok(fixture.storage.has(receiptKey), 'unconfirmed response discarded the idempotency receipt');
  assert.equal(fixture.element('#scenarioDialog').open, true);
  assert.equal(fixture.context.state.scenarioSubmitting, false);
  assert.deepEqual(fixture.toasts, []);
  assert.deepEqual(fixture.tabs, []);
  assert.ok(fixture.element('#scenarioError').textContent);
  assert.equal(fixture.timers.size, 0, 'completed request retained its timeout');
}

test('unconfirmed successful responses keep the submission receipt and retry the exact payload and key', async () => {
  for (const body of [null, {}, [], '', 'synthetic text', 42, true, {id:''}, {id:42}]) {
    const f = fixture((_, count) => count === 1 ? new Response(JSON.stringify(body), {status:202}) : accepted());
    await f.context.submitScenarioRun();
    assertUnconfirmed(f);
    assert.match(f.element('#scenarioError').textContent, /Unexpected experiment response/);
    await f.context.submitScenarioRun();
    assert.equal(f.requests.length, 2);
    assert.equal(f.requests[1].body, f.requests[0].body);
    assert.equal(f.requests[1].key, f.requests[0].key);
    assert.equal(f.generatedKeys(), 1);
    assert.equal(f.storage.has(receiptKey), false);
    assert.equal(f.element('#scenarioDialog').open, false);
    assert.deepEqual(f.tabs, ['experiments']);
    assert.equal(f.toasts.length, 1);
  }
});

test('unconfirmed submission receipts survive a reload and accept an already completed replay', async () => {
  const first = fixture(() => new Response('null', {status:202}));
  await first.context.submitScenarioRun();
  assertUnconfirmed(first);
  const replay = fixture(() => new Response(JSON.stringify({id:'run-confirmed', state:'completed', name:'synthetic'}), {status:200}), first.storage);
  await replay.context.submitScenarioRun();
  assert.equal(replay.requests[0].key, first.requests[0].key);
  assert.equal(replay.requests[0].body, first.requests[0].body);
  assert.equal(replay.generatedKeys(), 0);
  assert.equal(replay.storage.has(receiptKey), false);
  assert.equal(replay.toasts.length, 1);
});

test('HTTP rejection, malformed success, interrupted body and unknown transport outcomes retain the receipt', async () => {
  const responses = [
    ...[400, 409, 422, 503].map(status => () => new Response(JSON.stringify({error:'synthetic rejection'}), {status})),
    () => new Response(null, {status:204}),
    () => new Response('{broken', {status:202}),
    () => { throw new TypeError('synthetic transport interruption'); },
    () => ({ok:true, status:202, json:async()=>{ throw new DOMException('synthetic body interruption', 'AbortError'); }}),
  ];
  for (const response of responses) {
    const f = fixture(response);
    await f.context.submitScenarioRun();
    assertUnconfirmed(f);
  }
});

test('submission timeout preserves its receipt and reports an unknown outcome', async () => {
  const f = fixture(request => new Promise((resolve, reject) => request.options.signal.addEventListener('abort',
    () => reject(new DOMException('synthetic timeout', 'AbortError')), {once:true})));
  const task = f.context.submitScenarioRun();
  assert.equal(f.context.state.scenarioSubmitting, true);
  await f.context.submitScenarioRun();
  assert.equal(f.requests.length, 1, 'concurrent click submitted the same scenario twice');
  const timer = [...f.timers].find(timer => timer.delay === 30000);
  assert.ok(timer);
  timer.callback();
  await task;
  assertUnconfirmed(f);
  assert.match(f.element('#scenarioError').textContent, /may have been submitted/);
});

test('same-tab retries retain the payload and key when browser storage is unavailable', async () => {
  for (const mode of ['blocked', 'quota']) {
    const acceptedRuns = new Map();
    let lost = true;
    const f = fixture(request => {
      if (!acceptedRuns.has(request.key)) acceptedRuns.set(request.key, {id:'run-'+acceptedRuns.size, state:'queued', name:'synthetic'});
      if (lost) { lost = false; throw new TypeError('synthetic accepted response loss'); }
      return new Response(JSON.stringify(acceptedRuns.get(request.key)), {status:202});
    });
    f.context.sessionStorage = {
      getItem() { if (mode === 'blocked') throw new Error('synthetic blocked storage'); return null; },
      setItem() { throw new Error('synthetic storage quota'); },
      removeItem() { throw new Error('synthetic blocked storage'); },
    };
    await f.context.submitScenarioRun();
    assert.equal(f.element('#scenarioDialog').open, true);
    await f.context.submitScenarioRun();
    assert.equal(acceptedRuns.size, 1, 'storage failure duplicated accepted work');
    assert.equal(f.requests[0].key, f.requests[1].key);
    assert.equal(f.requests[0].body, f.requests[1].body);
    assert.equal(f.context.state.scenarioSubmissionReceipt, null);
    f.element('#scenarioDialog').open = true;
    await f.context.submitScenarioRun();
    assert.equal(acceptedRuns.size, 2, 'confirmed work prevented a deliberate fresh submission');
  }
});

test('a confirmed receipt is not resurrected when browser storage cannot remove it', async () => {
  const f = fixture(accepted);
  f.context.sessionStorage.removeItem = () => { throw new Error('synthetic remove denied'); };
  await f.context.submitScenarioRun();
  assert.ok(f.storage.has(receiptKey));
  f.element('#scenarioDialog').open = true;
  await f.context.submitScenarioRun();
  assert.notEqual(f.requests[1].key, f.requests[0].key);
});

test('changed input starts a new submission after an unknown outcome without browser storage', async () => {
  const f = fixture(() => { throw new TypeError('synthetic transport interruption'); });
  f.context.sessionStorage = undefined;
  await f.context.submitScenarioRun();
  f.element('#scenarioText').value += '# deliberate changed input\n';
  await f.context.submitScenarioRun();
  assert.notEqual(f.requests[1].key, f.requests[0].key);
  assert.notEqual(f.requests[1].body, f.requests[0].body);
});
