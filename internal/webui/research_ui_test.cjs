const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

function fixture({ request = async () => [], render = async () => {}, schedule = setTimeout, cancelTimer = clearTimeout } = {}) {
  const elements = new Map(), statuses = [];
  let parses = 0;
  const element = selector => {
    if (!elements.has(selector)) elements.set(selector, {
      innerHTML: '', value: '', hidden: false, listeners: {},
      addEventListener(type, listener) { this.listeners[type] = listener; },
    });
    return elements.get(selector);
  };
  const sandbox = {
    AbortController, DOMException, setTimeout:schedule, clearTimeout:cancelTimer,
    KPLResearch: { labels: { frt: 'First receipt' } },
    KPLResultImages: { jobDescription: job => job.state },
    KPLResearchCompare: { buildCharts: () => [] },
    KPLResearchFiles: { parseImport() {
      parses++;
      return { entries: [{ case: 'imported', analysis: { result: { name: 'late import' } } }], curves: [] };
    } },
  };
  vm.createContext(sandbox);
  vm.runInContext(fs.readFileSync(path.join(__dirname, 'static/research-ui.js'), 'utf8'), sandbox);
  const ui = sandbox.KPLResearchTools.create({
    document: { querySelector: element }, request, render,
    status: message => statuses.push(message), onJob() {}, getData: () => null, escape: value => String(value ?? ''),
  });
  return { element, statuses, ui, parses: () => parses };
}
const settled = () => new Promise(resolve => setImmediate(resolve));

async function selectSavedComparison(fixture) {
  fixture.element('#loadResearchRuns').listeners.click();
  await settled();
  fixture.element('#researchRuns').querySelectorAll = () => [{
    dataset:{researchRow:'0'},
    querySelector:selector=>({
      'input[type="checkbox"]':{checked:true},
      '[data-field="series"]':{value:'Series'},
      '[data-field="case"]':{value:'Case'},
      '[data-field="params"]':{value:''},
    })[selector],
  }];
}

for (const state of ['idle', 'completed', 'failed', 'interrupted', 'canceled']) {
  test(`comparison admission pins the observed ${state} analysis attempt`, async () => {
    const observed = {runId:'run', id:state === 'idle' ? '' : 'observed', state, stale:state === 'completed'};
    let posts = 0, rendered = 0;
    const f = fixture({
      request:async(url, options)=>{
        if (url === '/api/v1/results') return [{id:'run', name:'Run', state:'completed'}];
        if (options.method === 'POST') {
          posts++;
          assert.equal(options.headers['Content-Type'], 'application/json');
          assert.deepEqual(JSON.parse(options.body), {expectedAnalysis:{id:observed.id, state}});
          return {runId:'run', id:'confirmed', state:'completed'};
        }
        if (url.includes('/summary?')) return {result:{id:'run'}, analysisId:'confirmed'};
        return observed;
      },
      render:async()=>{ rendered++; },
    });
    await selectSavedComparison(f);
    f.element('#drawResearchCompare').listeners.click();
    await settled();
    assert.equal(posts, 1);
    assert.equal(rendered, 1, f.statuses.at(-1));
  });
}

test('comparison does not admit another attempt after the observed analysis changes', async () => {
  const observed = {runId:'run', id:'', state:'idle'};
  const current = {runId:'run', id:'external-fast-failure', state:'failed'};
  let posts = 0, admitted = 0, summaries = 0, rendered = 0;
  const f = fixture({
    request:async(url, options)=>{
      if (url === '/api/v1/results') return [{id:'run', name:'Run', state:'completed'}];
      if (options.method === 'POST') {
        posts++;
        const expected = options.body && JSON.parse(options.body).expectedAnalysis;
        // Match the Controller's optional-expectation compatibility contract.
        if (expected && (expected.id !== current.id || expected.state !== current.state)) {
          throw Object.assign(new Error('Analysis attempt changed; refresh its status before retrying.'),
            {status:409, code:'analysis-attempt-changed'});
        }
        admitted++;
        return {runId:'run', id:'unobserved-retry', state:'completed'};
      }
      if (url.includes('/summary?')) { summaries++; return {result:{id:'run'}, analysisId:'unobserved-retry'}; }
      return observed;
    },
    render:async()=>{ rendered++; },
  });
  await selectSavedComparison(f);
  f.element('#drawResearchCompare').listeners.click();
  await settled();
  assert.equal(posts, 1);
  assert.equal(admitted, 0, 'a delayed comparison overwrote the newly failed attempt');
  assert.equal(summaries, 0);
  assert.equal(rendered, 0);
  assert.match(f.statuses.at(-1), /attempt changed; refresh/);
});

for (const state of ['running', 'queued', 'completed']) {
  test(`comparison joins an existing ${state} analysis without another admission`, async () => {
    let posts = 0, rendered = 0, reads = 0;
    const f = fixture({
      schedule:setImmediate, cancelTimer:clearImmediate,
      request:async(url, options)=>{
        if (url === '/api/v1/results') return [{id:'run', name:'Run', state:'completed'}];
        if (options.method === 'POST') posts++;
        if (url.includes('/summary?')) return {result:{id:'run'}, analysisId:'existing'};
        return {runId:'run', id:'existing', state:reads++ === 0 ? state : 'completed'};
      },
      render:async()=>{ rendered++; },
    });
    await selectSavedComparison(f);
    f.element('#drawResearchCompare').listeners.click();
    for (let attempt=0; attempt<4 && !rendered; attempt++) await settled();
    assert.equal(posts, 0);
    assert.equal(rendered, 1, f.statuses.at(-1));
  });
}

for (const failure of [Object.assign(new Error('synthetic missing job'), {status:404}), new TypeError('synthetic transport failure')]) {
  test(`comparison stops after ${failure.status === 404 ? 'a missing job' : 'a transport failure'} without submitting analysis`, async () => {
    let posts = 0, rendered = 0;
    const f = fixture({
      request:async(url, options)=>{
        if (url === '/api/v1/results') return [{id:'run', name:'Run', state:'completed'}];
        if (options.method === 'POST') posts++;
        throw failure;
      },
      render:async()=>{ rendered++; },
    });
    await selectSavedComparison(f);
    f.element('#drawResearchCompare').listeners.click();
    await settled();
    assert.equal(posts, 0);
    assert.equal(rendered, 0);
    assert.equal(f.statuses.at(-1), failure.message);
  });
}

test('clearing the active analysis releases message options while retaining imported files', async () => {
  const { ui, element } = fixture();
  element('#researchFiles').listeners.change({ target: { files: [
    { name: 'complete.json', size: 2, text: async () => '{}' },
  ] } });
  await settled();
  const imports = element('#researchRuns').innerHTML;
  ui.setData({ research: { messages: [{ topic: 'topic', id: 'message' }] } });
  assert.match(element('#researchMessage').innerHTML, /topic \/ message/);
  ui.clearData();
  assert.equal(element('#researchMessage').innerHTML, '');
  assert.equal(element('#drawResearchMessage').disabled, true);
  assert.equal(element('#researchTools').hidden, true);
  assert.equal(element('#researchRuns').innerHTML, imports);
});

for (const [stale, analysisVersion] of [[true, 5], [true, 6], [false, 6]]) {
  test(`comparison checks the server stale marker before loading summaries: stale=${stale}, version=${analysisVersion}`, async () => {
    const requests = [];
    let rendered;
    const expectedID = stale ? 'updated' : 'cached';
    const { element, statuses } = fixture({
      request: async (url, options) => {
        requests.push([url, options.method || 'GET']);
        if (url === '/api/v1/results') return [{ id: 'run', name: 'Run', state: 'completed' }];
        if (url.includes('/summary?')) {
          assert.equal(url, `/api/v1/analysis-jobs/run/summary?jobId=${expectedID}`);
          return { result: { id: 'run' }, analysisId: expectedID };
        }
        if (options.method === 'POST') return { runId: 'run', id: 'updated', state: 'completed', analysisVersion: 6 };
        return { runId: 'run', id: 'cached', state: 'completed', analysisVersion, stale };
      },
      render: async (_charts, _name, extra) => { rendered = extra; },
    });
    element('#loadResearchRuns').listeners.click();
    await settled();
    element('#researchRuns').querySelectorAll = () => [{
      dataset: { researchRow: '0' },
      querySelector: selector => ({
        'input[type="checkbox"]': { checked: true },
        '[data-field="series"]': { value: 'Series' },
        '[data-field="case"]': { value: 'Case' },
        '[data-field="params"]': { value: '' },
      })[selector],
    }];
    element('#drawResearchCompare').listeners.click();
    await settled();
    assert.equal(requests.filter(([, method]) => method === 'POST').length, stale ? 1 : 0);
    assert.equal(rendered?.entries[0].analysis.analysisId, expectedID, statuses.at(-1));
  });
}

test('clearing imports cancels a pending file read before it can restore cleared data', async () => {
  const { element, statuses, parses } = fixture();
  let resolve;
  element('#researchFiles').listeners.change({ target: { files: [
    { name: 'late.json', size: 20, text: () => new Promise(yes => { resolve = yes; }) },
  ] } });
  element('#clearResearchImports').listeners.click();
  resolve('{}');
  await settled();
  assert.equal(parses(), 0, 'cleared file was still parsed');
  assert.equal(element('#researchRuns').innerHTML, '');
  assert.equal(statuses.at(-1), 'Imported data cleared.');
});

test('an older saved-results response cannot replace the latest comparison selection', async () => {
  const pending = [];
  const { element, statuses } = fixture({ request: () => new Promise(resolve => pending.push(resolve)) });
  element('#loadResearchRuns').listeners.click();
  element('#loadResearchRuns').listeners.click();
  pending[1]([{ id: 'new', name: 'Latest result', state: 'completed' }]);
  await settled();
  const rows = element('#researchRuns').innerHTML;
  const count = statuses.length;
  pending[0]([{ id: 'old', name: 'Old result', state: 'completed' }]);
  await settled();
  assert.equal(element('#researchRuns').innerHTML, rows);
  assert.equal(statuses.length, count);
});

for (const pendingPhase of ['job', 'summary']) {
  test(`closing comparison during ${pendingPhase} prevents late analysis or rendering`, async () => {
    let resolve, rendered = 0;
    const requests = [];
    const job = { runId: 'run', id: 'cached', state: 'completed', stale: pendingPhase === 'job' };
    const { element, ui } = fixture({
      request: async (url, options) => {
        requests.push([url, options.method || 'GET']);
        if (url === '/api/v1/results') return [{ id: 'run', name: 'Run', state: 'completed' }];
        if (url.includes('/summary?') || pendingPhase === 'job' && options.method !== 'POST')
          return new Promise(yes => { resolve = yes; });
        return job;
      },
      render: async () => { rendered++; },
    });
    element('#loadResearchRuns').listeners.click();
    await settled();
    element('#researchRuns').querySelectorAll = () => [{
      dataset: { researchRow: '0' },
      querySelector: selector => ({
        'input[type="checkbox"]': { checked: true },
        '[data-field="series"]': { value: 'Series' },
        '[data-field="case"]': { value: 'Case' },
        '[data-field="params"]': { value: '' },
      })[selector],
    }];
    element('#drawResearchCompare').listeners.click();
    await settled();
    const requestCount = requests.length;
    ui.cancel();
    resolve(pendingPhase === 'job' ? job : { result: { id: 'run' }, analysisId: job.id });
    await settled();
    assert.equal(requests.length, requestCount, 'closed view issued another analysis request');
    assert.equal(rendered, 0, 'closed view replaced the current images');
  });
}

test('closing the image view cancels a pending import while completed imports remain usable', async () => {
  const { element, ui, parses } = fixture();
  element('#researchFiles').listeners.change({ target: { files: [
    { name: 'complete.json', size: 2, text: async () => '{}' },
  ] } });
  await settled();
  assert.equal(parses(), 1);
  assert.match(element('#researchRuns').innerHTML, /late import/);
  const rows = element('#researchRuns').innerHTML;
  let resolve;
  element('#researchFiles').listeners.change({ target: { files: [
    { name: 'canceled.json', size: 2, text: () => new Promise(yes => { resolve = yes; }) },
  ] } });
  ui.cancel();
  resolve('{}');
  await settled();
  assert.equal(parses(), 1);
  assert.equal(element('#researchRuns').innerHTML, rows);
});
