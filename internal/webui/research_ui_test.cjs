const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

function fixture({ request = async () => [], render = async () => {} } = {}) {
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
    AbortController, DOMException, setTimeout, clearTimeout,
    KPLResearch: { labels: { frt: 'First receipt' } },
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
