// Run with: node --test internal/webui/scenario_library_test.cjs
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

const source = fs.readFileSync(path.join(__dirname, 'static/app.js'), 'utf8');
const functions = source.slice(source.indexOf('function escapeHTML('), source.indexOf('$("#scenarioText").value = defaultScenario;'));
const markup = fs.readFileSync(path.join(__dirname, 'static/index.html'), 'utf8');

function element(value = '') {
  const classes = new Set();
  const style = { height: '', removeProperty(name) { if (name === 'height') this.height = ''; } };
  return {
    value,
    disabled: false,
    hidden: false,
    textContent: '',
    innerHTML: '',
    style,
    boxHeight: 0,
    attributes: {},
    dataset: {},
    classList: {
      add: (name) => classes.add(name),
      remove: (name) => classes.delete(name),
      toggle(name, enabled) { enabled ? classes.add(name) : classes.delete(name); },
      contains: (name) => classes.has(name),
    },
    setAttribute(name, next) { this.attributes[name] = String(next); },
    getAttribute(name) { return this.attributes[name] ?? null; },
    focus() { this.focused = true; },
    showModal() { this.open = true; },
    close(value) { this.open = false; this.closedWith = value ?? ''; },
    getBoundingClientRect() { return { height: this.boxHeight }; },
  };
}

function response(body, status = 200) {
  return {
    ok: status >= 200 && status < 300,
    status,
    statusText: status === 404 ? 'Not Found' : status === 401 ? 'Unauthorized' : 'OK',
    async json() { return body; },
  };
}

function fixture(fetch) {
  const elements = new Map();
  for (const id of [
    'scenarioSearch', 'clearScenarioSearch', 'scenarioLibraryCount', 'scenarioLibrarySummary', 'scenarioListError', 'scenarioEditorHeading',
    'scenarioLibraryStatus', 'refreshScenarios', 'newScenario', 'chooseScenarioFile', 'scenarioFile', 'saveScenario', 'saveScenarioCopy',
    'scenarioLibraryError', 'scenarioEditingStatus', 'scenarioLibraryList', 'scenarioName',
    'scenarioText', 'scenarioError', 'runRepetitions', 'runScenario', 'scenarioDialog', 'toast',
    'validateScenario', 'scenarioValidation', 'scenarioValidationTitle', 'scenarioValidationMessage',
    'scenarioImportReview', 'scenarioImportList', 'scenarioImportSummary', 'scenarioImportDestination', 'scenarioImportHeading',
    'confirmScenarioImport', 'cancelScenarioImport',
  ]) elements.set(`#${id}`, element());
  elements.set('.scenario-library', element());
  elements.set('.scenario-workspace', element());
  elements.set('.agents-panel', element());
  elements.set('.events-panel', element());
  elements.get('#scenarioText').value = 'version: 1\nname: current\n';
  elements.get('#runRepetitions').value = '1';
  elements.get('#scenarioDialog').open = true;
  const closes = [element(), element()];
  const actions = new Map();
  const addAction = (attribute, id) => {
    const button = element();
    button.setAttribute(attribute, id);
    const selector = `[${attribute}]`;
    if (!actions.has(selector)) actions.set(selector, []);
    actions.get(selector).push(button);
    return button;
  };
  const state = {
    savedScenarios: null,
    scenariosLoading: false,
    scenariosError: '',
    scenarioActionError: '',
    selectedScenarioId: null,
    scenarioLoadingId: null,
    scenarioImporting: null,
    scenarioImportBatch: null,
    scenarioSaving: false,
    scenarioDeletingId: null,
    pendingScenarioDeleteId: null,
    scenarioLoadVersion: 0,
    scenarioSubmitting: false,
    scenarioEditorVersion: 0,
    scenarioValidating: false,
    scenarioValidation: null,
    scenarioValidationVersion: 0,
  };
  const storage = new Map();
  const viewport = { mobile: false };
  const requestTimers = [];
  const schedule = (callback, delay) => {
    if (delay === 2600) return { active: false };
    if (delay === 30000) {
      const timer = { active: true, callback };
      requestTimers.push(timer);
      return timer;
    }
    return setTimeout(callback, delay);
  };
  const cancel = (timer) => {
    if (timer && typeof timer === 'object' && Object.hasOwn(timer, 'active')) timer.active = false;
    else clearTimeout(timer);
  };
  const sandbox = {
    state,
    defaultScenario: 'version: 1\nname: default\n',
    $: (selector) => elements.get(selector),
    document: { querySelectorAll: (selector) => selector === '[data-scenario-close]' ? closes : actions.get(selector) || [] },
    window: { matchMedia: () => ({ matches: viewport.mobile }) },
    localStorage: {
      getItem: (key) => storage.get(key) || null,
      setItem: (key, value) => storage.set(key, value),
    },
    fetch,
    Headers,
    AbortController,
    TextDecoder,
    Intl,
    Date,
    setTimeout: schedule,
    clearTimeout: cancel,
  };
  vm.createContext(sandbox);
  vm.runInContext(functions, sandbox);
  const expireRequest = () => {
    const timer = requestTimers.find((candidate) => candidate.active);
    assert.ok(timer, 'no active request timeout');
    timer.active = false;
    timer.callback();
  };
  return { api: sandbox, state, elements, closes, storage, viewport, addAction, expireRequest };
}

function hangingResponse(options) {
  return new Promise((resolve, reject) => {
    options.signal.addEventListener('abort', () => {
      const error = new Error('aborted');
      error.name = 'AbortError';
      reject(error);
    }, { once: true });
  });
}

function scenarioFile(name, content) {
  const bytes = Buffer.isBuffer(content) ? content : Buffer.from(content, 'utf8');
  return {
    name,
    size: bytes.byteLength,
    async arrayBuffer() { return bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength); },
  };
}

function deferredScenarioFile(name, content) {
  const file = scenarioFile(name, content);
  const read = file.arrayBuffer.bind(file);
  let resolve;
  let reject;
  file.arrayBuffer = () => new Promise((done, failed) => { resolve = done; reject = failed; });
  return { file, finish: async () => resolve(await read()), fail: (error) => reject(error) };
}

test('scenario form has explicit non-submitting close controls', () => {
  const form = markup.slice(markup.indexOf('<form class="dialog-shell" id="scenarioForm">'), markup.indexOf('</form>', markup.indexOf('id="scenarioForm"')));
  assert.ok(form);
  assert.ok(!/<form[^>]+method=/.test(form));
  assert.equal((form.match(/type="button" data-scenario-close/g) || []).length, 2);
  assert.match(source, /#scenarioForm"\)\.addEventListener\("submit", \(event\) => event\.preventDefault\(\)\)/);
});

test('saved scenario list escapes server values and exposes the selected edit state', () => {
  const { api, state, elements } = fixture(async () => response([]));
  state.savedScenarios = [{
    id: 'id"><svg/onload=bad>',
    name: '<script>bad()</script>',
    updatedAt: '2026-09-05T03:04:05Z',
  }];
  state.selectedScenarioId = state.savedScenarios[0].id;
  api.renderSavedScenarios();

  const html = elements.get('#scenarioLibraryList').innerHTML;
  assert.ok(!html.includes('<script>'));
  assert.ok(!html.includes('<svg/onload'));
  assert.match(html, /&lt;script&gt;bad\(\)&lt;\/script&gt;/);
  assert.match(html, /aria-current="true"/);
  assert.equal(elements.get('#saveScenario').textContent, 'Save changes');
  assert.equal(elements.get('#saveScenarioCopy').hidden, false);
});

test('scenario list refresh is de-duplicated and accepts summaries without YAML', async () => {
  let release;
  let calls = 0;
  const pending = new Promise((resolve) => { release = resolve; });
  const { api, state, elements } = fixture(async (url, options) => {
    calls++;
    assert.equal(url, '/api/v1/scenarios');
    assert.equal(options.cache, 'no-store');
    await pending;
    return response([{ id: 'one', name: 'Baseline', createdAt: '2026-09-05T00:00:00Z', updatedAt: '2026-09-05T01:00:00Z' }]);
  });

  const first = api.refreshSavedScenarios();
  const duplicate = api.refreshSavedScenarios();
  assert.equal(calls, 1);
  assert.equal(elements.get('#scenarioText').disabled, true);
  assert.equal(elements.get('#runScenario').disabled, true);
  release();
  await Promise.all([first, duplicate]);
  assert.equal(state.savedScenarios.length, 1);
  assert.equal(state.savedScenarios[0].name, 'Baseline');
  assert.equal(state.scenariosLoading, false);
});

test('loading a saved scenario fetches detail before filling the editor', async () => {
  let release;
  let calls = 0;
  const pending = new Promise((resolve) => { release = resolve; });
  const { api, state, elements } = fixture(async (url, options) => {
    calls++;
    assert.equal(url, '/api/v1/scenarios/run%2Fone');
    assert.equal(options.cache, 'no-store');
    await pending;
    return response({ id: 'run/one', name: 'Loaded name', yaml: 'version: 1\nname: loaded\n', createdAt: 'now', updatedAt: 'now' });
  });
  state.savedScenarios = [{ id: 'run/one', name: 'Summary only', updatedAt: 'now' }];

  const first = api.loadSavedScenario('run/one');
  const duplicate = api.loadSavedScenario('run/one');
  const conflictingRun = api.submitScenarioRun();
  assert.equal(calls, 1);
  assert.equal(elements.get('#scenarioText').value, 'version: 1\nname: current\n');
  assert.equal(elements.get('#scenarioText').disabled, true);
  assert.equal(elements.get('#scenarioName').disabled, true);
  assert.equal(elements.get('#runScenario').disabled, true);
  release();
  await Promise.all([first, duplicate, conflictingRun]);
  assert.equal(state.selectedScenarioId, 'run/one');
  assert.equal(elements.get('#scenarioName').value, 'Loaded name');
  assert.equal(elements.get('#scenarioText').value, 'version: 1\nname: loaded\n');
  assert.equal(elements.get('#scenarioText').focused, true);
  assert.equal(elements.get('#scenarioText').disabled, false);
});

test('saving creates, updates, and copies scenarios with editable names', async () => {
  const requests = [];
  const replies = [
    { id: 'created', name: 'New name', yaml: 'version: 1\nname: run\n', createdAt: 'one', updatedAt: 'one' },
    { id: 'created', name: 'Renamed', yaml: 'version: 1\nname: run\n', createdAt: 'one', updatedAt: 'two' },
    { id: 'copy', name: 'Renamed', yaml: 'version: 1\nname: run\n', createdAt: 'three', updatedAt: 'three' },
  ];
  const { api, state, elements } = fixture(async (url, options) => {
    requests.push({ url, method: options.method, body: JSON.parse(options.body) });
    return response(replies.shift());
  });
  elements.get('#scenarioName').value = '  New name  ';
  elements.get('#scenarioText').value = 'version: 1\nname: run\n';

  await api.saveEditedScenario(false);
  assert.deepEqual(requests[0], { url: '/api/v1/scenarios', method: 'POST', body: { name: 'New name', yaml: 'version: 1\nname: run\n' } });
  assert.equal(state.selectedScenarioId, 'created');

  elements.get('#scenarioName').value = 'Renamed';
  state.savedScenarios.unshift({ id: 'other', name: 'Other', updatedAt: 'between' });
  await api.saveEditedScenario(false);
  assert.deepEqual(requests[1], { url: '/api/v1/scenarios/created', method: 'PUT', body: { name: 'Renamed', yaml: 'version: 1\nname: run\n' } });
  assert.equal(state.savedScenarios[0].name, 'Renamed');
  assert.equal(state.savedScenarios[0].id, 'created');

  await api.saveEditedScenario(true);
  assert.deepEqual(requests[2], { url: '/api/v1/scenarios', method: 'POST', body: { name: 'Renamed', yaml: 'version: 1\nname: run\n' } });
  assert.equal(state.selectedScenarioId, 'copy');
  assert.deepEqual(Array.from(state.savedScenarios, (item) => item.id), ['copy', 'created', 'other']);
});

test('blank scenario names fail locally without sending a request', async () => {
  let calls = 0;
  const { api, state, elements } = fixture(async () => { calls++; return response({}); });
  elements.get('#scenarioName').value = '   ';
  await api.saveEditedScenario(false);
  assert.equal(calls, 0);
  assert.match(state.scenarioActionError, /Enter a name/);
  assert.equal(elements.get('#scenarioName').focused, true);
});

test('repeated save clicks share one in-flight mutation', async () => {
  let calls = 0;
  let release;
  const pending = new Promise((resolve) => { release = resolve; });
  const { api, state, elements } = fixture(async () => {
    calls++;
    await pending;
    return response({ id: 'once', name: 'One request', yaml: 'version: 1\nname: once\n', createdAt: 'one', updatedAt: 'one' });
  });
  elements.get('#scenarioName').value = 'One request';
  elements.get('#scenarioText').value = 'version: 1\nname: once\n';

  const first = api.saveEditedScenario(false);
  const duplicate = api.saveEditedScenario(false);
  assert.equal(calls, 1);
  assert.equal(state.scenarioSaving, true);
  release();
  await Promise.all([first, duplicate]);
  assert.equal(state.scenarioSaving, false);
  assert.equal(state.savedScenarios.length, 1);
});

test('submitting locks conflicting controls but allows closing and reopening the editor', async () => {
  let calls = 0;
  let release;
  const pending = new Promise((resolve) => { release = resolve; });
  const { api, state, elements, closes } = fixture(async (url, options) => {
    calls++;
    assert.equal(url, '/api/v1/experiments');
    assert.equal(options.method, 'POST');
    await pending;
    return response({ id: 'run-one', name: 'Running scenario' });
  });
  state.savedScenarios = [{ id: 'saved', name: 'Saved', updatedAt: 'now' }];

  const run = api.submitScenarioRun();
  assert.equal(calls, 1);
  assert.equal(state.scenarioSubmitting, true);
  for (const id of ['#scenarioName', '#scenarioText', '#runRepetitions', '#runScenario', '#saveScenario', '#refreshScenarios', '#newScenario']) {
    assert.equal(elements.get(id).disabled, true, `${id} remained enabled`);
  }
  assert.ok(closes.every((button) => !button.disabled));
  await Promise.all([
    api.refreshSavedScenarios(),
    api.saveEditedScenario(false),
    api.loadSavedScenario('saved'),
    api.confirmScenarioDeletion('saved'),
  ]);
  api.requestScenarioDeletion('saved');
  api.closeScenarioEditor();
  assert.equal(calls, 1);
  assert.equal(elements.get('#scenarioDialog').open, false);
  assert.equal(elements.get('#scenarioDialog').closedWith, 'cancel');
  api.openScenarioEditor();
  assert.equal(elements.get('#scenarioDialog').open, true);
  assert.equal(calls, 1, 'reopening must not send a conflicting request');

  release();
  await run;
  assert.equal(state.scenarioSubmitting, false);
  assert.equal(elements.get('#scenarioDialog').open, true, 'late submission must not close the reopened editor');
  assert.equal(elements.get('#runScenario').disabled, false);
  assert.ok(closes.every((button) => !button.disabled));
});

test('a timed-out list Refresh releases controls with a clear read error', async () => {
  const { api, state, elements, closes, expireRequest } = fixture((url, options) => hangingResponse(options));
  const refreshing = api.refreshSavedScenarios();
  assert.equal(state.scenariosLoading, true);
  expireRequest();
  await refreshing;

  assert.equal(state.scenariosLoading, false);
  assert.equal(state.scenariosError, 'Request timed out.');
  assert.equal(elements.get('#refreshScenarios').disabled, false);
  assert.equal(elements.get('#scenarioText').disabled, false);
  assert.ok(closes.every((button) => !button.disabled));
});

test('a timed-out Load releases the editor and close controls', async () => {
  const { api, state, elements, closes, expireRequest } = fixture((url, options) => hangingResponse(options));
  state.savedScenarios = [{ id: 'load-timeout', name: 'Slow load', updatedAt: 'now' }];
  const loading = api.loadSavedScenario('load-timeout');
  assert.equal(elements.get('#scenarioText').disabled, true);
  expireRequest();
  await loading;

  assert.equal(state.scenarioLoadingId, null);
  assert.match(state.scenarioActionError, /Could not load the saved scenario: Request timed out\./);
  assert.equal(elements.get('#scenarioText').disabled, false);
  assert.equal(elements.get('#runScenario').disabled, false);
  assert.ok(closes.every((button) => !button.disabled));
});

test('a timed-out Save warns about uncertain completion and releases controls', async () => {
  const { api, state, elements, closes, expireRequest } = fixture((url, options) => hangingResponse(options));
  elements.get('#scenarioName').value = 'Slow save';
  const saving = api.saveEditedScenario(false);
  assert.equal(state.scenarioSaving, true);
  expireRequest();
  await saving;

  assert.equal(state.scenarioSaving, false);
  assert.match(state.scenarioActionError, /server may have completed this operation\. Refresh the saved scenario list/);
  assert.equal(elements.get('#saveScenario').disabled, false);
  assert.equal(elements.get('#scenarioText').disabled, false);
  assert.ok(closes.every((button) => !button.disabled));
});

test('a timed-out Delete preserves confirmation, warns about uncertain completion, and releases controls', async () => {
  const { api, state, elements, closes, addAction, expireRequest } = fixture((url, options) => hangingResponse(options));
  const confirm = addAction('data-confirm-scenario-delete', 'delete-timeout');
  state.savedScenarios = [{ id: 'delete-timeout', name: 'Slow delete', updatedAt: 'now' }];
  api.requestScenarioDeletion('delete-timeout');
  const deleting = api.confirmScenarioDeletion('delete-timeout');
  assert.equal(state.scenarioDeletingId, 'delete-timeout');
  expireRequest();
  await deleting;

  assert.equal(state.scenarioDeletingId, null);
  assert.equal(state.pendingScenarioDeleteId, 'delete-timeout');
  assert.match(state.scenarioActionError, /server may have completed this operation\. Refresh the saved scenario list/);
  assert.equal(elements.get('#runScenario').disabled, false);
  assert.ok(closes.every((button) => !button.disabled));
  assert.equal(confirm.focused, true);
});

test('a timed-out Run directs the user to Experiment progress and releases controls', async () => {
  const { api, state, elements, closes, expireRequest } = fixture((url, options) => hangingResponse(options));
  const running = api.submitScenarioRun();
  assert.equal(state.scenarioSubmitting, true);
  expireRequest();
  await running;

  assert.equal(state.scenarioSubmitting, false);
  assert.match(elements.get('#scenarioError').textContent, /experiment may have been submitted\. Check Experiment progress/);
  assert.equal(elements.get('#runScenario').disabled, false);
  assert.equal(elements.get('#scenarioText').disabled, false);
  assert.ok(closes.every((button) => !button.disabled));
  assert.equal(elements.get('#scenarioDialog').closedWith, undefined);
});

test('Enter in the saved-name field saves without submitting or closing the dialog', async () => {
  let calls = 0;
  const { api, state, elements } = fixture(async (url, options) => {
    calls++;
    assert.equal(url, '/api/v1/scenarios');
    assert.equal(options.method, 'POST');
    return response({ id: 'entered', name: 'Keyboard save', yaml: 'version: 1\nname: current\n', createdAt: 'now', updatedAt: 'now' });
  });
  elements.get('#scenarioName').value = 'Keyboard save';
  let prevented = 0;
  api.handleScenarioNameKeydown({ key: 'Enter', isComposing: false, preventDefault() { prevented++; } });
  await new Promise((resolve) => setImmediate(resolve));
  assert.equal(prevented, 1);
  assert.equal(calls, 1);
  assert.equal(state.selectedScenarioId, 'entered');
  assert.equal(elements.get('#scenarioDialog').closedWith, undefined);

  api.handleScenarioNameKeydown({ key: 'Enter', isComposing: true, preventDefault() { prevented++; } });
  assert.equal(prevented, 1);
  assert.equal(calls, 1);
});

test('scenario deletion requires an inline confirmation and retains loaded editor text', async () => {
  let calls = 0;
  const { api, state, elements, addAction } = fixture(async (url, options) => {
    calls++;
    assert.equal(url, '/api/v1/scenarios/selected');
    assert.equal(options.method, 'DELETE');
    return response(null, 204);
  });
  const confirm = addAction('data-confirm-scenario-delete', 'selected');
  const deleteSelected = addAction('data-delete-scenario', 'selected');
  const deleteNext = addAction('data-delete-scenario', 'next');
  state.savedScenarios = [
    { id: 'selected', name: 'Selected', updatedAt: 'now' },
    { id: 'next', name: 'Next', updatedAt: 'earlier' },
  ];
  state.selectedScenarioId = 'selected';
  const yaml = elements.get('#scenarioText').value;

  api.requestScenarioDeletion('selected');
  assert.equal(calls, 0);
  assert.equal(state.pendingScenarioDeleteId, 'selected');
  assert.match(elements.get('#scenarioLibraryList').innerHTML, /Confirm delete/);
  assert.equal(confirm.focused, true);
  api.cancelScenarioDeletion('selected');
  assert.equal(deleteSelected.focused, true);
  api.requestScenarioDeletion('selected');
  await api.confirmScenarioDeletion('selected');
  assert.equal(calls, 1);
  assert.deepEqual(Array.from(state.savedScenarios, (item) => item.id), ['next']);
  assert.equal(state.selectedScenarioId, null);
  assert.equal(elements.get('#scenarioText').value, yaml);
  assert.equal(deleteNext.focused, true);
});


test('Validate checks the current YAML without saving or running and keeps the editor editable', async () => {
  let calls = 0;
  let release;
  const waiting = new Promise((resolve) => { release = resolve; });
  const { api, state, elements } = fixture(async (url, options) => {
    calls++;
    assert.equal(url, '/api/v1/scenarios/validate');
    assert.equal(options.method, 'POST');
    assert.equal(options.headers.get('Content-Type'), 'application/yaml');
    assert.equal(options.body, elements.get('#scenarioText').value);
    await waiting;
    return response({ valid: true, name: 'Checked scenario', phases: 3 });
  });
  const validating = api.validateEditedScenario();
  await api.validateEditedScenario();
  assert.equal(calls, 1);
  assert.equal(elements.get('#validateScenario').disabled, true);
  assert.equal(elements.get('#scenarioText').disabled, false);
  assert.equal(elements.get('#scenarioValidation').hidden, false);
  release();
  await validating;
  assert.equal(state.scenarioValidating, false);
  assert.equal(state.scenarioValidation.kind, 'success');
  assert.equal(elements.get('#validateScenario').disabled, false);
  assert.equal(elements.get('#scenarioValidationTitle').textContent, 'Scenario is valid');
  assert.match(elements.get('#scenarioValidationMessage').textContent, /Checked scenario · 3 phases/);
  assert.equal(elements.get('#scenarioDialog').closedWith, undefined);
  assert.equal(state.savedScenarios, null);
});

test('validation shows multiline parser diagnostics as text and editing clears the result', async () => {
  const diagnostic = 'yaml: unmarshal errors:\n  line 4: field <img src=x> not found\n  line 8: invalid duration';
  const { api, state, elements } = fixture(async () => response({ error: diagnostic }, 400));
  await api.validateEditedScenario();
  assert.equal(state.scenarioValidation.kind, 'invalid');
  assert.equal(elements.get('#scenarioValidationMessage').textContent, diagnostic);
  assert.equal(elements.get('#scenarioValidationMessage').innerHTML, '');
  assert.equal(elements.get('#scenarioValidation').getAttribute('role'), 'alert');
  assert.equal(elements.get('#scenarioText').getAttribute('aria-invalid'), 'true');
  elements.get('#scenarioText').value = 'fixed';
  api.resetScenarioValidation();
  assert.equal(state.scenarioValidation, null);
  assert.equal(elements.get('#scenarioValidation').hidden, true);
  assert.equal(elements.get('#scenarioText').getAttribute('aria-invalid'), 'false');
  assert.match(source, /#scenarioText"\)\.addEventListener\("input", resetScenarioValidation\)/);
});

test('old validation responses cannot overwrite results for edited YAML', async () => {
  let finishOld;
  let finishNew;
  let calls = 0;
  const { api, state, elements } = fixture(async () => {
    calls++;
    return new Promise((resolve) => {
      if (calls === 1) finishOld = resolve;
      else finishNew = resolve;
    });
  });
  const oldCheck = api.validateEditedScenario();
  elements.get('#scenarioText').value = 'edited YAML';
  api.resetScenarioValidation();
  const newCheck = api.validateEditedScenario();
  finishNew(response({ valid: true, name: 'New input', phases: 1 }));
  await newCheck;
  finishOld(response({ error: 'Old input was invalid' }, 400));
  await oldCheck;
  assert.equal(state.scenarioValidation.kind, 'success');
  assert.match(elements.get('#scenarioValidationMessage').textContent, /New input · 1 phase\n/);
});

test('new and loaded scenarios invalidate checks of the previous editor contents', async () => {
  const { api, state, elements } = fixture(async (url) => {
    assert.equal(url, '/api/v1/scenarios/saved');
    return response({ id: 'saved', name: 'Saved', yaml: 'loaded YAML' });
  });
  state.scenarioValidation = { kind: 'success', title: 'Old result', message: 'old' };
  api.startNewScenario();
  assert.equal(state.scenarioValidation, null);
  state.savedScenarios = [{ id: 'saved', name: 'Saved' }];
  state.scenarioValidation = { kind: 'invalid', title: 'Old error', message: 'old' };
  await api.loadSavedScenario('saved');
  assert.equal(elements.get('#scenarioText').value, 'loaded YAML');
  assert.equal(state.scenarioValidation, null);
});

test('validation handles blank input, server failures, malformed responses, and timeouts', async () => {
  const blank = fixture(async () => { throw new Error('unexpected request'); });
  blank.elements.get('#scenarioText').value = ' \n';
  await blank.api.validateEditedScenario();
  assert.equal(blank.state.scenarioValidation.kind, 'invalid');
  assert.equal(blank.elements.get('#scenarioText').focused, true);

  for (const reply of [response({ error: 'Server unavailable' }, 503), response({ valid: false }), response({ valid: true, name: 'wrong', phases: -1 })]) {
    const current = fixture(async () => reply);
    await current.api.validateEditedScenario();
    assert.equal(current.state.scenarioValidation.kind, 'error');
    assert.equal(current.elements.get('#scenarioText').getAttribute('aria-invalid'), 'false');
    assert.equal(current.elements.get('#validateScenario').disabled, false);
  }
  const timed = fixture((url, options) => hangingResponse(options));
  const checking = timed.api.validateEditedScenario();
  timed.expireRequest();
  await checking;
  assert.equal(timed.state.scenarioValidating, false);
  assert.equal(timed.state.scenarioValidation.kind, 'error');
  assert.match(timed.elements.get('#scenarioValidationMessage').textContent, /timed out/);
  assert.equal(timed.elements.get('#validateScenario').disabled, false);
});

test('the editor closes immediately during a slow scenario list request', async () => {
  let release;
  const { api, state, elements, closes } = fixture(() => new Promise((resolve) => { release = resolve; }));
  const refreshing = api.refreshSavedScenarios();
  assert.equal(state.scenariosLoading, true);
  assert.ok(closes.every((button) => !button.disabled));
  api.closeScenarioEditor();
  assert.equal(elements.get('#scenarioDialog').open, false);
  assert.equal(state.scenariosLoading, true, 'closing must not cancel the pending operation');
  release(response([]));
  await refreshing;
  assert.equal(state.scenariosLoading, false);
  assert.equal(elements.get('#scenarioDialog').open, false);
});

test('a successful submission closes the original editor normally', async () => {
  const { api, state, elements } = fixture(async () => response({ id: 'run-one', name: 'Scenario' }));
  await api.submitScenarioRun();
  assert.equal(elements.get('#scenarioDialog').open, false);
  assert.equal(elements.get('#scenarioDialog').closedWith, '');
  assert.equal(state.scenarioSubmitting, false);
});

test('saved scenario runs submit their source ID and refresh groups without waiting for it', async () => {
  const requests=[];let rejectRefresh,refreshOptions;
  const {api,state,elements}=fixture(async(url,options)=>{
    requests.push(JSON.parse(options.body));
    return response({id:'run-one',name:'Edited scenario'});
  });
  state.selectedScenarioId='a'.repeat(32);
  elements.get('#scenarioText').value='version: 3\nname: Edited scenario\n';
  elements.get('#runRepetitions').value='2';
  api.KPLLibraryGroups={matches:()=>true,isFiltered:()=>false,selectionMarkup:()=>'',badgeMarkup:()=>'',refreshUI(){},
    refresh(options){refreshOptions=options;return new Promise((resolve,reject)=>{rejectRefresh=reject;});}};
  await api.submitScenarioRun();
  assert.deepEqual(requests,[{scenario:'version: 3\nname: Edited scenario\n',repetitions:2,scenarioId:'a'.repeat(32)}]);
  assert.equal(refreshOptions.fresh,true);
  assert.equal(elements.get('#scenarioDialog').open,false);
  assert.equal(state.scenarioSubmitting,false);
  assert.match(elements.get('#toast').textContent,/Queued 2 runs/);
  rejectRefresh(new Error('Groups unavailable'));
  await new Promise(setImmediate);
  assert.equal(elements.get('#scenarioError').textContent,'');
  assert.equal(requests.length,1,'failed group refresh must not resubmit the experiment');
});

test('new unsaved runs omit scenarioId even while viewing a group', async () => {
  let submitted,refreshes=0;
  const {api,elements}=fixture(async(url,options)=>{submitted=JSON.parse(options.body);return response({id:'run',name:'Draft'});});
  api.KPLLibraryGroups={matches:()=>true,isFiltered:()=>true,getImportGroup:()=> 'group',selectionMarkup:()=>'',badgeMarkup:()=>'',refreshUI(){},refresh(){refreshes++;return Promise.resolve();}};
  await api.submitScenarioRun();
  assert.equal(Object.hasOwn(submitted,'scenarioId'),false);
  assert.equal(refreshes,0);
  assert.equal(elements.get('#scenarioError').textContent,'');
});

test('changing the selected scenario changes an uncertain submission idempotency key', async () => {
  const keys=[],payloads=[],storage=new Map();let key=0;
  const {api,state}=fixture(async(url,options)=>{
    keys.push(options.headers.get('Idempotency-Key'));
    payloads.push(JSON.parse(options.body));
    throw new Error('Response unavailable');
  });
  api.sessionStorage={getItem:name=>storage.get(name),setItem:(name,value)=>storage.set(name,value),removeItem:name=>storage.delete(name)};
  api.crypto={randomUUID:()=>`request-${++key}`};
  state.selectedScenarioId='a'.repeat(32);
  await api.submitScenarioRun();
  await api.submitScenarioRun();
  state.selectedScenarioId='b'.repeat(32);
  await api.submitScenarioRun();
  assert.deepEqual(keys,['request-1','request-1','request-2']);
  assert.deepEqual(payloads.map(payload=>payload.scenarioId),['a'.repeat(32),'a'.repeat(32),'b'.repeat(32)]);
});

test('scenario search matches names and IDs without changing the editor or server order', () => {
  const { api, state, elements } = fixture(async () => response([]));
  state.savedScenarios = [
    { id: 'newer', name: 'Alpha' },
    { id: 'older', name: 'Alpha' },
    { id: 'special-ID', name: 'Beta' },
  ];
  state.selectedScenarioId = 'older';
  state.pendingScenarioDeleteId = 'newer';
  api.searchSavedScenarios(' ALPHA ');
  assert.deepEqual(Array.from(api.filteredSavedScenarios(), item => item.id), ['newer', 'older']);
  assert.equal(elements.get('#scenarioLibrarySummary').textContent, '2 of 3 scenarios');
  assert.equal(state.selectedScenarioId, 'older');
  assert.equal(state.pendingScenarioDeleteId, null);
  assert.equal(elements.get('#scenarioText').value, 'version: 1\nname: current\n');
  api.searchSavedScenarios('SPECIAL-id');
  assert.deepEqual(Array.from(api.filteredSavedScenarios(), item => item.id), ['special-ID']);
  api.searchSavedScenarios('not found');
  assert.match(elements.get('#scenarioLibraryList').innerHTML, /No matching scenarios/);
  api.searchSavedScenarios('');
  assert.equal(api.filteredSavedScenarios().length, 3);
  assert.equal(elements.get('#clearScenarioSearch').hidden, true);
});

test('mobile Load opens the editor without focusing the text input and failed loads stay in the library', async () => {
  const { api, state, elements, viewport } = fixture(async () => response({
    id: 'one', name: 'Baseline', yaml: 'version: 1\nname: loaded\n',
  }));
  viewport.mobile = true;
  state.savedScenarios = [{ id: 'one', name: 'Baseline' }];
  await api.loadSavedScenario('one');
  assert.equal(elements.get('.scenario-workspace').dataset.scenarioView, 'editor');
  assert.equal(elements.get('#scenarioEditorHeading').focused, true);
  assert.equal(elements.get('#scenarioText').focused, undefined);

  const failed = fixture(async () => response({ error: 'Unavailable' }, 503));
  failed.state.savedScenarios = [{ id: 'one', name: 'Baseline' }];
  failed.api.setScenarioView('library');
  await failed.api.loadSavedScenario('one');
  assert.equal(failed.elements.get('.scenario-workspace').dataset.scenarioView, 'library');
  assert.equal(failed.elements.get('#scenarioListError').hidden, false);
  assert.match(failed.elements.get('#scenarioListError').textContent, /Could not load/);
  assert.equal(failed.elements.get('#scenarioLibraryError').textContent, '');
});

test('file import derives the saved name from only the final extension and preserves the YAML', async () => {
  const yaml = '# Keep formatting and the run name\r\nversion: 3\r\nname: internal-run\r\n';
  for (const [filename, name] of [
    ['experiment.yaml', 'experiment'],
    ['experiment.v3.YML', 'experiment.v3'],
    ['실험.기준.yaml', '실험.기준'],
    ['extensionless', 'extensionless'],
    ['.hidden', '.hidden'],
    ['.hidden.yaml', '.hidden'],
  ]) {
    let calls = 0;
    const { api, state, elements } = fixture(async () => { calls++; return response([]); });
    state.selectedScenarioId = 'existing';
    state.pendingScenarioDeleteId = 'existing';
    state.scenarioValidation = { kind: 'success', title: 'Old validation', message: 'old' };
    await api.importScenarioFile(scenarioFile(filename, '\uFEFF' + yaml));
    assert.equal(elements.get('#scenarioName').value, name, filename);
    assert.equal(elements.get('#scenarioText').value, yaml);
    assert.equal(state.selectedScenarioId, null);
    assert.equal(state.pendingScenarioDeleteId, null);
    assert.equal(state.scenarioValidation, null);
    assert.equal(state.scenarioImporting, null);
    assert.equal(elements.get('.scenario-workspace').dataset.scenarioView, 'editor');
    assert.equal(calls, 0, 'selecting a file must not save, validate, or run it');
  }
});

test('saving an imported file creates a new library record instead of changing the selected scenario', async () => {
  const requests = [];
  const { api, state, elements } = fixture(async (url, options) => {
    const body = JSON.parse(options.body);
    requests.push({ url, method: options.method, body });
    return response({ id: 'imported-id', ...body });
  });
  state.savedScenarios = [{ id: 'existing', name: 'Existing' }];
  state.selectedScenarioId = 'existing';
  const yaml = 'version: 3\nname: separate-run-name\n';
  await api.importScenarioFile(scenarioFile('new-scenario.yaml', yaml));
  assert.equal(elements.get('#saveScenario').textContent, 'Save scenario');
  await api.saveEditedScenario(false);
  assert.deepEqual(requests, [{ url: '/api/v1/scenarios', method: 'POST', body: { name: 'new-scenario', yaml } }]);
  assert.equal(state.selectedScenarioId, 'imported-id');
  assert.ok(state.savedScenarios.some(item => item.id === 'existing' && item.name === 'Existing'));
});

test('invalid or unreadable files preserve the existing draft and selected saved scenario', async () => {
  for (const file of [
    scenarioFile('empty.yaml', ''),
    scenarioFile('blank.yaml', ' \r\n\t'),
    scenarioFile('invalid-utf8.yaml', Buffer.from([0xC3, 0x28])),
    { name: 'unreadable.yaml', size: 20, async arrayBuffer() { throw new Error('Device read failed'); } },
  ]) {
    const { api, state, elements } = fixture(async () => { throw new Error('Unexpected request'); });
    state.selectedScenarioId = 'existing';
    elements.get('#scenarioName').value = 'Original saved name';
    const yaml = elements.get('#scenarioText').value;
    await api.importScenarioFile(file);
    assert.equal(state.selectedScenarioId, 'existing', file.name);
    assert.equal(elements.get('#scenarioName').value, 'Original saved name');
    assert.equal(elements.get('#scenarioText').value, yaml);
    assert.ok(state.scenarioActionError, file.name + ' must show an error');
    assert.equal(elements.get('#scenarioLibraryError').focused, true, 'the read error must be brought into view');
    assert.equal(state.scenarioImporting, null);
    assert.equal(elements.get('#scenarioText').disabled, false);
    assert.equal(elements.get('#chooseScenarioFile').disabled, false);
  }
});

test('file import checks the one MiB byte limit before reading and accepts the exact boundary', async () => {
  const limit = 1 << 20;
  const { api, state, elements } = fixture(async () => { throw new Error('Unexpected request'); });
  const original = elements.get('#scenarioText').value;
  let read = false;
  await api.importScenarioFile({ name: 'too-large.yaml', size: limit + 1, async arrayBuffer() { read = true; return new ArrayBuffer(0); } });
  assert.equal(read, false);
  assert.equal(elements.get('#scenarioText').value, original);
  assert.ok(state.scenarioActionError);
  const yaml = 'a' + '한'.repeat((limit - 1) / 3);
  const exact = scenarioFile('exact.yaml', yaml);
  assert.equal(exact.size, limit);
  assert.ok(yaml.length < limit, 'the boundary must count UTF-8 bytes, not characters');
  await api.importScenarioFile(exact);
  assert.equal(elements.get('#scenarioText').value, yaml);
  assert.equal(state.scenarioActionError, '');
});

test('selecting no file preserves the draft without an error or request', async () => {
  const { api, state, elements } = fixture(async () => { throw new Error('Unexpected request'); });
  state.selectedScenarioId = 'existing';
  elements.get('#scenarioName').value = 'Draft';
  const yaml = elements.get('#scenarioText').value;
  await api.importScenarioFile(undefined);
  assert.equal(state.selectedScenarioId, 'existing');
  assert.equal(elements.get('#scenarioName').value, 'Draft');
  assert.equal(elements.get('#scenarioText').value, yaml);
  assert.equal(state.scenarioActionError, '');
  assert.equal(state.scenarioImporting, null);
});

test('reading a file blocks conflicting scenario operations and leaves close available', async () => {
  let calls = 0;
  const { api, state, elements, closes } = fixture(async () => { calls++; return response([]); });
  state.savedScenarios = [{ id: 'saved', name: 'Saved' }];
  const deferred = deferredScenarioFile('reading.yaml', 'version: 3\nname: loaded\n');
  const importing = api.importScenarioFile(deferred.file);
  assert.ok(state.scenarioImporting);
  for (const id of ['#scenarioName', '#scenarioText', '#saveScenario', '#runScenario', '#validateScenario', '#newScenario', '#refreshScenarios', '#chooseScenarioFile']) {
    assert.equal(elements.get(id).disabled, true, id + ' must be disabled while reading');
  }
  assert.ok(closes.every(button => !button.disabled));
  await Promise.all([api.saveEditedScenario(false), api.submitScenarioRun(), api.validateEditedScenario(), api.loadSavedScenario('saved'), api.refreshSavedScenarios()]);
  api.startNewScenario();
  assert.equal(calls, 0);
  assert.equal(elements.get('#scenarioText').value, 'version: 1\nname: current\n');
  await deferred.finish();
  await importing;
  assert.equal(state.scenarioImporting, null);
  assert.equal(elements.get('#runScenario').disabled, false);
});

test('closing and reopening ignores a late file read and preserves the original draft', async () => {
  const { api, state, elements } = fixture(async () => response([{ id: 'existing', name: 'Original' }]));
  state.selectedScenarioId = 'existing';
  elements.get('#scenarioName').value = 'Original';
  const yaml = elements.get('#scenarioText').value;
  const deferred = deferredScenarioFile('late.yaml', 'name: old file\n');
  const importing = api.importScenarioFile(deferred.file);
  api.closeScenarioEditor();
  assert.equal(state.scenarioImporting, null);
  assert.equal(elements.get('#scenarioDialog').open, false);
  api.openScenarioEditor();
  await new Promise(resolve => setImmediate(resolve));
  await deferred.finish();
  await importing;
  assert.equal(elements.get('#scenarioDialog').open, true);
  assert.equal(state.selectedScenarioId, 'existing');
  assert.equal(elements.get('#scenarioName').value, 'Original');
  assert.equal(elements.get('#scenarioText').value, yaml);
  assert.equal(state.scenarioActionError, '');
});

test('a cancelled read cannot release a newer import or overwrite its error state', async () => {
  const { api, state, elements } = fixture(async () => { throw new Error('Unexpected request'); });
  const first = deferredScenarioFile('first.yaml', 'name: first\n');
  const oldImport = api.importScenarioFile(first.file);
  api.cancelScenarioFileImport();
  const second = deferredScenarioFile('second.yaml', 'name: second\n');
  const newImport = api.importScenarioFile(second.file);
  const currentToken = state.scenarioImporting;
  first.fail(new Error('A cancelled device read failed'));
  await oldImport;
  assert.equal(state.scenarioImporting, currentToken);
  assert.equal(elements.get('#runScenario').disabled, true);
  assert.equal(state.scenarioActionError, '');
  await second.finish();
  await newImport;
  assert.equal(elements.get('#scenarioName').value, 'second');
  assert.equal(elements.get('#scenarioText').value, 'name: second\n');
  assert.equal(state.scenarioImporting, null);
});

test('imported YAML ignores a late validation response for the previous draft', async () => {
  let complete;
  const { api, state, elements } = fixture(() => new Promise(resolve => { complete = resolve; }));
  const validating = api.validateEditedScenario();
  await api.importScenarioFile(scenarioFile('replacement.yaml', 'name: replacement\n'));
  complete(response({ valid: true, name: 'Previous draft', phases: 1 }));
  await validating;
  assert.equal(elements.get('#scenarioText').value, 'name: replacement\n');
  assert.equal(state.scenarioValidation, null);
  assert.equal(state.scenarioValidating, false);
});

test('mobile file import opens the editor without raising the text keyboard', async () => {
  const { api, elements, viewport } = fixture(async () => { throw new Error('Unexpected request'); });
  viewport.mobile = true;
  await api.importScenarioFile(scenarioFile('mobile.yaml', 'name: mobile\n'));
  assert.equal(elements.get('.scenario-workspace').dataset.scenarioView, 'editor');
  assert.equal(elements.get('#scenarioEditorHeading').focused, true);
  assert.equal(elements.get('#scenarioText').focused, undefined);
});

test('multiple file selection opens a review with file-based names and preserves the draft', async () => {
  let requests = 0;
  const { api, state, elements } = fixture(async () => { requests++; throw new Error('Unexpected request'); });
  state.selectedScenarioId = 'existing';
  elements.get('#scenarioName').value = 'Original';
  const yaml = elements.get('#scenarioText').value;
  await api.importScenarioFiles([
    scenarioFile('실험.v3.YAML', 'name: first\n'),
    scenarioFile('second.yml', 'name: second\n'),
  ]);
  assert.equal(requests, 0);
  assert.equal(state.selectedScenarioId, 'existing');
  assert.equal(elements.get('#scenarioName').value, 'Original');
  assert.equal(elements.get('#scenarioText').value, yaml);
  assert.deepEqual(Array.from(state.scenarioImportBatch.rows, row => [row.name, row.status]), [['실험.v3', 'ready'], ['second', 'ready']]);
  assert.equal(elements.get('#scenarioImportReview').hidden, false);
  assert.equal(elements.get('#runScenario').disabled, true);
  assert.equal(elements.get('#scenarioImportHeading').focused, true);
  api.cancelScenarioBatchImport();
  assert.equal(state.scenarioImportBatch, null);
  assert.equal(elements.get('#scenarioText').value, yaml);
  assert.equal(elements.get('#runScenario').disabled, false);
  assert.match(markup, /id="scenarioFile"[^>]*\bmultiple\b/);
});

test('file selection dispatches one file to the existing draft editor and cancellation changes nothing', async () => {
  const { api, state, elements } = fixture(async () => { throw new Error('Unexpected request'); });
  await api.importScenarioFiles([]);
  assert.equal(state.scenarioImportBatch, null);
  await api.importScenarioFiles([scenarioFile('single.yml', 'name: one\n')]);
  assert.equal(state.scenarioImportBatch, null);
  assert.equal(elements.get('#scenarioName').value, 'single');
  assert.equal(elements.get('#scenarioText').value, 'name: one\n');
});

test('batch limits reject oversized selections before reading device files', async () => {
  for (const [count, size] of [[101, 1], [17, 1 << 20]]) {
    let reads = 0;
    const { api, state } = fixture(async () => { throw new Error('Unexpected request'); });
    const files = Array.from({ length: count }, (_, index) => ({
      name: index + '.yaml', size, async arrayBuffer() { reads++; return new ArrayBuffer(0); },
    }));
    await api.importScenarioFiles(files);
    assert.equal(reads, 0);
    assert.equal(state.scenarioImportBatch, null);
    assert.match(state.scenarioActionError, /100 files.*16 MiB/);
  }
});

test('batch review isolates invalid UTF-8, oversized files, empty files and duplicate saved names', async () => {
  let largeRead = false;
  const { api, state, elements } = fixture(async () => { throw new Error('Unexpected request'); });
  await api.importScenarioFiles([
    scenarioFile('good.yaml', 'name: good\n'),
    scenarioFile('good.yml', 'name: duplicate\n'),
    scenarioFile('utf8.yaml', Buffer.from([0xC3, 0x28])),
    scenarioFile('empty.yaml', ''),
    { name: 'large.yaml', size: (1 << 20) + 1, async arrayBuffer() { largeRead = true; } },
    scenarioFile('<script>.yaml', 'name: escaped\n'),
  ]);
  assert.equal(largeRead, false);
  assert.deepEqual(Array.from(state.scenarioImportBatch.rows, row => row.status), ['ready', 'invalid', 'invalid', 'invalid', 'invalid', 'ready']);
  const html = elements.get('#scenarioImportList').innerHTML;
  assert.ok(html.includes('&lt;script&gt;'));
  assert.ok(!html.includes('<script>'));
  assert.match(state.scenarioImportBatch.rows[1].error, /same saved name/);
});

test('batch imports are sequential and retry only rejected selected files', async () => {
  const requests = [];
  let active = 0;
  let maximum = 0;
  let rejectSecond = true;
  const { api, state, elements } = fixture(async (url, options) => {
    const body = JSON.parse(options.body);
    requests.push(body.name);
    maximum = Math.max(maximum, ++active);
    await new Promise(resolve => setImmediate(resolve));
    active--;
    if (body.name === 'second' && rejectSecond) return response({ error: 'Temporary validation rejection' }, 400);
    return response({ id: body.name + '-id', ...body });
  });
  await api.importScenarioFiles([
    scenarioFile('first.yaml', 'name: run-first\n'),
    scenarioFile('second.yaml', 'name: run-second\n'),
    scenarioFile('unselected.yaml', 'name: run-third\n'),
  ]);
  state.scenarioImportBatch.rows[2].selected = false;
  const saving = api.saveScenarioImportBatch();
  await api.saveScenarioImportBatch();
  await saving;
  assert.equal(maximum, 1);
  assert.deepEqual(requests, ['first', 'second']);
  assert.deepEqual(Array.from(state.scenarioImportBatch.rows, row => row.status), ['saved', 'failed', 'ready']);
  rejectSecond = false;
  await api.saveScenarioImportBatch();
  assert.deepEqual(requests, ['first', 'second', 'second']);
  assert.deepEqual(Array.from(state.scenarioImportBatch.rows, row => row.status), ['saved', 'saved', 'ready']);
  assert.equal(state.savedScenarios.length, 2);
  assert.equal(elements.get('#scenarioText').value, 'version: 1\nname: current\n');
  assert.equal(state.scenarioImportBatch.rows[0].yaml, '', 'release successful file data');
});

test('uncertain batch saves stop the queue and cannot be blindly retried', async () => {
  for (const outcome of ['timeout', 'network', 'server', 'malformed']) {
    const requests = [];
    const { api, state, expireRequest } = fixture(async (url, options) => {
      const body = JSON.parse(options.body);
      requests.push(body.name);
      if (body.name === 'second') return response({ id: 'second-id', ...body });
      if (outcome === 'timeout') return hangingResponse(options);
      if (outcome === 'network') throw new Error('Network unavailable');
      if (outcome === 'server') return response({ error: 'Persistence uncertain' }, 500);
      return response({ name: body.name });
    });
    await api.importScenarioFiles([scenarioFile('first.yaml', 'name: first\n'), scenarioFile('second.yaml', 'name: second\n')]);
    const saving = api.saveScenarioImportBatch();
    if (outcome === 'timeout') expireRequest();
    await saving;
    assert.equal(state.scenarioImportBatch.rows[0].status, 'uncertain', outcome);
    assert.equal(state.scenarioImportBatch.rows[1].status, 'ready', outcome);
    assert.deepEqual(requests, ['first']);
    await api.saveScenarioImportBatch();
    assert.deepEqual(requests, ['first', 'second'], outcome);
    assert.equal(state.scenarioImportBatch.rows[0].status, 'uncertain');
    assert.equal(state.scenarioImportBatch.rows[1].status, 'saved');
  }
});

test('closing an in-flight batch stops after the acknowledged file and reopening preserves pending files', async () => {
  let complete;
  const requests = [];
  const { api, state, elements } = fixture(async (url, options) => {
    const body = JSON.parse(options.body);
    requests.push(body.name);
    if (body.name === 'first') return new Promise(resolve => { complete = () => resolve(response({ id: 'first-id', ...body })); });
    return response({ id: 'second-id', ...body });
  });
  await api.importScenarioFiles([scenarioFile('first.yaml', 'name: first\n'), scenarioFile('second.yaml', 'name: second\n')]);
  const saving = api.saveScenarioImportBatch();
  api.closeScenarioEditor();
  complete();
  await saving;
  assert.equal(elements.get('#scenarioDialog').open, false);
  assert.deepEqual(requests, ['first']);
  assert.deepEqual(Array.from(state.scenarioImportBatch.rows, row => row.status), ['saved', 'ready']);
  api.openScenarioEditor();
  await api.saveScenarioImportBatch();
  assert.deepEqual(requests, ['first', 'second']);
  assert.deepEqual(Array.from(state.scenarioImportBatch.rows, row => row.status), ['saved', 'saved']);
});

test('a canceled batch device read cannot replace a later single-file draft', async () => {
  const { api, state, elements } = fixture(async () => { throw new Error('Unexpected request'); });
  const deferred = deferredScenarioFile('late.yaml', 'name: late\n');
  const pending = api.importScenarioFiles([deferred.file, scenarioFile('other.yaml', 'name: other\n')]);
  api.cancelScenarioBatchImport();
  await api.importScenarioFiles([scenarioFile('current.yaml', 'name: current file\n')]);
  await deferred.finish();
  await pending;
  assert.equal(state.scenarioImportBatch, null);
  assert.equal(state.scenarioImporting, null);
  assert.equal(elements.get('#scenarioName').value, 'current');
  assert.equal(elements.get('#scenarioText').value, 'name: current file\n');
});

test('batch group destination is captured and failed assignments retry without duplicate scenario creation', async () => {
  const creates = [];
  const assignments = [];
  let currentGroup = 'chosen-group';
  let rejectGroup = true;
  const { api, state, elements } = fixture(async (url, options) => {
    const body = JSON.parse(options.body);
    creates.push(body.name);
    return response({ id: body.name + '-id', ...body });
  });
  api.KPLLibraryGroups = {
    matches: () => true,
    isFiltered: () => false,
    selectionMarkup: () => "",
    badgeMarkup: () => "",
    refreshUI() {},
    getImportGroup: () => currentGroup,
    getGroupName: id => id === 'chosen-group' ? 'Chosen group' : 'Other group',
    async assign(keys, id) {
      assignments.push({ keys: Array.from(keys), id });
      if (rejectGroup) throw new Error('Group unavailable');
    },
  };
  await api.importScenarioFiles([scenarioFile('first.yaml', 'name: first\n'), scenarioFile('second.yaml', 'name: second\n')]);
  currentGroup = 'different-group';
  assert.equal(elements.get('#scenarioImportDestination').textContent, 'Group: Chosen group');
  await api.saveScenarioImportBatch();
  assert.deepEqual(creates, ['first']);
  assert.equal(state.scenarioImportBatch.rows[0].status, 'group-failed');
  rejectGroup = false;
  await api.saveScenarioImportBatch();
  assert.deepEqual(creates, ['first', 'second']);
  assert.deepEqual(assignments, [
    { keys: ['scenario:first-id'], id: 'chosen-group' },
    { keys: ['scenario:first-id'], id: 'chosen-group' },
    { keys: ['scenario:second-id'], id: 'chosen-group' },
  ]);
  assert.deepEqual(Array.from(state.scenarioImportBatch.rows, row => row.status), ['saved', 'saved']);
});
