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
    'scenarioLibraryStatus', 'refreshScenarios', 'newScenario', 'saveScenario', 'saveScenarioCopy',
    'scenarioLibraryError', 'scenarioEditingStatus', 'scenarioLibraryList', 'scenarioName',
    'scenarioText', 'scenarioError', 'runRepetitions', 'runScenario', 'scenarioDialog', 'toast',
    'validateScenario', 'scenarioValidation', 'scenarioValidationTitle', 'scenarioValidationMessage',
  ]) elements.set(`#${id}`, element());
  elements.set('.scenario-library', element());
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

test('scenario form has explicit non-submitting close controls', () => {
  const form = markup.slice(markup.indexOf('<form class="dialog-shell" id="scenarioForm">'), markup.indexOf('</form>', markup.indexOf('id="scenarioForm"')));
  assert.ok(form);
  assert.ok(!/<form[^>]+method=/.test(form));
  assert.equal((form.match(/type="button" data-scenario-close/g) || []).length, 2);
  assert.match(source, /#scenarioForm"\)\.addEventListener\("submit", \(event\) => event\.preventDefault\(\)\)/);
});

test('event panel follows Agent panel height on desktop and returns to natural mobile sizing', () => {
  const { api, elements, viewport } = fixture(async () => response([]));
  elements.get('.agents-panel').boxHeight = 417.2;
  api.syncDetailPanelHeight();
  assert.equal(elements.get('.events-panel').style.height, '417.2px');
  viewport.mobile = true;
  api.syncDetailPanelHeight();
  assert.equal(elements.get('.events-panel').style.height, '');
});

test('either collapsed detail panel releases the event height and reopening restores it', () => {
  const { api, elements } = fixture(async () => response([]));
  elements.get('.agents-panel').boxHeight = 420;
  for (const id of ['agents', 'events']) {
    const body = element();
    elements.set(`#panel-${id}-body`, body);
    api.syncDetailPanelHeight();
    assert.equal(elements.get('.events-panel').style.height, '420px');
    body.hidden = true;
    api.syncDetailPanelHeight();
    assert.equal(elements.get('.events-panel').style.height, '');
    body.hidden = false;
    api.syncDetailPanelHeight();
    assert.equal(elements.get('.events-panel').style.height, '420px');
  }
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
  assert.ok(!html.includes('<svg'));
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
