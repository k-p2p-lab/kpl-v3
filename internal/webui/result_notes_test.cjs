const test = require('node:test');
const assert = require('node:assert/strict');
const { createEditor } = require('./static/result-notes.js');
const fs = require('node:fs');
const vm = require('node:vm');

function fixture(api) {
  const elements = new Map(), saved = [];
  function el(id) {
    if (!elements.has(id)) {
      const listeners = new Map();
      elements.set(id, {
        value: '', textContent: '', disabled: false, hidden: false, open: false,
        attributes: {}, selectionStart: -1, focus() {}, setSelectionRange(start) { this.selectionStart = start; }, setAttribute(k, v) { this.attributes[k] = v; },
        addEventListener(name, callback) { if (!listeners.has(name)) listeners.set(name, []); listeners.get(name).push(callback); },
        emit(name, event = { preventDefault() {} }) { for (const callback of listeners.get(name) || []) callback(event); },
        showModal() { this.open = true; }, close() { this.open = false; this.emit('close'); },
      });
    }
    return elements.get(id);
  }
  const editor = createEditor({ document: { querySelector: el }, api, onSaved: note => saved.push(note), formatTime: () => 'Sep 25, 2026, 11:30 AM' });
  const type = text => { el('#resultNoteText').value = text; el('#resultNoteText').emit('input'); };
  return { editor, el, saved, type };
}
const run = { id: 'run-a', name: '<b>Experiment A</b>' };
const empty = { runId: run.id, text: '', revision: '0' };
const flush = () => new Promise(resolve => setImmediate(resolve));

test('loads on demand, saves plain text with revision, and closes only after success', async () => {
  const calls = [];
  const f = fixture(async (path, options) => {
    calls.push({ path, options });
    assert.ok(options.signal);
    if (options.method === 'GET') return empty;
    const body = JSON.parse(options.body);
    assert.deepEqual(body, { text: '한글\n<script>alert(1)</script>', revision: '0' });
    return { ...empty, ...body, revision: 'new', updatedAt: '2026-09-25T02:30:00Z' };
  });
  assert.equal(calls.length, 0);
  await f.editor.open(run);
  assert.equal(calls[0].path, '/api/v1/results/run-a/note');
  assert.equal(f.el('#resultNoteName').textContent, run.name);
  assert.equal(f.el('#saveResultNote').disabled, true);
  f.type('한글\n<script>alert(1)</script>');
  assert.equal(f.el('#saveResultNote').disabled, false);
  await f.editor.save();
  assert.equal(f.saved.length, 1);
  assert.equal(f.el('#resultNoteDialog').open, false);
 });

test('failed loading prevents saving and exposes retry', async () => {
  let fail = true;
  const f = fixture(async () => { if (fail) throw new Error('storage unavailable'); return empty; });
  await f.editor.open(run);
  assert.equal(f.el('#resultNoteText').disabled, true);
  assert.equal(f.el('#saveResultNote').disabled, true);
  assert.equal(f.el('#reloadResultNote').hidden, false);
  assert.match(f.el('#resultNoteError').textContent, /storage unavailable/);
  fail = false;
  f.el('#reloadResultNote').emit('click');
  await flush();
  assert.equal(f.el('#resultNoteText').disabled, false);
 });

test('failed saves preserve the draft and allow a retry', async () => {
  let writes = 0;
  const f = fixture(async (_path, options) => {
    if (options.method === 'GET') return empty;
    if (++writes === 1) throw new Error('disk full');
    return { ...empty, ...JSON.parse(options.body), revision: 'new' };
  });
  await f.editor.open(run);
  f.type('Do not lose this draft');
  await f.editor.save();
  assert.equal(f.el('#resultNoteDialog').open, true);
  assert.equal(f.el('#resultNoteText').value, 'Do not lose this draft');
  assert.equal(f.el('#saveResultNote').disabled, false);
  assert.match(f.el('#resultNoteError').textContent, /disk full/);
  await f.editor.save();
  assert.equal(f.saved.length, 1);
 });

for (const failure of ['conflict', 'timeout']) test(`${failure} requires reviewing latest note while preserving draft`, async () => {
  let writes = 0, reads = 0;
  const f = fixture(async (_path, options) => {
    if (options.method === 'GET') return ++reads === 1 ? empty : { ...empty, text: 'Another editor\'s note', revision: 'other' };
    if (++writes === 1) throw Object.assign(new Error(failure), failure === 'conflict' ? { status: 409 } : { name: 'AbortError' });
    assert.equal(JSON.parse(options.body).revision, 'other');
    return { ...empty, ...JSON.parse(options.body), revision: 'new' };
  });
  await f.editor.open(run);
  f.type('My draft');
  await f.editor.save();
  assert.equal(f.el('#saveResultNote').disabled, true);
  assert.equal(f.el('#resultNoteText').value, 'My draft');
  await f.editor.save();
  assert.equal(writes, 1);
  f.el('#reloadResultNote').emit('click');
  await flush();
  assert.equal(f.el('#latestResultNote').value, 'Another editor\'s note');
  assert.equal(f.el('#resultNoteConflict').hidden, false);
  assert.equal(f.el('#resultNoteText').value, 'My draft');
  assert.equal(f.el('#saveResultNote').disabled, false);
  await f.editor.save();
  assert.equal(f.saved[0].text, 'My draft');
 });

test('UTF-8 limit counts multibyte characters and never truncates a draft', async () => {
  const f = fixture(async () => empty);
  await f.editor.open(run);
  f.type('가'.repeat(5462));
  assert.equal(f.el('#saveResultNote').disabled, true);
  assert.match(f.el('#resultNoteCount').textContent, /16,386 \/ 16,384 bytes — note is too long/);
  assert.equal(f.el('#resultNoteText').value.length, 5462);
  f.type('😀'.repeat(4096));
  assert.equal(f.el('#saveResultNote').disabled, false);
  assert.match(f.el('#resultNoteCount').textContent, /16,384 \/ 16,384 bytes$/);
 });

test('empty text clears an existing note and timestamps use the supplied English formatter', async () => {
  const f = fixture(async (_path, options) => {
    if (options.method === 'GET') return { ...empty, text: 'old', revision: '1', updatedAt: '2026-09-25T02:30:00Z' };
    assert.deepEqual(JSON.parse(options.body), { text: '', revision: '1' });
    return { ...empty, revision: '2' };
  });
  await f.editor.open(run);
  assert.equal(f.el('#resultNoteStatus').textContent, 'Last saved Sep 25, 2026, 11:30 AM');
  assert.equal(f.el('#resultNoteText').selectionStart, 0);
  assert.equal(f.el('#resultNoteText').scrollTop, 0);
  f.type('');
  await f.editor.save();
  assert.equal(f.saved[0].text, '');
 });

test('closing a pending load aborts it; a late response cannot overwrite another run', async () => {
  let finishFirst, signal;
  const f = fixture(async (path, options) => {
    if (path.includes('run-a')) { signal = options.signal; return new Promise(resolve => { finishFirst = resolve; }); }
    return { text: 'Run B', revision: 'b' };
  });
  const first = f.editor.open(run);
  f.el('#cancelResultNote').emit('click');
  assert.equal(signal.aborted, true);
  await f.editor.open({ id: 'run-b' });
  finishFirst({ text: 'Run A', revision: 'a' });
  await first;
  assert.equal(f.el('#resultNoteText').value, 'Run B');
  assert.equal(f.el('#resultNoteID').textContent, 'run-b');
 });

test('saving disables duplicate submissions and closing until the response arrives', async () => {
  let finishSave, writes = 0;
  const f = fixture(async (_path, options) => {
    if (options.method === 'GET') return empty;
    writes++;
    return new Promise(resolve => { finishSave = resolve; });
  });
  await f.editor.open(run);
  f.type('saving');
  const saving = f.editor.save();
  assert.equal(f.el('#cancelResultNote').disabled, true);
  let prevented = false;
  f.el('#resultNoteDialog').emit('cancel', { preventDefault() { prevented = true; } });
  f.el('#cancelResultNote').emit('click');
  await f.editor.save();
  assert.equal(prevented, true);
  assert.equal(f.el('#resultNoteDialog').open, true);
  assert.equal(writes, 1);
  finishSave({ text: 'saving', revision: 'done' });
  await saving;
 });

test('result note previews and button attributes escape untrusted text', () => {
  const source = fs.readFileSync(require.resolve('./static/app.js'), 'utf8');
  const escape = source.slice(source.indexOf('function escapeHTML('), source.indexOf('// Preserve text selection'));
  const markup = source.slice(source.indexOf('function resultNoteMarkup('), source.indexOf('function savedResultRow('));
  const context = vm.createContext({});
  vm.runInContext(escape + markup, context);
  const html = context.resultNoteMarkup({ id: 'run-a', name: '"><img src=x>', note: { preview: '<script>alert(1)</script>' } });
  assert.ok(!html.includes('<script>') && !html.includes('<img'));
  assert.match(html, /&lt;script&gt;/);
  assert.match(html, /Edit note/);
  assert.match(context.resultNoteMarkup({ id: 'run-b' }), /Add note/);
 });

test('group editor uses the batch endpoint and restores run labels on the next open', async () => {
  const calls = [];
  const f = fixture(async (path, options) => {
    calls.push({path, options});
    if (options.method === 'GET') return {batchId: 'batch-a', text: '', revision: '0'};
    return {batchId: 'batch-a', ...JSON.parse(options.body), revision: 'new'};
  });
  await f.editor.open({id: 'batch-a', name: 'Repeated experiment', isBatch: true});
  assert.equal(f.el('#resultNoteHeading').textContent, 'Group note');
  assert.match(f.el('#resultNoteHelp').textContent, /retries.*Individual run notes stay separate/);
  f.type('Shared group observation');
  await f.editor.save();
  assert.deepEqual(calls.map(call => call.path), ['/api/v1/result-batches/batch-a/note', '/api/v1/result-batches/batch-a/note']);
  assert.equal(f.saved[0].batchId, 'batch-a');
  await f.editor.open(run);
  assert.equal(calls.at(-1).path, '/api/v1/results/run-a/note');
  assert.equal(f.el('#resultNoteHeading').textContent, 'Result note');
  assert.match(f.el('#resultNoteHelp').textContent, /view this result/);
});
