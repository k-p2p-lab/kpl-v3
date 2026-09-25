const test = require('node:test');
const assert = require('node:assert/strict');
const {describe} = require('./static/result-storage.js');
const ready = {availableBytes: 8 * 1024 ** 3, minFreeBytes: 1024 ** 3, lastCheckedAt: '2026-09-25T10:00:00Z'};
test('storage status distinguishes local space, archive discovery and archive errors', () => {
  assert.deepEqual(describe(ready), {text:'Local result space: 8 GiB free. Archive connected.', warning:false});
  assert.match(describe({...ready,lastCheckedAt:'0001-01-01T00:00:00Z'}).text,/Discovering saved archives/);
  const offline=describe({...ready,error:'mount unavailable'});
  assert.equal(offline.warning,true);assert.match(offline.text,/results remain local/);
});
test('low storage reports a pause before the next run and supports a disabled guard', () => {
  const low=describe({...ready,availableBytes:512 * 1024 ** 2});
  assert.equal(low.warning,true);assert.match(low.text,/512 MiB free/);assert.match(low.text,/before starting the next run/);
  assert.equal(describe({...ready,availableBytes:0,minFreeBytes:0}).warning,false);
});
test('only server-reported lack of progress warns, independently of client clock', () => {
  const transferring = {...ready, checking: true, phase: 'copying', checkStartedAt: '2020-01-01T00:00:00Z', lastProgressAt: '2020-01-01T00:01:00Z', stalled: false};
  const healthy = describe(transferring);
  assert.equal(healthy.warning, false);
  assert.match(healthy.text, /Copying results to archive/);
  const stalled = describe({...transferring, stalled: true});
  assert.equal(stalled.warning, true);
  assert.match(stalled.text, /Archive progress is delayed/);
  assert.match(stalled.text, /local recording remains available/);
  assert.doesNotMatch(stalled.text, /Archive connected/);
});

test('copying, verification, discovery, deletion and deliberate pauses stay neutral', () => {
  const labels = {copying: /Copying/, verifying: /Verifying/, discovering: /Discovering/, deleting: /Removing/, publishing: /Finalizing/, preparing: /Preparing/, paused: /paused while experiments/};
  for (const [phase, label] of Object.entries(labels)) {
    const status = describe({...ready, checking: true, phase, checkStartedAt: '2020-01-01T00:00:00Z', stalled: false});
    assert.equal(status.warning, false, phase);
    assert.match(status.text, label);
  }
  assert.match(describe({...ready, phase: 'paused', checking: false}).text, /paused while experiments/);
  assert.equal(describe({...ready, checking: true, phase: 'paused', stalled: true}).warning, false);
});

test('old Controller status does not interpret total cycle time as a NAS timeout', () => {
  const status = describe({...ready, checking: true, checkStartedAt: '2020-01-01T00:00:00Z'});
  assert.equal(status.warning, false);
  assert.match(status.text, /Archive work in progress/);
});
