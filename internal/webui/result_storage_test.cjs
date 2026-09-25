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
test('stalled archive worker is visible without claiming local recording stopped', () => {
  const status=describe({...ready,checking:true,checkStartedAt:'2026-09-25T10:00:00Z'},Date.parse('2026-09-25T10:00:20Z'));
  assert.equal(status.warning,true);assert.match(status.text,/local recording remains available/);
  assert.doesNotMatch(status.text,/Archive connected/);
});
