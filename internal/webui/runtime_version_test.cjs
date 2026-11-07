const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const {describe} = require('./static/runtime-version.js');

const digest = `sha256:${'a'.repeat(64)}`;
const fixture = () => ({
  build: {version:'dev', revision:'2139f42aaabbbbccc', modified:true, goVersion:'go1.25.0', binaryHash:`sha256:${'b'.repeat(64)}`, buildTime:'2026-11-07T00:00:00Z'},
  image:`registry.example:5000/team/kpl@${digest}`, imageDigest:digest, imageSource:'deployment',
});

test('runtime identity distinguishes source revision, binary hash and registry digest', () => {
  const data = fixture(), described = describe(data);
  assert.equal(described.summary, 'Version · dev (2139f42a+) · sha256:aaaaaaaaaaaa');
  assert.equal(described.image, data.image);
  assert.equal(described.digest, digest);
  assert.equal(described.binary, data.build.binaryHash);
  assert.equal(described.revision, '2139f42aaabbbbccc +modified');
  assert.equal(describe(null).digest, 'Unknown');
  assert.equal(describe({image:'team/kpl:latest', imageDigest:digest}).digest, 'Unknown');
  assert.equal(describe({...data,imageDigest:'sha256:short'}).digest, 'Unknown');
  assert.equal(describe({...data,imageDigest:`sha256:${'c'.repeat(64)}`}).digest, 'Unknown');
});

function browser() {
  const elements = new Map(), listeners = {};
  const element = selector => {
    if (!elements.has(selector)) elements.set(selector, {textContent:'', title:'', set innerHTML(value) { throw new Error('runtime identity must use plain text'); }});
    return elements.get(selector);
  };
  const document = {hidden:false, querySelector:element, addEventListener:(name, fn) => {listeners[name] = fn;}};
  const context = {document, AbortController, setTimeout, clearTimeout};
  vm.createContext(context);
  vm.runInContext(fs.readFileSync(path.join(__dirname,'static/runtime-version.js'),'utf8'),context);
  return {element, document, listeners, runtime:context.KPLRuntimeVersion};
}

test('runtime version loads once, preserves full plain text and refreshes after redeployment', async () => {
  const {element, document, listeners, runtime} = browser();
  let resolve, calls=0;
  runtime.init({api:(url, options) => {
    calls++;
    assert.equal(url, '/api/v1/version');
    assert.ok(options.signal instanceof AbortSignal);
    return new Promise(r => {resolve=r;});
  }});
  const pending = runtime.refresh();
  await Promise.resolve();
  assert.equal(calls,1);
  assert.equal(runtime.refresh(),pending);
  const data=fixture();
  data.image=`registry.example/${'very-long-repository/'.repeat(50)}<script>alert(1)</script>@${digest}`;
  resolve(data); await pending;
  assert.equal(element('#runtimeVersionImage').textContent,data.image);
  assert.equal(element('#runtimeVersionBinary').textContent,data.build.binaryHash);
  assert.match(element('#runtimeVersionSummary').title, /<script>/);
  assert.equal(element('#runtimeVersionStatus').textContent,'');
  document.hidden=true; runtime.refresh(); await Promise.resolve(); assert.equal(calls,1);
  document.hidden=false; listeners.visibilitychange(); await Promise.resolve(); assert.equal(calls,2);
  resolve({...data,build:{version:'v3.2.0'},image:'registry.example/kpl:latest',imageDigest:''});
  await runtime.refresh();
  assert.equal(element('#runtimeVersionSummary').textContent,'Version · v3.2.0 · Digest unknown');
  assert.equal(element('#runtimeVersionDigest').textContent,'Unknown');
});

test('a failed refresh keeps confirmed identity and exposes that it is stale', async () => {
  const {element,runtime} = browser();
  let fail=false;
  runtime.init({api:() => {if(fail) throw new Error('unavailable'); return fixture();}});
  await runtime.refresh();
  fail=true; await runtime.refresh();
  assert.equal(element('#runtimeVersionDigest').textContent,digest);
  assert.match(element('#runtimeVersionStatus').textContent,/last confirmed/);
  fail=false; await runtime.refresh();
  assert.equal(element('#runtimeVersionStatus').textContent,'');
});

test('initial failure presents unknown and a later refresh can recover', async () => {
  const {element,runtime} = browser();
  let fail=true;
  runtime.init({api:() => {if(fail) throw new Error('unavailable'); return fixture();}});
  await runtime.refresh();
  assert.equal(element('#runtimeVersionDigest').textContent,'Unknown');
  assert.match(element('#runtimeVersionStatus').textContent,/unavailable/);
  fail=false; await runtime.refresh();
  assert.equal(element('#runtimeVersionDigest').textContent,digest);
});
