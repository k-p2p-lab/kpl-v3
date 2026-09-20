const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const source = fs.readFileSync(path.join(__dirname, 'static/app.js'), 'utf8');
const functions = source.slice(source.indexOf('function escapeHTML('), source.indexOf('$("#scenarioText").value = defaultScenario;'));

test('dashboard operations reuse cookies and send the same-origin marker without bearer credentials', async () => {
  const calls = [];
  const context = { Headers, state: {}, fetch: async (url, options) => {
    calls.push({url, options});
    return {ok:true, status:204};
  }};
  vm.createContext(context);
  vm.runInContext(functions, context);
  for (const method of ['GET','HEAD','POST','PUT','DELETE']) {
    assert.equal(await context.api('/api/v1/results', {method, headers:{Accept:'application/json'}}), null);
  }
  for (const {options} of calls) {
    assert.equal(options.credentials, 'same-origin');
    assert.equal(options.headers.get('Authorization'), null);
    assert.equal(options.headers.get('Accept'), 'application/json');
    assert.equal(options.headers.get('X-KPL-Request'), ['GET','HEAD'].includes(options.method) ? null : 'dashboard');
  }
});

test('an expired session redirects once and stops stream reconnection without asking for tokens', async () => {
  let redirects=0, closed=0, cleared=0;
  const context = {
    Headers, state:{loginRedirecting:false, stream:{close(){closed++;}}, reconnectTimer:42},
    location:{replace(url){assert.equal(url,'/login');redirects++;}},
    clearTimeout(id){assert.equal(id,42);cleared++;},
    fetch:async()=>({ok:false,status:401,statusText:'Unauthorized',json:async()=>({error:'login required'})}),
  };
  vm.createContext(context);
  vm.runInContext(functions, context);
  for(let i=0;i<2;i++)await assert.rejects(context.api('/api/v1/results'),error=>error.status===401&&error.message==='login required');
  assert.equal(redirects,1);assert.equal(closed,1);assert.equal(cleared,1);
});
