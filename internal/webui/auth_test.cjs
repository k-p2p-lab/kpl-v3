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

test('raw analysis JSON keeps numeric text while using the same authentication and error handling', async () => {
  const source = '{"large":18446744073709551615}\n', calls = [];
  let failing = false;
  const context = {Headers, state:{}, fetch:async(url,options)=>{
    calls.push({url,options});
    if(failing)return {ok:false,status:403,json:async()=>({error:'forbidden'})};
    return {ok:true,status:200,text:async()=>source,json:async()=>{throw new Error('raw export must not parse and reserialize JSON')}};
  }};
  vm.createContext(context);vm.runInContext(functions,context);
  assert.equal(await context.api('/api/v1/analysis-jobs/run/result',{responseType:'text'}),source);
  assert.equal(calls[0].options.credentials,'same-origin');
  assert.equal(calls[0].options.headers.get('Authorization'),null);
  assert.equal('responseType' in calls[0].options,false);
  failing=true;
  await assert.rejects(context.api('/api/v1/analysis-jobs/run/result',{responseType:'text'}),error=>error.status===403&&error.message==='forbidden');
});

test('an expired session redirects once and stops stream reconnection without asking for tokens', async () => {
  let redirects=0, closed=0, cleared=0;
  const context = {
    Headers, state:{loginRedirecting:false, stream:{close(){closed++;}}, reconnectTimer:42},
    location:{replace(url){assert.equal(url,'/login-required');redirects++;}},
    clearTimeout(id){assert.equal(id,42);cleared++;},
    fetch:async()=>({ok:false,status:401,statusText:'Unauthorized',json:async()=>({error:'login required'})}),
  };
  vm.createContext(context);
  vm.runInContext(functions, context);
  for(let i=0;i<2;i++)await assert.rejects(context.api('/api/v1/results'),error=>error.status===401&&error.message==='login required');
  assert.equal(redirects,1);assert.equal(closed,1);assert.equal(cleared,1);
});

test('an explicit successful logout goes to the login form instead of the error page', async () => {
  const button = {disabled:false}, redirects = [];
  const context = {
    Headers, state:{loginRedirecting:false}, $:()=>button,
    location:{replace(url){redirects.push(url);}}, clearTimeout(){},
    fetch:async(url,options)=>{
      assert.equal(url,'/api/v1/auth/logout');
      assert.equal(options.method,'POST');
      return {ok:true,status:204};
    },
  };
  vm.createContext(context);
  vm.runInContext(functions,context);
  await context.logout();
  assert.deepEqual(redirects,['/login']);
  assert.equal(button.disabled,false);
});

test('host reboot admission metadata is preserved without treating a generic failure as a safe rejection',async()=>{
 let accepted=false;
 const context={Headers,state:{},fetch:async()=>({ok:false,status:409,json:async()=>({error:'Host changed',accepted})})};
 vm.createContext(context);vm.runInContext(functions,context);
 await assert.rejects(context.api('/api/v1/agent-hosts/reboot',{method:'POST'}),error=>error.status===409&&error.accepted===false);
 accepted=true;
 await assert.rejects(context.api('/api/v1/agent-hosts/reboot',{method:'POST'}),error=>error.accepted===true);
 accepted=undefined;
 await assert.rejects(context.api('/api/v1/agent-hosts/reboot',{method:'POST'}),error=>error.accepted===undefined);
});

test('HTTP errors preserve their status for every non-object or unreadable JSON body', async () => {
  const bodies = [null, [], ['synthetic-private-body'], 'synthetic-private-body', 42, true, false];
  for (const status of [404, 409, 422, 503]) {
    for (const body of bodies) {
      const context = {Headers, state:{}, fetch:async()=>new Response(JSON.stringify(body), {status, statusText:'Fixture failure'})};
      vm.createContext(context); vm.runInContext(functions, context);
      await assert.rejects(context.api('/api/v1/results/a', {method:'DELETE'}), error => {
        assert.equal(error.status, status);
        assert.equal(error.message, 'Fixture failure');
        assert.equal(error.code, undefined);
        assert.equal(error.accepted, undefined);
        assert.ok(!error.message.includes('synthetic-private-body'));
        return true;
      });
    }
    for (const body of ['', '{broken']) {
      const context = {Headers, state:{}, fetch:async()=>new Response(body, {status, statusText:'Fixture failure'})};
      vm.createContext(context); vm.runInContext(functions, context);
      await assert.rejects(context.api('/api/v1/results/a'), error => error.status === status && error.message === 'Fixture failure');
    }
    const context = {Headers, state:{}, fetch:async()=>({ok:false, status, statusText:'Fixture failure', json:async()=>{
      throw Object.assign(new Error('synthetic canceled body read'), {name:'AbortError'});
    }})};
    vm.createContext(context); vm.runInContext(functions, context);
    await assert.rejects(context.api('/api/v1/results/a'), error => error.status === status && error.message === 'Fixture failure');
  }
});

test('null error bodies retain confirmed absence and busy semantics across selected deletions', async () => {
  const {prepareDeletion, createDeletions} = require('./static/library-deletions.js');
  const results = [{id:'a', state:'completed'}, {id:'b', state:'completed'}];
  for (const status of [404, 409]) {
    const calls = [];
    const context = {Headers, state:{}, fetch:async url=>{
      calls.push(url);
      return url.endsWith('/a') ? new Response('null', {status, statusText:'Fixture failure'}) : new Response(null, {status:204});
    }};
    vm.createContext(context); vm.runInContext(functions, context);
    const plan = prepareDeletion('results', ['run:a', 'run:b'], results.map(run=>({key:'run:'+run.id})), results);
    const report = await createDeletions({api:context.api}).execute(plan);
    assert.deepEqual(calls, ['/api/v1/results/a', '/api/v1/results/b']);
    assert.deepEqual(report.deleted, status === 404 ? ['run:a', 'run:b'] : ['run:b']);
    assert.equal(report.failed.length, status === 404 ? 0 : 1);
    assert.deepEqual(report.remaining, []);
  }
});

test('null error bodies do not turn explicit bulk admission rejections into unknown outcomes', async () => {
  const {prepare, createAdmissions} = require('./static/result-bulk-actions.js');
  const results = [{id:'a', state:'completed'}, {id:'b', state:'completed'}];
  for (const status of [404, 409, 422]) {
    const calls = [];
    const context = {Headers, state:{}, fetch:async url=>{
      calls.push(url);
      return url.endsWith('/a') ? new Response('null', {status, statusText:'Fixture failure'})
        : new Response(JSON.stringify({id:'job-b', runId:'b', state:'queued'}), {status:202});
    }};
    vm.createContext(context); vm.runInContext(functions, context);
    const report = await createAdmissions({api:context.api}).execute(prepare('analysis', ['run:a', 'run:b'], results));
    assert.equal(report.unknown.length, 0);
    if (status === 422) {
      assert.equal(report.failed.length, 1);
      assert.equal(report.remaining.length, 1);
      assert.equal(calls.length, 1);
    } else {
      assert.equal(report.skipped.length, 1);
      assert.equal(report.accepted.length, 1);
      assert.equal(calls.length, 2);
    }
  }
});
