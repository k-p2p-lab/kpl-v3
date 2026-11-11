const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const {createDownloads} = require('./static/result-analysis-downloads.js');
const images = require('./static/result-images.js');

function artifact(id, analysisId='saved-'+id) {
  return JSON.stringify({version:1,analysisId,result:{id,name:'한글 / Analysis',state:'failed'},
    metrics:{definition:'session-window-v1'},latencyCDF:[],latencyHistogram:[],timeline:[],observations:[],
    originalFields:{missing:null,label:'이전 분석',large:'EXACT_INTEGER'}},null,2).replace('"EXACT_INTEGER"','18446744073709551615')+'\n';
}
function batchArtifact(id, analysisId='saved-'+id) {
  return JSON.stringify({version:1,analysisId,batchId:id,name:'한글 / Analysis',aggregation:'equal-run-mean-v1',
    expectedRuns:3,missingRuns:1,excluded:[{id:'failed',state:'failed'}],
    runs:['current-one','current-two'].map(run=>({result:{id:run,batchId:id,state:'completed'}})),
    summary:{publishedMessages:{mean:10,sd:2,n:2}},reliability:{includedRuns:2,previousAttempts:1},
    originalFields:{large:'EXACT_INTEGER'}},null,2).replace('"EXACT_INTEGER"','18446744073709551615')+'\n';
}
function entries(bytes) {
  const data=Buffer.from(bytes), result=[];
  let offset=0;
  while(data.readUInt32LE(offset)===0x04034b50) {
    assert.equal(data.readUInt16LE(offset+8),0);
    const size=data.readUInt32LE(offset+18), length=data.readUInt16LE(offset+26), extra=data.readUInt16LE(offset+28);
    const start=offset+30+length+extra;
    result.push({name:data.subarray(offset+30,offset+30+length).toString('utf8'),data:data.subarray(start,start+size)});
    offset=start+size;
  }
  assert.equal(data.readUInt32LE(offset),0x02014b50);
  assert.equal(data.readUInt32LE(data.length-22),0x06054b50);
  assert.equal(data.readUInt16LE(data.length-12),result.length);
  return result;
}
function fixture(api,results=[{id:'one'}],options={}) {
  const downloads=[],links=[],revoked=[];
  const url={createObjectURL(blob){downloads.push(blob);return 'blob:json-'+downloads.length},revokeObjectURL(value){revoked.push(value)}};
  const document={createElement(tag){assert.equal(tag,'a');return {click(){links.push(this)},remove(){}}},body:{appendChild(){}}};
  return {downloads,links,revoked,download:createDownloads({api,getResults:()=>results,document,url,pollInterval:0,format:'json',...options})};
}
function completed(path,options) {
  const id=path.split('/')[4];
  const isBatch=path.includes('/batch-analysis-jobs/');
  if(path.includes('/result?')) {assert.equal(options.responseType,'text');return isBatch?batchArtifact(id):artifact(id)}
  if(isBatch)return {batchId:id,id:'saved-'+id,state:'completed'};
  return {runId:id,id:'saved-'+id,state:'completed'};
}

test('all-missing repetition JSON exports its saved batch artifact without reanalysis',async()=>{
 const calls=[];
 const results=[{id:'missing-one',batchId:'b',repetitions:3,state:'missing',missing:true},
  {id:'missing-two',batchId:'b',repetitions:3,state:'missing',missing:true}];
 const f=fixture(async(path,options)=>{
  calls.push([path,options.method]);
  if(path.includes('/result?'))return batchArtifact('b','previous');
  return {batchId:'b',id:'previous',state:'idle',stale:true};
 },results);
 await f.download('results',['batch:b']);
 assert.deepEqual(calls,[['/api/v1/batch-analysis-jobs/b',undefined],['/api/v1/batch-analysis-jobs/b/result?jobId=previous&saved=1',undefined]]);
 assert.equal(f.downloads.length,1);
 const none=fixture(async(path,options)=>{assert.equal(options.method,undefined);return {batchId:'b',state:'idle'}},results);
 await assert.rejects(none.download('results',['batch:b']),/no completed analysis snapshot/);
 assert.equal(none.downloads.length,0);
});

test('JSON ZIP keeps repeated batches whole and preserves full original JSON bytes without chart generation',async()=>{
  const calls=[];
  const f=fixture(async(path,options)=>{calls.push({path,options});return completed(path,options)},
    [{id:'old-attempt',batchId:'batch',repetitions:3,state:'failed'},
      {id:'one',batchId:'batch',repetitions:3},{id:'retry',batchId:'batch',repetitions:3,previousRunIds:['old-attempt']},
      {id:'single'},{id:'unselected',batchId:'other',repetitions:2}]);
  const original=images.buildCharts;images.buildCharts=()=>{throw new Error('JSON export must not create charts')};
  try {await f.download('results',['batch:batch','run:single'])}finally{images.buildCharts=original}
  assert.equal(f.links.length,1);
  assert.match(f.links[0].download,/^results-analysis-\d{8}T\d{6}Z\.zip$/);
  const files=entries(await f.downloads[0].arrayBuffer());
  assert.deepEqual(files.map(file=>file.name),['[한글 _ Analysis]-batch-batch-analysis.json','[한글 _ Analysis]-single-analysis.json']);
  for(const [index,source] of [batchArtifact('batch'),artifact('single')].entries()) {
    assert.deepEqual(files[index].data,Buffer.from(source,'utf8'));
    assert.match(files[index].data.toString(),/18446744073709551615/);
    assert.doesNotMatch(files[index].data.toString(),/18446744073709552000/);
  }
  assert.ok(calls.every(call=>!call.options.method&&!call.path.includes('unselected')));
  assert.ok(calls.filter(call=>call.path.includes('/result?')).every(call=>call.path.endsWith('&saved=1')));
  assert.deepEqual(calls.map(call=>call.path),[
    '/api/v1/batch-analysis-jobs/batch','/api/v1/batch-analysis-jobs/batch/result?jobId=saved-batch&saved=1',
    '/api/v1/analysis-jobs/single','/api/v1/analysis-jobs/single/result?jobId=saved-single&saved=1']);
});

test('single submissions with batch IDs and legacy results retain per-Run JSON including previous attempts',async()=>{
  const calls=[];
  const f=fixture(async(path,options)=>{calls.push(path);return completed(path,options)},
    [{id:'original',batchId:'single-batch',repetitions:1,state:'failed'},
      {id:'retry',batchId:'single-batch',repetitions:1,previousRunIds:['original']},
      {id:'legacy',batchId:'legacy-batch'},{id:'standalone'}]);
  await f.download('results',['batch:single-batch','batch:legacy-batch','run:standalone']);
  assert.ok(calls.every(path=>!path.includes('/batch-analysis-jobs/')));
  const files=entries(await f.downloads[0].arrayBuffer());
  assert.deepEqual(files.map(file=>file.name),['original','retry','legacy','standalone'].map(id=>`[한글 _ Analysis]-${id}-analysis.json`));
});

test('a repetition uses its batch artifact even with missing listed members and incomplete first-member metadata',async()=>{
  for(const results of [[{id:'remaining',batchId:'batch',repetitions:3}],
    [{id:'partial',batchId:'batch'},{id:'known',batchId:'batch',repetitions:3}]]){
    const calls=[];
    const f=fixture(async(path,options)=>{calls.push(path);return completed(path,options)},results);
    await f.download('results',['batch:batch','batch:batch']);
    assert.equal(calls.length,2);
    assert.ok(calls.every(path=>path.startsWith('/api/v1/batch-analysis-jobs/batch')));
    assert.equal(entries(await f.downloads[0].arrayBuffer()).length,1);
  }
});

test('JSON export counts selected artifacts rather than expanding a long repetition attempt history',async()=>{
  let calls=0;
  const f=fixture(async(path,options)=>{calls++;return completed(path,options)},
    Array.from({length:2001},(_,i)=>({id:'attempt-'+i,batchId:'batch',repetitions:2})));
  await f.download('results',['batch:batch']);
  assert.equal(calls,2);
  const tooMany=fixture(()=>{throw Error('must not request')},Array.from({length:2001},(_,i)=>({id:'run-'+i})));
  await assert.rejects(tooMany.download('results',Array.from({length:2001},(_,i)=>'run:run-'+i)),/2,000 result sets/);
});

test('JSON export reuses completed analysis while verification or replacement is pending or failed',async()=>{
  for(const job of [{state:'running',id:'replacement',savedAnalysisId:'old'},{state:'failed',id:'replacement',savedAnalysisId:'old'},{state:'idle',stale:true,id:'old'}]) {
    const calls=[],f=fixture(async(path,options)=>{
      calls.push({path,options});
      return path.includes('/result?')?artifact('one','old'):{...job,runId:'one'};
    });
    await f.download('results',['run:one']);
    assert.equal(calls.length,2);
    assert.equal(calls[1].path,'/api/v1/analysis-jobs/one/result?jobId=old&saved=1');
    assert.ok(calls.every(call=>!call.options.method));
    assert.equal(f.links.length,1);
  }
});

test('batch JSON reuses the last completed snapshot while verification or replacement is pending or failed',async()=>{
  for(const job of [{state:'queued',id:'replacement',savedAnalysisId:'old'},
    {state:'running',id:'replacement',savedAnalysisId:'old'},
    {state:'failed',id:'replacement',savedAnalysisId:'old'},
    {state:'idle',stale:true,id:'old'}]){
    const calls=[],f=fixture(async(path,options)=>{
      calls.push({path,options});
      return path.includes('/result?')?batchArtifact('batch','old'):{...job,batchId:'batch'};
    },[{id:'member',batchId:'batch',repetitions:3}]);
    await f.download('results',['batch:batch']);
    assert.equal(calls.length,2);
    assert.equal(calls[1].path,'/api/v1/batch-analysis-jobs/batch/result?jobId=old&saved=1');
    assert.ok(calls.every(call=>!call.options.method));
    assert.deepEqual(entries(await f.downloads[0].arrayBuffer())[0].data,Buffer.from(batchArtifact('batch','old')));
  }
});

test('legacy batch exclusions remain byte-identical in selected analysis JSON downloads', async () => {
  for (const source of [
    batchArtifact('batch', 'old').replace(/"excluded": \[[\s\S]*?\],/, '"excluded": null,'),
    batchArtifact('batch', 'old').replace(/  "excluded": \[[\s\S]*?\],\n/, ''),
  ]) {
    const calls = [], f = fixture(async (path, options) => {
      calls.push([path, options.method]);
      return path.includes('/result?') ? source : {batchId:'batch', state:'failed', id:'replacement', savedAnalysisId:'old'};
    }, [{id:'member', batchId:'batch', repetitions:3}]);
    await f.download('results', ['batch:batch']);
    assert.deepEqual(calls, [
      ['/api/v1/batch-analysis-jobs/batch', undefined],
      ['/api/v1/batch-analysis-jobs/batch/result?jobId=old&saved=1', undefined],
    ]);
    const files = entries(await f.downloads[0].arrayBuffer());
    assert.equal(files.length, 1);
    assert.deepEqual(files[0].data, Buffer.from(source));
  }
});

test('missing batch analysis is queued once with its observed attempt and finished before the next selected result',async()=>{
  const calls=[],progress=[];let reads=0;
  const f=fixture(async(path,options)=>{
    calls.push({path,options});
    if(!path.includes('/batch-analysis-jobs/'))return completed(path,options);
    if(path.includes('/result?'))return batchArtifact('batch','new-batch');
    if(options.method==='POST'){
      assert.deepEqual(JSON.parse(options.body),{expectedAnalysis:{id:'failed-batch',state:'failed'}});
      return {batchId:'batch',id:'new-batch',state:'queued'};
    }
    reads++;
    return reads===1?{batchId:'batch',id:'failed-batch',state:'failed'}:
      {batchId:'batch',id:'new-batch',state:reads===2?'running':'completed',phase:'run 2/3'};
  },[{id:'one',batchId:'batch',repetitions:3},{id:'two',batchId:'batch',repetitions:3},{id:'single'}]);
  await f.download('results',['batch:batch','run:single'],{onProgress:message=>progress.push(message)});
  assert.equal(calls.filter(call=>call.options.method==='POST').length,1);
  assert.ok(calls.find(call=>call.options.method==='POST').path==='/api/v1/batch-analysis-jobs/batch');
  assert.ok(calls.findIndex(call=>call.path==='/api/v1/analysis-jobs/single')>
    calls.findIndex(call=>call.path==='/api/v1/batch-analysis-jobs/batch/result?jobId=new-batch'));
  assert.ok(progress.some(message=>message.includes('JSON ZIP 1/2 · Batch batch · Analysis queued')));
  assert.equal(entries(await f.downloads[0].arrayBuffer()).length,2);
});

test('batch attempt conflicts and insufficient completed members stop the export without per-Run fallback',async()=>{
  for(const conflict of [true,false]){
    const calls=[];
    const f=fixture(async(path,options)=>{
      calls.push({path,options});
      assert.ok(path.startsWith('/api/v1/batch-analysis-jobs/batch'));
      if(options.method==='POST')throw Object.assign(Error(conflict?'attempt changed':'at least two completed runs are required'),
        {status:conflict?409:422,...(conflict?{code:'analysis-attempt-changed'}:{})});
      return {batchId:'batch',state:'idle'};
    },[{id:'only-success',batchId:'batch',repetitions:3}]);
    await assert.rejects(f.download('results',['batch:batch']),conflict?/outcome unknown/:/at least two completed/);
    assert.equal(calls.length,2);
    assert.equal(f.downloads.length,0);
  }
});

test('invalid, replaced or unrelated batch snapshots prevent a partial selection download',async()=>{
  for(const failure of ['malformed','missing','wrong-job','wrong-batch','wrong-analysis','wrong-aggregation','duplicate-run','unrelated-run']){
    const f=fixture(async(path,options)=>{
      if(!path.includes('/batch-analysis-jobs/'))return completed(path,options);
      if(!path.includes('/result?'))return failure==='wrong-job'?{runId:'batch',id:'saved-batch',state:'completed'}:
        {batchId:'batch',id:'saved-batch',state:'completed'};
      if(failure==='malformed')return '{unfinished';
      if(failure==='missing')throw Error('archive unavailable');
      const data=JSON.parse(batchArtifact('batch'));
      if(failure==='wrong-batch')data.batchId='other';
      if(failure==='wrong-analysis')data.analysisId='replacement';
      if(failure==='wrong-aggregation')data.aggregation='sum';
      if(failure==='duplicate-run')data.runs[1]=data.runs[0];
      if(failure==='unrelated-run')data.runs[0].result.batchId='other';
      return JSON.stringify(data);
    },[{id:'single'},{id:'member',batchId:'batch',repetitions:3}]);
    await assert.rejects(f.download('results',['run:single','batch:batch']),/Batch batch:/);
    assert.equal(f.downloads.length,0);
    assert.equal(f.links.length,0);
  }
});

test('batch JSON cancellation and UTF-8 ZIP size bounds retain the existing all-or-nothing export contract',async()=>{
  const controller=new AbortController();let requests=0;
  const canceled=fixture(async()=>{requests++;return {batchId:'batch',id:'pending',state:'queued'}},
    [{id:'member',batchId:'batch',repetitions:2}]);
  await assert.rejects(canceled.download('results',['batch:batch'],{signal:controller.signal,onProgress(){controller.abort()}}),{name:'AbortError'});
  assert.equal(requests,1);assert.equal(canceled.downloads.length,0);
  const name='[한글 _ Analysis]-batch-batch-analysis.json';
  const exact=22+76+2*Buffer.byteLength(name)+Buffer.byteLength(batchArtifact('batch'));
  for(const accepted of [true,false]){
    const f=fixture(completed,[{id:'member',batchId:'batch',repetitions:2}],{byteLimit:accepted?exact:exact-1});
    if(accepted){await f.download('results',['batch:batch']);assert.equal(f.downloads[0].size,exact);}
    else {await assert.rejects(f.download('results',['batch:batch']),/128 MiB/);assert.equal(f.downloads.length,0);}
  }
});

test('batch and standalone results with the same ID retain distinct JSON entries and endpoints',async()=>{
  const f=fixture(completed,[{id:'member',batchId:'shared',repetitions:2},{id:'shared'}]);
  await f.download('results',['batch:shared','run:shared']);
  const files=entries(await f.downloads[0].arrayBuffer());
  assert.deepEqual(files.map(file=>file.name),['[한글 _ Analysis]-shared-batch-analysis.json','[한글 _ Analysis]-shared-analysis.json']);
  assert.deepEqual(files[0].data,Buffer.from(batchArtifact('shared')));
  assert.deepEqual(files[1].data,Buffer.from(artifact('shared')));
});

test('missing analyses are prepared sequentially with progress before JSON export',async()=>{
  const calls=[],progress=[],polls=new Map();
  const f=fixture(async(path,options)=>{
    const id=path.split('/')[4];calls.push(`${options.method||'GET'} ${path}`);
    if(path.includes('/result?'))return artifact(id,'new-'+id);
    if(options.method==='POST')return {runId:id,id:'new-'+id,state:'queued'};
    const count=polls.get(id)||0;polls.set(id,count+1);
    return count===0?{runId:id,state:'idle'}:{runId:id,id:'new-'+id,state:count===1?'running':'completed',phase:'events.jsonl'};
  },[{id:'one'},{id:'two'}]);
  await f.download('results',['run:one','run:two'],{onProgress:message=>progress.push(message)});
  assert.ok(calls.indexOf('GET /api/v1/analysis-jobs/two')>calls.indexOf('GET /api/v1/analysis-jobs/one/result?jobId=new-one'));
  assert.equal(calls.filter(call=>call.startsWith('POST')).length,2);
  assert.ok(progress.some(message=>message.includes('JSON ZIP 1/2')&&message.includes('Analyzing')));
  assert.equal(entries(await f.downloads[0].arrayBuffer()).length,2);
});

test('analysis export binds the observed job and stops changed attempts without retrying POST',async()=>{
  let posts=0;
  const f=fixture(async(path,options)=>{
    if(options.method==='POST'){
      posts++;assert.equal(options.headers['Content-Type'],'application/json');
      assert.deepEqual(JSON.parse(options.body),{expectedAnalysis:{id:'failed-old',state:'failed'}});
      throw Object.assign(Error('changed attempt'),{status:409,code:'analysis-attempt-changed'});
    }
    return {id:'failed-old',runId:'one',state:'failed'};
  });
  await assert.rejects(f.download('results',['run:one']),/outcome unknown/);
  assert.equal(posts,1);assert.equal(f.links.length,0);
});

test('invalid, missing or replaced JSON prevents downloading an incomplete selection',async()=>{
  for(const failure of ['malformed','missing','changed','wrong-run','wrong-job']) {
    const f=fixture(async(path,options)=>{
      const id=path.split('/')[4];
      if(!path.includes('/result?'))return {runId:failure==='wrong-job'&&id==='two'?'other':id,id:'saved-'+id,state:'completed'};
      if(id==='one')return artifact(id);
      if(failure==='malformed')return '{unfinished';
      if(failure==='missing')throw new Error('archive unavailable');
      return artifact(failure==='wrong-run'?'other':id,failure==='changed'?'replacement':'saved-'+id);
    },[{id:'one'},{id:'two'}]);
    await assert.rejects(f.download('results',['run:one','run:two']),/two:/);
    assert.equal(f.downloads.length,0);assert.equal(f.links.length,0);
  }
});

test('JSON preparation and request cancellation never start a download and timeouts are retryable',async()=>{
  for(const phase of ['queued','Preparing analysis JSON','Preparing selected analysis']) {
    const controller=new AbortController();
    const f=fixture(async(path,options)=>phase==='queued'?{runId:'one',id:'pending',state:'queued'}:completed(path,options));
    await assert.rejects(f.download('results',['run:one'],{signal:controller.signal,onProgress(message){if(message.toLowerCase().includes(phase.toLowerCase()))controller.abort()}}),{name:'AbortError'});
    assert.equal(f.links.length,0);
    assert.equal(f.revoked.length,f.downloads.length);
  }
  const timed=fixture((path,options)=>new Promise((resolve,reject)=>options.signal.addEventListener('abort',()=>reject(new DOMException('timeout','AbortError')))),undefined,{requestTimeout:1});
  await assert.rejects(timed.download('results',['run:one']),/Download → Analysis JSON ZIP again.*continues on the server/);
  assert.equal(timed.links.length,0);
});

test('JSON ZIP counts UTF-8 and ZIP overhead and rejects stale selection before any request',async()=>{
  const id='one',name=`[한글 _ Analysis]-${id}-analysis.json`;
  const exact=22+76+2*Buffer.byteLength(name)+Buffer.byteLength(artifact(id));
  const accepted=fixture(completed,undefined,{byteLimit:exact});
  await accepted.download('results',['run:one']);
  assert.equal(accepted.downloads[0].size,exact);
  const tooLarge=fixture(completed,undefined,{byteLimit:exact-1});
  await assert.rejects(tooLarge.download('results',['run:one']),/128 MiB.*fewer/);
  assert.equal(tooLarge.links.length,0);
  const noCalls=fixture(()=>{throw new Error('must not request')},[{id:'../escape'}]);
  await assert.rejects(noCalls.download('scenarios',['scenario:one']),/saved results only/);
  await assert.rejects(noCalls.download('results',['run:gone']),/no longer listed/);
  await assert.rejects(noCalls.download('results',['run:../escape']),/Invalid result identity/);
});

test('the shared export module is embedded and wired independently for CSV and JSON',()=>{
  const html=fs.readFileSync(__dirname+'/static/index.html','utf8'),app=fs.readFileSync(__dirname+'/static/app.js','utf8');
  assert.ok(html.indexOf('/result-analysis-downloads.js')>html.indexOf('/result-images.js'));
  assert.ok(html.indexOf('/result-analysis-downloads.js')<html.indexOf('/app.js'));
  assert.match(app,/downloadCSV: globalThis.KPLResultAnalysisDownloads\?\.createDownloads/);
  assert.match(app,/downloadJSON: globalThis.KPLResultAnalysisDownloads\?\.createDownloads\(\{api, format:"json"/);
});
