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
  if(path.includes('/result?')) {assert.equal(options.responseType,'text');return artifact(id)}
  return {runId:id,id:'saved-'+id,state:'completed'};
}

test('JSON ZIP expands batches and preserves full original JSON bytes without chart generation',async()=>{
  const calls=[];
  const f=fixture(async(path,options)=>{calls.push({path,options});return completed(path,options)},
    [{id:'one',batchId:'batch'},{id:'retry',batchId:'batch',state:'failed'},{id:'single'},{id:'unselected'}]);
  const original=images.buildCharts;images.buildCharts=()=>{throw new Error('JSON export must not create charts')};
  try {await f.download('results',['batch:batch','run:single'])}finally{images.buildCharts=original}
  assert.equal(f.links.length,1);
  assert.match(f.links[0].download,/^results-analysis-\d{8}T\d{6}Z\.zip$/);
  const files=entries(await f.downloads[0].arrayBuffer());
  assert.deepEqual(files.map(file=>file.name),['one','retry','single'].map(id=>`[한글 _ Analysis]-${id}-analysis.json`));
  for(const [index,id] of ['one','retry','single'].entries()) {
    assert.deepEqual(files[index].data,Buffer.from(artifact(id),'utf8'));
    assert.match(files[index].data.toString(),/18446744073709551615/);
    assert.doesNotMatch(files[index].data.toString(),/18446744073709552000/);
  }
  assert.ok(calls.every(call=>!call.options.method&&!call.path.includes('unselected')));
  assert.ok(calls.filter(call=>call.path.includes('/result?')).every(call=>call.path.endsWith('&saved=1')));
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
