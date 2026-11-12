const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const {createDownloads} = require('./static/result-analysis-downloads.js');
const images = require('./static/result-images.js');
const research = require('./static/research-charts.js');
const files = require('./static/research-files.js');
const batch = require('./static/batch-analysis.js');

function artifact(id, analysisId='saved-'+id) {
  return {version:1,analysisId,result:{id,name:'한글 CSV / 실험',state:'completed'},asOf:'2026-10-13T00:00:00Z',
    binSeconds:2,metrics:{definition:'session-window-v1',deliveryWindows:['1s'],latencySamples:2,averageLatencyMs:15,p95LatencyMs:20},
    latencyCDF:[{x:10,y:.5},{x:20,y:1}],latencyHistogram:[{x:10,y:1},{x:20,y:1}],
    timeline:[{at:'2026-10-13T00:00:00Z',publish:2,deliver:4,duplicate:0}],observations:[]};
}

function batchArtifact(id, analysisId='saved-'+id) {
  return {version:1,analysisVersion:9,analysisId,batchId:id,name:'한글 CSV / 실험',aggregation:'equal-run-mean-v1',
    asOf:'2026-10-13T01:00:00Z',sourceHash:'sha256:batch-source',
    expectedRuns:3,missingRuns:0,excluded:[{id:'old-attempt',state:'failed'}],
    runs:['one','two'].map(run=>({...artifact(id+'-'+run),result:{id:id+'-'+run,name:'한글 CSV / 실험',batchId:id,state:'completed',startedAt:'2026-10-13T00:00:00Z',sourceRevision:'captured-'+run}})),
    summary:{'metrics.averageLatencyMs':{average:15,deviation:2,count:2},'metrics.p95LatencyMs':{average:20,deviation:3,count:2}}};
}

function entries(bytes) {
  const data=Buffer.from(bytes), result=[];
  let offset=0;
  while(data.readUInt32LE(offset)===0x04034b50) {
    assert.equal(data.readUInt16LE(offset+8),0,'CSV bundles use the existing stored ZIP writer');
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

function fixture(api, results=[{id:'one'}], options={}) {
  const downloads=[], links=[];
  const url={createObjectURL(blob){downloads.push(blob);return 'blob:csv-'+downloads.length},revokeObjectURL(){}};
  const document={createElement(tag){assert.equal(tag,'a','CSV export must never create a canvas or image');const link={click(){links.push(this)},remove(){}};return link},body:{appendChild(){}}};
  return {downloads,links,download:createDownloads({api,getResults:()=>results,document,url,pollInterval:0,...options})};
}

test('selected repetition batches reuse batch means alongside individual CSV ZIPs without per-member analysis', async()=>{
  const calls=[];
  const f=fixture(async(path,options)=>{
    calls.push({path,options});
    const id=decodeURIComponent(path.split('/')[4]);
    if(path.includes('/batch-analysis-jobs/'))return path.includes('/result?') ? batchArtifact(id) : {batchId:id,id:'saved-'+id,state:'completed'};
    return path.includes('/result?') ? artifact(id) : {runId:id,id:'saved-'+id,state:'completed',stale:true};
  },[{id:'old-attempt',batchId:'batch',repetitions:3,state:'failed'},
    {id:'one',batchId:'batch',repetitions:3},{id:'retry',batchId:'batch',repetitions:3},
    {id:'second',batchId:'another',repetitions:2},{id:'single'},{id:'unselected',batchId:'other',repetitions:2}]);
  await f.download('results',['batch:batch','batch:another','run:single']);
  assert.equal(f.downloads.length,1);
  assert.equal(f.links.length,1);
  assert.match(f.links[0].download,/^results-csv-\d{8}T\d{6}Z\.zip$/);
  const outer=entries(await f.downloads[0].arrayBuffer());
  assert.equal(outer.length,3);
  for(const [index,id] of ['batch','another','single'].entries()) {
    const isBatch=id!=='single', artifactID=isBatch?`${id}-batch-mean`:id;
    assert.equal(outer[index].name,`[한글 CSV _ 실험]-${artifactID}-csv.zip`);
    const csvs=entries(outer[index].data);
    if(isBatch) {
      const summary=csvs.shift();
      assert.equal(summary.name,`${artifactID}-summary.csv`);
      assert.equal(summary.data.toString('utf8'),batch.summaryCSV(batchArtifact(id).summary));
      const snapshot=csvs.shift();
      assert.equal(snapshot.name,`${artifactID}-snapshot.csv`);
      const rows=files.csvRows(snapshot.data.toString('utf8'));
      assert.deepEqual(rows[0],['scope','id','analysis_id','analysis_version','as_of','source_hash','source_revision','state']);
      assert.deepEqual(rows[1],['batch',id,'saved-'+id,'9','2026-10-13T01:00:00Z','sha256:batch-source','','']);
      assert.deepEqual(rows.slice(2).map(row=>[row[0],row[1],row[6],row[7]]),
        [['included-run',id+'-one','captured-one','completed'],['included-run',id+'-two','captured-two','completed'],['excluded-run','old-attempt','','failed']]);
    }
    const charts=(isBatch?batch.build(batchArtifact(id),images.buildCharts):images.buildCharts(artifact(id))).map(research.formatChart);
    assert.deepEqual(csvs.map(f=>f.name),charts.map(chart=>`${artifactID}-${chart.id}.csv`));
    for(const [chartIndex,file] of csvs.entries()) assert.equal(file.data.toString('utf8'),files.chartCSV(charts[chartIndex]));
    assert.ok(csvs.every(file=>file.name.endsWith('.csv')));
  }
  assert.ok(calls.every(call=>!call.options.method),'completed analyses must be reused without reanalysis');
  assert.ok(calls.filter(call=>call.path.includes('/result?')).every(call=>call.path.endsWith('&saved=1')));
  assert.ok(calls.every(call=>!call.path.includes('unselected')));
  assert.deepEqual(calls.map(call=>call.path),[
    '/api/v1/batch-analysis-jobs/batch','/api/v1/batch-analysis-jobs/batch/result?jobId=saved-batch&saved=1',
    '/api/v1/batch-analysis-jobs/another','/api/v1/batch-analysis-jobs/another/result?jobId=saved-another&saved=1',
    '/api/v1/analysis-jobs/single','/api/v1/analysis-jobs/single/result?jobId=saved-single&saved=1']);
});

test('single submissions and legacy result groups retain per-Run CSVs including previous attempts',async()=>{
  const calls=[],f=fixture(async(path,options)=>{
    calls.push(path);
    const id=path.split('/')[4];
    return path.includes('/result?')?artifact(id):{runId:id,id:'saved-'+id,state:'completed'};
  },[{id:'one',batchId:'single',repetitions:1},{id:'retry',batchId:'single',repetitions:1},
    {id:'legacy',batchId:'legacy'},{id:'standalone'}]);
  await f.download('results',['batch:single','batch:legacy','run:standalone']);
  assert.ok(calls.every(path=>!path.includes('/batch-analysis-jobs/')));
  assert.deepEqual(entries(await f.downloads[0].arrayBuffer()).map(file=>file.name),
    ['one','retry','legacy','standalone'].map(id=>`[한글 CSV _ 실험]-${id}-csv.zip`));
});

test('batch CSV reuses saved snapshots through replacement states and missing member results',async()=>{
  for(const job of [{state:'queued',id:'replacement',savedAnalysisId:'old'},
    {state:'running',id:'replacement',savedAnalysisId:'old'},
    {state:'failed',id:'replacement',savedAnalysisId:'old'},
    {state:'idle',stale:true,id:'old'}]) {
    const calls=[],f=fixture(async(path,options)=>{
      calls.push([path,options.method]);
      return path.includes('/result?')?batchArtifact('batch','old'):{...job,batchId:'batch'};
    },[{id:'missing',batchId:'batch',repetitions:3,state:'missing',missing:true}]);
    await f.download('results',['batch:batch']);
    assert.deepEqual(calls,[['/api/v1/batch-analysis-jobs/batch',undefined],
      ['/api/v1/batch-analysis-jobs/batch/result?jobId=old&saved=1',undefined]]);
    assert.equal(entries(await f.downloads[0].arrayBuffer()).length,1);
  }
});

test('missing batch CSV analysis queues one batch job instead of separate member analyses',async()=>{
  const calls=[],progress=[];let reads=0;
  const f=fixture(async(path,options)=>{
    calls.push([path,options]);
    assert.match(path,/^\/api\/v1\/batch-analysis-jobs\/batch/);
    if(path.includes('/result?'))return batchArtifact('batch','new-batch');
    if(options.method==='POST') {
      assert.deepEqual(JSON.parse(options.body),{expectedAnalysis:{id:'',state:'idle'}});
      return {batchId:'batch',id:'new-batch',state:'queued'};
    }
    return reads++===0?{batchId:'batch',state:'idle'}:{batchId:'batch',id:'new-batch',state:'completed'};
  },[{id:'one',batchId:'batch',repetitions:3},{id:'two',batchId:'batch',repetitions:3}]);
  await f.download('results',['batch:batch'],{onProgress:message=>progress.push(message)});
  assert.equal(calls.filter(([,options])=>options.method==='POST').length,1);
  assert.equal(calls.length,4);
  assert.ok(progress.some(message=>message.includes('Analysis queued')));
  assert.equal(entries(await f.downloads[0].arrayBuffer()).length,1);
});

test('unavailable or mismatched batch CSV analysis fails without per-Run fallback or partial export',async()=>{
  for(const failure of ['unavailable','wrong-batch','missing-snapshot']) {
    const calls=[],f=fixture(async(path,options)=>{
      calls.push([path,options.method]);
      assert.match(path,/^\/api\/v1\/batch-analysis-jobs\/batch/);
      if(failure==='missing-snapshot')return {batchId:'batch',state:'idle'};
      if(path.includes('/result?')) {
        if(failure==='unavailable')throw new Error('source unavailable');
        return batchArtifact('other');
      }
      return {batchId:'batch',id:'saved-batch',state:'completed'};
    },[{id:'missing',batchId:'batch',repetitions:3,state:'missing',missing:true}]);
    await assert.rejects(f.download('results',['batch:batch']),/Batch batch:.*(unavailable|Invalid batch|no completed analysis snapshot)/);
    assert.ok(calls.every(([,method])=>!method));
    assert.equal(f.downloads.length,0);
  }
});

test('batch CSV cancellation and changed snapshots never start a download',async()=>{
  for(const stage of ['queued','csv','replaced']) {
    const controller=new AbortController(),calls=[];
    const f=fixture(async(path,options)=>{
      calls.push([path,options.method]);
      if(stage==='queued')return {batchId:'batch',id:'pending',state:'queued'};
      return path.includes('/result?')?batchArtifact('batch',stage==='replaced'?'replacement':'old'):
        {batchId:'batch',id:'old',state:'completed'};
    },[{id:'new-member',batchId:'batch',repetitions:3}]);
    await assert.rejects(f.download('results',['batch:batch'],{signal:controller.signal,onProgress(message){
      if(stage==='queued'&&message.includes('Analysis queued')||stage==='csv'&&message.includes('Preparing CSV'))controller.abort();
    }}),stage==='replaced'?/saved analysis changed/:{name:'AbortError'});
    assert.equal(calls.length,stage==='queued'?1:2);
    assert.ok(calls.every(([,method])=>!method));
    assert.equal(f.downloads.length,0);
  }
});

test('verification or failed replacement exports its saved completed analysis immediately', async()=>{
  for(const job of [{state:'running',id:'replacement',savedAnalysisId:'old'},{state:'failed',id:'replacement',savedAnalysisId:'old'},{state:'idle',stale:true,id:'old'}]) {
    const calls=[],f=fixture(async(path,options)=>{
      calls.push({path,options});
      return path.includes('/result?') ? artifact('one','old') : {...job,runId:'one'};
    });
    await f.download('results',['run:one']);
    assert.equal(calls.length,2);
    assert.equal(calls[1].path,'/api/v1/analysis-jobs/one/result?jobId=old&saved=1');
    assert.equal(f.links.length,1);
  }
});

test('missing analyses are submitted and polled sequentially before building each CSV bundle',async()=>{
  const calls=[],progress=[],polls=new Map();
  const f=fixture(async(path,options)=>{
    const id=path.split('/')[4];calls.push(`${options.method||'GET'} ${path}`);
    if(path.includes('/result?')) return artifact(id,'new-'+id);
    if(options.method==='POST')return {runId:id,id:'new-'+id,state:'queued'};
    const seen=polls.get(id)||0;polls.set(id,seen+1);
    return seen===0?{runId:id,state:'idle'}:{runId:id,id:'new-'+id,state:seen===1?'running':'completed',phase:'events.jsonl'};
  },[{id:'one'},{id:'two'}]);
  await f.download('results',['run:one','run:two'],{onProgress:message=>progress.push(message)});
  assert.ok(calls.indexOf('GET /api/v1/analysis-jobs/two')>calls.indexOf('GET /api/v1/analysis-jobs/one/result?jobId=new-one'));
  assert.equal(calls.filter(call=>call.startsWith('POST')).length,2);
  assert.ok(calls.every(call=>!call.includes('refresh=1')));
  assert.ok(progress.some(message=>message.includes('Analysis queued')));
  assert.ok(progress.some(message=>message.includes('Analyzing · events.jsonl')));
  assert.equal(entries(await f.downloads[0].arrayBuffer()).length,2);
});

test('an invalid snapshot or unreadable selected result prevents a partial ZIP download',async()=>{
  for(const failure of ['changed','missing','wrong-run','wrong-job']) {
    const f=fixture(async path=>{
      const id=path.split('/')[4];
      if(!path.includes('/result?')) return {runId:failure==='wrong-job'&&id==='two'?'other':id,id:'saved-'+id,state:'completed'};
      if(id==='one')return artifact(id);
      if(failure==='missing')throw new Error('source unavailable');
      return artifact(failure==='wrong-run'?'other':id,failure==='changed'?'replaced':'saved-'+id);
    },[{id:'one'},{id:'two'}]);
    await assert.rejects(f.download('results',['run:one','run:two']),/two:.*(changed|unavailable|Unexpected|analysis|invalid)/i);
    assert.equal(f.downloads.length,0);
    assert.equal(f.links.length,0);
  }
});

test('canceling queued analysis stops polling and never starts a partial download',async()=>{
  const controller=new AbortController(),calls=[];
  const f=fixture(async(path,options)=>{calls.push({path,options});return {runId:'one',id:'pending',state:'queued'}});
  await assert.rejects(f.download('results',['run:one'],{signal:controller.signal,onProgress(){controller.abort()}}),{name:'AbortError'});
  assert.equal(calls.length,1);
  assert.equal(f.links.length,0);
  const aborted=new AbortController();aborted.abort();
  await assert.rejects(f.download('results',['run:one'],{signal:aborted.signal}),{name:'AbortError'});
  assert.equal(calls.length,1);
});

test('cancel during CSV creation stops the bundle and request timeout leaves analysis retryable',async()=>{
  const controller=new AbortController();
  const f=fixture(async path=>path.includes('/result?')?artifact('one'):{runId:'one',id:'saved-one',state:'completed'});
  await assert.rejects(f.download('results',['run:one'],{signal:controller.signal,onProgress(message){if(message.includes('Preparing CSV'))controller.abort()}}),{name:'AbortError'});
  assert.equal(f.downloads.length,0);
  const timed=fixture((path,options)=>new Promise((resolve,reject)=>options.signal.addEventListener('abort',()=>reject(new DOMException('timeout','AbortError')))),undefined,{requestTimeout:1});
  await assert.rejects(timed.download('results',['run:one']),/timed out.*continues on the server/);
  assert.equal(timed.links.length,0);
});

test('cancellation wins over a crossed request error and malformed jobs never trigger new analysis',async()=>{
  const controller=new AbortController();
  const canceled=fixture(async()=>{controller.abort();throw new Error('crossed failure')});
  await assert.rejects(canceled.download('results',['run:one'],{signal:controller.signal}),{name:'AbortError'});
  for(const state of ['queued','running','completed']) {
    let requests=0;
    const invalid=fixture(async()=>{requests++;return {runId:'one',state}});
    await assert.rejects(invalid.download('results',['run:one']),/Unexpected analysis job/);
    assert.equal(requests,1);
    assert.equal(invalid.links.length,0);
  }
});

test('size, selection and path limits reject exports before a completed download exists',async()=>{
  let requests=0;
  const f=fixture(async path=>{requests++;return path.includes('/result?')?artifact('one'):{runId:'one',id:'saved-one',state:'completed'}},undefined,{byteLimit:128});
  await assert.rejects(f.download('results',['run:one']),/128 MiB.*fewer/);
  assert.equal(f.links.length,0);
  await assert.rejects(f.download('scenarios',['scenario:one']),/saved results only/);
  await assert.rejects(f.download('results',['run:gone']),/no longer listed/);
  const invalid=fixture(async()=>{requests++;},[{id:'../escape'}]);
  const before=requests;
  await assert.rejects(invalid.download('results',['run:../escape']),/Invalid result identity/);
  assert.equal(requests,before);
});

test('CSV export module is embedded before the app and selection UI connects it separately',()=>{
  const html=fs.readFileSync(__dirname+'/static/index.html','utf8'),app=fs.readFileSync(__dirname+'/static/app.js','utf8');
  assert.ok(html.indexOf('/result-analysis-downloads.js')>html.indexOf('/result-images.js'));
  assert.ok(html.indexOf('/result-analysis-downloads.js')<html.indexOf('/app.js'));
  assert.match(app,/downloadCSV: globalThis.KPLResultAnalysisDownloads\?\.createDownloads/);
});
