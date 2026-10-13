const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const {createDownloads} = require('./static/result-csv-downloads.js');
const images = require('./static/result-images.js');
const research = require('./static/research-charts.js');
const files = require('./static/research-files.js');

function artifact(id, analysisId='saved-'+id) {
  return {version:1,analysisId,result:{id,name:'한글 CSV / 실험',state:'completed'},asOf:'2026-10-13T00:00:00Z',
    binSeconds:2,metrics:{definition:'session-window-v1',deliveryWindows:['1s'],latencySamples:2,averageLatencyMs:15,p95LatencyMs:20},
    latencyCDF:[{x:10,y:.5},{x:20,y:1}],latencyHistogram:[{x:10,y:1},{x:20,y:1}],
    timeline:[{at:'2026-10-13T00:00:00Z',publish:2,deliver:4,duplicate:0}],observations:[]};
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

test('selected batches become per-Run CSV ZIPs in one ZIP with every chart and no PNG or JSON', async()=>{
  const calls=[];
  const f=fixture(async(path,options)=>{
    calls.push({path,options});
    const id=decodeURIComponent(path.split('/')[4]);
    return path.includes('/result?') ? artifact(id) : {runId:id,id:'saved-'+id,state:'completed',stale:true};
  },[{id:'one',batchId:'batch'},{id:'retry',batchId:'batch'},{id:'single'},{id:'unselected',batchId:'other'}]);
  await f.download('results',['batch:batch','run:single']);
  assert.equal(f.downloads.length,1);
  assert.equal(f.links.length,1);
  assert.match(f.links[0].download,/^results-csv-\d{8}T\d{6}Z\.zip$/);
  const outer=entries(await f.downloads[0].arrayBuffer());
  assert.equal(outer.length,3);
  for(const [index,id] of ['one','retry','single'].entries()) {
    assert.equal(outer[index].name,`[한글 CSV _ 실험]-${id}-csv.zip`);
    const csvs=entries(outer[index].data), charts=images.buildCharts(artifact(id)).map(research.formatChart);
    assert.deepEqual(csvs.map(f=>f.name),charts.map(chart=>`${id}-${chart.id}.csv`));
    for(const [chartIndex,file] of csvs.entries()) assert.equal(file.data.toString('utf8'),files.chartCSV(charts[chartIndex]));
    assert.ok(csvs.every(file=>file.name.endsWith('.csv')));
  }
  assert.ok(calls.every(call=>!call.options.method),'completed analyses must be reused without reanalysis');
  assert.ok(calls.filter(call=>call.path.includes('/result?')).every(call=>call.path.endsWith('&saved=1')));
  assert.ok(calls.every(call=>!call.path.includes('unselected')));
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
  assert.ok(html.indexOf('/result-csv-downloads.js')>html.indexOf('/result-images.js'));
  assert.ok(html.indexOf('/result-csv-downloads.js')<html.indexOf('/app.js'));
  assert.match(app,/downloadCSV: globalThis.KPLResultCSVDownloads\?\.createDownloads/);
});
