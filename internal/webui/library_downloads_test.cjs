const test = require('node:test');
const assert = require('node:assert/strict');
const {selectedIDs, createDownloads} = require('./static/library-downloads.js');

test('raw selections exclude missing placeholders and explain an entirely missing selection',()=>{
 const results=[{id:'saved',batchId:'b'},{id:'missing-slot',batchId:'b',state:'missing',missing:true},
  {id:'missing-all',batchId:'gone',state:'missing',missing:true}];
 assert.deepEqual(selectedIDs('results',['batch:b'],[],results),['saved']);
 assert.throws(()=>selectedIDs('results',['batch:gone'],[],results),/data is missing/);
});

test('selected result batches include every Run and previous attempt, without unrelated groups', () => {
 const results=[{id:'first',batchId:'batch'}, {id:'retry',batchId:'batch'}, {id:'single'}, {id:'other',batchId:'other'}, {id:'first',batchId:'batch'}];
 assert.deepEqual(selectedIDs('results',['batch:batch','run:single'],[],results),['first','retry','single']);
 assert.deepEqual(selectedIDs('scenarios',['scenario:two'],[{id:'one'},{id:'two'}],results),['two']);
 assert.throws(()=>selectedIDs('results',['batch:gone'],[],results),/no longer listed/);
 assert.throws(()=>selectedIDs('scenarios',[],[],[]),/no longer listed/);
 assert.throws(()=>selectedIDs('results',['batch:huge'],[],Array.from({length:2001},(_,i)=>({id:String(i),batchId:'huge'}))),/2,000/);
});

test('bulk export prepares one authenticated request and starts a native download without fetching a Blob', async () => {
 const calls=[],actions=[];
 const link={click(){actions.push('click')},remove(){actions.push('remove')}};
 const document={createElement(tag){assert.equal(tag,'a');return link},body:{appendChild(value){assert.equal(value,link);actions.push('append')}}};
 const download=createDownloads({document,getScenarios:()=>[{id:'one'}],getResults:()=>[],api:async(path,options)=>{
  calls.push({path,options});return {url:'/api/v1/downloads/token'};
 }});
 await download('scenarios',['scenario:one']);
 assert.equal(calls.length,1);
 assert.equal(calls[0].path,'/api/v1/downloads');
 assert.equal(calls[0].options.method,'POST');
 assert.deepEqual(JSON.parse(calls[0].options.body),{kind:'scenarios',ids:['one']});
 assert.deepEqual(actions,['append','click','remove']);
 assert.equal(link.href,'/api/v1/downloads/token');
 assert.equal(link.download,'');
 assert.equal(link.rel,'noopener');
});

test('invalid server links and preparation failures do not initiate downloads', async () => {
 let created=0;
 for (const url of ['https://foreign.invalid/file.zip','//foreign.invalid/zip','/api/v1/downloads/../auth/logout']) {
  const download=createDownloads({getScenarios:()=>[{id:'one'}],getResults:()=>[],document:{createElement(){created++}},api:async()=>({url})});
  await assert.rejects(download('scenarios',['scenario:one']),/response was invalid/);
 }
 const download=createDownloads({getScenarios:()=>[{id:'one'}],getResults:()=>[],document:{createElement(){created++}},api:async()=>{throw new Error('unreadable selection')}});
 await assert.rejects(download('scenarios',['scenario:one']),/unreadable selection/);
 assert.equal(created,0);
});

test('extract manifest downloads CSV for the exact saved runs of the selection',async()=>{
 const request=[],blobs=[],links=[];
 const csv='payload_bytes,degree,seed,run_id,label\r\n1000,6,9007199254740993,run-first,case\r\n';
 const download=createDownloads({manifest:true,getResults:()=>[{id:'run-first',batchId:'b'},
   {id:'missing',state:'missing',batchId:'b'},{id:'run-first',batchId:'b'},{id:'run-other'}],
   api:async(path,options)=>{request.push({path,options});return csv},
   url:{createObjectURL(blob){blobs.push(blob);return 'blob:manifest'},revokeObjectURL(){}},
   document:{createElement(){return {click(){links.push(this)},remove(){}}},body:{appendChild(){}}}});
 await assert.rejects(download('scenarios',['scenario:first']),/saved results only/);
 await download('results',['batch:b']);
 assert.equal(request.length,1);
 assert.equal(request[0].path,'/api/v1/downloads');
 assert.equal(request[0].options.responseType,'text');
 assert.deepEqual(JSON.parse(request[0].options.body),{kind:'extract-manifest',ids:['run-first']});
 assert.equal(links[0].download,'kpl_extract_manifest.csv');
 assert.equal(await blobs[0].text(),csv);
});
