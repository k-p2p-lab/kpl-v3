const test = require('node:test');
const assert = require('node:assert/strict');
const {prepare, createAdmissions} = require('./static/result-bulk-actions.js');

const results = () => [
  {id:'historical',batchId:'batch',state:'failed',iteration:2,repetitions:2,superseded:true},
  {id:'completed',batchId:'batch',state:'completed',iteration:1,repetitions:2},
  {id:'failed',batchId:'batch',state:'failed',iteration:2,repetitions:2,previousRunIds:['historical','deleted-ancestor']},
];

test('retry admission never treats known completed, historical or deleted ancestor IDs as a fresh attempt',async()=>{
  for(const id of ['completed','historical','deleted-ancestor']){
    const plan=prepare('retry',['batch:batch'],results());
    assert.equal(plan.entries.length,1);
    const admissions=createAdmissions({api:async()=>({id,batchId:'batch',state:'queued'})});
    const report=await admissions.execute(plan);
    assert.equal(report.accepted.length,0,`stale ${id} was claimed as new queued work`);
    assert.equal(report.unknown.length,1);
    await assert.rejects(admissions.execute(plan),/Outcome unknown/);
  }
});

test('fresh retry validation preserves the unfinished-run preview and the original selection snapshot',async()=>{
  const source=results();
  const plan=prepare('retry',['batch:batch','run:failed'],source);
  assert.deepEqual(plan.entries[0].runIds,['failed']);
  assert.equal(plan.entries[0].runCount,1);
  source[2].id='changed-after-preview';
  source[2].previousRunIds.push('later-inventory');
  const report=await createAdmissions({api:async(path,options)=>{
    assert.equal(options.headers['Content-Type'],'application/json');
    assert.deepEqual(JSON.parse(options.body),{expectedRunIds:['failed']});
    return {id:'fresh-attempt',batchId:'batch',state:'queued'};
  }}).execute(plan);
  assert.equal(report.accepted.length,1);
  assert.deepEqual(plan.entries[0].runIds,['failed']);
});
